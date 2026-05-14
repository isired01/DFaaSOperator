/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
	"dfaas-operator/internal/controller/monitoring"
)

// handleEnvDeletion drains per-environment cluster-wide state (Prometheus
// targets) and removes the finalizer. Per-environment Jobs and ConfigMaps
// carry ControllerReferences and are garbage-collected automatically.
func (r *EnvironmentReconciler) handleEnvDeletion(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(env, environmentFinalizer) {
		logger.Info("environment deletion: cleaning up Prometheus targets")

		mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
		_ = mm.CleanupTargets(ctx, env)

		controllerutil.RemoveFinalizer(env, environmentFinalizer)
		if err := r.Update(ctx, env); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// reconcileProvisioningVMs creates an Ansible Job that runs the base-OS +
// dFaaS playbook against the dfaas-worker nodes.
func (r *EnvironmentReconciler) reconcileProvisioningVMs(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	return r.runAnsiblePhase(ctx, env,
		dfaasv1.RoleDfaasWorker,
		"vms",
		dfaasv1.EnvProvisioningK6,
		"VMsProvisioned",
	)
}

// reconcileProvisioningK6 creates an Ansible Job that runs the k3s + k6
// playbook against the k6-load-generator nodes.
func (r *EnvironmentReconciler) reconcileProvisioningK6(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	return r.runAnsiblePhase(ctx, env,
		dfaasv1.RoleK6LoadGenerator,
		"k6",
		dfaasv1.EnvProvisioningMonitoring,
		"K6Provisioned",
	)
}

// runAnsiblePhase is the shared "ensure Ansible Job, wait, advance phase"
// loop used by both VM and K6 provisioning. jobSuffix becomes part of the Job
// name; nextPhase is the phase to advance to on success; reason is the
// condition reason stamped on completion.
func (r *EnvironmentReconciler) runAnsiblePhase(ctx context.Context,
	env *dfaasv1.Environment,
	role dfaasv1.NodeRole, jobSuffix string,
	nextPhase dfaasv1.EnvironmentPhase, reason string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	jobName := env.Name + "-infra-" + jobSuffix + "-job"
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: env.Namespace}, &job)

	if apierrors.IsNotFound(err) {
		// If no nodes match the role, skip the whole phase.
		if !env.HasNodeWithRole(role) {
			logger.Info("no nodes for role, skipping phase",
				"role", role, "nextPhase", nextPhase)
			return r.setEnvPhase(ctx, env, nextPhase)
		}

		logger.Info("creating Ansible Job for phase", "role", role, "job", jobName)

		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		newJob, secret, err := am.CreateJobForRole(ctx, env, role, jobSuffix)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionFalse,
			"AnsibleStarted", "Ansible Job started for role "+string(role))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if job.Status.Succeeded > 0 {
		logger.Info("Ansible Job succeeded", "job", jobName)
		_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionTrue,
			reason, "Ansible Job "+jobName+" completed")
		return r.setEnvPhase(ctx, env, nextPhase)
	}
	if job.Status.Failed > 0 {
		logger.Info("Ansible Job failed", "job", jobName)
		_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionFalse,
			"AnsibleFailed", "Ansible Job "+jobName+" failed; check logs")
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}

	logger.Info("Ansible Job in progress", "job", jobName)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// reconcileProvisioningMonitoring installs the Helm-based Prometheus/Grafana
// stack and writes per-environment scrape targets.
func (r *EnvironmentReconciler) reconcileProvisioningMonitoring(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {

	mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
	if err := mm.Deploy(ctx); err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	ready, _ := mm.Check(ctx)
	if !ready {
		_ = r.setEnvCondition(ctx, env, "MonitoringReady", metav1.ConditionFalse,
			"WaitingPods", "Monitoring pods not ready yet")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	_ = r.setEnvCondition(ctx, env, "MonitoringReady", metav1.ConditionTrue,
		"PodsRunning", "Monitoring stack up")
	_ = mm.ReconcileTargets(ctx, env)

	// Stamp k6/dfaas node summaries on the Environment so LoadTest can resolve
	// remote clusters without walking the spec.
	if err := r.syncNodeStatus(ctx, env); err != nil {
		return ctrl.Result{}, err
	}
	return r.setEnvPhase(ctx, env, dfaasv1.EnvReady)
}

// syncNodeStatus surfaces k6/dfaas node info into status, for fast lookup by
// the LoadTestReconciler.
func (r *EnvironmentReconciler) syncNodeStatus(ctx context.Context, env *dfaasv1.Environment) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(env), latest); err != nil {
			return err
		}
		var k6 []dfaasv1.K6NodeStatus
		var dfaas []string
		for _, n := range latest.Spec.Nodes {
			switch n.Role {
			case dfaasv1.RoleK6LoadGenerator:
				k6 = append(k6, dfaasv1.K6NodeStatus{
					NodeID:           n.NodeID,
					IPAddress:        n.IPAddress,
					KubeconfigSecret: n.NodeID + "-kubeconfig",
				})
			case dfaasv1.RoleDfaasWorker:
				dfaas = append(dfaas, n.NodeID)
			}
		}
		latest.Status.K6Nodes = k6
		latest.Status.DfaasNodes = dfaas
		return r.Status().Update(ctx, latest)
	})
}

// setEnvCondition sets a Condition on Environment.status using the
// rifetch-then-update pattern that absorbs informer cache lag.
func (r *EnvironmentReconciler) setEnvCondition(ctx context.Context,
	env *dfaasv1.Environment, condType string, status metav1.ConditionStatus,
	reason, message string) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(env), latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               condType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		})
		return r.Status().Update(ctx, latest)
	})
}
