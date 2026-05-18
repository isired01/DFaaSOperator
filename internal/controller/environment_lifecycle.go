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
// dFaaS playbook against the dfaas-worker nodes. Before the Job is created,
// it materializes operator-managed libp2p keys for any dfaas-worker node that
// did not ship a `privateKey` in spec. Phase advances to ProvisioningInfra
// (parallel K6 + Monitoring) once the Job succeeds.
func (r *EnvironmentReconciler) reconcileProvisioningVMs(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
	keys, err := am.EnsureLibp2pKeys(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.runAnsiblePhase(ctx, env,
		dfaasv1.RoleDfaasWorker,
		"vms",
		dfaasv1.EnvProvisioningInfra,
		"VMsProvisioned",
		keys,
	)
}

// reconcileProvisioningInfra runs the K6 Ansible Job and the monitoring stack
// install concurrently (side-by-side resources, not goroutines). Fan-in:
// advances to Ready only when BOTH have reached a terminal state and both
// succeeded; transitions to Failed only after BOTH have settled, so the
// Conditions reflect a coherent picture (e.g. "K6 failed, Monitoring OK")
// rather than aborting one mid-flight.
func (r *EnvironmentReconciler) reconcileProvisioningInfra(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {

	k6Done, k6Failed, err := r.ensureK6Job(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	monDone, monFailed, err := r.ensureMonitoring(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}

	k6Terminal := k6Done || k6Failed
	monTerminal := monDone || monFailed
	if !(k6Terminal && monTerminal) {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if k6Failed || monFailed {
		_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionFalse,
			"InfraFailed", "K6 or Monitoring provisioning failed; inspect K6Ready and MonitoringReady conditions")
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}
	_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionTrue,
		"InfraReady", "K6 and Monitoring provisioning completed")
	if err := r.syncNodeStatus(ctx, env); err != nil {
		return ctrl.Result{}, err
	}
	return r.setEnvPhase(ctx, env, dfaasv1.EnvReady)
}

// ensureK6Job is the non-advancing variant of runAnsiblePhase used by the
// parallel ProvisioningInfra fan-in. Returns done/failed flags and stamps the
// K6Ready Condition; never calls setEnvPhase.
func (r *EnvironmentReconciler) ensureK6Job(ctx context.Context,
	env *dfaasv1.Environment) (done bool, failed bool, err error) {
	logger := log.FromContext(ctx)

	if !env.HasNodeWithRole(dfaasv1.RoleK6LoadGenerator) {
		logger.Info("no k6-load-generator nodes, skipping K6 phase")
		_ = r.setEnvCondition(ctx, env, "K6Ready", metav1.ConditionTrue,
			"NoK6Nodes", "no k6-load-generator nodes in spec — phase skipped")
		return true, false, nil
	}

	jobName := ansible.JobNameForRole(env, "k6")
	var job batchv1.Job
	getErr := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: env.Namespace}, &job)

	if apierrors.IsNotFound(getErr) {
		logger.Info("creating Ansible Job for K6", "job", jobName)
		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		newJob, secret, err := am.CreateJobForRole(ctx, env, dfaasv1.RoleK6LoadGenerator, "k6", nil)
		if err != nil {
			return false, false, err
		}
		if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, false, err
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, false, err
		}
		_ = r.setEnvCondition(ctx, env, "K6Ready", metav1.ConditionFalse,
			"AnsibleRunning", "K6 Ansible Job started")
		return false, false, nil
	}
	if getErr != nil {
		return false, false, getErr
	}

	if job.Status.Succeeded > 0 {
		patchJobTTL(ctx, r.Client, &job, 600)
		_ = r.setEnvCondition(ctx, env, "K6Ready", metav1.ConditionTrue,
			"K6Provisioned", "K6 Ansible Job "+jobName+" completed")
		return true, false, nil
	}
	if job.Status.Failed > 0 {
		patchJobTTL(ctx, r.Client, &job, 86400)
		_ = r.setEnvCondition(ctx, env, "K6Ready", metav1.ConditionFalse,
			"AnsibleFailed", "K6 Ansible Job "+jobName+" failed; check logs")
		return false, true, nil
	}
	_ = r.setEnvCondition(ctx, env, "K6Ready", metav1.ConditionFalse,
		"AnsibleRunning", "K6 Ansible Job "+jobName+" in progress")
	return false, false, nil
}

// ensureMonitoring is the non-advancing variant of the monitoring-stack
// provisioning step. Returns done/failed and stamps MonitoringReady.
// Currently the monitoring stack can only be "not ready" or "ready" — there
// is no explicit failure path from Helm + check, so `failed` stays false and
// the operator simply requeues forever. Plumbed for future hardening.
func (r *EnvironmentReconciler) ensureMonitoring(ctx context.Context,
	env *dfaasv1.Environment) (done bool, failed bool, err error) {

	mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
	if err := mm.Deploy(ctx); err != nil {
		_ = r.setEnvCondition(ctx, env, "MonitoringReady", metav1.ConditionFalse,
			"HelmInstalling", "monitoring Helm install in progress / retrying: "+err.Error())
		return false, false, nil
	}
	ready, _ := mm.Check(ctx)
	if !ready {
		_ = r.setEnvCondition(ctx, env, "MonitoringReady", metav1.ConditionFalse,
			"WaitingPods", "monitoring pods not Ready yet")
		return false, false, nil
	}

	_ = r.setEnvCondition(ctx, env, "MonitoringReady", metav1.ConditionTrue,
		"PodsRunning", "monitoring stack up")
	_ = mm.ReconcileTargets(ctx, env)
	return true, false, nil
}

// runAnsiblePhase is the shared "ensure Ansible Job, wait, advance phase"
// loop used by VM provisioning (the only remaining sequential phase). For
// the parallel K6 + Monitoring phase use the non-advancing helpers above.
func (r *EnvironmentReconciler) runAnsiblePhase(ctx context.Context,
	env *dfaasv1.Environment,
	role dfaasv1.NodeRole, jobSuffix string,
	nextPhase dfaasv1.EnvironmentPhase, reason string,
	libp2pKeys map[string]string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	jobName := ansible.JobNameForRole(env, jobSuffix)
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
		newJob, secret, err := am.CreateJobForRole(ctx, env, role, jobSuffix, libp2pKeys)
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
		patchJobTTL(ctx, r.Client, &job, 600)
		_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionTrue,
			reason, "Ansible Job "+jobName+" completed")
		return r.setEnvPhase(ctx, env, nextPhase)
	}
	if job.Status.Failed > 0 {
		logger.Info("Ansible Job failed", "job", jobName)
		patchJobTTL(ctx, r.Client, &job, 86400)
		_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionFalse,
			"AnsibleFailed", "Ansible Job "+jobName+" failed; check logs")
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}

	logger.Info("Ansible Job in progress", "job", jobName)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// patchJobTTL sets Spec.TTLSecondsAfterFinished on a finished Ansible Job
// to drive Job-controller auto-cleanup. Used with ttlSec=600 on success and
// ttlSec=86400 (24h grace) on failure. Idempotent: skips when TTL is
// already set so re-reconciles do not churn.
func patchJobTTL(ctx context.Context, c client.Client, job *batchv1.Job, ttlSec int32) {
	if job.Spec.TTLSecondsAfterFinished != nil {
		return
	}
	patched := job.DeepCopy()
	ttl := ttlSec
	patched.Spec.TTLSecondsAfterFinished = &ttl
	if err := c.Patch(ctx, patched, client.MergeFrom(job)); err != nil {
		log.FromContext(ctx).Error(err, "patch TTL on Ansible Job",
			"job", job.Name, "ttlSeconds", ttlSec)
	}
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
					KubeconfigSecret: latest.Name + "-" + n.NodeID + "-kubeconfig",
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
