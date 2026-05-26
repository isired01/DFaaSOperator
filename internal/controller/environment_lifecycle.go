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
	"fmt"
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

// reconcileProvisioningVMs is currently a placeholder. The VMs are assumed
// to already exist (provisioned out-of-band).
func (r *EnvironmentReconciler) reconcileProvisioningVMs(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	log.FromContext(ctx).Info("ProvisioningVMs is a no-op placeholder — advancing",
		"env", env.Name)
	_ = r.setEnvCondition(ctx, env, "VMsReady", metav1.ConditionTrue,
		"Skipped", "VMs assumed pre-existing — placeholder phase")
	return r.setEnvPhase(ctx, env, dfaasv1.EnvProvisioningInfra)
}

// reconcileProvisioningInfra runs the dfaas-worker Ansible Job and the k6
// Ansible Job concurrently (side-by-side resources, not goroutines). Fan-in:
// advances to ProvisioningMonitoring only when BOTH have reached a terminal
// state and succeeded; transitions to Failed only after BOTH have settled,
// so the Conditions reflect a coherent picture (e.g. "dfaas OK, k6 failed")
// rather than aborting one mid-flight.
func (r *EnvironmentReconciler) reconcileProvisioningInfra(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {

	am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
	libp2pKeys, err := am.EnsureLibp2pKeys(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}

	vmsDone, vmsFailed, err := r.ensureVMsJob(ctx, env, libp2pKeys)
	if err != nil {
		return ctrl.Result{}, err
	}
	k6Done, k6Failed, err := r.ensureK6Job(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}

	vmsTerminal := vmsDone || vmsFailed
	k6Terminal := k6Done || k6Failed
	if !(vmsTerminal && k6Terminal) {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if vmsFailed || k6Failed {
		_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionFalse,
			"InfraFailed",
			"dfaas-worker or k6 provisioning failed; inspect DfaasWorkersReady and K6Ready conditions")
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}
	_ = r.setEnvCondition(ctx, env, "InfrastructureReady", metav1.ConditionTrue,
		"InfraReady", "dfaas-worker and k6 Ansible Jobs completed")
	return r.setEnvPhase(ctx, env, dfaasv1.EnvProvisioningMonitoring)
}

// reconcileProvisioningMonitoring installs Prometheus + Grafana via Helm,
// reconciles per-environment scrape targets, and stamps node status. Runs
// AFTER ProvisioningInfra so worker /metrics endpoints already exist when
// the first scrape fires.
func (r *EnvironmentReconciler) reconcileProvisioningMonitoring(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {

	done, failed, err := r.ensureMonitoring(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failed {
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}
	if !done {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if err := r.syncNodeStatus(ctx, env); err != nil {
		return ctrl.Result{}, err
	}
	return r.setEnvPhase(ctx, env, dfaasv1.EnvReady)
}

// ensureVMsJob is the non-advancing variant for the dfaas-worker Ansible
// Job, used by ProvisioningInfra fan-in. Returns done/failed flags and
// stamps the DfaasWorkersReady Condition; never calls setEnvPhase.
func (r *EnvironmentReconciler) ensureVMsJob(ctx context.Context,
	env *dfaasv1.Environment, libp2pKeys map[string]string) (done bool, failed bool, err error) {
	logger := log.FromContext(ctx)

	if !env.HasNodeWithRole(dfaasv1.RoleDfaasWorker) {
		logger.Info("no dfaas-worker nodes, skipping VMs Ansible phase")
		_ = r.setEnvCondition(ctx, env, "DfaasWorkersReady", metav1.ConditionTrue,
			"NoWorkers", "no dfaas-worker nodes in spec — phase skipped")
		return true, false, nil
	}

	jobName := ansible.JobNameForRole(env, "vms")
	var job batchv1.Job
	getErr := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: env.Namespace}, &job)

	if apierrors.IsNotFound(getErr) {
		logger.Info("creating Ansible Job for dfaas-worker", "job", jobName)
		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		newJob, secret, err := am.CreateJobForRole(ctx, env, dfaasv1.RoleDfaasWorker, "vms", libp2pKeys)
		if err != nil {
			return false, false, err
		}
		if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, false, err
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, false, err
		}
		_ = r.setEnvCondition(ctx, env, "DfaasWorkersReady", metav1.ConditionFalse,
			"AnsibleRunning", "dfaas-worker Ansible Job started")
		return false, false, nil
	}
	if getErr != nil {
		return false, false, getErr
	}

	if job.Status.Succeeded > 0 {
		patchJobTTL(ctx, r.Client, &job, 600)
		_ = r.setEnvCondition(ctx, env, "DfaasWorkersReady", metav1.ConditionTrue,
			"VMsProvisioned", "dfaas-worker Ansible Job "+jobName+" completed")
		return true, false, nil
	}
	if job.Status.Failed > 0 {
		patchJobTTL(ctx, r.Client, &job, 86400)
		_ = r.setEnvCondition(ctx, env, "DfaasWorkersReady", metav1.ConditionFalse,
			"AnsibleFailed", "dfaas-worker Ansible Job "+jobName+" failed; check logs")
		return false, true, nil
	}
	logger.Info("dfaas-worker Ansible Job still running",
		"job", jobName,
		"active", job.Status.Active,
		"succeeded", job.Status.Succeeded,
		"failed", job.Status.Failed)
	_ = r.setEnvCondition(ctx, env, "DfaasWorkersReady", metav1.ConditionFalse,
		"AnsibleRunning", "dfaas-worker Ansible Job "+jobName+" in progress")
	return false, false, nil
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
	logger.Info("k6 Ansible Job still running",
		"job", jobName,
		"active", job.Status.Active,
		"succeeded", job.Status.Succeeded,
		"failed", job.Status.Failed)
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

// resetTransientConditions flips every operator-managed provisioning
// Condition to False with reason=Updating. Called on generation drift so
// the UI stops showing stale True values during the update window. The
// ensure* helpers will re-stamp these Conditions to the appropriate state
// (False/AnsibleRunning, True/Provisioned, etc.) as the new-gen FSM
// progresses. No new Condition types are introduced.
func (r *EnvironmentReconciler) resetTransientConditions(ctx context.Context,
	env *dfaasv1.Environment) error {

	transient := []string{
		"VMsReady",
		"DfaasWorkersReady",
		"K6Ready",
		"InfrastructureReady",
		"MonitoringReady",
	}
	for _, t := range transient {
		if err := r.setEnvCondition(ctx, env, t, metav1.ConditionFalse,
			"Updating",
			"Environment spec edited; reconciler is restarting provisioning"); err != nil {
			return err
		}
	}
	return nil
}

// cleanupStaleGenJobs deletes Ansible Jobs for env whose generation label
// does not match env.Generation. Idempotent — no-op if no stale Jobs.
// Used on generation drift to abort old-gen Ansible runs before starting
// the new one (avoids two Ansible playbooks racing on the same VMs).
func (r *EnvironmentReconciler) cleanupStaleGenJobs(ctx context.Context,
	env *dfaasv1.Environment) error {
	logger := log.FromContext(ctx)

	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs,
		client.InNamespace(env.Namespace),
		client.MatchingLabels{ansible.LabelEnvironment: env.Name},
	); err != nil {
		return fmt.Errorf("list jobs: %w", err)
	}

	currentGen := fmt.Sprintf("%d", env.Generation)
	propagation := metav1.DeletePropagationBackground
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if j.Labels[ansible.LabelGeneration] == currentGen {
			continue
		}
		logger.Info("deleting stale-gen Ansible Job",
			"job", j.Name,
			"staleGen", j.Labels[ansible.LabelGeneration],
			"currentGen", currentGen)
		if err := r.Delete(ctx, j, &client.DeleteOptions{
			PropagationPolicy: &propagation,
		}); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "delete stale Job", "job", j.Name)
		}
	}
	return nil
}
