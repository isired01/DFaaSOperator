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
	"net"
	"strconv"
	"strings"
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

// monitoringAttemptsAnnotation persists the consecutive-error counter for
// the monitoring Helm install across reconciles. P4 mirror of the LoadTest
// dispatch budget.
const monitoringAttemptsAnnotation = "dfaas.dfaas.io/monitoring-attempts"

// monitoringRetryBudget is the max consecutive Helm install failures
// tolerated before the Environment is moved to EnvDegraded (P4 + P5).
const monitoringRetryBudget = 5

// sshProbeTimeout caps each per-host SSH-reachability TCP dial (P6).
const sshProbeTimeout = 2 * time.Second

// sshAttemptsAnnotation persists the consecutive SSH-unreachable counter,
// generation-scoped as "<generation>:<count>" so a spec edit restarts the
// budget fresh.
const sshAttemptsAnnotation = "dfaas.dfaas.io/ssh-attempts"

// sshRetryBudget is the max consecutive SSH-unreachable rounds tolerated
// before the Environment is moved to EnvFailed.
const sshRetryBudget = 3

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

// reconcileProvisioningVMs probes SSH reachability of every node in spec
// before advancing. P6 (option b): dial each <ip>:22 with a 2s timeout; if
// any node fails, stamp VMsReady=False/SSHUnreachable and requeue. If the
// spec has zero nodes (placeholder envs), keep the historical Skipped
// behavior so empty envs still advance.
func (r *EnvironmentReconciler) reconcileProvisioningVMs(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(env.Spec.Nodes) == 0 {
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
			metav1.ConditionTrue, dfaasv1.EnvReasonSkipped,
			"no nodes declared — placeholder phase")
		return r.setEnvPhase(ctx, env, dfaasv1.EnvProvisioningInfra)
	}

	var unreachable []string
	for _, n := range env.Spec.Nodes {
		if !probeSSH(n.IPAddress) {
			unreachable = append(unreachable, n.NodeID)
		}
	}
	if len(unreachable) > 0 {
		count, bumpErr := r.bumpSSHAttempts(ctx, env)
		if bumpErr != nil {
			logger.Error(bumpErr, "bumpSSHAttempts failed; continuing without budget enforcement")
		}
		if count >= sshRetryBudget {
			logger.Info("VMs not SSH-reachable, giving up", "nodes", unreachable, "attempts", count)
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonSSHUnreachable,
				fmt.Sprintf("SSH :22 dial failed for %v after %d attempts", unreachable, count))
			return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
		}
		logger.Info("VMs not SSH-reachable, retrying", "nodes", unreachable, "attempts", count)
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonSSHUnreachable,
			fmt.Sprintf("SSH :22 dial failed for: %v", unreachable))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if rerr := r.resetSSHAttempts(ctx, env); rerr != nil {
		logger.Error(rerr, "resetSSHAttempts failed; non-fatal")
	}
	_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonSSHReachable,
		"all declared nodes reachable on :22")
	return r.setEnvPhase(ctx, env, dfaasv1.EnvProvisioningInfra)
}

// probeSSH returns true if a TCP dial to ip:22 completes within
// sshProbeTimeout. Cheap reachability check — does NOT verify an SSH
// banner; that would require a real client + creds.
func probeSSH(ip string) bool {
	if ip == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, "22"), sshProbeTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
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
		// P3: surface libp2p key failure on DependenciesReady.
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDependenciesReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonLibp2pKeyError,
			"libp2p key ensure failed: "+condMessage(err))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDependenciesReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonInfraReady, "libp2p keys ready")

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
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondInfrastructureReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonInfraFailed,
			"dfaas-worker or k6 provisioning failed; inspect DfaasWorkersReady and K6Ready conditions")
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}
	// Both Jobs succeeded and the fan-in has settled. Now — and only now — is
	// it safe to stamp the success TTL: neither sibling will be re-checked
	// again from this phase, so auto-deletion can no longer trigger a recreate.
	r.patchAnsibleJobTTL(ctx, env, "vms", 600)
	r.patchAnsibleJobTTL(ctx, env, "k6", 600)
	_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondInfrastructureReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonInfraReady,
		"dfaas-worker and k6 Ansible Jobs completed")
	return r.setEnvPhase(ctx, env, dfaasv1.EnvProvisioningMonitoring)
}

// patchAnsibleJobTTL looks up the role-suffixed Ansible Job for env and sets
// its post-finish TTL. No-op (logs at V1) if the Job is gone — e.g. a phase
// that was skipped because the role has no nodes, so no Job was ever created.
func (r *EnvironmentReconciler) patchAnsibleJobTTL(ctx context.Context,
	env *dfaasv1.Environment, suffix string, ttlSec int32) {
	var job batchv1.Job
	name := ansible.JobNameForRole(env, suffix)
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: env.Namespace}, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "get Ansible Job for TTL patch", "job", name)
		}
		return
	}
	patchJobTTL(ctx, r.Client, &job, ttlSec)
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
		// P5: infra is up at this stage (we arrived from ProvisioningInfra
		// success); monitoring is terminally broken. Drop to Degraded
		// rather than Failed so LoadTests are still permitted.
		return r.setEnvPhase(ctx, env, dfaasv1.EnvDegraded)
	}
	if !done {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if err := r.syncNodeStatus(ctx, env); err != nil {
		// P3: surface syncNodeStatus failure.
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDependenciesReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonNodeStatusError,
			"syncNodeStatus failed: "+condMessage(err))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
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
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
			metav1.ConditionTrue, dfaasv1.EnvReasonNoWorkers,
			"no dfaas-worker nodes in spec — phase skipped")
		return true, false, nil
	}

	jobName := ansible.JobNameForRole(env, "vms")
	var job batchv1.Job
	getErr := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: env.Namespace}, &job)

	if apierrors.IsNotFound(getErr) {
		// P7: before kicking off the Job, the observed state is "we haven't
		// checked yet" — stamp Unknown/JobPending. Replaced by False/
		// AnsibleRunning once the Job exists.
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
			metav1.ConditionUnknown, dfaasv1.EnvReasonJobPending,
			"dfaas-worker Ansible Job not yet created")

		logger.Info("creating Ansible Job for dfaas-worker", "job", jobName)
		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		newJob, secret, jerr := am.CreateJobForRole(ctx, env, dfaasv1.RoleDfaasWorker, "vms", libp2pKeys)
		if jerr != nil {
			// P2: surface CreateJobForRole failure.
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"build dfaas-worker Ansible Job: "+condMessage(jerr))
			return false, false, jerr
		}
		if cerr := r.Create(ctx, secret); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			// P2.
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create dfaas-worker inventory Secret: "+condMessage(cerr))
			return false, false, cerr
		}
		if cerr := r.Create(ctx, newJob); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			// P2.
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create dfaas-worker Ansible Job: "+condMessage(cerr))
			return false, false, cerr
		}
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning,
			"dfaas-worker Ansible Job started")
		return false, false, nil
	}
	if getErr != nil {
		return false, false, getErr
	}

	if job.Status.Succeeded > 0 {
		// NB: do NOT set the success TTL here. While the sibling k6 Job may
		// still be running, ProvisioningInfra keeps re-reconciling every 10s;
		// a 600s TTL would let the Job controller delete this finished Job
		// mid-wait, the next ensureVMsJob would hit NotFound and recreate it,
		// re-running the playbook. The success TTL is applied once, after the
		// fan-in settles, in reconcileProvisioningInfra.
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
			metav1.ConditionTrue, dfaasv1.EnvReasonVMsProvisioned,
			"dfaas-worker Ansible Job completed")
		return true, false, nil
	}
	if job.Status.Failed > 0 {
		patchJobTTL(ctx, r.Client, &job, 86400)
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleFailed,
			"dfaas-worker Ansible Job failed; check logs")
		return false, true, nil
	}
	logger.Info("dfaas-worker Ansible Job still running",
		"job", jobName,
		"active", job.Status.Active,
		"succeeded", job.Status.Succeeded,
		"failed", job.Status.Failed)
	_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondDfaasWorkersReady,
		metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning,
		"dfaas-worker Ansible Job in progress")
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
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
			metav1.ConditionTrue, dfaasv1.EnvReasonNoK6Nodes,
			"no k6-load-generator nodes in spec — phase skipped")
		return true, false, nil
	}

	jobName := ansible.JobNameForRole(env, "k6")
	var job batchv1.Job
	getErr := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: env.Namespace}, &job)

	if apierrors.IsNotFound(getErr) {
		// P7.
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
			metav1.ConditionUnknown, dfaasv1.EnvReasonJobPending,
			"k6 Ansible Job not yet created")

		logger.Info("creating Ansible Job for K6", "job", jobName)
		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		newJob, secret, jerr := am.CreateJobForRole(ctx, env, dfaasv1.RoleK6LoadGenerator, "k6", nil)
		if jerr != nil {
			// P2.
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"build k6 Ansible Job: "+condMessage(jerr))
			return false, false, jerr
		}
		if cerr := r.Create(ctx, secret); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create k6 inventory Secret: "+condMessage(cerr))
			return false, false, cerr
		}
		if cerr := r.Create(ctx, newJob); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create k6 Ansible Job: "+condMessage(cerr))
			return false, false, cerr
		}
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning,
			"k6 Ansible Job started")
		return false, false, nil
	}
	if getErr != nil {
		return false, false, getErr
	}

	if job.Status.Succeeded > 0 {
		// NB: do NOT set the success TTL here — see ensureVMsJob. Deleting a
		// finished Job while the sibling is still running would trigger a
		// NotFound→recreate→rerun loop. Applied after fan-in in
		// reconcileProvisioningInfra.
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
			metav1.ConditionTrue, dfaasv1.EnvReasonK6Provisioned,
			"k6 Ansible Job completed")
		return true, false, nil
	}
	if job.Status.Failed > 0 {
		patchJobTTL(ctx, r.Client, &job, 86400)
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleFailed,
			"k6 Ansible Job failed; check logs")
		return false, true, nil
	}
	logger.Info("k6 Ansible Job still running",
		"job", jobName,
		"active", job.Status.Active,
		"succeeded", job.Status.Succeeded,
		"failed", job.Status.Failed)
	_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondK6Ready,
		metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning,
		"k6 Ansible Job in progress")
	return false, false, nil
}

// ensureMonitoring drives the Helm monitoring stack install. P4: after
// monitoringRetryBudget consecutive failures, returns failed=true so the
// caller transitions the Environment to EnvDegraded. Resets the counter on
// every successful round-trip.
func (r *EnvironmentReconciler) ensureMonitoring(ctx context.Context,
	env *dfaasv1.Environment) (done bool, failed bool, err error) {
	logger := log.FromContext(ctx)

	mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
	if derr := mm.Deploy(ctx); derr != nil {
		count, bumpErr := r.bumpMonitoringAttempts(ctx, env)
		if bumpErr != nil {
			logger.Error(bumpErr, "bumpMonitoringAttempts failed; continuing without budget enforcement")
		}
		if count >= monitoringRetryBudget {
			_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonHelmFailed,
				fmt.Sprintf("monitoring Helm install failed %d consecutive times: %s",
					count, condMessage(derr)))
			return false, true, nil
		}
		// P7 + P14: first ever observation is Unknown; subsequent retries
		// stay False/HelmInstalling. The sanitized message keeps
		// LastTransitionTime stable across reconciles when the error class
		// is the same.
		condStatus := metav1.ConditionFalse
		if count == 1 {
			condStatus = metav1.ConditionUnknown
		}
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
			condStatus, dfaasv1.EnvReasonHelmInstalling,
			"monitoring Helm install in progress / retrying: "+condMessage(derr))
		return false, false, nil
	}
	if rerr := r.resetMonitoringAttempts(ctx, env); rerr != nil {
		logger.Error(rerr, "resetMonitoringAttempts failed; non-fatal")
	}
	ready, _ := mm.Check(ctx)
	if !ready {
		_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonWaitingPods,
			"monitoring pods not Ready yet")
		return false, false, nil
	}

	_ = r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonPodsRunning,
		"monitoring stack up")
	_ = mm.ReconcileTargets(ctx, env)
	return true, false, nil
}

// bumpMonitoringAttempts increments the env-level retry counter for the
// monitoring Helm install. P4 mirror of LoadTest dispatchAttempts.
func (r *EnvironmentReconciler) bumpMonitoringAttempts(ctx context.Context,
	env *dfaasv1.Environment) (int, error) {
	var newVal int
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(env), latest); gerr != nil {
			return gerr
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		cur := 0
		if s, ok := latest.Annotations[monitoringAttemptsAnnotation]; ok {
			if n, perr := strconv.Atoi(s); perr == nil {
				cur = n
			}
		}
		cur++
		latest.Annotations[monitoringAttemptsAnnotation] = strconv.Itoa(cur)
		newVal = cur
		return r.Update(ctx, latest)
	})
	return newVal, err
}

// resetMonitoringAttempts zeroes the counter annotation on success. No-op
// when already "0" to avoid churn.
func (r *EnvironmentReconciler) resetMonitoringAttempts(ctx context.Context,
	env *dfaasv1.Environment) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(env), latest); gerr != nil {
			return gerr
		}
		if cur, ok := latest.Annotations[monitoringAttemptsAnnotation]; ok && cur == "0" {
			return nil
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[monitoringAttemptsAnnotation] = "0"
		return r.Update(ctx, latest)
	})
}

// parseSSHAttempts decodes the "<generation>:<count>" annotation. Returns the
// stored generation and count; (0, 0) if absent or malformed.
func parseSSHAttempts(s string) (gen int64, count int) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	g, gerr := strconv.ParseInt(parts[0], 10, 64)
	c, cerr := strconv.Atoi(parts[1])
	if gerr != nil || cerr != nil {
		return 0, 0
	}
	return g, c
}

// bumpSSHAttempts increments the consecutive SSH-unreachable counter,
// generation-scoped: a stored generation different from the current one
// (spec edit) restarts the budget at 1. Returns the count for this generation.
func (r *EnvironmentReconciler) bumpSSHAttempts(ctx context.Context,
	env *dfaasv1.Environment) (int, error) {
	var newVal int
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(env), latest); gerr != nil {
			return gerr
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		storedGen, cur := parseSSHAttempts(latest.Annotations[sshAttemptsAnnotation])
		if storedGen != latest.Generation {
			cur = 0
		}
		cur++
		latest.Annotations[sshAttemptsAnnotation] = fmt.Sprintf("%d:%d", latest.Generation, cur)
		newVal = cur
		return r.Update(ctx, latest)
	})
	return newVal, err
}

// resetSSHAttempts zeroes the counter for the current generation on success.
// No-op when already absent or zero to avoid churn.
func (r *EnvironmentReconciler) resetSSHAttempts(ctx context.Context,
	env *dfaasv1.Environment) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(env), latest); gerr != nil {
			return gerr
		}
		if _, cur := parseSSHAttempts(latest.Annotations[sshAttemptsAnnotation]); cur == 0 {
			return nil
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[sshAttemptsAnnotation] = fmt.Sprintf("%d:0", latest.Generation)
		return r.Update(ctx, latest)
	})
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
		// Note: omit LastTransitionTime — meta.SetStatusCondition stamps it
		// only when status/reason/message actually changes. Letting the
		// helper set it preserves stability across no-op reconciles (P14).
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    condType,
			Status:  status,
			Reason:  reason,
			Message: message,
		})
		return r.Status().Update(ctx, latest)
	})
}

// resetTransientConditions handles generation drift (P8). The five
// per-subsystem conditions are LEFT at their last-observed state — they
// are still factually true at the instant of spec edit. The single
// EnvCondUpdating condition is stamped True/SpecChanged to drive the UI
// "Updating" badge from one place. EnvCondReady aggregator flips to
// Unknown/Initializing so consumers stop trusting the previous True.
func (r *EnvironmentReconciler) resetTransientConditions(ctx context.Context,
	env *dfaasv1.Environment) error {
	if err := r.setEnvCondition(ctx, env, dfaasv1.EnvCondUpdating,
		metav1.ConditionTrue, dfaasv1.EnvReasonSpecChanged,
		"Environment spec edited; reconciler is restarting provisioning"); err != nil {
		return err
	}
	return r.setEnvCondition(ctx, env, dfaasv1.EnvCondReady,
		metav1.ConditionUnknown, dfaasv1.EnvReasonInitializing,
		"spec edited; awaiting provisioning to settle")
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
