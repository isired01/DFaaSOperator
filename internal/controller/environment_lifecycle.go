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
	corev1 "k8s.io/api/core/v1"
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

// healthCheckInterval is the cadence of the Ready-state SSH liveness probe.
const healthCheckInterval = time.Minute

// healthRetryInterval is the faster cadence used to confirm a suspected miss
// before giving up.
const healthRetryInterval = 20 * time.Second

// healthRetryBudget is the max consecutive unreachable health rounds tolerated
// before a Ready Environment is moved to EnvFailed. A small budget prevents a
// single dropped packet from flapping a healthy env into Failed.
const healthRetryBudget = 3

// healthMissesAnnotation persists the consecutive Ready-state unreachable
// counter, generation-scoped as "<generation>:<count>". Kept separate from
// sshAttemptsAnnotation so the health loop never clobbers provisioning state.
const healthMissesAnnotation = "dfaas.dfaas.io/health-misses"

// jobTTLSuccessSeconds is the TTLSecondsAfterFinished applied to a successful
// Ansible Job so the Job controller cleans it up shortly after completion.
const jobTTLSuccessSeconds int32 = 600

// jobTTLFailureGraceSeconds is the TTLSecondsAfterFinished applied to a failed
// Ansible Job (24h), giving an audit window before auto-cleanup.
const jobTTLFailureGraceSeconds int32 = 86400

// handleEnvDeletion drains per-environment cluster-wide state (Prometheus
// targets) and removes the finalizer. Per-environment Jobs and ConfigMaps
// carry ControllerReferences and are garbage-collected automatically.
func (r *EnvironmentReconciler) handleEnvDeletion(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(env, environmentFinalizer) {
		logger.Info("environment deletion: cleaning up Prometheus targets")

		mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
		logStatusErr(ctx, "cleanup Prometheus targets", mm.CleanupTargets(ctx, env))

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
		logStatusErr(ctx, "stamp VMsReady=True (skipped)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
			metav1.ConditionTrue, dfaasv1.EnvReasonSkipped,
			"no nodes declared — placeholder phase"))
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
			logStatusErr(ctx, "stamp VMsReady=False (budget exhausted)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonSSHUnreachable,
				fmt.Sprintf("SSH :22 dial failed for %v after %d attempts", unreachable, count)))
			return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
		}
		logger.Info("VMs not SSH-reachable, retrying", "nodes", unreachable, "attempts", count)
		logStatusErr(ctx, "stamp VMsReady=False (retrying)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonSSHUnreachable,
			fmt.Sprintf("SSH :22 dial failed for: %v", unreachable)))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if rerr := r.resetSSHAttempts(ctx, env); rerr != nil {
		logger.Error(rerr, "resetSSHAttempts failed; non-fatal")
	}
	logStatusErr(ctx, "stamp VMsReady=True (reachable)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonSSHReachable,
		"all declared nodes reachable on :22"))
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

// reconcileReadyHealth runs the periodic SSH (:22) liveness probe while an
// Environment is Ready. It replaces the old idle short-circuit: a node that
// dies after provisioning is now noticed within ~3 minutes instead of only
// when a test fails against it.
//
// Cadence is driven entirely by the returned RequeueAfter (60s healthy, 20s
// while confirming a miss). The reconciler advances its own FSM by
// re-reconciling on its status writes, so this method is throttled on
// status.lastHealthCheck: without that guard each health write would
// re-enqueue immediately and hot-loop. The throttle window shrinks to
// healthRetryInterval while the NodesReachable condition is False so misses
// are confirmed faster.
//
// After healthRetryBudget consecutive unreachable rounds the Environment is
// moved to EnvFailed; recovery is manual (spec edit → drift → re-provision),
// matching the user decision and the existing Failed semantics.
func (r *EnvironmentReconciler) reconcileReadyHealth(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(env.Spec.Nodes) == 0 {
		return ctrl.Result{RequeueAfter: healthCheckInterval}, nil
	}

	// Throttle against our own status writes.
	if last := env.Status.LastHealthCheck; last != nil {
		due := healthCheckInterval
		if c := meta.FindStatusCondition(env.Status.Conditions, dfaasv1.EnvCondNodesReachable); c != nil &&
			c.Status == metav1.ConditionFalse {
			due = healthRetryInterval
		}
		if elapsed := time.Since(last.Time); elapsed < due {
			return ctrl.Result{RequeueAfter: due - elapsed}, nil
		}
	}

	var unreachable []string
	for _, n := range env.Spec.Nodes {
		if !probeSSH(n.IPAddress) {
			unreachable = append(unreachable, n.NodeID)
		}
	}

	if len(unreachable) == 0 {
		if rerr := r.resetHealthMisses(ctx, env); rerr != nil {
			logger.Error(rerr, "resetHealthMisses failed; non-fatal")
		}
		if err := r.markNodesReachable(ctx, env); err != nil {
			logger.Error(err, "stamp NodesReachable=True failed; retrying")
		}
		return ctrl.Result{RequeueAfter: healthCheckInterval}, nil
	}

	count, bumpErr := r.bumpHealthMisses(ctx, env)
	if bumpErr != nil {
		logger.Error(bumpErr, "bumpHealthMisses failed; continuing without budget enforcement")
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(env, corev1.EventTypeWarning, dfaasv1.EnvReasonSSHUnreachable,
			"SSH :22 unreachable for nodes %v (attempt %d/%d)", unreachable, count, healthRetryBudget)
	}
	if count >= healthRetryBudget {
		logger.Info("Ready environment nodes unreachable; transitioning to Failed",
			"nodes", unreachable, "attempts", count)
		logStatusErr(ctx, "mark NodesReachable=False (budget exhausted)", r.markNodesUnreachable(ctx, env,
			fmt.Sprintf("SSH :22 dial failed for %v after %d consecutive health checks", unreachable, count)))
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}
	logger.Info("Ready environment nodes unreachable; will retry",
		"nodes", unreachable, "attempts", count)
	logStatusErr(ctx, "mark NodesReachable=False (retrying)", r.markNodesUnreachable(ctx, env,
		fmt.Sprintf("SSH :22 dial failed for %v (attempt %d/%d)", unreachable, count, healthRetryBudget)))
	return ctrl.Result{RequeueAfter: healthRetryInterval}, nil
}

// markNodesReachable stamps NodesReachable=True and refreshes lastHealthCheck
// in a single status update (one write per healthy round → one re-enqueue,
// caught by the throttle).
func (r *EnvironmentReconciler) markNodesReachable(ctx context.Context,
	env *dfaasv1.Environment) error {
	return r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(env), func(latest *dfaasv1.Environment) error {
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    dfaasv1.EnvCondNodesReachable,
			Status:  metav1.ConditionTrue,
			Reason:  dfaasv1.EnvReasonSSHReachable,
			Message: "all declared nodes reachable on :22",
		})
		now := metav1.Now()
		latest.Status.LastHealthCheck = &now
		return nil
	})
}

// markNodesUnreachable stamps NodesReachable=False with msg and refreshes
// lastHealthCheck in a single status update.
func (r *EnvironmentReconciler) markNodesUnreachable(ctx context.Context,
	env *dfaasv1.Environment, msg string) error {
	return r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(env), func(latest *dfaasv1.Environment) error {
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    dfaasv1.EnvCondNodesReachable,
			Status:  metav1.ConditionFalse,
			Reason:  dfaasv1.EnvReasonSSHUnreachable,
			Message: msg,
		})
		now := metav1.Now()
		latest.Status.LastHealthCheck = &now
		return nil
	})
}

// bumpHealthMisses increments the consecutive Ready-state unreachable counter,
// generation-scoped (a spec edit restarts the budget). Mirrors bumpSSHAttempts
// but on a dedicated annotation so provisioning state is never clobbered.
func (r *EnvironmentReconciler) bumpHealthMisses(ctx context.Context,
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
		storedGen, cur := parseSSHAttempts(latest.Annotations[healthMissesAnnotation])
		if storedGen != latest.Generation {
			cur = 0
		}
		cur++
		latest.Annotations[healthMissesAnnotation] = fmt.Sprintf("%d:%d", latest.Generation, cur)
		newVal = cur
		return r.Update(ctx, latest)
	})
	return newVal, err
}

// resetHealthMisses zeroes the health-miss counter for the current generation.
// No-op when already zero to avoid annotation churn (and a spurious re-enqueue).
func (r *EnvironmentReconciler) resetHealthMisses(ctx context.Context,
	env *dfaasv1.Environment) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(env), latest); gerr != nil {
			return gerr
		}
		if _, cur := parseSSHAttempts(latest.Annotations[healthMissesAnnotation]); cur == 0 {
			return nil
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[healthMissesAnnotation] = fmt.Sprintf("%d:0", latest.Generation)
		return r.Update(ctx, latest)
	})
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
		log.FromContext(ctx).Error(err, "libp2p key ensure failed; retrying")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
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
		logStatusErr(ctx, "stamp InfrastructureReady=False", r.setEnvCondition(ctx, env, dfaasv1.EnvCondInfrastructureReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonInfraFailed,
			"dfaas-worker or k6 provisioning failed; inspect DfaasWorkersReady and K6Ready conditions"))
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
	}
	// Both Jobs succeeded and the fan-in has settled. Now — and only now — is
	// it safe to stamp the success TTL: neither sibling will be re-checked
	// again from this phase, so auto-deletion can no longer trigger a recreate.
	r.patchAnsibleJobTTL(ctx, env, "vms", jobTTLSuccessSeconds)
	r.patchAnsibleJobTTL(ctx, env, "k6", jobTTLSuccessSeconds)
	logStatusErr(ctx, "stamp InfrastructureReady=True", r.setEnvCondition(ctx, env, dfaasv1.EnvCondInfrastructureReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonInfraReady,
		"dfaas-worker and k6 Ansible Jobs completed"))
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
		log.FromContext(ctx).Error(err, "syncNodeStatus failed; retrying")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return r.setEnvPhase(ctx, env, dfaasv1.EnvReady)
}

// ansibleJobSpec bundles the role-specific knobs shared by ensureVMsJob and
// ensureK6Job so the common ensureAnsibleJob body can stamp the right
// Condition type, reasons and messages for each provisioning stream.
type ansibleJobSpec struct {
	role          dfaasv1.NodeRole // node role this Job targets
	jobSuffix     string           // suffix passed to ansible.JobNameForRole
	condType      string           // Condition type stamped on env.status
	skipReason    string           // reason when the role has no nodes
	skipMessage   string           // message when the role has no nodes
	succeedReason string           // reason on Job success
	humanRole     string           // human-readable role name for log/messages
	libp2pKeys    map[string]string
}

// ensureVMsJob is the non-advancing variant for the dfaas-worker Ansible
// Job, used by ProvisioningInfra fan-in. Returns done/failed flags and
// stamps the DfaasWorkersReady Condition; never calls setEnvPhase.
func (r *EnvironmentReconciler) ensureVMsJob(ctx context.Context,
	env *dfaasv1.Environment, libp2pKeys map[string]string) (done bool, failed bool, err error) {
	return r.ensureAnsibleJob(ctx, env, ansibleJobSpec{
		role:          dfaasv1.RoleDfaasWorker,
		jobSuffix:     "vms",
		condType:      dfaasv1.EnvCondDfaasWorkersReady,
		skipReason:    dfaasv1.EnvReasonNoWorkers,
		skipMessage:   "no dfaas-worker nodes in spec — phase skipped",
		succeedReason: dfaasv1.EnvReasonVMsProvisioned,
		humanRole:     "dfaas-worker",
		libp2pKeys:    libp2pKeys,
	})
}

// ensureK6Job is the non-advancing variant used by the parallel
// ProvisioningInfra fan-in. Returns done/failed flags and stamps the K6Ready
// Condition; never calls setEnvPhase.
func (r *EnvironmentReconciler) ensureK6Job(ctx context.Context,
	env *dfaasv1.Environment) (done bool, failed bool, err error) {
	return r.ensureAnsibleJob(ctx, env, ansibleJobSpec{
		role:          dfaasv1.RoleK6LoadGenerator,
		jobSuffix:     "k6",
		condType:      dfaasv1.EnvCondK6Ready,
		skipReason:    dfaasv1.EnvReasonNoK6Nodes,
		skipMessage:   "no k6-load-generator nodes in spec — phase skipped",
		succeedReason: dfaasv1.EnvReasonK6Provisioned,
		humanRole:     "k6",
		libp2pKeys:    nil,
	})
}

// ensureAnsibleJob drives one provisioning stream of the parallel
// ProvisioningInfra fan-in: it creates the role-filtered Ansible Job on first
// sight, then reports its terminal state via done/failed flags while stamping
// the per-stream Condition described by spec. It never calls setEnvPhase, so
// the caller owns the FSM transition once both streams settle.
//
// NB: the success TTL is NOT set here. While the sibling Job may still be
// running, ProvisioningInfra keeps re-reconciling every 10s; a short TTL would
// let the Job controller delete this finished Job mid-wait, the next call would
// hit NotFound and recreate it, re-running the playbook. The success TTL is
// applied once, after the fan-in settles, in reconcileProvisioningInfra.
func (r *EnvironmentReconciler) ensureAnsibleJob(ctx context.Context,
	env *dfaasv1.Environment, spec ansibleJobSpec) (done bool, failed bool, err error) {
	logger := log.FromContext(ctx)

	if !env.HasNodeWithRole(spec.role) {
		logger.Info("no nodes for role, skipping Ansible phase", "role", spec.humanRole)
		logStatusErr(ctx, "stamp "+spec.condType+"=True (skipped)", r.setEnvCondition(ctx, env, spec.condType,
			metav1.ConditionTrue, spec.skipReason, spec.skipMessage))
		return true, false, nil
	}

	jobName := ansible.JobNameForRole(env, spec.jobSuffix)
	var job batchv1.Job
	getErr := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: env.Namespace}, &job)

	if apierrors.IsNotFound(getErr) {
		// P7: before kicking off the Job, the observed state is "we haven't
		// checked yet" — stamp Unknown/JobPending. Replaced by False/
		// AnsibleRunning once the Job exists.
		logStatusErr(ctx, "stamp "+spec.condType+"=Unknown (job pending)", r.setEnvCondition(ctx, env, spec.condType,
			metav1.ConditionUnknown, dfaasv1.EnvReasonJobPending,
			spec.humanRole+" Ansible Job not yet created"))

		logger.Info("creating Ansible Job", "role", spec.humanRole, "job", jobName)
		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		newJob, secret, jerr := am.CreateJobForRole(ctx, env, spec.role, spec.jobSuffix, spec.libp2pKeys)
		if jerr != nil {
			// P2: surface CreateJobForRole failure.
			logStatusErr(ctx, "stamp "+spec.condType+"=False (build failed)", r.setEnvCondition(ctx, env, spec.condType,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"build "+spec.humanRole+" Ansible Job: "+condMessage(jerr)))
			return false, false, jerr
		}
		if cerr := r.Create(ctx, secret); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			// P2.
			logStatusErr(ctx, "stamp "+spec.condType+"=False (secret create failed)", r.setEnvCondition(ctx, env, spec.condType,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create "+spec.humanRole+" inventory Secret: "+condMessage(cerr)))
			return false, false, cerr
		}
		if cerr := r.Create(ctx, newJob); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			// P2.
			logStatusErr(ctx, "stamp "+spec.condType+"=False (job create failed)", r.setEnvCondition(ctx, env, spec.condType,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create "+spec.humanRole+" Ansible Job: "+condMessage(cerr)))
			return false, false, cerr
		}
		logStatusErr(ctx, "stamp "+spec.condType+"=False (job started)", r.setEnvCondition(ctx, env, spec.condType,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning,
			spec.humanRole+" Ansible Job started"))
		return false, false, nil
	}
	if getErr != nil {
		return false, false, getErr
	}

	if job.Status.Succeeded > 0 {
		logStatusErr(ctx, "stamp "+spec.condType+"=True (completed)", r.setEnvCondition(ctx, env, spec.condType,
			metav1.ConditionTrue, spec.succeedReason,
			spec.humanRole+" Ansible Job completed"))
		return true, false, nil
	}
	if job.Status.Failed > 0 {
		patchJobTTL(ctx, r.Client, &job, jobTTLFailureGraceSeconds)
		logStatusErr(ctx, "stamp "+spec.condType+"=False (job failed)", r.setEnvCondition(ctx, env, spec.condType,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleFailed,
			spec.humanRole+" Ansible Job failed; check logs"))
		return false, true, nil
	}
	logger.Info("Ansible Job still running",
		"role", spec.humanRole,
		"job", jobName,
		"active", job.Status.Active,
		"succeeded", job.Status.Succeeded,
		"failed", job.Status.Failed)
	logStatusErr(ctx, "stamp "+spec.condType+"=False (in progress)", r.setEnvCondition(ctx, env, spec.condType,
		metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning,
		spec.humanRole+" Ansible Job in progress"))
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
			logStatusErr(ctx, "stamp MonitoringReady=False (helm failed)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonHelmFailed,
				fmt.Sprintf("monitoring Helm install failed %d consecutive times: %s",
					count, condMessage(derr))))
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
		logStatusErr(ctx, "stamp MonitoringReady (helm installing)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
			condStatus, dfaasv1.EnvReasonHelmInstalling,
			"monitoring Helm install in progress / retrying: "+condMessage(derr)))
		return false, false, nil
	}
	if rerr := r.resetMonitoringAttempts(ctx, env); rerr != nil {
		logger.Error(rerr, "resetMonitoringAttempts failed; non-fatal")
	}
	ready, _ := mm.Check(ctx)
	if !ready {
		logStatusErr(ctx, "stamp MonitoringReady=False (waiting pods)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonWaitingPods,
			"monitoring pods not Ready yet"))
		return false, false, nil
	}

	logStatusErr(ctx, "stamp MonitoringReady=True", r.setEnvCondition(ctx, env, dfaasv1.EnvCondMonitoringReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonPodsRunning,
		"monitoring stack up"))
	logStatusErr(ctx, "reconcile Prometheus targets", mm.ReconcileTargets(ctx, env))
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
	return r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(env), func(latest *dfaasv1.Environment) error {
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
		return nil
	})
}

// atomicStatusUpdate runs mutate against the freshly-fetched Environment under
// retry.RetryOnConflict and persists it via Status().Update. It is the shared
// implementation behind the best-effort status-stamping helpers: it re-fetches
// to absorb informer cache lag, retries on conflict, and returns the final
// error so callers can decide whether to surface it. Fire-and-forget callers
// should route the result through logStatusErr rather than discarding it.
func (r *EnvironmentReconciler) atomicStatusUpdate(ctx context.Context,
	key client.ObjectKey, mutate func(env *dfaasv1.Environment) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		if err := mutate(latest); err != nil {
			return err
		}
		return r.Status().Update(ctx, latest)
	})
}

// logStatusErr logs a best-effort operation failure instead of silently
// dropping it. op is a short human-readable label for the operation that
// failed. Used at fire-and-forget sites (condition/status stamping, Prometheus
// target reconcile) where a failure is non-fatal but should still be visible
// in logs.
func logStatusErr(ctx context.Context, op string, err error) {
	if err != nil {
		log.FromContext(ctx).Error(err, "best-effort operation failed", "op", op)
	}
}

// setEnvCondition sets a Condition on Environment.status using the
// re-fetch-then-update pattern that absorbs informer cache lag.
func (r *EnvironmentReconciler) setEnvCondition(ctx context.Context,
	env *dfaasv1.Environment, condType string, status metav1.ConditionStatus,
	reason, message string) error {

	return r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(env), func(latest *dfaasv1.Environment) error {
		// Note: omit LastTransitionTime — meta.SetStatusCondition stamps it
		// only when status/reason/message actually changes. Letting the
		// helper set it preserves stability across no-op reconciles (P14).
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    condType,
			Status:  status,
			Reason:  reason,
			Message: message,
		})
		return nil
	})
}

// resetTransientConditions handles generation drift (P8): the spec was edited
// after a settled run, so every condition is reset to Unknown — none is
// trustworthy until the fresh provisioning pass re-stamps it. The Ready
// aggregator plus the five per-subsystem conditions all flip to
// Unknown/Updating so consumers stop trusting the previous values.
func (r *EnvironmentReconciler) resetTransientConditions(ctx context.Context,
	env *dfaasv1.Environment) error {
	for _, condType := range []string{
		dfaasv1.EnvCondReady,
		dfaasv1.EnvCondVMsReady,
		dfaasv1.EnvCondDfaasWorkersReady,
		dfaasv1.EnvCondK6Ready,
		dfaasv1.EnvCondInfrastructureReady,
		dfaasv1.EnvCondMonitoringReady,
	} {
		if err := r.setEnvCondition(ctx, env, condType,
			metav1.ConditionUnknown, dfaasv1.EnvReasonUpdating,
			"spec edited; re-provisioning — condition will be re-evaluated"); err != nil {
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
