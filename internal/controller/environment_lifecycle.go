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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// tolerated before the Environment is moved to EnvFailed (P4).
const monitoringRetryBudget = 5

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
			logger.Info("VMs not SSH-reachable after fast-retry budget; entering Unreachable (will keep retrying)",
				"nodes", unreachable, "attempts", count)
			logStatusErr(ctx, "stamp VMsReady=False (entering Unreachable)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonSSHUnreachable,
				fmt.Sprintf("SSH :22 dial failed for %v after %d fast attempts; retrying every %s",
					unreachable, count, unreachableRetryInterval)))
			return r.setEnvPhase(ctx, env, dfaasv1.EnvUnreachable)
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
		"provisioning SSH check passed: all declared nodes reachable on :22"))
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
			"dfaas-worker or k6 provisioning failed; inspect DFaaSNodesReady and K6Ready conditions"))
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
		// Monitoring (incl. the SeaweedFS S3 sink) is required: a terminally
		// broken monitoring stack fails the Environment rather than degrading it,
		// so the operator surfaces it loudly instead of leaving a half-usable env.
		// Failed is terminal — no auto-retry; recovery is a spec edit or
		// delete+recreate (see the EnvFailed case in environment_controller.go).
		return r.setEnvPhase(ctx, env, dfaasv1.EnvFailed)
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

// ensureMonitoring drives the Helm monitoring stack install. P4: after
// monitoringRetryBudget consecutive failures, returns failed=true so the
// caller transitions the Environment to EnvFailed. Resets the counter on
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
	var n int
	err := updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(env), &dfaasv1.Environment{},
		func(latest *dfaasv1.Environment) bool {
			n = bumpPlainCounter(latest, monitoringAttemptsAnnotation)
			return true
		})
	return n, err
}

// resetMonitoringAttempts zeroes the counter annotation on success. No-op
// when already "0" to avoid churn.
func (r *EnvironmentReconciler) resetMonitoringAttempts(ctx context.Context,
	env *dfaasv1.Environment) error {
	return updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(env), &dfaasv1.Environment{},
		func(latest *dfaasv1.Environment) bool {
			return resetPlainCounter(latest, monitoringAttemptsAnnotation)
		})
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

// atomicStatusUpdate runs mutate against the freshly-fetched Environment and
// persists it via Status().Update. Typed adapter over statusUpdateWithRetry: it
// re-fetches to absorb informer cache lag, retries on conflict, and returns the
// final error so callers can decide whether to surface it. Fire-and-forget
// callers should route the result through logStatusErr rather than discarding it.
func (r *EnvironmentReconciler) atomicStatusUpdate(ctx context.Context,
	key client.ObjectKey, mutate func(env *dfaasv1.Environment) error) error {
	return statusUpdateWithRetry(ctx, r.Client, key, &dfaasv1.Environment{}, mutate)
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
		dfaasv1.EnvCondDFaaSNodesReady,
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
