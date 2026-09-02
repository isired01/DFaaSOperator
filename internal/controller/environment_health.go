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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
)

// sshProbeTimeout caps each per-host SSH-reachability TCP dial (P6).
const sshProbeTimeout = 2 * time.Second

// sshAttemptsAnnotation persists the consecutive SSH-unreachable counter,
// generation-scoped as "<generation>:<count>" so a spec edit restarts the
// budget fresh.
const sshAttemptsAnnotation = "dfaas.dfaas.io/ssh-attempts"

// sshRetryBudget is the max consecutive SSH-unreachable rounds tolerated
// during provisioning before the Environment enters the non-terminal
// EnvUnreachable phase (which re-probes indefinitely and auto-recovers).
const sshRetryBudget = 3

// healthCheckInterval is the cadence of the Ready-state SSH liveness probe.
const healthCheckInterval = time.Minute

// healthRetryInterval is the faster cadence used to confirm a suspected miss
// before giving up.
const healthRetryInterval = 20 * time.Second

// healthRetryBudget is the max consecutive unreachable health rounds tolerated
// before a Ready Environment enters the non-terminal EnvUnreachable phase. A
// small budget prevents a single dropped packet from flapping a healthy env.
const healthRetryBudget = 3

// healthMissesAnnotation persists the consecutive Ready-state unreachable
// counter, generation-scoped as "<generation>:<count>". Kept separate from
// sshAttemptsAnnotation so the health loop never clobbers provisioning state.
const healthMissesAnnotation = "dfaas.dfaas.io/health-misses"

// unreachableRetryInterval is the cadence of the indefinite SSH re-probe run
// while an Environment is in the non-terminal Unreachable phase.
const unreachableRetryInterval = 30 * time.Second

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
// moved to the non-terminal EnvUnreachable phase, which re-probes indefinitely
// and auto-recovers to Ready (or resumes provisioning) once the nodes answer
// again — no manual intervention needed for transient connectivity loss.
func (r *EnvironmentReconciler) reconcileReadyHealth(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(env.Spec.Nodes) == 0 {
		return ctrl.Result{RequeueAfter: healthCheckInterval}, nil
	}

	// Throttle against our own status writes.
	due := healthCheckInterval
	if c := meta.FindStatusCondition(env.Status.Conditions, dfaasv1.EnvCondNodesReachable); c != nil &&
		c.Status == metav1.ConditionFalse {
		due = healthRetryInterval
	}
	if wait := r.probeThrottle(ctx, env, due); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
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
		logger.Info("Ready environment nodes unreachable; entering Unreachable (will keep retrying)",
			"nodes", unreachable, "attempts", count)
		logStatusErr(ctx, "mark NodesReachable=False (entering Unreachable)", r.markNodesUnreachable(ctx, env,
			fmt.Sprintf("SSH :22 dial failed for %v after %d consecutive health checks; retrying every %s",
				unreachable, count, unreachableRetryInterval)))
		return r.setEnvPhase(ctx, env, dfaasv1.EnvUnreachable)
	}
	logger.Info("Ready environment nodes unreachable; will retry",
		"nodes", unreachable, "attempts", count)
	logStatusErr(ctx, "mark NodesReachable=False (retrying)", r.markNodesUnreachable(ctx, env,
		fmt.Sprintf("SSH :22 dial failed for %v (attempt %d/%d)", unreachable, count, healthRetryBudget)))
	return ctrl.Result{RequeueAfter: healthRetryInterval}, nil
}

// reconcileUnreachable drives the non-terminal Unreachable phase. An Environment
// whose nodes stopped answering SSH — during provisioning (after the fast
// sshRetryBudget) or while Ready (after healthRetryBudget) — lands here instead
// of terminal Failed and is re-probed indefinitely every unreachableRetryInterval.
// Once every node answers :22 again it auto-recovers: straight back to Ready when
// the infrastructure was already provisioned and the spec has not drifted since,
// otherwise forward into ProvisioningInfra (the nodes are reachable now, so the
// VMs step has nothing left to do). It never transitions to Failed on its own;
// recovery is automatic, via spec edit, or via delete.
//
// Throttled on status.lastHealthCheck exactly like reconcileReadyHealth so its
// own status writes don't hot-loop the reconciler.
func (r *EnvironmentReconciler) reconcileUnreachable(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if wait := r.probeThrottle(ctx, env, unreachableRetryInterval); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	var unreachable []string
	for _, n := range env.Spec.Nodes {
		if !probeSSH(n.IPAddress) {
			unreachable = append(unreachable, n.NodeID)
		}
	}

	if len(unreachable) > 0 {
		logger.Info("environment still unreachable; will keep retrying",
			"nodes", unreachable, "interval", unreachableRetryInterval)
		logStatusErr(ctx, "mark NodesReachable=False (unreachable)", r.markNodesUnreachable(ctx, env,
			fmt.Sprintf("SSH :22 dial failed for %v; retrying every %s", unreachable, unreachableRetryInterval)))
		return ctrl.Result{RequeueAfter: unreachableRetryInterval}, nil
	}

	// All nodes answer again. Clear both unreachable counters (no-op when zero)
	// and pick the recovery target.
	if rerr := r.resetHealthMisses(ctx, env); rerr != nil {
		logger.Error(rerr, "resetHealthMisses failed; non-fatal")
	}
	if rerr := r.resetSSHAttempts(ctx, env); rerr != nil {
		logger.Error(rerr, "resetSSHAttempts failed; non-fatal")
	}

	// Recover to Ready only when the infra is already up AND the spec hasn't
	// drifted since (observedGeneration current). Otherwise resume provisioning:
	// either we never finished it, or the spec was edited during the outage and
	// must be re-applied.
	wasReady := meta.IsStatusConditionTrue(env.Status.Conditions, dfaasv1.EnvCondInfrastructureReady) &&
		env.Status.ObservedGeneration == env.Generation
	if wasReady {
		logger.Info("environment reachable again; returning to Ready")
		logStatusErr(ctx, "mark NodesReachable=True (recovered)", r.markNodesReachable(ctx, env))
		return r.setEnvPhase(ctx, env, dfaasv1.EnvReady)
	}
	logger.Info("environment reachable again; resuming provisioning")
	logStatusErr(ctx, "stamp VMsReady=True (recovered)", r.setEnvCondition(ctx, env, dfaasv1.EnvCondVMsReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonSSHReachable,
		"nodes reachable again on :22; resuming provisioning"))
	return r.setEnvPhase(ctx, env, dfaasv1.EnvProvisioningInfra)
}

// markNodesReachable stamps NodesReachable=True and refreshes lastHealthCheck
// in a single status update (one write per healthy round → one re-enqueue,
// caught by the throttle).
// probeThrottle reports how long to wait before the next SSH probe, 0 when one
// is due now.
//
// It checks the cached status first and, only when that says "go", confirms
// against a LIVE read. The informer cache lags this controller's own status
// writes by a few milliseconds, so a reconcile triggered by our own write would
// see the previous lastHealthCheck, judge the interval elapsed, and probe again
// immediately. Measured on a real node outage: the miss counter went 1 → 3 in
// 22 s with lastHealthCheck advancing only 2 s between the last two probes, so
// healthRetryBudget stopped buying any tolerance for a transient blip — which
// is the entire reason the budget exists.
//
// Nil APIReader (unit tests) or a failed read falls back to the cached decision:
// the throttle is an optimisation, never a correctness gate.
func (r *EnvironmentReconciler) probeThrottle(ctx context.Context,
	env *dfaasv1.Environment, due time.Duration) time.Duration {

	remaining := func(last *metav1.Time) time.Duration {
		if last == nil {
			return 0
		}
		if elapsed := time.Since(last.Time); elapsed < due {
			return due - elapsed
		}
		return 0
	}

	if wait := remaining(env.Status.LastHealthCheck); wait > 0 {
		return wait
	}
	if r.APIReader == nil {
		return 0
	}
	var fresh dfaasv1.Environment
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(env), &fresh); err != nil {
		return 0
	}
	return remaining(fresh.Status.LastHealthCheck)
}

func (r *EnvironmentReconciler) markNodesReachable(ctx context.Context,
	env *dfaasv1.Environment) error {
	return r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(env), func(latest *dfaasv1.Environment) error {
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    dfaasv1.EnvCondNodesReachable,
			Status:  metav1.ConditionTrue,
			Reason:  dfaasv1.EnvReasonSSHReachable,
			Message: "live SSH liveness OK: all declared nodes reachable on :22",
		})
		// Kept in step with NodesReachable: VMsReady is written by provisioning
		// and was then left untouched by the health loop, so during a Ready-state
		// outage the conditions panel showed a green "VMsReady — nodes reachable
		// again on :22" directly above a red "NodesReachable — g4 unreachable".
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    dfaasv1.EnvCondVMsReady,
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
		// See markNodesReachable: the two must not disagree. A node that stopped
		// answering :22 is exactly what VMsReady=False means.
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    dfaasv1.EnvCondVMsReady,
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
	var n int
	err := updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(env), &dfaasv1.Environment{},
		func(latest *dfaasv1.Environment) bool {
			n = bumpGenCounter(latest, healthMissesAnnotation)
			return true
		})
	return n, err
}

// resetHealthMisses zeroes the health-miss counter for the current generation.
// No-op when already zero to avoid annotation churn (and a spurious re-enqueue).
func (r *EnvironmentReconciler) resetHealthMisses(ctx context.Context,
	env *dfaasv1.Environment) error {
	return updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(env), &dfaasv1.Environment{},
		func(latest *dfaasv1.Environment) bool {
			return resetGenCounter(latest, healthMissesAnnotation)
		})
}

// bumpSSHAttempts increments the consecutive SSH-unreachable counter,
// generation-scoped: a stored generation different from the current one
// (spec edit) restarts the budget at 1. Returns the count for this generation.
func (r *EnvironmentReconciler) bumpSSHAttempts(ctx context.Context,
	env *dfaasv1.Environment) (int, error) {
	var n int
	err := updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(env), &dfaasv1.Environment{},
		func(latest *dfaasv1.Environment) bool {
			n = bumpGenCounter(latest, sshAttemptsAnnotation)
			return true
		})
	return n, err
}

// resetSSHAttempts zeroes the counter for the current generation on success.
// No-op when already absent or zero to avoid churn.
func (r *EnvironmentReconciler) resetSSHAttempts(ctx context.Context,
	env *dfaasv1.Environment) error {
	return updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(env), &dfaasv1.Environment{},
		func(latest *dfaasv1.Environment) bool {
			return resetGenCounter(latest, sshAttemptsAnnotation)
		})
}
