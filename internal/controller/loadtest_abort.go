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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
)

// abortLoadTest performs the multi-cluster cascading abort: best-effort
// deletes every remote TestRun for lt across all k6-load-generator nodes
// listed in env.Status.K6Nodes, then marks the central CR terminal as
// Aborted with Conditions[Ready]=False reason=UserAborted.
//
// Target set = union of:
//   - lt.Status.TestRuns (authoritative for what was successfully dispatched)
//   - spec.PerNodeLoad with deterministic names (catches TestRuns that exist
//     remotely but were never stamped into status — e.g. startK6 errored
//     mid-loop before reaching the status update).
//
// Per-node errors are logged but do NOT abort the loop — every node is
// attempted on each tick. If any node errored we Requeue after 10s and
// retry the full set (idempotent via DeleteTestRun's IgnoreNotFound).
// Once ALL targets dispatch cleanly, phase + Condition are stamped.
func (r *LoadTestReconciler) abortLoadTest(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment,
	reason, message string) (ctrl.Result, error) {

	if failed := r.teardownRemoteTestRuns(ctx, lt, env); failed > 0 {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// The GO signal (syncStart) is moot once the test is aborted — hygiene.
	if lt.Spec.SyncStart {
		logStatusErr(ctx, "delete GO signal (abort)", deleteGoSignal(ctx, lt))
	}
	// Any k6 summaries already uploaded will never be consumed — sweep them.
	// Unconditional: DFAAS_SUMMARY_URL is injected regardless of syncStart.
	logStatusErr(ctx, "delete k6 summaries (abort)", deleteSummaryObjects(ctx, lt))

	// K6Healthy still carries the last count observed while the test was
	// running ("0/2 finished, 0 error, 2 running"). The teardown above just
	// deleted those TestRuns, so leaving it alone makes the conditions panel
	// claim runners are live on a test that has none. Restamp it to what is
	// now true.
	logStatusErr(ctx, "stamp K6Healthy=False (runners reclaimed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
		metav1.ConditionFalse, dfaasv1.LTReasonRunnersReclaimed,
		fmt.Sprintf("%d remote TestRun(s) deleted — test was aborted before completion", len(lt.Spec.PerNodeLoad))))

	// MetricsExported never ran on abort — stamp False/Skipped per P9 so
	// UI does not show "in flight" forever on the aborted CR.
	logStatusErr(ctx, "stamp MetricsExported=False (export skipped)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
		metav1.ConditionFalse, dfaasv1.LTReasonExportSkipped,
		"no exporter ran — test was aborted"))
	// Carry the caller's reason + message through the phase transition: the
	// aggregator would otherwise replace them with the generic "load test
	// aborted", and the UI looks the abort explanation up by
	// Ready/UserAborted specifically.
	return r.setLoadTestPhaseDetail(ctx, lt, dfaasv1.LoadTestAborted, reason, message)
}

// teardownRemoteTestRuns issues DeleteTestRun for every remote TestRun of lt
// (union of status.testRuns and deterministic names from spec.perNodeLoad —
// catches partial-dispatch races) and returns how many deletes failed.
// Per-node errors are logged but do not stop the sweep; callers requeue and
// retry the full set while failed > 0 (idempotent via IgnoreNotFound).
// Shared by the abort path and the syncStart barrier failure path.
func (r *LoadTestReconciler) teardownRemoteTestRuns(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (failed int) {
	logger := log.FromContext(ctx)

	// Union of status.testRuns and spec.perNodeLoad catches partial-dispatch
	// races. Remote names are deterministic per node, so the union is a set
	// of nodeIDs.
	nodeIDs := map[string]struct{}{}
	for _, ref := range lt.Status.TestRuns {
		nodeIDs[ref.NodeID] = struct{}{}
	}
	for _, perNode := range lt.Spec.PerNodeLoad {
		nodeIDs[perNode.NodeID] = struct{}{}
	}

	for nodeID := range nodeIDs {
		node, nerr := r.Dispatcher.Node(ctx, env, nodeID)
		if nerr != nil {
			// Unreachable by definition: no kubeconfig, no remote API to delete
			// through. Counting it as a failure would requeue the abort forever.
			logger.Error(nerr, "skipping remote TestRun delete", "node", nodeID)
			continue
		}
		if err := node.Delete(ctx, lt); err != nil {
			logger.Error(err, "remote TestRun delete failed", "node", nodeID)
			failed++
			continue
		}
		logger.Info("remote TestRun aborted", "node", nodeID)
	}
	return failed
}

// handleLoadTestDeletion is the finalizer flow triggered by a non-zero
// DeletionTimestamp. It guarantees remote TestRun cleanup before allowing
// K8s GC to remove the central CR — closes the "kubectl delete loadtest
// orphans remote runs" gap.
//
// Steps:
//  1. If our finalizer is absent, nothing to do.
//  2. Fetch the parent Environment. NotFound → the remote kubeconfig Secrets
//     went with it (ownerReference stamped by the k6 playbook, plus the
//     Environment finalizer's pruneK6Kubeconfigs sweep for older ones), so
//     there is no way to delete remote TestRuns. Just remove the finalizer
//     and let the CR go.
//  3. Drive abortLoadTest (reason=UserAborted) to issue DeleteTestRun
//     against every node in spec ∪ status. abortLoadTest itself transitions
//     phase to Aborted on the first clean sweep; we don't care about that
//     terminal state here — we care that DeleteTestRun was issued.
//  4. Poll: GetTestRun against every entry in spec ∪ status. When all
//     report NotFound, remove the finalizer and return. Until then,
//     RequeueAfter 3s.
func (r *LoadTestReconciler) handleLoadTestDeletion(ctx context.Context,
	lt *dfaasv1.LoadTest) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(lt, loadTestFinalizer) {
		return ctrl.Result{}, nil
	}

	// Look up the parent Environment (same pattern as Reconcile body).
	var env dfaasv1.Environment
	envKey := types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}
	err := r.Get(ctx, envKey, &env)
	if apierrors.IsNotFound(err) {
		// Environment already gone, and with it the kubeconfig Secrets: the
		// k6 playbook stamps an Environment ownerReference on each one, and
		// the Environment finalizer drains any that predate that change
		// (pruneK6Kubeconfigs). No credentials left to reach the remote k3s,
		// so best-effort is done; release the CR.
		logger.Info("environment gone during loadtest deletion — releasing finalizer",
			"env", lt.Spec.TargetEnvironment)
		return ctrl.Result{}, r.removeLTFinalizer(ctx, lt)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// Issue remote DeleteTestRun across the target set. abortLoadTest is
	// idempotent: DeleteTestRun uses IgnoreNotFound under the hood, and the
	// helper transitions phase → Aborted as a side effect. We ignore the
	// returned Result and run our own NotFound poll below.
	//
	// Skip once phase is already Aborted: abortLoadTest reaches that phase
	// only after a clean teardown pass, and re-running it every poll cycle
	// re-stamps Ready (UserAborted → Aborted flip-flop) — each pass bumped
	// resourceVersion after our Get, so the finalizer Update below hit a
	// guaranteed 409 and deletion livelocked.
	if lt.Status.Phase != dfaasv1.LoadTestAborted {
		if _, abortErr := r.abortLoadTest(ctx, lt, &env, dfaasv1.LTReasonUserAborted,
			"LoadTest aborted on user delete"); abortErr != nil {
			logger.Error(abortErr, "abortLoadTest during deletion failed; will retry")
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
	}

	// Poll: every TestRun in spec ∪ status must report NotFound on the
	// remote cluster before we drop the finalizer. Mirrors the abort target
	// set construction so a partial-dispatch ride-along is also covered.
	nodeIDs := map[string]struct{}{}
	for _, ref := range lt.Status.TestRuns {
		nodeIDs[ref.NodeID] = struct{}{}
	}
	for _, perNode := range lt.Spec.PerNodeLoad {
		nodeIDs[perNode.NodeID] = struct{}{}
	}
	for nodeID := range nodeIDs {
		node, nerr := r.Dispatcher.Node(ctx, &env, nodeID)
		if nerr != nil {
			// Nothing to poll on a node we cannot reach; it cannot hold up release.
			continue
		}
		_, err := node.Stage(ctx, lt)
		if err == nil {
			// Still present — keep polling.
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		if !errors.Is(err, k6dispatch.ErrNotFound) {
			// Transient (kubeconfig parse, network, etc). Keep polling rather
			// than wedge the CR; the finalizer guarantees we revisit.
			logger.Error(err, "remote TestRun NotFound-poll errored", "node", nodeID)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
	}

	// Every target NotFound on its remote — safe to release.
	if lt.Spec.SyncStart {
		logStatusErr(ctx, "delete GO signal (loadtest deletion)", deleteGoSignal(ctx, lt))
	}
	// Finalizer path is the catch-all sweep for k6 summaries: covers every
	// exit that skipped cleanup (crash, Failed before the exporter ran, …).
	logStatusErr(ctx, "delete k6 summaries (loadtest deletion)", deleteSummaryObjects(ctx, lt))
	logger.Info("all remote TestRuns reclaimed — removing loadtest finalizer")
	return ctrl.Result{}, r.removeLTFinalizer(ctx, lt)
}

// removeLTFinalizer drops the loadtest finalizer with the re-fetch-then-update
// pattern: the deletion path writes status (phase/conditions) between the
// reconcile's Get and this Update, so a plain r.Update on the stale object
// 409s deterministically. RemoveFinalizer's bool doubles as the write flag.
func (r *LoadTestReconciler) removeLTFinalizer(ctx context.Context, lt *dfaasv1.LoadTest) error {
	return updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(lt), &dfaasv1.LoadTest{},
		func(latest *dfaasv1.LoadTest) bool {
			return controllerutil.RemoveFinalizer(latest, loadTestFinalizer)
		})
}
