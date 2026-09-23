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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/statuswriter"
)

// Synchronized start (spec.syncStart) — the GO-signal barrier.
//
// Remote runners start normally (k6-operator's own starter unpauses them per
// cluster; that is NOT fought here). The generated script's setup() blocks
// polling the DFAAS_SYNC_URL every ~250ms, so a runner that is "started" is a
// runner parked on the barrier. Once EVERY TestRun reports stage "started",
// the reconciler publishes the GO object on the in-cluster SeaweedFS filer;
// all setups see it within one poll interval and the VUs start together.
//
// The filer is used because it is authless on both sides: the operator PUTs
// via the in-cluster Service DNS, the k6 VMs GET anonymously via the filer
// NodePort (30901) — no aws-sdk dependency, no bucket policy involved.

// syncWaitBudget caps how long the reconciler waits for all runners to reach
// stage "started" after the dispatch completes. Past it the whole test is
// marked Failed and its remote TestRuns deleted by the run end — a partially
// synchronized run is invalid experimental data.
const syncWaitBudget = 5 * time.Minute

// syncPollRequeue is the reconcile cadence while waiting on the barrier.
const syncPollRequeue = 3 * time.Second

// The filer addressing, the object layout and the HTTP calls all live in
// internal/syncchannel now; the reconciler reaches them through
// r.syncChannel(). What stays here is the barrier policy: how long to wait,
// how often to poll, and what to do when a runner never parks.

// awaitSyncBarrier is the post-dispatch gate for syncStart tests. It polls
// every remote TestRun until all report stage "started" (runner up and parked
// on the script barrier), then publishes the GO signal and finishes the
// dispatch (StartTime + phase Running). Failure policy per user decision:
// any TestRun in stage "error", or the syncWaitBudget expiring, fails the
// LoadTest; the run end (endRun) deletes every remote TestRun in that same
// pass, so an unreachable generator no longer blocks the Failed transition.
func (r *LoadTestReconciler) awaitSyncBarrier(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	total := len(lt.Status.TestRuns)
	started := 0
	for _, ref := range lt.Status.TestRuns {
		node, nerr := r.Dispatcher.Node(ctx, env, ref.NodeID)
		if nerr != nil {
			// A node that left the Environment will never report "started":
			// fail in one tick instead of burning the whole syncWaitBudget
			// waiting on a runner nobody can poll.
			logger.Info("sync barrier: node unusable; aborting all",
				"node", ref.NodeID, "cause", nerr.Error())
			return r.failLoadTest(ctx, lt, "synchronized start: "+nerr.Error(), statuswriter.Cond{
				Type: dfaasv1.LTCondSyncReady, Status: metav1.ConditionFalse,
				Reason: dfaasv1.LTReasonSyncTimeout, Message: nerr.Error()})
		}
		stage, err := node.Stage(ctx, lt)
		if err != nil {
			// Transient remote hiccup (or not yet visible): keep waiting, the
			// budget bounds us.
			logger.Error(err, "sync barrier: remote TestRun poll failed", "node", ref.NodeID)
			continue
		}
		switch stage {
		case "started", "finished", "stopped":
			// finished/stopped should not happen while parked on the barrier,
			// but count them as past-the-start so the GO still fires.
			started++
		case "error":
			logger.Info("sync barrier: TestRun errored while waiting; aborting all",
				"node", ref.NodeID)
			return r.failLoadTest(ctx, lt,
				fmt.Sprintf("synchronized start: runner on node %q errored before the GO signal", ref.NodeID),
				statuswriter.Cond{Type: dfaasv1.LTCondSyncReady, Status: metav1.ConditionFalse,
					Reason:  dfaasv1.LTReasonSyncTimeout,
					Message: fmt.Sprintf("runner on node %q reported stage=error before the GO signal", ref.NodeID)})
		}
	}

	if started < total {
		// Budget reference: when the dispatch completed, i.e. K6Dispatched
		// flipped to True. Re-fetch to see the freshest conditions.
		var latest dfaasv1.LoadTest
		if err := r.Get(ctx, types.NamespacedName{Name: lt.Name, Namespace: lt.Namespace}, &latest); err == nil {
			if cond := meta.FindStatusCondition(latest.Status.Conditions, dfaasv1.LTCondK6Dispatched); cond != nil &&
				cond.Status == metav1.ConditionTrue &&
				time.Since(cond.LastTransitionTime.Time) > syncWaitBudget {
				logger.Info("sync barrier: wait budget exhausted; aborting all",
					"started", started, "total", total)
				return r.failLoadTest(ctx, lt,
					fmt.Sprintf("synchronized start: only %d/%d runners started within %s", started, total, syncWaitBudget),
					statuswriter.Cond{Type: dfaasv1.LTCondSyncReady, Status: metav1.ConditionFalse,
						Reason:  dfaasv1.LTReasonSyncTimeout,
						Message: fmt.Sprintf("only %d/%d runners started within %s", started, total, syncWaitBudget)})
			}
		}
		r.cond(ctx, lt, dfaasv1.LTCondSyncReady,
			metav1.ConditionFalse, dfaasv1.LTReasonAwaitingRunners,
			fmt.Sprintf("%d/%d runners started, holding the GO signal", started, total))
		return ctrl.Result{RequeueAfter: syncPollRequeue}, nil
	}

	if err := r.syncChannel().PublishGo(ctx, lt); err != nil {
		// Filer hiccup: retry on the poll cadence; the wait budget above still
		// bounds the total time spent here.
		logger.Error(err, "sync barrier: GO signal publish failed; retrying")
		return ctrl.Result{RequeueAfter: syncPollRequeue}, nil
	}
	logger.Info("sync barrier: GO signal published", "runners", total)
	r.cond(ctx, lt, dfaasv1.LTCondSyncReady,
		metav1.ConditionTrue, dfaasv1.LTReasonGoPublished,
		fmt.Sprintf("GO signal published; %d runners released together", total))
	return r.finishDispatch(ctx, lt, lt.Status.TestRuns)
}
