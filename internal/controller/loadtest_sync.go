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

	// GO already out (a finishDispatch that failed after the publish): finish,
	// never publish twice.
	if c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondSyncReady); c != nil &&
		c.Status == metav1.ConditionTrue && c.Reason == dfaasv1.LTReasonGoPublished {
		return r.finishDispatch(ctx, lt, lt.Status.TestRuns)
	}

	total := len(lt.Status.TestRuns)
	nodeIDs := make([]string, 0, total)
	for _, ref := range lt.Status.TestRuns {
		nodeIDs = append(nodeIDs, ref.NodeID)
	}
	rd := r.survey(ctx, lt, env, nodeIDs)
	fail := func(ready, detail string) (ctrl.Result, error) {
		logger.Info("sync barrier: aborting all", "cause", detail)
		return r.failLoadTest(ctx, lt, "synchronized start: "+ready, statuswriter.Cond{
			Type: dfaasv1.LTCondSyncReady, Status: metav1.ConditionFalse,
			Reason: dfaasv1.LTReasonSyncTimeout, Message: detail})
	}

	// A generator that left the Environment will never report "started": fail
	// in one tick instead of burning the whole syncWaitBudget.
	if len(rd.unusable) > 0 {
		d := "generator unusable: " + rd.describe(rd.unusable)
		return fail(d, d)
	}
	for _, id := range nodeIDs {
		switch classifyStage(rd.stages[id]) {
		case stageErrored:
			return fail(fmt.Sprintf("runner on node %q errored before the GO signal", id),
				fmt.Sprintf("runner on node %q reported stage=error before the GO signal", id))
		case stageDone:
			// A runner past its script before GO ran unsynchronized: the data
			// is not the experiment that was asked for.
			return fail(fmt.Sprintf("runner on node %q finished before the GO signal", id),
				fmt.Sprintf("runner on node %q reported stage=%s before the GO signal", id, rd.stages[id]))
		}
	}

	// One wait, one bound: syncWaitBudget from K6Dispatched=True bounds both
	// the runners parking and the GO publish.
	expired := false
	if c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondK6Dispatched); c != nil &&
		c.Status == metav1.ConditionTrue && time.Since(c.LastTransitionTime.Time) > syncWaitBudget {
		expired = true
	}
	started := rd.count[stageStarted]

	if started < total {
		if expired {
			return fail(fmt.Sprintf("only %d/%d runners started within %s", started, total, syncWaitBudget),
				fmt.Sprintf("only %d/%d runners started within %s", started, total, syncWaitBudget))
		}
		msg := fmt.Sprintf("%d/%d runners started, holding the GO signal", started, total)
		if len(rd.missing) > 0 {
			msg += "; no status from " + rd.describe(rd.missing)
		}
		r.cond(ctx, lt, dfaasv1.LTCondSyncReady, metav1.ConditionFalse, dfaasv1.LTReasonAwaitingRunners, msg)
		return ctrl.Result{RequeueAfter: syncPollRequeue}, nil
	}

	if err := r.syncChannel().PublishGo(ctx, lt); err != nil {
		logger.Error(err, "sync barrier: GO signal publish failed")
		if expired {
			return fail("GO signal publish failed within "+syncWaitBudget.String(),
				"every runner started, but the GO signal could not be published within "+
					syncWaitBudget.String()+": "+condMessage(err))
		}
		r.cond(ctx, lt, dfaasv1.LTCondSyncReady, metav1.ConditionFalse, dfaasv1.LTReasonAwaitingRunners,
			fmt.Sprintf("%d/%d runners started; GO signal publish failed, retrying: %s", started, total, condMessage(err)))
		return ctrl.Result{RequeueAfter: syncPollRequeue}, nil
	}
	logger.Info("sync barrier: GO signal published", "runners", total)
	r.cond(ctx, lt, dfaasv1.LTCondSyncReady,
		metav1.ConditionTrue, dfaasv1.LTReasonGoPublished,
		fmt.Sprintf("GO signal published; %d runners released together", total))
	return r.finishDispatch(ctx, lt, lt.Status.TestRuns)
}
