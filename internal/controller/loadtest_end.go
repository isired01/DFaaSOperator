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
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/statuswriter"
	"dfaas-operator/internal/k6dispatch"
)

// Run end: the one place a LoadTest reaches Completed, Failed or Aborted.
//
// Every exit used to write its terminal phase on its own, and only the Sync
// barrier tore the remote TestRuns down first. A Failed test could therefore
// leave runners loading the DFaaS nodes while Occupancy freed the Environment
// for the next queued test. endRun owns that rule for every outcome: which
// runners may still be live, which generators to sweep, what the end-of-run
// Conditions say, and that all of it is one status write.
//
// The teardown is one bounded pass: the terminal phase is written in the same
// pass whatever the remote answers, so nothing can wedge a run end. A Delete
// that failed on a generator holding a TestRun this run applied stamps
// K6Healthy=RunnersUnreclaimed; Occupancy keeps the Environment busy on it and
// the terminal short-circuit retries the sweep (retryReclaim) until it
// succeeds or the user deletes the test.

// reclaimRetryInterval paces the background re-sweep of a terminal test whose
// runner could not be deleted.
const reclaimRetryInterval = 30 * time.Second

// deletionReclaimBudget bounds how long the deletion finalizer waits for a
// generator that cannot confirm its TestRun gone, measured from the
// immutable DeletionTimestamp.
const deletionReclaimBudget = 2 * time.Minute

// endRun ends the run with phase p, the Ready reason/message, and the
// caller's diagnostic Conditions, in one status write. It reads the LoadTest
// fresh (the caller's copy may predate TestRuns persisted earlier in the same
// pass) and is a no-op on a test that is already terminal or gone.
func (r *LoadTestReconciler) endRun(ctx context.Context, lt *dfaasv1.LoadTest,
	p dfaasv1.LoadTestPhase, reason, message string, conds ...statuswriter.Cond) (ctrl.Result, error) {

	var fresh dfaasv1.LoadTest
	if err := r.occupancyReader().Get(ctx, client.ObjectKeyFromObject(lt), &fresh); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if fresh.Status.Phase.Terminal() {
		return ctrl.Result{}, nil
	}
	endReason := reason
	if endReason == "" {
		endReason = dfaasv1.LTReasonFailed
	}

	var own []statuswriter.Cond
	if runnersMayBeLive(&fresh) {
		rec, err := r.reclaimRunners(ctx, &fresh)
		if err != nil {
			return ctrl.Result{}, err
		}
		own = append(own, rec.verdict(endReason))
	}
	if dispatchBegan(&fresh) {
		r.sweepFilerObjects(ctx, &fresh, "run end")
	}
	own = append(own, endRestamps(&fresh, p, endReason)...)

	passed := map[string]bool{}
	for _, c := range conds {
		passed[c.Type] = true
	}
	all := append([]statuswriter.Cond(nil), conds...)
	for _, c := range own {
		// The teardown verdict is the truth about the runners, so it wins over
		// a caller's K6Healthy; the restamps only fill types nobody passed.
		if c.Type == dfaasv1.LTCondK6Healthy || !passed[c.Type] {
			all = append(all, c)
		}
	}
	return r.phase(ctx, lt, p, reason, message, all...)
}

// dispatchBegan reports whether any remote work may exist for lt: a TestRun
// was persisted, or startK6 stamped its K6Dispatched marker before the first
// remote call.
func dispatchBegan(lt *dfaasv1.LoadTest) bool {
	return len(lt.Status.TestRuns) > 0 ||
		meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondK6Dispatched) != nil
}

// runnersMayBeLive reports whether lt may still have k6 runners generating
// load. Runners already observed done are not deleted, so their remote logs
// survive AllFailed, PartialFailure and every Exporting exit.
func runnersMayBeLive(lt *dfaasv1.LoadTest) bool {
	if !dispatchBegan(lt) || lt.Status.Phase == dfaasv1.LoadTestExporting {
		return false
	}
	done := map[string]bool{}
	for _, ref := range lt.Status.TestRuns {
		if b := classifyStage(ref.Phase); b == stageDone || b == stageErrored {
			done[ref.NodeID] = true
		}
	}
	for _, pn := range lt.Spec.PerNodeLoad {
		if !done[pn.NodeID] {
			return true
		}
	}
	return false
}

// runnersUnreclaimed reports a terminal test whose run end could not delete a
// runner it applied. Occupancy holds the Environment on it.
func runnersUnreclaimed(lt *dfaasv1.LoadTest) bool {
	if !lt.Status.Phase.Terminal() {
		return false
	}
	c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondK6Healthy)
	return c != nil && c.Reason == dfaasv1.LTReasonRunnersUnreclaimed
}

// reclaim is what one teardown pass found, per generator.
type reclaim struct {
	absent      []string
	held        []string // Delete failed on a generator holding a TestRun this run applied
	unconfirmed []string // unusable generator, Environment gone, or Delete failed with no TestRun applied
	causes      map[string]string
}

func (rc *reclaim) fail(held bool, nodeID, cause string) {
	if held {
		rc.held = append(rc.held, nodeID)
	} else {
		rc.unconfirmed = append(rc.unconfirmed, nodeID)
	}
	rc.causes[nodeID] = cause
}

func (rc *reclaim) describe(nodeIDs []string) string {
	parts := make([]string, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		parts = append(parts, fmt.Sprintf("%s (%s)", id, rc.causes[id]))
	}
	return strings.Join(parts, ", ")
}

// verdict is the K6Healthy Condition a teardown pass earns: RunnersUnreclaimed
// when an applied runner could not be deleted, RunnersReclaimed when every
// target is confirmed absent, otherwise the run's own reason with what could
// not be confirmed.
func (rc *reclaim) verdict(endReason string) statuswriter.Cond {
	c := statuswriter.Cond{Type: dfaasv1.LTCondK6Healthy, Status: metav1.ConditionFalse}
	switch {
	case len(rc.held) > 0:
		c.Reason = dfaasv1.LTReasonRunnersUnreclaimed
		c.Message = fmt.Sprintf("could not delete the TestRun on %s: its runner may still be sending load; "+
			"the operator retries every %s, and deleting this test stops the wait",
			rc.describe(rc.held), reclaimRetryInterval)
	case len(rc.unconfirmed) > 0:
		c.Reason = endReason
		c.Message = fmt.Sprintf("TestRun deleted on [%s]; could not confirm on %s",
			strings.Join(rc.absent, ", "), rc.describe(rc.unconfirmed))
	default:
		c.Reason = dfaasv1.LTReasonRunnersReclaimed
		c.Message = fmt.Sprintf("no TestRun of this test is left on %s", strings.Join(rc.absent, ", "))
	}
	return c
}

// reclaimRunners resolves the Environment and runs one teardown pass. With the
// Environment gone every target is unconfirmed: its kubeconfig Secrets went
// with it.
func (r *LoadTestReconciler) reclaimRunners(ctx context.Context, lt *dfaasv1.LoadTest) (*reclaim, error) {
	var env dfaasv1.Environment
	err := r.Get(ctx, types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}, &env)
	if apierrors.IsNotFound(err) {
		rec := &reclaim{causes: map[string]string{}}
		for _, id := range k6dispatch.TargetNodeIDs(lt) {
			rec.fail(false, id, fmt.Sprintf("environment %q not found", lt.Spec.TargetEnvironment))
		}
		return rec, nil
	}
	if err != nil {
		return nil, err
	}
	return r.teardownRemoteTestRuns(ctx, lt, &env), nil
}

// teardownRemoteTestRuns issues one Delete per target generator (the union of
// status.testRuns and spec.perNodeLoad, which catches partial-dispatch races)
// and sorts every target into absent, held or unconfirmed.
// ponytail: an unusable generator (no kubeconfig) is named, never held --
// nothing can reach it, so no retry could release the hold; a runner on a
// generator removed from the Environment keeps loading until its script ends.
// An Apply that landed while its TestRunRef failed to persist is not held.
func (r *LoadTestReconciler) teardownRemoteTestRuns(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) *reclaim {
	logger := log.FromContext(ctx)
	applied := map[string]bool{}
	for _, ref := range lt.Status.TestRuns {
		applied[ref.NodeID] = true
	}
	rec := &reclaim{causes: map[string]string{}}
	for _, nodeID := range k6dispatch.TargetNodeIDs(lt) {
		node, nerr := r.Dispatcher.Node(ctx, env, nodeID)
		if nerr != nil {
			logger.Error(nerr, "cannot reach generator to delete its TestRun", "node", nodeID)
			rec.fail(false, nodeID, condMessage(nerr))
			continue
		}
		if err := node.Delete(ctx, lt); err != nil {
			logger.Error(err, "remote TestRun delete failed", "node", nodeID)
			rec.fail(applied[nodeID], nodeID, condMessage(err))
			continue
		}
		rec.absent = append(rec.absent, nodeID)
	}
	sort.Strings(rec.absent)
	return rec
}

// sweepFilerObjects deletes, best-effort, what the run left on the filer: the
// GO object (syncStart) and any uploaded k6 summaries.
func (r *LoadTestReconciler) sweepFilerObjects(ctx context.Context, lt *dfaasv1.LoadTest, why string) {
	if lt.Spec.SyncStart {
		logStatusErr(ctx, "delete GO signal ("+why+")", r.syncChannel().DeleteGo(ctx, lt))
	}
	logStatusErr(ctx, "delete k6 summaries ("+why+")", r.syncChannel().DeleteSummaries(ctx, lt))
}

// endRestamps closes the Conditions a run end leaves in progress, so the panel
// does not show an export, a barrier or a dispatch still under way on a
// terminal test.
func endRestamps(lt *dfaasv1.LoadTest, p dfaasv1.LoadTestPhase, endReason string) []statuswriter.Cond {
	var out []statuswriter.Cond
	if p != dfaasv1.LoadTestCompleted && lt.Status.Phase != dfaasv1.LoadTestExporting {
		out = append(out, statuswriter.Cond{Type: dfaasv1.LTCondMetricsExported, Status: metav1.ConditionFalse,
			Reason: dfaasv1.LTReasonExportSkipped, Message: "no exporter ran — the test ended before its metrics export"})
	}
	if c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondSyncReady); c != nil &&
		c.Reason == dfaasv1.LTReasonAwaitingRunners {
		out = append(out, statuswriter.Cond{Type: dfaasv1.LTCondSyncReady, Status: metav1.ConditionFalse,
			Reason: endReason, Message: "the test ended before the GO signal"})
	}
	if c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondK6Dispatched); c != nil &&
		(c.Status == metav1.ConditionUnknown || c.Reason == dfaasv1.LTReasonInFlight) {
		out = append(out, statuswriter.Cond{Type: dfaasv1.LTCondK6Dispatched, Status: metav1.ConditionFalse,
			Reason: endReason, Message: fmt.Sprintf("the test ended with %d/%d TestRun(s) applied",
				len(lt.Status.TestRuns), len(lt.Spec.PerNodeLoad))})
	}
	return out
}

// retryReclaim re-sweeps a terminal test that still holds its Environment.
// Still unreclaimed: no write, try again later. Otherwise one write of the new
// K6Healthy verdict releases the queue.
// ponytail: each retry against a dead k3s API stalls the single LoadTest
// worker for up to the remote timeout per generator.
func (r *LoadTestReconciler) retryReclaim(ctx context.Context, lt *dfaasv1.LoadTest) (ctrl.Result, error) {
	var env dfaasv1.Environment
	err := r.Get(ctx, types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}, &env)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	endReason := dfaasv1.LTReasonFailed
	if c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondReady); c != nil && c.Reason != "" {
		endReason = c.Reason
	}
	v := r.teardownRemoteTestRuns(ctx, lt, &env).verdict(endReason)
	if v.Reason == dfaasv1.LTReasonRunnersUnreclaimed {
		return ctrl.Result{RequeueAfter: reclaimRetryInterval}, nil
	}
	return ctrl.Result{}, r.writer().Record(ctx, lt, ltTransition{Conditions: []statuswriter.Cond{v}})
}
