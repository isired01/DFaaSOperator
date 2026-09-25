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
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

const (
	// probeSettleAfter: a probe Pod still unfinished this long after
	// K6Dispatched=True (its own deadline is 30 s) did not run.
	probeSettleAfter = time.Minute
	// probeReadWindow bounds the extra remote reads; after it the reclaim
	// paths own any probe Pod left.
	probeReadWindow = 2 * time.Minute
)

// startProbe launches nodeID's reachability probe on url, the summary URL the
// TestRun was just applied with, never a recomputed one. Best-effort: a probe
// that cannot start is the operator log's business, never the test's.
func (r *LoadTestReconciler) startProbe(ctx context.Context, lt *dfaasv1.LoadTest,
	node k6dispatch.Node, nodeID, url string) {
	if url == "" {
		log.FromContext(ctx).Info("filer reachability probe skipped: no VM-facing filer URL resolved", "node", nodeID)
		return
	}
	if err := node.StartProbe(ctx, lt, url); err != nil {
		log.FromContext(ctx).Error(err, "filer reachability probe not started", "node", nodeID)
	}
}

// collectProbes reads every generator's probe once all have settled, warns on
// K6Dispatched (status unchanged, so the barrier budget does not move) when
// one could not reach the filer, and deletes the probe Pods once the warning
// is stored. A probe that did not run is logged only.
func (r *LoadTestReconciler) collectProbes(ctx context.Context, lt *dfaasv1.LoadTest, env *dfaasv1.Environment) {
	logger := log.FromContext(ctx)
	c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondK6Dispatched)
	if c == nil || c.Status != metav1.ConditionTrue {
		return
	}
	age := time.Since(c.LastTransitionTime.Time)
	if age > probeReadWindow {
		return
	}
	nodes := map[string]k6dispatch.Node{}
	outcomes := map[string]k6dispatch.ProbeOutcome{}
	for _, ref := range lt.Status.TestRuns {
		node, err := r.Dispatcher.Node(ctx, env, ref.NodeID)
		if err != nil {
			continue
		}
		o, err := node.ProbeResult(ctx, lt)
		if err != nil {
			logger.Error(err, "filer reachability probe unreadable", "node", ref.NodeID)
			return // read everything again on the next pass
		}
		if o.State == k6dispatch.ProbeRunning {
			if age < probeSettleAfter {
				return
			}
			o = k6dispatch.ProbeOutcome{State: k6dispatch.ProbeDidNotRun, URL: o.URL,
				Detail: "no verdict within " + probeSettleAfter.String()}
		}
		nodes[ref.NodeID], outcomes[ref.NodeID] = node, o
	}

	var failed []string
	for _, ref := range lt.Status.TestRuns {
		o, ok := outcomes[ref.NodeID]
		if !ok {
			continue
		}
		switch o.State {
		case k6dispatch.ProbeUnreachable:
			logger.Info("generator cannot reach the filer", "node", ref.NodeID, "url", o.URL, "error", o.Detail)
			failed = append(failed, fmt.Sprintf("%s tried %s: %s", ref.NodeID, o.URL, o.Detail))
		case k6dispatch.ProbeDidNotRun:
			logger.Info("filer reachability probe did not run", "node", ref.NodeID, "detail", o.Detail)
		}
	}
	// The first verdict stands: a later pass over a partly deleted set would
	// only name fewer generators. The probe Pods are the verdict's only
	// record, so a warning that did not land keeps them for the next pass.
	if len(failed) > 0 && c.Reason != dfaasv1.LTReasonDispatchedUnreachable {
		msg := fmt.Sprintf("dispatched %d remote TestRun(s), but %d generator(s) cannot reach the filer the runners report to: %s. "+
			"The GO signal and the end-of-test summaries will not reach them; DFAAS_SYNC_PUBLIC_URL must be an address every generator can reach.",
			len(lt.Status.TestRuns), len(failed), strings.Join(failed, "; "))
		if err := r.condErr(ctx, lt, dfaasv1.LTCondK6Dispatched, metav1.ConditionTrue,
			dfaasv1.LTReasonDispatchedUnreachable, msg); err != nil {
			logger.Error(err, "filer reachability warning not stored; probe Pods kept for the next pass")
			return
		}
		// condErr writes through a separate fresh Get inside the status writer;
		// it never touches lt itself. Mirror the same stamp onto the caller's
		// copy so a later read in this same pass (probeNote in awaitSyncBarrier's
		// fail, or the "first verdict stands" guard above on a second call) sees
		// it instead of the pre-warning reason.
		meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
			Type: dfaasv1.LTCondK6Dispatched, Status: metav1.ConditionTrue,
			Reason: dfaasv1.LTReasonDispatchedUnreachable, Message: msg,
		})
	}
	for _, ref := range lt.Status.TestRuns {
		if o, ok := outcomes[ref.NodeID]; ok && o.State != k6dispatch.ProbeAbsent {
			logStatusErr(ctx, "delete filer reachability probe", nodes[ref.NodeID].DeleteProbe(ctx, lt))
		}
	}
}

// probeNote is what a failure message adds when the dispatch-time probe had
// already found the filer unreachable: the likely cause, for whoever reads
// only the failure.
func probeNote(lt *dfaasv1.LoadTest) string {
	c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondK6Dispatched)
	if c == nil || c.Reason != dfaasv1.LTReasonDispatchedUnreachable {
		return ""
	}
	return " — at dispatch: " + c.Message
}

// reclaimProbes deletes, best-effort, the probe Pod of every target generator
// of a run whose runners were already done (the teardown deletes it otherwise).
func (r *LoadTestReconciler) reclaimProbes(ctx context.Context, lt *dfaasv1.LoadTest) {
	logger := log.FromContext(ctx)
	var env dfaasv1.Environment
	if err := r.Get(ctx, types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}, &env); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("environment gone; cannot delete filer reachability probes", "env", lt.Spec.TargetEnvironment)
		} else {
			logger.Error(err, "cannot resolve environment to delete filer reachability probes", "env", lt.Spec.TargetEnvironment)
		}
		return
	}
	for _, nodeID := range k6dispatch.TargetNodeIDs(lt) {
		node, err := r.Dispatcher.Node(ctx, &env, nodeID)
		if err != nil {
			logger.Error(err, "cannot resolve generator to delete its filer reachability probe", "node", nodeID)
			continue
		}
		logStatusErr(ctx, "delete filer reachability probe (run end)", node.DeleteProbe(ctx, lt))
	}
}
