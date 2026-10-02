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
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// probeRequest is one generator applied in a dispatch pass: its node and the
// summary URL its TestRun was applied with.
type probeRequest struct {
	node   k6dispatch.Node
	nodeID string
	url    string
}

// startProbes wipes a stale probe Pod and starts the probe of every generator
// applied in this dispatch pass. startK6 runs it after the Apply loop, so no
// probe call sits between two Applies. A pass that ended the run (a later
// generator unusable, the dispatch budget spent, the Sync barrier failing)
// starts none: the run end's teardown has already run, and a probe started
// after it would outlive the run.
func (r *LoadTestReconciler) startProbes(ctx context.Context, lt *dfaasv1.LoadTest, reqs []probeRequest) {
	if len(reqs) == 0 {
		return
	}
	var fresh dfaasv1.LoadTest
	if err := r.occupancyReader().Get(ctx, client.ObjectKeyFromObject(lt), &fresh); err != nil {
		log.FromContext(ctx).Error(err, "filer reachability probes not started: cannot tell whether the run is still live")
		return
	}
	if fresh.Status.Phase.Terminal() {
		log.FromContext(ctx).Info("filer reachability probes not started: the run ended in this pass", "phase", fresh.Status.Phase)
		return
	}
	for _, q := range reqs {
		// A probe Pod left by a previous run of this name would answer for it.
		logStatusErr(ctx, "delete stale filer reachability probe", q.node.DeleteProbe(ctx, lt))
		r.startProbe(ctx, lt, q.node, q.nodeID, q.url)
	}
}

// startProbe launches nodeID's reachability probe on url, the summary URL the
// TestRun was applied with, never a recomputed one. Best-effort: a probe
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
// is stored. A probe that did not run is logged only. Once every verdict is
// stored and every Pod deleted it marks the test and reads nothing more.
func (r *LoadTestReconciler) collectProbes(ctx context.Context, lt *dfaasv1.LoadTest, env *dfaasv1.Environment) {
	if _, done := r.probesRead.Load(lt.UID); done {
		return
	}
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
	consumed := true // every probe read, its verdict stored, its Pod deleted
	for _, ref := range lt.Status.TestRuns {
		node, err := r.Dispatcher.Node(ctx, env, ref.NodeID)
		if err != nil {
			consumed = false
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

	var failed, detected []string
	fellBack := false // some failing generator dialled the process-wide fallback
	for _, ref := range lt.Status.TestRuns {
		o, ok := outcomes[ref.NodeID]
		if !ok {
			continue
		}
		switch o.State {
		case k6dispatch.ProbeUnreachable:
			logger.Info("generator cannot reach the filer", "node", ref.NodeID, "url", o.URL, "error", o.Detail)
			failed = append(failed, fmt.Sprintf("%s tried %s: %s", ref.NodeID, o.URL, o.Detail))
			// The remedy depends on where the URL came from: the address
			// detected for this generator, or the process-wide fallback.
			if k6dispatch.GeneratorOf(env, ref.NodeID).MgmtAddr != "" {
				detected = append(detected, ref.NodeID)
			} else {
				fellBack = true
			}
		case k6dispatch.ProbeDidNotRun:
			logger.Info("filer reachability probe did not run", "node", ref.NodeID, "detail", o.Detail)
		}
	}
	// The first verdict stands: a later pass over a partly deleted set would
	// only name fewer generators. The probe Pods are the verdict's only
	// record, so a warning that did not land keeps them for the next pass.
	if len(failed) > 0 && c.Reason != dfaasv1.LTReasonDispatchedUnreachable {
		// Shown verbatim in the UI's conditions panel. The runners talk to the
		// filer, not the other way round, and only a syncStart runner fetches
		// the GO signal.
		cannot := "upload the end-of-test summaries"
		if lt.Spec.SyncStart {
			cannot = "fetch the GO signal or upload the end-of-test summaries"
		}
		msg := fmt.Sprintf("dispatched %d remote TestRun(s), but %d generator(s) cannot reach the SeaweedFS filer: %s. "+
			"Their runners cannot %s.",
			len(lt.Status.TestRuns), len(failed), strings.Join(failed, "; "), cannot)
		for _, id := range detected {
			msg += fmt.Sprintf(" The management address detected for %s at provisioning "+
				"(status.k6Nodes[].managementAddress) does not answer on the filer NodePort; "+
				"open the port or re-provision the Environment.", id)
		}
		if fellBack {
			msg += " DFAAS_SYNC_PUBLIC_URL must be an address every generator can reach."
		}
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
			if err := nodes[ref.NodeID].DeleteProbe(ctx, lt); err != nil {
				logStatusErr(ctx, "delete filer reachability probe", err)
				consumed = false // read again, and delete again, next pass
			}
		}
	}
	if consumed {
		r.probesRead.Store(lt.UID, struct{}{})
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
