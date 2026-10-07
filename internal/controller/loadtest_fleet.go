/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/statuswriter"
	"dfaas-operator/internal/k6dispatch"
)

// Fleet round: one poll of every generator a LoadTest runs on, per tick.
//
// A round polls everything once, never stops early, and the observe counter
// is charged once per round.
//
// Bounds, one per wait: observe = fetch-misses (fetchRetryBudget rounds);
// Sync barrier = syncWaitBudget from K6Dispatched=True, which also bounds the
// GO publish; deletion poll = deletionReclaimBudget from DeletionTimestamp;
// dispatch = dispatch-attempts. Every failed remote round is paced at
// remoteRetryInterval, because the counter write's own watch event would
// otherwise start the next attempt at once and burn a budget in seconds.
// Known limitation: rounds poll generators one after another, so K dead generators
// block the single worker for K remote timeouts per round.

// remoteRetryInterval is the minimum gap between a failed remote round of a
// LoadTest and its next attempt.
const remoteRetryInterval = 10 * time.Second

// stageBucket classifies a TestRun stage. The one Stage switch.
type stageBucket int

const (
	stagePending stageBucket = iota // created, initialization, "", anything unknown
	stageStarted                    // k6-operator stage "started" (the script may not be at the barrier yet)
	stageDone                       // finished or stopped
	stageErrored                    // error
)

func classifyStage(stage string) stageBucket {
	switch stage {
	case "started":
		return stageStarted
	case "finished", "stopped":
		return stageDone
	case "error":
		return stageErrored
	}
	return stagePending
}

// round is what one poll of the fleet found. Every generator is in exactly
// one of: answered (with a stage), missing (TestRun absent or remote
// unreachable) or unusable (no kubeconfig).
type round struct {
	stages   map[string]string
	count    map[stageBucket]int
	missing  []string
	unusable []string
	causes   map[string]string
}

// survey polls each generator's stage exactly once. It never writes and never
// charges a counter.
func (r *LoadTestReconciler) survey(ctx context.Context, lt *dfaasv1.LoadTest,
	env *dfaasv1.Environment, nodeIDs []string) *round {
	logger := log.FromContext(ctx)
	rd := &round{stages: map[string]string{}, count: map[stageBucket]int{}, causes: map[string]string{}}
	for _, id := range nodeIDs {
		node, nerr := r.Dispatcher.Node(ctx, env, id)
		if nerr != nil {
			rd.unusable = append(rd.unusable, id)
			rd.causes[id] = condMessage(nerr)
			continue
		}
		stage, err := node.Stage(ctx, lt)
		if err != nil {
			logger.Error(err, "remote TestRun fetch failed", "node", id)
			rd.missing = append(rd.missing, id)
			if errors.Is(err, k6dispatch.ErrNotFound) {
				rd.causes[id] = "TestRun not found"
			} else {
				rd.causes[id] = condMessage(err)
			}
			continue
		}
		rd.stages[id] = stage
		rd.count[classifyStage(stage)]++
	}
	return rd
}

// refs returns prev with every answering generator's stage refreshed; the
// others keep their last known stage.
func (rd *round) refs(prev []dfaasv1.TestRunRef) []dfaasv1.TestRunRef {
	out := make([]dfaasv1.TestRunRef, len(prev))
	copy(out, prev)
	for i := range out {
		if s, ok := rd.stages[out[i].NodeID]; ok {
			out[i].Phase = s
		}
	}
	return out
}

func (rd *round) describe(ids []string) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%s (%s)", id, rd.causes[id]))
	}
	return strings.Join(parts, ", ")
}

// retryInterval is the pacing floor after a failed remote round.
func (r *LoadTestReconciler) retryInterval() time.Duration {
	if r.retryEvery > 0 {
		return r.retryEvery
	}
	return remoteRetryInterval
}

// attempt is the single place a LoadTest Retry counter is charged. It also
// records the failed round, so the gate in Reconcile paces the next one.
func (r *LoadTestReconciler) attempt(ctx context.Context, lt *dfaasv1.LoadTest,
	b statuswriter.Budget[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]) (statuswriter.Outcome, error) {
	r.lastMiss.Store(client.ObjectKeyFromObject(lt), time.Now())
	return b.Attempt(ctx, lt)
}

// paced returns how long lt must still wait after its last failed remote
// round, or 0. It makes no remote call and no write.
// Known limitation: in-memory, so an operator restart forgets the pace; the next
// round then comes one interval early, bounded by the counters anyway.
func (r *LoadTestReconciler) paced(lt *dfaasv1.LoadTest) time.Duration {
	key := client.ObjectKeyFromObject(lt)
	v, ok := r.lastMiss.Load(key)
	if !ok {
		return 0
	}
	if wait := r.retryInterval() - time.Since(v.(time.Time)); wait > 0 {
		return wait
	}
	r.lastMiss.Delete(key)
	return 0
}
