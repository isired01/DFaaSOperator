/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package statuswriter is the one way a reconciler persists state about its
// own CR. Before it, twenty named entry points shared two primitives and
// picking the wrong one failed silently: a phase setter that returned
// requeue and a sibling that did not, a condition stamp that had to be issued
// through a special variant when a phase change followed it in the same
// tick (meta.SetStatusCondition overwrites Reason and Message wholesale),
// two on-disk counter encodings, and a log-and-swallow wrapper at 69 sites.
//
// Record takes a Transition — everything to persist in one write — and owns
// the retry, the re-fetch that absorbs informer-cache lag, the status
// subresource, the condition-before-aggregate ordering and the stable
// LastTransitionTime. Counters are generation-scoped, always. The writer never
// returns a ctrl.Result: whether and when to requeue is the reconciler's
// scheduling decision and stays at the call site.
package statuswriter

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Cond is one Condition to stamp. LastTransitionTime is deliberately absent:
// meta.SetStatusCondition stamps it only when status/reason/message actually
// change, which is what keeps it stable across no-op reconciles.
type Cond struct {
	Type    string
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

// Transition is everything one write persists on the CR's status.
//
// Conditions are stamped BEFORE the phase aggregate, so a specific diagnostic
// stamped alongside a phase change survives it — the ordering the old
// setLoadTestPhaseDetail existed to work around. Reason/Message override the
// aggregate's generic per-phase text and are only read when Phase is set.
// Touch is the escape hatch for status fields the writer does not model
// (StartTime, TestRuns, LastHealthCheck, …); it runs last, on the fresh copy.
type Transition[T client.Object, P ~string] struct {
	Phase      *P
	Reason     string
	Message    string
	Conditions []Cond
	Touch      func(T) error
}

// Writer persists Transitions for one CR type. The four funcs are the only
// things that differ between Environment and LoadTest; everything else — the
// retry, the subresource, the ordering — is shared.
type Writer[T client.Object, P ~string] struct {
	Client client.Client
	// New returns a zero object to Get into.
	New func() T
	// SetPhase writes the phase (and any per-CR side effect of a phase change,
	// e.g. observedGeneration on a settled Environment phase).
	SetPhase func(T, P)
	// Aggregate stamps the top-level Ready condition for the phase; reason and
	// message are the caller's overrides, empty for the generic text.
	Aggregate func(obj T, phase P, reason, message string)
	// Conditions returns the status.conditions slice to stamp into.
	Conditions func(T) *[]metav1.Condition
}

// Record persists t on the CR named by obj. obj is only used for its key; the
// object is re-fetched on every attempt so a caller may pass a stale copy.
func (w Writer[T, P]) Record(ctx context.Context, obj T, t Transition[T, P]) error {
	key := client.ObjectKeyFromObject(obj)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := w.New()
		if err := w.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		conds := w.Conditions(fresh)
		for _, c := range t.Conditions {
			meta.SetStatusCondition(conds, metav1.Condition{
				Type: c.Type, Status: c.Status, Reason: c.Reason, Message: c.Message,
			})
		}
		if t.Phase != nil {
			w.SetPhase(fresh, *t.Phase)
			w.Aggregate(fresh, *t.Phase, t.Reason, t.Message)
		}
		if t.Touch != nil {
			if err := t.Touch(fresh); err != nil {
				return err
			}
		}
		return w.Client.Status().Update(ctx, fresh)
	})
}

// RecordBestEffort is Record for stamps whose failure must not stop the
// reconcile: the failure is logged with a description derived from t itself,
// instead of a hand-written label at every call site.
func (w Writer[T, P]) RecordBestEffort(ctx context.Context, obj T, t Transition[T, P]) {
	if err := w.Record(ctx, obj, t); err != nil {
		log.FromContext(ctx).Error(err, "status write failed (best-effort)", "transition", describe(t))
	}
}

func describe[T client.Object, P ~string](t Transition[T, P]) string {
	var parts []string
	if t.Phase != nil {
		parts = append(parts, "phase="+string(*t.Phase))
	}
	for _, c := range t.Conditions {
		parts = append(parts, fmt.Sprintf("%s=%s/%s", c.Type, c.Status, c.Reason))
	}
	if t.Touch != nil {
		parts = append(parts, "touch")
	}
	return strings.Join(parts, " ")
}

// Bump increments the named retry counter and returns the new value.
//
// Counters live in annotations (ObjectMeta, hence Update rather than the
// status subresource) as "<generation>:<count>": a spec edit is uniformly
// "try again from zero". Legacy plain values parse as generation 0 and reset
// on the first bump after upgrade.
func (w Writer[T, P]) Bump(ctx context.Context, obj T, counter string) (int, error) {
	var n int
	err := w.updateMeta(ctx, obj, func(fresh T) bool {
		ann := fresh.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		storedGen, cur := ParseCounter(ann[counter])
		if storedGen != fresh.GetGeneration() {
			cur = 0
		}
		cur++
		n = cur
		ann[counter] = fmt.Sprintf("%d:%d", fresh.GetGeneration(), cur)
		fresh.SetAnnotations(ann)
		return true
	})
	return n, err
}

// Reset zeroes the named counter for the current generation. No write when it
// is already zero or absent, so a steady state does not churn the object (and
// re-enqueue itself).
func (w Writer[T, P]) Reset(ctx context.Context, obj T, counter string) error {
	return w.updateMeta(ctx, obj, func(fresh T) bool {
		ann := fresh.GetAnnotations()
		if _, cur := ParseCounter(ann[counter]); cur == 0 {
			return false
		}
		ann[counter] = fmt.Sprintf("%d:0", fresh.GetGeneration())
		fresh.SetAnnotations(ann)
		return true
	})
}

func (w Writer[T, P]) updateMeta(ctx context.Context, obj T, mutate func(T) (write bool)) error {
	key := client.ObjectKeyFromObject(obj)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := w.New()
		if err := w.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			return nil
		}
		return w.Client.Update(ctx, fresh)
	})
}

// ParseCounter decodes "<generation>:<count>". Anything else — absent,
// malformed, or a legacy plain integer — is (0, 0).
func ParseCounter(s string) (gen int64, count int) {
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
