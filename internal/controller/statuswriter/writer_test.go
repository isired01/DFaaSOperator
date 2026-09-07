/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package statuswriter

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
)

func ltWriter(c client.Client) Writer[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase] {
	return Writer[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{
		Client:   c,
		New:      func() *dfaasv1.LoadTest { return &dfaasv1.LoadTest{} },
		SetPhase: func(lt *dfaasv1.LoadTest, p dfaasv1.LoadTestPhase) { lt.Status.Phase = p },
		Aggregate: func(lt *dfaasv1.LoadTest, p dfaasv1.LoadTestPhase, reason, msg string) {
			if reason == "" {
				reason = "Generic"
			}
			if msg == "" {
				msg = "generic " + string(p)
			}
			meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
				Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: msg})
		},
		Conditions: func(lt *dfaasv1.LoadTest) *[]metav1.Condition { return &lt.Status.Conditions },
	}
}

func newLT(t *testing.T) (client.Client, *dfaasv1.LoadTest) {
	t.Helper()
	s := runtime.NewScheme()
	if err := dfaasv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	lt := &dfaasv1.LoadTest{ObjectMeta: metav1.ObjectMeta{Name: "lt", Namespace: "ns", Generation: 3}}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(lt).WithObjects(lt).Build()
	return c, lt
}

// The ordering trap the old setLoadTestPhaseDetail existed for: a specific
// condition stamped in the same transition as a phase change must survive the
// aggregate, and the aggregate must carry the override.
func TestRecordStampsConditionsBeforeAggregateAndKeepsOverride(t *testing.T) {
	c, lt := newLT(t)
	w := ltWriter(c)
	err := w.Record(context.Background(), lt, Transition[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{
		Phase:      ptr.To(dfaasv1.LoadTestFailed),
		Reason:     "DispatchFailed",
		Message:    "remote dispatch failed 15 times",
		Conditions: []Cond{{Type: "K6Dispatched", Status: metav1.ConditionFalse, Reason: "DispatchFailed", Message: "detail"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got dfaasv1.LoadTest
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(lt), &got)
	if got.Status.Phase != dfaasv1.LoadTestFailed {
		t.Errorf("phase = %q", got.Status.Phase)
	}
	if k := meta.FindStatusCondition(got.Status.Conditions, "K6Dispatched"); k == nil || k.Reason != "DispatchFailed" {
		t.Errorf("sub-condition lost: %+v", k)
	}
	if r := meta.FindStatusCondition(got.Status.Conditions, "Ready"); r == nil || r.Reason != "DispatchFailed" || r.Message != "remote dispatch failed 15 times" {
		t.Errorf("aggregate override lost: %+v", r)
	}
}

// Same (status, reason, message) twice must not move LastTransitionTime.
func TestRecordKeepsLastTransitionTimeStable(t *testing.T) {
	c, lt := newLT(t)
	w := ltWriter(c)
	tr := Transition[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{Conditions: []Cond{{Type: "X", Status: metav1.ConditionTrue, Reason: "R", Message: "m"}}}
	if err := w.Record(context.Background(), lt, tr); err != nil {
		t.Fatal(err)
	}
	var first dfaasv1.LoadTest
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(lt), &first)
	t0 := meta.FindStatusCondition(first.Status.Conditions, "X").LastTransitionTime
	if err := w.Record(context.Background(), lt, tr); err != nil {
		t.Fatal(err)
	}
	var second dfaasv1.LoadTest
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(lt), &second)
	if !meta.FindStatusCondition(second.Status.Conditions, "X").LastTransitionTime.Equal(&t0) {
		t.Error("LastTransitionTime moved on a no-op re-stamp")
	}
}

// Counters are generation-scoped; a legacy plain value parses as gen 0 and
// resets on the first bump; Reset does not write when already zero.
func TestCountersAreGenerationScoped(t *testing.T) {
	c, lt := newLT(t)
	w := ltWriter(c)
	ctx := context.Background()

	// Legacy plain "7" from before this change.
	lt.Annotations = map[string]string{"ctr": "7"}
	if err := c.Update(ctx, lt); err != nil {
		t.Fatal(err)
	}
	n, err := w.Bump(ctx, lt, "ctr")
	if err != nil || n != 1 {
		t.Fatalf("first bump after legacy value: n=%d err=%v (want 1)", n, err)
	}
	n, _ = w.Bump(ctx, lt, "ctr")
	if n != 2 {
		t.Errorf("second bump n=%d", n)
	}
	var got dfaasv1.LoadTest
	_ = c.Get(ctx, client.ObjectKeyFromObject(lt), &got)
	if got.Annotations["ctr"] != "3:2" {
		t.Errorf("encoding = %q, want 3:2", got.Annotations["ctr"])
	}
	if err := w.Reset(ctx, lt, "ctr"); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(ctx, client.ObjectKeyFromObject(lt), &got)
	rv := got.ResourceVersion
	if got.Annotations["ctr"] != "3:0" {
		t.Errorf("after reset = %q", got.Annotations["ctr"])
	}
	if err := w.Reset(ctx, lt, "ctr"); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(ctx, client.ObjectKeyFromObject(lt), &got)
	if got.ResourceVersion != rv {
		t.Error("Reset on an already-zero counter wrote the object")
	}
}

func TestTouchRunsOnFreshCopyAfterPhase(t *testing.T) {
	c, lt := newLT(t)
	w := ltWriter(c)
	now := metav1.Now()
	err := w.Record(context.Background(), lt, Transition[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{
		Phase: ptr.To(dfaasv1.LoadTestRunning),
		Touch: func(fresh *dfaasv1.LoadTest) error {
			if fresh.Status.Phase != dfaasv1.LoadTestRunning {
				t.Error("Touch ran before the phase was set")
			}
			fresh.Status.StartTime = &now
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got dfaasv1.LoadTest
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(lt), &got)
	if got.Status.StartTime == nil {
		t.Error("Touch's write was lost")
	}
}
