/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package statuswriter

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
)

const testCounter = "dfaas.dfaas.io/test-attempts"

func ltBudget(c client.Client, limit int) Budget[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase] {
	return Budget[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{
		Writer: ltWriter(c), Counter: testCounter, Limit: limit,
	}
}

// The five-step protocol, walked once: each failed round advances the count,
// the first is flagged, and the budget is spent exactly at the limit.
func TestBudgetWalksToExhaustion(t *testing.T) {
	c, lt := newLT(t)
	b := ltBudget(c, 3)
	ctx := context.Background()

	want := []Outcome{
		{Count: 1, First: true},
		{Count: 2},
		{Count: 3, Exhausted: true},
	}
	for i, expected := range want {
		got, err := b.Attempt(ctx, lt)
		if err != nil {
			t.Fatalf("attempt %d: %v", i+1, err)
		}
		if got != expected {
			t.Errorf("attempt %d: got %+v, want %+v", i+1, got, expected)
		}
	}

	// Past the limit it stays exhausted rather than wrapping or resetting.
	got, err := b.Attempt(ctx, lt)
	if err != nil {
		t.Fatalf("attempt 4: %v", err)
	}
	if !got.Exhausted || got.Count != 4 {
		t.Errorf("attempt 4: got %+v, want exhausted with count 4", got)
	}
}

// Clear is what a successful round does, and the next failure starts over.
func TestBudgetClearRestartsTheCount(t *testing.T) {
	c, lt := newLT(t)
	b := ltBudget(c, 3)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := b.Attempt(ctx, lt); err != nil {
			t.Fatalf("attempt: %v", err)
		}
	}
	if err := b.Clear(ctx, lt); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	got, err := b.Attempt(ctx, lt)
	if err != nil {
		t.Fatalf("attempt after Clear: %v", err)
	}
	if got.Count != 1 || !got.First {
		t.Errorf("got %+v, want the count restarted at 1 and flagged First", got)
	}
}

// A spec edit restarts every budget from zero: the counter is
// generation-scoped, which Budget inherits untouched (ADR-0003).
func TestBudgetRestartsOnANewGeneration(t *testing.T) {
	c, lt := newLT(t)
	b := ltBudget(c, 3)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := b.Attempt(ctx, lt); err != nil {
			t.Fatalf("attempt: %v", err)
		}
	}

	// Bump the generation the way a spec edit would.
	var live dfaasv1.LoadTest
	if err := c.Get(ctx, client.ObjectKeyFromObject(lt), &live); err != nil {
		t.Fatalf("get: %v", err)
	}
	live.Generation++
	if err := c.Update(ctx, &live); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := b.Attempt(ctx, lt)
	if err != nil {
		t.Fatalf("attempt after the spec edit: %v", err)
	}
	if got.Count != 1 || !got.First {
		t.Errorf("got %+v, want a fresh budget after a spec edit", got)
	}
}

// failingMetaClient fails exactly the write Bump and Reset make, so the
// counter is unavailable while everything else works.
type failingMetaClient struct {
	client.Client
	err error
}

func (f failingMetaClient) Update(_ context.Context, _ client.Object, _ ...client.UpdateOption) error {
	return f.err
}

// The bump-failure path: three inconsistent behaviours in the reconciler,
// none of them tested. Now there is one answer.
func TestBudgetReportsAnUnavailableCounter(t *testing.T) {
	c, lt := newLT(t)
	broken := failingMetaClient{Client: c, err: errors.New("conflict on the annotation update")}
	b := ltBudget(broken, 3)

	got, err := b.Attempt(context.Background(), lt)
	if err == nil {
		t.Fatal("want the write error returned, so a caller may log it")
	}
	want := Outcome{CounterUnavailable: true}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	// The three properties the callers rely on, spelled out:
	if got.Exhausted {
		t.Error("nothing may go terminal on the strength of an unknown count")
	}
	if got.First {
		t.Error("an unknown count is not a first observation — ensureMonitoring would stamp Unknown")
	}
	if got.Count != 0 {
		t.Errorf("Count = %d; an unknown count must not read as a measurement", got.Count)
	}
}

// The message fragment. One call site used to format the unavailable case as
// "attempt 0/3", which reads as a real measurement of zero.
func TestBudgetAttemptsRendering(t *testing.T) {
	b := Budget[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{Counter: testCounter, Limit: 3}
	cases := []struct {
		outcome Outcome
		want    string
	}{
		{Outcome{Count: 1, First: true}, "1/3"},
		{Outcome{Count: 3, Exhausted: true}, "3/3"},
		{Outcome{CounterUnavailable: true}, "?/3"},
	}
	for _, tc := range cases {
		if got := b.Attempts(tc.outcome); got != tc.want {
			t.Errorf("Attempts(%+v) = %q, want %q", tc.outcome, got, tc.want)
		}
	}
}

// A zero Limit means "count, never exhaust" rather than "exhaust immediately",
// which is what a caller that forgets to set it would otherwise get.
func TestBudgetWithNoLimitNeverExhausts(t *testing.T) {
	c, lt := newLT(t)
	b := ltBudget(c, 0)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		got, err := b.Attempt(ctx, lt)
		if err != nil {
			t.Fatalf("attempt %d: %v", i+1, err)
		}
		if got.Exhausted {
			t.Fatalf("attempt %d: a budget with no limit must never exhaust", i+1)
		}
	}
}

// Proof the type parameters carry an Environment too: the four real call sites
// are split across both CR types.
func TestBudgetWorksForEnvironmentsToo(t *testing.T) {
	s := runtime.NewScheme()
	if err := dfaasv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "bari", Namespace: "default", Generation: 2},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env).Build()

	b := Budget[*dfaasv1.Environment, dfaasv1.EnvironmentPhase]{
		Writer: Writer[*dfaasv1.Environment, dfaasv1.EnvironmentPhase]{
			Client:     c,
			New:        func() *dfaasv1.Environment { return &dfaasv1.Environment{} },
			SetPhase:   func(e *dfaasv1.Environment, p dfaasv1.EnvironmentPhase) { e.Status.Phase = p },
			Aggregate:  func(*dfaasv1.Environment, dfaasv1.EnvironmentPhase, string, string) {},
			Conditions: func(e *dfaasv1.Environment) *[]metav1.Condition { return &e.Status.Conditions },
		},
		Counter: testCounter,
		Limit:   2,
	}

	ctx := context.Background()
	if got, err := b.Attempt(ctx, env); err != nil || got.Count != 1 || !got.First {
		t.Fatalf("first attempt: %+v, %v", got, err)
	}
	if got, err := b.Attempt(ctx, env); err != nil || !got.Exhausted {
		t.Fatalf("second attempt: %+v, %v", got, err)
	}
}
