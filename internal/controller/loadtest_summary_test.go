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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	k6fake "dfaas-operator/internal/k6dispatch/fake"
	syncfake "dfaas-operator/internal/syncchannel/fake"
)

// The filer paths and the base-per-operation rules now live with the channel
// itself (internal/syncchannel). What belongs here is what the reconciler does
// with the answer -- and the case that matters is a synchronized start with no
// VM-facing GO URL. It must fail before anything is dispatched: otherwise every
// runner parks on a barrier nobody can open, the barrier holds at
// SyncReady=False/AwaitingRunners for the whole five-minute budget, and only
// then fails.

func syncStartLT(t *testing.T, syncStart bool) (client.Client, *dfaasv1.LoadTest) {
	t.Helper()
	s := runtime.NewScheme()
	if err := dfaasv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	lt := &dfaasv1.LoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: "lt-sample", Namespace: "default", Generation: 1},
		Spec: dfaasv1.LoadTestSpec{
			TargetEnvironment: "env",
			SyncStart:         syncStart,
			PerNodeLoad: []dfaasv1.PerNodeLoad{
				{NodeID: "gen-a", VUs: 1, Duration: "10s", ScriptConfigMap: corev1.LocalObjectReference{Name: "script"}},
			},
		},
	}
	c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(lt).WithObjects(lt).Build()
	return c, lt
}

func TestStartK6FailsLoudlyWithNoPublicBase(t *testing.T) {
	c, lt := syncStartLT(t, true)
	fleet := k6fake.New()
	// Public empty: neither DFAAS_SYNC_PUBLIC_URL nor HOST_IP resolved.
	r := &LoadTestReconciler{Client: c, Dispatcher: fleet, Sync: &syncfake.Channel{}}

	if _, err := r.startK6(context.Background(), lt, &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "default"},
	}); err != nil {
		t.Fatalf("startK6: %v", err)
	}

	var got dfaasv1.LoadTest
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(lt), &got); err != nil {
		t.Fatalf("get loadtest: %v", err)
	}
	if got.Status.Phase != dfaasv1.LoadTestFailed {
		t.Errorf("phase: want %s, got %s", dfaasv1.LoadTestFailed, got.Status.Phase)
	}
	if ready := meta.FindStatusCondition(got.Status.Conditions, dfaasv1.LTCondReady); ready != nil &&
		!strings.Contains(ready.Message, "DFAAS_SYNC_PUBLIC_URL") {
		t.Errorf("the message must name the missing override, got %q", ready.Message)
	}
	// The point of failing here rather than later: nothing was dispatched.
	if applied := fleet.Applied(); len(applied) != 0 {
		t.Errorf("dispatched %d TestRuns before failing; want none", len(applied))
	}
}

// Without syncStart there is no barrier, so an unresolvable public base is not
// fatal: a missing summary upload only degrades data richness.
func TestStartK6WithoutSyncStartDoesNotNeedAPublicBase(t *testing.T) {
	c, lt := syncStartLT(t, false)
	fleet := k6fake.New()
	r := &LoadTestReconciler{Client: c, Dispatcher: fleet, Sync: &syncfake.Channel{}}

	if _, err := r.startK6(context.Background(), lt, &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "default"},
	}); err != nil {
		t.Fatalf("startK6: %v", err)
	}

	var got dfaasv1.LoadTest
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(lt), &got); err != nil {
		t.Fatalf("get loadtest: %v", err)
	}
	if got.Status.Phase == dfaasv1.LoadTestFailed {
		ready := meta.FindStatusCondition(got.Status.Conditions, dfaasv1.LTCondReady)
		msg := ""
		if ready != nil {
			msg = ready.Message
		}
		if strings.Contains(msg, "DFAAS_SYNC_PUBLIC_URL") {
			t.Error("a non-syncStart LoadTest must not fail on a missing public base")
		}
	}
}
