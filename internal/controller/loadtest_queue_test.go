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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// newQueueLoadTest builds a minimal valid LoadTest targeting envName. The name
// prefix is deterministic so creationTimestamp ties break predictably by name.
func newQueueLoadTest(name, envName string) *dfaasv1.LoadTest {
	return &dfaasv1.LoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: dfaasv1.LoadTestSpec{
			TargetEnvironment: envName,
			PerNodeLoad: []dfaasv1.PerNodeLoad{{
				NodeID:          "generator",
				VUs:             1,
				Duration:        "10s",
				ScriptConfigMap: corev1.LocalObjectReference{Name: "script"},
			}},
			MetricsExport: dfaasv1.MetricsExportSpec{
				Metrics: []dfaasv1.MetricExportEntry{{Type: dfaasv1.MetricTypeRaw, Query: "up"}},
			},
		},
	}
}

var _ = Describe("Environment occupancy gate (FIFO queue)", func() {
	var (
		ctx        context.Context
		reconciler *LoadTestReconciler
		envName    string
		created    []*dfaasv1.LoadTest
	)

	BeforeEach(func() {
		ctx = context.Background()
		reconciler = &LoadTestReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		envName = "queue-env"
		created = nil
	})

	AfterEach(func() {
		for _, lt := range created {
			_ = k8sClient.Delete(ctx, lt)
		}
	})

	// create persists lt and sets its phase via a status subresource update.
	create := func(name string, phase dfaasv1.LoadTestPhase) *dfaasv1.LoadTest {
		lt := newQueueLoadTest(name, envName)
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		created = append(created, lt)
		if phase != "" {
			lt.Status.Phase = phase
			Expect(k8sClient.Status().Update(ctx, lt)).To(Succeed())
		}
		return lt
	}

	It("holds a test at Pending when a sibling is Running on the same env", func() {
		_ = create("busy-running", dfaasv1.LoadTestRunning)
		second := create("busy-waiter", "")

		proceed, res, err := reconciler.envOccupancyGate(ctx, second)
		Expect(err).NotTo(HaveOccurred())
		Expect(proceed).To(BeFalse())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		fresh := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(second), fresh)).To(Succeed())
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestPending))
		cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.LTCondQueued)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonEnvBusy))
	})

	It("treats an Exporting sibling as busy too", func() {
		_ = create("exp-running", dfaasv1.LoadTestExporting)
		second := create("exp-waiter", "")

		proceed, _, err := reconciler.envOccupancyGate(ctx, second)
		Expect(err).NotTo(HaveOccurred())
		Expect(proceed).To(BeFalse())

		fresh := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(second), fresh)).To(Succeed())
		cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.LTCondQueued)
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonEnvBusy))
	})

	It("serializes two waiting tests by creation order (FIFO front)", func() {
		front := create("aaa-front", "")
		behind := create("zzz-behind", "")

		// The younger / lexically-later test is held behind the front.
		proceed, res, err := reconciler.envOccupancyGate(ctx, behind)
		Expect(err).NotTo(HaveOccurred())
		Expect(proceed).To(BeFalse())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		fresh := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(behind), fresh)).To(Succeed())
		cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.LTCondQueued)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonQueuedBehind))

		// The front test is cleared to dispatch.
		proceed, _, err = reconciler.envOccupancyGate(ctx, front)
		Expect(err).NotTo(HaveOccurred())
		Expect(proceed).To(BeTrue())

		freshFront := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(front), freshFront)).To(Succeed())
		frontCond := meta.FindStatusCondition(freshFront.Status.Conditions, dfaasv1.LTCondQueued)
		Expect(frontCond).NotTo(BeNil())
		Expect(frontCond.Status).To(Equal(metav1.ConditionFalse))
		Expect(frontCond.Reason).To(Equal(dfaasv1.LTReasonDispatching))
	})

	It("does not count a suspended draft as the queue front", func() {
		// Draft created first, but suspended → excluded from the waiting set.
		draft := newQueueLoadTest("aaa-draft", envName)
		draft.Spec.Suspended = true
		Expect(k8sClient.Create(ctx, draft)).To(Succeed())
		created = append(created, draft)

		active := create("zzz-active", "")

		// Despite the draft's earlier name/creation, the active test is front.
		proceed, _, err := reconciler.envOccupancyGate(ctx, active)
		Expect(err).NotTo(HaveOccurred())
		Expect(proceed).To(BeTrue())
	})

	// Regression for the partial-dispatch race: a sibling still at Pending but
	// already holding remote TestRuns (startK6 flips to Running only after the
	// last node) must count as occupying, and must itself keep the env even
	// against an older newly-activated sibling — otherwise two TestRun sets run
	// concurrently on one Environment.
	It("holds a newcomer behind a Pending sibling that already dispatched TestRuns", func() {
		// Older waiter with NO dispatched runs (created first → oldest).
		older := create("aaa-older", dfaasv1.LoadTestPending)
		// Younger occupant that is still Pending but has persisted a TestRun.
		occupant := create("zzz-occupant", dfaasv1.LoadTestPending)
		occupant.Status.TestRuns = []dfaasv1.TestRunRef{{
			NodeID: "generator", Name: "zzz-occupant-generator", Namespace: "default",
		}}
		Expect(k8sClient.Status().Update(ctx, occupant)).To(Succeed())

		// The occupant proceeds unconditionally (rule 0) even though `older`
		// predates it — deferring would strand its live remote run.
		proceed, _, err := reconciler.envOccupancyGate(ctx, occupant)
		Expect(err).NotTo(HaveOccurred())
		Expect(proceed).To(BeTrue())

		// The older, un-dispatched sibling is held: the env is occupied by the
		// Pending-but-dispatched test, not free for a second concurrent run.
		proceed, res, err := reconciler.envOccupancyGate(ctx, older)
		Expect(err).NotTo(HaveOccurred())
		Expect(proceed).To(BeFalse())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		fresh := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(older), fresh)).To(Succeed())
		cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.LTCondQueued)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonEnvBusy))
	})
})
