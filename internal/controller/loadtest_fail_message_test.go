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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

var _ = Describe("LoadTest Ready aggregator messages", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// Regression: failLoadTest used to stamp its message on Ready and then let
	// the phase transition overwrite it with the generic "load test failed".
	// The UI renders this message verbatim, so the diagnostic must survive.
	It("keeps the caller's failure message on the Ready condition", func() {
		lt := newQueueLoadTest("fail-msg", "fail-msg-env")
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, lt) }()

		const detail = `k6 node "generator" is no longer part of the environment`
		reconciler := &LoadTestReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		_, err := reconciler.failLoadTest(ctx, lt, detail)
		Expect(err).NotTo(HaveOccurred())

		fresh := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), fresh)).To(Succeed())
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))

		cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.LTCondReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonFailed))
		Expect(cond.Message).To(Equal(detail))
	})

	// Abort path: the reason matters too — the UI looks the explanation up by
	// Ready/UserAborted specifically.
	It("lets an abort override both the aggregator reason and message", func() {
		lt := &dfaasv1.LoadTest{}
		const detail = "The test was manually aborted from the UI."
		stampLTAggregate(lt, dfaasv1.LoadTestAborted, dfaasv1.LTReasonUserAborted, detail)

		cond := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonUserAborted))
		Expect(cond.Message).To(Equal(detail))
	})

	It("falls back to the generic per-phase text with no override", func() {
		lt := &dfaasv1.LoadTest{}
		stampLTAggregate(lt, dfaasv1.LoadTestRunning, "", "")

		cond := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonRunning))
		Expect(cond.Message).To(Equal("remote TestRuns dispatched, k6 running"))
	})
})
