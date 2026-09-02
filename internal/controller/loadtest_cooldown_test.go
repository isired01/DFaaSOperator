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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// The management Prometheus federates worker metrics on a 1m interval, so the
// tail of a run lands after k6 stops. runExporter must therefore hold for
// exportCooldown before creating the exporter Job — otherwise the CSV is
// truncated by up to one federation period, by a different amount each run.
var _ = Describe("Export cool-down", func() {
	var (
		ctx context.Context
		env *dfaasv1.Environment
	)

	BeforeEach(func() {
		ctx = context.Background()
		env = &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: "cooldown-env", Namespace: "default"},
		}
	})

	// stampTimes puts the LoadTest in Exporting with EndTime `ago` in the past.
	stampTimes := func(lt *dfaasv1.LoadTest, ago time.Duration) {
		start := metav1.NewTime(time.Now().Add(-ago - time.Minute))
		end := metav1.NewTime(time.Now().Add(-ago))
		lt.Status.Phase = dfaasv1.LoadTestExporting
		lt.Status.StartTime = &start
		lt.Status.EndTime = &end
		Expect(k8sClient.Status().Update(ctx, lt)).To(Succeed())
	}

	It("waits and creates no exporter Job while the cool-down is unelapsed", func() {
		lt := newQueueLoadTest("cooldown-wait", env.Name)
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, lt) }()
		stampTimes(lt, 5*time.Second)

		reconciler := &LoadTestReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		res, err := reconciler.runExporter(ctx, lt, env)
		Expect(err).NotTo(HaveOccurred())

		// Requeued for roughly the remaining cool-down, never longer than the
		// full period (a regression to `RequeueAfter: exportCooldown` would
		// restart the wait on every reconcile and never converge).
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		Expect(res.RequeueAfter).To(BeNumerically("<", exportCooldown))

		// The property that actually protects the data: nothing exported yet.
		var job batchv1.Job
		gerr := k8sClient.Get(ctx, client.ObjectKey{Name: ExporterJobName(lt), Namespace: lt.Namespace}, &job)
		Expect(apierrors.IsNotFound(gerr)).To(BeTrue(), "exporter Job must not exist during cool-down")

		fresh := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), fresh)).To(Succeed())
		cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.LTCondMetricsExported)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(dfaasv1.LTReasonExportCooldown))
		// Still Exporting — the cool-down is a wait, not a new terminal state.
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestExporting))
	})

	It("stops holding once the cool-down has elapsed", func() {
		lt := newQueueLoadTest("cooldown-done", env.Name)
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, lt) }()
		stampTimes(lt, exportCooldown+30*time.Second)

		reconciler := &LoadTestReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		_, err := reconciler.runExporter(ctx, lt, env)
		Expect(err).NotTo(HaveOccurred())

		// Past the gate: whatever the export path then does (in envtest the
		// default S3 config is absent, so it degrades to the stdout dump), it
		// must no longer report itself as cooling down.
		fresh := &dfaasv1.LoadTest{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(lt), fresh)).To(Succeed())
		cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.LTCondMetricsExported)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).NotTo(Equal(dfaasv1.LTReasonExportCooldown))
	})

	// EndTime is persisted, so the deadline is absolute: an operator restart
	// mid-cool-down must resume the original minute, not begin a fresh one.
	It("keys the deadline off the persisted EndTime, not the reconcile time", func() {
		lt := newQueueLoadTest("cooldown-resume", env.Name)
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, lt) }()
		stampTimes(lt, exportCooldown-10*time.Second)

		reconciler := &LoadTestReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		res, err := reconciler.runExporter(ctx, lt, env)
		Expect(err).NotTo(HaveOccurred())

		// ~10s left, not a full minute.
		Expect(res.RequeueAfter).To(BeNumerically("<=", 11*time.Second))
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))
	})
})
