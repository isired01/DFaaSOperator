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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/statuswriter"
)

// 192.0.2.1 is TEST-NET-1 (RFC 5737) — guaranteed unroutable, so probeSSH
// always returns false and every reconcile counts as an SSH-unreachable round.
const unroutableIP = "192.0.2.1"

var _ = Describe("ProvisioningVMs SSH retry budget", func() {
	var (
		ctx        context.Context
		reconciler *EnvironmentReconciler
		env        *dfaasv1.Environment
		key        client.ObjectKey
	)

	BeforeEach(func() {
		ctx = context.Background()
		reconciler = &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}

		env = &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "ssh-retry-",
				Namespace:    "default",
			},
			Spec: dfaasv1.EnvironmentSpec{
				Nodes: []dfaasv1.EnvironmentNode{{
					NodeID:    "generator",
					IPAddress: unroutableIP,
					Role:      dfaasv1.RoleK6LoadGenerator,
					Capacity:  dfaasv1.CapacityLow,
					Username:  "ubuntu",
					Password:  "ubuntu",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, env)).To(Succeed())
		key = client.ObjectKeyFromObject(env)
	})

	AfterEach(func() {
		_ = k8sClient.Delete(ctx, env)
	})

	It("moves to Unreachable after sshRetryBudget consecutive unreachable rounds", func() {
		// First sshRetryBudget-1 rounds requeue, staying out of Unreachable.
		for i := 1; i < sshRetryBudget; i++ {
			res, err := reconciler.reconcileProvisioningVMs(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			fresh := &dfaasv1.Environment{}
			Expect(k8sClient.Get(ctx, key, fresh)).To(Succeed())
			Expect(fresh.Status.Phase).NotTo(Equal(dfaasv1.EnvUnreachable))
		}

		// The sshRetryBudget-th round gives up on fast retries → non-terminal
		// Unreachable (auto-recovering), not terminal Failed.
		_, err := reconciler.reconcileProvisioningVMs(ctx, env)
		Expect(err).NotTo(HaveOccurred())

		failed := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, failed)).To(Succeed())
		Expect(failed.Status.Phase).To(Equal(dfaasv1.EnvUnreachable))

		cond := meta.FindStatusCondition(failed.Status.Conditions, dfaasv1.EnvCondVMsReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(dfaasv1.EnvReasonSSHUnreachable))
	})

	It("restarts the budget fresh on a new generation", func() {
		// Burn the whole budget on the current generation → Unreachable.
		for i := 0; i < sshRetryBudget; i++ {
			_, err := reconciler.reconcileProvisioningVMs(ctx, env)
			Expect(err).NotTo(HaveOccurred())
		}
		failed := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, failed)).To(Succeed())
		Expect(failed.Status.Phase).To(Equal(dfaasv1.EnvUnreachable))

		// Edit the spec → metadata.generation bumps. The counter is
		// generation-scoped, so the first post-edit round must requeue
		// (attempt 1 of the new budget), not immediately re-fail.
		latest := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, latest)).To(Succeed())
		latest.Spec.Nodes[0].Capacity = dfaasv1.CapacityHigh
		Expect(k8sClient.Update(ctx, latest)).To(Succeed())

		afterEdit := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, afterEdit)).To(Succeed())
		Expect(afterEdit.Generation).To(BeNumerically(">", failed.Generation))

		res, err := reconciler.reconcileProvisioningVMs(ctx, afterEdit)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		reset := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, reset)).To(Succeed())
		gen, count := statuswriter.ParseCounter(reset.Annotations[sshAttemptsAnnotation])
		Expect(gen).To(Equal(reset.Generation))
		Expect(count).To(Equal(1))
	})
})
