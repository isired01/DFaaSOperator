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

var _ = Describe("Ready-state SSH health check", func() {
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
				GenerateName: "health-",
				Namespace:    "default",
			},
			Spec: dfaasv1.EnvironmentSpec{
				Nodes: []dfaasv1.EnvironmentNode{{
					NodeID:    "generator",
					IPAddress: unroutableIP, // RFC 5737 TEST-NET-1, always unreachable
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

	It("stamps NodesReachable=False, records lastHealthCheck, and enters Unreachable after the budget", func() {
		// Passing the same stale in-memory env (LastHealthCheck nil) each round
		// keeps the time-throttle disabled, so each call performs a real probe.
		for i := 1; i < healthRetryBudget; i++ {
			res, err := reconciler.reconcileReadyHealth(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(healthRetryInterval))

			fresh := &dfaasv1.Environment{}
			Expect(k8sClient.Get(ctx, key, fresh)).To(Succeed())
			Expect(fresh.Status.Phase).NotTo(Equal(dfaasv1.EnvUnreachable))
			Expect(fresh.Status.LastHealthCheck).NotTo(BeNil())

			cond := meta.FindStatusCondition(fresh.Status.Conditions, dfaasv1.EnvCondNodesReachable)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(dfaasv1.EnvReasonSSHUnreachable))
		}

		// The budget-th unreachable round moves the env to the non-terminal
		// Unreachable phase (auto-recovering), not terminal Failed.
		_, err := reconciler.reconcileReadyHealth(ctx, env)
		Expect(err).NotTo(HaveOccurred())

		failed := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, failed)).To(Succeed())
		Expect(failed.Status.Phase).To(Equal(dfaasv1.EnvUnreachable))
		cond := meta.FindStatusCondition(failed.Status.Conditions, dfaasv1.EnvCondNodesReachable)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
	})

	It("throttles back-to-back checks so the miss counter is not double-counted", func() {
		// One real round on the stale env: probes, bumps to 1, stamps status.
		_, err := reconciler.reconcileReadyHealth(ctx, env)
		Expect(err).NotTo(HaveOccurred())

		afterFirst := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, afterFirst)).To(Succeed())
		_, count := parseGenCounter(afterFirst.Annotations[healthMissesAnnotation])
		Expect(count).To(Equal(1))

		// Re-run with the refetched env (recent lastHealthCheck + NodesReachable
		// False → 20s throttle window). The call must short-circuit without
		// probing or bumping the counter.
		res, err := reconciler.reconcileReadyHealth(ctx, afterFirst)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		afterSecond := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, afterSecond)).To(Succeed())
		_, count2 := parseGenCounter(afterSecond.Annotations[healthMissesAnnotation])
		Expect(count2).To(Equal(1))
	})

	It("skips probing and requeues at the health interval when no nodes are declared", func() {
		// Build (do not persist) a node-less env: reconcileReadyHealth returns
		// before any client call in that case.
		empty := &dfaasv1.Environment{Spec: dfaasv1.EnvironmentSpec{}}
		res, err := reconciler.reconcileReadyHealth(ctx, empty)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(healthCheckInterval))
	})
})
