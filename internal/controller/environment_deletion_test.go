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
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// failingGetClient fails the Get of one specific key and delegates everything
// else. Used to simulate the shared prometheus-targets ConfigMap being
// unreadable while an Environment is being deleted.
type failingGetClient struct {
	client.Client
	failKey client.ObjectKey
}

func (c failingGetClient) Get(ctx context.Context, key client.ObjectKey,
	obj client.Object, opts ...client.GetOption) error {
	if key == c.failKey {
		return apierrors.NewInternalError(errors.New("injected prometheus-targets failure"))
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

var _ = Describe("Environment deletion finalizer", func() {
	var (
		ctx context.Context
		env *dfaasv1.Environment
		key client.ObjectKey
	)

	BeforeEach(func() {
		ctx = context.Background()
		env = &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "env-del-",
				Namespace:    "default",
				Finalizers:   []string{environmentFinalizer},
			},
			Spec: dfaasv1.EnvironmentSpec{
				Nodes: []dfaasv1.EnvironmentNode{validationNode("worker-1", "10.0.0.1")},
			},
		}
		Expect(k8sClient.Create(ctx, env)).To(Succeed())
		key = client.ObjectKeyFromObject(env)
	})

	AfterEach(func() {
		// Defensive: a failed assertion could leave the CR pinned by its finalizer.
		leftover := &dfaasv1.Environment{}
		if err := k8sClient.Get(ctx, key, leftover); err == nil {
			leftover.Finalizers = nil
			_ = k8sClient.Update(ctx, leftover)
			_ = k8sClient.Delete(ctx, leftover)
		}
	})

	It("keeps the finalizer when the Prometheus target cleanup fails", func() {
		Expect(k8sClient.Delete(ctx, env)).To(Succeed())

		pending := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, pending)).To(Succeed())
		Expect(pending.DeletionTimestamp).NotTo(BeNil())

		failing := &EnvironmentReconciler{
			Client: failingGetClient{
				Client:  k8sClient,
				failKey: client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"},
			},
			Scheme: scheme.Scheme,
		}
		_, err := failing.handleEnvDeletion(ctx, pending)
		Expect(err).To(HaveOccurred())

		// The per-environment scrape file does not cascade, so the CR must stay
		// pinned until the cleanup actually succeeds.
		stillThere := &dfaasv1.Environment{}
		Expect(k8sClient.Get(ctx, key, stillThere)).To(Succeed())
		Expect(stillThere.Finalizers).To(ContainElement(environmentFinalizer))

		// A clean pass releases the CR.
		healthy := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		_, err = healthy.handleEnvDeletion(ctx, stillThere)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, key, &dfaasv1.Environment{}))
		}).WithTimeout(10 * time.Second).WithPolling(200 * time.Millisecond).Should(BeTrue())
	})
})
