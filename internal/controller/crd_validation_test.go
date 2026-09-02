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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"
)

// validationNode builds a minimal valid dfaas-worker node.
func validationNode(nodeID, ip string) dfaasv1.EnvironmentNode {
	return dfaasv1.EnvironmentNode{
		NodeID:    nodeID,
		IPAddress: ip,
		Role:      dfaasv1.RoleDfaasWorker,
		Capacity:  dfaasv1.CapacityLow,
		Username:  "ubuntu",
		Password:  "ubuntu",
	}
}

var _ = Describe("Environment CRD validation", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	// spec.nodes is a listType=map keyed by nodeID, so the API server rejects
	// duplicates at admission. Without it both entries would fight over the same
	// derived names (kubeconfig Secret, libp2p key entry, remote TestRun).
	It("rejects two nodes sharing a nodeID", func() {
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "dup-node-", Namespace: "default"},
			Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
				validationNode("worker-1", "10.0.0.1"),
				validationNode("worker-1", "10.0.0.2"),
			}},
		}
		err := k8sClient.Create(ctx, env)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an admission rejection, got %v", err)
		Expect(err.Error()).To(ContainSubstring("Duplicate value"))
	})

	It("accepts distinct nodeIDs", func() {
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "uniq-node-", Namespace: "default"},
			Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
				validationNode("worker-1", "10.0.0.1"),
				validationNode("worker-2", "10.0.0.2"),
			}},
		}
		Expect(k8sClient.Create(ctx, env)).To(Succeed())
		Expect(k8sClient.Delete(ctx, env)).To(Succeed())
	})
})
