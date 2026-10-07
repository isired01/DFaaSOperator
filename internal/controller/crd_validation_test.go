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
		// A dfaas-worker with no functions is rejected at admission, so the
		// minimal VALID worker carries one.
		Functions: []dfaasv1.Function{{
			Name: "figlet", Image: "ghcr.io/openfaas/figlet:latest",
			ExecTimeout: 5, MaxInflight: 400, TimeoutMs: 6000, MaxRate: 100,
		}},
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

	// A dfaas-worker with no functions serves nothing, and the inventory turns
	// its nil list into the JSON literal `null` rather than `[]`: the prune task
	// then runs `null | map(attribute='name')` over a four-character string and
	// the whole playbook dies, leaving the Environment in Failed. Rejecting it
	// at admission is both the honest rule and the fix.
	It("rejects a dfaas-worker with no functions", func() {
		node := validationNode("worker-1", "10.0.0.1")
		node.Functions = nil
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "nofunc-", Namespace: "default"},
			Spec:       dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{node}},
		}
		err := k8sClient.Create(ctx, env)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an admission rejection, got %v", err)
		Expect(err.Error()).To(ContainSubstring("at least one function"))
	})

	It("rejects a dfaas-worker with an empty function list", func() {
		node := validationNode("worker-1", "10.0.0.1")
		node.Functions = []dfaasv1.Function{}
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "emptyfunc-", Namespace: "default"},
			Spec:       dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{node}},
		}
		err := k8sClient.Create(ctx, env)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an admission rejection, got %v", err)
	})

	// The rule is scoped to the worker role: a generator runs k6, not OpenFaaS,
	// and buildInventory emits no function var for it at all.
	It("accepts a k6-load-generator with no functions", func() {
		gen := validationNode("gen-1", "10.0.0.9")
		gen.Role = dfaasv1.RoleK6LoadGenerator
		gen.Functions = nil
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "genonly-", Namespace: "default"},
			Spec:       dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{gen}},
		}
		Expect(k8sClient.Create(ctx, env)).To(Succeed())
		Expect(k8sClient.Delete(ctx, env)).To(Succeed())
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

	// One machine is one node. The listMapKey above only guards nodeID, so
	// before the CEL rule two entries could share an ipAddress: the Ansible run
	// then installed dfaas-agent twice on that box with two different libp2p
	// keys, and the last one to land left every peer dialling a dead peer ID.
	// The rule needs MaxItems on spec.nodes and MaxLength on ipAddress to stay
	// inside the CRD cost budget — this spec fails at CRD-install time if either
	// is dropped, so it also guards the budget.
	It("rejects two nodes sharing an ipAddress", func() {
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "dup-ip-", Namespace: "default"},
			Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
				validationNode("worker-1", "10.0.0.1"),
				validationNode("worker-2", "10.0.0.1"),
			}},
		}
		err := k8sClient.Create(ctx, env)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an admission rejection, got %v", err)
		Expect(err.Error()).To(ContainSubstring("each ipAddress must appear at most once"))
	})

	// A k6 generator and a dfaas-worker on the same box is the same collision:
	// the rule spans the whole list, not one role.
	It("rejects an ipAddress shared across roles", func() {
		k6 := validationNode("gen-1", "10.0.0.1")
		k6.Role = dfaasv1.RoleK6LoadGenerator
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "dup-ip-roles-", Namespace: "default"},
			Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
				validationNode("worker-1", "10.0.0.1"),
				k6,
			}},
		}
		err := k8sClient.Create(ctx, env)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected an admission rejection, got %v", err)
	})
})
