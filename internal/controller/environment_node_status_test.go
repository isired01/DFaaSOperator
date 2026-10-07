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
	"errors"
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
)

// syncNodeStatus is where a management address detected by the k6 playbook
// becomes status.k6Nodes[].managementAddress, the field every per-generator
// URL is built on. The value travels on the generator's own kubeconfig Secret,
// so each generator must get the address from its own Secret and nothing else,
// and whatever is missing or unusable must read as "not detected", never as
// an error that holds the Environment in ProvisioningMonitoring.

// nodeStatusCase is one k6 generator: what its kubeconfig Secret carries and
// the managementAddress syncNodeStatus must record for it.
type nodeStatusCase struct {
	nodeID string
	// pushed is false when the playbook never pushed the Secret.
	pushed bool
	// annotations on the pushed Secret; nil means none at all (an older
	// playbook).
	annotations map[string]string
	want        string
}

func nodeStatusEnv(cases []nodeStatusCase) *dfaasv1.Environment {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo", Namespace: "default", UID: "uid-1", Generation: 1},
		Status:     dfaasv1.EnvironmentStatus{Phase: dfaasv1.EnvProvisioningMonitoring},
	}
	for i, tc := range cases {
		env.Spec.Nodes = append(env.Spec.Nodes, dfaasv1.EnvironmentNode{
			NodeID: tc.nodeID, IPAddress: fmt.Sprintf("10.0.0.%d", i+1), Role: dfaasv1.RoleK6LoadGenerator,
			Username: "u", Password: "p",
		})
	}
	return env
}

func TestSyncNodeStatusReadsEachGeneratorsManagementAddress(t *testing.T) {
	cases := []nodeStatusCase{
		{nodeID: "gen-v4", pushed: true,
			annotations: map[string]string{ansible.AnnotationKubeconfigManagementAddress: "100.64.0.11"},
			want:        "100.64.0.11"},
		{nodeID: "gen-v6", pushed: true,
			annotations: map[string]string{ansible.AnnotationKubeconfigManagementAddress: " fd7a:115c:a1e0:0:0:0:0:11 "},
			want:        "fd7a:115c:a1e0::11"},
		{nodeID: "gen-mapped", pushed: true,
			annotations: map[string]string{ansible.AnnotationKubeconfigManagementAddress: "::ffff:100.64.0.12"},
			want:        "100.64.0.12"},
		// Not pushed yet, or pruned: no address, and no error.
		{nodeID: "gen-unpushed", pushed: false},
		// The playbook detected nothing and wrote the annotation empty.
		{nodeID: "gen-empty", pushed: true,
			annotations: map[string]string{ansible.AnnotationKubeconfigManagementAddress: ""}},
		// Pushed by a playbook that predates the detection.
		{nodeID: "gen-older", pushed: true},
		// Recorded but unusable: never handed to a runner.
		{nodeID: "gen-loopback", pushed: true,
			annotations: map[string]string{ansible.AnnotationKubeconfigManagementAddress: "127.0.0.1"}},
		{nodeID: "gen-whole-ssh", pushed: true,
			annotations: map[string]string{ansible.AnnotationKubeconfigManagementAddress: "100.64.0.13 51234 100.64.0.9 22"}},
	}

	env := nodeStatusEnv(cases)
	env.Spec.Nodes = append(env.Spec.Nodes, dfaasv1.EnvironmentNode{
		NodeID: "w1", IPAddress: "10.0.1.1", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p",
	})
	// A re-provision rebuilds the list: an address the previous run recorded
	// is dropped when this run's Secret no longer carries one.
	env.Status.K6Nodes = []dfaasv1.K6NodeStatus{{
		NodeID: "gen-empty", IPAddress: "10.0.0.5", KubeconfigSecret: "env-demo-gen-empty-kubeconfig",
		ManagementAddress: "100.64.0.50",
	}}

	objs := []client.Object{env}
	for _, tc := range cases {
		if !tc.pushed {
			continue
		}
		sec := kubeconfigSecret(ansible.KubeconfigSecretName(env.Name, tc.nodeID), env.Name, tc.nodeID)
		sec.Annotations = tc.annotations
		objs = append(objs, sec)
	}
	// Secrets syncNodeStatus must never read for gen-unpushed: another
	// Environment's, one in another namespace, and one for the worker node.
	stray := map[string]string{ansible.AnnotationKubeconfigManagementAddress: "100.64.0.99"}
	other := kubeconfigSecret("other-env-gen-unpushed-kubeconfig", "other-env", "gen-unpushed")
	other.Annotations = stray
	elsewhere := kubeconfigSecret(ansible.KubeconfigSecretName(env.Name, "gen-unpushed"), env.Name, "gen-unpushed")
	elsewhere.Namespace = "other-ns"
	elsewhere.Annotations = stray
	worker := kubeconfigSecret(ansible.KubeconfigSecretName(env.Name, "w1"), env.Name, "w1")
	worker.Annotations = stray
	objs = append(objs, other, elsewhere, worker)

	c := fake.NewClientBuilder().WithScheme(pruneScheme(t)).
		WithStatusSubresource(env).WithObjects(objs...).Build()
	r := &EnvironmentReconciler{Client: c}

	if err := r.syncNodeStatus(context.Background(), env); err != nil {
		t.Fatalf("syncNodeStatus: %v", err)
	}

	var got dfaasv1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatalf("get environment: %v", err)
	}
	if len(got.Status.K6Nodes) != len(cases) {
		t.Fatalf("want %d k6 nodes in status, got %d: %+v", len(cases), len(got.Status.K6Nodes), got.Status.K6Nodes)
	}
	for i, tc := range cases {
		n := got.Status.K6Nodes[i]
		want := dfaasv1.K6NodeStatus{
			NodeID:            tc.nodeID,
			IPAddress:         env.Spec.Nodes[i].IPAddress,
			KubeconfigSecret:  "env-demo-" + tc.nodeID + "-kubeconfig",
			ManagementAddress: tc.want,
		}
		if n != want {
			t.Errorf("status.k6Nodes[%d]:\n got %+v\nwant %+v", i, n, want)
		}
	}
	if want := []string{"w1"}; !reflect.DeepEqual(got.Status.DfaasNodes, want) {
		t.Errorf("status.dfaasNodes: want %v, got %v", want, got.Status.DfaasNodes)
	}
}

// Only NotFound means "no address". Any other read failure is returned, so
// the caller requeues instead of writing a status that dropped a detected
// address, and nothing is written in the meantime.
func TestSyncNodeStatusReturnsSecretReadErrors(t *testing.T) {
	cases := []nodeStatusCase{{nodeID: "gen-a", pushed: true,
		annotations: map[string]string{ansible.AnnotationKubeconfigManagementAddress: "100.64.0.11"}}}
	env := nodeStatusEnv(cases)
	sec := kubeconfigSecret(ansible.KubeconfigSecretName(env.Name, "gen-a"), env.Name, "gen-a")
	sec.Annotations = cases[0].annotations

	refused := errors.New("secrets is forbidden: RBAC")
	c := fake.NewClientBuilder().WithScheme(pruneScheme(t)).
		WithStatusSubresource(env).WithObjects(env, sec).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return refused
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	r := &EnvironmentReconciler{Client: c}

	if err := r.syncNodeStatus(context.Background(), env); !errors.Is(err, refused) {
		t.Fatalf("want the Secret read error back, got %v", err)
	}
	var got dfaasv1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatalf("get environment: %v", err)
	}
	if len(got.Status.K6Nodes) != 0 {
		t.Errorf("status written despite the read error: %+v", got.Status.K6Nodes)
	}
}
