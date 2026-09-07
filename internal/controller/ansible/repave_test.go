/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/roles"
)

func repaveScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	if err := dfaasv1.AddToScheme(s); err != nil {
		t.Fatalf("add dfaasv1 to scheme: %v", err)
	}
	return s
}

// The k6 playbook stamps ownerReferences on the kubeconfig Secret it pushes to
// the management cluster, so it needs the Environment UID in its host vars. An
// empty uid renders an invalid ownerReference that the API server rejects.
func TestBuildInventoryK6EmitsEnvUID(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo", Namespace: "default", UID: "1f0c9c8e-uid"},
	}
	nodes := []dfaasv1.EnvironmentNode{{
		NodeID: "gen-a", IPAddress: "10.0.0.9", Role: dfaasv1.RoleK6LoadGenerator,
		Username: "ubuntu", Password: "pw",
	}}

	k6, _ := roles.For(dfaasv1.RoleK6LoadGenerator)
	inv, err := buildInventory(env, k6, nodes, nil)
	if err != nil {
		t.Fatalf("buildInventory: %v", err)
	}
	if !strings.Contains(inv, "env_uid='1f0c9c8e-uid'") {
		t.Errorf("inventory missing env_uid host var:\n%s", inv)
	}
}

// A node that flips away from dfaas-worker must lose its libp2p key entry,
// otherwise the Secret accumulates identities for machines that no longer run
// an agent. Two things must NOT be pruned: keys for nodes still holding the
// worker role, and pre-seeded keys for nodeIDs absent from spec entirely --
// that is the documented BYO escape hatch for pinning a peer identity before
// the node is added.
func TestEnsureLibp2pKeysPrunesFlippedNodeAndKeepsBYO(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo", Namespace: "default", UID: "uid-1"},
		Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
			{NodeID: "worker-a", IPAddress: "10.0.0.1", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p"},
			// flipped: was a worker, now a generator
			{NodeID: "flipped-b", IPAddress: "10.0.0.2", Role: dfaasv1.RoleK6LoadGenerator, Username: "u", Password: "p"},
		}},
	}

	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo-libp2p-keys", Namespace: "default"},
		Data: map[string][]byte{
			"worker-a":  []byte("key-a"),
			"flipped-b": []byte("key-b"),
			"byo-c":     []byte("key-c"), // pre-seeded, not in spec yet
		},
	}

	s := repaveScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()
	m := &Manager{Client: c, Scheme: s}

	out, err := m.EnsureLibp2pKeys(context.Background(), env)
	if err != nil {
		t.Fatalf("EnsureLibp2pKeys: %v", err)
	}
	if _, ok := out["flipped-b"]; ok {
		t.Error("returned keys still include the flipped node")
	}
	if out["worker-a"] != "key-a" {
		t.Errorf("worker key not preserved: got %q want %q", out["worker-a"], "key-a")
	}

	var got corev1.Secret
	if err := c.Get(context.Background(),
		client.ObjectKey{Name: "env-demo-libp2p-keys", Namespace: "default"}, &got); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if _, ok := got.Data["flipped-b"]; ok {
		t.Error("flipped node key still persisted in the Secret")
	}
	if string(got.Data["worker-a"]) != "key-a" {
		t.Error("worker-a key was clobbered or removed")
	}
	if string(got.Data["byo-c"]) != "key-c" {
		t.Error("pre-seeded BYO key for a nodeID absent from spec was pruned")
	}
}
