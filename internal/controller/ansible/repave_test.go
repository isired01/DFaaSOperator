/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/monitoring"
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

// The k6 playbook probes the management address it detects on the filer
// NodePort, and the port reaches it only through this host var: the playbook
// holds no copy of its own (a play var would also shadow this one). Without it
// the probe is skipped and no generator ever gets a detected address.
func TestBuildInventoryK6EmitsFilerNodePort(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo", Namespace: "default", UID: "1f0c9c8e-uid"},
	}
	nodes := []dfaasv1.EnvironmentNode{
		{NodeID: "gen-a", IPAddress: "10.0.0.9", Role: dfaasv1.RoleK6LoadGenerator, Username: "ubuntu", Password: "pw"},
		{NodeID: "gen-b", IPAddress: "10.0.0.10", Role: dfaasv1.RoleK6LoadGenerator, Username: "ubuntu", Password: "pw"},
	}

	k6, _ := roles.For(dfaasv1.RoleK6LoadGenerator)
	inv, err := buildInventory(env, k6, nodes, nil)
	if err != nil {
		t.Fatalf("buildInventory: %v", err)
	}
	want := fmt.Sprintf(" filer_node_port=%d", monitoring.FilerNodePort)
	lines := strings.Split(strings.TrimSpace(inv), "\n")[1:] // drop the [group] header
	if len(lines) != len(nodes) {
		t.Fatalf("expected %d host lines, got %d:\n%s", len(nodes), len(lines), inv)
	}
	for i, line := range lines {
		if !strings.Contains(line, want) {
			t.Errorf("host line for %s missing%s:\n%s", nodes[i].NodeID, want, line)
		}
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

// testLibp2pKey returns a base64 PKCS#8 ed25519 key, the format
// EnsureLibp2pKeys stores in the Secret and derivePeerID expects.
func testLibp2pKey(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal PKCS8: %v", err)
	}
	return base64.StdEncoding.EncodeToString(der)
}

// The bootstrap list is the federation topology for good: the agent dials it
// once in Initialize (agent/discovery/kademlia in the upstream dfaas repo) and
// its Kademlia discovery never returns a peer (it announces only its pod
// address, the same 10.42.0.0/24 on every node). A list holding only nodes[0]
// therefore produced a star, not a mesh, and every node whose single link
// dropped sat at 0 peers with all Conditions green.
// Node i must get nodes 0..i-1: each pair dialed once, no circular wait.
func TestBuildInventoryBootstrapListIsFullMesh(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-mesh", Namespace: "default", UID: "uid-1"},
	}
	nodes := []dfaasv1.EnvironmentNode{
		{NodeID: "w0", IPAddress: "100.0.0.10", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p"},
		{NodeID: "w1", IPAddress: "100.0.0.11", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p"},
		{NodeID: "w2", IPAddress: "100.0.0.12", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p"},
	}
	keys := map[string]string{}
	for _, n := range nodes {
		keys[n.NodeID] = testLibp2pKey(t)
	}

	worker, _ := roles.For(dfaasv1.RoleDfaasWorker)
	inv, err := buildInventory(env, worker, nodes, keys)
	if err != nil {
		t.Fatalf("buildInventory: %v", err)
	}

	peerIDs := make([]string, len(nodes))
	for i, n := range nodes {
		id, err := derivePeerID(keys[n.NodeID])
		if err != nil {
			t.Fatalf("derivePeerID(%s): %v", n.NodeID, err)
		}
		peerIDs[i] = id
	}

	// One inventory line per node, in spec order.
	lines := strings.Split(strings.TrimSpace(inv), "\n")
	if len(lines) != len(nodes)+1 { // +1 for the [group] header
		t.Fatalf("expected %d inventory lines, got %d:\n%s", len(nodes)+1, len(lines), inv)
	}

	for i, n := range nodes {
		line := lines[i+1]
		if !strings.HasPrefix(line, n.IPAddress+" ") {
			t.Fatalf("line %d is not node %s:\n%s", i, n.NodeID, line)
		}

		var want []string
		for j := 0; j < i; j++ {
			want = append(want, fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", nodes[j].IPAddress, libp2pBootstrapPort, peerIDs[j]))
		}
		wantAddr := "bootstrap_address='" + strings.Join(want, ",") + "'"
		if !strings.Contains(line, wantAddr) {
			t.Errorf("node %s: want %s\ngot: %s", n.NodeID, wantAddr, line)
		}

		// is_bootstrap gates AGENT_BOOTSTRAP_NODES and _FORCE, so the first
		// node must stay false: it has nobody to dial and would otherwise
		// block in init forever.
		wantFlag := fmt.Sprintf("is_bootstrap=%t", i != 0)
		if !strings.Contains(line, wantFlag) {
			t.Errorf("node %s: want %s\ngot: %s", n.NodeID, wantFlag, line)
		}
	}
}
