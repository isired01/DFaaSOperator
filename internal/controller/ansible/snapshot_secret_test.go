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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
)

func snapshotEnv(nodes ...dfaasv1.EnvironmentNode) *dfaasv1.Environment {
	return &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo", Namespace: "default", UID: "uid-1"},
		Spec:       dfaasv1.EnvironmentSpec{Nodes: nodes},
	}
}

func snapshotManager(t *testing.T, objs ...client.Object) *Manager {
	t.Helper()
	s := repaveScheme(t)
	return &Manager{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
		Scheme: s,
	}
}

// The snapshot is the only surviving copy of a node's SSH credentials once that
// node leaves spec.nodes, so every field the teardown inventory and the
// classifier need must survive the round trip -- password included.
func TestSaveSnapshotRoundTripsEveryField(t *testing.T) {
	nodes := baseSpec()
	m := snapshotManager(t)
	env := snapshotEnv(nodes...)

	if err := m.SaveSnapshot(context.Background(), env); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, found, err := m.LoadSnapshot(context.Background(), env)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if !found {
		t.Fatal("found = false right after SaveSnapshot")
	}
	if len(got) != len(nodes) {
		t.Fatalf("snapshot holds %d records, want %d", len(got), len(nodes))
	}
	for i, n := range nodes {
		rec, ok := got[n.NodeID]
		if !ok {
			t.Fatalf("node %q missing from snapshot", n.NodeID)
		}
		if rec.IPAddress != n.IPAddress || rec.Role != n.Role ||
			rec.Username != n.Username || rec.Password != n.Password ||
			rec.BalancingStrategy != n.BalancingStrategy || rec.Position != i {
			t.Errorf("node %q round-tripped as %+v, want it to mirror %+v at position %d",
				n.NodeID, rec, n, i)
		}
		if !sameFunctions(rec.Functions, n.Functions) {
			t.Errorf("node %q functions round-tripped as %+v, want %+v",
				n.NodeID, rec.Functions, n.Functions)
		}
	}
}

// found=false must be distinguishable from an empty snapshot: Classify treats
// the two as opposite statements, and conflating them wipes the federation.
func TestLoadSnapshotReportsAbsence(t *testing.T) {
	m := snapshotManager(t)

	got, found, err := m.LoadSnapshot(context.Background(), snapshotEnv(baseSpec()...))
	if err != nil {
		t.Fatalf("LoadSnapshot on a missing Secret returned an error: %v", err)
	}
	if found {
		t.Error("found = true with no Secret in the cluster")
	}
	if len(got) != 0 {
		t.Errorf("snapshot = %v, want empty", got)
	}
}

// Without the controller ownerReference the Secret survives its Environment,
// leaking node credentials into the namespace forever.
func TestSaveSnapshotStampsOwnerReference(t *testing.T) {
	m := snapshotManager(t)
	env := snapshotEnv(baseSpec()...)

	if err := m.SaveSnapshot(context.Background(), env); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	var sec corev1.Secret
	key := client.ObjectKey{Name: SnapshotSecretName(env), Namespace: env.Namespace}
	if err := m.Get(context.Background(), key, &sec); err != nil {
		t.Fatalf("get snapshot secret: %v", err)
	}
	owners := sec.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Name != env.Name || owners[0].Controller == nil || !*owners[0].Controller {
		t.Errorf("ownerReferences = %+v, want one controller ref to %q", owners, env.Name)
	}
}

// The snapshot is whole state, not an accumulator: merging would resurrect
// records for machines that have already been decommissioned, and those records
// would then keep showing up in every later Removed set.
func TestSaveSnapshotReplacesRatherThanMerges(t *testing.T) {
	m := snapshotManager(t)
	full := baseSpec()
	env := snapshotEnv(full...)
	if err := m.SaveSnapshot(context.Background(), env); err != nil {
		t.Fatalf("SaveSnapshot (first): %v", err)
	}

	shrunk := snapshotEnv(full[0], full[2]) // w1 removed
	if err := m.SaveSnapshot(context.Background(), shrunk); err != nil {
		t.Fatalf("SaveSnapshot (second): %v", err)
	}

	got, found, err := m.LoadSnapshot(context.Background(), shrunk)
	if err != nil || !found {
		t.Fatalf("LoadSnapshot: found=%v err=%v", found, err)
	}
	if _, stale := got["w1"]; stale {
		t.Error("w1 survived a save that no longer lists it: the snapshot merged instead of replacing")
	}
	if len(got) != 2 {
		t.Errorf("snapshot holds %d records, want 2", len(got))
	}
}

// The upgrade seed must not overwrite a real record: an Environment that has
// already provisioned knows more than its current spec does.
func TestSaveSnapshotIfAbsentDoesNotClobber(t *testing.T) {
	m := snapshotManager(t)
	full := baseSpec()
	env := snapshotEnv(full...)
	if err := m.SaveSnapshot(context.Background(), env); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	shrunk := snapshotEnv(full[0])
	if err := m.SaveSnapshotIfAbsent(context.Background(), shrunk); err != nil {
		t.Fatalf("SaveSnapshotIfAbsent: %v", err)
	}

	got, _, err := m.LoadSnapshot(context.Background(), env)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(got) != len(full) {
		t.Errorf("snapshot holds %d records, want the original %d: the seed clobbered a real record",
			len(got), len(full))
	}
}

// ...but it must write when there is nothing, or the first node removal after
// an operator upgrade has nothing to diff against and silently orphans a
// machine.
func TestSaveSnapshotIfAbsentSeedsWhenMissing(t *testing.T) {
	m := snapshotManager(t)
	env := snapshotEnv(baseSpec()...)

	if err := m.SaveSnapshotIfAbsent(context.Background(), env); err != nil {
		t.Fatalf("SaveSnapshotIfAbsent: %v", err)
	}

	got, found, err := m.LoadSnapshot(context.Background(), env)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if !found || len(got) != len(env.Spec.Nodes) {
		t.Errorf("found=%v with %d records, want true with %d", found, len(got), len(env.Spec.Nodes))
	}
}
