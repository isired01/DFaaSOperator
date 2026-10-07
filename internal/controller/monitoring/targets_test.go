/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package monitoring

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
)

func targetsManager(t *testing.T, objs ...client.Object) (*Manager, client.Client) {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := crfake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &Manager{Client: c}, c
}

func targetsEnv(ns, name, ip string) *dfaasv1.Environment {
	return &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{{
			NodeID: "w1", IPAddress: ip, Role: dfaasv1.RoleDfaasWorker, Capacity: dfaasv1.CapacityLow,
		}}},
	}
}

func targetsCM(t *testing.T, c client.Client) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "monitoring", Name: "prometheus-targets"}, cm); err != nil {
		t.Fatalf("get prometheus-targets: %v", err)
	}
	return cm
}

// Environment is namespaced: two with the same name must not share, or
// delete, one file.
func TestTargetsOfSameNamedEnvironmentsInTwoNamespacesCoexist(t *testing.T) {
	ctx := context.Background()
	m, c := targetsManager(t)
	a, b := targetsEnv("ns1", "lab", "10.0.0.1"), targetsEnv("ns2", "lab", "10.0.0.2")
	if err := m.ReconcileTargets(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := m.ReconcileTargets(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := m.CleanupTargets(ctx, a); err != nil {
		t.Fatal(err)
	}
	data := targetsCM(t, c).Data
	if _, ok := data["ns1_lab.json"]; ok {
		t.Errorf("ns1_lab.json survived its cleanup: %v", data)
	}
	if _, ok := data["ns2_lab.json"]; !ok {
		t.Errorf("cleaning up ns1/lab removed ns2/lab's targets: %v", data)
	}
}

// A file written before the namespaced key is "<name>.json". Its JSON has no
// namespace, so it goes unconditionally, in the same Update: both keys at
// once would list the same targets twice.
func TestTargetsMigrateTheLegacyKey(t *testing.T) {
	ctx := context.Background()
	legacy := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "monitoring", Name: "prometheus-targets"},
		Data:       map[string]string{"lab.json": "[]"},
	}
	m, c := targetsManager(t, legacy)
	env := targetsEnv("ns1", "lab", "10.0.0.1")
	if err := m.ReconcileTargets(ctx, env); err != nil {
		t.Fatal(err)
	}
	data := targetsCM(t, c).Data
	if _, ok := data["lab.json"]; ok {
		t.Errorf("legacy key kept after reconcile: %v", data)
	}
	if _, ok := data["ns1_lab.json"]; !ok {
		t.Errorf("namespaced key not written: %v", data)
	}

	cm := targetsCM(t, c)
	cm.Data["lab.json"] = "[]"
	if err := c.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if err := m.CleanupTargets(ctx, env); err != nil {
		t.Fatal(err)
	}
	if data := targetsCM(t, c).Data; len(data) != 0 {
		t.Errorf("cleanup left %v", data)
	}
}

// The Ready tick calls ReconcileTargets every minute: an unchanged file must
// not be rewritten.
func TestTargetsReconcileWritesNothingWhenCurrent(t *testing.T) {
	ctx := context.Background()
	m, c := targetsManager(t)
	env := targetsEnv("ns1", "lab", "10.0.0.1")
	if err := m.ReconcileTargets(ctx, env); err != nil {
		t.Fatal(err)
	}
	before := targetsCM(t, c).ResourceVersion
	if err := m.ReconcileTargets(ctx, env); err != nil {
		t.Fatal(err)
	}
	if after := targetsCM(t, c).ResourceVersion; after != before {
		t.Errorf("unchanged targets rewritten: resourceVersion %s -> %s", before, after)
	}
}
