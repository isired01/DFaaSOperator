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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	reachfake "dfaas-operator/internal/reach/fake"
)

// A settled Ready Environment re-asserts its target file: the write at the
// end of provisioning is best-effort and never retried, and a legacy
// "<name>.json" key from before the namespaced key must migrate without
// waiting for a spec edit.
func TestReadyEnvironmentMigratesItsLegacyTargetFile(t *testing.T) {
	s := faninScheme(t)
	env := faninEnv(false)
	env.Generation = 2
	env.Status.Phase = dfaasv1.EnvReady
	env.Status.ObservedGeneration = 2
	env.Finalizers = []string{environmentFinalizer}
	legacy := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "monitoring", Name: "prometheus-targets"},
		Data:       map[string]string{env.Name + ".json": "[]"},
	}
	c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env, legacy).Build()
	r := &EnvironmentReconciler{Client: c, Scheme: s, Prober: &reachfake.Nodes{}}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(legacy), cm); err != nil {
		t.Fatal(err)
	}
	if _, ok := cm.Data[env.Name+".json"]; ok {
		t.Errorf("legacy key still there: %v", cm.Data)
	}
	if _, ok := cm.Data[env.Namespace+"_"+env.Name+".json"]; !ok {
		t.Errorf("namespaced key not written: %v", cm.Data)
	}
}
