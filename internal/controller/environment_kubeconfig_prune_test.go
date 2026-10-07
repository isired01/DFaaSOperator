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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
)

func pruneScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := dfaasv1.AddToScheme(s); err != nil {
		t.Fatalf("add dfaasv1: %v", err)
	}
	return s
}

func kubeconfigSecret(name, envName, nodeID string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				ansible.LabelKubeconfigEnv:    envName,
				ansible.LabelKubeconfigNodeID: nodeID,
			},
		},
		Data: map[string][]byte{"kubeconfig": []byte("apiVersion: v1")},
	}
}

func secretExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), client.ObjectKey{Name: name, Namespace: "default"}, &corev1.Secret{})
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("get %s: %v", name, err)
	return false
}

// A node that leaves the k6-load-generator role leaves its kubeconfig Secret
// behind with credentials that the repave has already invalidated. The prune
// must delete exactly those, and nothing else: not Secrets for nodes still
// holding the role, not the unlabelled libp2p key Secret, not another
// environment's Secrets.
func TestPruneK6KubeconfigsRemovesOnlyFlippedNodes(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo", Namespace: "default", UID: "uid-1"},
		Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
			{NodeID: "gen-a", IPAddress: "10.0.0.1", Role: dfaasv1.RoleK6LoadGenerator, Username: "u", Password: "p"},
			// flipped away from k6-load-generator; its Secret must go
			{NodeID: "flipped-b", IPAddress: "10.0.0.2", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p"},
		}},
	}

	libp2p := &corev1.Secret{ // no labels at all — must be untouched
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo-libp2p-keys", Namespace: "default"},
		Data:       map[string][]byte{"gen-a": []byte("key")},
	}

	s := pruneScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		kubeconfigSecret("env-demo-gen-a-kubeconfig", "env-demo", "gen-a"),
		kubeconfigSecret("env-demo-flipped-b-kubeconfig", "env-demo", "flipped-b"),
		kubeconfigSecret("env-demo-removed-c-kubeconfig", "env-demo", "removed-c"),
		kubeconfigSecret("other-env-gen-z-kubeconfig", "other-env", "gen-z"),
		libp2p,
	).Build()
	r := &EnvironmentReconciler{Client: c, Scheme: s}

	keep := map[string]struct{}{"gen-a": {}}
	if err := r.pruneK6Kubeconfigs(context.Background(), env, keep); err != nil {
		t.Fatalf("pruneK6Kubeconfigs: %v", err)
	}

	if !secretExists(t, c, "env-demo-gen-a-kubeconfig") {
		t.Error("deleted the Secret of a node still holding the k6 role")
	}
	if secretExists(t, c, "env-demo-flipped-b-kubeconfig") {
		t.Error("flipped node's kubeconfig Secret was not deleted")
	}
	if secretExists(t, c, "env-demo-removed-c-kubeconfig") {
		t.Error("Secret for a node absent from spec was not deleted")
	}
	if !secretExists(t, c, "other-env-gen-z-kubeconfig") {
		t.Error("deleted another environment's kubeconfig Secret")
	}
	if !secretExists(t, c, "env-demo-libp2p-keys") {
		t.Error("deleted the unlabelled libp2p key Secret")
	}
}

// The finalizer path passes an empty keep set, which must drain every
// kubeconfig Secret for the environment. Covers Secrets created before the
// playbook started stamping ownerReferences, which Kubernetes GC cannot reach.
func TestPruneK6KubeconfigsEmptyKeepDrainsAll(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-demo", Namespace: "default", UID: "uid-1"},
	}

	s := pruneScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		kubeconfigSecret("env-demo-gen-a-kubeconfig", "env-demo", "gen-a"),
		kubeconfigSecret("env-demo-gen-b-kubeconfig", "env-demo", "gen-b"),
		kubeconfigSecret("other-env-gen-z-kubeconfig", "other-env", "gen-z"),
	).Build()
	r := &EnvironmentReconciler{Client: c, Scheme: s}

	if err := r.pruneK6Kubeconfigs(context.Background(), env, nil); err != nil {
		t.Fatalf("pruneK6Kubeconfigs: %v", err)
	}

	if secretExists(t, c, "env-demo-gen-a-kubeconfig") || secretExists(t, c, "env-demo-gen-b-kubeconfig") {
		t.Error("empty keep set did not drain all kubeconfig Secrets")
	}
	if !secretExists(t, c, "other-env-gen-z-kubeconfig") {
		t.Error("drained another environment's Secret")
	}
}
