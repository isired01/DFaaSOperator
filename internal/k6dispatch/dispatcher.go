/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package k6dispatch applies and observes k6.io/v1alpha1 TestRun resources on
// remote k3s clusters. Each k6-load-generator node in an Environment runs its
// own k3s + k6-operator and exposes its kubeconfig via a Secret in the
// management cluster. The Dispatcher reads those Secrets and talks directly
// to the remote API server — the management cluster's ServiceAccount does
// not need any RBAC on the remote TestRun resources.
package k6dispatch

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// remoteRequestTimeout caps each remote-cluster API request (dial + round
// trip). Without it a dead k6 node blocks on the OS-default TCP connect
// (~30s), stalling the single-threaded reconcile worker.
const remoteRequestTimeout = 10 * time.Second

// TestRunGVK is the GroupVersionKind of k6-operator's TestRun.
var TestRunGVK = schema.GroupVersionKind{
	Group:   "k6.io",
	Version: "v1alpha1",
	Kind:    "TestRun",
}

// Dispatcher applies k6 TestRuns to remote k3s clusters whose kubeconfigs are
// stored as Secrets in the management cluster.
type Dispatcher struct {
	// Local is the management-cluster client, used to read kubeconfig Secrets.
	Local client.Client
}

// remoteClient builds a new client.Client for the cluster whose kubeconfig is
// stored under key "kubeconfig" in the Secret named by secretRef.
func (d *Dispatcher) remoteClient(ctx context.Context, secretRef types.NamespacedName) (client.Client, error) {
	var sec corev1.Secret
	if err := d.Local.Get(ctx, secretRef, &sec); err != nil {
		return nil, fmt.Errorf("read kubeconfig secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
	}
	raw, ok := sec.Data["kubeconfig"]
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("secret %s/%s missing key \"kubeconfig\"", secretRef.Namespace, secretRef.Name)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	cfg.Timeout = remoteRequestTimeout
	return client.New(cfg, client.Options{})
}

// ApplyTestRun creates or updates a TestRun on the remote cluster. The caller
// is responsible for setting GVK + ObjectMeta + Spec on tr; this helper only
// dispatches.
func (d *Dispatcher) ApplyTestRun(ctx context.Context, secretRef types.NamespacedName,
	tr *unstructured.Unstructured) error {

	rc, err := d.remoteClient(ctx, secretRef)
	if err != nil {
		return err
	}
	tr.SetGroupVersionKind(TestRunGVK)
	if err := rc.Patch(ctx, tr, client.Apply, client.FieldOwner("dfaas-operator")); err != nil {
		return fmt.Errorf("apply remote TestRun: %w", err)
	}
	return nil
}

// GetTestRun fetches a remote TestRun for status polling.
func (d *Dispatcher) GetTestRun(ctx context.Context, secretRef, key types.NamespacedName) (*unstructured.Unstructured, error) {
	rc, err := d.remoteClient(ctx, secretRef)
	if err != nil {
		return nil, err
	}
	tr := &unstructured.Unstructured{}
	tr.SetGroupVersionKind(TestRunGVK)
	if err := rc.Get(ctx, key, tr); err != nil {
		return nil, err
	}
	return tr, nil
}

// DeleteTestRun removes a remote TestRun (best-effort cleanup).
func (d *Dispatcher) DeleteTestRun(ctx context.Context, secretRef, key types.NamespacedName) error {
	rc, err := d.remoteClient(ctx, secretRef)
	if err != nil {
		return err
	}
	tr := &unstructured.Unstructured{}
	tr.SetGroupVersionKind(TestRunGVK)
	tr.SetName(key.Name)
	tr.SetNamespace(key.Namespace)
	return client.IgnoreNotFound(rc.Delete(ctx, tr))
}

// ApplyConfigMap server-side-applies a ConfigMap on the remote cluster. Used
// to copy the per-node k6 script ConfigMap from the management cluster to the
// remote k3s where k6-operator runs and resolves spec.script.configMap.
func (d *Dispatcher) ApplyConfigMap(ctx context.Context, secretRef types.NamespacedName,
	name, namespace string, data map[string]string) error {

	rc, err := d.remoteClient(ctx, secretRef)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
	if err := rc.Patch(ctx, cm, client.Apply, client.FieldOwner("dfaas-operator")); err != nil {
		return fmt.Errorf("apply remote ConfigMap %s/%s: %w", namespace, name, err)
	}
	return nil
}

// StageOf reads .status.stage from an unstructured TestRun.
func StageOf(tr *unstructured.Unstructured) string {
	stage, found, err := unstructured.NestedString(tr.Object, "status", "stage")
	if err != nil || !found {
		return ""
	}
	return stage
}
