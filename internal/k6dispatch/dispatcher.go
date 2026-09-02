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
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
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

// restConfig reads the kubeconfig Secret named by secretRef (key "kubeconfig")
// and parses it into a *rest.Config with the remote-request timeout applied.
// Shared by remoteClient (controller-runtime client) and GetK6RunnerLogs
// (typed clientset) so the Secret-read + parse + timeout logic lives in one
// place.
func (d *Dispatcher) restConfig(ctx context.Context, secretRef types.NamespacedName) (*rest.Config, error) {
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
	return cfg, nil
}

// remoteClient builds a new client.Client for the cluster whose kubeconfig is
// stored under key "kubeconfig" in the Secret named by secretRef.
func (d *Dispatcher) remoteClient(ctx context.Context, secretRef types.NamespacedName) (client.Client, error) {
	cfg, err := d.restConfig(ctx, secretRef)
	if err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{})
}

// ApplyTestRun creates or updates a TestRun on the remote cluster. The caller
// provides ObjectMeta + Spec on tr; the GVK is (re-)stamped here, so callers
// need not set it themselves.
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

// k6LogLimitBytes caps the per-pod log read so a runaway runner log cannot
// blow up the operator's memory or the downstream ConfigMap (1 MiB etcd
// limit). 256 KiB comfortably covers a k6 end-of-test summary.
const k6LogLimitBytes int64 = 262144

// GetK6RunnerLogs streams the k6 end-of-test summary from the runner Pod(s)
// of the named TestRun on the remote cluster. k6-operator labels runner Pods
// app=k6,k6_cr=<testRunName>,runner=true and does not set cleanup=post, so
// finished Pods (and their logs) are retained for post-test extraction.
//
// The container name is "k6"; logs are capped at k6LogLimitBytes per Pod.
// When more than one runner Pod matches (parallelism > 1) each block is
// prefixed with a "==== pod <name> ====" header. Returns a clear error when
// no runner Pod is found.
func (d *Dispatcher) GetK6RunnerLogs(ctx context.Context, secretRef types.NamespacedName,
	testRunName, namespace string) (string, error) {

	cfg, err := d.restConfig(ctx, secretRef)
	if err != nil {
		return "", err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("build remote clientset: %w", err)
	}

	selector := fmt.Sprintf("app=k6,k6_cr=%s,runner=true", testRunName)
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("list k6 runner pods (%s): %w", selector, err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no k6 runner pod found for TestRun %q in namespace %q (selector %s)",
			testRunName, namespace, selector)
	}

	multi := len(pods.Items) > 1
	var b strings.Builder
	for _, pod := range pods.Items {
		logs, lerr := readPodLogs(ctx, clientset, namespace, pod.Name)
		if lerr != nil {
			return "", fmt.Errorf("stream logs for pod %s/%s: %w", namespace, pod.Name, lerr)
		}
		if multi {
			fmt.Fprintf(&b, "==== pod %s ====\n", pod.Name)
		}
		b.WriteString(logs)
		if multi && !strings.HasSuffix(logs, "\n") {
			b.WriteByte('\n')
		}
	}
	return b.String(), nil
}

// readPodLogs streams the "k6" container log of a single Pod, capped at
// k6LogLimitBytes, and returns the full content as a string.
func readPodLogs(ctx context.Context, clientset kubernetes.Interface,
	namespace, podName string) (string, error) {

	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  "k6",
		LimitBytes: ptr.To(k6LogLimitBytes),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	return string(data), nil
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
