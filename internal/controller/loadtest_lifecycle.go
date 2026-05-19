/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

// startK6 dispatches one remote TestRun per PerNodeLoad entry on its matching
// k6-load-generator node's k3s cluster, then transitions LoadTest → Running.
func (r *LoadTestReconciler) startK6(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Build a lookup from NodeID → kubeconfig Secret name, sourced from
	// Environment.status.k6Nodes (populated by EnvironmentReconciler).
	k6Index := map[string]dfaasv1.K6NodeStatus{}
	for _, n := range env.Status.K6Nodes {
		k6Index[n.NodeID] = n
	}

	var refs []dfaasv1.TestRunRef
	for _, perNode := range lt.Spec.PerNodeLoad {
		k6Node, ok := k6Index[perNode.NodeID]
		if !ok {
			return r.failLoadTest(ctx, lt,
				fmt.Sprintf("nodeID %q in perNodeLoad is not a k6-load-generator on env %q",
					perNode.NodeID, env.Name))
		}
		if k6Node.KubeconfigSecret == "" {
			return r.failLoadTest(ctx, lt,
				fmt.Sprintf("k6 node %q has no kubeconfig Secret on env %q",
					perNode.NodeID, env.Name))
		}

		trName := fmt.Sprintf("%s-%s", lt.Name, sanitize(perNode.NodeID))
		tr := buildRemoteTestRun(trName, lt, perNode)

		secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
		remoteKey := types.NamespacedName{Name: trName, Namespace: "default"}

		// Mirror the script ConfigMap onto the remote k3s. k6-operator resolves
		// spec.script.configMap in its own cluster, so the CM must exist there.
		if err := r.mirrorScriptConfigMap(ctx, lt, perNode, secretRef); err != nil {
			logger.Error(err, "remote script CM mirror failed", "node", perNode.NodeID)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// Wipe stale TestRun from previous runs so we start with fresh status.
		if err := r.Dispatcher.DeleteTestRun(ctx, secretRef, remoteKey); err != nil {
			logger.Error(err, "remote TestRun cleanup failed", "node", perNode.NodeID)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		if _, err := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey); err == nil {
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}

		if err := r.Dispatcher.ApplyTestRun(ctx, secretRef, tr); err != nil {
			logger.Error(err, "remote TestRun apply failed", "node", perNode.NodeID)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		refs = append(refs, dfaasv1.TestRunRef{
			NodeID:    perNode.NodeID,
			Name:      trName,
			Namespace: "default", // remote namespace; TestRun is applied in default on the k6 k3s
		})
	}

	// Stamp StartTime + TestRunRefs, transition to Running.
	now := metav1.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		latest.Status.StartTime = &now
		latest.Status.TestRuns = refs
		latest.Status.Phase = dfaasv1.LoadTestRunning
		return r.Status().Update(ctx, latest)
	}); err != nil {
		return ctrl.Result{}, err
	}
	_ = r.setLoadTestCondition(ctx, lt, "Ready", metav1.ConditionFalse,
		"K6Running", fmt.Sprintf("dispatched %d remote TestRun(s)", len(refs)))
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// observeK6 polls every remote TestRun. When all have reached a terminal
// stage (finished/stopped) it transitions to Exporting; on any error it
// fails the LoadTest.
func (r *LoadTestReconciler) observeK6(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	k6Index := map[string]dfaasv1.K6NodeStatus{}
	for _, n := range env.Status.K6Nodes {
		k6Index[n.NodeID] = n
	}

	allDone := true
	var anyError bool
	updatedRefs := make([]dfaasv1.TestRunRef, len(lt.Status.TestRuns))
	copy(updatedRefs, lt.Status.TestRuns)

	for i, ref := range lt.Status.TestRuns {
		k6Node, ok := k6Index[ref.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			return r.failLoadTest(ctx, lt,
				fmt.Sprintf("k6 node %q no longer present on env", ref.NodeID))
		}
		secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
		remoteKey := types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}

		tr, err := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey)
		if err != nil {
			logger.Error(err, "remote TestRun fetch failed", "node", ref.NodeID, "name", ref.Name)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		stage := k6dispatch.StageOf(tr)
		updatedRefs[i].Phase = stage

		switch stage {
		case "finished", "stopped":
			// done
		case "error":
			anyError = true
		default:
			allDone = false
		}
	}

	// Persist updated phases (best-effort).
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		latest.Status.TestRuns = updatedRefs
		return r.Status().Update(ctx, latest)
	})

	if anyError {
		return r.failLoadTest(ctx, lt, "at least one remote TestRun reported an error")
	}
	if !allDone {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// All TestRuns done → stamp EndTime and move to Exporting.
	now := metav1.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		latest.Status.EndTime = &now
		latest.Status.Phase = dfaasv1.LoadTestExporting
		return r.Status().Update(ctx, latest)
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// runExporter creates the in-cluster Job that pulls metrics from Prometheus
// over [StartTime, EndTime] and uploads to Google Drive (or stdout).
func (r *LoadTestReconciler) runExporter(ctx context.Context,
	lt *dfaasv1.LoadTest, _ *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	jobName := ExporterJobName(lt)
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: lt.Namespace}, &job)

	if apierrors.IsNotFound(err) {
		logger.Info("creating exporter Job", "job", jobName)
		if lt.Status.StartTime == nil || lt.Status.EndTime == nil {
			return r.failLoadTest(ctx, lt, "missing StartTime/EndTime; cannot run exporter")
		}
		newJob, err := r.createExporterJob(lt, lt.Status.StartTime.Time, lt.Status.EndTime.Time)
		if err != nil {
			return r.failLoadTest(ctx, lt, fmt.Sprintf("build exporter job: %v", err))
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &dfaasv1.LoadTest{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
				return err
			}
			latest.Status.ExporterJob = jobName
			return r.Status().Update(ctx, latest)
		})
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if job.Status.Succeeded > 0 {
		logger.Info("exporter Job succeeded")
		_ = r.setLoadTestCondition(ctx, lt, "Ready", metav1.ConditionTrue,
			"ExportSucceeded", "metrics exported")
		return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestCompleted)
	}
	if job.Status.Failed > 0 {
		return r.failLoadTest(ctx, lt, "exporter Job failed")
	}
	logger.Info("exporter Job running")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// buildRemoteTestRun assembles an unstructured k6.io/v1alpha1 TestRun manifest
// for one PerNodeLoad entry. Applied to the matching k6 machine's k3s.
func buildRemoteTestRun(name string, lt *dfaasv1.LoadTest, perNode dfaasv1.PerNodeLoad) *unstructured.Unstructured {
	tr := &unstructured.Unstructured{}
	tr.SetGroupVersionKind(k6dispatch.TestRunGVK)
	tr.SetName(name)
	tr.SetNamespace("default")
	tr.SetLabels(map[string]string{
		"dfaas.io/loadtest-name": lt.Name,
		"dfaas.io/node-id":       perNode.NodeID,
	})
	_ = unstructured.SetNestedMap(tr.Object, map[string]interface{}{
		"parallelism": int64(1),
		"script": map[string]interface{}{
			"configMap": map[string]interface{}{
				"name": perNode.ScriptConfigMap.Name,
				"file": "script.js",
			},
		},
		// VUs and duration are commonly set inside the script, but we surface
		// them as annotations so the operator can carry them across to the
		// remote cluster if a TestRun-level field is needed in the future.
	}, "spec")
	tr.SetAnnotations(map[string]string{
		"dfaas.io/vus":      fmt.Sprintf("%d", perNode.VUs),
		"dfaas.io/duration": perNode.Duration,
	})
	return tr
}

// mirrorScriptConfigMap reads the management-cluster ConfigMap named by
// perNode.ScriptConfigMap and server-side-applies a copy of it on the remote
// k3s in the "default" namespace (where the dispatched TestRun lives). The
// remote ConfigMap keeps the same name so spec.script.configMap.name in the
// TestRun resolves correctly.
func (r *LoadTestReconciler) mirrorScriptConfigMap(ctx context.Context,
	lt *dfaasv1.LoadTest, perNode dfaasv1.PerNodeLoad,
	secretRef types.NamespacedName) error {

	var src corev1.ConfigMap
	srcKey := types.NamespacedName{Name: perNode.ScriptConfigMap.Name, Namespace: lt.Namespace}
	if err := r.Get(ctx, srcKey, &src); err != nil {
		return fmt.Errorf("read script ConfigMap %s/%s: %w", srcKey.Namespace, srcKey.Name, err)
	}
	if _, ok := src.Data["script.js"]; !ok {
		return fmt.Errorf("script ConfigMap %s/%s missing key \"script.js\"", srcKey.Namespace, srcKey.Name)
	}
	return r.Dispatcher.ApplyConfigMap(ctx, secretRef,
		perNode.ScriptConfigMap.Name, "default", src.Data)
}

// abortLoadTest performs the multi-cluster cascading abort: best-effort
// deletes every remote TestRun for lt across all k6-load-generator nodes
// listed in env.Status.K6Nodes, then marks the central CR terminal as
// Aborted with Conditions[Ready]=False reason=UserAborted.
//
// Target set = union of:
//   - lt.Status.TestRuns (authoritative for what was successfully dispatched)
//   - spec.PerNodeLoad with deterministic names (catches TestRuns that exist
//     remotely but were never stamped into status — e.g. startK6 errored
//     mid-loop before reaching the status update).
//
// Per-node errors are logged but do NOT abort the loop — every node is
// attempted on each tick. If any node errored we Requeue after 10s and
// retry the full set (idempotent via DeleteTestRun's IgnoreNotFound).
// Once ALL targets dispatch cleanly, phase + Condition are stamped.
func (r *LoadTestReconciler) abortLoadTest(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	k6Index := map[string]dfaasv1.K6NodeStatus{}
	for _, n := range env.Status.K6Nodes {
		k6Index[n.NodeID] = n
	}

	type target struct{ nodeID, secretName, trName, trNs string }
	targets := map[string]target{}
	for _, ref := range lt.Status.TestRuns {
		k6Node, ok := k6Index[ref.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			continue
		}
		targets[ref.NodeID+"|"+ref.Name] = target{
			nodeID: ref.NodeID, secretName: k6Node.KubeconfigSecret,
			trName: ref.Name, trNs: ref.Namespace,
		}
	}
	for _, perNode := range lt.Spec.PerNodeLoad {
		k6Node, ok := k6Index[perNode.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			continue
		}
		trName := fmt.Sprintf("%s-%s", lt.Name, sanitize(perNode.NodeID))
		targets[perNode.NodeID+"|"+trName] = target{
			nodeID: perNode.NodeID, secretName: k6Node.KubeconfigSecret,
			trName: trName, trNs: "default",
		}
	}

	var failed int
	for _, t := range targets {
		secretRef := types.NamespacedName{Name: t.secretName, Namespace: lt.Namespace}
		remoteKey := types.NamespacedName{Name: t.trName, Namespace: t.trNs}
		if err := r.Dispatcher.DeleteTestRun(ctx, secretRef, remoteKey); err != nil {
			logger.Error(err, "remote TestRun delete failed",
				"node", t.nodeID, "testRun", t.trName)
			failed++
			continue
		}
		logger.Info("remote TestRun aborted", "node", t.nodeID, "testRun", t.trName)
	}
	if failed > 0 {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	_ = r.setLoadTestCondition(ctx, lt, "Ready", metav1.ConditionFalse,
		"UserAborted",
		"The test was manually aborted from the UI. Remote worker resources have been reclaimed.")
	return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestAborted)
}

// sanitize lowercases and replaces non-DNS-1123 chars with "-", to make Names
// valid for k8s objects.
func sanitize(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}
