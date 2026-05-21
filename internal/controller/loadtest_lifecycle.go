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
	"strconv"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

// dispatchAttemptsAnnotation persists the consecutive-error counter for
// remote-dispatcher operations across reconciles. Annotation lives on the
// LoadTest itself so it survives operator restarts without spec/status shape
// changes.
const dispatchAttemptsAnnotation = "dfaas.dfaas.io/dispatch-attempts"

// dispatchRetryBudget is the max number of consecutive identical dispatcher
// errors tolerated before failLoadTest fires with reason=DispatchFailed.
const dispatchRetryBudget = 5

// bumpDispatchAttempts increments the counter annotation by one with conflict
// retry, returning the new value. The Update is on the object (not status),
// since annotations live in ObjectMeta.
func (r *LoadTestReconciler) bumpDispatchAttempts(ctx context.Context,
	lt *dfaasv1.LoadTest) (int, error) {
	var newVal int
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		cur := 0
		if s, ok := latest.Annotations[dispatchAttemptsAnnotation]; ok {
			if n, perr := strconv.Atoi(s); perr == nil {
				cur = n
			}
		}
		cur++
		latest.Annotations[dispatchAttemptsAnnotation] = strconv.Itoa(cur)
		newVal = cur
		return r.Update(ctx, latest)
	})
	return newVal, err
}

// resetDispatchAttempts zeroes the counter annotation. Safe to call when the
// annotation is absent — it will be created. No-op (cheap Get) if the value
// is already "0", to avoid pointless Updates on every successful dispatch.
func (r *LoadTestReconciler) resetDispatchAttempts(ctx context.Context,
	lt *dfaasv1.LoadTest) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		if cur, ok := latest.Annotations[dispatchAttemptsAnnotation]; ok && cur == "0" {
			return nil
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[dispatchAttemptsAnnotation] = "0"
		return r.Update(ctx, latest)
	})
}

// onDispatchError centralises the retry-budget bookkeeping on a dispatcher
// error path. Bumps the counter; if it tips the budget, returns a Result that
// transitions the LoadTest to Failed via failLoadTest (reason=DispatchFailed).
// Otherwise returns a RequeueAfter Result and the caller short-circuits.
// fatal=true means the caller MUST stop (counter tripped or failLoadTest ran).
func (r *LoadTestReconciler) onDispatchError(ctx context.Context,
	lt *dfaasv1.LoadTest, dispatchErr error) (ctrl.Result, bool, error) {
	logger := log.FromContext(ctx)
	count, bumpErr := r.bumpDispatchAttempts(ctx, lt)
	if bumpErr != nil {
		logger.Error(bumpErr, "bumpDispatchAttempts failed; continuing without budget enforcement")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
	}
	if count >= dispatchRetryBudget {
		// Stamp the budget-trip reason explicitly before transitioning to
		// Failed so the UI sees reason=DispatchFailed (failLoadTest itself
		// stamps reason=Failed which is too generic for this path).
		_ = r.setLoadTestCondition(ctx, lt, "Ready", metav1.ConditionFalse,
			"DispatchFailed",
			fmt.Sprintf("remote dispatch failed %d consecutive times: %v", count, dispatchErr))
		res, err := r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestFailed)
		return res, true, err
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
}

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

	// Seed refs from existing Status.TestRuns so that a re-entry after a
	// partial-dispatch error resumes from where it left off. The per-node
	// guard below skips nodes already represented in this slice.
	refs := make([]dfaasv1.TestRunRef, len(lt.Status.TestRuns))
	copy(refs, lt.Status.TestRuns)
	alreadyDispatched := map[string]bool{}
	for _, ref := range refs {
		alreadyDispatched[ref.NodeID+"|"+ref.Name] = true
	}

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

		// Per-node guard: if Status.TestRuns already has {NodeID, Name} for
		// this entry, the remote TestRun is live and we must NOT re-delete
		// and re-apply it (that would yank a running k6 test off the
		// worker). Skip straight to the next entry.
		if alreadyDispatched[perNode.NodeID+"|"+trName] {
			continue
		}

		tr := buildRemoteTestRun(trName, lt, perNode)
		secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
		remoteKey := types.NamespacedName{Name: trName, Namespace: "default"}

		// Mirror the script ConfigMap onto the remote k3s. k6-operator resolves
		// spec.script.configMap in its own cluster, so the CM must exist there.
		if err := r.mirrorScriptConfigMap(ctx, lt, perNode, secretRef); err != nil {
			logger.Error(err, "remote script CM mirror failed", "node", perNode.NodeID)
			res, _, oerr := r.onDispatchError(ctx, lt, err)
			return res, oerr
		}

		// Wipe stale TestRun from previous runs so we start with fresh status.
		if err := r.Dispatcher.DeleteTestRun(ctx, secretRef, remoteKey); err != nil {
			logger.Error(err, "remote TestRun cleanup failed", "node", perNode.NodeID)
			res, _, oerr := r.onDispatchError(ctx, lt, err)
			return res, oerr
		}
		if _, err := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey); err == nil {
			// Delete still propagating on the remote — wait a tick then retry.
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}

		if err := r.Dispatcher.ApplyTestRun(ctx, secretRef, tr); err != nil {
			logger.Error(err, "remote TestRun apply failed", "node", perNode.NodeID)
			// Persist any partial refs accumulated so far so the next
			// reconcile resumes from this exact node rather than re-applying
			// already-running TestRuns.
			if perr := r.persistTestRuns(ctx, lt, refs); perr != nil {
				logger.Error(perr, "persist partial TestRuns failed")
			}
			res, _, oerr := r.onDispatchError(ctx, lt, err)
			return res, oerr
		}

		// Apply succeeded — append and persist incrementally so a failure
		// later in the loop leaves the already-dispatched runs visible to
		// observeK6 / abortLoadTest / the deletion finalizer.
		refs = append(refs, dfaasv1.TestRunRef{
			NodeID:    perNode.NodeID,
			Name:      trName,
			Namespace: "default", // remote namespace; TestRun is applied in default on the k6 k3s
		})
		alreadyDispatched[perNode.NodeID+"|"+trName] = true
		if err := r.persistTestRuns(ctx, lt, refs); err != nil {
			// Failed to persist — back off without losing the dispatch (it's
			// on the remote already). Next reconcile will re-encounter the
			// same TestRun and the guard above (if status caught up) or the
			// delete-then-apply path will reconcile it.
			logger.Error(err, "persist TestRuns after apply failed")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Successful dispatcher round-trip — reset the budget counter.
		if err := r.resetDispatchAttempts(ctx, lt); err != nil {
			logger.Error(err, "resetDispatchAttempts failed; non-fatal")
		}
	}

	// All TestRuns dispatched — stamp StartTime + transition to Running.
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

// persistTestRuns writes the cumulative TestRunRef slice into
// Status.TestRuns with conflict retry. Called incrementally from startK6
// after each successful ApplyTestRun so a mid-loop failure leaves the
// already-dispatched runs visible to observeK6 / abort / deletion paths.
func (r *LoadTestReconciler) persistTestRuns(ctx context.Context,
	lt *dfaasv1.LoadTest, refs []dfaasv1.TestRunRef) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		latest.Status.TestRuns = refs
		return r.Status().Update(ctx, latest)
	})
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
			res, _, oerr := r.onDispatchError(ctx, lt, err)
			return res, oerr
		}
		// Successful dispatcher round-trip — reset the budget counter.
		if rerr := r.resetDispatchAttempts(ctx, lt); rerr != nil {
			logger.Error(rerr, "resetDispatchAttempts failed; non-fatal")
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
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment,
	reason, message string) (ctrl.Result, error) {
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
		reason, message)
	return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestAborted)
}

// handleLoadTestDeletion is the finalizer flow triggered by a non-zero
// DeletionTimestamp. It guarantees remote TestRun cleanup before allowing
// K8s GC to remove the central CR — closes the "kubectl delete loadtest
// orphans remote runs" gap.
//
// Steps:
//  1. If our finalizer is absent, nothing to do.
//  2. Fetch the parent Environment. NotFound → remote kubeconfig Secrets
//     are gone with it, no way to delete remote TestRuns. Just remove the
//     finalizer and let the CR go.
//  3. Drive abortLoadTest (reason=UserAborted) to issue DeleteTestRun
//     against every node in spec ∪ status. abortLoadTest itself transitions
//     phase to Aborted on the first clean sweep; we don't care about that
//     terminal state here — we care that DeleteTestRun was issued.
//  4. Poll: GetTestRun against every entry in spec ∪ status. When all
//     report NotFound, remove the finalizer and return. Until then,
//     RequeueAfter 3s.
func (r *LoadTestReconciler) handleLoadTestDeletion(ctx context.Context,
	lt *dfaasv1.LoadTest) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(lt, loadTestFinalizer) {
		return ctrl.Result{}, nil
	}

	// Look up the parent Environment (same pattern as Reconcile body).
	var env dfaasv1.Environment
	envKey := types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}
	err := r.Get(ctx, envKey, &env)
	if apierrors.IsNotFound(err) {
		// Environment already gone — kubeconfig Secrets cascade with it.
		// Best-effort done; release the CR.
		logger.Info("environment gone during loadtest deletion — releasing finalizer",
			"env", lt.Spec.TargetEnvironment)
		controllerutil.RemoveFinalizer(lt, loadTestFinalizer)
		return ctrl.Result{}, r.Update(ctx, lt)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// Issue remote DeleteTestRun across the target set. abortLoadTest is
	// idempotent: DeleteTestRun uses IgnoreNotFound under the hood, and the
	// helper transitions phase → Aborted as a side effect. We ignore the
	// returned Result and run our own NotFound poll below.
	if _, abortErr := r.abortLoadTest(ctx, lt, &env, "UserAborted",
		"LoadTest aborted on user delete"); abortErr != nil {
		logger.Error(abortErr, "abortLoadTest during deletion failed; will retry")
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	// Poll: every TestRun in spec ∪ status must report NotFound on the
	// remote cluster before we drop the finalizer. Mirrors the abort target
	// set construction so a partial-dispatch ride-along is also covered.
	k6Index := map[string]dfaasv1.K6NodeStatus{}
	for _, n := range env.Status.K6Nodes {
		k6Index[n.NodeID] = n
	}
	type pollTarget struct{ secretName, trName, trNs string }
	targets := map[string]pollTarget{}
	for _, ref := range lt.Status.TestRuns {
		k6Node, ok := k6Index[ref.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			continue
		}
		targets[ref.NodeID+"|"+ref.Name] = pollTarget{
			secretName: k6Node.KubeconfigSecret,
			trName:     ref.Name,
			trNs:       ref.Namespace,
		}
	}
	for _, perNode := range lt.Spec.PerNodeLoad {
		k6Node, ok := k6Index[perNode.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			continue
		}
		trName := fmt.Sprintf("%s-%s", lt.Name, sanitize(perNode.NodeID))
		targets[perNode.NodeID+"|"+trName] = pollTarget{
			secretName: k6Node.KubeconfigSecret,
			trName:     trName,
			trNs:       "default",
		}
	}

	for _, t := range targets {
		secretRef := types.NamespacedName{Name: t.secretName, Namespace: lt.Namespace}
		remoteKey := types.NamespacedName{Name: t.trName, Namespace: t.trNs}
		_, err := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey)
		if err == nil {
			// Still present — keep polling.
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		if !apierrors.IsNotFound(err) {
			// Transient (kubeconfig parse, network, etc). Keep polling rather
			// than wedge the CR; the finalizer guarantees we revisit.
			logger.Error(err, "remote TestRun NotFound-poll errored",
				"node", t.trName)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
	}

	// Every target NotFound on its remote — safe to release.
	logger.Info("all remote TestRuns reclaimed — removing loadtest finalizer")
	controllerutil.RemoveFinalizer(lt, loadTestFinalizer)
	return ctrl.Result{}, r.Update(ctx, lt)
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
