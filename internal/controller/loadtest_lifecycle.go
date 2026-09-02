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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
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

// dispatchRetryBudget is the max number of consecutive dispatcher errors
// tolerated before failLoadTest fires with reason=DispatchFailed. With
// onDispatchError's 10s RequeueAfter and the dispatcher's 10s request cap
// (remoteRequestTimeout), ~15 attempts ≈ 5 min grace — enough for a briefly
// unreachable k6 node to come back before the LoadTest is failed.
const dispatchRetryBudget = 15

// bumpDispatchAttempts increments the counter annotation by one with conflict
// retry, returning the new value. The Update is on the object (not status),
// since annotations live in ObjectMeta.
func (r *LoadTestReconciler) bumpDispatchAttempts(ctx context.Context,
	lt *dfaasv1.LoadTest) (int, error) {
	var n int
	err := updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(lt), &dfaasv1.LoadTest{},
		func(latest *dfaasv1.LoadTest) bool {
			n = bumpPlainCounter(latest, dispatchAttemptsAnnotation)
			return true
		})
	return n, err
}

// resetDispatchAttempts zeroes the counter annotation. Safe to call when the
// annotation is absent — it will be created. No-op (cheap Get) if the value
// is already "0", to avoid pointless Updates on every successful dispatch.
func (r *LoadTestReconciler) resetDispatchAttempts(ctx context.Context,
	lt *dfaasv1.LoadTest) error {
	return updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(lt), &dfaasv1.LoadTest{},
		func(latest *dfaasv1.LoadTest) bool {
			return resetPlainCounter(latest, dispatchAttemptsAnnotation)
		})
}

// onDispatchError centralises the retry-budget bookkeeping on a dispatcher
// error path. Bumps the counter; if it tips the budget, transitions the
// LoadTest to Failed and stamps K6Dispatched=False/DispatchFailed. Otherwise
// stamps K6Dispatched=False/<subReason> (P13) and returns a RequeueAfter
// Result. fatal=true means the caller MUST stop.
func (r *LoadTestReconciler) onDispatchError(ctx context.Context,
	lt *dfaasv1.LoadTest, dispatchErr error, subReason string) (ctrl.Result, bool, error) {
	logger := log.FromContext(ctx)
	count, bumpErr := r.bumpDispatchAttempts(ctx, lt)
	if bumpErr != nil {
		logger.Error(bumpErr, "bumpDispatchAttempts failed; continuing without budget enforcement")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
	}
	if count >= dispatchRetryBudget {
		detail := fmt.Sprintf("remote dispatch failed %d consecutive times: %s",
			count, condMessage(dispatchErr))
		logStatusErr(ctx, "stamp K6Dispatched=False (dispatch failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Dispatched,
			metav1.ConditionFalse, dfaasv1.LTReasonDispatchFailed, detail))
		res, err := r.setLoadTestPhaseDetail(ctx, lt, dfaasv1.LoadTestFailed,
			dfaasv1.LTReasonDispatchFailed, detail)
		return res, true, err
	}
	// P13: sub-reason (ScriptMirrorFailed / StaleCleanupFailed / ApplyFailed)
	// carries the diagnostic detail, retry-budget counter is in the message.
	logStatusErr(ctx, "stamp K6Dispatched=False (retrying)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Dispatched,
		metav1.ConditionFalse, subReason,
		fmt.Sprintf("attempt %d/%d: %s",
			count, dispatchRetryBudget, condMessage(dispatchErr))))
	return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
}

// startK6 dispatches one remote TestRun per PerNodeLoad entry on its matching
// k6-load-generator node's k3s cluster, then transitions LoadTest → Running.
func (r *LoadTestReconciler) startK6(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	// Synchronized start needs a VM-facing GO URL before anything is
	// dispatched — fail loudly instead of parking every runner on a barrier
	// nobody can open (e.g. `make run` without DFAAS_SYNC_PUBLIC_URL).
	if lt.Spec.SyncStart && syncGoURL(lt) == "" {
		return r.failLoadTest(ctx, lt,
			"synchronized start: cannot resolve the VM-facing GO URL — set DFAAS_SYNC_PUBLIC_URL (or run in-cluster with HOST_IP injected)")
	}

	// P9: first observation — nothing dispatched yet, status is Unknown.
	// Subsequent calls below upgrade this to False/InFlight or True/
	// AllDispatched. SetStatusCondition is idempotent on no transition.
	if len(lt.Status.TestRuns) == 0 {
		logStatusErr(ctx, "stamp K6Dispatched=Unknown (pending)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Dispatched,
			metav1.ConditionUnknown, dfaasv1.LTReasonPending,
			"awaiting first remote TestRun apply"))
	}

	k6Index := computeK6NodeIndex(env)

	// Seed refs from existing Status.TestRuns so that a re-entry after a
	// partial-dispatch error resumes from where it left off. The per-node
	// guard below skips nodes already represented in this slice.
	refs, alreadyDispatched := resumePartialDispatch(lt)

	for _, perNode := range lt.Spec.PerNodeLoad {
		updatedRefs, stop, res, err := r.dispatchTestRunForNode(
			ctx, lt, env, perNode, k6Index, refs, alreadyDispatched)
		refs = updatedRefs
		if stop {
			return res, err
		}
	}

	// All TestRuns dispatched.
	logStatusErr(ctx, "stamp K6Dispatched=True (all dispatched)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Dispatched,
		metav1.ConditionTrue, dfaasv1.LTReasonAllDispatched,
		fmt.Sprintf("dispatched %d remote TestRun(s)", len(refs))))

	// Synchronized start: hold Running until every runner is parked on the
	// script barrier, then publish the GO signal (loadtest_sync.go).
	if lt.Spec.SyncStart {
		lt.Status.TestRuns = refs
		return r.awaitSyncBarrier(ctx, lt, env)
	}
	return r.finishDispatch(ctx, lt, refs)
}

// finishDispatch stamps StartTime + transitions to Running once traffic is
// (about to be) flowing: immediately after dispatch for plain tests, after
// the GO signal for syncStart tests — so StartTime tracks actual traffic
// start and the exporter's PROM window stays faithful.
func (r *LoadTestReconciler) finishDispatch(ctx context.Context,
	lt *dfaasv1.LoadTest, refs []dfaasv1.TestRunRef) (ctrl.Result, error) {
	logStatusErr(ctx, "stamp K6Healthy=Unknown (awaiting observation)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
		metav1.ConditionUnknown, dfaasv1.LTReasonRunning,
		"k6 TestRuns dispatched, awaiting observation"))
	now := metav1.Now()
	if err := r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
		latest.Status.StartTime = &now
		latest.Status.TestRuns = refs
		latest.Status.Phase = dfaasv1.LoadTestRunning
		stampLTAggregate(latest, dfaasv1.LoadTestRunning, "", "")
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// computeK6NodeIndex builds a lookup from NodeID → kubeconfig Secret name,
// sourced from Environment.status.k6Nodes (populated by EnvironmentReconciler).
func computeK6NodeIndex(env *dfaasv1.Environment) map[string]dfaasv1.K6NodeStatus {
	k6Index := map[string]dfaasv1.K6NodeStatus{}
	for _, n := range env.Status.K6Nodes {
		k6Index[n.NodeID] = n
	}
	return k6Index
}

// resolveK6Node looks one nodeID up in the Environment's k6 index and returns
// the node only when it is actually usable, i.e. still declared on the
// Environment AND carrying a kubeconfig Secret. Single policy for every caller:
// an unusable node is an error, never a silent skip. A node that vanished
// mid-test can neither be polled nor released, so the paths that need it to
// make progress (observeK6, awaitSyncBarrier) fail the LoadTest on the spot
// instead of waiting out a budget it can never satisfy, and the best-effort
// cleanup paths (teardownRemoteTestRuns, captureK6Logs) log the same error
// before moving on — counting it as a failure there would wedge the abort loop
// on a node that is unreachable by definition.
func resolveK6Node(k6Index map[string]dfaasv1.K6NodeStatus, nodeID string) (dfaasv1.K6NodeStatus, error) {
	node, ok := k6Index[nodeID]
	if !ok {
		return dfaasv1.K6NodeStatus{}, fmt.Errorf("k6 node %q is no longer part of the environment", nodeID)
	}
	if node.KubeconfigSecret == "" {
		return dfaasv1.K6NodeStatus{}, fmt.Errorf("k6 node %q has no kubeconfig Secret on the environment", nodeID)
	}
	return node, nil
}

// resumePartialDispatch seeds the dispatch state from existing
// Status.TestRuns so a re-entry after a partial-dispatch error resumes from
// where it left off. The returned refs slice is a copy of the persisted runs;
// alreadyDispatched keys each run as "<nodeID>|<name>" so the per-node guard
// can skip nodes that were already dispatched.
func resumePartialDispatch(lt *dfaasv1.LoadTest) ([]dfaasv1.TestRunRef, map[string]bool) {
	refs := make([]dfaasv1.TestRunRef, len(lt.Status.TestRuns))
	copy(refs, lt.Status.TestRuns)
	alreadyDispatched := map[string]bool{}
	for _, ref := range refs {
		alreadyDispatched[ref.NodeID+"|"+ref.Name] = true
	}
	return refs, alreadyDispatched
}

// dispatchTestRunForNode dispatches the remote TestRun for a single
// PerNodeLoad entry. It returns the (possibly extended) refs slice, a stop
// flag, and the Result/error the caller must return when stop is true. When
// stop is false the caller continues to the next PerNodeLoad entry. The
// alreadyDispatched map is mutated in place. Same control flow and error
// handling as the original inline loop body in startK6.
func (r *LoadTestReconciler) dispatchTestRunForNode(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment, perNode dfaasv1.PerNodeLoad,
	k6Index map[string]dfaasv1.K6NodeStatus, refs []dfaasv1.TestRunRef,
	alreadyDispatched map[string]bool) (updatedRefs []dfaasv1.TestRunRef, stop bool, res ctrl.Result, err error) {
	logger := log.FromContext(ctx)

	k6Node, ok := k6Index[perNode.NodeID]
	if !ok {
		res, ferr := r.failLoadTest(ctx, lt,
			fmt.Sprintf("nodeID %q in perNodeLoad is not a k6-load-generator on env %q",
				perNode.NodeID, env.Name))
		return refs, true, res, ferr
	}
	if k6Node.KubeconfigSecret == "" {
		res, ferr := r.failLoadTest(ctx, lt,
			fmt.Sprintf("k6 node %q has no kubeconfig Secret on env %q",
				perNode.NodeID, env.Name))
		return refs, true, res, ferr
	}

	trName := fmt.Sprintf("%s-%s", lt.Name, sanitize(perNode.NodeID))

	// Per-node guard: if Status.TestRuns already has {NodeID, Name} for
	// this entry, the remote TestRun is live and we must NOT re-delete
	// and re-apply it (that would yank a running k6 test off the
	// worker). Skip straight to the next entry.
	if alreadyDispatched[perNode.NodeID+"|"+trName] {
		return refs, false, ctrl.Result{}, nil
	}

	tr, berr := buildRemoteTestRun(trName, lt, perNode)
	if berr != nil {
		res, ferr := r.failLoadTest(ctx, lt,
			fmt.Sprintf("build remote TestRun for node %q: %v", perNode.NodeID, berr))
		return refs, true, res, ferr
	}
	secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
	remoteKey := types.NamespacedName{Name: trName, Namespace: "default"}

	// Mirror the script ConfigMap onto the remote k3s. k6-operator resolves
	// spec.script.configMap in its own cluster, so the CM must exist there.
	if merr := r.mirrorScriptConfigMap(ctx, lt, perNode, secretRef); merr != nil {
		logger.Error(merr, "remote script CM mirror failed", "node", perNode.NodeID)
		res, _, oerr := r.onDispatchError(ctx, lt, merr, dfaasv1.LTReasonScriptMirrorFailed)
		return refs, true, res, oerr
	}

	// Wipe stale TestRun from previous runs so we start with fresh status.
	if derr := r.Dispatcher.DeleteTestRun(ctx, secretRef, remoteKey); derr != nil {
		logger.Error(derr, "remote TestRun cleanup failed", "node", perNode.NodeID)
		res, _, oerr := r.onDispatchError(ctx, lt, derr, dfaasv1.LTReasonStaleCleanupFailed)
		return refs, true, res, oerr
	}
	_, gerr := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey)
	switch {
	case gerr == nil:
		// Delete still propagating on the remote — wait a tick then retry.
		return refs, true, ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	case !apierrors.IsNotFound(gerr):
		// Only NotFound proves the stale TestRun is gone. An unreachable node
		// or an unparsable kubeconfig says nothing about the remote state, so
		// applying on top of it would risk racing a still-running k6 test:
		// route it through the retry budget instead.
		logger.Error(gerr, "remote TestRun delete-confirm poll failed", "node", perNode.NodeID)
		res, _, oerr := r.onDispatchError(ctx, lt, gerr, dfaasv1.LTReasonStaleCleanupFailed)
		return refs, true, res, oerr
	}

	if aerr := r.Dispatcher.ApplyTestRun(ctx, secretRef, tr); aerr != nil {
		logger.Error(aerr, "remote TestRun apply failed", "node", perNode.NodeID)
		// Persist any partial refs accumulated so far so the next
		// reconcile resumes from this exact node rather than re-applying
		// already-running TestRuns.
		if perr := r.persistTestRuns(ctx, lt, refs); perr != nil {
			logger.Error(perr, "persist partial TestRuns failed")
		}
		res, _, oerr := r.onDispatchError(ctx, lt, aerr, dfaasv1.LTReasonApplyFailed)
		return refs, true, res, oerr
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
	if perr := r.persistTestRuns(ctx, lt, refs); perr != nil {
		// Failed to persist — back off without losing the dispatch (it's
		// on the remote already). Next reconcile will re-encounter the
		// same TestRun and the guard above (if status caught up) or the
		// delete-then-apply path will reconcile it.
		logger.Error(perr, "persist TestRuns after apply failed")
		return refs, true, ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	// Successful dispatcher round-trip — reset the budget counter and
	// surface partial progress on K6Dispatched (P9).
	if rerr := r.resetDispatchAttempts(ctx, lt); rerr != nil {
		logger.Error(rerr, "resetDispatchAttempts failed; non-fatal")
	}
	if len(refs) < len(lt.Spec.PerNodeLoad) {
		logStatusErr(ctx, "stamp K6Dispatched=False (in flight)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Dispatched,
			metav1.ConditionFalse, dfaasv1.LTReasonInFlight,
			fmt.Sprintf("%d/%d TestRun(s) dispatched",
				len(refs), len(lt.Spec.PerNodeLoad))))
	}
	return refs, false, ctrl.Result{}, nil
}

// persistTestRuns writes the cumulative TestRunRef slice into
// Status.TestRuns with conflict retry. Called incrementally from startK6
// after each successful ApplyTestRun so a mid-loop failure leaves the
// already-dispatched runs visible to observeK6 / abort / deletion paths.
func (r *LoadTestReconciler) persistTestRuns(ctx context.Context,
	lt *dfaasv1.LoadTest, refs []dfaasv1.TestRunRef) error {
	return r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
		latest.Status.TestRuns = refs
		return nil
	})
}

// ensureMirroredS3Secret copies the S3 config Secret from S3ConfigNamespace
// into the LoadTest's namespace under the same name. The mirror is a SHARED
// same-namespace cache: any LoadTest, against any Environment, may consume
// it. No controller OwnerRef is set — Kubernetes' single-Controller invariant
// would otherwise pin the mirror to the first LT and block every subsequent
// consumer with AlreadyOwnedError.
//
// Lifecycle is operator-out-of-band: orphans are identified by the
// dfaas.io/s3-config=true label and may be cleaned by a future GC job or
// `kubectl delete secret`. Storage cost is negligible (credentials < 1 KB).
//
// Idempotent: re-runs refresh data + label in place. apierrors.IsNotFound on
// the source is the caller-handled "S3ConfigMissing" path.
func (r *LoadTestReconciler) ensureMirroredS3Secret(ctx context.Context,
	lt *dfaasv1.LoadTest, configName string) (string, error) {

	var src corev1.Secret
	srcKey := types.NamespacedName{Name: configName, Namespace: S3ConfigNamespace}
	if err := r.Get(ctx, srcKey, &src); err != nil {
		return "", err
	}

	mirror := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configName,
			Namespace: lt.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, mirror, func() error {
		if mirror.Labels == nil {
			mirror.Labels = map[string]string{}
		}
		mirror.Labels[S3ConfigLabel] = "true"
		mirror.Type = src.Type
		mirror.Data = make(map[string][]byte, len(src.Data))
		for k, v := range src.Data {
			mirror.Data[k] = v
		}
		// Strip any legacy controller OwnerRef from a prior LT/Env-scoped
		// mirror. The shared model has no controller; leaving stale refs
		// would cascade-delete the mirror when the legacy owner is GC'd.
		mirror.OwnerReferences = nil
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("mirror S3 config %q: %w", configName, err)
	}
	return mirror.Name, nil
}

// buildRemoteTestRun assembles an unstructured k6.io/v1alpha1 TestRun manifest
// for one PerNodeLoad entry. Applied to the matching k6 machine's k3s. Returns
// an error if the spec map cannot be nested (a dispatched TestRun with an empty
// spec would start no runner at all).
func buildRemoteTestRun(name string, lt *dfaasv1.LoadTest, perNode dfaasv1.PerNodeLoad) (*unstructured.Unstructured, error) {
	tr := &unstructured.Unstructured{}
	tr.SetGroupVersionKind(k6dispatch.TestRunGVK)
	tr.SetName(name)
	tr.SetNamespace("default")
	tr.SetLabels(map[string]string{
		"dfaas.io/loadtest-name": lt.Name,
		"dfaas.io/node-id":       perNode.NodeID,
	})
	spec := map[string]interface{}{
		"parallelism": int64(1),
		"script": map[string]interface{}{
			"configMap": map[string]interface{}{
				"name": perNode.ScriptConfigMap.Name,
				"file": "script.js",
			},
		},
		// VUs and duration deliberately do not appear here: k6 takes both from
		// options.scenarios inside the script, and we set no TestRun field that
		// could carry them. They used to be mirrored onto annotations "in case a
		// TestRun-level field is needed in the future" — nothing ever read them,
		// so they are gone. spec.perNodeLoad on the LoadTest CR still records
		// both, which is where the UI reads them from.
	}
	// The generated script's handleSummary() PUTs its end-of-test summary
	// JSON here (see loadtest_sync.go). Always injected: empty value (public
	// base unresolvable) or scripts without handleSummary just skip it.
	runnerEnv := []interface{}{
		map[string]interface{}{"name": "DFAAS_SUMMARY_URL", "value": summaryURL(lt, perNode.NodeID)},
	}
	if lt.Spec.SyncStart {
		// The generated script's setup() blocks polling this URL until the
		// reconciler publishes the GO signal (see loadtest_sync.go). Scripts
		// without the barrier simply ignore the env var.
		runnerEnv = append(runnerEnv,
			map[string]interface{}{"name": "DFAAS_SYNC_URL", "value": syncGoURL(lt)})
	}
	spec["runner"] = map[string]interface{}{"env": runnerEnv}
	if err := unstructured.SetNestedMap(tr.Object, spec, "spec"); err != nil {
		return nil, fmt.Errorf("set TestRun spec: %w", err)
	}
	return tr, nil
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
