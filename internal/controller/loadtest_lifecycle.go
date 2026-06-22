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
		logStatusErr(ctx, "stamp K6Dispatched=False (dispatch failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Dispatched,
			metav1.ConditionFalse, dfaasv1.LTReasonDispatchFailed,
			fmt.Sprintf("remote dispatch failed %d consecutive times: %s",
				count, condMessage(dispatchErr))))
		res, err := r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestFailed)
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

	// All TestRuns dispatched — stamp StartTime + transition to Running.
	logStatusErr(ctx, "stamp K6Dispatched=True (all dispatched)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Dispatched,
		metav1.ConditionTrue, dfaasv1.LTReasonAllDispatched,
		fmt.Sprintf("dispatched %d remote TestRun(s)", len(refs))))
	logStatusErr(ctx, "stamp K6Healthy=Unknown (awaiting observation)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
		metav1.ConditionUnknown, dfaasv1.LTReasonRunning,
		"k6 TestRuns dispatched, awaiting observation"))
	now := metav1.Now()
	if err := r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
		latest.Status.StartTime = &now
		latest.Status.TestRuns = refs
		latest.Status.Phase = dfaasv1.LoadTestRunning
		stampLTAggregate(latest, dfaasv1.LoadTestRunning)
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

	tr := buildRemoteTestRun(trName, lt, perNode)
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
	if _, gerr := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey); gerr == nil {
		// Delete still propagating on the remote — wait a tick then retry.
		return refs, true, ctrl.Result{RequeueAfter: 3 * time.Second}, nil
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

// observeK6 polls every remote TestRun. When all have reached a terminal
// stage (finished/stopped) it transitions to Exporting; on any error it
// fails the LoadTest.
func (r *LoadTestReconciler) observeK6(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	k6Index := computeK6NodeIndex(env)

	allDone := true
	var errorCount, finishedCount int
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
			res, _, oerr := r.onDispatchError(ctx, lt, err, dfaasv1.LTReasonApplyFailed)
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
			finishedCount++
		case "error":
			errorCount++
		default:
			allDone = false
		}
	}

	// Persist updated phases (best-effort).
	logStatusErr(ctx, "persist updated TestRun phases", r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
		latest.Status.TestRuns = updatedRefs
		return nil
	}))

	total := len(lt.Status.TestRuns)
	runningCount := total - finishedCount - errorCount

	// P9: K6Healthy rollup with a per-node count in the message.
	if !allDone {
		logStatusErr(ctx, "stamp K6Healthy=Unknown (running)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionUnknown, dfaasv1.LTReasonRunning,
			fmt.Sprintf("%d/%d finished, %d error, %d running",
				finishedCount, total, errorCount, runningCount)))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	switch {
	case errorCount == 0:
		logStatusErr(ctx, "stamp K6Healthy=True (all finished)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionTrue, dfaasv1.LTReasonAllFinished,
			fmt.Sprintf("%d/%d TestRun(s) finished cleanly", finishedCount, total)))
	case finishedCount == 0:
		logStatusErr(ctx, "stamp K6Healthy=False (all failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionFalse, dfaasv1.LTReasonAllFailed,
			fmt.Sprintf("%d/%d TestRun(s) reported error", errorCount, total)))
		return r.failLoadTest(ctx, lt,
			fmt.Sprintf("all %d remote TestRuns reported error stage", errorCount))
	default:
		logStatusErr(ctx, "stamp K6Healthy=False (partial failure)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionFalse, dfaasv1.LTReasonPartialFailure,
			fmt.Sprintf("%d finished, %d error (of %d)",
				finishedCount, errorCount, total)))
		return r.failLoadTest(ctx, lt,
			fmt.Sprintf("%d of %d remote TestRuns reported error stage", errorCount, total))
	}

	// Capture each VM's k6 end-of-test summary into per-node ConfigMaps
	// before transitioning to Exporting. Best-effort: capture failures never
	// block the phase transition.
	r.captureK6Logs(ctx, lt, env)

	// All TestRuns done cleanly → stamp EndTime and move to Exporting.
	now := metav1.Now()
	if err := r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
		latest.Status.EndTime = &now
		latest.Status.Phase = dfaasv1.LoadTestExporting
		stampLTAggregate(latest, dfaasv1.LoadTestExporting)
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// captureK6Logs reads each k6-load-generator VM's k6 runner-Pod logs (the
// end-of-test summary) off the remote k3s and stores them in one ConfigMap
// per node in lt.Namespace, named "<lt.Name>-k6log-<sanitized nodeID>". The
// exporter Job later mounts these CMs and ships them the same way as metrics
// (S3 when configured, else stdout).
//
// Whole loop is best-effort: a per-node capture error is recorded as a
// placeholder inside the ConfigMap (so the failure is still exported) and any
// ConfigMap write error is logged via logStatusErr — nothing here blocks the
// Exporting transition. ConfigMaps carry an OwnerRef to the LoadTest so they
// cascade-delete with it.
func (r *LoadTestReconciler) captureK6Logs(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) {
	logger := log.FromContext(ctx)

	k6Index := computeK6NodeIndex(env)
	for _, ref := range lt.Status.TestRuns {
		k6Node, ok := k6Index[ref.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			logger.Info("skipping k6 log capture: node has no kubeconfig secret", "node", ref.NodeID)
			continue
		}

		secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
		logs, err := r.Dispatcher.GetK6RunnerLogs(ctx, secretRef, ref.Name, ref.Namespace)
		if err != nil {
			// Record the failure as a placeholder so the operator/user still
			// gets a per-VM artifact noting capture did not succeed.
			logger.Error(err, "k6 log capture failed", "node", ref.NodeID, "testRun", ref.Name)
			logs = fmt.Sprintf("k6 log capture failed for node %s: %v", ref.NodeID, err)
		}

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-k6log-%s", lt.Name, sanitize(ref.NodeID)),
				Namespace: lt.Namespace,
			},
		}
		nodeID := ref.NodeID
		logContent := logs
		_, cerr := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
			if cm.Labels == nil {
				cm.Labels = map[string]string{}
			}
			cm.Labels["dfaas.io/loadtest-name"] = lt.Name
			cm.Labels["dfaas.io/node-id"] = nodeID
			cm.Labels["dfaas.io/k6-log"] = "true"
			cm.Data = map[string]string{"k6.log": logContent}
			return controllerutil.SetOwnerReference(lt, cm, r.Scheme)
		})
		logStatusErr(ctx, fmt.Sprintf("upsert k6-log ConfigMap for node %s", nodeID), cerr)
	}
}

// runExporter creates the in-cluster Job that pulls metrics from Prometheus
// over [StartTime, EndTime] and uploads to S3. The destination defaults to the
// in-cluster SeaweedFS sink (DefaultS3ConfigName) when env.spec.s3ConfigRef is
// unset; an explicit ref selects that config instead. Only if the default
// config itself is missing does the exporter fall back to a stdout dump.
func (r *LoadTestReconciler) runExporter(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	jobName := ExporterJobName(lt)
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: lt.Namespace}, &job)

	if apierrors.IsNotFound(err) {
		logger.Info("creating exporter Job", "job", jobName)
		if lt.Status.StartTime == nil || lt.Status.EndTime == nil {
			logStatusErr(ctx, "stamp MetricsExported=False (missing times)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
				metav1.ConditionFalse, dfaasv1.LTReasonJobFailed,
				"missing StartTime/EndTime; cannot run exporter"))
			return r.failLoadTest(ctx, lt, "missing StartTime/EndTime; cannot run exporter")
		}

		// Always resolve an S3 config name. With no explicit s3ConfigRef the
		// Environment defaults to the in-cluster SeaweedFS sink (DefaultS3ConfigName)
		// instead of the legacy stdout dump.
		configName := DefaultS3ConfigName
		explicitRef := env.Spec.S3ConfigRef != nil
		if explicitRef {
			configName = env.Spec.S3ConfigRef.Name
		}
		var s3SecretName string
		mirrored, mirrorErr := r.ensureMirroredS3Secret(ctx, lt, configName)
		if mirrorErr != nil {
			if apierrors.IsNotFound(mirrorErr) {
				// Stamp the S3ConfigMissing condition either way for visibility.
				logStatusErr(ctx, "stamp MetricsExported=False (s3 config missing)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
					metav1.ConditionFalse, dfaasv1.LTReasonS3ConfigMissing,
					fmt.Sprintf("S3 config %q not found in namespace %s",
						configName, S3ConfigNamespace)))
				if explicitRef {
					// Explicit ref must exist — a missing one is a hard failure
					// (unchanged behaviour).
					return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestFailed)
				}
				// Default sink missing (e.g. SeaweedFS not yet deployed) — degrade
				// gracefully to the stdout path rather than failing the test.
				logger.Info("default S3 config not found; falling back to stdout export",
					"config", configName)
				s3SecretName = ""
			} else {
				return ctrl.Result{}, mirrorErr
			}
		} else {
			s3SecretName = mirrored
		}

		// Per-node k6-log ConfigMaps captured at the end of observeK6. The
		// exporter Job mounts these (one file per VM) and ships them the same
		// way as metrics.
		k6LogCMs := make([]k6LogConfigMapRef, 0, len(lt.Status.TestRuns))
		for _, ref := range lt.Status.TestRuns {
			k6LogCMs = append(k6LogCMs, k6LogConfigMapRef{
				NodeID:    ref.NodeID,
				ConfigMap: fmt.Sprintf("%s-k6log-%s", lt.Name, sanitize(ref.NodeID)),
			})
		}

		newJob, err := r.createExporterJob(lt, env, lt.Status.StartTime.Time, lt.Status.EndTime.Time, s3SecretName, k6LogCMs)
		if err != nil {
			logStatusErr(ctx, "stamp MetricsExported=False (build failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
				metav1.ConditionFalse, dfaasv1.LTReasonJobFailed,
				"build exporter job: "+condMessage(err)))
			return r.failLoadTest(ctx, lt, fmt.Sprintf("build exporter job: %v", err))
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		logStatusErr(ctx, "stamp MetricsExported=Unknown (exporter running)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
			metav1.ConditionUnknown, dfaasv1.LTReasonExporterRunning,
			"exporter Job created, awaiting completion"))
		logStatusErr(ctx, "persist exporter Job name", r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
			latest.Status.ExporterJob = jobName
			return nil
		}))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if job.Status.Succeeded > 0 {
		logger.Info("exporter Job succeeded")
		logStatusErr(ctx, "stamp MetricsExported=True", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
			metav1.ConditionTrue, dfaasv1.LTReasonExportSucceeded,
			"metrics exported"))
		return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestCompleted)
	}
	if job.Status.Failed > 0 {
		logStatusErr(ctx, "stamp MetricsExported=False (job failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
			metav1.ConditionFalse, dfaasv1.LTReasonJobFailed,
			"exporter Job reported Failed"))
		return r.failLoadTest(ctx, lt, "exporter Job failed")
	}
	logger.Info("exporter Job running")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
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

	k6Index := computeK6NodeIndex(env)

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

	// MetricsExported never ran on abort — stamp False/Skipped per P9 so
	// UI does not show "in flight" forever on the aborted CR.
	logStatusErr(ctx, "stamp MetricsExported=False (export skipped)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
		metav1.ConditionFalse, dfaasv1.LTReasonExportSkipped,
		"no exporter ran — test was aborted"))
	logStatusErr(ctx, "stamp Ready=False (aborted)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondReady, metav1.ConditionFalse,
		reason, message))
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
	if _, abortErr := r.abortLoadTest(ctx, lt, &env, dfaasv1.LTReasonUserAborted,
		"LoadTest aborted on user delete"); abortErr != nil {
		logger.Error(abortErr, "abortLoadTest during deletion failed; will retry")
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	// Poll: every TestRun in spec ∪ status must report NotFound on the
	// remote cluster before we drop the finalizer. Mirrors the abort target
	// set construction so a partial-dispatch ride-along is also covered.
	k6Index := computeK6NodeIndex(&env)
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
