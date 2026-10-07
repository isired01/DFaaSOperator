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
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/monitoring"
	"dfaas-operator/internal/controller/statuswriter"
	"dfaas-operator/internal/k6dispatch"
)

// dispatchAttemptsAnnotation persists the consecutive-error counter for
// remote-dispatcher operations across reconciles. Annotation lives on the
// LoadTest itself so it survives operator restarts without spec/status shape
// changes.
const dispatchAttemptsAnnotation = "dfaas.dfaas.io/dispatch-attempts"

// dispatchRetryBudget is the max number of consecutive dispatcher errors
// tolerated before the run ends with reason=DispatchFailed. Every failed
// round is paced at remoteRetryInterval (10s) by the gate in Reconcile, so
// ~15 attempts ≈ 2.5-5 min grace (plus the dispatcher's 10s request cap per
// call) — enough for a briefly unreachable k6 node to come back.
const dispatchRetryBudget = 15

// fetchMissesAnnotation counts consecutive observe rounds in which some
// generator could not report its TestRun's stage. Separate from the dispatch
// counter: a Retry counter bounds one operation.
const fetchMissesAnnotation = "dfaas.dfaas.io/fetch-misses"

// fetchRetryBudget is how many consecutive failed observe rounds end the run.
const fetchRetryBudget = 15

// onDispatchError centralises the retry-budget bookkeeping on a dispatcher
// error path. Bumps the counter; if it tips the budget, ends the run as Failed
// with K6Dispatched=False/DispatchFailed, and endRun deletes the TestRuns the
// other generators already run. Otherwise
// stamps K6Dispatched=False/<subReason> (P13) and returns a RequeueAfter
// Result. Every path is terminal for the current reconcile: the caller
// returns whatever this returns.
func (r *LoadTestReconciler) onDispatchError(ctx context.Context,
	lt *dfaasv1.LoadTest, dispatchErr error, subReason string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	budget := r.budget(dispatchAttemptsAnnotation, dispatchRetryBudget)
	outcome, bumpErr := r.attempt(ctx, lt, budget)
	if bumpErr != nil {
		// The round still happened, so it is still stamped -- this used to
		// return here, so a persistently conflicting annotation Update left the
		// LoadTest requeuing every 10s forever with no K6Dispatched Condition
		// ever written and nothing but a log line to show for it.
		logger.Error(bumpErr, "bumpDispatchAttempts failed; the budget cannot advance")
	}

	if outcome.Exhausted {
		detail := fmt.Sprintf("remote dispatch failed %d consecutive times: %s",
			outcome.Count, condMessage(dispatchErr))
		// endRun reclaims the TestRuns the other generators already run.
		return r.endRun(ctx, lt, dfaasv1.LoadTestFailed, dfaasv1.LTReasonDispatchFailed, detail,
			statuswriter.Cond{Type: dfaasv1.LTCondK6Dispatched, Status: metav1.ConditionFalse,
				Reason: dfaasv1.LTReasonDispatchFailed, Message: detail})
	}
	// P13: sub-reason (ScriptMirrorFailed / StaleCleanupFailed / ApplyFailed)
	// carries the diagnostic detail, retry-budget counter is in the message.
	r.cond(ctx, lt, dfaasv1.LTCondK6Dispatched,
		metav1.ConditionFalse, subReason,
		fmt.Sprintf("attempt %s: %s", budget.Attempts(outcome), condMessage(dispatchErr)))
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// startK6 dispatches one remote TestRun per PerNodeLoad entry on its matching
// k6-load-generator node's k3s cluster, then transitions LoadTest → Running.
func (r *LoadTestReconciler) startK6(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	// Synchronized start needs a VM-facing GO URL for every generator before
	// anything is dispatched — fail loudly instead of parking runners on a
	// barrier they cannot see open. A generator has one when a management
	// address was detected for it at provisioning, or else when the
	// process-wide fallback resolved (it does not under `make run` without
	// DFAAS_SYNC_PUBLIC_URL, against an Environment provisioned before the
	// detection existed).
	if lt.Spec.SyncStart {
		var unresolved []string
		for _, perNode := range lt.Spec.PerNodeLoad {
			if r.syncChannel().GoURL(lt, k6dispatch.GeneratorOf(env, perNode.NodeID)) == "" {
				unresolved = append(unresolved, perNode.NodeID)
			}
		}
		if len(unresolved) > 0 {
			return r.failLoadTest(ctx, lt, fmt.Sprintf(
				"synchronized start: cannot resolve the VM-facing GO URL for generator(s) %s — no management address "+
					"was detected for them at provisioning (re-provision the Environment by editing its spec), and no "+
					"fallback is set: set DFAAS_SYNC_PUBLIC_URL (or run in-cluster with HOST_IP injected)",
				strings.Join(unresolved, ", ")))
		}
	}

	// P9: first observation — nothing dispatched yet, status is Unknown.
	// Subsequent calls below upgrade this to False/InFlight or True/
	// AllDispatched. SetStatusCondition is idempotent on no transition.
	if len(lt.Status.TestRuns) == 0 {
		r.cond(ctx, lt, dfaasv1.LTCondK6Dispatched,
			metav1.ConditionUnknown, dfaasv1.LTReasonPending,
			"awaiting first remote TestRun apply")
	}

	// Seed from Status.TestRuns so a re-entry after a partial-dispatch error
	// resumes where it left off: a node already represented there has a live
	// remote run that must NOT be re-deleted and re-applied (that would yank a
	// running k6 test off the generator).
	refs := append([]dfaasv1.TestRunRef(nil), lt.Status.TestRuns...)
	dispatched := map[string]bool{}
	for _, ref := range refs {
		dispatched[ref.NodeID] = true
	}

	// The filer probes start after the Apply loop, never between two Applies:
	// each is two remote calls, and the TestRuns must start as close together
	// as they would without a probe. Deferred, so a generator applied in this
	// pass is probed whichever return the pass takes after it.
	var probes []probeRequest
	defer func() { r.startProbes(ctx, lt, probes) }()

	for _, perNode := range lt.Spec.PerNodeLoad {
		if dispatched[perNode.NodeID] {
			continue
		}
		node, nerr := r.Dispatcher.Node(ctx, env, perNode.NodeID)
		if nerr != nil {
			return r.failLoadTest(ctx, lt,
				fmt.Sprintf("perNodeLoad %q on env %q: %v", perNode.NodeID, env.Name, nerr))
		}
		// Computed once: the probe must check the URL the runner is given.
		renv := r.runnerEnv(lt, env, perNode.NodeID)
		if res, stop, err := r.dispatchOne(ctx, lt, node, perNode, renv); stop {
			return res, err
		}
		probes = append(probes, probeRequest{node: node, nodeID: perNode.NodeID, url: renv.SummaryURL})
		// Apply succeeded — record and persist incrementally so a failure later
		// in the loop leaves the already-dispatched runs visible to observeK6 /
		// abortLoadTest / the deletion finalizer.
		refs = append(refs, node.Ref(lt))
		dispatched[perNode.NodeID] = true
		if perr := r.persistTestRuns(ctx, lt, refs); perr != nil {
			// The run is live on the remote; back off and let the next reconcile
			// re-encounter it through the guard above once status catches up.
			log.FromContext(ctx).Error(perr, "persist TestRuns after apply failed")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Successful dispatcher round-trip — reset the budget counter and
		// surface partial progress on K6Dispatched (P9).
		if rerr := r.budget(dispatchAttemptsAnnotation, dispatchRetryBudget).Clear(ctx, lt); rerr != nil {
			log.FromContext(ctx).Error(rerr, "resetDispatchAttempts failed; non-fatal")
		}
		if len(refs) < len(lt.Spec.PerNodeLoad) {
			r.cond(ctx, lt, dfaasv1.LTCondK6Dispatched,
				metav1.ConditionFalse, dfaasv1.LTReasonInFlight,
				fmt.Sprintf("%d/%d TestRun(s) dispatched", len(refs), len(lt.Spec.PerNodeLoad)))
		}
	}

	// All TestRuns dispatched. Only on the transition: a syncStart test comes
	// back here on every barrier poll, and a restamp would erase a
	// DispatchedUnreachable warning (same status, so the budget is unaffected
	// either way).
	if c := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondK6Dispatched); c == nil || c.Status != metav1.ConditionTrue {
		r.cond(ctx, lt, dfaasv1.LTCondK6Dispatched,
			metav1.ConditionTrue, dfaasv1.LTReasonAllDispatched,
			fmt.Sprintf("dispatched %d remote TestRun(s)", len(refs)))
	}

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
	r.cond(ctx, lt, dfaasv1.LTCondK6Healthy,
		metav1.ConditionUnknown, dfaasv1.LTReasonRunning,
		"k6 TestRuns dispatched, awaiting observation")
	now := metav1.Now()
	if err := r.writer().Record(ctx, lt, ltTransition{Touch: func(latest *dfaasv1.LoadTest) error {
		latest.Status.StartTime = &now
		latest.Status.TestRuns = refs
		latest.Status.Phase = dfaasv1.LoadTestRunning
		stampLTAggregate(latest, dfaasv1.LoadTestRunning, "", "")
		return nil
	}}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// dispatchOne runs the mirror → wipe → confirm-gone → apply protocol for lt on
// one node, applying the TestRun with renv. stop=true means the caller returns
// (res, err) as-is: the LoadTest failed, the retry budget spoke, or we wait a
// tick for the remote delete to propagate. stop=false means the TestRun is
// applied. It makes no probe call: startK6 starts the probes after the loop.
func (r *LoadTestReconciler) dispatchOne(ctx context.Context, lt *dfaasv1.LoadTest,
	node k6dispatch.Node, perNode dfaasv1.PerNodeLoad, renv k6dispatch.RunnerEnv) (res ctrl.Result, stop bool, err error) {
	logger := log.FromContext(ctx)
	nodeID := perNode.NodeID

	// Mirror the script ConfigMap onto the remote k3s first: k6-operator
	// resolves spec.script.configMap in its own cluster.
	var src corev1.ConfigMap
	srcKey := types.NamespacedName{Name: perNode.ScriptConfigMap.Name, Namespace: lt.Namespace}
	if gerr := r.Get(ctx, srcKey, &src); gerr != nil {
		merr := fmt.Errorf("read script ConfigMap %s/%s: %w", srcKey.Namespace, srcKey.Name, gerr)
		logger.Error(merr, "remote script CM mirror failed", "node", nodeID)
		res, oerr := r.onDispatchError(ctx, lt, merr, dfaasv1.LTReasonScriptMirrorFailed)
		return res, true, oerr
	}
	if _, ok := src.Data["script.js"]; !ok {
		merr := fmt.Errorf("script ConfigMap %s/%s missing key \"script.js\"", srcKey.Namespace, srcKey.Name)
		logger.Error(merr, "remote script CM mirror failed", "node", nodeID)
		res, oerr := r.onDispatchError(ctx, lt, merr, dfaasv1.LTReasonScriptMirrorFailed)
		return res, true, oerr
	}
	if merr := node.MirrorConfigMap(ctx, lt, &src); merr != nil {
		logger.Error(merr, "remote script CM mirror failed", "node", nodeID)
		res, oerr := r.onDispatchError(ctx, lt, merr, dfaasv1.LTReasonScriptMirrorFailed)
		return res, true, oerr
	}

	// Wipe a stale TestRun from a previous run so we start with fresh status,
	// and only proceed once the remote confirms it is gone: an unreachable node
	// or an unparsable kubeconfig says nothing about the remote state, and
	// applying on top of it would risk racing a still-running k6 test.
	if derr := node.Delete(ctx, lt); derr != nil {
		logger.Error(derr, "remote TestRun cleanup failed", "node", nodeID)
		res, oerr := r.onDispatchError(ctx, lt, derr, dfaasv1.LTReasonStaleCleanupFailed)
		return res, true, oerr
	}
	_, serr := node.Stage(ctx, lt)
	switch {
	case serr == nil:
		// Delete still propagating on the remote — wait a tick then retry.
		return ctrl.Result{RequeueAfter: 3 * time.Second}, true, nil
	case !errors.Is(serr, k6dispatch.ErrNotFound):
		logger.Error(serr, "remote TestRun delete-confirm poll failed", "node", nodeID)
		res, oerr := r.onDispatchError(ctx, lt, serr, dfaasv1.LTReasonStaleCleanupFailed)
		return res, true, oerr
	}

	if aerr := node.Apply(ctx, lt, perNode, renv); aerr != nil {
		logger.Error(aerr, "remote TestRun apply failed", "node", nodeID)
		res, oerr := r.onDispatchError(ctx, lt, aerr, dfaasv1.LTReasonApplyFailed)
		return res, true, oerr
	}
	return ctrl.Result{}, false, nil
}

// runnerEnv is what the reconciler decides about nodeID's k6 runner
// environment: the URLs the generated script talks to, built on the management
// address detected for that generator when there is one. The dispatcher only
// carries them.
func (r *LoadTestReconciler) runnerEnv(lt *dfaasv1.LoadTest, env *dfaasv1.Environment, nodeID string) k6dispatch.RunnerEnv {
	g := k6dispatch.GeneratorOf(env, nodeID)
	renv := k6dispatch.RunnerEnv{SummaryURL: r.syncChannel().SummaryURL(lt, g)}
	if lt.Spec.SyncStart {
		renv.SyncURL = r.syncChannel().GoURL(lt, g)
	}
	// Only on a detected address, with no HOST_IP fallback: HOST_IP can be an
	// address only the local site reaches, and an asset base built on it would
	// override the URL the gateway baked from SEAWEEDFS_PUBLIC_URL.
	if g.MgmtAddr != "" {
		renv.AssetBase = monitoring.S3PublicBase(g.MgmtAddr)
	}
	return renv
}

// persistTestRuns writes the cumulative TestRunRef slice into
// Status.TestRuns with conflict retry. Called incrementally from startK6
// after each successful ApplyTestRun so a mid-loop failure leaves the
// already-dispatched runs visible to observeK6 / abort / deletion paths.
func (r *LoadTestReconciler) persistTestRuns(ctx context.Context,
	lt *dfaasv1.LoadTest, refs []dfaasv1.TestRunRef) error {
	return r.writer().Record(ctx, lt, ltTransition{Touch: func(latest *dfaasv1.LoadTest) error {
		latest.Status.TestRuns = refs
		return nil
	}})
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
