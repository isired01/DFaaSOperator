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
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

// abortLoadTest ends the run as Aborted with the caller's Ready reason and
// message (the UI looks the abort explanation up by Ready/UserAborted).
// endRun reclaims the remote TestRuns in the same pass.
func (r *LoadTestReconciler) abortLoadTest(ctx context.Context,
	lt *dfaasv1.LoadTest, reason, message string) (ctrl.Result, error) {
	return r.endRun(ctx, lt, dfaasv1.LoadTestAborted, reason, message)
}

// handleLoadTestDeletion is the finalizer flow triggered by a non-zero
// DeletionTimestamp: no remote TestRun of the test may survive its CR.
//
//  1. Environment gone: its kubeconfig Secrets went with it, so no remote is
//     reachable. Sweep the filer objects and release.
//  2. Not yet ended: end the run as Aborted (endRun reclaims the runners), and
//     come back to poll.
//  3. Ended: poll every target generator, deleting what is still there, and
//     release once all report NotFound. A generator that cannot answer holds
//     the release for at most deletionReclaimBudget from DeletionTimestamp;
//     after that the unconfirmed generators are logged and the CR goes —
//     except a RunnersUnreclaimed test (at most one per Environment, since
//     Occupancy holds it there), which still gets one last Delete pass below
//     before that same budget releases it.
//
// Completed and Failed records are never rewritten, and a test that never
// dispatched makes no remote call.
func (r *LoadTestReconciler) handleLoadTestDeletion(ctx context.Context,
	lt *dfaasv1.LoadTest) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(lt, loadTestFinalizer) {
		return ctrl.Result{}, nil
	}

	var env dfaasv1.Environment
	envKey := types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}
	err := r.Get(ctx, envKey, &env)
	if apierrors.IsNotFound(err) {
		logger.Info("environment gone during loadtest deletion — releasing finalizer",
			"env", lt.Spec.TargetEnvironment)
		if dispatchBegan(lt) {
			r.sweepFilerObjects(ctx, lt, "loadtest deletion")
		}
		return ctrl.Result{}, r.removeLTFinalizer(ctx, lt)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if !lt.Status.Phase.Terminal() {
		if _, err := r.endRun(ctx, lt, dfaasv1.LoadTestAborted, dfaasv1.LTReasonUserAborted,
			"LoadTest aborted on user delete"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	if dispatchBegan(lt) {
		// Past the budget, release without another remote call — except a
		// RunnersUnreclaimed test, which falls through to the normal pass
		// below instead: releasing it with no Delete attempt would leave its
		// runner free to keep loading the DFaaS nodes while Occupancy frees
		// the Environment for the next test. At most one such test exists
		// per Environment (Occupancy holds it), so this costs at most one
		// extra pass. The post-poll budget branch below still releases it
		// once that pass is done. With one LoadTest worker, a dead generator
		// otherwise costs remoteRequestTimeout per Stage, and an Environment
		// deletion drains every dispatched test at once: this caps the worst
		// case at deletionReclaimBudget for every other test.
		if time.Since(lt.DeletionTimestamp.Time) >= deletionReclaimBudget && !runnersUnreclaimed(lt) {
			logger.Info("deletion budget spent — releasing with remote TestRuns unconfirmed")
			r.sweepFilerObjects(ctx, lt, "loadtest deletion")
			return ctrl.Result{}, r.removeLTFinalizer(ctx, lt)
		}

		var pending, errored []string
		for _, nodeID := range k6dispatch.TargetNodeIDs(lt) {
			node, nerr := r.Dispatcher.Node(ctx, &env, nodeID)
			if nerr != nil {
				continue // nothing reachable to poll
			}
			_, serr := node.Stage(ctx, lt)
			switch {
			case errors.Is(serr, k6dispatch.ErrNotFound):
				// The TestRun is gone; its mirrored script is the last remote
				// object this test left on the node.
				for _, pn := range lt.Spec.PerNodeLoad {
					if pn.NodeID == nodeID {
						logStatusErr(ctx, "delete remote script ConfigMap (loadtest deletion)",
							node.DeleteScript(ctx, lt, pn.ScriptConfigMap.Name))
					}
				}
				logStatusErr(ctx, "delete filer reachability probe (loadtest deletion)", node.DeleteProbe(ctx, lt))
			case serr != nil:
				logger.Error(serr, "remote TestRun NotFound-poll errored", "node", nodeID)
				errored = append(errored, nodeID)
			default:
				logStatusErr(ctx, "delete remote TestRun (loadtest deletion)", node.Delete(ctx, lt))
				pending = append(pending, nodeID)
			}
		}
		if len(pending)+len(errored) > 0 {
			if time.Since(lt.DeletionTimestamp.Time) < deletionReclaimBudget {
				if len(errored) > 0 {
					return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
				}
				return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
			}
			logger.Info("deletion budget spent — releasing with remote TestRuns unconfirmed",
				"pending", pending, "errored", errored)
		}
		r.sweepFilerObjects(ctx, lt, "loadtest deletion")
	}
	logger.Info("remote TestRuns reclaimed — removing loadtest finalizer")
	return ctrl.Result{}, r.removeLTFinalizer(ctx, lt)
}

// removeLTFinalizer drops the loadtest finalizer with the re-fetch-then-update
// pattern: the deletion path writes status (phase/conditions) between the
// reconcile's Get and this Update, so a plain r.Update on the stale object
// 409s deterministically. RemoveFinalizer's bool doubles as the write flag.
func (r *LoadTestReconciler) removeLTFinalizer(ctx context.Context, lt *dfaasv1.LoadTest) error {
	return updateWithRetry(ctx, r.Client, client.ObjectKeyFromObject(lt), &dfaasv1.LoadTest{},
		func(latest *dfaasv1.LoadTest) bool {
			return controllerutil.RemoveFinalizer(latest, loadTestFinalizer)
		})
}
