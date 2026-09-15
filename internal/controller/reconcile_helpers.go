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
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/statuswriter"
)

// updateWithRetry persists an ObjectMeta change (finalizers) via Update under
// conflict retry. mutate returns false to skip the write. Status and counters
// go through the statuswriter instead.
func updateWithRetry[T client.Object](ctx context.Context, c client.Client,
	key client.ObjectKey, obj T, mutate func(T) (write bool)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(ctx, key, obj); err != nil {
			return err
		}
		if !mutate(obj) {
			return nil
		}
		return c.Update(ctx, obj)
	})
}

// ensureFinalizer adds finalizer to obj when missing and persists it. Returns
// added=true when it wrote a change (the caller should return/requeue so the
// next reconcile sees the updated object), false when the finalizer was already
// present. Shared by the EnvironmentReconciler and LoadTestReconciler reconcile
// entrypoints.
func ensureFinalizer(ctx context.Context, c client.Client,
	obj client.Object, finalizer string) (added bool, err error) {
	if controllerutil.ContainsFinalizer(obj, finalizer) {
		return false, nil
	}
	controllerutil.AddFinalizer(obj, finalizer)
	return true, c.Update(ctx, obj)
}

// isSettledPhase reports whether an Environment phase is a settled outcome.
// On a settled phase the reconciler stamps observedGeneration, so a later spec
// edit (generation bump) is detected as drift. Keeping the set in one place
// means the phase writer and the drift guard cannot disagree on what "settled" means.
func isSettledPhase(p dfaasv1.EnvironmentPhase) bool {
	return p == dfaasv1.EnvReady || p == dfaasv1.EnvFailed
}

// Transition aliases for the two CRs. See statuswriter for the contract.
type (
	ltTransition  = statuswriter.Transition[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]
	envTransition = statuswriter.Transition[*dfaasv1.Environment, dfaasv1.EnvironmentPhase]
)
