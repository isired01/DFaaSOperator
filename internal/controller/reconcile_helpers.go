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

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dfaasv1 "dfaas-operator/api/v1"
)

// statusUpdateWithRetry re-fetches obj by key, applies mutate, and persists it
// via Status().Update under retry.RetryOnConflict. The re-fetch absorbs
// informer cache lag and the retry resolves the resourceVersion conflicts that
// concurrent writers produce. It is the shared core behind the per-reconciler
// atomicStatusUpdate adapters (Environment, LoadTest); obj is re-populated by
// Get on every attempt, so a single zero-valued object can be passed in.
func statusUpdateWithRetry[T client.Object](ctx context.Context, c client.Client,
	key client.ObjectKey, obj T, mutate func(T) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(ctx, key, obj); err != nil {
			return err
		}
		if err := mutate(obj); err != nil {
			return err
		}
		return c.Status().Update(ctx, obj)
	})
}

// updateWithRetry is the metadata twin of statusUpdateWithRetry: it persists via
// Update (not Status().Update), for changes that live in ObjectMeta such as
// annotation counters. mutate returns false to skip the write, so a no-op
// reconcile avoids a pointless Update (and the re-enqueue it would trigger).
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
// means setEnvPhase and the drift guard cannot disagree on what "settled" means.
func isSettledPhase(p dfaasv1.EnvironmentPhase) bool {
	return p == dfaasv1.EnvReady || p == dfaasv1.EnvFailed || p == dfaasv1.EnvDegraded
}

// parseGenCounter decodes a generation-scoped "<generation>:<count>" counter
// annotation. Returns the stored generation and count; (0, 0) if absent or
// malformed. Backs both the SSH-attempts and health-misses counters.
func parseGenCounter(s string) (gen int64, count int) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	g, gerr := strconv.ParseInt(parts[0], 10, 64)
	c, cerr := strconv.Atoi(parts[1])
	if gerr != nil || cerr != nil {
		return 0, 0
	}
	return g, c
}

// bumpGenCounter increments a generation-scoped "<generation>:<count>" counter
// annotation on obj, mutating its annotations in place and returning the new
// count. A stored generation different from obj's current generation (a spec
// edit) restarts the budget at 1. Decodes with parseGenCounter. Pair it with
// updateWithRetry to persist.
func bumpGenCounter(obj client.Object, key string) int {
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	storedGen, cur := parseGenCounter(ann[key])
	if storedGen != obj.GetGeneration() {
		cur = 0
	}
	cur++
	ann[key] = fmt.Sprintf("%d:%d", obj.GetGeneration(), cur)
	obj.SetAnnotations(ann)
	return cur
}

// resetGenCounter zeroes a generation-scoped counter annotation on obj for the
// current generation. Returns write=false when the counter is already zero or
// absent, so updateWithRetry skips a pointless write.
func resetGenCounter(obj client.Object, key string) (write bool) {
	if _, cur := parseGenCounter(obj.GetAnnotations()[key]); cur == 0 {
		return false
	}
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[key] = fmt.Sprintf("%d:0", obj.GetGeneration())
	obj.SetAnnotations(ann)
	return true
}

// parsePlainCounter decodes a plain integer counter annotation. Returns 0 when
// the value is absent or malformed.
func parsePlainCounter(s string) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return 0
}

// bumpPlainCounter increments a plain integer counter annotation on obj (no
// generation scoping), mutating its annotations in place and returning the new
// count. Used for budgets that persist across spec edits (monitoring Helm
// install, remote dispatch). Pair it with updateWithRetry to persist.
func bumpPlainCounter(obj client.Object, key string) int {
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	cur := parsePlainCounter(ann[key])
	cur++
	ann[key] = fmt.Sprintf("%d", cur)
	obj.SetAnnotations(ann)
	return cur
}

// resetPlainCounter zeroes a plain integer counter annotation on obj. Returns
// write=false when the annotation is already "0", so updateWithRetry skips a
// pointless write.
func resetPlainCounter(obj client.Object, key string) (write bool) {
	if cur, ok := obj.GetAnnotations()[key]; ok && cur == "0" {
		return false
	}
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[key] = "0"
	obj.SetAnnotations(ann)
	return true
}
