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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

// LoadTestReconciler owns the LoadTest CRD lifecycle. It uses a Lookup
// pattern against the referenced Environment: nothing happens until the
// Environment is Ready, after which the reconciler dispatches one k6 TestRun
// per PerNodeLoad entry to the matching k6 machine's k3s cluster, then runs
// the metrics exporter.
type LoadTestReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Dispatcher *k6dispatch.Dispatcher
}

//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=loadtests,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=loadtests/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=loadtests/finalizers,verbs=update
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=environments,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *LoadTestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var lt dfaasv1.LoadTest
	if err := r.Get(ctx, req.NamespacedName, &lt); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal phases — no-op. Aborted is included so re-PATCHing
	// spec.stop on an already-aborted CR is a silent no-op.
	if lt.Status.Phase == dfaasv1.LoadTestCompleted ||
		lt.Status.Phase == dfaasv1.LoadTestFailed ||
		lt.Status.Phase == dfaasv1.LoadTestAborted {
		return ctrl.Result{}, nil
	}

	// Lookup target Environment in the same namespace.
	var env dfaasv1.Environment
	envKey := types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}
	if err := r.Get(ctx, envKey, &env); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failLoadTest(ctx, &lt,
				fmt.Sprintf("environment %q not found in namespace %s",
					lt.Spec.TargetEnvironment, lt.Namespace))
		}
		return ctrl.Result{}, err
	}

	// Abort short-circuit. User PATCHed spec.stop=true: dispatch remote
	// TestRun deletions across all k6 nodes, then mark the CR terminal as
	// Aborted (the CR persists as a historical record — no deletion).
	// Only honored from "" / Pending / Running per user-confirmed scope.
	// Drafts (suspended=true, phase=Pending) are allowed: abortLoadTest is
	// a no-op on the remote side (no TestRuns dispatched yet) and just
	// flips phase to Aborted as an "abandoned draft" historical marker.
	if lt.Spec.Stop {
		inAbortWindow := lt.Status.Phase == "" ||
			lt.Status.Phase == dfaasv1.LoadTestPending ||
			lt.Status.Phase == dfaasv1.LoadTestRunning
		if inAbortWindow {
			return r.abortLoadTest(ctx, &lt, &env)
		}
	}

	// Strict create-time gate: a LoadTest may only enter the FSM if its
	// Environment is already Ready. Submitting a LoadTest against a
	// Pending / Provisioning / Failed Environment fails fast with an
	// explanatory message — no quiet Pending limbo waiting on infra that
	// may never come up. Only runs on the very first reconcile
	// (`phase == ""`); once the LoadTest has moved past it, Environment
	// drift out of Ready does NOT retroactively cancel the run.
	//
	// RELAXED for drafts: when spec.suspended is true the LoadTest is
	// just saved, not executed, so the Environment does not need to be
	// Ready yet. Existence is still required (NotFound above fails).
	if lt.Status.Phase == "" && !lt.Spec.Suspended && env.Status.Phase != dfaasv1.EnvReady {
		return r.failLoadTest(ctx, &lt,
			fmt.Sprintf("environment %q is %q; it must be Ready before creating a LoadTest",
				env.Name, env.Status.Phase))
	}

	// Stamp an OwnerReference Environment → LoadTest on first encounter so
	// `kubectl delete environment` cascades into the dependent LoadTests via
	// Kubernetes garbage collection. SetOwnerReference (not Controller) is
	// the correct choice: a LoadTest is owned-by an Environment for GC
	// purposes but is NOT controlled-by it (LoadTestReconciler is the
	// controller). Idempotent: skip the Update when the ref is already there.
	if !hasOwnerRef(&lt, &env) {
		if err := controllerutil.SetOwnerReference(&env, &lt, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Update(ctx, &lt); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Run-once guard + suspended gate. Only consult spec.suspended in the
	// pre-execution window (phase empty or Pending). Once the test has
	// entered Running/Exporting (or terminal, already short-circuited above)
	// spec.suspended changes are ignored — the state machine is immutable.
	preExecution := lt.Status.Phase == "" || lt.Status.Phase == dfaasv1.LoadTestPending
	if preExecution {
		if lt.Spec.Suspended {
			// Suspended gate: stamp the Condition and hold at Pending.
			// Mirrors batch/v1.Job.spec.suspend semantics — no new phase.
			_ = r.setLoadTestCondition(ctx, &lt, "Suspended", metav1.ConditionTrue,
				"DraftSaved",
				"LoadTest saved as draft — PATCH spec.suspended=false to start")
			if lt.Status.Phase == "" {
				return r.setLoadTestPhase(ctx, &lt, dfaasv1.LoadTestPending)
			}
			return ctrl.Result{}, nil
		}

		// Not suspended (or just un-suspended): flip the Condition to False
		// so the UI clears the Draft badge before the phase machine fires.
		_ = r.setLoadTestCondition(ctx, &lt, "Suspended", metav1.ConditionFalse,
			"Activated", "LoadTest is active")

		// Block while Environment is mid-flight (only reachable
		// post-first-reconcile or when un-suspending a draft against an
		// Environment that is not Ready yet).
		if env.Status.Phase != dfaasv1.EnvReady {
			logger.Info("waiting for environment", "env", env.Name, "phase", env.Status.Phase)
			return r.setLoadTestPhase(ctx, &lt, dfaasv1.LoadTestPending)
		}
	}

	// Phase machine.
	switch lt.Status.Phase {
	case "", dfaasv1.LoadTestPending:
		return r.startK6(ctx, &lt, &env)
	case dfaasv1.LoadTestRunning:
		return r.observeK6(ctx, &lt, &env)
	case dfaasv1.LoadTestExporting:
		return r.runExporter(ctx, &lt, &env)
	}
	return ctrl.Result{}, nil
}

// setLoadTestPhase patches status.phase, retrying on conflict.
func (r *LoadTestReconciler) setLoadTestPhase(ctx context.Context,
	lt *dfaasv1.LoadTest, phase dfaasv1.LoadTestPhase) (ctrl.Result, error) {

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		latest.Status.Phase = phase
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// failLoadTest stamps Failed phase + a Condition with the reason.
func (r *LoadTestReconciler) failLoadTest(ctx context.Context,
	lt *dfaasv1.LoadTest, message string) (ctrl.Result, error) {

	_ = r.setLoadTestCondition(ctx, lt, "Ready", metav1.ConditionFalse, "Failed", message)
	return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestFailed)
}

// setLoadTestCondition sets a Condition on status using the rifetch-then-update pattern.
func (r *LoadTestReconciler) setLoadTestCondition(ctx context.Context,
	lt *dfaasv1.LoadTest, condType string, status metav1.ConditionStatus,
	reason, message string) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               condType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		})
		return r.Status().Update(ctx, latest)
	})
}

// hasOwnerRef reports whether lt already lists env among its OwnerReferences.
// Match is by UID (controllerutil.SetOwnerReference also matches by UID, so
// this gate avoids needless Updates on every reconcile once the ref is in
// place).
func hasOwnerRef(lt *dfaasv1.LoadTest, env *dfaasv1.Environment) bool {
	for _, o := range lt.OwnerReferences {
		if o.UID == env.UID {
			return true
		}
	}
	return false
}

func (r *LoadTestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfaasv1.LoadTest{}).
		Watches(
			&dfaasv1.Environment{},
			handler.EnqueueRequestsFromMapFunc(r.loadTestsForEnv),
		).
		Complete(r)
}

// loadTestsForEnv enqueues every LoadTest in the Environment's namespace whose
// spec.targetEnvironment matches. Fires when an Environment phase transition
// (e.g. into Ready) should unblock its dependent LoadTests.
func (r *LoadTestReconciler) loadTestsForEnv(ctx context.Context, obj client.Object) []reconcile.Request {
	envName := obj.GetName()
	var list dfaasv1.LoadTestList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, lt := range list.Items {
		if lt.Spec.TargetEnvironment == envName {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
				Name: lt.Name, Namespace: lt.Namespace,
			}})
		}
	}
	return out
}
