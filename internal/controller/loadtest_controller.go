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
	"time"

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

const loadTestFinalizer = "dfaas.dfaas.io/loadtest-finalizer"

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
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *LoadTestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var lt dfaasv1.LoadTest
	if err := r.Get(ctx, req.NamespacedName, &lt); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !lt.DeletionTimestamp.IsZero() {
		return r.handleLoadTestDeletion(ctx, &lt)
	}
	if !controllerutil.ContainsFinalizer(&lt, loadTestFinalizer) {
		controllerutil.AddFinalizer(&lt, loadTestFinalizer)
		return ctrl.Result{}, r.Update(ctx, &lt)
	}

	// Terminal phases — no-op.
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
			// P9: surface link state on EnvironmentLinked before failing.
			_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondEnvironmentLinked,
				metav1.ConditionFalse, dfaasv1.LTReasonEnvNotFound,
				fmt.Sprintf("environment %q not found in namespace %s",
					lt.Spec.TargetEnvironment, lt.Namespace))
			return r.failLoadTest(ctx, &lt,
				fmt.Sprintf("environment %q not found in namespace %s",
					lt.Spec.TargetEnvironment, lt.Namespace))
		}
		return ctrl.Result{}, err
	}

	// P9: env exists — stamp EnvironmentLinked. EnvDegraded is advisory:
	// LoadTests against a degraded env are permitted, so we mark True with
	// reason=EnvDegraded so consumers know to expect missing metrics later.
	switch env.Status.Phase {
	case dfaasv1.EnvFailed:
		_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondEnvironmentLinked,
			metav1.ConditionFalse, dfaasv1.LTReasonEnvFailed,
			fmt.Sprintf("environment %q is in phase Failed", env.Name))
	case dfaasv1.EnvDegraded:
		_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondEnvironmentLinked,
			metav1.ConditionTrue, dfaasv1.LTReasonEnvDegraded,
			fmt.Sprintf("environment %q is Degraded — monitoring unavailable, exporter step may fail", env.Name))
	default:
		_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondEnvironmentLinked,
			metav1.ConditionTrue, dfaasv1.LTReasonEnvFound,
			fmt.Sprintf("environment %q resolved", env.Name))
	}

	// Abort short-circuit. User PATCHed spec.stop=true.
	if lt.Spec.Stop {
		inAbortWindow := lt.Status.Phase == "" ||
			lt.Status.Phase == dfaasv1.LoadTestPending ||
			lt.Status.Phase == dfaasv1.LoadTestRunning
		if inAbortWindow {
			return r.abortLoadTest(ctx, &lt, &env, dfaasv1.LTReasonUserAborted,
				"The test was manually aborted from the UI. Remote worker resources have been reclaimed.")
		}
	}

	// Scheduled-start branch (P9: stamps LTCondScheduled).
	if lt.Spec.StartAt != nil &&
		(lt.Status.Phase == "" || lt.Status.Phase == dfaasv1.LoadTestPending) &&
		lt.Spec.Suspended {

		fireT := lt.Spec.StartAt.Time
		switch {
		case time.Now().Before(fireT):
			// Armed: future startAt.
			_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondScheduled,
				metav1.ConditionTrue, dfaasv1.LTReasonScheduledArmed,
				"armed for "+fireT.UTC().Format(time.RFC3339))
			return ctrl.Result{RequeueAfter: time.Until(fireT)}, nil

		case env.Status.Phase != dfaasv1.EnvReady && env.Status.Phase != dfaasv1.EnvDegraded:
			// Fire time elapsed but the target Environment is not ready
			// (Degraded is treated as good-enough to dispatch — only
			// Failed / still-Provisioning hold the schedule).
			_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondScheduled,
				metav1.ConditionTrue, dfaasv1.LTReasonScheduledDelayedEnvNot,
				fmt.Sprintf("schedule fired at %s; waiting for env %s phase=%s",
					fireT.UTC().Format(time.RFC3339), env.Name, env.Status.Phase))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil

		default:
			// Fire.
			patchErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				return r.Patch(ctx, &lt, client.RawPatch(types.MergePatchType,
					[]byte(`{"spec":{"suspended":false}}`)))
			})
			if patchErr != nil {
				return ctrl.Result{}, patchErr
			}
			_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondScheduled,
				metav1.ConditionTrue, dfaasv1.LTReasonScheduledFired,
				"schedule fired at "+fireT.UTC().Format(time.RFC3339))
			return ctrl.Result{Requeue: true}, nil
		}
	}

	// Strict create-time gate.
	if lt.Status.Phase == "" && !lt.Spec.Suspended && env.Status.Phase != dfaasv1.EnvReady && env.Status.Phase != dfaasv1.EnvDegraded {
		return r.failLoadTest(ctx, &lt,
			fmt.Sprintf("environment %q is %q; it must be Ready before creating a LoadTest",
				env.Name, env.Status.Phase))
	}

	// OwnerReference Environment → LoadTest.
	if !hasOwnerRef(&lt, &env) {
		if err := controllerutil.SetOwnerReference(&env, &lt, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Update(ctx, &lt); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Run-once guard + suspended gate.
	preExecution := lt.Status.Phase == "" || lt.Status.Phase == dfaasv1.LoadTestPending
	if preExecution {
		// P11: spec is editable in the pre-execution window.
		_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondSpecLocked,
			metav1.ConditionFalse, dfaasv1.LTReasonPending,
			"spec editable until the test starts")

		if lt.Spec.Suspended {
			// Idempotent: SetStatusCondition keeps LastTransitionTime
			// stable unless status/reason/message actually changes. Safe
			// to stamp on every reconcile.
			_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondSuspended,
				metav1.ConditionTrue, dfaasv1.LTReasonDraftSaved,
				"LoadTest saved as draft — PATCH spec.suspended=false to start")
			if lt.Status.Phase == "" {
				return r.setLoadTestPhase(ctx, &lt, dfaasv1.LoadTestPending)
			}
			return ctrl.Result{}, nil
		}

		// P12: only stamp Activated on the True → False (or absent → False)
		// edge. Steady-state "suspended is False" does not need to mutate
		// the condition every tick.
		prev := meta.FindStatusCondition(lt.Status.Conditions, dfaasv1.LTCondSuspended)
		if prev == nil || prev.Status != metav1.ConditionFalse {
			_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondSuspended,
				metav1.ConditionFalse, dfaasv1.LTReasonActivated,
				"LoadTest is active")
		}

		// Block while Environment is mid-flight.
		if env.Status.Phase != dfaasv1.EnvReady && env.Status.Phase != dfaasv1.EnvDegraded {
			logger.Info("waiting for environment", "env", env.Name, "phase", env.Status.Phase)
			return r.setLoadTestPhase(ctx, &lt, dfaasv1.LoadTestPending)
		}
	} else {
		// P11: post-Pending the state machine is immutable; record it on
		// the condition so consumers know PATCHes are silently ignored.
		_ = r.setLoadTestCondition(ctx, &lt, dfaasv1.LTCondSpecLocked,
			metav1.ConditionTrue, dfaasv1.LTReasonPostStart,
			"spec is immutable once the test has started")
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

// setLoadTestPhase patches status.phase, retrying on conflict. Also stamps
// the LTCondReady aggregator (P9) and SpecLocked (P11) when transitioning
// into a post-execution phase.
func (r *LoadTestReconciler) setLoadTestPhase(ctx context.Context,
	lt *dfaasv1.LoadTest, phase dfaasv1.LoadTestPhase) (ctrl.Result, error) {

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		latest.Status.Phase = phase
		stampLTAggregate(latest, phase)
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// stampLTAggregate writes the LTCondReady aggregator (P9) and the
// LTCondSpecLocked latch (P11) onto the in-memory LoadTest. Pure function;
// caller persists via Status().Update.
func stampLTAggregate(lt *dfaasv1.LoadTest, phase dfaasv1.LoadTestPhase) {
	var (
		ready                     metav1.ConditionStatus
		reason, message           string
		specLocked                metav1.ConditionStatus
		specLockedReason, specMsg string
		stampLocked               = true
	)
	switch phase {
	case dfaasv1.LoadTestCompleted:
		ready = metav1.ConditionTrue
		reason = dfaasv1.LTReasonCompleted
		message = "metrics exported, test complete"
		specLocked = metav1.ConditionTrue
		specLockedReason = dfaasv1.LTReasonPostStart
		specMsg = "spec is immutable post-completion"
	case dfaasv1.LoadTestFailed:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonFailed
		message = "load test failed"
		specLocked = metav1.ConditionTrue
		specLockedReason = dfaasv1.LTReasonPostStart
		specMsg = "spec is immutable post-failure"
	case dfaasv1.LoadTestAborted:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonAborted
		message = "load test aborted"
		specLocked = metav1.ConditionTrue
		specLockedReason = dfaasv1.LTReasonPostStart
		specMsg = "spec is immutable post-abort"
	case dfaasv1.LoadTestRunning:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonRunning
		message = "remote TestRuns dispatched, k6 running"
		specLocked = metav1.ConditionTrue
		specLockedReason = dfaasv1.LTReasonPostStart
		specMsg = "spec is immutable once the test has started"
	case dfaasv1.LoadTestExporting:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonExporterRunning
		message = "k6 finished, metrics export in progress"
		specLocked = metav1.ConditionTrue
		specLockedReason = dfaasv1.LTReasonPostStart
		specMsg = "spec is immutable during export"
	case dfaasv1.LoadTestPending:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonPending
		message = "pending — waiting for environment or activation"
		stampLocked = false
	default:
		ready = metav1.ConditionUnknown
		reason = dfaasv1.LTReasonPending
		message = "awaiting first reconcile"
		stampLocked = false
	}
	meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
		Type:    dfaasv1.LTCondReady,
		Status:  ready,
		Reason:  reason,
		Message: message,
	})
	if stampLocked {
		meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
			Type:    dfaasv1.LTCondSpecLocked,
			Status:  specLocked,
			Reason:  specLockedReason,
			Message: specMsg,
		})
	}
}

// failLoadTest stamps Failed phase + a generic failure message on the Ready
// aggregator. Callers wanting a richer reason should stamp a sub-condition
// before calling failLoadTest (the aggregator overwrites only Ready).
func (r *LoadTestReconciler) failLoadTest(ctx context.Context,
	lt *dfaasv1.LoadTest, message string) (ctrl.Result, error) {

	_ = r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondReady, metav1.ConditionFalse,
		dfaasv1.LTReasonFailed, message)
	return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestFailed)
}

// setLoadTestCondition sets a Condition on status using the rifetch-then-update pattern.
// Omits LastTransitionTime so meta.SetStatusCondition keeps it stable across
// reconciles that produce the same (status, reason, message) tuple (P14).
func (r *LoadTestReconciler) setLoadTestCondition(ctx context.Context,
	lt *dfaasv1.LoadTest, condType string, status metav1.ConditionStatus,
	reason, message string) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.LoadTest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(lt), latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    condType,
			Status:  status,
			Reason:  reason,
			Message: message,
		})
		return r.Status().Update(ctx, latest)
	})
}

// hasOwnerRef reports whether lt already lists env among its OwnerReferences.
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
