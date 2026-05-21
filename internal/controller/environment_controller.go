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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
)

const environmentFinalizer = "dfaas.dfaas.io/environment-finalizer"

// EnvironmentReconciler owns the Environment CRD lifecycle: it walks the
// infrastructure FSM (ProvisioningVMs → ProvisioningInfra → ProvisioningMonitoring
// → Ready) by delegating each phase to the helpers in the ansible/ and
// monitoring/ subpackages. Once an Environment is Ready it stays idle until
// the spec changes (detected via generation drift).
type EnvironmentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=environments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=environments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=environments/finalizers,verbs=update

//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps;services;secrets;serviceaccounts;persistentvolumeclaims;namespaces,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets;replicasets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

func (r *EnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var env dfaasv1.Environment
	if err := r.Get(ctx, req.NamespacedName, &env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !env.DeletionTimestamp.IsZero() {
		return r.handleEnvDeletion(ctx, &env)
	}
	if !controllerutil.ContainsFinalizer(&env, environmentFinalizer) {
		controllerutil.AddFinalizer(&env, environmentFinalizer)
		return ctrl.Result{}, r.Update(ctx, &env)
	}

	if env.Status.Phase == dfaasv1.EnvReady &&
		env.Status.ObservedGeneration == env.Generation {
		logger.V(1).Info("environment ready, generation unchanged — idle")
		return ctrl.Result{}, nil
	}

	// Generation drift: spec was edited after a successful run. Reset stale
	// Conditions, delete previous-gen Ansible Jobs (avoid concurrent runs on
	// the same VMs), and restart the FSM from ProvisioningVMs.
	// observedGeneration is NOT touched here — it gets re-stamped only when
	// the new run reaches Ready, which is the canonical signal the UI uses
	// to compute isUpdating = (metadata.generation > status.observedGeneration).
	// Generation drift handling. Stale-gen Job cleanup is idempotent and runs
	// on every tick while observedGeneration trails Generation — picks up
	// previous-gen Jobs whether the edit happened at Ready or mid-flight.
	// Phase-reset + Conditions-reset only fire when the previous run had
	// already reached Ready; otherwise we'd loop forever (drift block keeps
	// resetting to VMs while VMs handler keeps advancing). Mid-flight drift
	// is absorbed automatically because JobNameForRole embeds env.Generation,
	// so the next ensure*Job hits NotFound and creates the new-gen Job.
	if env.Status.ObservedGeneration > 0 && env.Status.ObservedGeneration < env.Generation {
		if err := r.cleanupStaleGenJobs(ctx, &env); err != nil {
			logger.Error(err, "stale-gen Job cleanup failed")
		}
		if env.Status.Phase == dfaasv1.EnvReady || env.Status.Phase == dfaasv1.EnvFailed {
			logger.Info("generation drift detected — restarting provisioning",
				"phase", env.Status.Phase,
				"observed", env.Status.ObservedGeneration, "current", env.Generation)
			if err := r.resetTransientConditions(ctx, &env); err != nil {
				logger.Error(err, "reset transient Conditions failed")
			}
			return r.setEnvPhase(ctx, &env, dfaasv1.EnvProvisioningVMs)
		}
	}

	switch env.Status.Phase {
	case "", dfaasv1.EnvIdle:
		return r.setEnvPhase(ctx, &env, dfaasv1.EnvProvisioningVMs)
	case dfaasv1.EnvProvisioningVMs:
		return r.reconcileProvisioningVMs(ctx, &env)
	case dfaasv1.EnvProvisioningInfra:
		return r.reconcileProvisioningInfra(ctx, &env)
	case dfaasv1.EnvProvisioningMonitoring:
		return r.reconcileProvisioningMonitoring(ctx, &env)
	case dfaasv1.EnvReady:
		// Generation drifted: restart from VMs.
		return r.setEnvPhase(ctx, &env, dfaasv1.EnvProvisioningVMs)
	case dfaasv1.EnvFailed:
		// Terminal failure. Recovery requires either a spec edit (drift block
		// restarts the FSM at the top of Reconcile) or delete-and-recreate.
		// No auto-restart — that would loop forever against persistently-failing
		// Ansible Jobs whose status survives across reconciles.
		logger.V(1).Info("environment failed, awaiting spec edit or recreation")
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

// setEnvPhase patches status.phase, retrying on conflict. When transitioning
// into Ready it also stamps observedGeneration so future ticks short-circuit.
// Returns no Requeue: the Status().Update emits a watch event that triggers
// reconcile when the informer cache catches up. Returning Requeue:true here
// would race the cache and re-fire the same phase handler for ~3s of log
// spam until the watch event arrived.
func (r *EnvironmentReconciler) setEnvPhase(ctx context.Context,
	env *dfaasv1.Environment, phase dfaasv1.EnvironmentPhase) (ctrl.Result, error) {

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(env), latest); err != nil {
			return err
		}
		latest.Status.Phase = phase
		if phase == dfaasv1.EnvReady {
			latest.Status.ObservedGeneration = latest.Generation
		}
		return r.Status().Update(ctx, latest)
	})
	return ctrl.Result{}, err
}

func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfaasv1.Environment{}).
		Complete(r)
}
