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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	if env.Status.ObservedGeneration > 0 && env.Status.ObservedGeneration < env.Generation {
		if err := r.cleanupStaleGenJobs(ctx, &env); err != nil {
			logger.Error(err, "stale-gen Job cleanup failed")
		}
		if env.Status.Phase == dfaasv1.EnvReady ||
			env.Status.Phase == dfaasv1.EnvDegraded ||
			env.Status.Phase == dfaasv1.EnvFailed {
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
	case dfaasv1.EnvDegraded:
		// Non-terminal: infra is up but monitoring is broken. Tests are
		// still permitted. Recovery is via spec edit (drift block above)
		// or explicit deletion+recreate. Like EnvFailed we do not auto-
		// retry — that would loop forever against a persistently broken
		// chart / OCI registry.
		if env.Status.ObservedGeneration == 0 {
			// Settled without ever stamping (failed before reaching Ready, or
			// pre-dates the settle-time stamp). Record the current generation
			// so a later spec edit is seen as drift and re-triggers provisioning.
			return r.setEnvPhase(ctx, &env, dfaasv1.EnvDegraded)
		}
		logger.V(1).Info("environment degraded, awaiting spec edit or recreation")
		return ctrl.Result{}, nil
	case dfaasv1.EnvFailed:
		// Terminal failure. Recovery requires either a spec edit (drift block
		// restarts the FSM at the top of Reconcile) or delete-and-recreate.
		if env.Status.ObservedGeneration == 0 {
			// Settled without ever stamping (failed on first provision, or
			// pre-dates the settle-time stamp). Record the current generation
			// so a later spec edit is seen as drift and re-triggers provisioning.
			return r.setEnvPhase(ctx, &env, dfaasv1.EnvFailed)
		}
		logger.V(1).Info("environment failed, awaiting spec edit or recreation")
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

// setEnvPhase patches status.phase, retrying on conflict. When transitioning
// into a settled phase (Ready, Failed, or Degraded) it also stamps
// observedGeneration so future ticks short-circuit and a later spec edit is
// seen as generation drift. Stamps the top-level EnvCondReady aggregator (P1)
// and clears the EnvCondUpdating flag on settled phases (P8).
func (r *EnvironmentReconciler) setEnvPhase(ctx context.Context,
	env *dfaasv1.Environment, phase dfaasv1.EnvironmentPhase) (ctrl.Result, error) {

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Environment{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(env), latest); err != nil {
			return err
		}
		latest.Status.Phase = phase
		// Stamp observedGeneration on every *settled* outcome — Ready, Failed,
		// and Degraded — not just Ready. It records the spec generation the
		// controller has finished processing, so a later spec edit (generation
		// bump) is detected as drift at the top of Reconcile and re-triggers
		// provisioning. Without this, an Environment that failed on its very
		// first provision kept observedGeneration == 0, the drift guard
		// (`observedGeneration > 0`) skipped it, and editing the spec from the
		// UI was silently ignored — wedging the Environment in Failed forever.
		if phase == dfaasv1.EnvReady ||
			phase == dfaasv1.EnvFailed ||
			phase == dfaasv1.EnvDegraded {
			latest.Status.ObservedGeneration = latest.Generation
		}
		stampEnvAggregate(latest, phase)
		return r.Status().Update(ctx, latest)
	})
	return ctrl.Result{}, err
}

// stampEnvAggregate writes both the EnvCondReady aggregator and the
// EnvCondUpdating flag on the in-memory Environment. Pure function on the
// status slice; caller must persist via Status().Update.
func stampEnvAggregate(env *dfaasv1.Environment, phase dfaasv1.EnvironmentPhase) {
	var status metav1.ConditionStatus
	var reason, message string

	switch phase {
	case dfaasv1.EnvReady:
		status = metav1.ConditionTrue
		reason = dfaasv1.EnvReasonAllSubsystemsReady
		message = "infrastructure + monitoring up"
	case dfaasv1.EnvDegraded:
		status = metav1.ConditionFalse
		reason = dfaasv1.EnvReasonDegraded
		message = "infrastructure up, monitoring stack unavailable; LoadTests are still permitted"
	case dfaasv1.EnvFailed:
		status = metav1.ConditionFalse
		reason = dfaasv1.EnvReasonFailed
		message = "provisioning failed; spec edit or delete+recreate required"
	case "", dfaasv1.EnvIdle:
		status = metav1.ConditionUnknown
		reason = dfaasv1.EnvReasonInitializing
		message = "awaiting first reconcile"
	default:
		status = metav1.ConditionFalse
		reason = dfaasv1.EnvReasonProvisioning
		message = "subsystems still provisioning"
	}

	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type:    dfaasv1.EnvCondReady,
		Status:  status,
		Reason:  reason,
		Message: message,
	})

	// Clear the Updating flag once we've reached a terminal phase. Leave
	// it alone otherwise (resetTransientConditions sets it on drift; the
	// flag stays True through every Provisioning* tick until we settle).
	if phase == dfaasv1.EnvReady ||
		phase == dfaasv1.EnvDegraded ||
		phase == dfaasv1.EnvFailed {
		meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
			Type:    dfaasv1.EnvCondUpdating,
			Status:  metav1.ConditionFalse,
			Reason:  dfaasv1.EnvReasonAllSubsystemsReady,
			Message: "no spec update in flight",
		})
	}
}

func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfaasv1.Environment{}).
		Complete(r)
}
