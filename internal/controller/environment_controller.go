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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Clientset reads management-cluster Pod logs (controller-runtime's client
	// cannot stream logs). Used best-effort to extract the failing Ansible task
	// from a retained failed Job pod; nil-safe (unit tests leave it unset).
	Clientset kubernetes.Interface
	// APIReader is an uncached reader used to confirm status.lastHealthCheck
	// before running an SSH probe. The informer cache lags this controller's own
	// status writes, and reading a stale timestamp let a second probe through
	// milliseconds after the first — collapsing the health-miss budget. Nil-safe
	// (unit tests leave it unset and fall back to the cached value).
	APIReader client.Reader
}

//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=environments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=environments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=environments/finalizers,verbs=update

//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps;services;secrets;serviceaccounts;persistentvolumeclaims;namespaces,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods/log,verbs=get
//+kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets;replicasets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *EnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var env dfaasv1.Environment
	if err := r.Get(ctx, req.NamespacedName, &env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !env.DeletionTimestamp.IsZero() {
		return r.handleEnvDeletion(ctx, &env)
	}
	if added, err := ensureFinalizer(ctx, r.Client, &env, environmentFinalizer); added || err != nil {
		return ctrl.Result{}, err
	}

	if env.Status.Phase == dfaasv1.EnvReady &&
		env.Status.ObservedGeneration == env.Generation {
		// No longer idle while Ready: run a periodic SSH liveness probe so a
		// node that dies after provisioning is noticed instead of only
		// surfacing when a test fails against it.
		return r.reconcileReadyHealth(ctx, &env)
	}

	// Generation drift: spec was edited after a settled run. Restart the FSM
	// from ProvisioningVMs (see handleGenerationDrift). Done in one place so the
	// stale-Job cleanup runs once per drift, not on every reconcile of the
	// subsequent re-provisioning window.
	if handled, res, err := r.handleGenerationDrift(ctx, &env); handled || err != nil {
		return res, err
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
	case dfaasv1.EnvUnreachable:
		// Non-terminal: nodes stopped answering SSH. Re-probe indefinitely and
		// auto-recover when they return (→ Ready or → ProvisioningInfra).
		return r.reconcileUnreachable(ctx, &env)
	case dfaasv1.EnvDegraded, dfaasv1.EnvFailed:
		// Settled but not Ready. Degraded = infra up, monitoring broken (tests
		// still permitted); Failed = provisioning failed (terminal). Neither
		// auto-retries — that would loop forever against a persistently broken
		// chart / OCI registry. Recovery is a spec edit (drift block above)
		// or delete+recreate.
		if env.Status.ObservedGeneration == 0 {
			// Settled without ever stamping observedGeneration (failed before
			// reaching Ready, or pre-dates the settle-time stamp). Record the
			// current generation so a later spec edit is seen as drift and
			// re-triggers provisioning.
			return r.setEnvPhase(ctx, &env, env.Status.Phase)
		}
		logger.V(1).Info("environment settled, awaiting spec edit or recreation",
			"phase", env.Status.Phase)
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

// handleGenerationDrift detects a spec edit applied after a settled run
// (observedGeneration older than the current generation) and restarts the FSM:
// it deletes stale-generation Ansible Jobs, resets the transient Conditions, and
// moves the phase back to ProvisioningVMs. Returns handled=true when it took over
// the reconcile — the caller must return res/err immediately.
//
// Cleanup runs only on this settled→ProvisioningVMs transition, not on every
// reconcile of the subsequent re-provisioning window: observedGeneration stays
// behind until the new run reaches Ready, but once the old-gen Jobs are deleted
// the fresh Jobs carry the current generation in their name and labels, so a
// repeated LIST would only churn finding nothing. Drift seen mid-provisioning
// (a non-settled phase) needs no cleanup — generation-scoped Job names already
// keep the previous run's Jobs from being mistaken for the current one.
func (r *EnvironmentReconciler) handleGenerationDrift(ctx context.Context,
	env *dfaasv1.Environment) (handled bool, res ctrl.Result, err error) {
	if env.Status.ObservedGeneration == 0 || env.Status.ObservedGeneration >= env.Generation {
		return false, ctrl.Result{}, nil
	}
	if !isSettledPhase(env.Status.Phase) {
		return false, ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("generation drift detected — restarting provisioning",
		"phase", env.Status.Phase,
		"observed", env.Status.ObservedGeneration, "current", env.Generation)
	if cerr := r.cleanupStaleGenJobs(ctx, env); cerr != nil {
		logger.Error(cerr, "stale-gen Job cleanup failed")
	}
	if rerr := r.resetTransientConditions(ctx, env); rerr != nil {
		logger.Error(rerr, "reset transient Conditions failed")
	}
	res, err = r.setEnvPhase(ctx, env, dfaasv1.EnvProvisioningVMs)
	return true, res, err
}

// setEnvPhase patches status.phase, retrying on conflict via atomicStatusUpdate.
// When transitioning into a settled phase (Ready, Failed, or Degraded) it also
// stamps observedGeneration so future ticks short-circuit and a later spec edit
// is seen as generation drift. Stamps the top-level EnvCondReady aggregator (P1).
func (r *EnvironmentReconciler) setEnvPhase(ctx context.Context,
	env *dfaasv1.Environment, phase dfaasv1.EnvironmentPhase) (ctrl.Result, error) {

	err := r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(env), func(latest *dfaasv1.Environment) error {
		latest.Status.Phase = phase
		// Stamp observedGeneration on every *settled* outcome — Ready, Failed,
		// and Degraded — not just Ready. It records the spec generation the
		// controller has finished processing, so a later spec edit (generation
		// bump) is detected as drift at the top of Reconcile and re-triggers
		// provisioning. Without this, an Environment that failed on its very
		// first provision kept observedGeneration == 0, the drift guard
		// (`observedGeneration > 0`) skipped it, and editing the spec from the
		// UI was silently ignored — wedging the Environment in Failed forever.
		if isSettledPhase(phase) {
			latest.Status.ObservedGeneration = latest.Generation
		}
		stampEnvAggregate(latest, phase)
		return nil
	})
	return ctrl.Result{}, err
}

// stampEnvAggregate writes the EnvCondReady aggregator on the in-memory
// Environment. Pure function on the status slice; caller must persist via
// Status().Update.
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
	case dfaasv1.EnvUnreachable:
		status = metav1.ConditionFalse
		reason = dfaasv1.EnvReasonSSHUnreachable
		message = "nodes unreachable via SSH; retrying automatically — see the NodesReachable / VMsReady condition"
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
}

func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfaasv1.Environment{}).
		Complete(r)
}
