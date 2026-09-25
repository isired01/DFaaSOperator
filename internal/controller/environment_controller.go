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
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
	"dfaas-operator/internal/controller/monitoring"
	"dfaas-operator/internal/controller/statuswriter"
	"dfaas-operator/internal/reach"
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
	// Prober answers which declared Nodes do not respond on :22. Nil-safe:
	// prober() falls back to reach.TCP{}, the same convention APIReader and
	// Clientset use here.
	Prober reach.Prober
	// Monitoring is the Helm monitoring stack. Nil-safe: monitoring() falls
	// back to a real &monitoring.Manager{}, which performs real Helm installs.
	Monitoring monitoring.Stack
	// now is the clock the deletion drain budget runs on; nil means time.Now.
	// Tests move it forward, since the API server will not backdate a
	// DeletionTimestamp.
	now func() time.Time
}

// clock is the nil-safe accessor for now.
func (r *EnvironmentReconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// monitoringStack is the nil-safe accessor for Monitoring.
func (r *EnvironmentReconciler) monitoringStack() monitoring.Stack {
	if r.Monitoring != nil {
		return r.Monitoring
	}
	return &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
}

// prober is the nil-safe accessor for Prober.
func (r *EnvironmentReconciler) prober() reach.Prober {
	if r.Prober != nil {
		return r.Prober
	}
	return reach.TCP{}
}

//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=loadtests,verbs=get;list;watch;delete
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

	// Seed the provisioning snapshot for an Environment that settled before this
	// operator version started recording one. A settled Environment is, by the
	// FSM's own invariant, installed exactly as spec.nodes describes, so the
	// spec is a faithful record — and without this seed the FIRST node removal
	// after an operator upgrade has nothing to diff against and silently leaves
	// a live machine running k3s and the dfaas-agent. Never clobbers: a real
	// snapshot knows more than the current spec does.
	if env.Status.ObservedGeneration > 0 && env.Status.ObservedGeneration == env.Generation &&
		meta.IsStatusConditionTrue(env.Status.Conditions, dfaasv1.EnvCondInfrastructureReady) {
		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		logStatusErr(ctx, "seed provisioning snapshot", am.SaveSnapshotIfAbsent(ctx, &env))
	}

	if env.Status.Phase == dfaasv1.EnvReady &&
		env.Status.ObservedGeneration == env.Generation {
		// No longer idle while Ready: run a periodic SSH liveness probe so a
		// node that dies after provisioning is noticed instead of only
		// surfacing when a test fails against it.

		// Re-assert this Environment's Prometheus target file on every Ready
		// tick: the write at the end of provisioning is best-effort, and a
		// legacy "<name>.json" key from before the namespaced key migrates
		// here. ReconcileTargets writes nothing when the file is current.
		logStatusErr(ctx, "reconcile Prometheus targets", r.monitoringStack().ReconcileTargets(ctx, &env))
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
		return r.phase(ctx, &env, dfaasv1.EnvProvisioningVMs)
	case dfaasv1.EnvProvisioningVMs:
		return r.reconcileProvisioningVMs(ctx, &env)
	case dfaasv1.EnvProvisioningInfra:
		return r.reconcileProvisioningInfra(ctx, &env)
	case dfaasv1.EnvProvisioningMonitoring:
		return r.reconcileProvisioningMonitoring(ctx, &env)
	case dfaasv1.EnvReady:
		// Generation drifted: restart from VMs.
		return r.phase(ctx, &env, dfaasv1.EnvProvisioningVMs)
	case dfaasv1.EnvUnreachable:
		// Non-terminal: nodes stopped answering SSH. Re-probe indefinitely and
		// auto-recover when they return (→ Ready or → ProvisioningInfra).
		return r.reconcileUnreachable(ctx, &env)
	case dfaasv1.EnvFailed:
		// Settled but not Ready: provisioning failed (terminal). No auto-retry
		// — that would loop forever against a persistently broken chart / OCI
		// registry. Recovery is a spec edit (drift block above) or
		// delete+recreate.
		if env.Status.ObservedGeneration == 0 {
			// Settled without ever stamping observedGeneration (failed before
			// reaching Ready, or pre-dates the settle-time stamp). Record the
			// current generation so a later spec edit is seen as drift and
			// re-triggers provisioning.
			return r.phase(ctx, &env, env.Status.Phase)
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
// One rule for every phase: drift mid-provisioning (or while Unreachable)
// restarts the run too, so an edit that lands after the Ansible Jobs ran is
// never stamped installed. The new run's ProvisioningInfra waits for the older
// generation's Jobs to terminate before creating its own, so two generations
// of playbooks never run on the same machines.
func (r *EnvironmentReconciler) handleGenerationDrift(ctx context.Context,
	env *dfaasv1.Environment) (handled bool, res ctrl.Result, err error) {
	// Settled phases compare what the last run installed; every other phase
	// compares what the current run is applying. 0 means "no record": do
	// nothing (a fresh Environment, or an object an older operator wrote).
	recorded := env.Status.ObservedGeneration
	if !isSettledPhase(env.Status.Phase) {
		recorded = env.Status.ProvisioningGeneration
	}
	if recorded == 0 || recorded >= env.Generation {
		return false, ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("generation drift detected — restarting provisioning",
		"phase", env.Status.Phase,
		"observed", env.Status.ObservedGeneration, "current", env.Generation)
	if _, cerr := r.cleanupStaleGenJobs(ctx, env); cerr != nil {
		logger.Error(cerr, "stale-gen Job cleanup failed")
	}
	if rerr := r.resetTransientConditions(ctx, env); rerr != nil {
		logger.Error(rerr, "reset transient Conditions failed")
	}
	res, err = r.phase(ctx, env, dfaasv1.EnvProvisioningVMs)
	return true, res, err
}

// writer is the one way this reconciler persists Environment status. On a
// settled phase (Ready, Failed) SetPhase also stamps
// observedGeneration: it records the spec generation the controller has
// finished processing, so a later spec edit is detected as drift at the top
// of Reconcile. Without it an Environment that failed on its first provision
// kept observedGeneration == 0, the drift guard skipped it, and a spec edit
// from the UI was silently ignored — wedging it in Failed forever.
func (r *EnvironmentReconciler) writer() statuswriter.Writer[*dfaasv1.Environment, dfaasv1.EnvironmentPhase] {
	return statuswriter.Writer[*dfaasv1.Environment, dfaasv1.EnvironmentPhase]{
		Client: r.Client,
		New:    func() *dfaasv1.Environment { return &dfaasv1.Environment{} },
		SetPhase: func(env *dfaasv1.Environment, p dfaasv1.EnvironmentPhase) {
			env.Status.Phase = p
			// Every provisioning run starts at ProvisioningVMs: record the
			// generation it applies, from the copy being written.
			if p == dfaasv1.EnvProvisioningVMs {
				env.Status.ProvisioningGeneration = env.Generation
			}
			// A settle stamps the generation the run applied, not whatever the
			// re-fetched object carries now: an edit that landed mid-run must
			// stay drift. No record (an object an older operator wrote) falls
			// back to metadata.generation, never 0.
			if isSettledPhase(p) {
				if env.Status.ProvisioningGeneration > 0 {
					env.Status.ObservedGeneration = env.Status.ProvisioningGeneration
				} else {
					env.Status.ObservedGeneration = env.Generation
				}
			}
		},
		Aggregate:  func(env *dfaasv1.Environment, p dfaasv1.EnvironmentPhase, _, _ string) { stampEnvAggregate(env, p) },
		Conditions: func(env *dfaasv1.Environment) *[]metav1.Condition { return &env.Status.Conditions },
	}
}

// budget builds one named Retry counter with its ceiling. The counters stay
// generation-scoped (ADR-0003); Budget owns only the policy on top.
func (r *EnvironmentReconciler) budget(counter string, limit int) statuswriter.Budget[*dfaasv1.Environment, dfaasv1.EnvironmentPhase] {
	return statuswriter.Budget[*dfaasv1.Environment, dfaasv1.EnvironmentPhase]{
		Writer: r.writer(), Counter: counter, Limit: limit,
	}
}

// cond stamps one Condition, best-effort (logged, never fatal).
func (r *EnvironmentReconciler) cond(ctx context.Context, env *dfaasv1.Environment,
	condType string, status metav1.ConditionStatus, reason, message string) {
	r.writer().RecordBestEffort(ctx, env, envTransition{
		Conditions: []statuswriter.Cond{{Type: condType, Status: status, Reason: reason, Message: message}},
	})
}

// phase moves the Environment to p. No requeue: every Environment phase
// handler already returns its own RequeueAfter, and the status watch
// re-enqueues on the write. The reconciler's scheduling policy, stated once.
func (r *EnvironmentReconciler) phase(ctx context.Context, env *dfaasv1.Environment,
	p dfaasv1.EnvironmentPhase) (ctrl.Result, error) {
	return ctrl.Result{}, r.writer().Record(ctx, env, envTransition{Phase: &p})
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
