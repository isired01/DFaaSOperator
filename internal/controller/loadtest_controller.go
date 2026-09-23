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
	"sync"
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
	"dfaas-operator/internal/controller/statuswriter"
	"dfaas-operator/internal/k6dispatch"
	"dfaas-operator/internal/syncchannel"
)

const loadTestFinalizer = "dfaas.dfaas.io/loadtest-finalizer"

// LoadTestReconciler owns the LoadTest CRD lifecycle. It uses a Lookup
// pattern against the referenced Environment: nothing happens until the
// Environment is Ready, after which the reconciler dispatches one k6 TestRun
// per PerNodeLoad entry to the matching k6 machine's k3s cluster, then runs
// the metrics exporter.
type LoadTestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Dispatcher is the seam to the k6 fleet: k6dispatch.Live in production,
	// k6dispatch/fake.Fleet in tests. Everything about reaching a node lives
	// behind it; the reconciler only knows nodeIDs.
	Dispatcher k6dispatch.Dispatcher
	// APIReader reads straight from the API server, bypassing the informer
	// cache. Used for the single decisive List in envOccupancyGate: the
	// single-active-test-per-Environment invariant is decided from that List
	// while sibling TestRuns are persisted through direct Status().Update
	// calls, so a lagging cache can show two tests an empty Environment.
	// Nil-safe: falls back to the cached client when unset (unit tests).
	APIReader client.Reader
	// Sync is the authless object channel shared with the k6 VMs: the GO
	// signal that opens the Sync barrier and the per-Generator summaries.
	// Nil-safe -- syncChannel() falls back to one resolved from the
	// environment, resolved once for the whole process.
	Sync syncchannel.Channel

	// retryEvery paces remote rounds after a failed one; zero means
	// remoteRetryInterval. Tests that drive Reconcile back to back set it tiny.
	retryEvery time.Duration
	// lastMiss holds, per LoadTest, when its last remote round failed.
	lastMiss sync.Map
}

// envSyncChannel is the process-wide fallback: the bases are read from the
// environment exactly once, never per operation.
var envSyncChannel = sync.OnceValue(func() syncchannel.Channel {
	return syncchannel.FromEnv()
})

// syncChannel is the nil-safe accessor for Sync.
// budget builds one named Retry counter with its ceiling.
func (r *LoadTestReconciler) budget(counter string, limit int) statuswriter.Budget[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase] {
	return statuswriter.Budget[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{
		Writer: r.writer(), Counter: counter, Limit: limit,
	}
}

func (r *LoadTestReconciler) syncChannel() syncchannel.Channel {
	if r.Sync != nil {
		return r.Sync
	}
	return envSyncChannel()
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
	if added, err := ensureFinalizer(ctx, r.Client, &lt, loadTestFinalizer); added || err != nil {
		return ctrl.Result{}, err
	}

	// Terminal phases — no-op, unless the run end left a runner it applied
	// undeleted: keep re-sweeping it, since Occupancy holds the Environment.
	if lt.Status.Phase.Terminal() {
		if runnersUnreclaimed(&lt) {
			return r.retryReclaim(ctx, &lt)
		}
		return ctrl.Result{}, nil
	}

	// Pace remote retries: the watch event of a failed round's own counter or
	// status write must not start the next attempt at once.
	if wait := r.paced(&lt); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	// Lookup target Environment in the same namespace.
	var env dfaasv1.Environment
	envKey := types.NamespacedName{Name: lt.Spec.TargetEnvironment, Namespace: lt.Namespace}
	if err := r.Get(ctx, envKey, &env); err != nil {
		if apierrors.IsNotFound(err) {
			// P9: surface link state on EnvironmentLinked, in the same write.
			msg := fmt.Sprintf("environment %q not found in namespace %s",
				lt.Spec.TargetEnvironment, lt.Namespace)
			return r.failLoadTest(ctx, &lt, msg, statuswriter.Cond{Type: dfaasv1.LTCondEnvironmentLinked,
				Status: metav1.ConditionFalse, Reason: dfaasv1.LTReasonEnvNotFound, Message: msg})
		}
		return ctrl.Result{}, err
	}

	// P9: env exists — stamp EnvironmentLinked.
	switch env.Status.Phase {
	case dfaasv1.EnvFailed:
		r.cond(ctx, &lt, dfaasv1.LTCondEnvironmentLinked,
			metav1.ConditionFalse, dfaasv1.LTReasonEnvFailed,
			fmt.Sprintf("environment %q is in phase Failed", env.Name))
	default:
		r.cond(ctx, &lt, dfaasv1.LTCondEnvironmentLinked,
			metav1.ConditionTrue, dfaasv1.LTReasonEnvFound,
			fmt.Sprintf("environment %q resolved", env.Name))
	}

	// Abort short-circuit. User PATCHed spec.stop=true.
	if lt.Spec.Stop {
		inAbortWindow := lt.Status.Phase.PreExecution() ||
			lt.Status.Phase == dfaasv1.LoadTestRunning
		if inAbortWindow {
			return r.abortLoadTest(ctx, &lt, dfaasv1.LTReasonUserAborted,
				"The test was manually aborted from the UI.")
		}
	}

	// An Environment being deleted dispatches nothing new: its finalizer is
	// draining the tests it owns before it prunes their kubeconfig Secrets.
	// ponytail: a test created meanwhile never gets an OwnerRef and ends
	// Failed/EnvNotFound once the Environment is gone.
	if !env.DeletionTimestamp.IsZero() && lt.Status.Phase.PreExecution() {
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	// Park every suspended test (draft or scheduled) at Pending on its first
	// pass, before the scheduled branch can arm or fire it. Every later
	// un-suspend -- the fire PATCH below, the gateway's Activate, a kubectl
	// patch -- then lands on a Pending test, outside the phase == "" guards
	// that follow. Before this, a scheduled test sat at "" until it fired and
	// the startAt guard below failed it.
	if lt.Spec.Suspended && lt.Status.Phase == "" {
		return r.phase(ctx, &lt, dfaasv1.LoadTestPending, "", "")
	}

	// Scheduled-start branch (P9: stamps LTCondScheduled).
	if lt.Spec.StartAt != nil && lt.Status.Phase.PreExecution() && lt.Spec.Suspended {

		fireT := lt.Spec.StartAt.Time
		switch {
		case time.Now().Before(fireT):
			// Armed: future startAt.
			r.cond(ctx, &lt, dfaasv1.LTCondScheduled,
				metav1.ConditionTrue, dfaasv1.LTReasonScheduledArmed,
				"armed for "+fireT.UTC().Format(time.RFC3339))
			return ctrl.Result{RequeueAfter: time.Until(fireT)}, nil

		case !env.Status.Phase.Dispatchable():
			// Fire time elapsed but the target Environment is not Ready.
			r.cond(ctx, &lt, dfaasv1.LTCondScheduled,
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
			r.cond(ctx, &lt, dfaasv1.LTCondScheduled,
				metav1.ConditionTrue, dfaasv1.LTReasonScheduledFired,
				"schedule fired at "+fireT.UTC().Format(time.RFC3339))
			return ctrl.Result{Requeue: true}, nil
		}
	}

	// startAt is honoured only on a suspended LoadTest: the scheduled branch
	// above requires Suspended, so a test carrying startAt without it falls
	// straight through and dispatches immediately, ignoring the schedule with
	// no diagnostic anywhere. The gateway rejects that on both create paths;
	// a kubectl apply reaches here instead, so fail it loudly. Every suspended
	// test was parked at Pending above, so a test here at phase "" with
	// startAt and without suspended was created, or un-suspended, before the
	// operator ever parked it -- either way the schedule was never armed.
	if lt.Status.Phase == "" && lt.Spec.StartAt != nil && !lt.Spec.Suspended {
		return r.failLoadTest(ctx, &lt,
			"spec.startAt requires spec.suspended=true; otherwise the schedule is ignored and the test starts immediately")
	}

	// Strict create-time gate.
	if lt.Status.Phase == "" && !lt.Spec.Suspended && !env.Status.Phase.Dispatchable() {
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
	preExecution := lt.Status.Phase.PreExecution()
	if preExecution {
		if lt.Spec.Suspended {
			// Save-as-Draft or armed schedule, parked at Pending above:
			// dispatch nothing until spec.suspended=false.
			return ctrl.Result{}, nil
		}

		// Block while Environment is mid-flight.
		if !env.Status.Phase.Dispatchable() {
			logger.Info("waiting for environment", "env", env.Name, "phase", env.Status.Phase)
			return r.phase(ctx, &lt, dfaasv1.LoadTestPending, "", "")
		}
	}

	// Env occupancy gate (FIFO serialization). Only in the pre-execution
	// window, for an active (non-draft) test against a usable Environment.
	// Sits after the scheduled-fire branch, so a just-fired scheduled test is
	// caught here on its next reconcile and queued rather than colliding with
	// a sibling already running on the same Environment.
	if preExecution && !lt.Spec.Suspended &&
		env.Status.Phase.Dispatchable() {
		proceed, res, err := r.envOccupancyGate(ctx, &lt)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !proceed {
			return res, nil
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

// stampLTAggregate writes the LTCondReady aggregator (P9) onto the in-memory
// LoadTest. Pure function; caller persists via Status().Update. A non-empty
// reasonOverride / msgOverride replaces the generic per-phase value.
func stampLTAggregate(lt *dfaasv1.LoadTest, phase dfaasv1.LoadTestPhase, reasonOverride, msgOverride string) {
	var ready metav1.ConditionStatus
	var reason, message string
	switch phase {
	case dfaasv1.LoadTestCompleted:
		ready = metav1.ConditionTrue
		reason = dfaasv1.LTReasonCompleted
		message = "metrics exported, test complete"
	case dfaasv1.LoadTestFailed:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonFailed
		message = "load test failed"
	case dfaasv1.LoadTestAborted:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonAborted
		message = "load test aborted"
	case dfaasv1.LoadTestRunning:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonRunning
		message = "remote TestRuns dispatched, k6 running"
	case dfaasv1.LoadTestExporting:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonExporterRunning
		message = "k6 finished, metrics export in progress"
	case dfaasv1.LoadTestPending:
		ready = metav1.ConditionFalse
		reason = dfaasv1.LTReasonPending
		message = "pending — waiting for environment or activation"
	default:
		ready = metav1.ConditionUnknown
		reason = dfaasv1.LTReasonPending
		message = "awaiting first reconcile"
	}
	if reasonOverride != "" {
		reason = reasonOverride
	}
	if msgOverride != "" {
		message = msgOverride
	}
	meta.SetStatusCondition(&lt.Status.Conditions, metav1.Condition{
		Type:    dfaasv1.LTCondReady,
		Status:  ready,
		Reason:  reason,
		Message: message,
	})
}

// failLoadTest ends the run as Failed, carrying the caller's message onto the
// Ready aggregator. Pass diagnostics as conds so they land in the same write;
// endRun reclaims whatever the run may still have live.
func (r *LoadTestReconciler) failLoadTest(ctx context.Context,
	lt *dfaasv1.LoadTest, message string, conds ...statuswriter.Cond) (ctrl.Result, error) {

	return r.endRun(ctx, lt, dfaasv1.LoadTestFailed, dfaasv1.LTReasonFailed, message, conds...)
}

// writer is the one way this reconciler persists LoadTest status. Stateless;
// built per call from the client.
func (r *LoadTestReconciler) writer() statuswriter.Writer[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase] {
	return statuswriter.Writer[*dfaasv1.LoadTest, dfaasv1.LoadTestPhase]{
		Client:     r.Client,
		New:        func() *dfaasv1.LoadTest { return &dfaasv1.LoadTest{} },
		SetPhase:   func(lt *dfaasv1.LoadTest, p dfaasv1.LoadTestPhase) { lt.Status.Phase = p },
		Aggregate:  stampLTAggregate,
		Conditions: func(lt *dfaasv1.LoadTest) *[]metav1.Condition { return &lt.Status.Conditions },
	}
}

// cond stamps one Condition, best-effort: a failed stamp is logged, never
// fatal to the reconcile. Hot-path state changes go through phase or Record.
func (r *LoadTestReconciler) cond(ctx context.Context, lt *dfaasv1.LoadTest,
	condType string, status metav1.ConditionStatus, reason, message string) {
	r.writer().RecordBestEffort(ctx, lt, ltTransition{
		Conditions: []statuswriter.Cond{{Type: condType, Status: status, Reason: reason, Message: message}},
	})
}

// condErr is cond for the few sites where a failed stamp must surface to the
// caller (the occupancy gate: a queued test whose Queued condition did not
// land would look un-queued to the UI).
func (r *LoadTestReconciler) condErr(ctx context.Context, lt *dfaasv1.LoadTest,
	condType string, status metav1.ConditionStatus, reason, message string) error {
	return r.writer().Record(ctx, lt, ltTransition{
		Conditions: []statuswriter.Cond{{Type: condType, Status: status, Reason: reason, Message: message}},
	})
}

// phase moves the LoadTest to p, with conds stamped in the same write, and
// requeues immediately so the next phase handler runs without waiting for the
// watch. reason/message override the Ready aggregator's generic text (empty =
// generic). After a terminal write it does not requeue: an immediate pass would
// read the pre-terminal cached copy and stamp in-progress Conditions onto an
// ended test; the write's own watch event arrives once the cache holds it.
// This is the reconciler's scheduling policy, stated once here; the writer
// itself never decides requeue.
func (r *LoadTestReconciler) phase(ctx context.Context, lt *dfaasv1.LoadTest,
	p dfaasv1.LoadTestPhase, reason, message string, conds ...statuswriter.Cond) (ctrl.Result, error) {
	if err := r.writer().Record(ctx, lt, ltTransition{Phase: &p, Reason: reason, Message: message, Conditions: conds}); err != nil {
		return ctrl.Result{}, err
	}
	if p.Terminal() {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{Requeue: true}, nil
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

// envOccupancyGate serializes LoadTests that share a target Environment into a
// FIFO queue. It is evaluated in the pre-execution window only, just before the
// phase machine. Returns proceed=true when this test may dispatch (env free AND
// this test is at the front of the queue); otherwise it holds the test at
// Pending with a Queued condition and returns a 10s requeue. The 10s polling
// (plus MaxConcurrentReconciles=1 and the FIFO front rule) guarantees a single
// in-flight dispatch per Environment without any extra watch wiring: when the
// occupant reaches a terminal phase, the next tick lets the front test through.
//
// Queue model:
//   - "Occupant" = self, once it has persisted remote TestRuns (rule 0): it
//     holds the env until terminal and bypasses the gate so a partial dispatch
//     is never handed off mid-flight.
//   - "Busy"   = a sibling LoadTest on the same env in phase Running OR Exporting,
//     OR still "" / Pending but already holding remote TestRuns (partial dispatch).
//   - "Waiting" = siblings (incl. self) with phase ∈ {"", Pending}, no TestRuns yet,
//     not suspended, and whose schedule has already fired (startAt nil or past).
//   - "Front"   = the waiting sibling with the oldest creationTimestamp
//     (tie-break: lexicographic name). Starvation is bounded by creation order.
func (r *LoadTestReconciler) envOccupancyGate(ctx context.Context,
	lt *dfaasv1.LoadTest) (proceed bool, res ctrl.Result, err error) {

	var list dfaasv1.LoadTestList
	if err := r.occupancyReader().List(ctx, &list, client.InNamespace(lt.Namespace)); err != nil {
		return false, ctrl.Result{}, err
	}

	// hold keeps this test at Pending (re-queued) with a Queued condition.
	hold := func(reason, message string) (bool, ctrl.Result, error) {
		if cerr := r.condErr(ctx, lt, dfaasv1.LTCondQueued,
			metav1.ConditionTrue, reason, message); cerr != nil {
			return false, ctrl.Result{}, cerr
		}
		if lt.Status.Phase != dfaasv1.LoadTestPending {
			if _, perr := r.phase(ctx, lt, dfaasv1.LoadTestPending, "", ""); perr != nil {
				return false, ctrl.Result{}, perr
			}
		}
		return false, ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// 0. If this test has already dispatched remote TestRuns, it is the
	// de-facto occupant of the Environment: a partial-dispatch reconcile can
	// leave it at Pending (startK6 flips to Running only after the last node)
	// with live remote runs. Proceed unconditionally so startK6 resumes /
	// observeK6 takes over — never defer to a sibling here, which would strand
	// these runs AND let a second TestRun set dispatch concurrently on the same
	// Environment (the single-active-test invariant this gate exists to hold).
	if len(lt.Status.TestRuns) > 0 {
		if cerr := r.condErr(ctx, lt, dfaasv1.LTCondQueued,
			metav1.ConditionFalse, dfaasv1.LTReasonDispatching,
			"resuming in-flight dispatch — environment held by this test"); cerr != nil {
			return false, ctrl.Result{}, cerr
		}
		return true, ctrl.Result{}, nil
	}

	// 1. Busy check: any *other* sibling mid-execution on the same env. A
	// sibling counts as occupying not only in Running/Exporting but also when
	// it is still "" / Pending yet has already persisted remote TestRuns — the
	// partial-dispatch window handled by rule 0 above, invisible to a naive
	// phase-only check.
	for i := range list.Items {
		sib := &list.Items[i]
		if sib.Name == lt.Name || sib.Spec.TargetEnvironment != lt.Spec.TargetEnvironment {
			continue
		}
		if runnersUnreclaimed(sib) {
			return hold(dfaasv1.LTReasonEnvBusy,
				fmt.Sprintf("waiting: load test %q has ended but a runner it applied on the generators "+
					"could not be deleted; the operator keeps retrying — delete that test to release environment %q",
					sib.Name, lt.Spec.TargetEnvironment))
		}
		occupying := sib.Status.Phase == dfaasv1.LoadTestRunning ||
			sib.Status.Phase == dfaasv1.LoadTestExporting ||
			(sib.Status.Phase.PreExecution() && len(sib.Status.TestRuns) > 0)
		if occupying {
			return hold(dfaasv1.LTReasonEnvBusy,
				fmt.Sprintf("waiting: environment %q occupied by load test %q",
					lt.Spec.TargetEnvironment, sib.Name))
		}
	}

	// 2. FIFO front check: pick the oldest waiting sibling.
	now := time.Now()
	var front *dfaasv1.LoadTest
	for i := range list.Items {
		sib := &list.Items[i]
		if sib.Spec.TargetEnvironment != lt.Spec.TargetEnvironment {
			continue
		}
		if !sib.Status.Phase.PreExecution() {
			continue
		}
		if sib.Spec.Suspended {
			continue
		}
		// ponytail: an un-suspended test still carrying a future startAt
		// (kubectl, or a pre-fix gateway's Start) is skipped here too, so younger
		// waiting tests may go first until its startAt passes. Bounded priority
		// inversion; it still dispatches once none waits.
		if sib.Spec.StartAt != nil && now.Before(sib.Spec.StartAt.Time) {
			continue
		}
		if front == nil || loadTestBefore(sib, front) {
			front = sib
		}
	}
	if front != nil && front.Name != lt.Name {
		return hold(dfaasv1.LTReasonQueuedBehind,
			fmt.Sprintf("position behind load test %q in the queue for environment %q",
				front.Name, lt.Spec.TargetEnvironment))
	}

	// 3. Env free and this test is the front — clear the Queued gate and let
	// the phase machine dispatch.
	if cerr := r.condErr(ctx, lt, dfaasv1.LTCondQueued,
		metav1.ConditionFalse, dfaasv1.LTReasonDispatching,
		"environment free and test at front of queue — dispatching"); cerr != nil {
		return false, ctrl.Result{}, cerr
	}
	return true, ctrl.Result{}, nil
}

// occupancyReader returns the reader backing the occupancy List: the
// cache-bypassing APIReader when wired, otherwise the cached client.
func (r *LoadTestReconciler) occupancyReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// loadTestBefore orders two LoadTests by creationTimestamp, tie-broken by name.
func loadTestBefore(a, b *dfaasv1.LoadTest) bool {
	at, bt := a.CreationTimestamp.Time, b.CreationTimestamp.Time
	if at.Equal(bt) {
		return a.Name < b.Name
	}
	return at.Before(bt)
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
