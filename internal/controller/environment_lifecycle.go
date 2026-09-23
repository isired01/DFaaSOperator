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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
	"dfaas-operator/internal/controller/roles"
	"dfaas-operator/internal/controller/statuswriter"
)

// monitoringAttemptsAnnotation persists the consecutive-error counter for
// the monitoring Helm install across reconciles. P4 mirror of the LoadTest
// dispatch budget.
const monitoringAttemptsAnnotation = "dfaas.dfaas.io/monitoring-attempts"

// monitoringRetryBudget is the max consecutive Helm install failures
// tolerated before the Environment is moved to EnvFailed (P4).
const monitoringRetryBudget = 5

// handleEnvDeletion drains per-environment cluster-wide state (Prometheus
// targets) and removes the finalizer. Per-environment Jobs and ConfigMaps
// carry ControllerReferences and are garbage-collected automatically.
//
// The finalizer is dropped only once CleanupTargets succeeds: it is the single
// piece of state that does NOT cascade (the prometheus-targets ConfigMap is
// shared across environments and owned by none), so releasing the CR on a
// failed cleanup would leave this environment's scrape file behind forever,
// with Prometheus retrying dead targets. A failed cleanup is returned so the
// deletion is retried with backoff — same guarantee the LoadTest finalizer
// gives for remote TestRuns.
func (r *EnvironmentReconciler) handleEnvDeletion(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(env, environmentFinalizer) {
		// Drain the LoadTests this Environment owns that may still have work on
		// the generators, before the kubeconfig Secrets below are pruned: their
		// finalizers need those Secrets to reach the remote k3s. At most one
		// test per Environment can have live or held runners (Occupancy), so
		// the wait is about one teardown pass plus the deletion budget.
		// ponytail: kubectl delete --cascade=foreground lets GC delete the
		// Secrets in parallel and defeats this ordering; background (the
		// default, and what the gateway uses) keeps it.
		var lts dfaasv1.LoadTestList
		if err := r.List(ctx, &lts, client.InNamespace(env.Namespace)); err != nil {
			return ctrl.Result{}, fmt.Errorf("list loadtests: %w", err)
		}
		draining := 0
		for i := range lts.Items {
			lt := &lts.Items[i]
			if !hasOwnerRef(lt, env) || (lt.Status.Phase.Terminal() && !runnersUnreclaimed(lt)) {
				continue
			}
			if lt.DeletionTimestamp.IsZero() {
				if err := r.Delete(ctx, lt); client.IgnoreNotFound(err) != nil {
					return ctrl.Result{}, fmt.Errorf("delete loadtest %s: %w", lt.Name, err)
				}
			}
			draining++
		}
		if draining > 0 {
			logger.Info("environment deletion: waiting for its load tests to reclaim their runners", "count", draining)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}

		logger.Info("environment deletion: cleaning up Prometheus targets")

		if err := r.monitoringStack().CleanupTargets(ctx, env); err != nil {
			logger.Error(err, "cleanup Prometheus targets failed; keeping the finalizer and retrying")
			return ctrl.Result{}, fmt.Errorf("cleanup Prometheus targets: %w", err)
		}

		// Kubeconfig Secrets pushed before the playbook started stamping
		// ownerReferences have no owner, so GC cannot reach them. Held to the
		// same guarantee as CleanupTargets: keep the finalizer on failure
		// rather than leaking node credentials into the namespace forever.
		if err := r.pruneK6Kubeconfigs(ctx, env, nil); err != nil {
			logger.Error(err, "cleanup k6 kubeconfig Secrets failed; keeping the finalizer and retrying")
			return ctrl.Result{}, fmt.Errorf("cleanup k6 kubeconfig secrets: %w", err)
		}

		controllerutil.RemoveFinalizer(env, environmentFinalizer)
		if err := r.Update(ctx, env); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// reconcileProvisioningVMs probes SSH reachability of every node in spec
// before advancing. P6 (option b): dial each <ip>:22 with a 2s timeout; if
// any node fails, stamp VMsReady=False/SSHUnreachable and requeue. If the
// spec has zero nodes (placeholder envs), keep the historical Skipped
// behavior so empty envs still advance.
func (r *EnvironmentReconciler) reconcileProvisioningVMs(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(env.Spec.Nodes) == 0 {
		r.cond(ctx, env, dfaasv1.EnvCondVMsReady,
			metav1.ConditionTrue, dfaasv1.EnvReasonSkipped,
			"no nodes declared — placeholder phase")
		return r.phase(ctx, env, dfaasv1.EnvProvisioningInfra)
	}

	unreachable := r.prober().Unreachable(ctx, env)
	if len(unreachable) > 0 {
		budget := r.budget(sshAttemptsAnnotation, sshRetryBudget)
		outcome, bumpErr := budget.Attempt(ctx, env)
		if bumpErr != nil {
			logger.Error(bumpErr, "bumpSSHAttempts failed; the budget cannot advance")
		}
		if outcome.Exhausted {
			logger.Info("VMs not SSH-reachable after fast-retry budget; entering Unreachable (will keep retrying)",
				"nodes", unreachable, "attempts", outcome.Count)
			r.cond(ctx, env, dfaasv1.EnvCondVMsReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonSSHUnreachable,
				fmt.Sprintf("SSH :22 dial failed for %v after %d fast attempts; retrying every %s",
					unreachable, outcome.Count, unreachableRetryInterval))
			return r.phase(ctx, env, dfaasv1.EnvUnreachable)
		}
		logger.Info("VMs not SSH-reachable, retrying", "nodes", unreachable, "attempts", outcome.Count)
		r.cond(ctx, env, dfaasv1.EnvCondVMsReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonSSHUnreachable,
			fmt.Sprintf("SSH :22 dial failed for: %v", unreachable))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if rerr := r.budget(sshAttemptsAnnotation, sshRetryBudget).Clear(ctx, env); rerr != nil {
		logger.Error(rerr, "resetSSHAttempts failed; non-fatal")
	}
	r.cond(ctx, env, dfaasv1.EnvCondVMsReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonSSHReachable,
		"provisioning SSH check passed: all declared nodes reachable on :22")
	return r.phase(ctx, env, dfaasv1.EnvProvisioningInfra)
}

// reconcileProvisioningInfra runs the dfaas-worker Ansible Job and the k6
// Ansible Job concurrently (side-by-side resources, not goroutines). Fan-in:
// advances to ProvisioningMonitoring only when BOTH have reached a terminal
// state and succeeded; transitions to Failed only after BOTH have settled,
// so the Conditions reflect a coherent picture (e.g. "dfaas OK, k6 failed")
// rather than aborting one mid-flight.
func (r *EnvironmentReconciler) reconcileProvisioningInfra(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {

	// An older generation's playbooks must be gone before this generation's
	// start on the same machines.
	older, err := r.cleanupStaleGenJobs(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(older) > 0 {
		msg := "waiting for older-generation Ansible Job(s) to terminate: " + strings.Join(older, ", ")
		logStatusErr(ctx, "stamp older-generation wait", r.writer().Record(ctx, env, envTransition{Conditions: []statuswriter.Cond{
			{Type: dfaasv1.EnvCondDFaaSNodesReady, Status: metav1.ConditionUnknown, Reason: dfaasv1.EnvReasonJobPending, Message: msg},
			{Type: dfaasv1.EnvCondK6Ready, Status: metav1.ConditionUnknown, Reason: dfaasv1.EnvReasonJobPending, Message: msg},
		}}))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
	libp2pKeys, err := am.EnsureLibp2pKeys(ctx, env)
	if err != nil {
		log.FromContext(ctx).Error(err, "libp2p key ensure failed; retrying")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// One provisioning stream per role, every role driven every tick, so the
	// Conditions reflect a coherent picture ("dfaas OK, k6 failed") rather
	// than aborting one stream mid-flight.
	allTerminal, anyFailed := true, false
	for _, rs := range roles.All() {
		keys := libp2pKeys
		if !rs.NeedsLibp2pKeys {
			keys = nil
		}
		done, failed, err := r.ensureAnsibleJob(ctx, env, rs, keys)
		if err != nil {
			return ctrl.Result{}, err
		}
		allTerminal = allTerminal && (done || failed)
		anyFailed = anyFailed || failed
	}
	if !allTerminal {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if anyFailed {
		r.cond(ctx, env, dfaasv1.EnvCondInfrastructureReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonInfraFailed,
			"dfaas-worker or k6 provisioning failed; inspect DFaaSNodesReady and K6Ready conditions")
		return r.phase(ctx, env, dfaasv1.EnvFailed)
	}
	// Both Jobs succeeded and the fan-in has settled. Now — and only now — is
	// it safe to stamp the success TTL: neither sibling will be re-checked
	// again from this phase, so auto-deletion can no longer trigger a recreate.
	for _, rs := range roles.All() {
		r.patchAnsibleJobTTL(ctx, env, rs.JobSuffix, jobTTLSuccessSeconds)
	}
	// Record what Ansible just installed. Written here rather than at Ready
	// because the snapshot is about node installation, and monitoring is not:
	// an Environment whose infra succeeded but whose Helm monitoring then failed
	// terminally still HAS its machines installed, and would otherwise be left
	// with no record at all — so a later node removal would have nothing to diff
	// against and would silently orphan a live machine. Best-effort: a failed
	// write degrades safely (no snapshot ⇒ full re-provision, no teardown) and
	// the settled-Environment seed in Reconcile backfills it on the next tick.
	logStatusErr(ctx, "save provisioning snapshot", am.SaveSnapshot(ctx, env))
	r.cond(ctx, env, dfaasv1.EnvCondInfrastructureReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonInfraReady,
		"dfaas-worker and k6 Ansible Jobs completed")
	return r.phase(ctx, env, dfaasv1.EnvProvisioningMonitoring)
}

// reconcileProvisioningMonitoring installs Prometheus + Grafana via Helm,
// reconciles per-environment scrape targets, and stamps node status. Runs
// AFTER ProvisioningInfra so worker /metrics endpoints already exist when
// the first scrape fires.
func (r *EnvironmentReconciler) reconcileProvisioningMonitoring(ctx context.Context,
	env *dfaasv1.Environment) (ctrl.Result, error) {

	done, failed, err := r.ensureMonitoring(ctx, env)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failed {
		// Monitoring (incl. the SeaweedFS S3 sink) is required: a terminally
		// broken monitoring stack fails the Environment rather than degrading it,
		// so the operator surfaces it loudly instead of leaving a half-usable env.
		// Failed is terminal — no auto-retry; recovery is a spec edit or
		// delete+recreate (see the EnvFailed case in environment_controller.go).
		return r.phase(ctx, env, dfaasv1.EnvFailed)
	}
	if !done {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if err := r.syncNodeStatus(ctx, env); err != nil {
		log.FromContext(ctx).Error(err, "syncNodeStatus failed; retrying")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	// Both roles have finished provisioning by this point, so a node that
	// flipped away from k6-load-generator has already been repaved and its
	// pushed kubeconfig is dead. Best-effort: a stale Secret is inert (dispatch
	// resolves nodes through status.k6Nodes), so it must not hold up Ready.
	logStatusErr(ctx, "prune k6 kubeconfig secrets",
		r.pruneK6Kubeconfigs(ctx, env, k6NodeIDs(env)))
	return r.phase(ctx, env, dfaasv1.EnvReady)
}

// ensureMonitoring drives the Helm monitoring stack install. P4: after
// monitoringRetryBudget consecutive failures, returns failed=true so the
// caller transitions the Environment to EnvFailed. Resets the counter on
// every successful round-trip.
func (r *EnvironmentReconciler) ensureMonitoring(ctx context.Context,
	env *dfaasv1.Environment) (done bool, failed bool, err error) {
	logger := log.FromContext(ctx)

	mm := r.monitoringStack()
	if derr := mm.Deploy(ctx); derr != nil {
		budget := r.budget(monitoringAttemptsAnnotation, monitoringRetryBudget)
		outcome, bumpErr := budget.Attempt(ctx, env)
		if bumpErr != nil {
			logger.Error(bumpErr, "bumpMonitoringAttempts failed; the budget cannot advance")
		}
		if outcome.Exhausted {
			r.cond(ctx, env, dfaasv1.EnvCondMonitoringReady,
				metav1.ConditionFalse, dfaasv1.EnvReasonHelmFailed,
				fmt.Sprintf("monitoring Helm install failed %d consecutive times: %s",
					outcome.Count, condMessage(derr)))
			return false, true, nil
		}
		// P7 + P14: first ever observation is Unknown; subsequent retries
		// stay False/HelmInstalling. The sanitized message keeps
		// LastTransitionTime stable across reconciles when the error class
		// is the same. Outcome.First names what count == 1 meant -- and when
		// the counter could not be written it is false, so an unknown count
		// reads as a failure rather than as a first observation.
		condStatus := metav1.ConditionFalse
		if outcome.First {
			condStatus = metav1.ConditionUnknown
		}
		r.cond(ctx, env, dfaasv1.EnvCondMonitoringReady,
			condStatus, dfaasv1.EnvReasonHelmInstalling,
			"monitoring Helm install in progress / retrying: "+condMessage(derr))
		return false, false, nil
	}
	if rerr := r.budget(monitoringAttemptsAnnotation, monitoringRetryBudget).Clear(ctx, env); rerr != nil {
		logger.Error(rerr, "resetMonitoringAttempts failed; non-fatal")
	}
	ready, checkErr := mm.Check(ctx)
	if checkErr != nil {
		// "Could not evaluate" is not "not ready yet": a refused Pod List (RBAC
		// regression, API server down) would otherwise be indistinguishable from
		// a slow rollout and the Environment would sit in ProvisioningMonitoring
		// with a reassuring "pods not Ready yet" message.
		r.cond(ctx, env, dfaasv1.EnvCondMonitoringReady,
			metav1.ConditionUnknown, dfaasv1.EnvReasonCheckFailed,
			"monitoring readiness could not be evaluated: "+condMessage(checkErr))
		return false, false, nil
	}
	if !ready {
		r.cond(ctx, env, dfaasv1.EnvCondMonitoringReady,
			metav1.ConditionFalse, dfaasv1.EnvReasonWaitingPods,
			"monitoring pods not Ready yet")
		return false, false, nil
	}

	r.cond(ctx, env, dfaasv1.EnvCondMonitoringReady,
		metav1.ConditionTrue, dfaasv1.EnvReasonPodsRunning,
		"monitoring stack up")
	logStatusErr(ctx, "reconcile Prometheus targets", mm.ReconcileTargets(ctx, env))
	return true, false, nil
}

// syncNodeStatus surfaces k6/dfaas node info into status, for fast lookup by
// the LoadTestReconciler.
func (r *EnvironmentReconciler) syncNodeStatus(ctx context.Context, env *dfaasv1.Environment) error {
	return r.writer().Record(ctx, env, envTransition{Touch: func(latest *dfaasv1.Environment) error {
		var k6 []dfaasv1.K6NodeStatus
		var dfaas []string
		for _, n := range latest.Spec.Nodes {
			switch n.Role {
			case dfaasv1.RoleK6LoadGenerator:
				k6 = append(k6, dfaasv1.K6NodeStatus{
					NodeID:           n.NodeID,
					IPAddress:        n.IPAddress,
					KubeconfigSecret: latest.Name + "-" + n.NodeID + "-kubeconfig",
				})
			case dfaasv1.RoleDfaasWorker:
				dfaas = append(dfaas, n.NodeID)
			}
		}
		latest.Status.K6Nodes = k6
		latest.Status.DfaasNodes = dfaas
		return nil
	}})
}

// pruneK6Kubeconfigs deletes the per-node kubeconfig Secrets of env whose
// nodeID is absent from keep. Those Secrets are pushed by the k6 playbook via
// `delegate_to: localhost` and hold credentials for that node's k3s API. Once a
// node leaves the k6-load-generator role its k3s has been wiped and
// reinstalled, so the stored credentials are dead — and nothing else removes
// them: the LoadTest dispatch index is derived from status.k6Nodes, which drops
// the node silently.
//
// Two call sites, deliberately different keep sets:
//
//   - reconcile: keep = every current k6-load-generator nodeID, so a role flip
//     drops exactly the Secret of the node that moved.
//   - deletion: keep = nil, draining the lot. Redundant for Secrets carrying
//     the ownerReference the playbook now stamps, but Secrets pushed before
//     that change have no owner at all and Kubernetes GC cannot reach them.
//
// Selection is by the labels the playbook writes, never by name, and a Secret
// with no node-id label is skipped — that is what keeps the unlabelled
// <env>-libp2p-keys Secret out of scope.
func (r *EnvironmentReconciler) pruneK6Kubeconfigs(ctx context.Context,
	env *dfaasv1.Environment, keep map[string]struct{}) error {

	var secrets corev1.SecretList
	if err := r.List(ctx, &secrets, client.InNamespace(env.Namespace),
		client.MatchingLabels{ansible.LabelKubeconfigEnv: env.Name}); err != nil {
		return fmt.Errorf("list k6 kubeconfig secrets: %w", err)
	}

	var errs []error
	for i := range secrets.Items {
		sec := &secrets.Items[i]
		nodeID := sec.Labels[ansible.LabelKubeconfigNodeID]
		if nodeID == "" {
			continue
		}
		if _, ok := keep[nodeID]; ok {
			continue
		}
		if err := r.Delete(ctx, sec); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete kubeconfig secret %s: %w", sec.Name, err))
		}
	}
	return errors.Join(errs...)
}

// k6NodeIDs returns the set of nodeIDs currently holding the k6-load-generator
// role, i.e. the keep set for pruneK6Kubeconfigs on the reconcile path.
func k6NodeIDs(env *dfaasv1.Environment) map[string]struct{} {
	keep := map[string]struct{}{}
	for _, n := range env.Spec.Nodes {
		if n.Role == dfaasv1.RoleK6LoadGenerator {
			keep[n.NodeID] = struct{}{}
		}
	}
	return keep
}

// logStatusErr logs a best-effort operation failure instead of silently
// dropping it. op is a short human-readable label for the operation that
// failed. Used at fire-and-forget sites (condition/status stamping, Prometheus
// target reconcile) where a failure is non-fatal but should still be visible
// in logs.
func logStatusErr(ctx context.Context, op string, err error) {
	if err != nil {
		log.FromContext(ctx).Error(err, "best-effort operation failed", "op", op)
	}
}

// resetTransientConditions handles generation drift (P8): the spec was edited
// after a settled run, so every condition is reset to Unknown — none is
// trustworthy until the fresh provisioning pass re-stamps it. One write.
func (r *EnvironmentReconciler) resetTransientConditions(ctx context.Context,
	env *dfaasv1.Environment) error {
	var conds []statuswriter.Cond
	for _, condType := range []string{
		dfaasv1.EnvCondReady,
		dfaasv1.EnvCondVMsReady,
		dfaasv1.EnvCondDFaaSNodesReady,
		dfaasv1.EnvCondK6Ready,
		dfaasv1.EnvCondInfrastructureReady,
		dfaasv1.EnvCondMonitoringReady,
		dfaasv1.EnvCondNodesReachable,
	} {
		conds = append(conds, statuswriter.Cond{Type: condType, Status: metav1.ConditionUnknown,
			Reason: dfaasv1.EnvReasonUpdating, Message: "spec edited; re-provisioning — condition will be re-evaluated"})
	}
	return r.writer().Record(ctx, env, envTransition{Conditions: conds})
}
