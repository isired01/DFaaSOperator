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
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

// Synchronized start (spec.syncStart) — the GO-signal barrier.
//
// Remote runners start normally (k6-operator's own starter unpauses them per
// cluster; that is NOT fought here). The generated script's setup() blocks
// polling the DFAAS_SYNC_URL every ~250ms, so a runner that is "started" is a
// runner parked on the barrier. Once EVERY TestRun reports stage "started",
// the reconciler publishes the GO object on the in-cluster SeaweedFS filer;
// all setups see it within one poll interval and the VUs start together.
//
// The filer is used because it is authless on both sides: the operator PUTs
// via the in-cluster Service DNS, the k6 VMs GET anonymously via the filer
// NodePort (30901) — no aws-sdk dependency, no bucket policy involved.

// syncWaitBudget caps how long the reconciler waits for all runners to reach
// stage "started" after the dispatch completes. Past it the whole test is
// aborted (remote TestRuns deleted) and marked Failed — a partially
// synchronized run is invalid experimental data.
const syncWaitBudget = 5 * time.Minute

// syncPollRequeue is the reconcile cadence while waiting on the barrier.
const syncPollRequeue = 3 * time.Second

// syncFilerBase is the in-cluster SeaweedFS filer endpoint the OPERATOR
// writes the GO object to. Distinct from the VM-facing public URL.
const syncFilerBase = "http://seaweedfs-all-in-one.monitoring.svc.cluster.local:8888"

// syncFilerNodePort is the filer's NodePort, used to build the VM-facing GO
// URL from the management node IP.
const syncFilerNodePort = "30901"

// syncRequestTimeout caps each filer HTTP call.
const syncRequestTimeout = 10 * time.Second

// operatorFilerBase is the filer endpoint the OPERATOR ITSELF dials (GO
// signal publish/delete, summary cleanup). In-cluster the Service DNS works;
// with `make run` outside the cluster it does not resolve, so DFAAS_FILER_URL
// overrides it (e.g. http://<node-ip>:30901 — the filer NodePort). Distinct
// from syncFilerBase uses that end up inside in-cluster consumers (the
// exporter Job), which must keep the internal DNS endpoint.
func operatorFilerBase() string {
	if v := strings.TrimRight(os.Getenv("DFAAS_FILER_URL"), "/"); v != "" {
		return v
	}
	return syncFilerBase
}

// syncGoPath is the filer path of the GO object for one LoadTest. Lives
// outside /buckets so it never shows up as an S3 bucket.
func syncGoPath(lt *dfaasv1.LoadTest) string {
	return fmt.Sprintf("/dfaas-sync/%s/%s.go", lt.Namespace, lt.Name)
}

// k6 end-of-test summaries — same authless filer channel as the GO signal.
// Each runner's handleSummary() PUTs its summary JSON to DFAAS_SUMMARY_URL
// (VM-facing NodePort); the dataExporter Job GETs them back via the
// in-cluster DNS endpoint and flattens them into the metrics CSV.

// summaryDirPath is the per-LoadTest filer directory holding one summary
// object per k6 node. Outside /buckets, like the GO object.
func summaryDirPath(lt *dfaasv1.LoadTest) string {
	return fmt.Sprintf("/dfaas-k6-summary/%s/%s", lt.Namespace, lt.Name)
}

// summaryPath is the filer path of one node's summary object.
func summaryPath(lt *dfaasv1.LoadTest, nodeID string) string {
	return fmt.Sprintf("%s/%s.json", summaryDirPath(lt), sanitize(nodeID))
}

// summaryURL is the VM-facing URL injected into the runner as
// DFAAS_SUMMARY_URL. Empty when the public base is unresolvable — the
// generated script then skips the upload (a missing summary only degrades
// data richness, unlike the sync barrier which must fail loudly).
func summaryURL(lt *dfaasv1.LoadTest, nodeID string) string {
	base := syncPublicBase()
	if base == "" {
		return ""
	}
	return base + summaryPath(lt, nodeID)
}

// summaryFilerURL is the in-cluster URL the exporter Job fetches one node's
// summary from. Composed operator-side so the exporter never has to
// re-implement sanitize() (drift there would 404 every fetch).
func summaryFilerURL(lt *dfaasv1.LoadTest, nodeID string) string {
	return syncFilerBase + summaryPath(lt, nodeID)
}

// deleteSummaryObjects best-effort removes the whole per-LoadTest summary
// directory (one recursive filer DELETE instead of N per-node ones — also
// sweeps files from nodes later removed from the spec). Called once the
// exporter has consumed the summaries, and from the abort/deletion paths.
func deleteSummaryObjects(ctx context.Context, lt *dfaasv1.LoadTest) error {
	ctx, cancel := context.WithTimeout(ctx, syncRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		operatorFilerBase()+summaryDirPath(lt)+"/?recursive=true", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete k6 summaries: filer returned %s", resp.Status)
	}
	return nil
}

// syncPublicBase resolves the base URL the k6 VMs poll the GO object from:
// DFAAS_SYNC_PUBLIC_URL env override (Helm-injectable, for multi-subnet labs)
// → else http://<HOST_IP>:30901 with HOST_IP injected via the downward API
// (status.hostIP on the manager Deployment). Empty when neither is available
// (e.g. `make run` outside the cluster without the override) — callers must
// fail loudly rather than dispatch a barrier nobody can open.
func syncPublicBase() string {
	if v := strings.TrimRight(os.Getenv("DFAAS_SYNC_PUBLIC_URL"), "/"); v != "" {
		return v
	}
	if ip := os.Getenv("HOST_IP"); ip != "" {
		return "http://" + net.JoinHostPort(ip, syncFilerNodePort)
	}
	return ""
}

// syncGoURL is the full VM-facing URL injected into the runner as
// DFAAS_SYNC_URL. Empty when syncPublicBase is unresolvable.
func syncGoURL(lt *dfaasv1.LoadTest) string {
	base := syncPublicBase()
	if base == "" {
		return ""
	}
	return base + syncGoPath(lt)
}

// publishGoSignal PUTs the GO object on the filer. Idempotent: re-PUTting the
// same path just rewrites the file.
func publishGoSignal(ctx context.Context, lt *dfaasv1.LoadTest) error {
	ctx, cancel := context.WithTimeout(ctx, syncRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		operatorFilerBase()+syncGoPath(lt), strings.NewReader("go"))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("put GO signal: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("put GO signal: filer returned %s", resp.Status)
	}
	return nil
}

// deleteGoSignal best-effort removes the GO object once the barrier is moot
// (test finished, aborted, or deleted). Stale objects are harmless — this is
// hygiene, so errors are only logged by callers via logStatusErr.
func deleteGoSignal(ctx context.Context, lt *dfaasv1.LoadTest) error {
	ctx, cancel := context.WithTimeout(ctx, syncRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		operatorFilerBase()+syncGoPath(lt), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Filer returns 204/202 on delete, 404 when already gone — all fine.
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete GO signal: filer returned %s", resp.Status)
	}
	return nil
}

// awaitSyncBarrier is the post-dispatch gate for syncStart tests. It polls
// every remote TestRun until all report stage "started" (runner up and parked
// on the script barrier), then publishes the GO signal and finishes the
// dispatch (StartTime + phase Running). Failure policy per user decision:
// any TestRun in stage "error", or the syncWaitBudget expiring, tears down
// every remote TestRun and fails the LoadTest.
func (r *LoadTestReconciler) awaitSyncBarrier(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	k6Index := computeK6NodeIndex(env)
	total := len(lt.Status.TestRuns)
	started := 0
	for _, ref := range lt.Status.TestRuns {
		k6Node, nerr := resolveK6Node(k6Index, ref.NodeID)
		if nerr != nil {
			// A node that left the Environment will never report "started":
			// fail in one tick instead of burning the whole syncWaitBudget
			// waiting on a runner nobody can poll.
			logger.Info("sync barrier: node unusable; aborting all",
				"node", ref.NodeID, "cause", nerr.Error())
			if failed := r.teardownRemoteTestRuns(ctx, lt, env); failed > 0 {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			logStatusErr(ctx, "stamp SyncReady=False (node unusable)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondSyncReady,
				metav1.ConditionFalse, dfaasv1.LTReasonSyncTimeout, nerr.Error()))
			return r.failLoadTest(ctx, lt, "synchronized start: "+nerr.Error())
		}
		secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
		remoteKey := types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}
		tr, err := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey)
		if err != nil {
			// Transient remote hiccup: keep waiting, the budget bounds us.
			logger.Error(err, "sync barrier: remote TestRun poll failed", "node", ref.NodeID)
			continue
		}
		switch k6dispatch.StageOf(tr) {
		case "started", "finished", "stopped":
			// finished/stopped should not happen while parked on the barrier,
			// but count them as past-the-start so the GO still fires.
			started++
		case "error":
			logger.Info("sync barrier: TestRun errored while waiting; aborting all",
				"node", ref.NodeID)
			if failed := r.teardownRemoteTestRuns(ctx, lt, env); failed > 0 {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			logStatusErr(ctx, "stamp SyncReady=False (runner error)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondSyncReady,
				metav1.ConditionFalse, dfaasv1.LTReasonSyncTimeout,
				fmt.Sprintf("runner on node %q reported stage=error before the GO signal", ref.NodeID)))
			return r.failLoadTest(ctx, lt,
				fmt.Sprintf("synchronized start: runner on node %q errored before the GO signal", ref.NodeID))
		}
	}

	if started < total {
		// Budget reference: when the dispatch completed, i.e. K6Dispatched
		// flipped to True. Re-fetch to see the freshest conditions.
		var latest dfaasv1.LoadTest
		if err := r.Get(ctx, types.NamespacedName{Name: lt.Name, Namespace: lt.Namespace}, &latest); err == nil {
			if cond := meta.FindStatusCondition(latest.Status.Conditions, dfaasv1.LTCondK6Dispatched); cond != nil &&
				cond.Status == metav1.ConditionTrue &&
				time.Since(cond.LastTransitionTime.Time) > syncWaitBudget {
				logger.Info("sync barrier: wait budget exhausted; aborting all",
					"started", started, "total", total)
				if failed := r.teardownRemoteTestRuns(ctx, lt, env); failed > 0 {
					return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
				}
				logStatusErr(ctx, "stamp SyncReady=False (timeout)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondSyncReady,
					metav1.ConditionFalse, dfaasv1.LTReasonSyncTimeout,
					fmt.Sprintf("only %d/%d runners started within %s", started, total, syncWaitBudget)))
				return r.failLoadTest(ctx, lt,
					fmt.Sprintf("synchronized start: only %d/%d runners started within %s", started, total, syncWaitBudget))
			}
		}
		logStatusErr(ctx, "stamp SyncReady=False (awaiting runners)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondSyncReady,
			metav1.ConditionFalse, dfaasv1.LTReasonAwaitingRunners,
			fmt.Sprintf("%d/%d runners started, holding the GO signal", started, total)))
		return ctrl.Result{RequeueAfter: syncPollRequeue}, nil
	}

	if err := publishGoSignal(ctx, lt); err != nil {
		// Filer hiccup: retry on the poll cadence; the wait budget above still
		// bounds the total time spent here.
		logger.Error(err, "sync barrier: GO signal publish failed; retrying")
		return ctrl.Result{RequeueAfter: syncPollRequeue}, nil
	}
	logger.Info("sync barrier: GO signal published", "runners", total)
	logStatusErr(ctx, "stamp SyncReady=True (go published)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondSyncReady,
		metav1.ConditionTrue, dfaasv1.LTReasonGoPublished,
		fmt.Sprintf("GO signal published; %d runners released together", total)))
	return r.finishDispatch(ctx, lt, lt.Status.TestRuns)
}
