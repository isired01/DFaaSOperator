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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/statuswriter"
	"dfaas-operator/internal/k6dispatch"
)

// federationInterval is how often the management Prometheus pulls worker
// metrics through /federate: server.global.scrape_interval in
// internal/controller/monitoring/values/prometheus-values.yaml. Both constants
// below derive from it, so changing the interval in that file means changing it
// here -- and federation_interval_test.go fails when the two disagree.
const federationInterval = 15 * time.Second

// exportTailWindow extends the export query past the LoadTest's EndTime.
// Federation gives the mgmt instance one sample per node per interval, and
// each node is pulled at its own offset, so the last sample *inside* the k6
// window can be up to one full interval older than EndTime — and no amount of
// waiting adds samples to a window that has already closed. A smoke test on
// 2026-09-11 measured the effect directly: with k6 stopping at 17:43:29 the
// last exported sample was 17:42:31 for one node and 17:43:16 for another, so
// the same run gave a different tail, and a different sample count, per node.
// Extending the window by one interval puts at least one sample at or after
// EndTime for every node. The extra samples are flat: k6 has stopped, and
// every metric here is a counter.
const exportTailWindow = federationInterval

// exportCooldown is how long the reconciler waits, after k6 finishes, before
// creating the exporter Job. It covers the widened window: one interval for
// the last in-window pull, one more for the pull that lands after EndTime.
const exportCooldown = federationInterval + exportTailWindow

// observeK6 polls every remote TestRun. When all have reached a terminal
// stage (finished/stopped) it transitions to Exporting; on any error it
// fails the LoadTest, and endRun deletes whatever may still be running.
func (r *LoadTestReconciler) observeK6(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	nodeIDs := make([]string, 0, len(lt.Status.TestRuns))
	for _, ref := range lt.Status.TestRuns {
		nodeIDs = append(nodeIDs, ref.NodeID)
	}
	rd := r.survey(ctx, lt, env, nodeIDs)
	updatedRefs := rd.refs(lt.Status.TestRuns)

	if len(rd.unusable) > 0 {
		// A generator that left the Environment can neither be polled nor
		// released: fail on the spot instead of waiting out a budget it cannot
		// satisfy.
		return r.failLoadTest(ctx, lt, "generator unusable: "+rd.describe(rd.unusable))
	}

	r.collectProbes(ctx, lt, env)

	budget := r.budget(fetchMissesAnnotation, fetchRetryBudget)
	if len(rd.missing) > 0 {
		// ErrNotFound included: a TestRun that disappeared under a running test
		// is a failed round, charged once however many generators missed.
		outcome, berr := r.attempt(ctx, lt, budget)
		if berr != nil {
			logger.Error(berr, "fetch-misses counter write failed; the budget cannot advance")
		}
		detail := "no status from " + rd.describe(rd.missing)
		if outcome.Exhausted {
			return r.failLoadTest(ctx, lt, fmt.Sprintf("observe: %d consecutive rounds failed: %s", outcome.Count, detail))
		}
		logStatusErr(ctx, "persist failed observe round", r.writer().Record(ctx, lt, ltTransition{
			Conditions: []statuswriter.Cond{{Type: dfaasv1.LTCondK6Healthy, Status: metav1.ConditionUnknown,
				Reason: dfaasv1.LTReasonFetchFailed, Message: fmt.Sprintf("attempt %s: %s", budget.Attempts(outcome), detail)}},
			Touch: func(latest *dfaasv1.LoadTest) error {
				latest.Status.TestRuns = updatedRefs
				return nil
			},
		}))
		return ctrl.Result{RequeueAfter: r.retryInterval()}, nil
	}
	// A complete round — reset the counter (a no-op at zero).
	if rerr := budget.Clear(ctx, lt); rerr != nil {
		logger.Error(rerr, "reset fetch-misses failed; non-fatal")
	}
	finishedCount, errorCount := rd.count[stageDone], rd.count[stageErrored]
	allDone := rd.count[stagePending]+rd.count[stageStarted] == 0

	// Persist updated phases (best-effort).
	logStatusErr(ctx, "persist updated TestRun phases", r.writer().Record(ctx, lt, ltTransition{Touch: func(latest *dfaasv1.LoadTest) error {
		latest.Status.TestRuns = updatedRefs
		return nil
	}}))

	total := len(lt.Status.TestRuns)
	runningCount := total - finishedCount - errorCount

	// P9: K6Healthy rollup with a per-node count in the message.
	if !allDone {
		r.cond(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionUnknown, dfaasv1.LTReasonRunning,
			fmt.Sprintf("%d/%d finished, %d error, %d running",
				finishedCount, total, errorCount, runningCount))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	switch {
	case errorCount == 0:
		r.cond(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionTrue, dfaasv1.LTReasonAllFinished,
			fmt.Sprintf("%d/%d TestRun(s) finished cleanly", finishedCount, total))
	case finishedCount == 0:
		return r.failLoadTest(ctx, lt,
			fmt.Sprintf("all %d remote TestRuns reported error stage", errorCount),
			statuswriter.Cond{Type: dfaasv1.LTCondK6Healthy, Status: metav1.ConditionFalse,
				Reason: dfaasv1.LTReasonAllFailed, Message: fmt.Sprintf("%d/%d TestRun(s) reported error", errorCount, total)})
	default:
		return r.failLoadTest(ctx, lt,
			fmt.Sprintf("%d of %d remote TestRuns reported error stage", errorCount, total),
			statuswriter.Cond{Type: dfaasv1.LTCondK6Healthy, Status: metav1.ConditionFalse,
				Reason:  dfaasv1.LTReasonPartialFailure,
				Message: fmt.Sprintf("%d finished, %d error (of %d)", finishedCount, errorCount, total)})
	}

	// Capture each VM's k6 end-of-test summary into per-node ConfigMaps
	// before transitioning to Exporting. Best-effort: capture failures never
	// block the phase transition.
	r.captureK6Logs(ctx, lt, env)

	// The GO signal (syncStart) served its purpose once every runner has
	// finished — best-effort hygiene, stale objects are harmless.
	if lt.Spec.SyncStart {
		logStatusErr(ctx, "delete GO signal (test finished)", r.syncChannel().DeleteGo(ctx, lt))
	}

	// All TestRuns done cleanly → stamp EndTime and move to Exporting.
	now := metav1.Now()
	if err := r.writer().Record(ctx, lt, ltTransition{Touch: func(latest *dfaasv1.LoadTest) error {
		latest.Status.EndTime = &now
		latest.Status.Phase = dfaasv1.LoadTestExporting
		stampLTAggregate(latest, dfaasv1.LoadTestExporting, "", "")
		return nil
	}}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// captureK6Logs reads each k6-load-generator VM's k6 runner-Pod logs (the
// end-of-test summary) off the remote k3s and stores them in one ConfigMap
// per node in lt.Namespace, named "<lt.Name>-k6log-<sanitized nodeID>". The
// exporter Job later mounts these CMs and ships them the same way as metrics
// (S3 when configured, else stdout).
//
// Whole loop is best-effort: a per-node capture error is recorded as a
// placeholder inside the ConfigMap (so the failure is still exported) and any
// ConfigMap write error is logged via logStatusErr — nothing here blocks the
// Exporting transition. ConfigMaps carry an OwnerRef to the LoadTest so they
// cascade-delete with it.
func (r *LoadTestReconciler) captureK6Logs(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) {
	logger := log.FromContext(ctx)

	for _, ref := range lt.Status.TestRuns {
		node, nerr := r.Dispatcher.Node(ctx, env, ref.NodeID)
		if nerr != nil {
			// Best-effort path: nothing to read from a node we cannot reach.
			logger.Error(nerr, "skipping k6 log capture", "node", ref.NodeID)
			continue
		}
		logs, err := node.Logs(ctx, lt)
		if err != nil {
			// Record the failure as a placeholder so the operator/user still
			// gets a per-VM artifact noting capture did not succeed.
			logger.Error(err, "k6 log capture failed", "node", ref.NodeID, "testRun", ref.Name)
			logs = fmt.Sprintf("k6 log capture failed for node %s: %v", ref.NodeID, err)
		}

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      k6dispatch.K6LogConfigMap(lt, ref.NodeID),
				Namespace: lt.Namespace,
			},
		}
		nodeID := ref.NodeID
		logContent := logs
		_, cerr := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
			if cm.Labels == nil {
				cm.Labels = map[string]string{}
			}
			cm.Labels["dfaas.io/loadtest-name"] = lt.Name
			cm.Labels["dfaas.io/node-id"] = nodeID
			cm.Labels["dfaas.io/k6-log"] = "true"
			cm.Data = map[string]string{"k6.log": logContent}
			return controllerutil.SetOwnerReference(lt, cm, r.Scheme)
		})
		logStatusErr(ctx, fmt.Sprintf("upsert k6-log ConfigMap for node %s", nodeID), cerr)
	}
}

// runExporter creates the in-cluster Job that pulls metrics from Prometheus
// over [StartTime, EndTime] and uploads to S3. The destination defaults to the
// in-cluster SeaweedFS sink (DefaultS3ConfigName) when env.spec.s3ConfigRef is
// unset; an explicit ref selects that config instead. Only if the default
// config itself is missing does the exporter fall back to a stdout dump.
func (r *LoadTestReconciler) runExporter(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	jobName := ExporterJobName(lt)
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: lt.Namespace}, &job)

	if apierrors.IsNotFound(err) {
		if lt.Status.StartTime == nil || lt.Status.EndTime == nil {
			return r.failLoadTest(ctx, lt, "missing StartTime/EndTime; cannot run exporter",
				exportFailed("missing StartTime/EndTime; cannot run exporter"))
		}

		// Cool-down before querying. The management Prometheus does not scrape
		// the workers directly — it federates from each worker's own Prometheus
		// every federationInterval (15s), so at the instant k6 stops, the tail
		// of the run may not have been pulled across yet. Exporting immediately
		// truncates the CSV by up to one federation period, and by a different
		// amount on every run (it depends where EndTime lands in the cycle),
		// which makes the tails of two otherwise-identical runs incomparable.
		//
		// The query window IS extended by exportTailWindow (one interval past
		// EndTime, see its doc), so every node has a sample at or after the k6
		// finish; exportCooldown covers that widened window. Keyed off the
		// persisted Status.EndTime, so an operator restart mid-cool-down resumes
		// with the correct deadline rather than starting the wait again.
		if waited := time.Since(lt.Status.EndTime.Time); waited < exportCooldown {
			remaining := exportCooldown - waited
			r.cond(ctx, lt, dfaasv1.LTCondMetricsExported,
				metav1.ConditionUnknown, dfaasv1.LTReasonExportCooldown,
				fmt.Sprintf("cooling down %s before export so Prometheus federates the end of the run (%s left)",
					exportCooldown, remaining.Truncate(time.Second)))
			logger.Info("export cool-down in progress", "remaining", remaining.Truncate(time.Second))
			return ctrl.Result{RequeueAfter: remaining}, nil
		}

		logger.Info("creating exporter Job", "job", jobName)

		// Always resolve an S3 config name. With no explicit s3ConfigRef the
		// Environment defaults to the in-cluster SeaweedFS sink (DefaultS3ConfigName)
		// instead of the legacy stdout dump.
		configName := DefaultS3ConfigName
		explicitRef := env.Spec.S3ConfigRef != nil
		if explicitRef {
			configName = env.Spec.S3ConfigRef.Name
		}
		var s3SecretName string
		runningMsg := "exporter Job created, awaiting completion"
		mirrored, mirrorErr := r.ensureMirroredS3Secret(ctx, lt, configName)
		if mirrorErr != nil {
			if apierrors.IsNotFound(mirrorErr) {
				missing := fmt.Sprintf("S3 config %q not found in namespace %s",
					configName, S3ConfigNamespace)
				if explicitRef {
					// Explicit ref must exist — a missing one is a hard failure
					// (unchanged behaviour).
					return r.endRun(ctx, lt, dfaasv1.LoadTestFailed, dfaasv1.LTReasonS3ConfigMissing, missing,
						statuswriter.Cond{Type: dfaasv1.LTCondMetricsExported, Status: metav1.ConditionFalse,
							Reason: dfaasv1.LTReasonS3ConfigMissing, Message: missing})
				}
				// Default sink missing (e.g. SeaweedFS not yet deployed) — degrade
				// gracefully to the stdout path rather than failing the test. No
				// S3ConfigMissing stamp here: that reason is terminal, and this
				// path is not; the running message names the fallback instead.
				runningMsg = "exporter Job created, awaiting completion; " + missing +
					", so the CSV goes to the Job's stdout (kubectl logs)"
				logger.Info("default S3 config not found; falling back to stdout export",
					"config", configName)
				s3SecretName = ""
			} else {
				return ctrl.Result{}, mirrorErr
			}
		} else {
			s3SecretName = mirrored
		}

		// Per-node k6-log ConfigMaps captured at the end of observeK6. The
		// exporter Job mounts these (one file per VM) and ships them the same
		// way as metrics.
		k6LogCMs := make([]k6LogConfigMapRef, 0, len(lt.Status.TestRuns))
		for _, ref := range lt.Status.TestRuns {
			k6LogCMs = append(k6LogCMs, k6LogConfigMapRef{
				NodeID:    ref.NodeID,
				ConfigMap: k6dispatch.K6LogConfigMap(lt, ref.NodeID),
			})
		}

		newJob, err := r.createExporterJob(lt, env, lt.Status.StartTime.Time,
			// One federation interval past the end, so every node has a
			// sample at or after EndTime. See exportTailWindow.
			lt.Status.EndTime.Time.Add(exportTailWindow), s3SecretName, k6LogCMs)
		if err != nil {
			return r.failLoadTest(ctx, lt, fmt.Sprintf("build exporter job: %v", err),
				exportFailed("build exporter job: "+condMessage(err)))
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		r.cond(ctx, lt, dfaasv1.LTCondMetricsExported,
			metav1.ConditionUnknown, dfaasv1.LTReasonExporterRunning, runningMsg)
		logStatusErr(ctx, "persist exporter Job name", r.writer().Record(ctx, lt, ltTransition{Touch: func(latest *dfaasv1.LoadTest) error {
			latest.Status.ExporterJob = jobName
			return nil
		}}))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if job.Status.Succeeded > 0 || jobConditionTrue(&job, batchv1.JobComplete) {
		logger.Info("exporter Job succeeded")
		// endRun sweeps the summaries the exporter consumed off the filer.
		return r.endRun(ctx, lt, dfaasv1.LoadTestCompleted, "", "",
			statuswriter.Cond{Type: dfaasv1.LTCondMetricsExported, Status: metav1.ConditionTrue,
				Reason: dfaasv1.LTReasonExportSucceeded, Message: "metrics exported"})
	}
	// Decide terminal failure from the JobFailed condition, not the raw
	// Status.Failed counter: that counter tracks failed *attempts*, and the Job
	// has a non-zero BackoffLimit, so a single transient pod failure would
	// otherwise fail the LoadTest while Kubernetes is still spawning a retry pod
	// that may yet succeed (same backoff-aware pattern as the Ansible Jobs).
	if jobConditionTrue(&job, batchv1.JobFailed) {
		return r.failLoadTest(ctx, lt, "exporter Job failed", exportFailed("exporter Job reported Failed"))
	}
	logger.Info("exporter Job running")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// exportFailed is the MetricsExported Condition of a failed export step.
func exportFailed(message string) statuswriter.Cond {
	return statuswriter.Cond{Type: dfaasv1.LTCondMetricsExported, Status: metav1.ConditionFalse,
		Reason: dfaasv1.LTReasonJobFailed, Message: message}
}
