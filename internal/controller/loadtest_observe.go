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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

// observeK6 polls every remote TestRun. When all have reached a terminal
// stage (finished/stopped) it transitions to Exporting; on any error it
// fails the LoadTest.
func (r *LoadTestReconciler) observeK6(ctx context.Context,
	lt *dfaasv1.LoadTest, env *dfaasv1.Environment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	k6Index := computeK6NodeIndex(env)

	allDone := true
	var errorCount, finishedCount int
	updatedRefs := make([]dfaasv1.TestRunRef, len(lt.Status.TestRuns))
	copy(updatedRefs, lt.Status.TestRuns)

	for i, ref := range lt.Status.TestRuns {
		k6Node, ok := k6Index[ref.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			return r.failLoadTest(ctx, lt,
				fmt.Sprintf("k6 node %q no longer present on env", ref.NodeID))
		}
		secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
		remoteKey := types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}

		tr, err := r.Dispatcher.GetTestRun(ctx, secretRef, remoteKey)
		if err != nil {
			logger.Error(err, "remote TestRun fetch failed", "node", ref.NodeID, "name", ref.Name)
			res, _, oerr := r.onDispatchError(ctx, lt, err, dfaasv1.LTReasonApplyFailed)
			return res, oerr
		}
		// Successful dispatcher round-trip — reset the budget counter.
		if rerr := r.resetDispatchAttempts(ctx, lt); rerr != nil {
			logger.Error(rerr, "resetDispatchAttempts failed; non-fatal")
		}
		stage := k6dispatch.StageOf(tr)
		updatedRefs[i].Phase = stage

		switch stage {
		case "finished", "stopped":
			finishedCount++
		case "error":
			errorCount++
		default:
			allDone = false
		}
	}

	// Persist updated phases (best-effort).
	logStatusErr(ctx, "persist updated TestRun phases", r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
		latest.Status.TestRuns = updatedRefs
		return nil
	}))

	total := len(lt.Status.TestRuns)
	runningCount := total - finishedCount - errorCount

	// P9: K6Healthy rollup with a per-node count in the message.
	if !allDone {
		logStatusErr(ctx, "stamp K6Healthy=Unknown (running)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionUnknown, dfaasv1.LTReasonRunning,
			fmt.Sprintf("%d/%d finished, %d error, %d running",
				finishedCount, total, errorCount, runningCount)))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	switch {
	case errorCount == 0:
		logStatusErr(ctx, "stamp K6Healthy=True (all finished)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionTrue, dfaasv1.LTReasonAllFinished,
			fmt.Sprintf("%d/%d TestRun(s) finished cleanly", finishedCount, total)))
	case finishedCount == 0:
		logStatusErr(ctx, "stamp K6Healthy=False (all failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionFalse, dfaasv1.LTReasonAllFailed,
			fmt.Sprintf("%d/%d TestRun(s) reported error", errorCount, total)))
		return r.failLoadTest(ctx, lt,
			fmt.Sprintf("all %d remote TestRuns reported error stage", errorCount))
	default:
		logStatusErr(ctx, "stamp K6Healthy=False (partial failure)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondK6Healthy,
			metav1.ConditionFalse, dfaasv1.LTReasonPartialFailure,
			fmt.Sprintf("%d finished, %d error (of %d)",
				finishedCount, errorCount, total)))
		return r.failLoadTest(ctx, lt,
			fmt.Sprintf("%d of %d remote TestRuns reported error stage", errorCount, total))
	}

	// Capture each VM's k6 end-of-test summary into per-node ConfigMaps
	// before transitioning to Exporting. Best-effort: capture failures never
	// block the phase transition.
	r.captureK6Logs(ctx, lt, env)

	// All TestRuns done cleanly → stamp EndTime and move to Exporting.
	now := metav1.Now()
	if err := r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
		latest.Status.EndTime = &now
		latest.Status.Phase = dfaasv1.LoadTestExporting
		stampLTAggregate(latest, dfaasv1.LoadTestExporting)
		return nil
	}); err != nil {
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

	k6Index := computeK6NodeIndex(env)
	for _, ref := range lt.Status.TestRuns {
		k6Node, ok := k6Index[ref.NodeID]
		if !ok || k6Node.KubeconfigSecret == "" {
			logger.Info("skipping k6 log capture: node has no kubeconfig secret", "node", ref.NodeID)
			continue
		}

		secretRef := types.NamespacedName{Name: k6Node.KubeconfigSecret, Namespace: lt.Namespace}
		logs, err := r.Dispatcher.GetK6RunnerLogs(ctx, secretRef, ref.Name, ref.Namespace)
		if err != nil {
			// Record the failure as a placeholder so the operator/user still
			// gets a per-VM artifact noting capture did not succeed.
			logger.Error(err, "k6 log capture failed", "node", ref.NodeID, "testRun", ref.Name)
			logs = fmt.Sprintf("k6 log capture failed for node %s: %v", ref.NodeID, err)
		}

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-k6log-%s", lt.Name, sanitize(ref.NodeID)),
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
		logger.Info("creating exporter Job", "job", jobName)
		if lt.Status.StartTime == nil || lt.Status.EndTime == nil {
			logStatusErr(ctx, "stamp MetricsExported=False (missing times)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
				metav1.ConditionFalse, dfaasv1.LTReasonJobFailed,
				"missing StartTime/EndTime; cannot run exporter"))
			return r.failLoadTest(ctx, lt, "missing StartTime/EndTime; cannot run exporter")
		}

		// Always resolve an S3 config name. With no explicit s3ConfigRef the
		// Environment defaults to the in-cluster SeaweedFS sink (DefaultS3ConfigName)
		// instead of the legacy stdout dump.
		configName := DefaultS3ConfigName
		explicitRef := env.Spec.S3ConfigRef != nil
		if explicitRef {
			configName = env.Spec.S3ConfigRef.Name
		}
		var s3SecretName string
		mirrored, mirrorErr := r.ensureMirroredS3Secret(ctx, lt, configName)
		if mirrorErr != nil {
			if apierrors.IsNotFound(mirrorErr) {
				// Stamp the S3ConfigMissing condition either way for visibility.
				logStatusErr(ctx, "stamp MetricsExported=False (s3 config missing)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
					metav1.ConditionFalse, dfaasv1.LTReasonS3ConfigMissing,
					fmt.Sprintf("S3 config %q not found in namespace %s",
						configName, S3ConfigNamespace)))
				if explicitRef {
					// Explicit ref must exist — a missing one is a hard failure
					// (unchanged behaviour).
					return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestFailed)
				}
				// Default sink missing (e.g. SeaweedFS not yet deployed) — degrade
				// gracefully to the stdout path rather than failing the test.
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
				ConfigMap: fmt.Sprintf("%s-k6log-%s", lt.Name, sanitize(ref.NodeID)),
			})
		}

		newJob, err := r.createExporterJob(lt, env, lt.Status.StartTime.Time, lt.Status.EndTime.Time, s3SecretName, k6LogCMs)
		if err != nil {
			logStatusErr(ctx, "stamp MetricsExported=False (build failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
				metav1.ConditionFalse, dfaasv1.LTReasonJobFailed,
				"build exporter job: "+condMessage(err)))
			return r.failLoadTest(ctx, lt, fmt.Sprintf("build exporter job: %v", err))
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		logStatusErr(ctx, "stamp MetricsExported=Unknown (exporter running)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
			metav1.ConditionUnknown, dfaasv1.LTReasonExporterRunning,
			"exporter Job created, awaiting completion"))
		logStatusErr(ctx, "persist exporter Job name", r.atomicStatusUpdate(ctx, client.ObjectKeyFromObject(lt), func(latest *dfaasv1.LoadTest) error {
			latest.Status.ExporterJob = jobName
			return nil
		}))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if job.Status.Succeeded > 0 {
		logger.Info("exporter Job succeeded")
		logStatusErr(ctx, "stamp MetricsExported=True", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
			metav1.ConditionTrue, dfaasv1.LTReasonExportSucceeded,
			"metrics exported"))
		return r.setLoadTestPhase(ctx, lt, dfaasv1.LoadTestCompleted)
	}
	if job.Status.Failed > 0 {
		logStatusErr(ctx, "stamp MetricsExported=False (job failed)", r.setLoadTestCondition(ctx, lt, dfaasv1.LTCondMetricsExported,
			metav1.ConditionFalse, dfaasv1.LTReasonJobFailed,
			"exporter Job reported Failed"))
		return r.failLoadTest(ctx, lt, "exporter Job failed")
	}
	logger.Info("exporter Job running")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}
