/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"
)

// ExporterJobName composes the deterministic exporter Job name including
// `lt.UID[:8]` and `lt.Generation`, so re-running a LoadTest after a spec
// edit (or a delete + recreate with the same name) never recovers a stale
// Job from the previous incarnation.
func ExporterJobName(lt *dfaasv1.LoadTest) string {
	uid := string(lt.UID)
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return fmt.Sprintf("%s-exporter-%s-g%d-job", lt.Name, uid, lt.Generation)
}

// createExporterJob builds the in-cluster Job that runs the dfaas-exporter
// image to pull metrics from Prometheus over [startTime, endTime] and either
// upload a CSV to S3 (bucket-per-environment, auto-created on first run) or
// print it to stdout. The S3 credentials are sourced from a mirrored Secret
// in the LoadTest namespace named by s3ConfigSecretName; pass empty to skip
// S3 wiring and fall back to the stdout path inside dataExporter.
func (r *LoadTestReconciler) createExporterJob(lt *dfaasv1.LoadTest,
	env *dfaasv1.Environment, startTime, endTime time.Time,
	s3ConfigSecretName string) (*batchv1.Job, error) {
	// Resolve raw-type defaults: when Type=raw and MetricName is empty,
	// the bare metric name (Query) doubles as the alias. CEL validation
	// already guarantees custom-promql entries have MetricName set.
	resolved := make([]dfaasv1.MetricExportEntry, len(lt.Spec.MetricsExport.Metrics))
	for i, m := range lt.Spec.MetricsExport.Metrics {
		resolved[i] = m
		if resolved[i].MetricName == "" && resolved[i].Type == dfaasv1.MetricTypeRaw {
			resolved[i].MetricName = resolved[i].Query
		}
	}
	metricsJSON, err := json.Marshal(resolved)
	if err != nil {
		return nil, fmt.Errorf("encode metrics payload: %w", err)
	}

	step := lt.Spec.MetricsExport.Step
	if step == "" {
		step = "15s"
	}

	envVars := []corev1.EnvVar{
		{Name: "PROM_URL", Value: "http://prometheus-server.monitoring.svc.cluster.local:9090"},
		{Name: "METRICS_JSON", Value: string(metricsJSON)},
		{Name: "START_TIME", Value: startTime.UTC().Format(time.RFC3339)},
		{Name: "END_TIME", Value: endTime.UTC().Format(time.RFC3339)},
		{Name: "STEP", Value: step},
		{Name: "EXP_NAME", Value: lt.Name},
	}

	// S3 wiring: when an S3 config Secret was mirrored into this namespace,
	// surface its keys as env vars via secretKeyRef so credentials are never
	// embedded in the Job spec. The exporter binary dispatches on
	// S3_BUCKET_PREFIX being non-empty (stdout fallback otherwise).
	if s3ConfigSecretName != "" {
		envVars = append(envVars,
			corev1.EnvVar{Name: "S3_ENDPOINT", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3ConfigSecretName},
					Key:                  "endpoint",
					// AWS default endpoint is fine when the key is absent.
					Optional: ptr.To(true),
				},
			}},
			corev1.EnvVar{Name: "S3_REGION", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3ConfigSecretName},
					Key:                  "region",
				},
			}},
			corev1.EnvVar{Name: "S3_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3ConfigSecretName},
					Key:                  "access_key_id",
				},
			}},
			corev1.EnvVar{Name: "S3_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3ConfigSecretName},
					Key:                  "secret_access_key",
				},
			}},
			corev1.EnvVar{Name: "S3_FORCE_PATH_STYLE", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3ConfigSecretName},
					Key:                  "force_path_style",
				},
			}},
			// Plain envs feed the bucket-name + object-key composition inside
			// the exporter. S3_BUCKET_PREFIX is the dispatch signal.
			corev1.EnvVar{Name: "S3_BUCKET_PREFIX", Value: env.Name},
			corev1.EnvVar{Name: "ENV_UID", Value: string(env.UID)},
			corev1.EnvVar{Name: "LOADTEST_NAME", Value: lt.Name},
		)
	}

	// PodReplacementPolicy=Failed retains failed Pods for post-mortem debug:
	// Job controller waits for full Pod termination before replacing and does
	// not delete failed Pods on BackoffLimit exceeded (TTL handles cleanup).
	prFailed := batchv1.Failed
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ExporterJobName(lt),
			Namespace: lt.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:         int32Ptr(2),
			PodReplacementPolicy: &prFailed,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "exporter",
							Image: "ghcr.io/isired01/dfaas-exporter:latest",
							Env:   envVars,
						},
					},
					// RestartPolicyNever ensures each retry creates a distinct Pod;
					// OnFailure restarts the container in-place and loses prior attempt logs.
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}
	_ = ctrl.SetControllerReference(lt, job, r.Scheme)
	return job, nil
}

func int32Ptr(i int32) *int32 { return &i }
