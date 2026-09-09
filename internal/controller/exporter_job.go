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
	"os"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
)

// exporterImage resolves the dataExporter image at Job-build time. The Helm
// chart pins it via DFAAS_EXPORTER_IMAGE on the operator Deployment; the
// fallback keeps `make run` workable locally.
func exporterImage() string {
	if v := strings.TrimSpace(os.Getenv("DFAAS_EXPORTER_IMAGE")); v != "" {
		return v
	}
	return "ghcr.io/isired01/dfaas-exporter:latest"
}

// ExporterJobName composes the deterministic exporter Job name including
// `lt.UID[:8]` and `lt.Generation`, so re-running a LoadTest after a spec
// edit (or a delete + recreate with the same name) never recovers a stale
// Job from the previous incarnation.
//
// The LoadTest name is truncated when needed to keep the Job name inside the
// 63-byte label-value cap (see ansible.BoundedJobName): the Job controller
// copies this name into the pod-template `job-name` label, so an over-long
// name is rejected at CREATE and the export then retries forever. A
// UI-generated name like "lt-bari-20260901-152930-saturation-e76fcb"
// (41 chars) already overflows it.
func ExporterJobName(lt *dfaasv1.LoadTest) string {
	uid := string(lt.UID)
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return ansible.BoundedJobName(lt.Name, fmt.Sprintf("-exporter-%s-g%d-job", uid, lt.Generation))
}

// k6LogConfigMapRef pairs a k6-load-generator nodeID with the ConfigMap that
// holds its captured k6 end-of-test summary (key "k6.log"). The exporter Job
// projects each into one file named "<nodeID>.log".
type k6LogConfigMapRef struct {
	NodeID    string
	ConfigMap string
}

// k6LogMountPath is where the per-VM k6-log ConfigMaps are projected inside
// the exporter Pod. The dataExporter binary reads it via K6_LOG_DIR.
const k6LogMountPath = "/var/run/k6logs"

// k6SummarySource pairs a k6-load-generator nodeID with the in-cluster filer
// URL its handleSummary JSON was uploaded to. JSON-encoded into the exporter
// Job's K6_SUMMARY_SOURCES env var — the operator composes full URLs so the
// exporter never re-implements sanitize() (drift would 404 every fetch).
type k6SummarySource struct {
	NodeID string `json:"nodeId"`
	URL    string `json:"url"`
}

// createExporterJob builds the in-cluster Job that runs the dfaas-exporter
// image to pull metrics from Prometheus over [startTime, endTime] and either
// upload a CSV to S3 (bucket-per-environment, auto-created on first run) or
// print it to stdout. The S3 credentials are sourced from a mirrored Secret
// in the LoadTest namespace named by s3ConfigSecretName; pass empty to skip
// S3 wiring and fall back to the stdout path inside dataExporter.
//
// k6Logs lists the per-node ConfigMaps holding captured k6 end-of-test
// summaries; when non-empty they are projected read-only at k6LogMountPath
// (one "<nodeID>.log" file each) and the exporter ships them alongside the
// metrics CSV.
func (r *LoadTestReconciler) createExporterJob(lt *dfaasv1.LoadTest,
	env *dfaasv1.Environment, startTime, endTime time.Time,
	s3ConfigSecretName string, k6Logs []k6LogConfigMapRef) (*batchv1.Job, error) {
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

	// k6 end-of-test summaries: one filer URL per node, fetched by the
	// exporter and flattened into the CSV. In the base block (not the S3 one)
	// because the summaries exist on the filer regardless of S3 wiring.
	if len(k6Logs) > 0 {
		sources := make([]k6SummarySource, 0, len(k6Logs))
		for _, l := range k6Logs {
			sources = append(sources, k6SummarySource{NodeID: l.NodeID, URL: r.syncChannel().InClusterSummaryURL(lt, l.NodeID)})
		}
		sourcesJSON, err := json.Marshal(sources)
		if err != nil {
			return nil, fmt.Errorf("encode k6 summary sources: %w", err)
		}
		envVars = append(envVars, corev1.EnvVar{Name: "K6_SUMMARY_SOURCES", Value: string(sourcesJSON)})
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

	// k6-log projection: one projected volume sourcing each per-node
	// ConfigMap into a "<nodeID>.log" file, mounted read-only at
	// k6LogMountPath. Sources are Optional so a missing/uncaptured CM does
	// not wedge the Pod on a mount error — the exporter tolerates absent
	// files. K6_LOG_DIR tells the exporter where to find them.
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	if len(k6Logs) > 0 {
		sources := make([]corev1.VolumeProjection, 0, len(k6Logs))
		for _, l := range k6Logs {
			sources = append(sources, corev1.VolumeProjection{
				ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: l.ConfigMap},
					Items: []corev1.KeyToPath{
						{Key: "k6.log", Path: l.NodeID + ".log"},
					},
					Optional: ptr.To(true),
				},
			})
		}
		volumes = append(volumes, corev1.Volume{
			Name: "k6logs",
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{Sources: sources},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "k6logs",
			MountPath: k6LogMountPath,
			ReadOnly:  true,
		})
		envVars = append(envVars, corev1.EnvVar{Name: "K6_LOG_DIR", Value: k6LogMountPath})
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
			BackoffLimit: ptr.To[int32](2),
			// Cap runtime so a hung Prometheus query or S3 upload can't wedge the
			// LoadTest in Exporting forever (BackoffLimit never trips on a hang).
			// ponytail: fixed 10-min ceiling; raise it if exports legitimately run longer.
			ActiveDeadlineSeconds: ptr.To[int64](600),
			PodReplacementPolicy:  &prFailed,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:         "exporter",
							Image:        exporterImage(),
							Env:          envVars,
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
					// RestartPolicyNever ensures each retry creates a distinct Pod;
					// OnFailure restarts the container in-place and loses prior attempt logs.
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(lt, job, r.Scheme); err != nil {
		return nil, fmt.Errorf("set controller ref on exporter job: %w", err)
	}
	return job, nil
}
