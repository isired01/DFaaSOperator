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
// upload a CSV to Google Drive or print it to stdout.
func (r *LoadTestReconciler) createExporterJob(lt *dfaasv1.LoadTest, startTime, endTime time.Time) (*batchv1.Job, error) {
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

	env := []corev1.EnvVar{
		{Name: "PROM_URL", Value: "http://prometheus-server.monitoring.svc.cluster.local:9090"},
		{Name: "METRICS_JSON", Value: string(metricsJSON)},
		{Name: "START_TIME", Value: startTime.UTC().Format(time.RFC3339)},
		{Name: "END_TIME", Value: endTime.UTC().Format(time.RFC3339)},
		{Name: "STEP", Value: step},
		{Name: "EXP_NAME", Value: lt.Name},
	}

	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	if lt.Spec.MetricsExport.GoogleDrive != nil {
		gd := lt.Spec.MetricsExport.GoogleDrive
		env = append(env,
			corev1.EnvVar{Name: "GDRIVE_FOLDER_ID", Value: gd.FolderID},
			corev1.EnvVar{Name: "GDRIVE_CREDENTIALS_PATH", Value: "/var/run/gdrive/credentials.json"},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "gdrive-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: gd.CredentialsSecretRef},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "gdrive-creds",
			MountPath: "/var/run/gdrive",
			ReadOnly:  true,
		})
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ExporterJobName(lt),
			Namespace: lt.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(2),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:         "exporter",
							Image:        "ghcr.io/isired01/dfaas-exporter:latest",
							Env:          env,
							VolumeMounts: volumeMounts,
						},
					},
					Volumes:       volumes,
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}
	_ = ctrl.SetControllerReference(lt, job, r.Scheme)
	return job, nil
}

func int32Ptr(i int32) *int32 { return &i }
