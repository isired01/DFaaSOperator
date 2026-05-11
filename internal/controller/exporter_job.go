package controller

import (
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"
)

// createExporterJob costruisce il Job che lancia il container dfaas-exporter
// per estrarre le metriche da Prometheus.
// startTime = termTime - 1min, endTime = adesso - 30s.
//
// Destinazione CSV:
//   - se exp.Spec.GoogleDrive != nil: monta il Secret indicato in
//     CredentialsSecretRef su /var/run/gdrive e passa GDRIVE_FOLDER_ID +
//     GDRIVE_CREDENTIALS_PATH al binario, che caricherà il CSV su Drive.
//   - se nil: il binario stamperà il CSV su stdout (recuperabile via
//     `kubectl logs job/<exp>-exporter-job`).
func (r *EsperimentoReconciler) createExporterJob(exp *dfaasv1.Esperimento, queries string, termTime time.Time) *batchv1.Job {
	startTime := termTime.Add(-1 * time.Minute).UTC().Format(time.RFC3339)
	endTime := time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)

	env := []corev1.EnvVar{
		{Name: "PROM_URL", Value: "http://prometheus-server.monitoring.svc.cluster.local:9090"},
		{Name: "QUERIES", Value: queries},
		{Name: "START_TIME", Value: startTime},
		{Name: "END_TIME", Value: endTime},
		{Name: "STEP", Value: "5s"},
		{Name: "EXP_NAME", Value: exp.Name},
	}

	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	if exp.Spec.GoogleDrive != nil {
		env = append(env,
			corev1.EnvVar{Name: "GDRIVE_FOLDER_ID", Value: exp.Spec.GoogleDrive.FolderID},
			corev1.EnvVar{Name: "GDRIVE_CREDENTIALS_PATH", Value: "/var/run/gdrive/credentials.json"},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "gdrive-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: exp.Spec.GoogleDrive.CredentialsSecretRef,
				},
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
			Name:      exp.Name + "-exporter-job",
			Namespace: exp.Namespace,
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

	_ = ctrl.SetControllerReference(exp, job, r.Scheme)
	return job
}

func int32Ptr(i int32) *int32 { return &i }
