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
// per estrarre le metriche da Prometheus e caricarle su MinIO.
// startTime = termTime - 1min, endTime = adesso - 30s.
func (r *EsperimentoReconciler) createExporterJob(exp *dfaasv1.Esperimento, queries string, termTime time.Time) *batchv1.Job {
	startTime := termTime.Add(-1 * time.Minute).UTC().Format(time.RFC3339)
	endTime := time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)

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
							Name:  "exporter",
							Image: "ghcr.io/isired01/dfaas-exporter:latest",
							Env: []corev1.EnvVar{
								{Name: "PROM_URL", Value: "http://prometheus-service.monitoring.svc.cluster.local:9090"},
								{Name: "QUERIES", Value: queries},
								{Name: "START_TIME", Value: startTime},
								{Name: "END_TIME", Value: endTime},
								{Name: "STEP", Value: "5s"},
								{Name: "EXP_NAME", Value: exp.Name},
								{Name: "MINIO_ENDPOINT", Value: "minio-service.monitoring.svc.cluster.local:9000"},
								{Name: "MINIO_ACCESS_KEY", Value: "admin"},       // Idealmente da Secret
								{Name: "MINIO_SECRET_KEY", Value: "password123"}, // Idealmente da Secret
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}

	_ = ctrl.SetControllerReference(exp, job, r.Scheme)
	return job
}

func int32Ptr(i int32) *int32 { return &i }
