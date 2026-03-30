package controller

import (
	"bytes"
	"context"
	"fmt"
	"text/template"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// Definiamo uno struct di supporto per passare dati puliti al template
type k6TemplateData struct {
	Exp      *dfaasv1.Esperimento
	IPMapper map[string]string // Mappa TargetNodeID -> IndirizzoIP
}

const k6Template = `
import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  tags: {
    esperimento: '{{ .Exp.Name }}',
  },
  scenarios: {
    {{- range .Exp.Spec.Profilo.Scenari }}
    "{{ .NomeScenario }}": {
      executor: 'ramping-arrival-rate',
      startRate: {{ .StartTime }},
      timeUnit: '1s',
      preAllocatedVUs: {{ .VuAllocati }},
      maxVUs: {{ .VuAllocati }},
      stages: [
        {{- range .Stages }}
        { duration: '{{ .Durata }}', target: {{ .TargetRps }} },
        {{- end }}
      ],
      env: { 
        // Usiamo la mappa per recuperare l'IP corretto tramite l'ID del nodo
        TARGET_URL: 'http://{{ index $.IPMapper .TargetNodeID }}/function/{{ .NomeFunzioneTarget }}',
        BODY_CONTENT: '{{ .Body }}'
      },
    },
    {{- end }}
  },
};

export default function () {
  const url = __ENV.TARGET_URL;
  const payload = __ENV.BODY_CONTENT;
  
  const params = {
    headers: {
      {{ .Exp.Spec.Profilo.CommonHeaders }} 
    },
  };

  http.post(url, payload, params);
}
`

func (r *EsperimentoReconciler) buildScriptK6(exp *dfaasv1.Esperimento) (string, error) {
	// 1. Prepariamo la mappa degli IP per il template
	ipMap := make(map[string]string)
	for _, nodo := range exp.Spec.Federazione.Nodi {
		ipMap[nodo.IDNodo] = nodo.IndirizzoIP
	}

	// 2. Verifichiamo che tutti i nodi usati negli scenari esistano nella federazione
	for _, scenario := range exp.Spec.Profilo.Scenari {
		if _, ok := ipMap[scenario.TargetNodeID]; !ok {
			return "", fmt.Errorf("nodo target %s non trovato nella configurazione federazione", scenario.TargetNodeID)
		}
	}

	data := k6TemplateData{
		Exp:      exp,
		IPMapper: ipMap,
	}

	tmpl, err := template.New("k6").Parse(k6Template)
	if err != nil {
		return "", err
	}

	var script bytes.Buffer
	if err := tmpl.Execute(&script, data); err != nil {
		return "", err
	}

	return script.String(), nil
}

func (r *EsperimentoReconciler) runK6Job(ctx context.Context,
	exp *dfaasv1.Esperimento) error {
	terminate := int64(30000000)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exp.Name + "-k6-job",
			Namespace: exp.Namespace,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &terminate,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "k6",
							Image: "grafana/k6:1.4.2",
							Args:  []string{"run", "/test/test.js", "--out", "experimental-prometheus-rw"},
							Env: []corev1.EnvVar{
								{
									// URL dell'endpoint write di Prometheus (usando il DNS interno di K8s)
									Name:  "K6_PROMETHEUS_RW_SERVER_URL",
									Value: "http://prometheus-service.monitoring.svc.cluster.local:9090/api/v1/write",
								},
								{
									// Specifichiamo quali statistiche vogliamo
									Name:  "K6_PROMETHEUS_RW_TREND_STATS",
									Value: "p(95),p(99),avg,max",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "script-volume",
									MountPath: "/test",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "script-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: exp.Name + "-script-k6", // Deve combaciare con la CM creata
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Imposta l'Esperimento come "proprietario" del Job (se cancelli l'esperimento, sparisce il job)
	if err := ctrl.SetControllerReference(exp, job, r.Scheme); err != nil {
		return err
	}

	return r.Create(ctx, job)
}

func (r *EsperimentoReconciler) reconcileK6Config(ctx context.Context,
	exp *dfaasv1.Esperimento, script string) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exp.Name + "-script-k6",
			Namespace: exp.Namespace,
		},
		Data: map[string]string{"test.js": script},
	}

	if err := ctrl.SetControllerReference(exp, cm, r.Scheme); err != nil {
		return err
	}

	// Tenta creazione, se esiste già fa l'update del contenuto
	err := r.Create(ctx, cm)
	if errors.IsAlreadyExists(err) {
		existingCm := &corev1.ConfigMap{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cm), existingCm); err != nil {
			return err
		}
		existingCm.Data = cm.Data
		return r.Update(ctx, existingCm)
	}
	return err
}
