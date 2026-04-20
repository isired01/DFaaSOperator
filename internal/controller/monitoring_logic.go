package controller

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

//go:embed monitoring/*.yaml
var monitoringConfig embed.FS

func (r *EsperimentoReconciler) deployMonitoringStack(ctx context.Context) error {
	log := log.FromContext(ctx)

	// Legge i file dalla cartella monitoring (quella con l'embed)
	entries, err := monitoringConfig.ReadDir("monitoring")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		fileData, err := monitoringConfig.ReadFile("monitoring/" + entry.Name())
		if err != nil {
			return err
		}

		// Decoder per gestire file multi-oggetto (separati da ---)
		decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(fileData), 4096)
		for {
			unstructuredObj := &unstructured.Unstructured{}
			if err := decoder.Decode(unstructuredObj); err != nil {
				break // Fine file
			}

			if unstructuredObj.Object == nil {
				continue
			}

			unstructuredObj.SetNamespace("monitoring")

			// Tenta la creazione. Se esiste già, passa oltre (Idempotenza)
			err = r.Create(ctx, unstructuredObj)
			if err != nil {
				if errors.IsAlreadyExists(err) {
					continue
				}
				if errors.IsInvalid(err) {
					log.Info("⚠️ Risorsa già configurata o porta occupata, salto...", "file", entry.Name())
					continue
				}
				return err
			}
			log.Info("✅ Creato componente monitoraggio:", "kind", unstructuredObj.GetKind(), "name", unstructuredObj.GetName())
		}
	}
	return nil
}

func (r *EsperimentoReconciler) checkMonitoringStack(ctx context.Context) (bool, error) {
	podList := &corev1.PodList{}
	opts := []client.ListOption{
		client.InNamespace("monitoring"),
	}

	if err := r.List(ctx, podList, opts...); err != nil {
		return false, err
	}

	promReady := false
	grafanaReady := false
	minioReady := false // AGGIUNTO: Check per MinIO

	for _, pod := range podList.Items {
		isReady := false
		if pod.Status.Phase == corev1.PodRunning {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					isReady = true
					break
				}
			}
		}

		if isReady {
			if strings.Contains(pod.Name, "prometheus") {
				promReady = true
			}
			if strings.Contains(pod.Name, "grafana") {
				grafanaReady = true
			}
			if strings.Contains(pod.Name, "minio") { // AGGIUNTO
				minioReady = true
			}
		}
	}

	// L'esperimento parte solo se anche MinIO è pronto a ricevere i dati
	return promReady && grafanaReady && minioReady, nil
}

// reconcilePrometheusTargets gestisce la registrazione dinamica dei target di monitoraggio.
// Implementa il pattern "File-Based Service Discovery" di Prometheus: invece di modificare
// la configurazione globale, l'operatore inietta un file JSON dedicato all'esperimento
// in una ConfigMap condivisa montata nel pod di monitoraggio.
//
// Flusso operativo:
// 1. Definisce i target (IP e Porte) e applica label identificative (id_esperimento)
//    per permettere a Grafana di filtrare i dati di questo specifico test.
// 2. Serializza la struttura in formato JSON compatibile con Prometheus SD.
// 3. Aggiorna la ConfigMap 'prometheus-targets' nel namespace 'monitoring'.
// 4. Sfrutta il sidecar 'config-reloader' per notificare Prometheus del nuovo file
//    senza causare il riavvio del servizio, garantendo continuità nella raccolta metriche.

func (r *EsperimentoReconciler) reconcilePrometheusTargets(ctx context.Context, exp *dfaasv1.Esperimento) error {
	log := log.FromContext(ctx)

	var targets []PrometheusTarget

	// Cicliamo sui nodi definiti nell'oggetto Esperimento
	for _, nodo := range exp.Spec.Federazione.Nodi {
		target := PrometheusTarget{
			Targets: []string{nodo.IndirizzoIP + ":30909"}, // Usa l'IP della CRD e la porta di dFaaS
			Labels: map[string]string{
				"esperimento": exp.Name,
				"nodo_id":     nodo.IDNodo,
				"tipo_nodo":   nodo.Capacità.Tipo,
			},
		}
		targets = append(targets, target)
	}

	// 2. Serializziamo in JSON
	jsonData, err := json.Marshal(targets)
	if err != nil {
		return err
	}

	// 3. Recuperiamo la ConfigMap globale dei target
	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	if err := r.Get(ctx, cmKey, cm); err != nil {
		return err
	}

	// 4. Inseriamo il file specifico per questo esperimento
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	fileName := exp.Name + ".json"
	cm.Data[fileName] = string(jsonData)

	// 5. Update su Kubernetes
	if err := r.Update(ctx, cm); err != nil {
		log.Error(err, "Impossibile aggiornare la ConfigMap dei target")
		return err
	}

	log.Info("🎯 Target di monitoraggio aggiornati in Prometheus", "file", fileName)
	return nil
}

// cleanupPrometheusTargets rimuove il file di configurazione specifico dell'esperimento
// dalla ConfigMap di Prometheus. Questa operazione interrompe il monitoraggio dei nodi
// associati a questo test, liberando risorse nel database centrale.
func (r *EsperimentoReconciler) cleanupPrometheusTargets(ctx context.Context, exp *dfaasv1.Esperimento) error {
	log := log.FromContext(ctx)

	// 1. Recuperiamo la ConfigMap globale dei target
	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	if err := r.Get(ctx, cmKey, cm); err != nil {
		// Se la ConfigMap non esiste, non c'è nulla da pulire
		return client.IgnoreNotFound(err)
	}

	// 2. Verifichiamo se il file dell'esperimento esiste e lo rimuoviamo
	fileName := exp.Name + ".json"
	if _, esiste := cm.Data[fileName]; esiste {
		delete(cm.Data, fileName)

		// 3. Update della ConfigMap per notificare il Sidecar della rimozione
		if err := r.Update(ctx, cm); err != nil {
			log.Error(err, "Errore durante la rimozione del file JSON da Prometheus targets")
			return err
		}
		log.Info("🗑️ Target di monitoraggio rimossi con successo", "file", fileName)
	}

	return nil
}