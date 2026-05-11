package monitoring

import (
	"context"
	"encoding/json"
	"fmt"

	dfaasv1 "dfaas-operator/api/v1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ReconcileTargets gestisce la registrazione dinamica dei target di
// monitoraggio. Implementa il pattern "File-Based Service Discovery" di
// Prometheus: invece di modificare la configurazione globale, l'operatore
// inietta un file JSON dedicato all'esperimento in una ConfigMap condivisa
// montata nel pod di monitoraggio.
//
// Flusso operativo:
//  1. Definisce i target (IP e Porte) e applica label identificative
//     (esperimento, nodo_id, tipo_nodo) per permettere a Grafana di filtrare
//     i dati di questo specifico test.
//  2. Serializza la struttura in formato JSON compatibile con Prometheus SD.
//  3. Aggiorna la ConfigMap 'prometheus-targets' nel namespace 'monitoring'.
//  4. Prometheus rilegge i file *.json in /etc/prometheus/file_sd ogni
//     refresh_interval (30s — vedi values/prometheus-values.yaml). Nessun
//     reload necessario per cambi di target SD.
func (m *Manager) ReconcileTargets(ctx context.Context, exp *dfaasv1.Esperimento) error {
	log := log.FromContext(ctx)

	var targets []PrometheusTarget
	for _, nodo := range exp.Spec.Federation.Nodes {
		target := PrometheusTarget{
			Targets: []string{nodo.IpAddress + ":30909"},
			Labels: map[string]string{
				"esperimento": exp.Name,
				"nodo_id":     nodo.NodeID,
				"tipo_nodo":   string(nodo.Capacity),
			},
		}
		targets = append(targets, target)
	}

	jsonData, err := json.Marshal(targets)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	if err := m.Get(ctx, cmKey, cm); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "prometheus-targets",
				Namespace: "monitoring",
			},
			Data: map[string]string{},
		}
		if err := m.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create prometheus-targets configmap: %w", err)
		}
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	fileName := exp.Name + ".json"
	cm.Data[fileName] = string(jsonData)

	if err := m.Update(ctx, cm); err != nil {
		log.Error(err, "Impossibile aggiornare la ConfigMap dei target")
		return err
	}

	log.Info("🎯 Target di monitoraggio aggiornati in Prometheus", "file", fileName)
	return nil
}

// CleanupTargets rimuove il file di configurazione specifico dell'esperimento
// dalla ConfigMap di Prometheus. Interrompe il monitoraggio dei nodi
// associati a questo test, liberando risorse nel database centrale.
func (m *Manager) CleanupTargets(ctx context.Context, exp *dfaasv1.Esperimento) error {
	log := log.FromContext(ctx)

	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	if err := m.Get(ctx, cmKey, cm); err != nil {
		return client.IgnoreNotFound(err)
	}

	fileName := exp.Name + ".json"
	if _, esiste := cm.Data[fileName]; esiste {
		delete(cm.Data, fileName)

		if err := m.Update(ctx, cm); err != nil {
			log.Error(err, "Errore durante la rimozione del file JSON da Prometheus targets")
			return err
		}
		log.Info("🗑️ Target di monitoraggio rimossi con successo", "file", fileName)
	}

	return nil
}
