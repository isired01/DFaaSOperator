/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package monitoring

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
)

// ReconcileTargets writes one JSON file per Environment into the shared
// `prometheus-targets` ConfigMap in the `monitoring` namespace. Prometheus
// re-reads /etc/prometheus/file_sd/*.json every refresh_interval (30s — see
// values/prometheus-values.yaml), so no reload is required when targets
// change.
//
// Only dfaas-worker nodes are scraped — k6-load-generator nodes run their own
// k3s and are not part of the operator-cluster monitoring.
func (m *Manager) ReconcileTargets(ctx context.Context, env *dfaasv1.Environment) error {
	logger := log.FromContext(ctx)

	var targets []PrometheusTarget
	for _, n := range env.Spec.Nodes {
		if n.Role != dfaasv1.RoleDfaasWorker {
			continue
		}
		targets = append(targets, PrometheusTarget{
			Targets: []string{n.IPAddress + ":30909"},
			Labels: map[string]string{
				"environment": env.Name,
				"nodo_id":     n.NodeID,
				"tipo_nodo":   string(n.Capacity),
			},
		})
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
			ObjectMeta: metav1.ObjectMeta{Name: "prometheus-targets", Namespace: "monitoring"},
			Data:       map[string]string{},
		}
		if err := m.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create prometheus-targets configmap: %w", err)
		}
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	fileName := env.Name + ".json"
	cm.Data[fileName] = string(jsonData)

	if err := m.Update(ctx, cm); err != nil {
		logger.Error(err, "unable to update prometheus-targets ConfigMap")
		return err
	}
	logger.Info("prometheus targets reconciled", "file", fileName)
	return nil
}

// CleanupTargets removes the per-environment file from the shared
// `prometheus-targets` ConfigMap.
func (m *Manager) CleanupTargets(ctx context.Context, env *dfaasv1.Environment) error {
	logger := log.FromContext(ctx)

	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	if err := m.Get(ctx, cmKey, cm); err != nil {
		return client.IgnoreNotFound(err)
	}

	fileName := env.Name + ".json"
	if _, exists := cm.Data[fileName]; exists {
		delete(cm.Data, fileName)
		if err := m.Update(ctx, cm); err != nil {
			logger.Error(err, "failed to remove env file from prometheus-targets")
			return err
		}
		logger.Info("prometheus targets removed", "file", fileName)
	}
	return nil
}
