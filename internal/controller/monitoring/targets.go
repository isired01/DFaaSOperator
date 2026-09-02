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
	"k8s.io/client-go/util/retry"
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
//
// The ConfigMap is shared by every Environment, so the whole Get→mutate→Update
// runs under retry.RetryOnConflict: two Environments reconciling at once
// otherwise race, and the loser's file is dropped with a bare 409.
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
				"node_id":     n.NodeID,
				"node_type":   string(n.Capacity),
			},
		})
	}

	jsonData, err := json.Marshal(targets)
	if err != nil {
		return err
	}

	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	fileName := env.Name + ".json"

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{}
		if gerr := m.Get(ctx, cmKey, cm); gerr != nil {
			if !apierrors.IsNotFound(gerr) {
				return gerr
			}
			cm = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: cmKey.Name, Namespace: cmKey.Namespace},
				Data:       map[string]string{},
			}
			if cerr := m.Create(ctx, cm); cerr != nil {
				if !apierrors.IsAlreadyExists(cerr) {
					return fmt.Errorf("create prometheus-targets configmap: %w", cerr)
				}
				// Lost the create race with another environment's reconcile. Re-Get
				// the real object so we merge our file into its current Data (and
				// pick up its resourceVersion) rather than blind-overwriting it with
				// a fresh CM that holds only this environment's entry.
				if rerr := m.Get(ctx, cmKey, cm); rerr != nil {
					return fmt.Errorf("re-get prometheus-targets after create race: %w", rerr)
				}
			}
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data[fileName] = string(jsonData)
		return m.Update(ctx, cm)
	}); err != nil {
		logger.Error(err, "unable to update prometheus-targets ConfigMap")
		return err
	}
	logger.Info("prometheus targets reconciled", "file", fileName)
	return nil
}

// CleanupTargets removes the per-environment file from the shared
// `prometheus-targets` ConfigMap. Same conflict retry as ReconcileTargets: the
// ConfigMap is cross-environment, so a concurrent reconcile must not turn a
// deletion cleanup into a lost update (the Environment finalizer blocks on the
// returned error).
func (m *Manager) CleanupTargets(ctx context.Context, env *dfaasv1.Environment) error {
	logger := log.FromContext(ctx)

	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	fileName := env.Name + ".json"

	removed := false
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		removed = false
		cm := &corev1.ConfigMap{}
		if gerr := m.Get(ctx, cmKey, cm); gerr != nil {
			return client.IgnoreNotFound(gerr)
		}
		if _, exists := cm.Data[fileName]; !exists {
			return nil
		}
		delete(cm.Data, fileName)
		if uerr := m.Update(ctx, cm); uerr != nil {
			return uerr
		}
		removed = true
		return nil
	}); err != nil {
		logger.Error(err, "failed to remove env file from prometheus-targets")
		return err
	}
	if removed {
		logger.Info("prometheus targets removed", "file", fileName)
	}
	return nil
}
