/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"
)

// EnsurePlaybookConfigMap creates/updates the per-role ConfigMap holding the
// embedded Ansible playbook. The ConfigMap name encodes both env and role so
// the dfaas-worker and k6-load-generator phases never collide.
func (m *Manager) EnsurePlaybookConfigMap(ctx context.Context, env *dfaasv1.Environment, role dfaasv1.NodeRole) error {
	cmName := playbookConfigMapName(env, role)

	data := map[string]string{
		"requirements.yml": galaxyRequirements,
	}
	switch role {
	case dfaasv1.RoleK6LoadGenerator:
		data["setup-k6-nodes.yml"] = k6Playbook
	default:
		data["setup-nodes.yml"] = ansiblePlaybook
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: env.Namespace},
		Data:       data,
	}
	if err := ctrl.SetControllerReference(env, cm, m.Scheme); err != nil {
		return err
	}

	found := &corev1.ConfigMap{}
	err := m.Get(ctx, types.NamespacedName{Name: cmName, Namespace: env.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		return m.Create(ctx, cm)
	}
	if err != nil {
		return err
	}
	found.Data = cm.Data
	return m.Update(ctx, found)
}

// EnsureHelmValues creates/updates the ConfigMap holding Helm-values templates
// consumed by the dfaas-worker playbook (HAProxy, OpenFaaS, per-VM
// Prometheus). The same ConfigMap is mounted by both phase Jobs even though
// the k6 playbook ignores it.
func (m *Manager) EnsureHelmValues(ctx context.Context, env *dfaasv1.Environment) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "helm-values-config-" + env.Name,
			Namespace: env.Namespace,
		},
		Data: map[string]string{
			"haproxy.yaml":    haproxyValues,
			"openfaas.yaml":   openfaasValues,
			"prometheus.yaml": prometheusValues,
		},
	}
	if err := ctrl.SetControllerReference(env, cm, m.Scheme); err != nil {
		return err
	}

	found := &corev1.ConfigMap{}
	err := m.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: env.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		return m.Create(ctx, cm)
	}
	if err != nil {
		return err
	}
	found.Data = cm.Data
	return m.Update(ctx, found)
}
