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

func (m *Manager) EnsureConfigMap(ctx context.Context, exp *dfaasv1.Esperimento) error {
	cmName := "ansible-playbooks-" + exp.Name
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: exp.Namespace,
		},
		Data: map[string]string{
			"setup-Nodes.yml": ansiblePlaybook,
		},
	}

	if err := ctrl.SetControllerReference(exp, cm, m.Scheme); err != nil {
		return err
	}

	found := &corev1.ConfigMap{}
	err := m.Get(ctx, types.NamespacedName{Name: cmName, Namespace: exp.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		return m.Create(ctx, cm)
	} else if err == nil {
		found.Data = cm.Data
		return m.Update(ctx, found)
	}
	return err
}

func (m *Manager) EnsureHelmValues(ctx context.Context, exp *dfaasv1.Esperimento) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "helm-values-config-" + exp.Name,
			Namespace: exp.Namespace,
		},
		Data: map[string]string{
			"haproxy.yaml":    haproxyValues,
			"openfaas.yaml":   openfaasValues,
			"prometheus.yaml": prometheusValues,
		},
	}

	if err := ctrl.SetControllerReference(exp, cm, m.Scheme); err != nil {
		return err
	}

	found := &corev1.ConfigMap{}
	err := m.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: exp.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		return m.Create(ctx, cm)
	} else if err == nil {
		found.Data = cm.Data
		return m.Update(ctx, found)
	}
	return err
}
