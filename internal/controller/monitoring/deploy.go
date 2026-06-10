package monitoring

import (
	"bytes"
	"context"
	"fmt"

	"dfaas-operator/internal/helm"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	sigsyaml "sigs.k8s.io/yaml"
)

// Deploy installs or upgrades the two Helm releases of the monitoring stack
// (Prometheus + Grafana) in the "monitoring" namespace. Idempotent: the first
// Reconcile runs an Install, subsequent ones an Upgrade.
func (m *Manager) Deploy(ctx context.Context) error {
	log := log.FromContext(ctx)

	// 1. Ensure the "monitoring" namespace exists. Helm Install would do
	//    CreateNamespace=true, but ReconcileTargets creates the
	//    prometheus-targets ConfigMap in the same namespace and requires it to
	//    exist regardless of the order of the first two reconciles.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "monitoring"}}
	if err := m.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create monitoring namespace: %w", err)
	}

	// 2. Prometheus
	var promValues map[string]interface{}
	if err := sigsyaml.Unmarshal(prometheusValuesYAML, &promValues); err != nil {
		return fmt.Errorf("parse prometheus values: %w", err)
	}
	if _, err := helm.InstallOrUpgradeFromArchive(
		ctx, log, "prometheus",
		bytes.NewReader(prometheusChartTGZ),
		"monitoring", promValues,
	); err != nil {
		return fmt.Errorf("helm prometheus: %w", err)
	}

	// 3. Grafana
	var grafValues map[string]interface{}
	if err := sigsyaml.Unmarshal(grafanaValuesYAML, &grafValues); err != nil {
		return fmt.Errorf("parse grafana values: %w", err)
	}
	if _, err := helm.InstallOrUpgradeFromArchive(
		ctx, log, "grafana",
		bytes.NewReader(grafanaChartTGZ),
		"monitoring", grafValues,
	); err != nil {
		return fmt.Errorf("helm grafana: %w", err)
	}

	return nil
}
