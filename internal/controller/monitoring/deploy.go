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

// Deploy installs or upgrades the three Helm releases of the monitoring stack
// (Prometheus + Grafana + SeaweedFS) in the "monitoring" namespace. Idempotent:
// the first Reconcile runs an Install, subsequent ones an Upgrade.
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
	// The dashboard JSON is kept as its own file -- editable, diffable, pinned
	// by a test -- and handed to the chart here instead of being pasted into
	// the values YAML as a 600-line string. The provider that mounts it is
	// declared in grafana-values.yaml; the two names must match.
	grafValues["dashboards"] = map[string]interface{}{
		"dfaas": map[string]interface{}{
			liveDashboardUID: map[string]interface{}{"json": string(liveDashboardJSON)},
		},
	}
	if _, err := helm.InstallOrUpgradeFromArchive(
		ctx, log, "grafana",
		bytes.NewReader(grafanaChartTGZ),
		"monitoring", grafValues,
	); err != nil {
		return fmt.Errorf("helm grafana: %w", err)
	}

	// 4. SeaweedFS — the in-cluster default S3 sink, from the official chart in
	//    all-in-one mode (master + volume + filer + S3 gateway in one process).
	//    The Service resolves as
	//    seaweedfs-all-in-one.monitoring.svc.cluster.local:8333 and is referenced
	//    by the dfaas-s3/seaweedfs-default config Secret. The S3 port 8333 →
	//    NodePort 30900 and the filer 8888 → 30901 are chosen to avoid colliding
	//    with Prometheus (9090/30090) and Grafana (30300).
	var swfsValues map[string]interface{}
	if err := sigsyaml.Unmarshal(seaweedfsValuesYAML, &swfsValues); err != nil {
		return fmt.Errorf("parse seaweedfs values: %w", err)
	}
	if _, err := helm.InstallOrUpgradeFromArchive(
		ctx, log, "seaweedfs",
		bytes.NewReader(seaweedfsChartTGZ),
		"monitoring", swfsValues,
	); err != nil {
		return fmt.Errorf("helm seaweedfs: %w", err)
	}

	return nil
}
