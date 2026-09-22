package monitoring

import (
	"bytes"
	"context"
	"fmt"

	"dfaas-operator/internal/helm"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	if err := m.pruneLegacySeaweedFS(ctx); err != nil {
		return fmt.Errorf("prune legacy seaweedfs: %w", err)
	}
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

// pruneLegacySeaweedFS deletes the four objects the operator used to
// server-side-apply before SeaweedFS became a Helm release. They carry no Helm
// ownership metadata, so `helm install` would not adopt them, and the legacy
// "seaweedfs" Service holds NodePorts 30900/30901 — the install would fail with
// "provided port is already allocated" until it is gone. The Service therefore
// goes first: it frees both ports at once and has no finalizer, so nothing has
// to be awaited before the install.
//
// Deleting the legacy PVC discards CSVs and k6 logs exported before the
// migration; the chart provisions a fresh seaweedfs-all-in-one-data claim.
//
// ponytail: dead code once every cluster has migrated — drop it then. Every
// name here differs from the release's own objects (seaweedfs-all-in-one,
// seaweedfs-all-in-one-data, seaweedfs-s3-secret), so this can never remove
// what the chart just installed.
func (m *Manager) pruneLegacySeaweedFS(ctx context.Context) error {
	logger := log.FromContext(ctx)

	// The claim and the Secret are listed under BOTH names on purpose. The
	// pre-Helm SeaweedFS Deployment was itself a rename of the even earlier
	// MinIO one and kept MinIO's claim and Secret, so on any cluster that came
	// up before that rename the live objects are `minio-pvc` / `minio-creds`
	// and the seaweedfs-* names never existed. Pruning only the latter left a
	// bound 10Gi claim orphaned in `monitoring` with no workload mounting it —
	// invisible, still billed, and confusing to whoever looks next.
	legacy := []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "seaweedfs", Namespace: "monitoring"}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "seaweedfs-deployment", Namespace: "monitoring"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "seaweedfs-pvc", Namespace: "monitoring"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "seaweedfs-creds", Namespace: "monitoring"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "minio-pvc", Namespace: "monitoring"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "minio-creds", Namespace: "monitoring"}},
	}

	for _, obj := range legacy {
		if err := m.Delete(ctx, obj); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("delete %T %s: %w", obj, obj.GetName(), err)
		}
		logger.Info("removed pre-Helm SeaweedFS object",
			"kind", fmt.Sprintf("%T", obj), "name", obj.GetName())
	}
	return nil
}
