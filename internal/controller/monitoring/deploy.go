package monitoring

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"dfaas-operator/internal/helm"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	sigsyaml "sigs.k8s.io/yaml"
)

// minioFieldOwner is the server-side-apply field manager used for the raw
// MinIO objects, so re-applies are an idempotent merge rather than a clobber.
const minioFieldOwner = "dfaas-operator"

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

	// 4. MinIO — the in-cluster default S3 sink. Not a Helm chart: decode the
	//    embedded multi-doc manifest and server-side-apply each object. The
	//    Service resolves as minio.monitoring.svc.cluster.local:9000 and is
	//    referenced by the dfaas-s3/minio-default config Secret. Ports 9000/
	//    9001 + NodePorts 30900/30901 are chosen to avoid colliding with
	//    Prometheus (9090/30090) and Grafana (30300).
	if err := m.deployMinIO(ctx); err != nil {
		return fmt.Errorf("deploy minio: %w", err)
	}

	return nil
}

// deployMinIO decodes the embedded multi-doc minio.yaml and server-side-applies
// each object with FieldOwner "dfaas-operator". Idempotent: re-runs merge into
// the existing objects rather than recreating them. The "monitoring" namespace
// is guaranteed to exist by the caller (Deploy creates it first).
func (m *Manager) deployMinIO(ctx context.Context) error {
	logger := log.FromContext(ctx)

	objs, err := decodeMultiDocYAML(minioYAML)
	if err != nil {
		return fmt.Errorf("decode minio manifest: %w", err)
	}

	for _, obj := range objs {
		gvk := obj.GroupVersionKind()
		logger.Info("applying minio object",
			"kind", gvk.Kind, "name", obj.GetName(), "namespace", obj.GetNamespace())
		if err := m.Patch(ctx, obj, client.Apply,
			client.FieldOwner(minioFieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("apply minio %s/%s: %w", gvk.Kind, obj.GetName(), err)
		}
	}
	logger.Info("minio default S3 sink applied", "objects", len(objs))
	return nil
}

// decodeMultiDocYAML splits a multi-document YAML stream into a slice of
// unstructured objects, skipping empty documents. Returned objects are ready
// for server-side apply (each carries apiVersion/kind/metadata from the
// manifest).
func decodeMultiDocYAML(data []byte) ([]*unstructured.Unstructured, error) {
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var out []*unstructured.Unstructured
	for {
		raw := map[string]interface{}{}
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}
		out = append(out, &unstructured.Unstructured{Object: raw})
	}
	return out, nil
}
