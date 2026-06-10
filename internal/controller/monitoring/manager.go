// Package monitoring encapsulates management of the monitoring stack
// (Prometheus + Grafana via Helm) and the dynamic configuration of the
// file-based service discovery targets.
//
// Helm assets (chart .tgz and values.yaml) are bundled into the binary via
// go:embed (see assets.go). Install/upgrade operations are delegated to the
// SDK wrapper in internal/helm/.
package monitoring

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager runs the monitoring provisioning operations (Prometheus + Grafana)
// and the reconciliation of Prometheus targets for an environment.
type Manager struct {
	client.Client
	Scheme *runtime.Scheme
}

// PrometheusTarget is the file-based service discovery unit for Prometheus.
// Serialized to JSON and injected into the prometheus-targets ConfigMap, it is
// read by the Prometheus pod via file_sd_configs.
type PrometheusTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}
