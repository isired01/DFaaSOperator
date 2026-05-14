// Package monitoring incapsula la gestione del monitoring stack (Prometheus +
// Grafana via Helm) e la configurazione dinamica dei target di service
// discovery file-based.
//
// Asset Helm (chart .tgz e values.yaml) sono bundleati nel binario via
// go:embed (vedi assets.go). Le operazioni di install/upgrade sono delegate
// al wrapper SDK in internal/helm/.
package monitoring

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager esegue le operazioni di provisioning monitoring (Prometheus +
// Grafana) e la riconciliazione dei target Prometheus per un esperimento.
type Manager struct {
	client.Client
	Scheme *runtime.Scheme
}

// PrometheusTarget è l'unità di service discovery file-based per Prometheus.
// Serializzato in JSON e iniettato nella ConfigMap prometheus-targets, viene
// letto dal pod Prometheus tramite file_sd_configs.
type PrometheusTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}
