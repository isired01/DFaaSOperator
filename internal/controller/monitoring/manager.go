// Package monitoring manages the monitoring stack (Prometheus, Grafana and
// SeaweedFS via Helm) and the file-based service discovery targets.
//
// The chart archives and values files are embedded in the binary (see
// assets.go). Install and upgrade go through the SDK wrapper in internal/helm/.
package monitoring

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// Stack is the monitoring stack as the Environment reconciler needs it.
// Manager satisfies this interface verbatim; monitoring/fake records the calls.
type Stack interface {
	Deploy(ctx context.Context) error
	Check(ctx context.Context) (ready bool, err error)
	ReconcileTargets(ctx context.Context, env *dfaasv1.Environment) error
	CleanupTargets(ctx context.Context, env *dfaasv1.Environment) error
}

var _ Stack = (*Manager)(nil)

// Manager runs the monitoring provisioning operations (Prometheus, Grafana and SeaweedFS)
// and the reconciliation of Prometheus targets for an environment.
type Manager struct {
	client.Client
}

// PrometheusTarget is the file-based service discovery unit for Prometheus.
// Serialized to JSON and injected into the prometheus-targets ConfigMap, it is
// read by the Prometheus pod via file_sd_configs.
type PrometheusTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}
