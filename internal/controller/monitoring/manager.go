// Package monitoring encapsulates management of the monitoring stack
// (Prometheus + Grafana via Helm) and the dynamic configuration of the
// file-based service discovery targets.
//
// Helm assets (chart .tgz and values.yaml) are bundled into the binary via
// go:embed (see assets.go). Install/upgrade operations are delegated to the
// SDK wrapper in internal/helm/.
package monitoring

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// Stack is the monitoring stack as the Environment reconciler needs it. It
// exists because the reconciler used to build a *Manager inside its own body,
// and Manager.Deploy reaches helm.InstallOrUpgradeFromArchive -> cli.New(),
// which resolves $KUBECONFIG or the in-cluster config and performs a real Helm
// install. Nothing could be substituted, so ensureMonitoring -- five distinct
// outcomes, including the deliberate distinction between "readiness could not
// be evaluated" and "not ready yet" -- was referenced in zero test files.
//
// Manager satisfies this interface verbatim; monitoring/fake records the calls.
type Stack interface {
	Deploy(ctx context.Context) error
	Check(ctx context.Context) (ready bool, err error)
	ReconcileTargets(ctx context.Context, env *dfaasv1.Environment) error
	CleanupTargets(ctx context.Context, env *dfaasv1.Environment) error
}

var _ Stack = (*Manager)(nil)

// Manager runs the monitoring provisioning operations (Prometheus + Grafana)
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
