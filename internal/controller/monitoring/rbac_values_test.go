package monitoring

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sigsyaml "sigs.k8s.io/yaml"
)

// clusterRoleName is the ClusterRole the dfaas chart pre-creates for the
// Prometheus server, because the operator's own ClusterRole deliberately
// carries no clusterroles/clusterrolebindings verbs.
const clusterRoleName = "dfaas-prometheus-server"

// TestMonitoringChartsCreateNoClusterRBAC guards the settings that keep the
// operator-driven Helm installs inside its RBAC budget. Both charts create a
// ClusterRole + ClusterRoleBinding by default; the operator cannot, and giving
// it the verbs to would also mean giving it escalate/bind. Drop either setting
// and Deploy() fails at install time with a Forbidden the Environment surfaces
// only as a phase that never reaches Ready.
func TestMonitoringChartsCreateNoClusterRBAC(t *testing.T) {
	var prom struct {
		Server struct {
			UseExistingClusterRoleName string `json:"useExistingClusterRoleName"`
		} `json:"server"`
	}
	if err := sigsyaml.Unmarshal(prometheusValuesYAML, &prom); err != nil {
		t.Fatalf("parse prometheus values: %v", err)
	}
	// Setting this suppresses the chart's ClusterRole *and* its ClusterRoleBinding.
	if got := prom.Server.UseExistingClusterRoleName; got != clusterRoleName {
		t.Errorf("server.useExistingClusterRoleName = %q, want %q", got, clusterRoleName)
	}

	var graf struct {
		RBAC struct {
			Namespaced bool `json:"namespaced"`
		} `json:"rbac"`
	}
	if err := sigsyaml.Unmarshal(grafanaValuesYAML, &graf); err != nil {
		t.Fatalf("parse grafana values: %v", err)
	}
	if !graf.RBAC.Namespaced {
		t.Error("rbac.namespaced must be true: otherwise grafana wants a ClusterRole the operator cannot create")
	}
}

// TestClusterRoleShippedByChart ties the name above to the chart template that
// actually declares it. The two are coupled by a hard-coded string across a Go
// values file and a Helm template, so a rename on one side is invisible until
// Prometheus starts up unable to read any target.
func TestClusterRoleShippedByChart(t *testing.T) {
	path := filepath.Join("..", "..", "..", "charts", "dfaas", "templates", "monitoring-rbac.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	tpl := string(b)

	for _, want := range []string{
		"name: " + clusterRoleName,
		"name: prometheus-server", // ClusterRoleBinding subject
		"namespace: monitoring",
	} {
		if !strings.Contains(tpl, want) {
			t.Errorf("%s does not contain %q", path, want)
		}
	}
}
