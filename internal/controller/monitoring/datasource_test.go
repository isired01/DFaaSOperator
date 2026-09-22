package monitoring

import (
	"fmt"
	"strings"
	"testing"

	sigsyaml "sigs.k8s.io/yaml"
)

// Grafana reaches Prometheus by a URL written in one values file, while the
// port it answers on is set in another. Nothing connects the two, and when
// they disagree Grafana says nothing useful: every panel is simply empty, and
// the datasource health check reports a timeout on port 80. Found on a live
// run, where the whole dashboard looked broken and the dashboard was fine.
func TestGrafanaDatasourceURLCarriesThePrometheusPort(t *testing.T) {
	var prom struct {
		Server struct {
			Service struct {
				ServicePort int `json:"servicePort"`
			} `json:"service"`
		} `json:"server"`
	}
	if err := sigsyaml.Unmarshal(prometheusValuesYAML, &prom); err != nil {
		t.Fatalf("parse prometheus values: %v", err)
	}
	if prom.Server.Service.ServicePort == 0 {
		t.Fatal("server.service.servicePort is unset; the chart default would apply and this pin is blind")
	}

	var graf struct {
		Datasources map[string]struct {
			Datasources []struct {
				Name string `json:"name"`
				URL  string `json:"url"`
			} `json:"datasources"`
		} `json:"datasources"`
	}
	if err := sigsyaml.Unmarshal(grafanaValuesYAML, &graf); err != nil {
		t.Fatalf("parse grafana values: %v", err)
	}

	want := fmt.Sprintf(":%d", prom.Server.Service.ServicePort)
	found := false
	for _, file := range graf.Datasources {
		for _, ds := range file.Datasources {
			if ds.Name != "Prometheus" {
				continue
			}
			found = true
			if !strings.HasSuffix(ds.URL, want) {
				t.Errorf("datasource URL %q does not end in %q — Grafana would query port 80 and every panel stays empty",
					ds.URL, want)
			}
		}
	}
	if !found {
		t.Error("no Prometheus datasource in the grafana values")
	}
}
