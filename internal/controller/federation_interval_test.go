package controller

import (
	"os"
	"testing"
	"time"

	sigsyaml "sigs.k8s.io/yaml"
)

// The export cool-down is derived from federationInterval, and the CSV loses
// its tail when the two disagree: the reconciler would wait for pulls that
// come at a different rhythm than it assumes. The number lives in two places
// that no compiler connects -- a Go constant and a Helm values file -- so it
// is pinned here instead.
func TestFederationIntervalMatchesThePrometheusValues(t *testing.T) {
	raw, err := os.ReadFile("monitoring/values/prometheus-values.yaml")
	if err != nil {
		t.Fatalf("read prometheus values: %v", err)
	}
	var values struct {
		Server struct {
			Global struct {
				ScrapeInterval string `json:"scrape_interval"`
			} `json:"global"`
		} `json:"server"`
	}
	if err := sigsyaml.Unmarshal(raw, &values); err != nil {
		t.Fatalf("parse prometheus values: %v", err)
	}
	got := values.Server.Global.ScrapeInterval
	if got == "" {
		t.Fatal("server.global.scrape_interval is unset: the chart default (1m) then applies, " +
			"and nothing here notices")
	}
	parsed, err := time.ParseDuration(got)
	if err != nil {
		t.Fatalf("scrape_interval %q is not a Go duration: %v", got, err)
	}
	if parsed != federationInterval {
		t.Errorf("scrape_interval = %v, federationInterval = %v — the export cool-down (%v) derives "+
			"from the constant, so the two must move together", parsed, federationInterval, exportCooldown)
	}
}
