package controller

import (
	"testing"

	"dfaas-operator/internal/controller/monitoring"
)

// The cool-down is a fixed minute on purpose, while the
// federation interval is read from the embedded Prometheus values. The minute
// has to cover what the export needs from federation: one interval for the
// last pull inside the k6 window, one for the pull that lands after EndTime
// (see exportTailWindow). A scrape_interval above 30s fails here, before the
// CSV starts losing its tail.
func TestExportCooldownCoversTwoFederationIntervals(t *testing.T) {
	if need := 2 * monitoring.FederationInterval; exportCooldown < need {
		t.Errorf("exportCooldown = %v, below two federation intervals (%v): the CSV loses its tail",
			exportCooldown, need)
	}
}
