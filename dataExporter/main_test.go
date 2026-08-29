package main

import "testing"

// two-node fixture: one counter, one rate, one trend, one gauge.
func summaryFixture() map[string]k6Summary {
	return map[string]k6Summary{
		"node-1": {Metrics: map[string]k6SummaryMetric{
			"http_reqs":         {Type: "counter", Values: map[string]float64{"count": 100, "rate": 10}},
			"http_req_failed":   {Type: "rate", Values: map[string]float64{"rate": 0.1, "passes": 10, "fails": 90}},
			"http_req_duration": {Type: "trend", Values: map[string]float64{"avg": 50, "min": 10, "max": 200, "p(95)": 120}},
			"vus":               {Type: "gauge", Values: map[string]float64{"value": 5, "min": 1, "max": 5}},
		}},
		"node-2": {Metrics: map[string]k6SummaryMetric{
			"http_reqs":         {Type: "counter", Values: map[string]float64{"count": 300, "rate": 30}},
			"http_req_failed":   {Type: "rate", Values: map[string]float64{"rate": 0.2, "passes": 60, "fails": 240}},
			"http_req_duration": {Type: "trend", Values: map[string]float64{"avg": 70, "min": 5, "max": 400, "p(95)": 300}},
			"vus":               {Type: "gauge", Values: map[string]float64{"value": 5, "min": 2, "max": 6}},
		}},
	}
}

// rowsToMap indexes rows by "<ID_Nodo>/<MetricName>" → Valore.
func rowsToMap(rows [][]string) map[string]string {
	out := map[string]string{}
	for _, r := range rows {
		out[r[1]+"/"+r[3]] = r[6]
	}
	return out
}

func TestNormalizeStat(t *testing.T) {
	cases := map[string]string{"p(95)": "p95", "p(99.9)": "p99.9", "avg": "avg"}
	for in, want := range cases {
		if got := normalizeStat(in); got != want {
			t.Errorf("normalizeStat(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFlattenSummaryRows(t *testing.T) {
	rows := flattenSummaryRows("node-1", "2026-08-28T10:00:00Z", summaryFixture()["node-1"])
	m := rowsToMap(rows)
	if m["node-1/http_req_duration_p95"] != "120" {
		t.Errorf("p95 row = %q, want 120", m["node-1/http_req_duration_p95"])
	}
	if m["node-1/http_reqs_count"] != "100" {
		t.Errorf("count row = %q, want 100", m["node-1/http_reqs_count"])
	}
	// fixed columns
	r := rows[0]
	if r[0] != "2026-08-28T10:00:00Z" || r[2] != "k6" || r[4] != "handleSummary" {
		t.Errorf("row layout wrong: %v", r)
	}
}

func TestAggregateSummaryRows(t *testing.T) {
	rows := aggregateSummaryRows("2026-08-28T10:00:00Z", summaryFixture())
	m := rowsToMap(rows)

	// counter: sum of counts, per-second rate omitted
	if m["__all__/http_reqs_count"] != "400" {
		t.Errorf("http_reqs_count = %q, want 400", m["__all__/http_reqs_count"])
	}
	if _, ok := m["__all__/http_reqs_rate"]; ok {
		t.Error("counter rate must be omitted from __all__")
	}
	// rate: recomputed from summed passes/fails = 70/400
	if m["__all__/http_req_failed_rate"] != "0.175" {
		t.Errorf("http_req_failed_rate = %q, want 0.175", m["__all__/http_req_failed_rate"])
	}
	// trend: min-of-min / max-of-max, percentiles and avg omitted
	if m["__all__/http_req_duration_min"] != "5" || m["__all__/http_req_duration_max"] != "400" {
		t.Errorf("trend min/max wrong: %v / %v",
			m["__all__/http_req_duration_min"], m["__all__/http_req_duration_max"])
	}
	for _, banned := range []string{"http_req_duration_p95", "http_req_duration_avg", "vus_value"} {
		if _, ok := m["__all__/"+banned]; ok {
			t.Errorf("%s must be omitted from __all__", banned)
		}
	}
	// gauge min/max
	if m["__all__/vus_min"] != "1" || m["__all__/vus_max"] != "6" {
		t.Errorf("gauge min/max wrong: %v / %v", m["__all__/vus_min"], m["__all__/vus_max"])
	}
}

func TestAggregateZeroDenominator(t *testing.T) {
	byNode := map[string]k6Summary{
		"node-1": {Metrics: map[string]k6SummaryMetric{
			"checks": {Type: "rate", Values: map[string]float64{"rate": 0, "passes": 0, "fails": 0}},
		}},
	}
	m := rowsToMap(aggregateSummaryRows("2026-08-28T10:00:00Z", byNode))
	if _, ok := m["__all__/checks_rate"]; ok {
		t.Error("rate with zero denominator must be omitted")
	}
	if m["__all__/checks_passes"] != "0" {
		t.Errorf("passes = %q, want 0", m["__all__/checks_passes"])
	}
}
