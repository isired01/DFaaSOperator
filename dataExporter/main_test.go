package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
)

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

// --- run(), end to end ----------------------------------------------------
//
// run() used to be main(): 194 lines inlining six env reads, three time
// parses, a Prometheus client, MkdirAll + Create, the query loop, the "0 rows
// → log.Fatalf" rule, the summary fetch, the S3-vs-stdout branch and two more
// shipping paths. main_test.go could only reach four pure helpers; the CSV
// layout, the empty-export rule and the destination choice were unreachable.

// fakeSink records every artifact it is handed, and can be told to fail one.
type fakeSink struct {
	got     []Artifact
	bodies  map[string]string
	failKey string
}

func (f *fakeSink) Put(_ context.Context, a Artifact) error {
	body, err := io.ReadAll(a.Body)
	if err != nil {
		return err
	}
	if f.bodies == nil {
		f.bodies = map[string]string{}
	}
	f.got = append(f.got, a)
	f.bodies[a.Label] = string(body)
	if f.failKey != "" && strings.Contains(a.Key, f.failKey) {
		return errors.New("storage refused this artifact")
	}
	return nil
}

func (f *fakeSink) labels() []string {
	var out []string
	for _, a := range f.got {
		out = append(out, a.Label)
	}
	return out
}

// cannedQuerier answers from a fixed matrix, or with an error.
type cannedQuerier struct {
	matrix   model.Matrix
	err      error
	warnings []string
	queries  []string
}

func (q *cannedQuerier) Range(_ context.Context, query string, _ PromRange) (model.Matrix, []string, error) {
	q.queries = append(q.queries, query)
	return q.matrix, q.warnings, q.err
}

func testConfig() Config {
	end := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	return Config{
		ExpName:      "exp",
		LoadTestName: "lt-sample",
		Range:        PromRange{Start: end.Add(-time.Minute), End: end, Step: 15 * time.Second},
		Metrics: []MetricEntry{
			{Type: "raw", MetricName: "cpu", Query: "node_cpu_seconds_total", Comment: "cpu time"},
		},
	}
}

func oneSeries() model.Matrix {
	return model.Matrix{{
		Metric: model.Metric{"node_id": "w1", "job": "dfaas"},
		Values: []model.SamplePair{
			{Timestamp: model.Time(1757419200000), Value: 1.5},
			{Timestamp: model.Time(1757419215000), Value: 2.5},
		},
	}}
}

func TestRunWritesTheEightColumnCSV(t *testing.T) {
	sink := &fakeSink{}
	rows, err := run(context.Background(), testConfig(), &cannedQuerier{matrix: oneSeries()}, sink)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}

	body, ok := sink.bodies["CSV"]
	if !ok {
		t.Fatalf("no CSV artifact stored; got %v", sink.labels())
	}
	records, err := csv.NewReader(strings.NewReader(body)).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	if len(records) != 3 { // header + two samples
		t.Fatalf("want header + 2 rows, got %d records", len(records))
	}
	wantHeader := []string{
		"Timestamp", "ID_Nodo", "Type", "MetricName", "Query", "Comment", "Valore", "Labels",
	}
	if !reflect.DeepEqual(records[0], wantHeader) {
		t.Errorf("header = %v, want %v", records[0], wantHeader)
	}
	if got := len(records[1]); got != 8 {
		t.Errorf("row has %d columns, want 8", got)
	}
	// The columns the operator and the thesis read back by position.
	if records[1][1] != "w1" {
		t.Errorf("ID_Nodo = %q, want the series' node_id", records[1][1])
	}
	if records[1][2] != "raw" || records[1][3] != "cpu" {
		t.Errorf("Type/MetricName = %q/%q, want raw/cpu", records[1][2], records[1][3])
	}
	if records[1][5] != "cpu time" {
		t.Errorf("Comment = %q, want the entry's comment", records[1][5])
	}
	if records[1][6] != "1.5" {
		t.Errorf("Valore = %q, want 1.5", records[1][6])
	}
	if !strings.Contains(records[1][7], "node_id") {
		t.Errorf("Labels = %q, want the full label set", records[1][7])
	}
	// The CSV object key, which the UI turns into a results link.
	if key := sink.got[0].Key; !strings.HasPrefix(key, "metrics/lt-sample/") || !strings.HasSuffix(key, ".csv") {
		t.Errorf("key = %q, want metrics/<loadtest>/<ts>.csv", key)
	}
}

// A series with no node_id label still has to land somewhere identifiable.
func TestRunLabelsUnknownNode(t *testing.T) {
	sink := &fakeSink{}
	matrix := model.Matrix{{
		Metric: model.Metric{"job": "dfaas"},
		Values: []model.SamplePair{{Timestamp: model.Time(1757419200000), Value: 1}},
	}}
	if _, err := run(context.Background(), testConfig(), &cannedQuerier{matrix: matrix}, sink); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(sink.bodies["CSV"], ",unknown,") {
		t.Errorf("a series with no node_id must be recorded as \"unknown\":\n%s", sink.bodies["CSV"])
	}
}

// The rule that keeps a LoadTest from reporting Completed with no data. It was
// a log.Fatalf, so testing it meant spawning a subprocess.
func TestRunRefusesAnEmptyExport(t *testing.T) {
	sink := &fakeSink{}
	rows, err := run(context.Background(), testConfig(), &cannedQuerier{matrix: model.Matrix{}}, sink)
	if err == nil {
		t.Fatal("want an error when the export produced no data points")
	}
	if rows != 0 {
		t.Errorf("rows = %d, want 0", rows)
	}
	if !strings.Contains(err.Error(), "0 data points") {
		t.Errorf("error should say what went wrong: %v", err)
	}
	// And nothing was stored: an empty CSV must not reach the object store.
	if len(sink.got) != 0 {
		t.Errorf("stored %v despite an empty export", sink.labels())
	}
}

// A query that errors is counted, not fatal -- unless it leaves zero rows.
func TestRunSurvivesAQueryError(t *testing.T) {
	cfg := testConfig()
	cfg.Metrics = append(cfg.Metrics, MetricEntry{Type: "raw", MetricName: "mem", Query: "mem_bytes"})
	q := &cannedQuerier{err: errors.New("prometheus unreachable")}
	if _, err := run(context.Background(), cfg, q, &fakeSink{}); err == nil {
		t.Fatal("every query failing leaves zero rows, which must be an error")
	}
	if len(q.queries) != 2 {
		t.Errorf("ran %d queries, want both attempted", len(q.queries))
	}
}

// An entry with an empty query is skipped without being counted as attempted,
// so it cannot on its own trigger the empty-export failure.
func TestRunSkipsEmptyQueries(t *testing.T) {
	cfg := testConfig()
	cfg.Metrics = []MetricEntry{{Type: "raw", MetricName: "nothing", Query: ""}}
	q := &cannedQuerier{matrix: model.Matrix{}}
	rows, err := run(context.Background(), cfg, q, &fakeSink{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rows != 0 {
		t.Errorf("rows = %d, want 0", rows)
	}
	if len(q.queries) != 0 {
		t.Errorf("ran %v, want no query at all", q.queries)
	}
}

// One Generator's log failing to store must warn and leave the run successful:
// the k6 run already happened, and the metrics CSV is already stored.
func TestRunKeepsGoingWhenOneGeneratorLogFails(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"gen-a.log", "gen-b.log"} {
		if err := os.WriteFile(dir+"/"+name, []byte("k6 summary for "+name+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A directory and a non-.log file, both of which must be ignored.
	if err := os.Mkdir(dir+"/nested", 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/notes.txt", []byte("ignore me"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.K6LogDir = dir
	sink := &fakeSink{failKey: "gen-a"}

	rows, err := run(context.Background(), cfg, &cannedQuerier{matrix: oneSeries()}, sink)
	if err != nil {
		t.Fatalf("one failed log upload must not fail the run: %v", err)
	}
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	labels := sink.labels()
	for _, want := range []string{"CSV", "K6 gen-a", "K6 gen-b"} {
		found := false
		for _, got := range labels {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%q was never attempted; stored %v", want, labels)
		}
	}
	for _, unwanted := range []string{"K6 notes", "K6 nested"} {
		for _, got := range labels {
			if strings.HasPrefix(got, unwanted) {
				t.Errorf("stored %q, want only .log files", got)
			}
		}
	}
}

// A missing K6_LOG_DIR is normal (no Generators, or logs never captured).
func TestRunToleratesAMissingLogDir(t *testing.T) {
	cfg := testConfig()
	cfg.K6LogDir = "/definitely/not/here"
	sink := &fakeSink{}
	if _, err := run(context.Background(), cfg, &cannedQuerier{matrix: oneSeries()}, sink); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := sink.labels(); len(got) != 1 || got[0] != "CSV" {
		t.Errorf("stored %v, want only the CSV", got)
	}
}

// --- the destinations -----------------------------------------------------

// With no S3 config the CSV goes to stdout between the markers the README
// documents as the recovery path.
func TestStdoutSinkKeepsTheDocumentedMarkers(t *testing.T) {
	var buf bytes.Buffer
	sink := StdoutSink{Out: &buf}
	if err := sink.Put(context.Background(), Artifact{
		Key: "metrics/lt/x.csv", Label: "CSV", ContentType: "text/csv",
		Body: strings.NewReader("a,b\n1,2\n"),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got := buf.String()
	if !strings.HasPrefix(got, "----- BEGIN CSV -----\n") {
		t.Errorf("missing the BEGIN marker:\n%s", got)
	}
	if !strings.HasSuffix(got, "----- END CSV -----\n") {
		t.Errorf("missing the END marker:\n%s", got)
	}
	if !strings.Contains(got, "a,b\n1,2\n") {
		t.Errorf("body not printed verbatim:\n%s", got)
	}
}

// A k6 log may not end in a newline; the END marker must still be on its own
// line or the log is unrecoverable.
func TestStdoutSinkTerminatesABodyWithoutTrailingNewline(t *testing.T) {
	var buf bytes.Buffer
	sink := StdoutSink{Out: &buf}
	if err := sink.Put(context.Background(), Artifact{
		Label: "K6 gen-a", Body: strings.NewReader("no trailing newline"),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !strings.Contains(buf.String(), "no trailing newline\n----- END K6 gen-a -----\n") {
		t.Errorf("END marker is not on its own line:\n%s", buf.String())
	}
}

func TestNewSinkPicksStdoutWhenS3IsUnset(t *testing.T) {
	sink, err := newSink(context.Background(), S3Config{})
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	if _, ok := sink.(StdoutSink); !ok {
		t.Errorf("got %T, want StdoutSink when no bucket prefix is configured", sink)
	}
}

// --- bucketNameFor --------------------------------------------------------
//
// Referenced in zero test files despite four documented edge cases, in a repo
// that got bitten by exactly this bug class (the 63-byte Job name) and now
// pins that one. This table is duplicated on the UI side, where the function
// is copied verbatim so uploads land in the bucket the exporter reads.

func TestBucketNameFor(t *testing.T) {
	cases := []struct {
		name            string
		envName, envUID string
		want            string
	}{
		{
			name:    "the ordinary case: name plus six hex of the UID",
			envName: "bari", envUID: "abcdef12-3456-7890-abcd-ef1234567890",
			want: "bari-abcdef",
		},
		{
			name:    "uppercase and separators are folded",
			envName: "Bari_Test.Env", envUID: "ABCDEF12-3456",
			want: "bari-test-env-abcdef",
		},
		{
			name:    "runs of dashes collapse and edges are trimmed",
			envName: "--bari__test--", envUID: "abcdef12",
			want: "bari-test-abcdef",
		},
		{
			// 56 + 1 + 6 = 63, S3's maximum.
			name:    "a long name is clamped so the total stays within 63",
			envName: strings.Repeat("a", 80), envUID: "abcdef12",
			want: strings.Repeat("a", 56) + "-abcdef",
		},
		{
			// Clamping must not leave a trailing dash before the suffix.
			name:    "clamping does not leave a dangling dash",
			envName: strings.Repeat("a", 56) + "-bbbb", envUID: "abcdef12",
			want: strings.Repeat("a", 56) + "-abcdef",
		},
		{
			name:    "no UID: the name is padded to S3's three-char minimum",
			envName: "ab", envUID: "",
			want: "abe",
		},
		{
			name:    "no UID and a long enough name is returned as is",
			envName: "bari", envUID: "",
			want: "bari",
		},
		{
			name:    "a name that sanitises to nothing falls back to a stable prefix",
			envName: "___", envUID: "abcdef12",
			want: "dfaas-abcdef",
		},
		{
			name:    "a short UID is used as-is rather than padded",
			envName: "bari", envUID: "ab",
			want: "bari-ab",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bucketNameFor(tc.envName, tc.envUID)
			if got != tc.want {
				t.Errorf("bucketNameFor(%q, %q) = %q, want %q", tc.envName, tc.envUID, got, tc.want)
			}
			// Whatever the input, the result must be a legal bucket name.
			if len(got) < 3 || len(got) > 63 {
				t.Errorf("%q is %d chars; S3 allows 3-63", got, len(got))
			}
			if strings.HasPrefix(got, "-") || strings.HasSuffix(got, "-") {
				t.Errorf("%q must not start or end with a dash", got)
			}
			for _, r := range got {
				if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
					t.Errorf("%q contains %q, which S3 rejects", got, r)
					break
				}
			}
		})
	}
}

// The three key builders, whose layout the UI turns into browsable links.
func TestObjectKeyLayout(t *testing.T) {
	if got := objectKeyFor("lt-sample"); !strings.HasPrefix(got, "metrics/lt-sample/") ||
		!strings.HasSuffix(got, ".csv") {
		t.Errorf("objectKeyFor = %q, want metrics/<loadtest>/<ts>.csv", got)
	}
	if got := k6ObjectKeyFor("lt-sample", "gen-a"); !strings.HasPrefix(got, "k6/lt-sample/gen-a-") ||
		!strings.HasSuffix(got, ".log") {
		t.Errorf("k6ObjectKeyFor = %q, want k6/<loadtest>/<nodeID>-<ts>.log", got)
	}
	if got := k6SummaryKeyFor("lt-sample", "gen-a"); !strings.HasPrefix(got, "k6/lt-sample/gen-a-summary-") ||
		!strings.HasSuffix(got, ".json") {
		t.Errorf("k6SummaryKeyFor = %q, want k6/<loadtest>/<nodeID>-summary-<ts>.json", got)
	}
}
