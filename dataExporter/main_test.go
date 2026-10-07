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

func TestNormalizeStat(t *testing.T) {
	cases := map[string]string{"p(95)": "p95", "p(99.9)": "p99.9", "avg": "avg"}
	for in, want := range cases {
		if got := normalizeStat(in); got != want {
			t.Errorf("normalizeStat(%q) = %q, want %q", in, got, want)
		}
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

func TestRunStoresTheThreeCSVs(t *testing.T) {
	sink := &fakeSink{}
	rows, err := run(context.Background(), testConfig(), &cannedQuerier{matrix: oneSeries()}, sink)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}

	// The status CSV is stored first: it is the one artifact that has to
	// survive an export that fails on the next line.
	if got := sink.labels(); len(got) < 3 || got[0] != "QUERY STATUS" {
		t.Fatalf("stored %v, want QUERY STATUS first", got)
	}
	for _, want := range []string{"QUERY STATUS", "CSV", "K6 CSV"} {
		if _, ok := sink.bodies[want]; !ok {
			t.Errorf("no %q artifact; got %v", want, sink.labels())
		}
	}

	keys := map[string]string{}
	for _, a := range sink.got {
		keys[a.Label] = a.Key
	}
	want := map[string]string{
		"QUERY STATUS": "metrics/lt-sample/query-status-20260909T120000Z.csv",
		"CSV":          "metrics/lt-sample/20260909T120000Z.csv",
		"K6 CSV":       "k6/lt-sample/summary-20260909T120000Z.csv",
	}
	for label, wantKey := range want {
		if keys[label] != wantKey {
			t.Errorf("%s key = %q, want %q", label, keys[label], wantKey)
		}
	}

	records := parseCSV(t, []byte(sink.bodies["CSV"]))
	if got := column(t, records, 1, "node_id"); got != "w1" {
		t.Errorf("node_id = %q, want w1", got)
	}
	if got := column(t, records, 1, "query_name"); got != "cpu" {
		t.Errorf("query_name = %q, want cpu", got)
	}
}

// Every entry of spec.metricsExport.metrics gets a row in the status CSV,
// including one the exporter never ran.
func TestRunReportsASkippedQueryInTheStatusCSV(t *testing.T) {
	cfg := testConfig()
	cfg.Metrics = append(cfg.Metrics, MetricEntry{Type: "raw", MetricName: "nothing", Query: ""})
	sink := &fakeSink{}
	if _, err := run(context.Background(), cfg, &cannedQuerier{matrix: oneSeries()}, sink); err != nil {
		t.Fatalf("run: %v", err)
	}
	records := parseCSV(t, []byte(sink.bodies["QUERY STATUS"]))
	if len(records) != 3 {
		t.Fatalf("want header + one row per configured metric, got %d", len(records))
	}
	if got := column(t, records, 2, "error"); got == "" {
		t.Error("an entry with an empty query must say why it produced nothing")
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
	// The status CSV is stored anyway -- it is what names the query that
	// failed, and the Job's logs leave with its pod. The metrics CSV is not:
	// an empty file beside a Failed LoadTest reads like a successful run that
	// measured nothing.
	if got := sink.labels(); len(got) != 1 || got[0] != "QUERY STATUS" {
		t.Errorf("stored %v, want only the query status CSV", got)
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
	if got := sink.labels(); len(got) != 3 {
		t.Errorf("stored %v, want the three CSVs and no k6 log", got)
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

// The key builders, whose layout the UI turns into browsable links. Every
// artifact of one run shares the stamp, so a retried exporter Job overwrites
// its own objects instead of leaving near-identical copies minutes apart.
func TestConfigStampIsTheWindowEnd(t *testing.T) {
	if got := testConfig().Stamp(); got != "20260909T120000Z" {
		t.Errorf("Stamp() = %q, want END_TIME in compact UTC", got)
	}
}

func TestObjectKeyLayout(t *testing.T) {
	const stamp = "20260909T120000Z"
	cases := []struct{ got, want string }{
		{objectKeyFor("lt-sample", stamp), "metrics/lt-sample/20260909T120000Z.csv"},
		{queryStatusKeyFor("lt-sample", stamp), "metrics/lt-sample/query-status-20260909T120000Z.csv"},
		{k6SummaryCSVKeyFor("lt-sample", stamp), "k6/lt-sample/summary-20260909T120000Z.csv"},
		{k6ObjectKeyFor("lt-sample", "gen-a", stamp), "k6/lt-sample/gen-a-20260909T120000Z.log"},
		{k6SummaryKeyFor("lt-sample", "gen-a", stamp), "k6/lt-sample/gen-a-summary-20260909T120000Z.json"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("key = %q, want %q", c.got, c.want)
		}
	}
}

// --- The metrics CSV: the wide Prometheus layout --------------------------
//
// Every label of every series becomes a column: the old layout promoted
// node_id and crushed the rest into one cell as {__name__="x", job="y"},
// which needs a regex before pandas can group by anything.

// parseCSV is the check every CSV test starts with: valid CSV, header first.
func parseCSV(t *testing.T, body []byte) [][]string {
	t.Helper()
	records, err := csv.NewReader(bytes.NewReader(body)).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v\n%s", err, body)
	}
	if len(records) == 0 {
		t.Fatal("CSV has no header")
	}
	return records
}

// column returns one row's value for a named column.
func column(t *testing.T, records [][]string, row int, name string) string {
	t.Helper()
	for i, h := range records[0] {
		if h == name {
			return records[row][i]
		}
	}
	t.Fatalf("no column %q in header %v", name, records[0])
	return ""
}

func seriesWith(labels model.Metric, values ...float64) *model.SampleStream {
	s := &model.SampleStream{Metric: labels}
	for i, v := range values {
		s.Values = append(s.Values, model.SamplePair{
			Timestamp: model.Time(1757419200000 + int64(i)*15000),
			Value:     model.SampleValue(v),
		})
	}
	return s
}

func TestMetricsCSVPutsEveryLabelInItsOwnColumn(t *testing.T) {
	results := []queryResult{{
		entry:  MetricEntry{Type: "raw", MetricName: "cpu", Query: "node_cpu_seconds_total", Comment: "cpu time"},
		matrix: model.Matrix{seriesWith(model.Metric{"__name__": "node_cpu_seconds_total", "node_id": "w1", "job": "dfaas"}, 1.5, 2.5)},
	}}
	body, rows := buildMetricsCSV("lt-sample", results)
	if rows != 2 {
		t.Errorf("rows = %d, want one per sample", rows)
	}
	records := parseCSV(t, body)

	want := []string{
		"timestamp", "node_id", "value", "loadtest",
		"query_name", "query_type", "query_expr", "query_comment",
		"__name__", "job",
	}
	if !reflect.DeepEqual(records[0], want) {
		t.Fatalf("header = %v, want %v", records[0], want)
	}
	if got := column(t, records, 1, "node_id"); got != "w1" {
		t.Errorf("node_id = %q, want w1", got)
	}
	if got := column(t, records, 1, "job"); got != "dfaas" {
		t.Errorf("job = %q, want dfaas", got)
	}
	if got := column(t, records, 1, "value"); got != "1.5" {
		t.Errorf("value = %q, want 1.5", got)
	}
	if got := column(t, records, 1, "loadtest"); got != "lt-sample" {
		t.Errorf("loadtest = %q, want lt-sample", got)
	}
	if got := column(t, records, 1, "query_comment"); got != "cpu time" {
		t.Errorf("query_comment = %q, want the entry's comment", got)
	}
	if got := column(t, records, 1, "timestamp"); got != "2025-09-09T12:00:00Z" {
		t.Errorf("timestamp = %q, want RFC3339 UTC", got)
	}
}

// Two queries with different label sets share one header: the union, with an
// empty cell where a series does not carry that label.
func TestMetricsCSVUnionsLabelsAcrossQueries(t *testing.T) {
	results := []queryResult{
		{
			entry:  MetricEntry{Type: "raw", MetricName: "cpu", Query: "cpu"},
			matrix: model.Matrix{seriesWith(model.Metric{"node_id": "w1", "job": "dfaas"}, 1)},
		},
		{
			entry:  MetricEntry{Type: "custom-promql", MetricName: "invocations", Query: "rate(x[1m])"},
			matrix: model.Matrix{seriesWith(model.Metric{"node_id": "w2", "function_name": "figlet"}, 2)},
		},
	}
	records := parseCSV(t, mustBody(buildMetricsCSV("lt-sample", results)))
	if len(records) != 3 {
		t.Fatalf("want header + 2 rows, got %d", len(records))
	}
	for _, name := range []string{"job", "function_name"} {
		found := false
		for _, h := range records[0] {
			if h == name {
				found = true
			}
		}
		if !found {
			t.Errorf("header %v is missing %q", records[0], name)
		}
	}
	if got := column(t, records, 1, "function_name"); got != "" {
		t.Errorf("a series without function_name must leave the cell empty, got %q", got)
	}
	if got := column(t, records, 2, "job"); got != "" {
		t.Errorf("a series without job must leave the cell empty, got %q", got)
	}
	if got := column(t, records, 2, "function_name"); got != "figlet" {
		t.Errorf("function_name = %q, want figlet", got)
	}
}

// A label named like a fixed column would produce two columns with the same
// name, which read_csv silently renames to value.1 -- three wrong plots later
// you find out.
func TestMetricsCSVSuffixesALabelThatCollidesWithAFixedColumn(t *testing.T) {
	results := []queryResult{{
		entry:  MetricEntry{Type: "raw", MetricName: "odd", Query: "odd"},
		matrix: model.Matrix{seriesWith(model.Metric{"node_id": "w1", "value": "42", "loadtest": "other"}, 7)},
	}}
	records := parseCSV(t, mustBody(buildMetricsCSV("lt-sample", results)))
	seen := map[string]int{}
	for _, h := range records[0] {
		seen[h]++
	}
	for h, n := range seen {
		if n > 1 {
			t.Errorf("column %q appears %d times in %v", h, n, records[0])
		}
	}
	if got := column(t, records, 1, "value"); got != "7" {
		t.Errorf("value = %q, want the sample value", got)
	}
	if got := column(t, records, 1, "value_label"); got != "42" {
		t.Errorf("value_label = %q, want the label's value", got)
	}
	if got := column(t, records, 1, "loadtest_label"); got != "other" {
		t.Errorf("loadtest_label = %q, want the label's value", got)
	}
}

// No node_id leaves the cell empty (NaN in pandas), which is filterable --
// the old "unknown" string was not.
func TestMetricsCSVLeavesAMissingNodeIDEmpty(t *testing.T) {
	results := []queryResult{{
		entry:  MetricEntry{Type: "raw", MetricName: "cpu", Query: "cpu"},
		matrix: model.Matrix{seriesWith(model.Metric{"job": "dfaas"}, 1)},
	}}
	records := parseCSV(t, mustBody(buildMetricsCSV("lt-sample", results)))
	if got := column(t, records, 1, "node_id"); got != "" {
		t.Errorf("node_id = %q, want an empty cell", got)
	}
}

// A query that errored contributes no rows but must not break the header.
func TestMetricsCSVIgnoresFailedQueries(t *testing.T) {
	results := []queryResult{
		{entry: MetricEntry{Type: "raw", MetricName: "gone", Query: "gone"}, err: errors.New("unreachable")},
		{
			entry:  MetricEntry{Type: "raw", MetricName: "cpu", Query: "cpu"},
			matrix: model.Matrix{seriesWith(model.Metric{"node_id": "w1"}, 1)},
		},
	}
	body, rows := buildMetricsCSV("lt-sample", results)
	if rows != 1 {
		t.Errorf("rows = %d, want only the successful query's sample", rows)
	}
	records := parseCSV(t, body)
	if len(records) != 2 {
		t.Fatalf("want header + 1 row, got %d", len(records))
	}
}

func mustBody(body []byte, _ int) []byte { return body }

// --- The query-status CSV: one row per query, whatever happened ------------
//
// A query with a typo returns zero series and nothing else says so: the metric
// is simply absent from every plot. This file is where "did all my queries
// work?" gets an answer that survives the Job's pod.

func TestQueryStatusCSVReportsEveryQuery(t *testing.T) {
	results := []queryResult{
		{
			entry:    MetricEntry{Type: "raw", MetricName: "cpu", Query: "node_cpu_seconds_total"},
			matrix:   model.Matrix{seriesWith(model.Metric{"node_id": "w1"}, 1, 2, 3)},
			warnings: []string{"series limit hit"},
		},
		{
			entry: MetricEntry{Type: "raw", MetricName: "gone", Query: "typo_metric"},
		},
		{
			entry: MetricEntry{Type: "custom-promql", MetricName: "broken", Query: "rate("},
			err:   errors.New("parse error at char 5"),
		},
	}
	records := parseCSV(t, buildQueryStatusCSV("lt-sample", results))

	want := []string{"loadtest", "query_name", "query_type", "query_expr", "series", "samples", "warnings", "error"}
	if !reflect.DeepEqual(records[0], want) {
		t.Fatalf("header = %v, want %v", records[0], want)
	}
	if len(records) != 4 {
		t.Fatalf("want header + one row per query, got %d records", len(records))
	}

	if got := column(t, records, 1, "series"); got != "1" {
		t.Errorf("series = %q, want 1", got)
	}
	if got := column(t, records, 1, "samples"); got != "3" {
		t.Errorf("samples = %q, want 3", got)
	}
	if got := column(t, records, 1, "warnings"); got != "series limit hit" {
		t.Errorf("warnings = %q, want the Prometheus warning", got)
	}
	if got := column(t, records, 1, "error"); got != "" {
		t.Errorf("error = %q, want empty on a query that worked", got)
	}
	// The silent one: no error, no data.
	if got := column(t, records, 2, "series"); got != "0" {
		t.Errorf("a query that matched nothing must report series=0, got %q", got)
	}
	if got := column(t, records, 3, "error"); got != "parse error at char 5" {
		t.Errorf("error = %q, want the query's error", got)
	}
	if got := column(t, records, 3, "query_expr"); got != "rate(" {
		t.Errorf("query_expr = %q, want the PromQL as sent", got)
	}
}

// --- The k6 summary CSV: the k6 summaries, long ----------------------------
//
// One row per (node, metric, stat), with the k6 type the old layout discarded
// and without the END_TIME the old rows wore as if it were an instant of
// measurement. The whole-window aggregates have no timestamp; they have a
// window, and it is in the columns.

func k6TestConfig() Config {
	cfg := testConfig()
	cfg.EnvName = "bari"
	cfg.Summaries = []summarySource{
		{NodeID: "node-1", URL: "http://filer/node-1"},
		{NodeID: "node-2", URL: "http://filer/node-2"},
	}
	return cfg
}

// k6Cell indexes "<node>/<metric>/<stat>" → the named column.
func k6Cell(t *testing.T, records [][]string, key, col string) string {
	t.Helper()
	at := map[string]int{}
	for i, h := range records[0] {
		at[h] = i
	}
	for i, r := range records[1:] {
		if r[at["node_id"]]+"/"+r[at["metric"]]+"/"+r[at["stat"]] == key {
			return column(t, records, i+1, col)
		}
	}
	t.Fatalf("no row %q in %v", key, records)
	return ""
}

func TestK6CSVIsLongAndKeepsTheK6Type(t *testing.T) {
	records := parseCSV(t, buildK6CSV(k6TestConfig(), summaryFixture(), nil))

	want := []string{
		"loadtest", "environment", "window_start", "window_end",
		"node_id", "metric", "metric_type", "stat", "value", "note",
	}
	if !reflect.DeepEqual(records[0], want) {
		t.Fatalf("header = %v, want %v", records[0], want)
	}
	if got := k6Cell(t, records, "node-1/http_req_duration/p95", "value"); got != "120" {
		t.Errorf("p95 = %q, want 120", got)
	}
	if got := k6Cell(t, records, "node-1/http_req_duration/p95", "metric_type"); got != "trend" {
		t.Errorf("metric_type = %q, want trend -- the k6 type the CSV used to drop", got)
	}
	if got := k6Cell(t, records, "node-2/http_reqs/count", "value"); got != "300" {
		t.Errorf("count = %q, want 300", got)
	}
	if got := k6Cell(t, records, "node-1/http_reqs/count", "window_end"); got != "2026-09-09T12:00:00Z" {
		t.Errorf("window_end = %q, want the end of the test window", got)
	}
	if got := k6Cell(t, records, "node-1/http_reqs/count", "window_start"); got != "2026-09-09T11:59:00Z" {
		t.Errorf("window_start = %q, want the start of the test window", got)
	}
	if got := k6Cell(t, records, "node-1/http_reqs/count", "environment"); got != "bari" {
		t.Errorf("environment = %q, want the Environment name", got)
	}
	// The cross-node aggregate is gone: pandas does it in one groupby, and
	// "__all__" inside node_id double-counted every ungrouped sum.
	for _, r := range records {
		for _, cell := range r {
			if cell == "__all__" {
				t.Errorf("no row may carry __all__: %v", r)
			}
		}
	}
}

// Every expected Generator gets a meta row, so a node that never answered is
// present with 0 instead of simply absent.
func TestK6CSVMarksEveryExpectedNode(t *testing.T) {
	byNode := map[string]k6Summary{"node-1": summaryFixture()["node-1"]}
	notes := map[string]string{"node-2": "connection refused"}
	records := parseCSV(t, buildK6CSV(k6TestConfig(), byNode, notes))

	if got := k6Cell(t, records, "node-1/dfaas_summary_fetched/value", "value"); got != "1" {
		t.Errorf("a node that answered must be marked 1, got %q", got)
	}
	if got := k6Cell(t, records, "node-1/dfaas_summary_fetched/value", "note"); got != "" {
		t.Errorf("note = %q, want empty for a node that answered", got)
	}
	if got := k6Cell(t, records, "node-2/dfaas_summary_fetched/value", "value"); got != "0" {
		t.Errorf("a node that never answered must be marked 0, got %q", got)
	}
	if got := k6Cell(t, records, "node-2/dfaas_summary_fetched/value", "note"); got != "connection refused" {
		t.Errorf("note = %q, want the failure reason", got)
	}
	if got := k6Cell(t, records, "node-1/dfaas_summary_fetched/value", "metric_type"); got != "meta" {
		t.Errorf("metric_type = %q, want meta so the row filters out of any analysis", got)
	}
	// The data rows are the answering node's only.
	for _, r := range records[1:] {
		if r[4] == "node-2" && r[6] != "meta" {
			t.Errorf("node-2 produced no summary, so it can have no data row: %v", r)
		}
	}
}

func TestK6CSVWithNoGeneratorsIsHeaderOnly(t *testing.T) {
	cfg := testConfig()
	cfg.Summaries = nil
	records := parseCSV(t, buildK6CSV(cfg, nil, nil))
	if len(records) != 1 {
		t.Errorf("want header only, got %d records: %v", len(records), records)
	}
}

// Suffixing a colliding label can collide again: a series carrying both
// "value" (renamed to value_label) and a label literally named "value_label"
// used to write both into one column, and Go's randomized map iteration picked
// the winner per row -- the same export, run twice, disagreed. A synthesized
// label from label_replace() in a custom-promql entry is enough to hit it.
func TestMetricsCSVKeepsTwiceCollidingLabelsApart(t *testing.T) {
	results := []queryResult{{
		entry: MetricEntry{Type: "custom-promql", MetricName: "odd", Query: "label_replace(x, ...)"},
		matrix: model.Matrix{seriesWith(model.Metric{
			"node_id": "w1", "value": "labelA", "value_label": "labelB",
		}, 7)},
	}}

	first := parseCSV(t, mustBody(buildMetricsCSV("lt-sample", results)))
	if got := column(t, first, 1, "value"); got != "7" {
		t.Errorf("value = %q, want the sample value", got)
	}
	seen := map[string]int{}
	for _, h := range first[0] {
		seen[h]++
	}
	for h, n := range seen {
		if n > 1 {
			t.Errorf("column %q appears %d times in %v", h, n, first[0])
		}
	}
	// Both labels must survive, in different columns, with their own values.
	values := map[string]bool{}
	for i, h := range first[0] {
		if strings.HasPrefix(h, "value_label") {
			values[first[1][i]] = true
		}
	}
	for _, want := range []string{"labelA", "labelB"} {
		if !values[want] {
			t.Errorf("label value %q was dropped; header %v, row %v", want, first[0], first[1])
		}
	}

	// And the mapping is stable: same input, same bytes, every time.
	base := mustBody(buildMetricsCSV("lt-sample", results))
	for i := 0; i < 20; i++ {
		if again := mustBody(buildMetricsCSV("lt-sample", results)); !bytes.Equal(base, again) {
			t.Fatalf("buildMetricsCSV is not deterministic:\n%s\nvs\n%s", base, again)
		}
	}
}
