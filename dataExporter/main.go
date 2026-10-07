package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// MetricEntry mirrors the operator's MetricExportEntry. The operator
// resolves raw-type metricName defaults before serialization, so each
// entry here always has both metricName and query populated.
type MetricEntry struct {
	Type       string `json:"type"`
	MetricName string `json:"metricName"`
	Query      string `json:"query"`
	Comment    string `json:"comment,omitempty"`
}

// summarySource mirrors the operator's k6SummarySource: one filer URL per k6
// node, JSON-encoded into K6_SUMMARY_SOURCES. URLs are composed operator-side
// (they embed k6dispatch.Sanitize(nodeID)) — never rebuild them here.
type summarySource struct {
	NodeID string `json:"nodeId"`
	URL    string `json:"url"`
}

// k6SummaryMetric is one entry of handleSummary's data.metrics: type is one
// of counter|gauge|rate|trend, values holds the type-dependent stats
// (count/rate, value/min/max, rate/passes/fails, avg/min/med/max/p(90)/p(95)).
type k6SummaryMetric struct {
	Type   string             `json:"type"`
	Values map[string]float64 `json:"values"`
}

// k6Summary is the subset of the handleSummary JSON the exporter consumes.
type k6Summary struct {
	Metrics map[string]k6SummaryMetric `json:"metrics"`
}

// fetchSummaries GETs each node's summary JSON from the filer (10s timeout
// each). Per-node fetch or parse failures warn and skip — the Prometheus
// metrics are still valuable, so this path is never fatal. Returns the parsed
// summaries, the raw bytes (for the verbatim S3 upload) and, per node that
// produced nothing, the reason: it becomes the note on that node's roster row,
// which is the only place it survives the Job's pod.
func fetchSummaries(ctx context.Context, sources []summarySource) (map[string]k6Summary, map[string][]byte, map[string]string) {
	client := &http.Client{Timeout: 10 * time.Second}
	parsed := make(map[string]k6Summary)
	raw := make(map[string][]byte)
	notes := make(map[string]string)
	fail := func(nodeID, reason string) {
		fmt.Printf("k6 summary for node %s: %s\n", nodeID, reason)
		notes[nodeID] = reason
	}
	for _, src := range sources {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
		if err != nil {
			fail(src.NodeID, fmt.Sprintf("build request: %v", err))
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			fail(src.NodeID, fmt.Sprintf("fetch %s: %v", src.URL, err))
			continue
		}
		body, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fail(src.NodeID, fmt.Sprintf("%s returned %s", src.URL, resp.Status))
			continue
		}
		if rerr != nil {
			fail(src.NodeID, fmt.Sprintf("read body: %v", rerr))
			continue
		}
		var s k6Summary
		if err := json.Unmarshal(body, &s); err != nil {
			fail(src.NodeID, fmt.Sprintf("parse error: %v", err))
			continue
		}
		if len(s.Metrics) == 0 {
			// Shape drift guard (pre-v0.34 k6 lacks the values nesting).
			snippet := body
			if len(snippet) > 200 {
				snippet = snippet[:200]
			}
			fail(src.NodeID, fmt.Sprintf("no metrics parsed; first bytes: %s", snippet))
			continue
		}
		parsed[src.NodeID] = s
		raw[src.NodeID] = body
	}
	return parsed, raw, notes
}

// normalizeStat makes k6 stat names CSV/analysis friendly: p(95) → p95.
func normalizeStat(stat string) string {
	return strings.NewReplacer("(", "", ")", "").Replace(stat)
}

// queryResult is one metric entry's outcome, kept whole rather than written
// as it arrives: the CSV header is the union of every series' label set, so
// nothing can be written until the last query has answered.
type queryResult struct {
	entry    MetricEntry
	matrix   model.Matrix
	warnings []string
	err      error
}

// series and samples are what the query-status CSV reports for this entry.
func (r queryResult) series() int { return len(r.matrix) }

func (r queryResult) samples() int {
	n := 0
	for _, s := range r.matrix {
		n += len(s.Values)
	}
	return n
}

// metricsFixedColumns lead every row of the metrics CSV. node_id is a label
// like any other -- it is hoisted here only because it is the one every
// analysis groups by, and the alphabetical block would bury it.
var metricsFixedColumns = []string{
	"timestamp", "node_id", "value", "loadtest",
	"query_name", "query_type", "query_expr", "query_comment",
}

const nodeIDLabel = "node_id"

// labelColumns assigns every label of the run its own CSV column, once, for
// the whole export. A label named like a fixed column cannot keep its name --
// pandas renames a duplicate header to "value.1" without a word -- so it takes
// a "_label" suffix; and because that suffixed name can itself already belong
// to another label on the same series, the suffix is applied until the name is
// free. Deciding this per row instead would let Go's randomized map iteration
// pick which of two labels lands in the shared column, so the same export run
// twice disagreed with itself, silently.
//
// names must be sorted: the assignment is what the header is built from.
func labelColumns(names []string) map[string]string {
	claimed := make(map[string]bool, len(metricsFixedColumns))
	for _, fixed := range metricsFixedColumns {
		claimed[fixed] = true
	}
	// node_id is a label; the fixed column IS its column.
	delete(claimed, nodeIDLabel)

	cols := make(map[string]string, len(names))
	for _, name := range names {
		col := name
		for claimed[col] {
			col += "_label"
		}
		claimed[col] = true
		cols[name] = col
	}
	return cols
}

// buildMetricsCSV renders the metrics CSV (metrics/<lt>/<stamp>.csv): one row per (series, sample), one column per
// label seen anywhere in the run. Returns the bytes and the number of data
// rows, which is what decides whether the export counts as empty.
func buildMetricsCSV(loadtest string, results []queryResult) ([]byte, int) {
	seen := map[string]bool{}
	for _, r := range results {
		for _, s := range r.matrix {
			for name := range s.Metric {
				seen[string(name)] = true
			}
		}
	}
	cols := labelColumns(sortedKeys(seen))

	extra := make([]string, 0, len(cols))
	for name, col := range cols {
		if name != nodeIDLabel {
			extra = append(extra, col)
		}
	}
	sort.Strings(extra)

	header := append([]string{}, metricsFixedColumns...)
	header = append(header, extra...)
	at := make(map[string]int, len(header))
	for i, h := range header {
		at[h] = i
	}

	var buf bytes.Buffer
	// The writer is backed by a bytes.Buffer, whose Write never fails, so the
	// per-row errors below are unreachable rather than ignored. Point any of
	// these builders at a real io.Writer and they need an error path.
	w := csv.NewWriter(&buf)
	_ = w.Write(header)

	rows := 0
	for _, r := range results {
		for _, s := range r.matrix {
			for _, pair := range s.Values {
				row := make([]string, len(header))
				row[at["timestamp"]] = pair.Timestamp.Time().UTC().Format(time.RFC3339)
				row[at["value"]] = pair.Value.String()
				row[at["loadtest"]] = loadtest
				row[at["query_name"]] = r.entry.MetricName
				row[at["query_type"]] = r.entry.Type
				row[at["query_expr"]] = r.entry.Query
				row[at["query_comment"]] = r.entry.Comment
				for name, value := range s.Metric {
					row[at[cols[string(name)]]] = string(value)
				}
				_ = w.Write(row)
				rows++
			}
		}
	}
	w.Flush()
	return buf.Bytes(), rows
}

// buildQueryStatusCSV renders the query-status CSV: the outcome of every entry in
// spec.metricsExport.metrics, including the ones that worked. Without it a
// query that matches nothing is indistinguishable from a metric the workers
// never emitted, and both are invisible until a figure comes out empty.
func buildQueryStatusCSV(loadtest string, results []queryResult) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{
		"loadtest", "query_name", "query_type", "query_expr",
		"series", "samples", "warnings", "error",
	})
	for _, r := range results {
		errMsg := ""
		if r.err != nil {
			errMsg = r.err.Error()
		}
		_ = w.Write([]string{
			loadtest, r.entry.MetricName, r.entry.Type, r.entry.Query,
			strconv.Itoa(r.series()), strconv.Itoa(r.samples()),
			strings.Join(r.warnings, "; "), errMsg,
		})
	}
	w.Flush()
	return buf.Bytes()
}

// k6MetaMetric marks the roster rows of the k6 summary CSV: one per Generator the operator
// told us to expect, value 1 when its summary arrived and 0 when it did not.
// Without it a node that never answered is simply absent from the file, which
// reads exactly like a node that ran and measured nothing.
const k6MetaMetric = "dfaas_summary_fetched"

// buildK6CSV renders the k6 summary CSV. Metric and stat are separate columns
// (so df[df.stat=="p95"] works across every metric), the k6 type is kept, and
// the test window is carried in the window_start and window_end columns.
func buildK6CSV(cfg Config, byNode map[string]k6Summary, notes map[string]string) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{
		"loadtest", "environment", "window_start", "window_end",
		"node_id", "metric", "metric_type", "stat", "value", "note",
	})

	prefix := []string{cfg.LoadTestName, cfg.EnvName, cfg.StartTimeString(), cfg.EndTimeString()}
	row := func(nodeID, metric, metricType, stat, value, note string) []string {
		return append(append([]string{}, prefix...), nodeID, metric, metricType, stat, value, note)
	}

	// Every node we expected, plus any that answered without being expected.
	expected := map[string]bool{}
	for _, src := range cfg.Summaries {
		expected[src.NodeID] = true
	}
	for nodeID := range byNode {
		expected[nodeID] = true
	}

	for _, nodeID := range sortedKeys(expected) {
		summary, fetched := byNode[nodeID]
		mark := "0"
		if fetched {
			mark = "1"
		}
		_ = w.Write(row(nodeID, k6MetaMetric, "meta", "value", mark, notes[nodeID]))
		if !fetched {
			continue
		}
		for _, metric := range sortedKeys(summary.Metrics) {
			m := summary.Metrics[metric]
			for _, stat := range sortedKeys(m.Values) {
				_ = w.Write(row(nodeID, metric, m.Type, normalizeStat(stat),
					strconv.FormatFloat(m.Values[stat], 'g', -1, 64), ""))
			}
		}
	}
	w.Flush()
	return buf.Bytes()
}

// sortedKeys returns the map's keys in sorted order (deterministic CSV).
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func main() {
	cfg, err := ConfigFromEnv()
	if err != nil {
		log.Fatalf("%v", err)
	}

	ctx := context.Background()

	querier, err := newPromQuerier(cfg.PromURL)
	if err != nil {
		log.Fatalf("Prometheus client: %v", err)
	}

	sink, err := newSink(ctx, cfg.S3)
	if err != nil {
		log.Fatalf("destination: %v", err)
	}

	rows, err := run(ctx, cfg, querier, sink)
	if err != nil {
		log.Fatalf("%v", err)
	}
	fmt.Printf("export complete: %d CSV rows\n", rows)
}

// newSink picks the destination. No S3 config is not an error: the CSV goes to
// stdout between markers, recoverable with `kubectl logs`.
func newSink(ctx context.Context, cfg S3Config) (Sink, error) {
	if !cfg.Enabled() {
		fmt.Println("S3 not configured. Artifacts will be dumped to stdout.")
		return StdoutSink{Out: os.Stdout}, nil
	}
	return NewS3Sink(ctx, cfg)
}

// Querier is the metrics source: the interface lets tests replace the
// Prometheus client.
type Querier interface {
	// Range runs one range query. Prometheus warnings come back separately:
	// they are not errors, but they explain a partial or truncated result.
	Range(ctx context.Context, query string, r PromRange) (model.Matrix, []string, error)
}

type promQuerier struct{ api v1.API }

func newPromQuerier(url string) (Querier, error) {
	client, err := api.NewClient(api.Config{Address: url})
	if err != nil {
		return nil, err
	}
	return promQuerier{api: v1.NewAPI(client)}, nil
}

func (p promQuerier) Range(ctx context.Context, query string, r PromRange) (model.Matrix, []string, error) {
	result, warnings, err := p.api.QueryRange(ctx, query, v1.Range{Start: r.Start, End: r.End, Step: r.Step})
	if err != nil {
		return nil, warnings, err
	}
	matrix, ok := result.(model.Matrix)
	if !ok {
		return nil, warnings, fmt.Errorf("non-Matrix result (type=%T)", result)
	}
	return matrix, warnings, nil
}

// run is the whole export, with no environment, no clock and no network of its
// own: it queries through q and stores through sink. It returns the number of
// metrics rows written, and an error only for what must fail the exporter Job —
// which is the metrics CSV and nothing else.
//
// Order matters: the per-query status CSV goes out before the empty-export
// check, because that is the run where it is worth the most.
func run(ctx context.Context, cfg Config, q Querier, sink Sink) (int, error) {
	fmt.Printf("starting metrics export for experiment: %s\n", cfg.ExpName)

	results := make([]queryResult, 0, len(cfg.Metrics))
	var attempted, errored int
	for _, m := range cfg.Metrics {
		if m.Query == "" {
			fmt.Printf("skipping entry %q: empty query\n", m.MetricName)
			results = append(results, queryResult{entry: m, err: errors.New("empty query: nothing was run")})
			continue
		}
		attempted++

		matrix, warnings, err := q.Range(ctx, m.Query, cfg.Range)
		// Warnings are not errors, but they explain partial or truncated
		// results (series limits, lookback misses) during a post-mortem.
		if len(warnings) > 0 {
			fmt.Printf("query %s: Prometheus warnings: %s\n", m.MetricName, strings.Join(warnings, "; "))
		}
		if err != nil {
			fmt.Printf("query error %s (%s): %v\n", m.MetricName, m.Query, err)
			errored++
		}
		results = append(results, queryResult{entry: m, matrix: matrix, warnings: warnings, err: err})
	}
	if attempted > 0 && errored > 0 {
		fmt.Printf("metrics export: %d/%d queries failed\n", errored, attempted)
	}

	// The query-status CSV first, and never fatal: it is what names the query that failed on
	// the run where the next check ends the Job.
	if err := sink.Put(ctx, Artifact{
		Key:         queryStatusKeyFor(cfg.LoadTestName, cfg.Stamp()),
		Label:       "QUERY STATUS",
		ContentType: "text/csv",
		Body:        bytes.NewReader(buildQueryStatusCSV(cfg.LoadTestName, results)),
	}); err != nil {
		fmt.Printf("store query status CSV failed: %v\n", err)
	}

	metricsCSV, rowsWritten := buildMetricsCSV(cfg.LoadTestName, results)

	// Fail loudly when the export captured nothing: a header-only CSV that
	// uploads fine still lets the LoadTest report Completed with no data
	// (Prometheus unreachable, all queries wrong, or the samples were lost to a
	// restart). An error here exits non-zero, so the exporter Job -- and thus
	// the LoadTest -- fails instead of silently "succeeding" with an empty
	// result.
	if attempted > 0 && rowsWritten == 0 {
		return 0, fmt.Errorf("metrics export produced 0 data points across %d queries (%d errored); "+
			"refusing to report success with an empty CSV — check Prometheus reachability and the queries",
			attempted, errored)
	}

	// The metrics CSV is the one artifact whose loss fails the run.
	if err := sink.Put(ctx, Artifact{
		Key:         objectKeyFor(cfg.LoadTestName, cfg.Stamp()),
		Label:       "CSV",
		ContentType: "text/csv",
		Body:        bytes.NewReader(metricsCSV),
	}); err != nil {
		return rowsWritten, fmt.Errorf("store metrics CSV: %w", err)
	}

	// Everything below is per-Generator: a failure warns and the run goes on.
	// The k6 summary CSV is written even with no Generators at all — a header-only file
	// keeps a glob honest, while an absent one looks like a lost upload.
	byNode, summaryRaw, notes := fetchSummaries(ctx, cfg.Summaries)
	fmt.Printf("k6 summaries: %d/%d nodes fetched\n", len(byNode), len(cfg.Summaries))
	if err := sink.Put(ctx, Artifact{
		Key:         k6SummaryCSVKeyFor(cfg.LoadTestName, cfg.Stamp()),
		Label:       "K6 CSV",
		ContentType: "text/csv",
		Body:        bytes.NewReader(buildK6CSV(cfg, byNode, notes)),
	}); err != nil {
		fmt.Printf("store k6 summary CSV failed: %v\n", err)
	}

	shipK6Logs(ctx, cfg, sink)
	shipK6Summaries(ctx, cfg, sink, summaryRaw)

	return rowsWritten, nil
}

// shipK6Summaries stores each Generator's raw handleSummary JSON, so the
// official k6 aggregates survive next to the flattened CSV rows. Per-node
// failures warn and continue.
func shipK6Summaries(ctx context.Context, cfg Config, sink Sink, raw map[string][]byte) {
	for _, nodeID := range sortedKeys(raw) {
		err := sink.Put(ctx, Artifact{
			Key:         k6SummaryKeyFor(cfg.LoadTestName, nodeID, cfg.Stamp()),
			Label:       "K6 SUMMARY " + nodeID,
			ContentType: "application/json",
			Body:        bytes.NewReader(raw[nodeID]),
		})
		if err != nil {
			fmt.Printf("k6 summary for node %s failed: %v\n", nodeID, err)
		}
	}
}

// shipK6Logs stores each "<nodeID>.log" under cfg.K6LogDir (projected from the
// operator's per-node ConfigMaps). An unset, missing or empty dir is a no-op,
// and one node's failure warns and continues -- the run already succeeded.
func shipK6Logs(ctx context.Context, cfg Config, sink Sink) {
	if cfg.K6LogDir == "" {
		return
	}
	entries, err := os.ReadDir(cfg.K6LogDir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		fmt.Printf("k6 log dir %q read error: %v\n", cfg.K6LogDir, err)
		return
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		nodeID := strings.TrimSuffix(e.Name(), ".log")
		data, rerr := os.ReadFile(cfg.K6LogDir + "/" + e.Name())
		if rerr != nil {
			fmt.Printf("read k6 log for node %s failed: %v\n", nodeID, rerr)
			continue
		}
		err := sink.Put(ctx, Artifact{
			Key:         k6ObjectKeyFor(cfg.LoadTestName, nodeID, cfg.Stamp()),
			Label:       "K6 " + nodeID,
			ContentType: "text/plain",
			Body:        bytes.NewReader(data),
		})
		if err != nil {
			fmt.Printf("k6 log for node %s failed: %v\n", nodeID, err)
		}
	}
}

// ensureBucket runs HeadBucket and, when the bucket is missing, CreateBucket.
// Called once per run, from NewS3Sink.
func ensureBucket(ctx context.Context, client *s3.Client, bucket, region string) error {
	// HeadBucket. NotFound (404 / *types.NotFound) → try to create.
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		if !isS3NotFound(err) {
			return fmt.Errorf("head bucket %q: %w", bucket, err)
		}
		fmt.Printf("bucket %q missing — creating\n", bucket)
		createIn := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
		// AWS rejects LocationConstraint=us-east-1 (default region for
		// the v2 API path). For everything else we MUST set it or the
		// bucket lands wherever the endpoint defaults to.
		if region != "" && region != "us-east-1" {
			createIn.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
				LocationConstraint: s3types.BucketLocationConstraint(region),
			}
		}
		if _, cerr := client.CreateBucket(ctx, createIn); cerr != nil {
			switch {
			case isS3OwnedByYou(cerr):
				// Race: another exporter Pod for the same env created it.
				fmt.Printf("bucket %q already owned by us, continuing\n", bucket)
			case isS3AlreadyExists(cerr):
				// Globally-unique name collision with another tenant. Try
				// the PutObject anyway — it will fail loudly if access is
				// denied, which is the correct surface.
				fmt.Printf("WARNING: bucket %q already exists in another tenant; attempting PutObject anyway\n", bucket)
			default:
				return fmt.Errorf("create bucket %q: %w", bucket, cerr)
			}
		}
	}

	return nil
}

// isS3NotFound returns true for the HeadBucket / GetObject "missing" surface.
// AWS SDK v2 surfaces this as *types.NotFound (HeadBucket lacks a NoSuchBucket
// modeled error and downgrades to NotFound), with the wrapped APIError code
// "NotFound" or "NoSuchBucket".
func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NotFound" || code == "NoSuchBucket" || code == "404"
	}
	return false
}

// isS3OwnedByYou matches CreateBucket's idempotent-success error for the
// "already created by this account" case.
func isS3OwnedByYou(err error) bool {
	if err == nil {
		return false
	}
	var owned *s3types.BucketAlreadyOwnedByYou
	if errors.As(err, &owned) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "BucketAlreadyOwnedByYou"
	}
	return false
}

// isS3AlreadyExists matches CreateBucket's cross-tenant collision: the bucket
// name is taken globally by another AWS account.
func isS3AlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var exists *s3types.BucketAlreadyExists
	if errors.As(err, &exists) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "BucketAlreadyExists"
	}
	return false
}

// bucketRegexp matches every character that is NOT valid inside an S3
// bucket DNS label (lowercase alnum and dash). Anything else is replaced
// with a dash and then runs of dashes are collapsed.
var bucketRegexp = regexp.MustCompile(`[^a-z0-9-]+`)
var dashRunRegexp = regexp.MustCompile(`-+`)

// bucketNameFor builds a deterministic, S3-compliant bucket name from the
// Environment name and UID:
//   - lowercase the env name,
//   - replace non-[a-z0-9-] with "-",
//   - collapse repeated "-",
//   - trim leading/trailing "-",
//   - clamp the base to 56 chars,
//   - append "-" + first 6 chars of envUID (lowercase hex).
//
// Final length is at most 63 chars (56 + 1 + 6), satisfying S3's 3-63 char
// rule. The UID suffix prevents global namespace collisions when two
// different operator installs happen to name their Environment the same.
func bucketNameFor(envName, envUID string) string {
	name := strings.ToLower(envName)
	name = bucketRegexp.ReplaceAllString(name, "-")
	name = dashRunRegexp.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if len(name) > 56 {
		name = name[:56]
		name = strings.TrimRight(name, "-")
	}

	suffix := strings.ToLower(envUID)
	suffix = bucketRegexp.ReplaceAllString(suffix, "")
	if len(suffix) > 6 {
		suffix = suffix[:6]
	}

	if suffix == "" {
		// No UID supplied (defensive — operator always sets ENV_UID).
		// Pad the base to at least 3 chars so the result is S3-valid.
		if len(name) < 3 {
			name = (name + "env")[:3]
		}
		return name
	}
	if name == "" {
		// Defensive: env name sanitised to empty. Use a stable prefix.
		return "dfaas-" + suffix
	}
	return name + "-" + suffix
}

// The object keys of one export. Every one of them takes the run's stamp
// rather than reading the clock: the stamp is the end of the test window, so a
// retried exporter Job rewrites the same objects instead of leaving a second
// near-identical set, and the CSVs, the raw summaries and the logs of one run
// are trivially paired by filename.

// objectKeyFor is the metrics CSV: metrics/<loadtestName>/<stamp>.csv.
func objectKeyFor(loadtestName, stamp string) string {
	return fmt.Sprintf("metrics/%s/%s.csv", loadtestName, stamp)
}

// queryStatusKeyFor is the per-query outcome CSV, sibling of the metrics one:
// metrics/<loadtestName>/query-status-<stamp>.csv.
func queryStatusKeyFor(loadtestName, stamp string) string {
	return fmt.Sprintf("metrics/%s/query-status-%s.csv", loadtestName, stamp)
}

// k6SummaryCSVKeyFor is the flattened k6 summary of every Generator:
// k6/<loadtestName>/summary-<stamp>.csv.
func k6SummaryCSVKeyFor(loadtestName, stamp string) string {
	return fmt.Sprintf("k6/%s/summary-%s.csv", loadtestName, stamp)
}

// k6ObjectKeyFor is one VM's captured k6 output:
// k6/<loadtestName>/<nodeID>-<stamp>.log.
func k6ObjectKeyFor(loadtestName, nodeID, stamp string) string {
	return fmt.Sprintf("k6/%s/%s-%s.log", loadtestName, nodeID, stamp)
}

// k6SummaryKeyFor is one node's raw handleSummary JSON, kept verbatim next to
// the flattened CSV: k6/<loadtestName>/<nodeID>-summary-<stamp>.json.
func k6SummaryKeyFor(loadtestName, nodeID, stamp string) string {
	return fmt.Sprintf("k6/%s/%s-summary-%s.json", loadtestName, nodeID, stamp)
}
