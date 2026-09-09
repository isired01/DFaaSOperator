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
// (they embed the operator's sanitize(nodeID)) — never rebuild them here.
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

// summaryAllNodes is the ID_Nodo value of the cross-node aggregate rows.
const summaryAllNodes = "__all__"

// fetchSummaries GETs each node's summary JSON from the filer (10s timeout
// each). Per-node fetch or parse failures warn and skip — the Prometheus
// metrics are still valuable, so this path is never fatal. Returns parsed
// summaries and the raw bytes (for the verbatim S3 upload), both keyed by
// nodeID.
func fetchSummaries(ctx context.Context, sources []summarySource) (map[string]k6Summary, map[string][]byte) {
	client := &http.Client{Timeout: 10 * time.Second}
	parsed := make(map[string]k6Summary)
	raw := make(map[string][]byte)
	for _, src := range sources {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
		if err != nil {
			fmt.Printf("k6 summary for node %s: build request: %v\n", src.NodeID, err)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("k6 summary for node %s: fetch %s: %v\n", src.NodeID, src.URL, err)
			continue
		}
		body, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("k6 summary for node %s: %s returned %s\n", src.NodeID, src.URL, resp.Status)
			continue
		}
		if rerr != nil {
			fmt.Printf("k6 summary for node %s: read body: %v\n", src.NodeID, rerr)
			continue
		}
		var s k6Summary
		if err := json.Unmarshal(body, &s); err != nil {
			fmt.Printf("k6 summary for node %s: parse error: %v\n", src.NodeID, err)
			continue
		}
		if len(s.Metrics) == 0 {
			// Shape drift guard (pre-v0.34 k6 lacks the values nesting).
			snippet := body
			if len(snippet) > 200 {
				snippet = snippet[:200]
			}
			fmt.Printf("k6 summary for node %s: no metrics parsed; first bytes: %s\n", src.NodeID, snippet)
			continue
		}
		parsed[src.NodeID] = s
		raw[src.NodeID] = body
	}
	return parsed, raw
}

// normalizeStat makes k6 stat names CSV/analysis friendly: p(95) → p95.
func normalizeStat(stat string) string {
	return strings.NewReplacer("(", "", ")", "").Replace(stat)
}

// summaryRow builds one CSV row in the existing 8-column layout. Query is the
// fixed marker "handleSummary" so k6 rows are trivially filterable.
func summaryRow(endTime, nodeID, metric, stat string, v float64) []string {
	return []string{
		endTime, nodeID, "k6",
		metric + "_" + normalizeStat(stat),
		"handleSummary", "",
		strconv.FormatFloat(v, 'g', -1, 64), "",
	}
}

// flattenSummaryRows converts one node's summary into CSV rows, every metric
// and every stat, deterministically ordered.
func flattenSummaryRows(nodeID, endTime string, s k6Summary) [][]string {
	var rows [][]string
	for _, metric := range sortedKeys(s.Metrics) {
		vals := s.Metrics[metric].Values
		stats := make([]string, 0, len(vals))
		for st := range vals {
			stats = append(stats, st)
		}
		sort.Strings(stats)
		for _, st := range stats {
			rows = append(rows, summaryRow(endTime, nodeID, metric, st, vals[st]))
		}
	}
	return rows
}

// aggregateSummaryRows computes the __all__ rows across nodes. Only
// mathematically valid cross-node aggregations are emitted:
//   - counter: count = sum (per-second rate omitted — windows may differ)
//   - rate:    passes/fails = sum, rate recomputed as passes/(passes+fails)
//   - gauge/trend: min = min-of-min, max = max-of-max
//
// avg/med/value and all percentiles are deliberately omitted: averaging
// per-node percentiles is statistically invalid.
func aggregateSummaryRows(endTime string, byNode map[string]k6Summary) [][]string {
	if len(byNode) == 0 {
		return nil
	}
	// metric name → per-node entries (deterministic node order).
	type acc struct {
		typ  string
		vals []map[string]float64
	}
	metrics := make(map[string]*acc)
	for _, nodeID := range sortedKeys(byNode) {
		for name, m := range byNode[nodeID].Metrics {
			a := metrics[name]
			if a == nil {
				a = &acc{typ: m.Type}
				metrics[name] = a
			}
			a.vals = append(a.vals, m.Values)
		}
	}

	sum := func(vals []map[string]float64, stat string) (float64, bool) {
		total, seen := 0.0, false
		for _, v := range vals {
			if x, ok := v[stat]; ok {
				total += x
				seen = true
			}
		}
		return total, seen
	}
	extreme := func(vals []map[string]float64, stat string, wantMax bool) (float64, bool) {
		best, seen := 0.0, false
		for _, v := range vals {
			x, ok := v[stat]
			if !ok {
				continue
			}
			if !seen || (wantMax && x > best) || (!wantMax && x < best) {
				best = x
			}
			seen = true
		}
		return best, seen
	}

	var rows [][]string
	for _, name := range sortedKeys(metrics) {
		a := metrics[name]
		switch a.typ {
		case "counter":
			if v, ok := sum(a.vals, "count"); ok {
				rows = append(rows, summaryRow(endTime, summaryAllNodes, name, "count", v))
			}
		case "rate":
			passes, okP := sum(a.vals, "passes")
			fails, okF := sum(a.vals, "fails")
			if okP {
				rows = append(rows, summaryRow(endTime, summaryAllNodes, name, "passes", passes))
			}
			if okF {
				rows = append(rows, summaryRow(endTime, summaryAllNodes, name, "fails", fails))
			}
			if okP && okF && passes+fails > 0 {
				rows = append(rows, summaryRow(endTime, summaryAllNodes, name, "rate", passes/(passes+fails)))
			}
		case "gauge", "trend":
			if v, ok := extreme(a.vals, "min", false); ok {
				rows = append(rows, summaryRow(endTime, summaryAllNodes, name, "min", v))
			}
			if v, ok := extreme(a.vals, "max", true); ok {
				rows = append(rows, summaryRow(endTime, summaryAllNodes, name, "max", v))
			}
		}
	}
	return rows
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

// Querier is the metrics source. The concrete Prometheus client is a struct
// with no interface, built inside main from an env var, which is why the query
// loop and the CSV layout below were unreachable by any test.
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
// CSV rows written, and an error only for what must fail the exporter Job --
// which is the metrics CSV and nothing else.
func run(ctx context.Context, cfg Config, q Querier, sink Sink) (int, error) {
	var csvBuf bytes.Buffer
	writer := csv.NewWriter(&csvBuf)
	_ = writer.Write([]string{
		"Timestamp", "ID_Nodo", "Type", "MetricName", "Query", "Comment", "Valore", "Labels",
	})

	fmt.Printf("starting metrics export for experiment: %s\n", cfg.ExpName)
	var attempted, errored, rowsWritten int
	for _, m := range cfg.Metrics {
		if m.Query == "" {
			fmt.Printf("skipping entry %q: empty query\n", m.MetricName)
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
			continue
		}

		for _, series := range matrix {
			nodeID := string(series.Metric["node_id"])
			if nodeID == "" {
				nodeID = "unknown"
			}
			allLabels := series.Metric.String()

			for _, pair := range series.Values {
				_ = writer.Write([]string{
					pair.Timestamp.Time().Format(time.RFC3339),
					nodeID,
					m.Type,
					m.MetricName,
					m.Query,
					m.Comment,
					pair.Value.String(),
					allLabels,
				})
				rowsWritten++
			}
		}
	}
	if attempted > 0 && errored > 0 {
		fmt.Printf("metrics export: %d/%d queries failed\n", errored, attempted)
	}
	// Fail loudly when the export captured nothing: a header-only CSV that
	// uploads fine still lets the LoadTest report Completed with no data
	// (Prometheus unreachable, all queries wrong, or the samples were lost to a
	// restart). An error here exits non-zero, so the exporter Job -- and thus
	// the LoadTest -- fails instead of silently "succeeding" with an empty
	// result. It used to be a log.Fatalf, testable only by spawning a
	// subprocess.
	if attempted > 0 && rowsWritten == 0 {
		return 0, fmt.Errorf("metrics export produced 0 data points across %d queries (%d errored); "+
			"refusing to report success with an empty CSV — check Prometheus reachability and the queries",
			attempted, errored)
	}

	// k6 end-of-test summaries: fetch each Generator's handleSummary JSON from
	// the filer and flatten every metric into the same CSV, plus an __all__
	// aggregate row set. No sources (scripts generated before the feature, or
	// no Generators) skips it. Never fatal: the Prometheus rows above are still
	// valuable on partial or total summary loss.
	var summaryRaw map[string][]byte
	if len(cfg.Summaries) > 0 {
		byNode, raw := fetchSummaries(ctx, cfg.Summaries)
		summaryRaw = raw
		summaryRows := 0
		endStr := cfg.EndTimeString()
		for _, nodeID := range sortedKeys(byNode) {
			for _, row := range flattenSummaryRows(nodeID, endStr, byNode[nodeID]) {
				_ = writer.Write(row)
				summaryRows++
			}
		}
		for _, row := range aggregateSummaryRows(endStr, byNode) {
			_ = writer.Write(row)
			summaryRows++
		}
		fmt.Printf("k6 summaries: %d/%d nodes fetched, %d CSV rows\n",
			len(byNode), len(cfg.Summaries), summaryRows)
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return rowsWritten, fmt.Errorf("CSV writer: %w", err)
	}

	// The metrics CSV is the one artifact whose loss fails the run.
	if err := sink.Put(ctx, Artifact{
		Key:         objectKeyFor(cfg.LoadTestName),
		Label:       "CSV",
		ContentType: "text/csv",
		Body:        bytes.NewReader(csvBuf.Bytes()),
	}); err != nil {
		return rowsWritten, fmt.Errorf("store metrics CSV: %w", err)
	}

	// Everything below is per-Generator: a failure warns and the run goes on.
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
			Key:         k6SummaryKeyFor(cfg.LoadTestName, nodeID),
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
			Key:         k6ObjectKeyFor(cfg.LoadTestName, nodeID),
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
// Called once per run, from NewS3Sink -- the old per-upload memo existed only
// because every upload redid the probe.
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

// objectKeyFor builds the S3 object key for a single LoadTest CSV:
// metrics/<loadtestName>/<UTC RFC3339-compact>.csv. The compact timestamp
// is filename-safe and sortable.
func objectKeyFor(loadtestName string) string {
	return fmt.Sprintf("metrics/%s/%s.csv",
		loadtestName,
		time.Now().UTC().Format("20060102T150405Z"))
}

// k6ObjectKeyFor builds the S3 object key for one VM's k6 end-of-test summary:
// k6/<loadtestName>/<nodeID>-<UTC RFC3339-compact>.log. Sits in a sibling "k6/"
// prefix to the metrics CSVs so per-test artifacts group together.
func k6ObjectKeyFor(loadtestName, nodeID string) string {
	return fmt.Sprintf("k6/%s/%s-%s.log",
		loadtestName,
		nodeID,
		time.Now().UTC().Format("20060102T150405Z"))
}

// k6SummaryKeyFor builds the S3 object key for one node's raw handleSummary
// JSON: k6/<loadtestName>/<nodeID>-summary-<UTC RFC3339-compact>.json —
// sibling of the k6ObjectKeyFor log keys.
func k6SummaryKeyFor(loadtestName, nodeID string) string {
	return fmt.Sprintf("k6/%s/%s-summary-%s.json",
		loadtestName,
		nodeID,
		time.Now().UTC().Format("20060102T150405Z"))
}
