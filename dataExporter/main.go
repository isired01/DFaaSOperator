package main

import (
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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	// 1. Read parameters from env vars (set by the operator).
	promURL := os.Getenv("PROM_URL")
	startStr := os.Getenv("START_TIME")
	endStr := os.Getenv("END_TIME")
	stepStr := os.Getenv("STEP")
	expName := os.Getenv("EXP_NAME")
	metricsJSON := os.Getenv("METRICS_JSON")

	if metricsJSON == "" {
		log.Fatalf("METRICS_JSON env var is empty")
	}
	var metrics []MetricEntry
	if err := json.Unmarshal([]byte(metricsJSON), &metrics); err != nil {
		log.Fatalf("METRICS_JSON parse error: %v", err)
	}
	if len(metrics) == 0 {
		log.Fatalf("METRICS_JSON contains zero entries")
	}

	// 2. Parse times and step.
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		log.Fatalf("parse START_TIME %q: %v", startStr, err)
	}
	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		log.Fatalf("parse END_TIME %q: %v", endStr, err)
	}
	step, err := time.ParseDuration(stepStr)
	if err != nil {
		log.Fatalf("parse STEP %q: %v", stepStr, err)
	}

	// 3. Set up Prometheus client.
	client, err := api.NewClient(api.Config{Address: promURL})
	if err != nil {
		log.Fatalf("Prometheus client: %v", err)
	}
	promAPI := v1.NewAPI(client)

	// 4. Create local (temporary) CSV file.
	outDir := "tmp/export"
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("create directory %s: %v", outDir, err)
	}
	fileName := fmt.Sprintf("%s/%s_report.csv", outDir, expName)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("create CSV file: %v", err)
	}

	writer := csv.NewWriter(file)
	_ = writer.Write([]string{
		"Timestamp", "ID_Nodo", "Type", "MetricName", "Query", "Comment", "Valore", "Labels",
	})

	// 5. Pull data from Prometheus.
	ctx := context.Background()
	queryRange := v1.Range{Start: start, End: end, Step: step}

	fmt.Printf("starting metrics export for experiment: %s\n", expName)
	var attempted, errored, rowsWritten int
	for _, m := range metrics {
		if m.Query == "" {
			fmt.Printf("skipping entry %q: empty query\n", m.MetricName)
			continue
		}
		attempted++

		result, warnings, err := promAPI.QueryRange(ctx, m.Query, queryRange)
		if err != nil {
			fmt.Printf("query error %s (%s): %v\n", m.MetricName, m.Query, err)
			errored++
			continue
		}
		// Warnings are not errors, but they explain partial or truncated
		// results (series limits, lookback misses) during a post-mortem.
		if len(warnings) > 0 {
			fmt.Printf("query %s: Prometheus warnings: %s\n", m.MetricName, strings.Join(warnings, "; "))
		}

		matrix, ok := result.(model.Matrix)
		if !ok {
			fmt.Printf("query %s: non-Matrix result (type=%T), skipping\n", m.MetricName, result)
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
	// restart). Exit non-zero so the exporter Job — and thus the LoadTest —
	// fails instead of silently "succeeding" with an empty result.
	if attempted > 0 && rowsWritten == 0 {
		log.Fatalf("metrics export produced 0 data points across %d queries (%d errored); "+
			"refusing to report success with an empty CSV — check Prometheus reachability and the queries", attempted, errored)
	}

	// 5b. k6 end-of-test summaries: fetch each node's handleSummary JSON from
	// the SeaweedFS filer and flatten every metric into the same CSV, plus an
	// __all__ aggregate row set. Unset env var (scripts generated before the
	// feature, or no k6 nodes) → skip entirely. Never fatal: the Prometheus
	// rows above are still valuable on partial or total summary loss.
	var summaryRaw map[string][]byte
	if src := os.Getenv("K6_SUMMARY_SOURCES"); src != "" {
		var sources []summarySource
		if err := json.Unmarshal([]byte(src), &sources); err != nil {
			fmt.Printf("K6_SUMMARY_SOURCES parse error: %v\n", err)
		} else {
			byNode, raw := fetchSummaries(ctx, sources)
			summaryRaw = raw
			summaryRows := 0
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
				len(byNode), len(sources), summaryRows)
		}
	}

	// Close writer + file before reading / uploading.
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatalf("CSV writer: %v", err)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("close CSV file: %v", err)
	}
	fmt.Printf("local export complete: %s\n", fileName)

	// 6. Destination: S3 when S3_BUCKET_PREFIX is set, otherwise stdout. The
	// bucket is computed once and reused by both the metrics CSV and the
	// per-VM k6 logs.
	bucketPrefix := os.Getenv("S3_BUCKET_PREFIX")
	loadtestName := os.Getenv("LOADTEST_NAME")
	s3Enabled := bucketPrefix != ""
	var bucket string
	if s3Enabled {
		bucket = bucketNameFor(bucketPrefix, os.Getenv("ENV_UID"))
	}

	if !s3Enabled {
		fmt.Println("S3 not configured. Dumping CSV to stdout:")
		dumpToStdout(fileName)
	} else {
		key := objectKeyFor(loadtestName)
		if err := uploadToS3(ctx, fileName, bucket, key, "text/csv"); err != nil {
			log.Fatalf("S3 upload: %v", err)
		}
	}

	// 7. Per-VM k6 end-of-test summaries: one file per node under K6_LOG_DIR
	// (projected from the operator's per-node ConfigMaps). Shipped the same
	// way as metrics — S3 when configured, else stdout. A missing/empty dir
	// (no k6 logs) is tolerated without error.
	exportK6Logs(ctx, s3Enabled, bucket, loadtestName)

	// 8. Raw per-node handleSummary JSONs, verbatim: the official k6
	// aggregates survive next to the flattened CSV rows.
	exportK6Summaries(ctx, s3Enabled, bucket, loadtestName, summaryRaw)
}

// exportK6Summaries ships each node's raw summary JSON: to S3 (sibling of the
// k6 logs) when enabled, otherwise to stdout between per-node markers.
// Per-node failures warn and continue, matching exportK6Logs.
func exportK6Summaries(ctx context.Context, s3Enabled bool, bucket, loadtestName string, raw map[string][]byte) {
	for _, nodeID := range sortedKeys(raw) {
		if !s3Enabled {
			fmt.Printf("----- BEGIN K6 SUMMARY %s -----\n", nodeID)
			fmt.Println(string(raw[nodeID]))
			fmt.Printf("----- END K6 SUMMARY %s -----\n", nodeID)
			continue
		}
		path := fmt.Sprintf("tmp/export/%s-summary.json", nodeID)
		if err := os.WriteFile(path, raw[nodeID], 0o644); err != nil {
			fmt.Printf("k6 summary for node %s: write temp file: %v\n", nodeID, err)
			continue
		}
		key := k6SummaryKeyFor(loadtestName, nodeID)
		if err := uploadToS3(ctx, path, bucket, key, "application/json"); err != nil {
			fmt.Printf("k6 summary S3 upload for node %s failed: %v\n", nodeID, err)
		}
	}
}

// exportK6Logs reads each "<nodeID>.log" file under K6_LOG_DIR and ships it:
// to S3 (k6ObjectKeyFor key) when S3 is enabled, otherwise to stdout between
// per-node markers. No-op when K6_LOG_DIR is unset, missing, or empty.
func exportK6Logs(ctx context.Context, s3Enabled bool, bucket, loadtestName string) {
	dir := os.Getenv("K6_LOG_DIR")
	if dir == "" {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		fmt.Printf("k6 log dir %q read error: %v\n", dir, err)
		return
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		nodeID := strings.TrimSuffix(e.Name(), ".log")
		fullPath := dir + "/" + e.Name()

		if s3Enabled {
			key := k6ObjectKeyFor(loadtestName, nodeID)
			if err := uploadToS3(ctx, fullPath, bucket, key, "text/plain"); err != nil {
				fmt.Printf("k6 log S3 upload for node %s failed: %v\n", nodeID, err)
			}
			continue
		}

		data, rerr := os.ReadFile(fullPath)
		if rerr != nil {
			fmt.Printf("read k6 log for node %s failed: %v\n", nodeID, rerr)
			continue
		}
		fmt.Printf("----- BEGIN K6 %s -----\n", nodeID)
		fmt.Print(string(data))
		if len(data) > 0 && data[len(data)-1] != '\n' {
			fmt.Println()
		}
		fmt.Printf("----- END K6 %s -----\n", nodeID)
	}
}

// dumpToStdout reads the CSV and prints it to stdout between markers so it
// can be recovered via `kubectl logs job/<exp>-exporter-job`.
func dumpToStdout(fileName string) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("read csv: %v", err)
	}
	fmt.Println("----- BEGIN CSV -----")
	fmt.Print(string(data))
	fmt.Println("----- END CSV -----")
}

// s3UploadAttempts bounds the PutObject tries inside uploadToS3, and
// s3RetryBackoff is the pause before the first retry (doubled each round: 2s
// then 4s, so at most 6s of extra wall time). One flaky PutObject used to fail
// the exporter Job and with it the whole LoadTest, even though the k6 run had
// already succeeded. The AWS SDK retries connection-level faults on its own;
// this outer loop also covers what it treats as terminal (e.g. a SeaweedFS
// bucket still settling right after CreateBucket).
const (
	s3UploadAttempts = 3
	s3RetryBackoff   = 2 * time.Second
)

// ensuredBuckets memoizes the HeadBucket → CreateBucket probe per bucket name:
// every upload used to redo it (1 + 2N round trips per run, always against the
// same bucket). Only successful probes are recorded, so a failed one is retried
// by the next upload. The exporter is single-threaded, so a plain map suffices.
var ensuredBuckets = map[string]bool{}

// ensureBucket runs HeadBucket and, when the bucket is missing, CreateBucket.
// Its result is memoized, so the probe costs at most one round trip per run.
func ensureBucket(ctx context.Context, client *s3.Client, bucket, region string) error {
	if ensuredBuckets[bucket] {
		return nil
	}

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

	ensuredBuckets[bucket] = true
	return nil
}

// uploadToS3 uploads the file at path to s3://bucket/key with the given
// Content-Type. It ensures the bucket exists (once per run, see ensureBucket)
// and then PutObjects with a bounded retry. Static creds come from
// S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY; region from S3_REGION; optional
// custom endpoint from S3_ENDPOINT (e.g. SeaweedFS); path-style addressing
// toggled by S3_FORCE_PATH_STYLE.
func uploadToS3(ctx context.Context, path, bucket, key, contentType string) error {
	region := os.Getenv("S3_REGION")
	endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY_ID")
	secretKey := os.Getenv("S3_SECRET_ACCESS_KEY")
	forcePathStyle, _ := strconv.ParseBool(os.Getenv("S3_FORCE_PATH_STYLE"))

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = forcePathStyle
	})

	// 1. Bucket: HeadBucket → CreateBucket when missing, memoized per run.
	if err := ensureBucket(ctx, client, bucket, region); err != nil {
		return err
	}

	// 2. PutObject, retried up to s3UploadAttempts times. The body is rewound
	// before every attempt because PutObject consumes the reader.
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	fmt.Printf("uploading to s3://%s/%s\n", bucket, key)
	backoff := s3RetryBackoff
	for attempt := 1; ; attempt++ {
		if _, serr := f.Seek(0, io.SeekStart); serr != nil {
			return fmt.Errorf("rewind file: %w", serr)
		}
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        f,
			ContentType: aws.String(contentType),
		})
		if err == nil {
			break
		}
		if attempt == s3UploadAttempts {
			return fmt.Errorf("put object after %d attempts: %w", s3UploadAttempts, err)
		}
		fmt.Printf("put object s3://%s/%s attempt %d/%d failed: %v — retrying in %s\n",
			bucket, key, attempt, s3UploadAttempts, err, backoff)
		select {
		case <-ctx.Done():
			return fmt.Errorf("put object: %w", err)
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	fmt.Printf("uploaded s3://%s/%s\n", bucket, key)
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
