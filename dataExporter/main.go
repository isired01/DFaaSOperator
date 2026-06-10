package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
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
	for _, m := range metrics {
		if m.Query == "" {
			fmt.Printf("skipping entry %q: empty query\n", m.MetricName)
			continue
		}

		result, _, err := promAPI.QueryRange(ctx, m.Query, queryRange)
		if err != nil {
			fmt.Printf("query error %s (%s): %v\n", m.MetricName, m.Query, err)
			continue
		}

		matrix, ok := result.(model.Matrix)
		if !ok {
			fmt.Printf("query %s: non-Matrix result (type=%T), skipping\n", m.MetricName, result)
			continue
		}

		for _, series := range matrix {
			nodeID := string(series.Metric["nodo_id"])
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
			}
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
		if err := uploadToS3(ctx, fileName, bucket, key); err != nil {
			log.Fatalf("S3 upload: %v", err)
		}
	}

	// 7. Per-VM k6 end-of-test summaries: one file per node under K6_LOG_DIR
	// (projected from the operator's per-node ConfigMaps). Shipped the same
	// way as metrics — S3 when configured, else stdout. A missing/empty dir
	// (no k6 logs) is tolerated without error.
	exportK6Logs(ctx, s3Enabled, bucket, loadtestName)
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
			if err := uploadToS3(ctx, fullPath, bucket, key); err != nil {
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

// uploadToS3 uploads csvPath to s3://bucket/key. The function is responsible
// for HeadBucket → CreateBucket (when missing) → PutObject. Static creds
// come from S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY; region from S3_REGION;
// optional custom endpoint from S3_ENDPOINT (e.g. MinIO); path-style
// addressing toggled by S3_FORCE_PATH_STYLE.
func uploadToS3(ctx context.Context, csvPath, bucket, key string) error {
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

	// 1. HeadBucket. NotFound (404 / *types.NotFound) → try to create.
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		if isS3NotFound(err) {
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
		} else {
			return fmt.Errorf("head bucket %q: %w", bucket, err)
		}
	}

	// 2. PutObject.
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	fmt.Printf("uploading to s3://%s/%s\n", bucket, key)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        f,
		ContentType: aws.String("text/csv"),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
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
