package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// The exporter's whole input, parsed in one place. main() used to read 16
// os.Getenv calls spread across four functions -- six at the top, six more
// inside uploadToS3 on every single call, one in exportK6Logs, and the rest
// mid-main -- so nothing below main() could be exercised without setting
// process-wide state, and main_test.go could only reach the four pure helpers.

// PromRange is the query window.
type PromRange struct {
	Start, End time.Time
	Step       time.Duration
}

// S3Config is the object-store destination. A zero BucketPrefix means "not
// configured", which is the stdout path -- not an error.
type S3Config struct {
	BucketPrefix   string
	EnvUID         string
	Region         string
	Endpoint       string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
}

// Enabled reports whether S3 is configured. The operator sets S3_BUCKET_PREFIX
// only when it resolved an S3 config Secret.
func (s S3Config) Enabled() bool { return s.BucketPrefix != "" }

// Bucket is the deterministic bucket name for this Environment.
func (s S3Config) Bucket() string { return bucketNameFor(s.BucketPrefix, s.EnvUID) }

// Config is everything the exporter needs, parsed once from the environment.
type Config struct {
	PromURL      string
	Range        PromRange
	Metrics      []MetricEntry
	ExpName      string
	LoadTestName string
	// EnvName is the Environment the test ran against. It labels the k6 CSV,
	// whose rows carry no Prometheus labels to say where they came from.
	EnvName string
	// Summaries are the per-Generator filer URLs. Empty means the k6 summary
	// step is skipped entirely -- scripts generated before the feature, or a
	// LoadTest with no Generators.
	Summaries []summarySource
	// K6LogDir holds one "<nodeID>.log" per Generator, projected from the
	// operator's ConfigMaps. Empty, missing or empty-on-disk are all fine.
	K6LogDir string
	S3       S3Config
}

// ConfigFromEnv parses the environment the operator sets on the exporter Job.
// Every failure here is fatal and worth naming: a bad METRICS_JSON or an
// unparseable window would otherwise produce an empty CSV that uploads fine.
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		PromURL:      os.Getenv("PROM_URL"),
		ExpName:      os.Getenv("EXP_NAME"),
		LoadTestName: os.Getenv("LOADTEST_NAME"),
		EnvName:      os.Getenv("ENV_NAME"),
		K6LogDir:     os.Getenv("K6_LOG_DIR"),
		S3: S3Config{
			BucketPrefix: os.Getenv("S3_BUCKET_PREFIX"),
			EnvUID:       os.Getenv("ENV_UID"),
			Region:       os.Getenv("S3_REGION"),
			Endpoint:     os.Getenv("S3_ENDPOINT"),
			AccessKey:    os.Getenv("S3_ACCESS_KEY_ID"),
			SecretKey:    os.Getenv("S3_SECRET_ACCESS_KEY"),
		},
	}
	cfg.S3.ForcePathStyle, _ = strconv.ParseBool(os.Getenv("S3_FORCE_PATH_STYLE"))

	metricsJSON := os.Getenv("METRICS_JSON")
	if metricsJSON == "" {
		return cfg, fmt.Errorf("METRICS_JSON env var is empty")
	}
	if err := json.Unmarshal([]byte(metricsJSON), &cfg.Metrics); err != nil {
		return cfg, fmt.Errorf("METRICS_JSON parse error: %w", err)
	}
	if len(cfg.Metrics) == 0 {
		return cfg, fmt.Errorf("METRICS_JSON contains zero entries")
	}

	var err error
	if cfg.Range, err = parseRange(
		os.Getenv("START_TIME"), os.Getenv("END_TIME"), os.Getenv("STEP"),
	); err != nil {
		return cfg, err
	}

	// A malformed K6_SUMMARY_SOURCES is NOT fatal, matching the old behaviour:
	// the Prometheus rows are still valuable on total summary loss.
	if src := os.Getenv("K6_SUMMARY_SOURCES"); src != "" {
		if err := json.Unmarshal([]byte(src), &cfg.Summaries); err != nil {
			fmt.Printf("K6_SUMMARY_SOURCES parse error: %v\n", err)
			cfg.Summaries = nil
		}
	}

	return cfg, nil
}

func parseRange(startStr, endStr, stepStr string) (PromRange, error) {
	var r PromRange
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return r, fmt.Errorf("parse START_TIME %q: %w", startStr, err)
	}
	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		return r, fmt.Errorf("parse END_TIME %q: %w", endStr, err)
	}
	step, err := time.ParseDuration(stepStr)
	if err != nil {
		return r, fmt.Errorf("parse STEP %q: %w", stepStr, err)
	}
	return PromRange{Start: start, End: end, Step: step}, nil
}

// StartTimeString and EndTimeString are the test window, written onto every
// k6 summary row: those rows aggregate the whole window and have no instant of
// their own.
func (c Config) StartTimeString() string {
	return c.Range.Start.UTC().Format(time.RFC3339)
}

func (c Config) EndTimeString() string {
	return c.Range.End.UTC().Format(time.RFC3339)
}

// Stamp identifies one export in every object key. It is the end of the test
// window, not the clock: the same run always produces the same keys.
func (c Config) Stamp() string {
	return c.Range.End.UTC().Format("20060102T150405Z")
}
