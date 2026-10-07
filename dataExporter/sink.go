package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Artifact is one thing the exporter stores. Label exists because the stdout
// destination is a documented recovery path (`kubectl logs job/...`, see the
// README) and its markers name the artifact rather than its object key.
type Artifact struct {
	Key         string
	Label       string
	ContentType string
	Body        io.Reader
}

// Sink stores one artifact. The error is per-artifact, and the caller decides
// whether it is fatal: the metrics CSV is, one Generator's log is not.
type Sink interface {
	Put(ctx context.Context, a Artifact) error
}

// StdoutSink prints each artifact between BEGIN/END markers so it can be
// recovered from the Job's logs when no object store is configured.
type StdoutSink struct{ Out io.Writer }

func (s StdoutSink) Put(_ context.Context, a Artifact) error {
	out := s.Out
	if out == nil {
		return fmt.Errorf("StdoutSink has no writer")
	}
	body, err := io.ReadAll(a.Body)
	if err != nil {
		return fmt.Errorf("read %s: %w", a.Label, err)
	}
	if _, err := fmt.Fprintf(out, "----- BEGIN %s -----\n", a.Label); err != nil {
		return err
	}
	if _, err := out.Write(body); err != nil {
		return err
	}
	// The CSV already ends in a newline; a k6 log may not.
	if len(body) > 0 && body[len(body)-1] != '\n' {
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(out, "----- END %s -----\n", a.Label)
	return err
}

// s3UploadAttempts bounds the PutObject tries, and s3RetryBackoff is the pause
// before the first retry (doubled each round: 2s then 4s, so at most 6s of
// extra wall time). One flaky PutObject must not fail the exporter Job and with
// it the whole LoadTest, even though the k6 run had already succeeded. The AWS
// SDK retries connection-level faults on its own; this outer loop also covers
// what it treats as terminal (e.g. a SeaweedFS bucket still settling right
// after CreateBucket).
const (
	s3UploadAttempts = 3
	s3RetryBackoff   = 2 * time.Second
)

// S3Sink stores artifacts in one bucket. The client is built and the bucket
// ensured once, at construction, not per upload.
type S3Sink struct {
	client *s3.Client
	bucket string
}

// Bucket is the resolved bucket name, for logging.
func (s *S3Sink) Bucket() string { return s.bucket }

// NewS3Sink builds the client and makes sure the bucket exists.
func NewS3Sink(ctx context.Context, cfg S3Config) (*S3Sink, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})

	bucket := cfg.Bucket()
	if err := ensureBucket(ctx, client, bucket, cfg.Region); err != nil {
		return nil, err
	}
	return &S3Sink{client: client, bucket: bucket}, nil
}

func (s *S3Sink) Put(ctx context.Context, a Artifact) error {
	// PutObject consumes the reader, so a retry needs to replay the body. Buffering
	// it avoids requiring every artifact to be a file on disk.
	body, err := io.ReadAll(a.Body)
	if err != nil {
		return fmt.Errorf("read %s: %w", a.Label, err)
	}

	fmt.Printf("uploading to s3://%s/%s\n", s.bucket, a.Key)
	backoff := s3RetryBackoff
	for attempt := 1; ; attempt++ {
		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(a.Key),
			Body:        bytes.NewReader(body),
			ContentType: aws.String(a.ContentType),
		})
		if err == nil {
			break
		}
		if attempt == s3UploadAttempts {
			return fmt.Errorf("put object after %d attempts: %w", s3UploadAttempts, err)
		}
		fmt.Printf("put object s3://%s/%s attempt %d/%d failed: %v — retrying in %s\n",
			s.bucket, a.Key, attempt, s3UploadAttempts, err, backoff)
		select {
		case <-ctx.Done():
			return fmt.Errorf("put object: %w", err)
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	fmt.Printf("uploaded s3://%s/%s\n", s.bucket, a.Key)
	return nil
}
