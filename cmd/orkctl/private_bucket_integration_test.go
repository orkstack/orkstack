//go:build integration

package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestMain runs this package's integration tests in an object-storage bucket
// of their own (internal/testsupport/s3bucket). Each test here boots one or
// more real engines, and every boot reads the whole namespace registry and
// Orkflow store; on the shared development bucket that is every earlier run's
// leftovers, which made each boot cost time proportional to the bucket's
// history and pushed this package toward its `go test -timeout`.
//
// It also removes the binaries internal/testsupport/testbin built for this
// process, which live outside any t.TempDir because every test of the
// process shares them: left behind, each run of this package kept 167 MB in
// the temp directory.
func TestMain(m *testing.M) {
	code := s3bucket.Main(m, "orkctl")
	testbin.Cleanup()
	os.Exit(code)
}

// TestIntegration_ThisBinaryOwnsItsObjectStore pins that isolation: every
// engine, Orker and in-process store in this package resolves its bucket
// through ORK_S3_BUCKET, so the variable must name a bucket that exists and
// belongs to this process — never the shared development bucket.
func TestIntegration_ThisBinaryOwnsItsObjectStore(t *testing.T) {
	cfg := artifacts.ConfigFromEnv()
	requireObjectStoreForIdempotency(t, cfg)

	if cfg.S3Bucket == artifacts.DefaultS3Bucket {
		t.Fatalf("ORK_S3_BUCKET is the shared development bucket %q; every engine boot here would read every earlier run's namespaces and Orkflows", cfg.S3Bucket)
	}
	if want := "orkstack-orkctl-" + strconv.Itoa(os.Getpid()) + "-"; !strings.HasPrefix(cfg.S3Bucket, want) {
		t.Fatalf("ORK_S3_BUCKET = %q, want this process's own bucket %s<suffix>", cfg.S3Bucket, want)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := s3bucket.Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		t.Fatalf("this binary's bucket %s does not exist: %v", cfg.S3Bucket, err)
	}
	t.Logf("object store for this binary: s3://%s", cfg.S3Bucket)
}
