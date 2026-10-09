// Package s3bucket gives one integration test binary an object-storage bucket
// of its own.
//
// Every engine boot reads the whole namespace registry and Orkflow store
// (cmd/ork openNamespaceStore, openOrkflowStore), one object at a time. On the
// shared development bucket those prefixes hold every namespace and Orkflow
// every earlier test run left behind — data a test must never delete — so a
// boot there costs time proportional to the bucket's whole history rather
// than to the test: one measured boot spent ~26 s listing
// 2,400 namespaces and 3,200 Orkflows it had no use for, and a package that
// boots a few hundred engines crept toward its `go test -timeout`.
//
// Main runs a test binary inside a fresh bucket instead: every process the
// tests start inherits ORK_S3_BUCKET, and every in-process store built from
// artifacts.ConfigFromEnv reads the same bucket, so the binary sees only what
// it wrote itself. A bucket whose tests all passed is removed; one with a
// failure is kept, and its name printed, for the post-mortem.
package s3bucket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// EnvBucket is the variable every OrkStack binary and artifacts.ConfigFromEnv
// read the bucket name from.
const EnvBucket = "ORK_S3_BUCKET"

// envRequireInfra is the integration suites' "absence is a failure" switch
// (AGENTS.md §11.1).
const envRequireInfra = "ORK_REQUIRE_INFRA"

// labelPattern bounds the caller's label so Name always yields a valid S3
// bucket name: lowercase letters, digits and hyphens, at most 63 bytes.
var labelPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,19}$`)

// Name is the bucket a test binary labelled label, running as pid, uses:
// "orkstack-<label>-<pid>-<suffix>", where suffix separates two binaries that
// reuse a pid. It panics on a label that could not form a valid bucket name,
// which is a programming error in the caller's TestMain.
func Name(label string, pid int, suffix int64) string {
	if !labelPattern.MatchString(label) {
		panic(fmt.Sprintf("s3bucket: label %q must match %s", label, labelPattern))
	}
	if suffix < 0 {
		suffix = -suffix
	}
	return fmt.Sprintf("orkstack-%s-%d-%d", label, pid, suffix%1_000_000_000)
}

// Main is a TestMain body: it creates a bucket named for label, points
// ORK_S3_BUCKET at it for the rest of the process, runs the tests, and
// removes the bucket when every test passed. It returns m.Run's exit code.
//
// When the object store cannot be reached the tests run unchanged — each one
// that needs object storage probes it itself and skips — unless
// ORK_REQUIRE_INFRA is set, where the absence is the failure.
func Main(m *testing.M, label string) int {
	cfg := artifacts.ConfigFromEnv()
	bucket := Name(label, os.Getpid(), time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := create(ctx, cfg, bucket)
	cancel()
	if err != nil {
		if required, _ := strconv.ParseBool(os.Getenv(envRequireInfra)); required {
			fmt.Fprintf(os.Stderr, "s3bucket: create private bucket %s at %s: %v\n", bucket, cfg.S3Endpoint, err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "s3bucket: no private bucket (%v); tests that need object storage probe it themselves\n", err)
		return m.Run()
	}
	if err := os.Setenv(EnvBucket, bucket); err != nil {
		fmt.Fprintf(os.Stderr, "s3bucket: set %s: %v\n", EnvBucket, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "s3bucket: this test binary's object store is s3://%s (%s)\n", bucket, cfg.S3Endpoint)

	code := m.Run()
	if code != 0 {
		fmt.Fprintf(os.Stderr, "s3bucket: kept s3://%s for the failure's post-mortem\n", bucket)
		return code
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := Remove(ctx, client, bucket); err != nil {
		// The tests passed; a bucket left behind costs disk, never a later
		// run's time, because no later run reads it.
		fmt.Fprintf(os.Stderr, "s3bucket: remove s3://%s: %v\n", bucket, err)
	}
	return code
}

// Client is a raw S3 client for cfg's endpoint and credentials.
func Client(ctx context.Context, cfg artifacts.Config) (*s3.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	}), nil
}

func create(ctx context.Context, cfg artifacts.Config, bucket string) (*s3.Client, error) {
	client, err := Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return nil, fmt.Errorf("create bucket %s: %w", bucket, err)
	}
	return client, nil
}

// Remove deletes every object in bucket and then the bucket itself.
func Remove(ctx context.Context, client *s3.Client, bucket string) error {
	var (
		errs   []error
		failed int
	)
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list s3://%s: %w", bucket, err)
		}
		for _, object := range page.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key}); err != nil {
				failed++
				if len(errs) < 3 {
					errs = append(errs, fmt.Errorf("delete s3://%s/%s: %w", bucket, aws.ToString(object.Key), err))
				}
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d objects not deleted: %w", failed, errors.Join(errs...))
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return fmt.Errorf("delete bucket %s: %w", bucket, err)
	}
	return nil
}
