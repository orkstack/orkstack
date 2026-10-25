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
//
// Removing a bucket is the object store's heaviest per-bucket operation: it
// grows and retires volumes, and at a higher PROOF_JOBS that churn stalled
// other processes' engine boots. A proof therefore sets EnvLeftFile, and a
// process whose tests passed names its bucket in that file instead of removing
// it; test/tools/proofrun removes the named buckets once the integration phase
// has ended (Finish).
package s3bucket

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/orkstack/orkstack/internal/testsupport/bootwait"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// EnvBucket is the variable every OrkStack binary and artifacts.ConfigFromEnv
// read the bucket name from.
const EnvBucket = "ORK_S3_BUCKET"

// EnvLeftFile names a file a test process writes its bucket's name to when its
// tests passed, instead of removing the bucket (Finish). The proof runner sets
// it, one file per test process, and removes the buckets those files name after
// the integration phase. Without it a process removes its own bucket, as it
// always did.
const EnvLeftFile = "ORK_TEST_BUCKET_LEFT_FILE"

// envOwned names the bucket a test process created for itself. A helper
// process it starts from its own binary inherits both variables, and runs in
// the parent's bucket instead of creating one it could never remove.
const envOwned = "ORK_TEST_OWNED_BUCKET"

// envRequireInfra is the integration suites' "absence is a failure" switch
// (AGENTS.md §11.1).
const envRequireInfra = "ORK_REQUIRE_INFRA"

// createTimeout bounds the bucket create. A create has been measured stalling
// with the rest of the store's per-bucket work at a higher PROOF_JOBS; a
// TestMain that gives up at 30 s fails every test of its package, so the bound
// is well past the 50 s stall seen on an engine boot's first read.
const createTimeout = 120 * time.Second

// labelPattern bounds the caller's label so Name always yields a valid S3
// bucket name: lowercase letters, digits and hyphens, at most 63 bytes.
var labelPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,19}$`)

// namePattern is the shape of every name Name yields.
var namePattern = regexp.MustCompile(`^orkstack-[a-z][a-z0-9-]{0,19}-[0-9]+-[0-9]+$`)

// IsName reports whether bucket has the shape of a name Name yields, which no
// shared bucket (the development stack's orkstack-artifacts, a deployment's)
// has. A sweeper that reads a bucket name from a file checks it first, so a
// file can name only a test process's own bucket.
func IsName(bucket string) bool { return namePattern.MatchString(bucket) }

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
	// A listing (`-test.list`, which test/tools/proofrun runs before it
	// plans) runs no test: it needs no bucket, and must not need MinIO.
	if !flag.Parsed() {
		flag.Parse()
	}
	if f := flag.Lookup("test.list"); f != nil && f.Value.String() != "" {
		return m.Run()
	}
	// A test binary a test re-executes as its own helper process inherits
	// its parent's bucket: it runs inside it, and neither creates a bucket
	// of its own nor removes the parent's.
	if inherited := os.Getenv(envOwned); inherited != "" && inherited == os.Getenv(EnvBucket) {
		return m.Run()
	}
	cfg := artifacts.ConfigFromEnv()
	bucket := Name(label, os.Getpid(), time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), createTimeout)
	createStart := time.Now()
	client, err := create(ctx, cfg, bucket)
	createTook := time.Since(createStart)
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
	if err := os.Setenv(envOwned, bucket); err != nil {
		fmt.Fprintf(os.Stderr, "s3bucket: set %s: %v\n", envOwned, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "s3bucket: this test binary's object store is s3://%s (%s), created in %s\n",
		bucket, cfg.S3Endpoint, createTook.Round(time.Millisecond))
	if createTook > bootwait.WarnAfter {
		// The same wording as bootwait's, which `proofrun overhead` reads: a
		// slow boot is recorded where the proof's logs show it.
		fmt.Fprintf(os.Stderr, "s3bucket: WARN: creating s3://%s took %s (over the %s warning threshold); a slow boot is recorded here, not absorbed\n",
			bucket, createTook.Round(time.Millisecond), bootwait.WarnAfter)
	}

	code := m.Run()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	Finish(ctx, code, bucket, os.Getenv(EnvLeftFile), func(ctx context.Context) error {
		return Remove(ctx, client, bucket)
	}, os.Stderr)
	return code
}

// Finish ends a test process's bucket once its tests have run, given the exit
// code m.Run returned:
//
//   - a failure keeps the bucket, for the post-mortem, and does nothing else;
//   - a pass with leftFile set writes the bucket's name to that file and leaves
//     the bucket to whoever sweeps it (test/tools/proofrun, after the
//     integration phase). If the file cannot be written nothing would ever
//     remove the bucket, so it is removed here, as without a sweeper;
//   - a pass with no leftFile removes the bucket by calling remove.
//
// A removal that fails costs disk, never a later run's time, because no later
// run reads the bucket: it is reported on stderr and is not an error.
func Finish(ctx context.Context, code int, bucket, leftFile string, remove func(context.Context) error, stderr io.Writer) {
	if code != 0 {
		fmt.Fprintf(stderr, "s3bucket: kept s3://%s for the failure's post-mortem\n", bucket)
		return
	}
	if leftFile != "" {
		err := os.WriteFile(leftFile, []byte(bucket+"\n"), 0o644)
		if err == nil {
			fmt.Fprintf(stderr, "s3bucket: left s3://%s for removal after the run (%s)\n", bucket, leftFile)
			return
		}
		fmt.Fprintf(stderr, "s3bucket: cannot hand s3://%s to its sweeper (%v); removing it here\n", bucket, err)
	}
	if err := remove(ctx); err != nil {
		fmt.Fprintf(stderr, "s3bucket: remove s3://%s: %v\n", bucket, err)
	}
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

// Remove deletes bucket and everything in it.
//
// It asks the store to delete the bucket first. A store that deletes a
// non-empty bucket whole (SeaweedFS can) does it in that one request, which is
// the cheapest removal there is; one that refuses with BucketNotEmpty (AWS S3
// does) gets the bucket emptied object by object and is then asked again. A
// bucket that is already gone is removed. Any other refusal is returned.
func Remove(ctx context.Context, client *s3.Client, bucket string) error {
	err := deleteBucket(ctx, client, bucket)
	if err == nil || errorCode(err) == "NoSuchBucket" {
		return nil
	}
	if errorCode(err) != "BucketNotEmpty" {
		return fmt.Errorf("delete bucket %s: %w", bucket, err)
	}
	if err := empty(ctx, client, bucket); err != nil {
		return err
	}
	if err := deleteBucket(ctx, client, bucket); err != nil && errorCode(err) != "NoSuchBucket" {
		return fmt.Errorf("delete bucket %s: %w", bucket, err)
	}
	return nil
}

func deleteBucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	return err
}

// errorCode is the S3 error code of err, or "" when err is not an S3 answer.
func errorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// empty deletes every object in bucket, one request each.
func empty(ctx context.Context, client *s3.Client, bucket string) error {
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
	return nil
}
