// Command bucket-ensure creates or removes the bucket ORK_S3_BUCKET names,
// against the S3-compatible endpoint ORK_S3_* names.
//
//	ORK_S3_BUCKET=orkstack-admin-<stamp> go run ./test/tools/bucket-ensure
//	ORK_BUCKET_ACTION=cleanup ORK_S3_BUCKET=orkstack-admin-<stamp> \
//		go run ./test/tools/bucket-ensure
//
// ORK_BUCKET_ACTION selects the mode: "ensure" (the default — create if
// absent, idempotently), "cleanup" (empty the bucket and delete it,
// idempotently against a bucket that is already gone) or "list" (print every
// bucket's name; ORK_S3_BUCKET is not needed).
//
// WHY THIS EXISTS. `deploy/docker-compose.yml`'s `minio-setup` container makes
// exactly one bucket, `orkstack-artifacts`, once, and every e2e spec in this
// module shares it — which is right for most of them, and wrong for the one
// assertion in ui/e2e/admin.spec.ts that needs a namespace registry with NOTHING in it
// yet ("default is undeletable when it is the only namespace a deployment
// has"). A shared bucket accumulates every earlier run's stamped namespaces
// forever, so by the second time this proof has ever run, "only default"
// cannot occur there again — not a flake, a permanent fact about the fixture.
//
// So that spec's engine and Orker run against a bucket of THIS RUN's
// own, created here and never reused across runs (the caller names it with a
// timestamp). CreateBucket is genuinely idempotent — BucketAlreadyOwnedByYou
// and BucketAlreadyExists are both treated as success — so a second call
// against the same name is a no-op. `ui/e2e/admin.spec.ts`'s own `afterAll`
// runs this same binary with ORK_BUCKET_ACTION=cleanup once the browser
// session that used the bucket has finished with it, so the bucket does not
// simply accumulate the way the shared one would have.
//
// It builds its own S3 client rather than importing pkg/artifacts, because
// bucket lifecycle (create, empty, delete) is not part of the Store contract
// that package exposes — Store is "read, write, list, delete OBJECTS in a
// bucket the deployment already has". This tool's construction mirrors
// pkg/artifacts.newS3Client exactly (same env vars, same path-style flag,
// same static-credentials rule) so what it proves reachable — and what it
// removes — is the bucket the engine and the Orker actually opened.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func main() {
	action := envOr("ORK_BUCKET_ACTION", "ensure")
	var err error
	switch action {
	case "ensure":
		err = ensure()
	case "cleanup":
		err = cleanup()
	case "list":
		err = list()
	default:
		err = fmt.Errorf("unknown ORK_BUCKET_ACTION %q: want \"ensure\", \"cleanup\" or \"list\"", action)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "bucket-ensure: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// newClient builds the S3 client this tool shares between ensure and
// cleanup, and returns the bucket name and endpoint it read from the
// environment alongside it — both actions need to name what they are
// talking to in their own output.
func newClient(ctx context.Context) (client *s3.Client, bucket, endpoint string, err error) {
	bucket = os.Getenv("ORK_S3_BUCKET")
	if bucket == "" {
		return nil, "", "", errors.New("ORK_S3_BUCKET is required: this tool never guesses a bucket")
	}
	client, endpoint, err = newBareClient(ctx)
	return client, bucket, endpoint, err
}

// newBareClient is the client without a bucket, for list.
func newBareClient(ctx context.Context) (client *s3.Client, endpoint string, err error) {
	region := envOr("ORK_S3_REGION", "us-east-1")
	endpoint = os.Getenv("ORK_S3_ENDPOINT")
	accessKey := os.Getenv("ORK_S3_ACCESS_KEY")
	secretKey := os.Getenv("ORK_S3_SECRET_KEY")

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	}
	if accessKey != "" && secretKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, os.Getenv("ORK_S3_SESSION_TOKEN")),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, "", fmt.Errorf("load aws config: %w", err)
	}
	client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = true
	})
	return client, endpoint, nil
}

// list prints every bucket's name, one per line: what scripts/proof-clean.sh
// reads to find the buckets failed test processes kept for their post-mortem.
func list() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, endpoint, err := newBareClient(ctx)
	if err != nil {
		return err
	}
	out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return fmt.Errorf("list buckets at %s: %w", endpoint, err)
	}
	for _, b := range out.Buckets {
		fmt.Println(aws.ToString(b.Name))
	}
	return nil
}

func ensure() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, bucket, endpoint, err := newClient(ctx)
	if err != nil {
		return err
	}

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		fmt.Printf("bucket-ensure: created %q at %s\n", bucket, endpoint)
		return nil
	}
	var alreadyOwned *types.BucketAlreadyOwnedByYou
	var alreadyExists *types.BucketAlreadyExists
	if errors.As(err, &alreadyOwned) || errors.As(err, &alreadyExists) {
		fmt.Printf("bucket-ensure: %q already exists\n", bucket)
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" ||
		apiErr.ErrorCode() == "BucketAlreadyExists") {
		// Some S3-compatible servers (MinIO included, on some versions) answer
		// with the error CODE but not the typed exception the SDK matches
		// above — caught here rather than trusted to the type switch alone.
		fmt.Printf("bucket-ensure: %q already exists\n", bucket)
		return nil
	}
	return fmt.Errorf("create bucket %q at %s: %w", bucket, endpoint, err)
}

// cleanup empties and removes the bucket this run created. S3 (and MinIO)
// refuse DeleteBucket on a non-empty bucket, so every object is listed and
// batch-deleted first, in pages, before the bucket itself goes — and both
// steps are idempotent against a bucket that is already gone, since the
// alternative (this tool erroring on a re-run, or on a run that never got
// far enough to create the bucket) would make every OTHER call site have to
// guard a call that promises to be a no-op.
func cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, bucket, endpoint, err := newClient(ctx)
	if err != nil {
		return err
	}

	removed := 0
	var continuationToken *string
	for {
		listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			if bucketAbsent(err) {
				fmt.Printf("bucket-ensure: %q is already gone\n", bucket)
				return nil
			}
			return fmt.Errorf("list objects in %q at %s: %w", bucket, endpoint, err)
		}
		if len(listed.Contents) > 0 {
			ids := make([]types.ObjectIdentifier, 0, len(listed.Contents))
			for _, obj := range listed.Contents {
				ids = append(ids, types.ObjectIdentifier{Key: obj.Key})
			}
			if _, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
			}); err != nil {
				return fmt.Errorf("delete %d object(s) from %q at %s: %w", len(ids), bucket, endpoint, err)
			}
			removed += len(ids)
		}
		if listed.IsTruncated == nil || !*listed.IsTruncated {
			break
		}
		continuationToken = listed.NextContinuationToken
	}

	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		if bucketAbsent(err) {
			fmt.Printf("bucket-ensure: %q is already gone (removed %d object(s) first)\n", bucket, removed)
			return nil
		}
		return fmt.Errorf("delete bucket %q at %s (removed %d object(s) first): %w",
			bucket, endpoint, removed, err)
	}
	fmt.Printf("bucket-ensure: removed %q at %s (%d object(s))\n", bucket, endpoint, removed)
	return nil
}

// bucketAbsent reports whether err is S3's own "no such bucket" — the one
// failure cleanup treats as success, since the state it wanted (nothing
// named `bucket` at `endpoint`) already holds.
func bucketAbsent(err error) bool {
	var noSuch *types.NoSuchBucket
	if errors.As(err, &noSuch) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchBucket", "NotFound":
			return true
		}
	}
	return false
}
