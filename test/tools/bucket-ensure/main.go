// Command bucket-ensure creates or removes the bucket ORK_S3_BUCKET names,
// against the S3-compatible endpoint ORK_S3_* names.
//
//	ORK_S3_BUCKET=orkstack-admin-<stamp> go run ./test/tools/bucket-ensure
//	ORK_BUCKET_ACTION=cleanup ORK_S3_BUCKET=orkstack-admin-<stamp> \
//		go run ./test/tools/bucket-ensure
//
// ORK_BUCKET_ACTION selects the mode: "ensure" (the default — create if
// absent, idempotently), "cleanup" (delete the bucket, emptying it first when
// the store refuses a non-empty one; idempotently against a bucket that is
// already gone), "list" (print every bucket's name; ORK_S3_BUCKET is not
// needed) or "leftovers" (print the test processes' buckets the store still
// holds, the ones scripts/proof-clean.sh lists, and remove those whose process
// is gone when ORK_BUCKET_REMOVE=1).
//
// WHY THIS EXISTS. The development stack makes exactly one bucket,
// `orkstack-artifacts`, once, and every e2e spec file used to share it. A
// shared bucket accumulates every earlier run's stamped namespaces, Orkflows
// and deployment-wide documents forever: an engine boot there costs time
// proportional to the bucket's whole history, two spec files running beside
// each other read each other's retention, image policy, audit and webhooks,
// and ui/e2e/admin.spec.ts's "default is undeletable when it is the only
// namespace a deployment has" cannot occur there a second time — not a flake,
// a permanent fact about the fixture.
//
// So every spec file's engine and Orker run against a bucket of THAT FILE's
// own (ui/e2e/harness.ts ownBucket, named like internal/testsupport/s3bucket's
// so scripts/proof-clean.sh recognises a leftover), created here before they
// boot and never reused across runs. CreateBucket is genuinely idempotent —
// BucketAlreadyOwnedByYou and BucketAlreadyExists are both treated as success
// — so a second call against the same name is a no-op. Each file's `afterAll`
// runs this same binary with ORK_BUCKET_ACTION=cleanup once the browser
// session that used the bucket has finished with it. The binary is built once
// per run (ui/e2e/global-setup.ts, a proof's binaries step) rather than
// `go run` per call.
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
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
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
	case "leftovers":
		err = leftovers()
	default:
		err = fmt.Errorf("unknown ORK_BUCKET_ACTION %q: want \"ensure\", \"cleanup\", \"list\" or \"leftovers\"", action)
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

// leftoverName is the shape of a test process's bucket
// (internal/testsupport/s3bucket.Name, and a spec file's own, ui/e2e/harness.ts
// ownBucket): orkstack-<label>-<pid>-<suffix>. Its pid is the process that
// made it.
var leftoverName = regexp.MustCompile(`^orkstack-[a-z][a-z0-9-]*-([0-9]+)-[0-9]+$`)

// recordedName finds a bucket name a proof's integration logs record: one a
// failing process kept, and one the sweep could not remove.
var recordedName = regexp.MustCompile(`(?:s3bucket: kept|bucket sweep: left) s3://([^\s]+)`)

// leftovers prints the buckets of test processes the store still holds, and
// removes each whose process is gone when ORK_BUCKET_REMOVE=1. It is
// scripts/proof-clean.sh's bucket section.
func leftovers() error {
	ctx := context.Background()
	lctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	client, endpoint, err := newBareClient(lctx)
	if err != nil {
		return err
	}
	return findLeftovers(ctx, client, endpoint, envOr("ORK_PROOF_ROOT", "proof"),
		os.Getenv("ORK_BUCKET_REMOVE") == "1", processAlive, os.Stdout)
}

// findLeftovers looks for leftover buckets in two places, because neither is
// enough alone: the store's own listing, and the names the proofs under root
// recorded (each process's integration/<job>.bucket hand-off file, which a
// stopped run's sweep leaves, and the "kept" and "left" lines of their logs).
// SeaweedFS 4.48 with its auth off answers ListBuckets with no bucket at all,
// so on the development store the recorded names are the whole of what is
// found. A recorded name the store no longer holds is skipped. A bucket whose
// process is alive is in use and left alone; with remove set, every other one
// is removed (cleanupBucket). A bucket it cannot remove is reported, never an
// error.
func findLeftovers(ctx context.Context, client *s3.Client, endpoint, root string, remove bool,
	alive func(pid int) bool, out io.Writer) error {
	held := map[string]bool{}
	lctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	listed, err := client.ListBuckets(lctx, &s3.ListBucketsInput{})
	cancel()
	if err != nil {
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			return fmt.Errorf("list buckets at %s: %w", endpoint, err)
		}
		fmt.Fprintf(out, "  (the store refused its bucket listing: %v; only the names proofs recorded are checked)\n", err)
	}
	if listed != nil {
		for _, b := range listed.Buckets {
			if name := aws.ToString(b.Name); leftoverName.MatchString(name) {
				held[name] = true
			}
		}
	}
	for _, name := range recordedNames(root) {
		if held[name] || !leftoverName.MatchString(name) {
			continue
		}
		hctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		_, err := client.HeadBucket(hctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
		cancel()
		switch {
		case err == nil:
			held[name] = true
		case !bucketAbsent(err):
			fmt.Fprintf(out, "  %s  (recorded by a proof; the store did not say whether it holds it: %v)\n", name, err)
		}
	}

	names := make([]string, 0, len(held))
	for name := range held {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pid, _ := strconv.Atoi(leftoverName.FindStringSubmatch(name)[1])
		if alive(pid) {
			fmt.Fprintf(out, "  %s  (in use: pid %d is alive; left alone)\n", name, pid)
			continue
		}
		fmt.Fprintf(out, "  %s\n", name)
		if !remove {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		if err := cleanupBucket(rctx, client, name, endpoint, io.Discard); err != nil {
			fmt.Fprintf(out, "    (not removed: %v)\n", err)
		}
		cancel()
	}
	return nil
}

// recordedNames are the bucket names the proofs under root recorded: the
// hand-off files a passing process wrote and a sweep has not removed, and the
// buckets their logs say were kept or left. A file that cannot be read
// records nothing.
func recordedNames(root string) []string {
	var names []string
	files, _ := filepath.Glob(filepath.Join(root, "*", "integration", "*.bucket"))
	for _, file := range files {
		if data, err := os.ReadFile(file); err == nil {
			names = append(names, strings.TrimSpace(string(data)))
		}
	}
	logs, _ := filepath.Glob(filepath.Join(root, "*", "integration", "*.log"))
	more, _ := filepath.Glob(filepath.Join(root, "*", "integration.log"))
	for _, file := range append(logs, more...) {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		for _, m := range recordedName.FindAllSubmatch(data, -1) {
			names = append(names, string(m[1]))
		}
	}
	return names
}

// processAlive reports whether a process with pid exists, whoever owns it. A
// reused pid keeps a leftover a little longer; it never removes a live one.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
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

// cleanup removes the bucket this run created.
func cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, bucket, endpoint, err := newClient(ctx)
	if err != nil {
		return err
	}
	return cleanupBucket(ctx, client, bucket, endpoint, os.Stdout)
}

// cleanupBucket removes bucket, idempotently against one that is already gone
// (the alternative — this tool erroring on a re-run, or on a run that never got
// far enough to create the bucket — would make every OTHER call site guard a
// call that promises to be a no-op).
//
// It asks the store to delete the bucket first. A store that deletes a
// non-empty bucket whole (SeaweedFS can) does it in that one request, the
// cheapest removal there is, and at a higher PROOF_JOBS the one that stalls
// other processes least. One that refuses with BucketNotEmpty (S3 does) gets
// every object listed and batch-deleted in pages, then the bucket again.
func cleanupBucket(ctx context.Context, client *s3.Client, bucket, endpoint string, out io.Writer) error {
	_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	switch {
	case err == nil:
		fmt.Fprintf(out, "bucket-ensure: removed %q at %s\n", bucket, endpoint)
		return nil
	case bucketAbsent(err):
		fmt.Fprintf(out, "bucket-ensure: %q is already gone\n", bucket)
		return nil
	case !bucketNotEmpty(err):
		return fmt.Errorf("delete bucket %q at %s: %w", bucket, endpoint, err)
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
				fmt.Fprintf(out, "bucket-ensure: %q is already gone\n", bucket)
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
			fmt.Fprintf(out, "bucket-ensure: %q is already gone (removed %d object(s) first)\n", bucket, removed)
			return nil
		}
		return fmt.Errorf("delete bucket %q at %s (removed %d object(s) first): %w",
			bucket, endpoint, removed, err)
	}
	fmt.Fprintf(out, "bucket-ensure: removed %q at %s (%d object(s))\n", bucket, endpoint, removed)
	return nil
}

// bucketNotEmpty reports whether err is S3's refusal to delete a bucket that
// still holds objects: the one failure that sends cleanup to empty the bucket
// itself.
func bucketNotEmpty(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "BucketNotEmpty"
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
