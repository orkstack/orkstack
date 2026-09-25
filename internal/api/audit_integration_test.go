//go:build integration

package api_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestAudit_UnwritableStoreDoesNotFailWrite is the best-effort claim against
// real MinIO and a real Temporal namespace: the audit store points at a bucket
// that does not exist, UpdateRetentionPolicy still succeeds, the retention
// document in the (private, real) bucket changes, and the engine log carries
// the dropped record — never the caller's response.
//
// The retention policy is one deployment-wide document, so it lives in a
// bucket private to this test, and the Temporal namespace whose retention the
// controller syncs is private too: nothing another suite reads is touched.
func TestAudit_UnwritableStoreDoesNotFailWrite(t *testing.T) {
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireAuditMinIO(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	raw := auditS3Client(t, cfg)
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	policyCfg := cfg
	policyCfg.S3Bucket = "orkstack-audit-policy-" + stamp
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(policyCfg.S3Bucket)}); err != nil {
		t.Fatalf("create private policy bucket: %v", err)
	}
	t.Cleanup(func() { emptyAndDeleteBucket(raw, policyCfg.S3Bucket) })
	missingCfg := cfg
	missingCfg.S3Bucket = "orkstack-audit-missing-" + stamp

	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, policyCfg)
	if err != nil {
		t.Fatalf("NewS3RetentionPolicyStore: %v", err)
	}
	auditStore, err := artifacts.NewS3AuditStore(ctx, missingCfg)
	if err != nil {
		t.Fatalf("NewS3AuditStore: %v", err)
	}
	namespace := "ork-audit-" + stamp
	temporalClient := auditTemporalClient(ctx, t, namespace)
	reader := engine.NewTemporalRunReader(temporalClient)

	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(logs, os.Stderr), nil))
	counter := &fakeAuditCounter{}
	svc := api.NewService(api.Config{
		Namespaces:   newFakeRegistry(),
		Retention:    archive.NewRetentionController(policies, reader, namespace, 24*time.Hour, logger),
		AuditStore:   auditStore,
		AuditCounter: counter,
		Logger:       logger,
	})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	t.Cleanup(server.Close)
	system := orkstackv1connect.NewSystemServiceClient(server.Client(), server.URL)

	const want = 96 * time.Hour
	req := connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{MaxRunRetention: durationpb.New(want)})
	req.Header().Set(api.AuditPrincipalHeader, "proof-unwritable")
	resp, err := system.UpdateRetentionPolicy(ctx, req)
	if err != nil {
		t.Fatalf("UpdateRetentionPolicy with an unwritable audit store = %v, want success", err)
	}
	if got := resp.Msg.GetPolicy().GetMaxRunRetention().AsDuration(); got != want {
		t.Fatalf("response max_run_retention = %s, want %s", got, want)
	}
	stored, err := policies.Get(ctx)
	if err != nil || stored.GetMaxRunRetention().AsDuration() != want {
		t.Fatalf("stored retention document = %v, %v; want max_run_retention %s", stored, err, want)
	}
	logged := logs.String()
	if !strings.Contains(logged, "audit record dropped") || !strings.Contains(logged, "action=retention.update") ||
		!strings.Contains(logged, "level=WARN") {
		t.Fatalf("engine log does not carry the drop:\n%s", logged)
	}
	if got := counter.seen(); len(got) != 1 || got[0] != "retention.update/dropped" {
		t.Fatalf("counter = %v, want [retention.update/dropped]", got)
	}
	var dropLine string
	for _, line := range strings.Split(logged, "\n") {
		if strings.Contains(line, "audit record dropped") {
			dropLine = line
		}
	}
	transcript := fmt.Sprintf("# unwritable audit store (bucket %s does not exist)\n"+
		"UpdateRetentionPolicy(max_run_retention=%s) -> ok, response max_run_retention=%s\n"+
		"stored retention document (s3://%s/%s): max_run_retention=%s\n"+
		"engine log: %s\ncounter: %v\n",
		missingCfg.S3Bucket, want, resp.Msg.GetPolicy().GetMaxRunRetention().AsDuration(),
		policyCfg.S3Bucket, artifacts.RetentionPolicyKey, stored.GetMaxRunRetention().AsDuration(), dropLine, counter.seen())
	proof.Write(t, "api", "audit-unwritable-store.txt", []byte(transcript))
	t.Logf("EVIDENCE audit_unwritable_store update=ok stored_max_run_retention=%s counter=%v",
		stored.GetMaxRunRetention().AsDuration(), counter.seen())
	t.Logf("EVIDENCE audit_unwritable_store_log %s", dropLine)
}

func requireAuditMinIO(t *testing.T, cfg artifacts.Config) {
	t.Helper()
	base, err := url.Parse(cfg.S3Endpoint)
	if err != nil {
		infraAbsent(t, "MinIO endpoint %q is unusable: %v", cfg.S3Endpoint, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.JoinPath("/minio/health/live").String(), nil)
	if err != nil {
		infraAbsent(t, "MinIO health request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		infraAbsent(t, "MinIO not reachable at %s (%v); run `make infra-up`", cfg.S3Endpoint, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		infraAbsent(t, "MinIO health returned %d; run `make infra-up`", resp.StatusCode)
	}
}

func auditTemporalClient(ctx context.Context, t *testing.T, namespace string) client.Client {
	t.Helper()
	address := os.Getenv("ORK_TEMPORAL_ADDRESS")
	if address == "" {
		address = client.DefaultHostPort
	}
	bootstrap, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: client.DefaultNamespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	if err := task.EnsureNamespace(ctx, bootstrap, namespace); err != nil {
		bootstrap.Close()
		t.Fatalf("provision namespace %q: %v", namespace, err)
	}
	bootstrap.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
		if err == nil {
			t.Cleanup(c.Close)
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %q did not become usable: %v", namespace, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func auditS3Client(t *testing.T, cfg artifacts.Config) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
}

func emptyAndDeleteBucket(raw *s3.Client, bucket string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err != nil || len(page.Contents) == 0 {
			break
		}
		for _, object := range page.Contents {
			_, _ = raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key})
		}
	}
	_, _ = raw.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
