//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestImagePolicyStore_GetIsNotFoundUntilWrittenThenRoundTrips runs against
// real MinIO. The image policy is one deployment-wide document at a fixed
// key, so it lives in a bucket private to this test: a policy written into
// the shared bucket would refuse images in every other suite that reads it.
func TestImagePolicyStore_GetIsNotFoundUntilWrittenThenRoundTrips(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	cfg.S3Bucket = "orkstack-image-policy-" + strings.ToLower(uniqueToken(t))
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		t.Fatalf("create private bucket %s: %v", cfg.S3Bucket, err)
	}
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, "")
		_, _ = raw.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(cfg.S3Bucket)})
	})

	store, err := artifacts.NewS3ImagePolicyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3ImagePolicyStore: %v", err)
	}
	if _, err := store.Get(ctx); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get before any write = %v, want ErrNotFound", err)
	}
	first, err := store.Put(ctx, &orkstackv1.ImagePolicy{
		AllowedRegistries: []string{"ghcr.io/acme/", "localhost:5001/orkstack/"},
		SigningKeys:       map[string]string{"release": task.FixedSigningKeyPEM},
	}, artifacts.PutConditions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if strings.Join(got.GetAllowedRegistries(), " ") != "ghcr.io/acme/ localhost:5001/orkstack/" ||
		got.GetSigningKeys()["release"] != task.FixedSigningKeyPEM || got.GetUpdatedAt() == nil ||
		!got.GetUpdatedAt().AsTime().Equal(first.GetUpdatedAt().AsTime()) {
		t.Fatalf("Get = %v, want the two registries, the key and the stamped updated_at", got)
	}
	t.Logf("stored %s: %d registries, key release %s, updated_at %s", store.Location(),
		len(got.GetAllowedRegistries()), task.SigningKeyPEMFingerprint(got.GetSigningKeys()["release"]),
		got.GetUpdatedAt().AsTime())

	if _, err := store.Put(ctx, &orkstackv1.ImagePolicy{RequireDigest: true}, artifacts.PutConditions{}); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	replaced, err := store.Get(ctx)
	if err != nil {
		t.Fatalf("Get after replace: %v", err)
	}
	if !replaced.GetRequireDigest() || len(replaced.GetAllowedRegistries()) != 0 || len(replaced.GetSigningKeys()) != 0 {
		t.Fatalf("second Put did not replace whole: %v", replaced)
	}
	body := getObject(t, ctx, raw, cfg.S3Bucket, artifacts.ImagePolicyKey)
	if !strings.Contains(string(body), "\"requireDigest\": true") {
		t.Fatalf("stored document:\n%s", body)
	}
}
