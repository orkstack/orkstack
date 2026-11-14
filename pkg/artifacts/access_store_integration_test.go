//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// privateIdentityBucket gives a test a bucket of its own: the access
// document and the mode record are deployment-wide objects at fixed keys.
func privateIdentityBucket(t *testing.T) artifacts.Config {
	t.Helper()
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	cfg.S3Bucket = "orkstack-identity-" + strings.ToLower(uniqueToken(t))
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		t.Fatalf("create private bucket %s: %v", cfg.S3Bucket, err)
	}
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, "")
		_, _ = raw.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(cfg.S3Bucket)})
	})
	return cfg
}

// TestAccessStore_S3SwapLossRetryAndRevalidate runs the access document's
// write rules against the real store: a revision exists before the head
// moves, a stale etag loses the swap, a fresh read wins it, and a
// conditional get on the current etag answers "unchanged".
func TestAccessStore_S3SwapLossRetryAndRevalidate(t *testing.T) {
	cfg := privateIdentityBucket(t)
	ctx := context.Background()
	store, err := artifacts.NewS3AccessStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get before any write = %v, want ErrNotFound", err)
	}
	at := time.Now()
	base, err := store.Write(ctx, accessDoc(adminBinding("group:corp:sre")), nil, artifacts.AccessWrite{At: at, By: "user:corp:a", ByVerified: true})
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	if ok, err := store.HasRevision(ctx, base.Meta); err != nil || !ok {
		t.Fatalf("revision 1 exists = %v, %v", ok, err)
	}
	if _, err := store.Write(ctx, accessDoc(adminBinding("group:corp:other")), nil, artifacts.AccessWrite{At: at}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a second first write = %v, want ErrPreconditionFailed", err)
	}
	if _, changed, err := store.Revalidate(ctx, base.ETag); err != nil || changed {
		t.Fatalf("Revalidate on the current etag = changed %v, %v; want unchanged", changed, err)
	}
	winner, err := store.Write(ctx, accessDoc(adminBinding("group:corp:ops")), base, artifacts.AccessWrite{At: at})
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if _, err := store.Write(ctx, accessDoc(adminBinding("group:corp:lost")), base, artifacts.AccessWrite{At: at}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a write on a stale etag = %v, want ErrPreconditionFailed", err)
	}
	head, changed, err := store.Revalidate(ctx, base.ETag)
	if err != nil || !changed || head.Meta.SHA256 != winner.Meta.SHA256 || head.Meta.PreviousSHA256 != base.Meta.SHA256 {
		t.Fatalf("Revalidate after the swap = %+v, %v, %v; want the winner linked to revision 1", head, changed, err)
	}
	if _, err := store.Write(ctx, accessDoc(adminBinding("group:corp:lost")), head, artifacts.AccessWrite{At: at}); err != nil {
		t.Fatalf("the retry from a fresh read: %v", err)
	}
	rev, err := store.GetRevision(ctx, 2, winner.Meta.SHA256)
	if err != nil || rev.Meta.PreviousSHA256 != base.Meta.SHA256 {
		t.Fatalf("revision 2 = %+v, %v", rev, err)
	}
	t.Logf("access document %s: three revisions written, one orphan, the chain followed from the head", store.Location())
}

// TestModeStore_S3CompareAndSwap holds the mode record's first write and
// its swap to the real store's conditions.
func TestModeStore_S3CompareAndSwap(t *testing.T) {
	cfg := privateIdentityBucket(t)
	ctx := context.Background()
	store, err := artifacts.NewS3ModeStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(ctx); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get before any write = %v, want ErrNotFound", err)
	}
	now := time.Now().UTC()
	etag, err := store.Put(ctx, artifacts.ModeRecord{Mode: artifacts.AuthModeObserve, RaisedAt: now, UpdatedAt: now}, artifacts.PutConditions{IfAbsent: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, artifacts.ModeRecord{Mode: artifacts.AuthModeEnforce, UpdatedAt: now}, artifacts.PutConditions{IfMatch: `"stale"`}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a swap on a stale etag = %v, want ErrPreconditionFailed", err)
	}
	if _, err := store.Put(ctx, artifacts.ModeRecord{Mode: artifacts.AuthModeEnforce, RaisedAt: now, UpdatedAt: now}, artifacts.PutConditions{IfMatch: etag}); err != nil {
		t.Fatalf("a swap on the read etag: %v", err)
	}
	rec, _, err := store.Get(ctx)
	if err != nil || rec.Mode != artifacts.AuthModeEnforce {
		t.Fatalf("Get = %+v, %v", rec, err)
	}
}
