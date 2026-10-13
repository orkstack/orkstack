//go:build integration

package artifacts_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_S3WriteContinuationIsCreateIfAbsent proves a continuation
// object lands create-if-absent on MinIO: the same bytes again succeed on the
// same key, and a key already holding other bytes is refused and left as it
// is (spec 1.2).
func TestIntegration_S3WriteContinuationIsCreateIfAbsent(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	runID := "ork-1700000000-" + uniqueToken(t)
	data := []byte("gzip members of a checkpoint " + runID)
	first, err := store.WriteContinuation(ctx, "default", runID, 1, artifacts.ContinuationKindCheckpoint, data)
	if err != nil {
		t.Fatalf("first WriteContinuation: %v", err)
	}
	second, err := store.WriteContinuation(ctx, "default", runID, 1, artifacts.ContinuationKindCheckpoint, data)
	if err != nil {
		t.Fatalf("second WriteContinuation of the same bytes: %v", err)
	}
	if first != second {
		t.Fatalf("URIs differ: %q then %q", first, second)
	}
	sum := artifacts.ContinuationSHA256(data)
	if !artifacts.ContinuationURIMatches(first, "default", runID, 1, artifacts.ContinuationKindCheckpoint, sum) {
		t.Fatalf("URI %q does not name its own key", first)
	}
	t.Logf("continuation object %s", first)

	// Plant other bytes at the content key of a second object, then write it.
	planted := []byte("the real segment " + runID)
	key, err := artifacts.RunContinuationKey("default", runID, 0, artifacts.ContinuationKindEvents, artifacts.ContinuationSHA256(planted))
	if err != nil {
		t.Fatal(err)
	}
	raw := newRawS3Client(t, cfg)
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("tampered"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteContinuation(ctx, "default", runID, 0, artifacts.ContinuationKindEvents, planted); err == nil {
		t.Fatal("WriteContinuation accepted a key holding other bytes")
	} else {
		t.Logf("other bytes at the key refused: %v", err)
	}
	got, err := store.ReadSnapshot(ctx, "s3://"+cfg.S3Bucket+"/"+key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "tampered" {
		t.Fatalf("WriteContinuation overwrote the existing object: %q", got)
	}
}

// TestIntegration_S3ReadContinuationRange proves a ranged read returns the
// exact bytes, checks the stored size against the reference, and reports a
// missing object as ErrNotFound (spec 1.2).
func TestIntegration_S3ReadContinuationRange(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	runID := "ork-1700000000-" + uniqueToken(t)
	data := bytes.Repeat([]byte("0123456789"), 1000)
	uri, err := store.WriteContinuation(ctx, "default", runID, 2, artifacts.ContinuationKindSnapshot, data)
	if err != nil {
		t.Fatal(err)
	}
	size := int64(len(data))
	for _, r := range []struct{ off, length int64 }{{0, 1}, {0, size}, {4093, 4099}, {size - 7, 7}} {
		got, err := store.ReadContinuationRange(ctx, uri, size, r.off, r.length)
		if err != nil {
			t.Fatalf("ReadContinuationRange(%d, %d): %v", r.off, r.length, err)
		}
		if !bytes.Equal(got, data[r.off:r.off+r.length]) {
			t.Fatalf("ReadContinuationRange(%d, %d) returned other bytes", r.off, r.length)
		}
	}
	if _, err := store.ReadContinuationRange(ctx, uri, size+1, 0, 10); !errors.Is(err, artifacts.ErrContinuationSize) {
		t.Fatalf("larger recorded size: err = %v, want ErrContinuationSize", err)
	}
	if _, err := store.ReadContinuationRange(ctx, uri, size+100, size+10, 10); !errors.Is(err, artifacts.ErrContinuationSize) {
		t.Fatalf("a range past the stored end: err = %v, want ErrContinuationSize", err)
	}
	if _, err := store.ReadContinuationRange(ctx, uri, size-1, 0, 10); !errors.Is(err, artifacts.ErrContinuationSize) {
		t.Fatalf("smaller recorded size: err = %v, want ErrContinuationSize", err)
	}
	missing := strings.Replace(uri, artifacts.ContinuationSHA256(data), strings.Repeat("0", 64), 1)
	if _, err := store.ReadContinuationRange(ctx, missing, size, 0, 10); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("missing object: err = %v, want ErrNotFound", err)
	}
	t.Logf("ranged reads of %s (%d bytes) verified", uri, size)
}
