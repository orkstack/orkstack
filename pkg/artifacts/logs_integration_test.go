//go:build integration

// Integration coverage for the S3 task log store.
//
// Run against the repo's local infrastructure (`make infra-up`):
//
//	go test -tags=integration ./pkg/artifacts/... -count=1
//
// The tests skip (never fail) when MinIO is unreachable, unless
// ORK_REQUIRE_INFRA is set — see infraAbsent in infra_required_test.go.
package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The local store and the S3 store must answer identically, because a
// developer proves a behavior against the local one and a deployment runs the
// other. This is the S3 half of TestLocalLogStore_ReadsChunksInOrderAndResumes…
// against real MinIO: ordering, resume and the limit, over a listing that is
// genuinely paginated by the server rather than by a sorted slice.
func TestIntegration_S3LogStore_OrdersAndResumesOverARealListing(t *testing.T) {
	store, cfg := newS3LogStore(t)
	ctx := context.Background()
	ref := artifacts.LogRef{
		RunID:   fmt.Sprintf("ork-logs-%s", uniqueToken(t)),
		TaskID:  "emit",
		Attempt: 1,
	}
	cleanupLogChunks(t, cfg, ref)

	// More than a handful, so the sequence crosses a digit boundary: unpadded
	// keys would put 10 before 2 and this is where that shows.
	const chunks = 25
	for seq := int64(1); seq <= chunks; seq++ {
		if err := store.PutLogChunk(ctx, artifacts.LogChunk{
			LogRef: ref, Seq: seq, Data: []byte(fmt.Sprintf("line %d\n", seq)),
		}); err != nil {
			t.Fatalf("PutLogChunk(%d): %v", seq, err)
		}
	}

	all, err := store.ReadLogChunks(ctx, ref, 0, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks: %v", err)
	}
	if len(all) != chunks {
		t.Fatalf("read %d chunks from MinIO, want %d", len(all), chunks)
	}
	for i, chunk := range all {
		if want := int64(i + 1); chunk.Seq != want {
			t.Fatalf("chunk %d has seq %d, want %d: the bucket listing is not in sequence order",
				i, chunk.Seq, want)
		}
		if want := fmt.Sprintf("line %d\n", chunk.Seq); string(chunk.Data) != want {
			t.Errorf("chunk %d carries %q, want %q", i, chunk.Data, want)
		}
	}

	resumed, err := store.ReadLogChunks(ctx, ref, 20, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks(after 20): %v", err)
	}
	if len(resumed) != 5 || resumed[0].Seq != 21 {
		t.Fatalf("resume after 20 read %d chunks starting at %d, want 5 starting at 21",
			len(resumed), resumed[0].Seq)
	}
}

// A truncation notice must survive the round trip through a real bucket: the
// flag lives in the key's suffix, so this is also a check that MinIO's listing
// hands the suffix back unchanged.
func TestIntegration_S3LogStore_TruncationNoticeIsVisibleAndResumesPastIt(t *testing.T) {
	store, cfg := newS3LogStore(t)
	ctx := context.Background()
	ref := artifacts.LogRef{
		RunID:   fmt.Sprintf("ork-trunc-%s", uniqueToken(t)),
		TaskID:  "chatty",
		Attempt: 2,
	}
	cleanupLogChunks(t, cfg, ref)

	if err := store.PutLogChunk(ctx, artifacts.LogChunk{
		LogRef: ref, Seq: 1, Data: []byte("kept\n"),
	}); err != nil {
		t.Fatalf("PutLogChunk: %v", err)
	}
	if err := store.PutLogChunk(ctx, artifacts.LogChunk{
		LogRef: ref, Seq: 2, Data: []byte("[ork] log truncated at cap\n"), Truncated: true,
	}); err != nil {
		t.Fatalf("PutLogChunk(notice): %v", err)
	}

	chunks, err := store.ReadLogChunks(ctx, ref, 0, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks: %v", err)
	}
	if len(chunks) != 2 || !chunks[1].Truncated {
		t.Fatalf("read %d chunks with truncated=%v on the last; want 2 with the notice marked",
			len(chunks), len(chunks) > 0 && chunks[len(chunks)-1].Truncated)
	}

	// Resuming after the notice's own sequence must return nothing: the
	// ceiling key is the notice spelling, so the same object cannot come back.
	after, err := store.ReadLogChunks(ctx, ref, 2, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks(after the notice): %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("resume after the truncation notice replayed %d chunks", len(after))
	}
}

func TestIntegration_S3LogStore_LatestAttemptScansKeysOnly(t *testing.T) {
	store, cfg := newS3LogStore(t)
	ctx := context.Background()
	runID := fmt.Sprintf("ork-attempts-%s", uniqueToken(t))

	if _, err := store.LatestLogAttempt(ctx, runID, "never-ran"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("LatestLogAttempt with no logs = %v, want ErrNotFound", err)
	}
	for _, attempt := range []int32{1, 2, 11} {
		ref := artifacts.LogRef{RunID: runID, TaskID: "flaky", Attempt: attempt}
		cleanupLogChunks(t, cfg, ref)
		if err := store.PutLogChunk(ctx, artifacts.LogChunk{
			LogRef: ref, Seq: 1, Data: []byte("try\n"),
		}); err != nil {
			t.Fatalf("PutLogChunk(attempt %d): %v", attempt, err)
		}
	}
	latest, err := store.LatestLogAttempt(ctx, runID, "flaky")
	if err != nil {
		t.Fatalf("LatestLogAttempt: %v", err)
	}
	if latest != 11 {
		t.Errorf("LatestLogAttempt = %d, want 11", latest)
	}
}

func newS3LogStore(t *testing.T) (*artifacts.S3LogStore, artifacts.Config) {
	t.Helper()
	cfg := s3TestConfig()
	requireMinIO(t, cfg)

	store, err := artifacts.NewS3LogStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3LogStore: %v", err)
	}
	return store, cfg
}

// cleanupLogChunks removes an attempt's objects so repeated runs leave the
// shared dev bucket as they found it.
func cleanupLogChunks(t *testing.T, cfg artifacts.Config, ref artifacts.LogRef) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		client := newRawS3Client(t, cfg)
		prefix, err := artifacts.LogAttemptPrefix(ref)
		if err != nil {
			t.Fatalf("LogAttemptPrefix: %v", err)
		}
		for _, key := range listKeys(t, ctx, client, cfg.S3Bucket, prefix) {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(cfg.S3Bucket),
				Key:    aws.String(key),
			}); err != nil {
				t.Logf("cleanup %s: %v", key, err)
			}
		}
	})
}
