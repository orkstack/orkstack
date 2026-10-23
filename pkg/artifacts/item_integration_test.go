//go:build integration

// Integration coverage for fan-out item keys on the S3 backends
// (docs/STORAGE.md "Key layout"), against real MinIO: a listing genuinely produced by the
// server, where an item's records sit one segment below its task's.
package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestS3AttemptStore_ItemScopes(t *testing.T) {
	store, cfg := newS3AttemptStore(t)
	ctx := context.Background()
	runID := fmt.Sprintf("ork-items-%s", uniqueToken(t))
	cleanupAttemptRecords(t, cfg, runID, "convert")

	put := func(item artifacts.Item, attempt int32) {
		t.Helper()
		if err := store.PutAttempt(ctx, &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion,
			RunId:         runID, TaskId: "convert", Attempt: attempt,
			Item:    item.Proto(),
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		}); err != nil {
			t.Fatalf("PutAttempt: %v", err)
		}
	}
	put(artifacts.ItemAt(0), 1)
	put(artifacts.ItemAt(3), 1)
	put(artifacts.ItemAt(3), 2)

	own, err := store.ListAttempts(ctx, runID, "convert", artifacts.NoItem)
	if err != nil || len(own) != 0 {
		t.Fatalf("NoItem on a fan-out = %d records (%v), want 0: the S3 listing of the task prefix includes item keys and must filter them", len(own), err)
	}
	three, err := store.ListAttempts(ctx, runID, "convert", artifacts.ItemAt(3))
	if err != nil || len(three) != 2 || three[0].GetAttempt() != 1 || three[1].GetAttempt() != 2 {
		t.Fatalf("ItemAt(3) = %v (%v), want attempts 1 and 2", three, err)
	}
	refs, err := store.ListRunAttemptRefs(ctx, runID)
	if err != nil || len(refs) != 3 {
		t.Fatalf("ListRunAttemptRefs = %+v (%v), want 3 item refs", refs, err)
	}
	for _, ref := range refs {
		if !ref.Item.Set {
			t.Fatalf("an item record was listed without its item: %+v", ref)
		}
	}
}

func TestS3LogStore_ItemScopedLatestAttempt(t *testing.T) {
	store, cfg := newS3LogStore(t)
	ctx := context.Background()
	runID := fmt.Sprintf("ork-item-logs-%s", uniqueToken(t))
	for _, c := range []struct {
		item    artifacts.Item
		attempt int32
	}{{artifacts.ItemAt(1), 1}, {artifacts.ItemAt(1), 2}, {artifacts.ItemAt(2), 1}} {
		ref := artifacts.LogRef{RunID: runID, TaskID: "convert", Item: c.item, Attempt: c.attempt}
		cleanupLogChunks(t, cfg, ref)
		if err := store.PutLogChunk(ctx, artifacts.LogChunk{LogRef: ref, Seq: 1, Data: []byte("x\n")}); err != nil {
			t.Fatalf("PutLogChunk: %v", err)
		}
	}
	if _, err := store.LatestLogAttempt(ctx, runID, "convert", artifacts.NoItem); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("NoItem on a fan-out = %v, want ErrNotFound", err)
	}
	if got, err := store.LatestLogAttempt(ctx, runID, "convert", artifacts.ItemAt(1)); err != nil || got != 2 {
		t.Fatalf("ItemAt(1) = %d, %v; want 2", got, err)
	}
	if got, err := store.LatestLogAttempt(ctx, runID, "convert", artifacts.ItemAt(2)); err != nil || got != 1 {
		t.Fatalf("ItemAt(2) = %d, %v; want 1", got, err)
	}
}

func TestS3CommitStore_ItemKeys(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3CommitStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := artifacts.CommitRoot(cfg)
	runID := "run-" + uniqueToken(t)
	for _, index := range []int32{0, 1} {
		key, err := artifacts.ItemCommitKey("default", runID, "convert", index)
		if err != nil {
			t.Fatal(err)
		}
		uri := strings.TrimSuffix(root, "/") + "/" + key
		if _, err := store.Create(ctx, uri, &orkstackv1.ActivityCommit{
			SchemaVersion: 1, Namespace: "default", RunId: runID, TaskId: "convert",
			IdempotencyKey: fmt.Sprintf("default/%s/convert/%d", runID, index),
		}); err != nil {
			t.Fatalf("Create item %d: %v", index, err)
		}
		// The second create on one item's key loses: each item has exactly
		// one commit, and siblings never contend for it.
		if _, err := store.Create(ctx, uri, &orkstackv1.ActivityCommit{SchemaVersion: 1}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
			t.Fatalf("second Create on item %d = %v, want ErrPreconditionFailed", index, err)
		}
		record, err := store.Get(ctx, uri)
		if err != nil || !strings.HasSuffix(record.Commit.GetIdempotencyKey(), fmt.Sprintf("/%d", index)) {
			t.Fatalf("Get item %d = %+v, %v", index, record, err)
		}
	}
}
