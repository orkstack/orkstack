package artifacts

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// countingRunReader counts the round trips ReadRunAttempts makes.
type countingRunReader struct {
	RunAttemptReader
	listings atomic.Int64
	reads    atomic.Int64
	failRef  *AttemptRef
	gone     *AttemptRef
}

func (c *countingRunReader) ListRunAttemptRefs(ctx context.Context, runID string) ([]AttemptRef, error) {
	c.listings.Add(1)
	return c.RunAttemptReader.ListRunAttemptRefs(ctx, runID)
}

func (c *countingRunReader) GetAttempt(ctx context.Context, ref AttemptRef) (*orkstackv1.TaskAttempt, error) {
	c.reads.Add(1)
	if c.failRef != nil && *c.failRef == ref {
		return nil, errors.New("store unavailable")
	}
	if c.gone != nil && *c.gone == ref {
		return nil, ErrNotFound
	}
	return c.RunAttemptReader.GetAttempt(ctx, ref)
}

func itemAttempt(runID, taskID string, item int32, attempt int32) *orkstackv1.TaskAttempt {
	record := sampleAttempt(runID, taskID, attempt)
	record.Item = proto.Int32(item)
	return record
}

// A run with a 1000-item fan-out is ONE listing, not one per item, and every
// scope reads exactly what one ListAttempts of that scope reads.
func TestReadRunAttempts_OneListingAnswersEveryScopeLikeListAttempts(t *testing.T) {
	ctx := context.Background()
	for name, store := range runWideStores(t) {
		t.Run(name, func(t *testing.T) {
			const run = "ork-5-1"
			records := []*orkstackv1.TaskAttempt{
				sampleAttempt(run, "build", 2), sampleAttempt(run, "build", 1),
				sampleAttempt(run, "fan", 1), sampleAttempt("ork-5-2", "build", 1),
			}
			for i := int32(0); i < 1000; i++ {
				records = append(records, itemAttempt(run, "fan", i, 1))
				if i%7 == 0 {
					records = append(records, itemAttempt(run, "fan", i, 2))
				}
			}
			for _, record := range records {
				if err := store.PutAttempt(ctx, record); err != nil {
					t.Fatalf("PutAttempt: %v", err)
				}
			}
			counting := &countingRunReader{RunAttemptReader: store}
			grouped, err := ReadRunAttempts(ctx, counting, run, 8)
			if err != nil {
				t.Fatalf("ReadRunAttempts: %v", err)
			}
			if got := counting.listings.Load(); got != 1 {
				t.Fatalf("listings = %d, want 1", got)
			}
			scopes := []AttemptScope{{TaskID: "build"}, {TaskID: "fan"}}
			for i := int32(0); i < 1000; i++ {
				scopes = append(scopes, AttemptScope{TaskID: "fan", Item: ItemAt(i)})
			}
			if len(grouped) != len(scopes) {
				t.Fatalf("scopes = %d, want %d", len(grouped), len(scopes))
			}
			for _, scope := range scopes {
				want, err := store.ListAttempts(ctx, run, scope.TaskID, scope.Item)
				if err != nil {
					t.Fatalf("ListAttempts: %v", err)
				}
				got := grouped[scope]
				if len(got) != len(want) {
					t.Fatalf("scope %+v: %d records, ListAttempts has %d", scope, len(got), len(want))
				}
				for i := range want {
					if !proto.Equal(got[i], want[i]) {
						t.Fatalf("scope %+v record %d differs:\n got %v\nwant %v", scope, i, got[i], want[i])
					}
				}
			}
		})
	}
}

func TestReadRunAttempts_AGoneRecordIsMissingAndAnErrorFailsTheRun(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryAttemptStore()
	const run = "ork-6-1"
	for _, record := range []*orkstackv1.TaskAttempt{
		sampleAttempt(run, "a", 1), sampleAttempt(run, "a", 2), sampleAttempt(run, "b", 1),
	} {
		if err := store.PutAttempt(ctx, record); err != nil {
			t.Fatalf("PutAttempt: %v", err)
		}
	}
	gone := AttemptRef{RunID: run, TaskID: "a", Attempt: 2}
	grouped, err := ReadRunAttempts(ctx, &countingRunReader{RunAttemptReader: store, gone: &gone}, run, 2)
	if err != nil {
		t.Fatalf("ReadRunAttempts: %v", err)
	}
	if got := len(grouped[AttemptScope{TaskID: "a"}]); got != 1 {
		t.Fatalf("task a = %d records, want 1 (the reclaimed one reads as missing)", got)
	}

	failing := AttemptRef{RunID: run, TaskID: "b", Attempt: 1}
	if _, err := ReadRunAttempts(ctx, &countingRunReader{RunAttemptReader: store, failRef: &failing}, run, 2); err == nil {
		t.Fatal("a failed record read answered; want the error")
	}

	empty, err := ReadRunAttempts(ctx, store, "ork-6-9", 0)
	if err != nil || len(empty) != 0 {
		t.Fatalf("a run with no records = %v, %v; want empty and no error", empty, err)
	}
}
