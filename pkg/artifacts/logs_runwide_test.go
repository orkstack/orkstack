package artifacts

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// F-ui-perf-1: a run's attempt high-water marks come from one listing, not a
// listing of every chunk of every scope; and one scope's comes from a
// listing delimited at the attempt level. Both agree with what the full,
// key-by-key listing answers.

// logLayout writes a run's logs: a task with three attempts (the last one
// long), a task with one, a fan-out whose items reached different attempts,
// an item-only fan-out task (no chunks of its own), a foreign object, and
// another run's chunks that must never be counted.
func logLayout(t *testing.T, store LogStore, runID string, longAttempt int) map[LogScope]int32 {
	t.Helper()
	ctx := context.Background()
	put := func(taskID string, item Item, attempt int32, chunks int) {
		for seq := 1; seq <= chunks; seq++ {
			if err := store.PutLogChunk(ctx, LogChunk{
				LogRef: LogRef{RunID: runID, TaskID: taskID, Item: item, Attempt: attempt},
				Seq:    int64(seq),
				Data:   []byte(fmt.Sprintf("line %d\n", seq)),
			}); err != nil {
				t.Fatalf("put chunk: %v", err)
			}
		}
	}
	put("build", NoItem, 1, 2)
	put("build", NoItem, 2, 1)
	put("build", NoItem, 3, longAttempt)
	put("deploy", NoItem, 1, 1)
	put("convert", ItemAt(0), 1, 1)
	put("convert", ItemAt(1), 1, 3)
	put("convert", ItemAt(1), 2, 2)
	put("convert", ItemAt(7), 4, 1)
	// Another run's logs share the bucket and are never counted.
	for seq := 1; seq <= 3; seq++ {
		if err := store.PutLogChunk(ctx, LogChunk{
			LogRef: LogRef{RunID: runID + "x", TaskID: "build", Attempt: 9}, Seq: int64(seq), Data: []byte("x"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return map[LogScope]int32{
		{TaskID: "build", Item: NoItem}:      3,
		{TaskID: "deploy", Item: NoItem}:     1,
		{TaskID: "convert", Item: ItemAt(0)}: 1,
		{TaskID: "convert", Item: ItemAt(1)}: 2,
		{TaskID: "convert", Item: ItemAt(7)}: 4,
	}
}

func s3LogStore(t *testing.T, d *s3Double) *S3LogStore {
	t.Helper()
	store, err := NewS3LogStore(context.Background(), d.config())
	if err != nil {
		t.Fatalf("NewS3LogStore: %v", err)
	}
	return store
}

func TestLatestLogAttempts_MatchesEveryScopeQuery(t *testing.T) {
	local, err := NewLocalLogStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	double := newS3Double(t)
	backends := map[string]interface {
		LogStore
		RunLogAttemptLister
	}{"local": local, "s3": s3LogStore(t, double)}
	for name, store := range backends {
		t.Run(name, func(t *testing.T) {
			want := logLayout(t, store, "ork-runwide-1", 5)
			got, err := store.LatestLogAttempts(context.Background(), "ork-runwide-1")
			if err != nil {
				t.Fatalf("LatestLogAttempts: %v", err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("LatestLogAttempts = %v, want %v", got, want)
			}
			// The scope-by-scope answer, for every scope, is the same.
			for scope, attempt := range want {
				one, err := store.LatestLogAttempt(context.Background(), "ork-runwide-1", scope.TaskID, scope.Item)
				if err != nil || one != attempt {
					t.Errorf("LatestLogAttempt(%v) = %d, %v; want %d", scope, one, err, attempt)
				}
			}
			// A fan-out's own scope has no chunks: not found, never an item's.
			if _, err := store.LatestLogAttempt(context.Background(), "ork-runwide-1", "convert", NoItem); !errors.Is(err, ErrNotFound) {
				t.Errorf("convert's own scope: err = %v, want ErrNotFound", err)
			}
			empty, err := store.LatestLogAttempts(context.Background(), "ork-no-logs")
			if err != nil || len(empty) != 0 {
				t.Errorf("a run with no logs = %v, %v; want empty", empty, err)
			}
		})
	}
}

// One listing for the run however many scopes it has; a long attempt does
// not page the listing chunk by chunk.
func TestS3LatestLogAttempts_ListsOncePerPageOfAttempts(t *testing.T) {
	double := newS3Double(t)
	store := s3LogStore(t, double)
	ctx := context.Background()
	const items = 1000
	for i := 0; i < items; i++ {
		if err := store.PutLogChunk(ctx, LogChunk{
			LogRef: LogRef{RunID: "ork-wide", TaskID: "convert", Item: ItemAt(int32(i)), Attempt: 1}, Seq: 1, Data: []byte("x"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	double.resetCounts()
	got, err := store.LatestLogAttempts(ctx, "ork-wide")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != items {
		t.Fatalf("scopes = %d, want %d", len(got), items)
	}
	if _, _, lists := double.counts(); lists > 2 {
		t.Errorf("a 1000-item run cost %d listings, want at most 2 (one page of 1000 keys, then the end)", lists)
	}

	// 2500 chunks in ONE attempt: the full listing is 3 pages; skipping past
	// the attempt the first page stopped in makes it 2 requests at most.
	for seq := 1; seq <= 2500; seq++ {
		if err := store.PutLogChunk(ctx, LogChunk{
			LogRef: LogRef{RunID: "ork-long", TaskID: "chatty", Attempt: 2}, Seq: int64(seq), Data: []byte("x"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PutLogChunk(ctx, LogChunk{LogRef: LogRef{RunID: "ork-long", TaskID: "quiet", Attempt: 1}, Seq: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	double.resetCounts()
	long, err := store.LatestLogAttempts(ctx, "ork-long")
	if err != nil {
		t.Fatal(err)
	}
	if long[LogScope{TaskID: "chatty", Item: NoItem}] != 2 || long[LogScope{TaskID: "quiet", Item: NoItem}] != 1 {
		t.Fatalf("got %v", long)
	}
	if _, _, lists := double.counts(); lists != 2 {
		t.Errorf("a 2500-chunk attempt cost %d listings, want 2", lists)
	}
}

// The per-scope read is delimited: the same answer for one request however
// many chunks the attempts hold.
func TestS3LatestLogAttempt_DelimitedListingMatchesTheFullOne(t *testing.T) {
	double := newS3Double(t)
	store := s3LogStore(t, double)
	want := logLayout(t, store, "ork-delim", 2500)
	double.resetCounts()
	got, err := store.LatestLogAttempt(context.Background(), "ork-delim", "build", NoItem)
	if err != nil || got != want[LogScope{TaskID: "build", Item: NoItem}] {
		t.Fatalf("LatestLogAttempt = %d, %v; want 3", got, err)
	}
	if _, _, lists := double.counts(); lists != 1 {
		t.Errorf("a scope with a 2500-chunk attempt cost %d listings, want 1", lists)
	}
	// An item scope is delimited the same way.
	if got, err := store.LatestLogAttempt(context.Background(), "ork-delim", "convert", ItemAt(1)); err != nil || got != 2 {
		t.Errorf("item 1 = %d, %v; want 2", got, err)
	}
}
