package api_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// F-ui-perf-1: ListTaskAttempts with no task — the run page's seed, `run get
// --attempts`, `run timing` — reads a whole run's records with ONE listing
// and its log attempts with ONE more, where it used to list records and every
// log chunk per task and per fan-out item. Its answer is exactly the
// scope-by-scope one: the same rows, the same UNREPORTED gaps, the same
// log-only attempts, in the same order.

// countingAttempts counts the reads a timeline costs.
type countingAttempts struct {
	*artifacts.MemoryAttemptStore
	mu                         sync.Mutex
	runListings, gets, listing int
}

func (c *countingAttempts) ListRunAttemptRefs(ctx context.Context, runID string) ([]artifacts.AttemptRef, error) {
	c.mu.Lock()
	c.runListings++
	c.mu.Unlock()
	return c.MemoryAttemptStore.ListRunAttemptRefs(ctx, runID)
}

func (c *countingAttempts) GetAttempt(ctx context.Context, ref artifacts.AttemptRef) (*orkstackv1.TaskAttempt, error) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.MemoryAttemptStore.GetAttempt(ctx, ref)
}

func (c *countingAttempts) ListAttempts(ctx context.Context, runID, taskID string, item artifacts.Item) ([]*orkstackv1.TaskAttempt, error) {
	c.mu.Lock()
	c.listing++
	c.mu.Unlock()
	return c.MemoryAttemptStore.ListAttempts(ctx, runID, taskID, item)
}

// scopedAttempts is the same store seen through AttemptStore alone: no
// run-wide read, so the Service takes the scope-by-scope path.
type scopedAttempts struct{ artifacts.AttemptStore }

// countingLogs counts the log reads.
type countingLogs struct {
	*memoryLogStore
	mu           sync.Mutex
	runWide, one int
}

func (c *countingLogs) LatestLogAttempts(ctx context.Context, runID string) (map[artifacts.LogScope]int32, error) {
	c.mu.Lock()
	c.runWide++
	c.mu.Unlock()
	return c.memoryLogStore.LatestLogAttempts(ctx, runID)
}

func (c *countingLogs) LatestLogAttempt(ctx context.Context, runID, taskID string, item artifacts.Item) (int32, error) {
	c.mu.Lock()
	c.one++
	c.mu.Unlock()
	return c.memoryLogStore.LatestLogAttempt(ctx, runID, taskID, item)
}

// scopedLogs hides the run-wide read.
type scopedLogs struct{ artifacts.LogStore }

// fanOutRunWith is a run of a plain retried task, a fan-out of n items, and
// a task that never dispatched.
func fanOutRunWith(runID string, n int) *orkstackv1.Run {
	at := time.Date(2026, 9, 9, 17, 0, 0, 0, time.UTC)
	items := make([]*orkstackv1.TaskRun, n)
	for i := range items {
		items[i] = &orkstackv1.TaskRun{TaskId: "convert", Item: proto.Int32(int32(i)), Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1}
	}
	return &orkstackv1.Run{
		Id: runID, OrkflowName: "fan", OrkflowVersion: "0.1.0", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "list", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 3},
			{TaskId: "convert", Status: orkstackv1.Status_STATUS_SUCCEEDED, Each: &orkstackv1.TaskEach{Expanded: true, Count: int32(n)}, Items: items},
			{TaskId: "never", Status: orkstackv1.Status_STATUS_CANCELED},
		},
	}
}

func itemRecord(runID string, item int32, attempt int32, outcome orkstackv1.AttemptOutcome) *orkstackv1.TaskAttempt {
	record := attemptRecord(runID, "convert", attempt, outcome)
	record.Item = proto.Int32(item)
	return record
}

// seedRunWide writes the mix the two paths must agree on: a task with a
// missing middle record (an UNREPORTED gap), an item whose logs reached an
// attempt with no record (a log-only attempt), items out of write order,
// and records of another run.
func seedRunWide(t *testing.T, attempts artifacts.AttemptStore, logs *memoryLogStore, runID string, n int) {
	t.Helper()
	ctx := context.Background()
	put := func(record *orkstackv1.TaskAttempt) {
		if err := attempts.PutAttempt(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	put(attemptRecord(runID, "list", 3, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))
	put(attemptRecord(runID, "list", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))
	for i := n - 1; i >= 0; i-- {
		put(itemRecord(runID, int32(i), 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))
	}
	put(itemRecord(runID, 7, 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))
	put(attemptRecord(runID+"-other", "list", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))
	logs.write(artifacts.LogRef{RunID: runID, TaskID: "list", Attempt: 3}, 1, "list\n")
	// Item 4 logged an attempt 3 no record exists for.
	logs.write(artifacts.LogRef{RunID: runID, TaskID: "convert", Item: artifacts.ItemAt(4), Attempt: 3}, 1, "convert 4\n")
}

func listAll(t *testing.T, attempts artifacts.AttemptStore, logs artifacts.LogStore, run *orkstackv1.Run) []*orkstackv1.TaskAttempt {
	t.Helper()
	reader := &fakeReader{
		runs:   map[string]*orkstackv1.Run{run.GetId(): run},
		events: map[string][]*orkstackv1.Event{run.GetId(): {}},
	}
	fixture := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Attempts = attempts
		cfg.Logs = logs
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := fixture.runClient().ListTaskAttempts(ctx, connect.NewRequest(&orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()}))
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	return response.Msg.GetAttempts()
}

func timelineRows(attempts []*orkstackv1.TaskAttempt) []string {
	rows := make([]string, len(attempts))
	for i, attempt := range attempts {
		item := "-"
		if attempt.Item != nil {
			item = fmt.Sprint(attempt.GetItem())
		}
		rows[i] = fmt.Sprintf("%s[%s]#%d=%s/%s", attempt.GetTaskId(), item, attempt.GetAttempt(),
			attempt.GetOutcome(), attempt.GetOrkerId())
	}
	return rows
}

func TestListTaskAttempts_RunWideIsTwoListingsForAThousandItems(t *testing.T) {
	const n = 1000
	run := fanOutRunWith("ork-runwide-1000", n)
	attempts := &countingAttempts{MemoryAttemptStore: artifacts.NewMemoryAttemptStore()}
	logs := &countingLogs{memoryLogStore: &memoryLogStore{}}
	seedRunWide(t, attempts, logs.memoryLogStore, run.GetId(), n)

	got := listAll(t, attempts, logs, run)
	if attempts.runListings != 1 || attempts.listing != 0 {
		t.Errorf("record listings: %d run-wide + %d per scope, want 1 + 0", attempts.runListings, attempts.listing)
	}
	if logs.runWide != 1 || logs.one != 0 {
		t.Errorf("log listings: %d run-wide + %d per scope, want 1 + 0", logs.runWide, logs.one)
	}
	if want := n + 3; attempts.gets != want {
		t.Errorf("record reads = %d, want one per record of this run (%d)", attempts.gets, want)
	}
	// list 1..3 (2 UNREPORTED), 1000 items (item 4 to attempt 3, item 7 to 2), never: nothing.
	if want := 3 + n + 2 + 1; len(got) != want {
		t.Fatalf("rows = %d, want %d", len(got), want)
	}
}

func TestListTaskAttempts_RunWideAnswersExactlyAsScopeByScope(t *testing.T) {
	const n = 12
	run := fanOutRunWith("ork-runwide-eq", n)
	store := artifacts.NewMemoryAttemptStore()
	logs := &memoryLogStore{}
	seedRunWide(t, store, logs, run.GetId(), n)

	wide := listAll(t, store, logs, run)
	scoped := listAll(t, scopedAttempts{store}, scopedLogs{logs}, run)
	// Run-wide records with per-scope logs, and the reverse, too.
	mixed := listAll(t, store, scopedLogs{logs}, run)
	if fmt.Sprint(timelineRows(wide)) != fmt.Sprint(timelineRows(scoped)) {
		t.Fatalf("run-wide:\n%v\nscope by scope:\n%v", timelineRows(wide), timelineRows(scoped))
	}
	if fmt.Sprint(timelineRows(mixed)) != fmt.Sprint(timelineRows(scoped)) {
		t.Fatalf("run-wide records, per-scope logs:\n%v\nscope by scope:\n%v", timelineRows(mixed), timelineRows(scoped))
	}
	for i := range wide {
		if !proto.Equal(wide[i], scoped[i]) {
			t.Fatalf("row %d differs:\nrun-wide %v\nscoped   %v", i, wide[i], scoped[i])
		}
	}
	rows := timelineRows(wide)
	for _, want := range []string{
		"list[-]#2=ATTEMPT_OUTCOME_UNREPORTED/",
		"convert[4]#3=ATTEMPT_OUTCOME_UNREPORTED/",
		"convert[7]#2=ATTEMPT_OUTCOME_FAILED/orker-a1",
	} {
		found := false
		for _, row := range rows {
			found = found || row == want
		}
		if !found {
			t.Errorf("no row %q in %v", want, rows)
		}
	}
	if rows[0] != "list[-]#1=ATTEMPT_OUTCOME_FAILED/orker-a1" || rows[3] != "convert[0]#1=ATTEMPT_OUTCOME_SUCCEEDED/orker-a1" {
		t.Errorf("order: %v", rows[:4])
	}
}
