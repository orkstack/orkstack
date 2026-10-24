package api_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// scopedAttempts is an attempt store that offers ONLY the per-scope listing,
// so the flake index takes its per-row fallback over the same records a
// run-wide store holds.
type scopedAttempts struct {
	inner    *artifacts.MemoryAttemptStore
	listings atomic.Int64
}

func (s *scopedAttempts) PutAttempt(ctx context.Context, attempt *orkstackv1.TaskAttempt) error {
	return s.inner.PutAttempt(ctx, attempt)
}

func (s *scopedAttempts) ListAttempts(ctx context.Context, runID, taskID string, item artifacts.Item) ([]*orkstackv1.TaskAttempt, error) {
	s.listings.Add(1)
	return s.inner.ListAttempts(ctx, runID, taskID, item)
}

// runWideAttempts is the same store with the run-wide reader every durable
// backend has, counting each kind of round trip.
type runWideAttempts struct {
	*scopedAttempts
	runListings atomic.Int64
	reads       atomic.Int64
}

func (r *runWideAttempts) ListRunAttemptRefs(ctx context.Context, runID string) ([]artifacts.AttemptRef, error) {
	r.runListings.Add(1)
	return r.inner.ListRunAttemptRefs(ctx, runID)
}

func (r *runWideAttempts) GetAttempt(ctx context.Context, ref artifacts.AttemptRef) (*orkstackv1.TaskAttempt, error) {
	r.reads.Add(1)
	return r.inner.GetAttempt(ctx, ref)
}

// fanOutRun archives one finished run holding a plain task and a fan-out of
// items items, and files records for them: every third item retried, every
// fifth item unrecorded, the plain task retried.
func fanOutRun(t *testing.T, fixture *breakdownFixture, store artifacts.AttemptStore, at time.Time, items int) string {
	t.Helper()
	fixture.seeded++
	id := fmt.Sprintf("ork-%d-%d", at.UnixNano(), fixture.seeded)
	put := func(task string, item *int32, attempt int32, outcome orkstackv1.AttemptOutcome) {
		record := &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion, RunId: id, TaskId: task, Item: item,
			Attempt: attempt, Outcome: outcome, OrkerId: fmt.Sprintf("orker-%d", attempt), Phase: "commit",
		}
		if err := store.PutAttempt(context.Background(), record); err != nil {
			t.Fatalf("put attempt: %v", err)
		}
	}
	fan := &orkstackv1.TaskRun{TaskId: "fan", Pool: "main", Each: &orkstackv1.TaskEach{}, Status: orkstackv1.Status_STATUS_SUCCEEDED}
	for i := 0; i < items; i++ {
		index := proto.Int32(int32(i))
		attempt := int32(1)
		switch {
		case i%5 == 0:
		case i%3 == 0:
			attempt = 2
			put("fan", index, 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED)
			put("fan", index, 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED)
		default:
			put("fan", index, 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED)
		}
		fan.Items = append(fan.Items, &orkstackv1.TaskRun{
			TaskId: "fan", Item: index, Pool: "main", Attempt: attempt, Status: orkstackv1.Status_STATUS_SUCCEEDED,
		})
	}
	put("build", nil, 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED)
	put("build", nil, 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED)
	run := &orkstackv1.Run{
		Id: id, Namespace: "payments", OrkflowName: "reconcile", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "build", Pool: "main", Attempt: 2, Status: orkstackv1.Status_STATUS_SUCCEEDED},
			fan,
			{TaskId: "skipped", Status: orkstackv1.Status_STATUS_SKIPPED},
		},
	}
	if err := engine.NewRunArchiver(fixture.bodies.inner).Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("archive run %s: %v", id, err)
	}
	return id
}

func flakeAnswer(t *testing.T, svc *api.Service) *orkstackv1.GetFlakeIndexResponse {
	t.Helper()
	got, err := api.NewStatsHandler(svc, 0).GetFlakeIndex(context.Background(), connect.NewRequest(
		flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow, dimOrkerAPI)))
	if err != nil {
		t.Fatalf("GetFlakeIndex: %v", err)
	}
	got.Msg.ComputedAt = nil
	return got.Msg
}

// A run with a 1000-item fan-out costs ONE attempt listing, not one per item,
// and its samples are exactly the per-row path's.
func TestGetFlakeIndex_ARunWideStoreListsEachRunOnceAndAnswersLikeThePerRowPath(t *testing.T) {
	runWide := &runWideAttempts{scopedAttempts: &scopedAttempts{inner: artifacts.NewMemoryAttemptStore()}}
	wide := newBreakdownFixture(t, flakeNow, []string{"payments"}, withAttempts(runWide))
	scoped := &scopedAttempts{inner: artifacts.NewMemoryAttemptStore()}
	perRow := newBreakdownFixture(t, flakeNow, []string{"payments"}, withAttempts(scoped))
	for i, items := range []int{1000, 7, 0} {
		at := flakeNow.Add(-time.Duration(i+1) * time.Minute)
		fanOutRun(t, wide, runWide, at, items)
		fanOutRun(t, perRow, scoped, at, items)
	}

	got := flakeAnswer(t, wide.svc)
	if n := runWide.runListings.Load(); n != 3 {
		t.Errorf("run-wide listings = %d, want one per run (3)", n)
	}
	if n := runWide.listings.Load(); n != 0 {
		t.Errorf("per-scope listings = %d on a run-wide store, want 0", n)
	}
	want := flakeAnswer(t, perRow.svc)
	if n := scoped.listings.Load(); n != 1+1000+1+7+1 {
		t.Errorf("per-row listings = %d, want one per dispatched row (1010)", n)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("the run-wide answer differs from the per-row one:\n got %v\nwant %v", got, want)
	}
	if len(got.GetGroups()) == 0 {
		t.Fatal("no groups: the comparison proved nothing")
	}
}

// The row budget counts tasks AND items, from the bodies, before any attempt
// record is read.
func TestGetFlakeIndex_OverTheRowBudgetIsRefusedBeforeAnyAttemptRead(t *testing.T) {
	runWide := &runWideAttempts{scopedAttempts: &scopedAttempts{inner: artifacts.NewMemoryAttemptStore()}}
	fixture := newBreakdownFixture(t, flakeNow, []string{"payments"}, withAttempts(runWide))
	// (1 task + 25,000 items) + (1 task + 24,999 items) = 50,001 rows, one
	// past the budget. The skipped task of each run is no row.
	half := api.FlakeIndexMaxRows / 2
	fanOutRun(t, fixture, artifacts.NewMemoryAttemptStore(), flakeNow.Add(-time.Minute), half)
	fanOutRun(t, fixture, artifacts.NewMemoryAttemptStore(), flakeNow.Add(-2*time.Minute), half-1)

	_, err := api.NewStatsHandler(fixture.svc, 0).GetFlakeIndex(context.Background(), connect.NewRequest(
		flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow)))
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("an over-budget row count answered %v, want ResourceExhausted", err)
	}
	for _, want := range []string{fmt.Sprint(api.FlakeIndexMaxRows + 1), fmt.Sprint(api.FlakeIndexMaxRows), "narrow the window", "name one orkflow"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
	if n := runWide.runListings.Load() + runWide.listings.Load() + runWide.reads.Load(); n != 0 {
		t.Errorf("read attempt records %d times before refusing, want 0", n)
	}
}
