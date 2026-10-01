package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe GetUsage as a handler: the namespace and window it
// answers for, the dimensions it accepts, and the budget it stamps on a
// measurement it never caches the budget into.

var usageAt = time.Date(2026, 9, 24, 10, 51, 0, 0, time.UTC)

// usageFixture is a breakdownFixture whose registry a test can edit, with
// the usage cache TTL the test names.
type usageFixture struct {
	*breakdownFixture
	docs    registry
	handler *api.StatsHandler
}

func newUsageFixture(t *testing.T, ttl time.Duration) *usageFixture {
	t.Helper()
	docs := registry{"default": {Name: "default"}, "analytics": {Name: "analytics"}, "payments": {Name: "payments"}}
	f := &usageFixture{docs: docs}
	f.breakdownFixture = newBreakdownFixture(t, usageAt, nil, func(cfg *api.Config, _ *breakdownFixture) {
		cfg.Namespaces = docs
		cfg.UsageCacheTTL = ttl
	})
	f.handler = api.NewStatsHandler(f.svc, 0)
	return f
}

// held archives one finished run of namespace whose single dispatched task
// held its Orker for `held` reserving cpu cores and 1 GiB.
func (f *usageFixture) held(t *testing.T, namespace string, status orkstackv1.Status, at time.Time, held time.Duration, cpu float64) string {
	t.Helper()
	f.seeded++
	id := fmt.Sprintf("ork-%d-%d", at.UnixNano(), f.seeded)
	run := &orkstackv1.Run{
		Id: id, Namespace: namespace, OrkflowName: "backfill", Status: status,
		CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "load", Pool: "gpu", Attempt: 1, Status: status}},
	}
	if err := f.attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion, RunId: id, TaskId: "load", Attempt: 1,
		StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(held)),
		Resources: &orkstackv1.ReservedResources{Cpu: cpu, MemoryBytes: 1 << 30},
	}); err != nil {
		t.Fatalf("PutAttempt: %v", err)
	}
	if err := engine.NewRunArchiver(f.bodies.inner).Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("archive run %s: %v", id, err)
	}
	return id
}

func (f *usageFixture) usage(req *orkstackv1.GetUsageRequest) (*orkstackv1.GetUsageResponse, error) {
	resp, err := f.handler.GetUsage(context.Background(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func lookback(d time.Duration) *orkstackv1.GetUsageRequest {
	return &orkstackv1.GetUsageRequest{Namespace: "analytics", Lookback: durationpb.New(d)}
}

func TestGetUsage_UnknownNamespaceIsNotFoundHoweverWarmTheCache(t *testing.T) {
	f := newUsageFixture(t, time.Hour)
	f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), time.Minute, 1)
	if _, err := f.usage(lookback(24 * time.Hour)); err != nil {
		t.Fatalf("GetUsage(analytics): %v", err)
	}
	_, err := f.usage(&orkstackv1.GetUsageRequest{Namespace: "ghost", Lookback: durationpb.New(24 * time.Hour)})
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetUsage(ghost) = %v, want NotFound", err)
	}
}

func TestGetUsage_AnEmptyWindowIsInvalidArgument(t *testing.T) {
	f := newUsageFixture(t, 0)
	_, err := f.usage(&orkstackv1.GetUsageRequest{
		Namespace: "analytics", Since: timestamppb.New(usageAt), Until: timestamppb.New(usageAt.Add(-time.Hour)),
	})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("an empty window = %v, want InvalidArgument", err)
	}
}

func TestGetUsage_GroupByAcceptsOrkflowTaskPoolAndPrependsOrkflowForTask(t *testing.T) {
	f := newUsageFixture(t, 0)
	f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), time.Minute, 1)
	req := lookback(24 * time.Hour)
	req.GroupBy = []orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK}
	got, err := f.usage(req)
	if err != nil {
		t.Fatalf("GetUsage([TASK]): %v", err)
	}
	want := []orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW, orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK}
	if fmt.Sprint(got.GetGroupBy()) != fmt.Sprint(want) {
		t.Fatalf("group_by echo = %v, want %v", got.GetGroupBy(), want)
	}
	if len(got.GetRows()) != 1 || got.GetRows()[0].GetOrkflowName() != "backfill" || got.GetRows()[0].GetTaskId() != "load" {
		t.Fatalf("rows = %v", got.GetRows())
	}
	pool := lookback(24 * time.Hour)
	pool.GroupBy = []orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL}
	if got, err := f.usage(pool); err != nil || fmt.Sprint(got.GetGroupBy()) != fmt.Sprint(pool.GroupBy) {
		t.Fatalf("GetUsage([POOL]) = %v / %v", got.GetGroupBy(), err)
	}
	for name, dims := range map[string][]orkstackv1.FailureGroupBy{
		"CODE":        {orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE},
		"UNSPECIFIED": {orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED},
		"POOL":        {orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL, orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL},
	} {
		req := lookback(24 * time.Hour)
		req.GroupBy = dims
		_, err := f.usage(req)
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "group_by "+name) {
			t.Errorf("group_by %v = %v, want InvalidArgument naming %s", dims, err, name)
		}
	}
}

func TestGetUsage_PopulationReconcilesWithGetStats(t *testing.T) {
	f := newUsageFixture(t, 0)
	f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), time.Minute, 1)
	f.held(t, "analytics", orkstackv1.Status_STATUS_FAILED, usageAt.Add(-2*time.Hour), time.Minute, 1)
	f.held(t, "analytics", orkstackv1.Status_STATUS_CANCELED, usageAt.Add(-3*time.Hour), time.Minute, 1)
	f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-48*time.Hour), time.Minute, 1)
	f.held(t, "payments", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), time.Minute, 1)
	since, until := timestamppb.New(usageAt.Add(-24*time.Hour)), timestamppb.New(usageAt)
	stats, err := f.handler.GetStats(context.Background(), connect.NewRequest(&orkstackv1.GetStatsRequest{
		Namespace: "analytics", Since: since, Until: until,
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	byStatus := stats.Msg.GetRunsByStatus()
	finished := byStatus["succeeded"] + byStatus["failed"] + byStatus["canceled"]
	got, err := f.usage(&orkstackv1.GetUsageRequest{Namespace: "analytics", Since: since, Until: until})
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if finished != 3 || got.GetRunsInWindow() != finished {
		t.Fatalf("runs_in_window = %d, GetStats finished = %d; want both 3", got.GetRunsInWindow(), finished)
	}
}

func (f *usageFixture) setBudget(budget *orkstackv1.Budget) {
	f.docs["analytics"] = &orkstackv1.Namespace{Name: "analytics", Budget: budget}
}

func TestGetUsage_BudgetWindowOnlyForTheExactTrailingPeriod(t *testing.T) {
	f := newUsageFixture(t, 0)
	f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), 100*time.Second, 1)
	f.setBudget(&orkstackv1.Budget{CpuSeconds: 50, Period: durationpb.New(720 * time.Hour), Enforce: true})
	exact, err := f.usage(lookback(720 * time.Hour))
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if !exact.GetBudgetWindow() || !exact.GetBudgetReached() || exact.GetBudget().GetCpuSeconds() != 50 {
		t.Fatalf("lookback = period: window=%v reached=%v budget=%v", exact.GetBudgetWindow(), exact.GetBudgetReached(), exact.GetBudget())
	}
	for name, req := range map[string]*orkstackv1.GetUsageRequest{
		"719h": lookback(719 * time.Hour),
		"since set": {Namespace: "analytics", Lookback: durationpb.New(720 * time.Hour),
			Since: timestamppb.New(usageAt.Add(-720 * time.Hour))},
		"until set": {Namespace: "analytics", Lookback: durationpb.New(720 * time.Hour), Until: timestamppb.New(usageAt)},
	} {
		got, err := f.usage(req)
		if err != nil {
			t.Fatalf("%s: GetUsage: %v", name, err)
		}
		if got.GetBudgetWindow() || got.GetBudgetReached() || got.GetBudget() == nil {
			t.Errorf("%s: window=%v reached=%v budget=%v; want the budget stamped and no share", name,
				got.GetBudgetWindow(), got.GetBudgetReached(), got.GetBudget())
		}
	}
	f.setBudget(nil)
	none, err := f.usage(lookback(720 * time.Hour))
	if err != nil || none.GetBudgetWindow() || none.GetBudget() != nil {
		t.Fatalf("no budget: %v / window=%v budget=%v", err, none.GetBudgetWindow(), none.GetBudget())
	}
}

func TestGetUsage_BudgetReachedIsTheGatesDecision(t *testing.T) {
	f := newUsageFixture(t, 0)
	f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), 100*time.Second, 1)
	for _, bound := range []int64{99, 100, 101, 1000} {
		budget := &orkstackv1.Budget{CpuSeconds: bound, Period: durationpb.New(time.Hour * 2)}
		f.setBudget(budget)
		got, err := f.usage(lookback(2 * time.Hour))
		if err != nil {
			t.Fatalf("GetUsage: %v", err)
		}
		want := engine.BudgetReached(budget, engine.BudgetUsage{CPUSeconds: got.GetTotal().GetCpuSeconds(), GiBSeconds: got.GetTotal().GetGibSeconds()})
		if got.GetBudgetReached() != want {
			t.Errorf("bound %d: budget_reached = %v, engine.BudgetReached = %v", bound, got.GetBudgetReached(), want)
		}
	}
}

func TestGetUsage_ABudgetEditIsVisibleBeforeTheMeasurementExpires(t *testing.T) {
	f := newUsageFixture(t, time.Hour)
	f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), 100*time.Second, 1)
	f.setBudget(&orkstackv1.Budget{CpuSeconds: 5000, Period: durationpb.New(720 * time.Hour)})
	first, err := f.usage(lookback(720 * time.Hour))
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	edited := &orkstackv1.Budget{CpuSeconds: 50, Period: durationpb.New(720 * time.Hour), Enforce: true}
	f.setBudget(edited)
	second, err := f.usage(lookback(720 * time.Hour))
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if !proto.Equal(first.GetComputedAt(), second.GetComputedAt()) {
		t.Fatalf("computed_at moved (%v -> %v): the second answer should be the cached measurement",
			first.GetComputedAt().AsTime(), second.GetComputedAt().AsTime())
	}
	if !proto.Equal(second.GetBudget(), edited) || !second.GetBudgetReached() {
		t.Fatalf("budget = %v reached=%v, want the edit, reached", second.GetBudget(), second.GetBudgetReached())
	}
}

func TestGetUsage_MapsEveryMeasureErrorToItsCode(t *testing.T) {
	t.Run("scan bound", func(t *testing.T) {
		f := newUsageFixture(t, 0)
		for i := range api.UsageMaxScan + 1 {
			f.bodies.entries = append(f.bodies.entries, artifacts.RunIndexEntry{
				RunID: fmt.Sprintf("ork-9-%d", i), Namespace: "analytics", Time: usageAt.Add(-time.Minute), Status: "succeeded",
			})
		}
		_, err := f.usage(lookback(time.Hour))
		if connect.CodeOf(err) != connect.CodeResourceExhausted ||
			!strings.Contains(err.Error(), `usage of namespace "analytics" over 1h0m0s would read 10001 finished runs, more than the 10000 a usage read is bounded to; narrow the window with since/until`) {
			t.Fatalf("scan bound = %v", err)
		}
	})
	t.Run("no archive", func(t *testing.T) {
		svc := api.NewService(api.Config{
			Reader: &fakeReader{executions: map[string]engine.Executions{}}, Attempts: artifacts.NewMemoryAttemptStore(),
			Starter: &fakeStarter{}, Now: newStatsClock(usageAt).Now,
		})
		_, err := api.NewStatsHandler(svc, 0).GetUsage(context.Background(), connect.NewRequest(&orkstackv1.GetUsageRequest{}))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "ORK_ARTIFACT_BACKEND") {
			t.Fatalf("no archive = %v", err)
		}
	})
	t.Run("no run-wide reader", func(t *testing.T) {
		f := newBreakdownFixture(t, usageAt, nil, withAttempts(attemptsBroken{}))
		_, err := api.NewStatsHandler(f.svc, 0).GetUsage(context.Background(), connect.NewRequest(&orkstackv1.GetUsageRequest{}))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
			!strings.Contains(err.Error(), "task attempt store is not configured for run-wide reads") {
			t.Fatalf("no run-wide reader = %v", err)
		}
	})
	t.Run("unreadable body", func(t *testing.T) {
		f := newUsageFixture(t, 0)
		id := f.held(t, "analytics", orkstackv1.Status_STATUS_SUCCEEDED, usageAt.Add(-time.Hour), time.Minute, 1)
		f.bodies.broken[id] = errors.New("bucket unreachable")
		_, err := f.usage(lookback(24 * time.Hour))
		if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), id) {
			t.Fatalf("unreadable body = %v, want Unavailable naming %s", err, id)
		}
	})
}
