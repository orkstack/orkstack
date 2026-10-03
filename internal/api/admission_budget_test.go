package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe the third reason inside the one admission gate: an
// enforced budget, measured over its trailing period, refusing new runs with
// the same "leaves nothing behind" property a hold has.

// listCountingBodies is countingBodies that also counts index listings, so a
// test can prove a submit never measured usage.
type listCountingBodies struct {
	*countingBodies
	mu       sync.Mutex
	listings int
}

func (l *listCountingBodies) ListRuns(ctx context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	l.mu.Lock()
	l.listings++
	l.mu.Unlock()
	return l.countingBodies.ListRuns(ctx, q)
}

func (l *listCountingBodies) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.listings
}

// brokenRunListing is a run-wide attempt reader whose listing fails.
type brokenRunListing struct{ *artifacts.MemoryAttemptStore }

func (brokenRunListing) ListRunAttemptRefs(context.Context, string) ([]artifacts.AttemptRef, error) {
	return nil, errors.New("attempt bucket unreachable")
}

type budgetFixture struct {
	*admissionFixture
	bodies   *listCountingBodies
	attempts *artifacts.MemoryAttemptStore
	seeded   int
}

func newBudgetFixture(t *testing.T, ttl time.Duration, configure ...func(*api.Config)) *budgetFixture {
	t.Helper()
	local, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	f := &budgetFixture{
		bodies:   &listCountingBodies{countingBodies: newCountingBodies(local)},
		attempts: artifacts.NewMemoryAttemptStore(),
	}
	edits := append([]func(*api.Config){func(cfg *api.Config) {
		cfg.Archive = engine.NewRunArchiver(f.bodies)
		cfg.Attempts = f.attempts
		cfg.UsageCacheTTL = ttl
	}}, configure...)
	f.admissionFixture = newAdmissionFixture(t, edits...)
	return f
}

// spend archives one finished run of payments, created ten minutes ago,
// whose one task held one core for `seconds`.
func (f *budgetFixture) spend(t *testing.T, seconds float64) {
	t.Helper()
	f.seeded++
	at := time.Now().UTC().Add(-10 * time.Minute)
	id := fmt.Sprintf("ork-spent-%d", f.seeded)
	held := time.Duration(seconds * float64(time.Second))
	if err := f.attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion, RunId: id, TaskId: "work", Attempt: 1,
		Outcome:   orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(held)),
		Resources: &orkstackv1.ReservedResources{Cpu: 1, MemoryBytes: 1 << 30},
	}); err != nil {
		t.Fatalf("PutAttempt: %v", err)
	}
	run := &orkstackv1.Run{
		Id: id, Namespace: "payments", OrkflowName: "reconcile", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "work", Pool: "main", Attempt: 1, Status: orkstackv1.Status_STATUS_SUCCEEDED}},
	}
	if err := engine.NewRunArchiver(f.bodies.inner).Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("archive %s: %v", id, err)
	}
}

func (f *budgetFixture) setBudget(t *testing.T, cpu int64, enforce bool) {
	t.Helper()
	f.edit(t, "payments", func(ns *orkstackv1.Namespace) {
		ns.Budget = &orkstackv1.Budget{CpuSeconds: cpu, Period: durationpb.New(time.Hour), Enforce: enforce}
	})
}

const budgetSentence = `namespace "payments" has reserved 12 cpu-seconds over the last 1h0m0s, at or above its enforced budget of 10 (measured `
const budgetFix = "New runs are refused until the period's usage falls below the budget; raise it with " +
	"`orkctl namespace update payments --budget-cpu-seconds N`, or stop enforcing with " +
	"`orkctl namespace update payments --budget-enforce=false`"

func TestSubmitRun_RefusedByBudgetLeavesNothingBehind(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.spend(t, 12)
	f.setBudget(t, 10, true)

	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, budgetSentence, budgetFix)
	f.assertNothingBehind(t, 0)
	listed, err := f.runClient().ListRuns(context.Background(),
		connect.NewRequest(&orkstackv1.ListRunsRequest{Namespace: "payments"}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	for _, run := range listed.Msg.GetRuns() {
		if !strings.HasPrefix(run.GetId(), "ork-spent-") {
			t.Errorf("ListRuns(payments) lists %s: a refusal must record nothing", run.GetId())
		}
	}
	_, err = f.submit("order-42", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, budgetSentence)
	if proposed, _ := f.claimer.counts(); proposed != 0 {
		t.Errorf("claimer proposed %d candidates, want 0: a refused key must not be claimed", proposed)
	}
	f.assertNothingBehind(t, 0)
}

func TestSubmitRun_ASoftBudgetNeverReadsUsage(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.spend(t, 12)
	f.setBudget(t, 10, false)
	if _, err := f.submit("", false); err != nil {
		t.Fatalf("submit under a soft, reached budget: %v", err)
	}
	if got := f.bodies.count(); got != 0 {
		t.Errorf("the index was listed %d times; a soft budget must never read usage at submit", got)
	}
}

func TestSubmitRun_BudgetIsCheckedAfterHoldAndCap(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.spend(t, 12)
	f.setBudget(t, 10, true)
	f.hold(t, heldReason)
	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, heldReason)
	f.release(t)

	f.cluster.reader.mu.Lock()
	f.cluster.reader.executions["payments"] = engine.Executions{Running: []*orkstackv1.Run{{Id: "r1", Namespace: "payments"}}}
	f.cluster.reader.mu.Unlock()
	f.setCap(t, 1)
	_, err = f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, "already has 1 runs in flight")
	if got := f.bodies.count(); got != 0 {
		t.Errorf("usage was read %d times behind a hold or a cap refusal, want 0", got)
	}
}

func TestSubmitRun_UnderBudgetIsAdmitted(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.spend(t, 9.9)
	f.setBudget(t, 10, true)
	if _, err := f.submit("", false); err != nil {
		t.Fatalf("submit at 9.9 of 10 cpu-seconds: %v", err)
	}
	if got := f.bodies.count(); got != 1 {
		t.Errorf("index listings = %d, want the one measurement", got)
	}
}

func TestSubmitRun_AnUnmeasurableEnforcedBudgetRefusesRatherThanAdmits(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.setBudget(t, 10, true)
	for i := range api.UsageMaxScan + 1 {
		f.bodies.entries = append(f.bodies.entries, artifacts.RunIndexEntry{
			RunID: fmt.Sprintf("ork-9-%d", i), Namespace: "payments", Time: time.Now().Add(-time.Minute), Status: "succeeded",
		})
	}
	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition,
		`namespace "payments" enforces a budget over 1h0m0s and its usage could not be measured: usage of namespace "payments" over 1h0m0s would read 10001 finished runs, more than the 10000 a usage read is bounded to. `+
			"New runs are refused while an enforced budget cannot be measured; shorten the period with "+
			"`orkctl namespace update payments --budget-period D`, or stop enforcing with `orkctl namespace update payments --budget-enforce=false`")
	f.assertNothingBehind(t, 0)
}

func TestSubmitRun_AnUnreadableUsageIsUnavailable(t *testing.T) {
	f := newBudgetFixture(t, 0, func(cfg *api.Config) {
		cfg.Attempts = brokenRunListing{artifacts.NewMemoryAttemptStore()}
	})
	f.spend(t, 12)
	f.setBudget(t, 10, true)
	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeUnavailable,
		`namespace "payments" budget could not be checked against its usage: `, "attempt bucket unreachable", "ask again")
	f.assertNothingBehind(t, 0)
	if lines := refusalLines(f.logs.String()); len(lines) != 0 {
		t.Errorf("an unreadable usage was logged as a refusal: %v", lines)
	}
}

func TestSubmitRun_AllowEmptyPoolDoesNotBypassABudget(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.spend(t, 12)
	f.setBudget(t, 10, true)
	_, err := f.submit("", true)
	requireRefusal(t, err, connect.CodeFailedPrecondition, budgetSentence)
	if calls := f.pools.calls(); calls != 0 {
		t.Errorf("the pool probe was asked %d times; the budget must refuse before any pool is looked at", calls)
	}
	f.assertNothingBehind(t, 0)
}

func TestSubmitRun_AKeyedRepeatDuringABudgetRefusalIsRefusedNotAnswered(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.setBudget(t, 10, true)
	first, err := f.submit("order-42", false)
	if err != nil {
		t.Fatalf("keyed submit under budget: %v", err)
	}
	f.spend(t, 12)
	_, err = f.submit("order-42", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, budgetSentence)
	got, err := f.runClient().GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: first.GetId(), Namespace: "payments"}))
	if err != nil || got.Msg.GetRun().GetId() != first.GetId() {
		t.Fatalf("GetRun(%s) after a refused repeat = %v / %v", first.GetId(), got, err)
	}
	if started := f.starter.startedRuns(); len(started) != 1 {
		t.Errorf("started = %v, want only the first", started)
	}
}

func TestSubmitRun_ABudgetNeverCancelsAnAcceptedRun(t *testing.T) {
	f := newBudgetFixture(t, 0)
	f.setBudget(t, 10, true)
	run, err := f.submit("", false)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	f.spend(t, 12)
	if _, err := f.submit("", false); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("submit over budget = %v, want FailedPrecondition", err)
	}
	if canceled := f.starter.canceledRuns(); len(canceled) != 0 {
		t.Errorf("a budget canceled %v", canceled)
	}
	got, err := f.runClient().GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: run.GetId(), Namespace: "payments"}))
	if err != nil || got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("the accepted run after the budget was reached = %v / %v, want still RUNNING", got, err)
	}
}

func TestSubmitRun_TheGateAndGetUsageShareOneMeasurement(t *testing.T) {
	f := newBudgetFixture(t, time.Hour)
	f.spend(t, 12)
	f.setBudget(t, 10, true)
	_, refusal := f.submit("", false)
	requireRefusal(t, refusal, connect.CodeFailedPrecondition, budgetSentence)
	listings := f.bodies.count()

	stats := orkstackv1connect.NewStatsServiceClient(f.client, f.baseURL)
	resp, err := stats.GetUsage(context.Background(), connect.NewRequest(&orkstackv1.GetUsageRequest{
		Namespace: "payments", Lookback: durationpb.New(time.Hour),
	}))
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if got := f.bodies.count(); got != listings {
		t.Errorf("GetUsage listed the index again (%d -> %d): it should share the gate's measurement", listings, got)
	}
	measured := resp.Msg.GetComputedAt().AsTime().UTC().Format(time.RFC3339)
	if !strings.Contains(refusal.Error(), "(measured "+measured+")") {
		t.Errorf("the refusal's measured instant does not equal GetUsage's computed_at %s", measured)
	}
	if !resp.Msg.GetBudgetWindow() || !resp.Msg.GetBudgetReached() || resp.Msg.GetTotal().GetCpuSeconds() != 12 {
		t.Errorf("GetUsage = window %v reached %v cpu %v", resp.Msg.GetBudgetWindow(), resp.Msg.GetBudgetReached(), resp.Msg.GetTotal().GetCpuSeconds())
	}
}

func TestSubmitRun_BudgetRefusalIsLoggedAndCounted(t *testing.T) {
	recorder := &recordingAdmissionObserver{}
	f := newBudgetFixture(t, 0, func(cfg *api.Config) { cfg.Admissions = recorder })
	f.spend(t, 12)
	f.setBudget(t, 10, true)
	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, budgetSentence)

	for i := range api.UsageMaxScan + 1 {
		f.bodies.entries = append(f.bodies.entries, artifacts.RunIndexEntry{
			RunID: fmt.Sprintf("ork-9-%d", i), Namespace: "payments", Time: time.Now().Add(-time.Minute), Status: "succeeded",
		})
	}
	_, err = f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, "could not be measured")

	seen := recorder.seen()
	if len(seen) != 2 || seen[0].reason != "BUDGET_EXCEEDED" || seen[1].reason != "BUDGET_UNMEASURABLE" {
		t.Fatalf("ObserveRefusal calls = %+v, want BUDGET_EXCEEDED then BUDGET_UNMEASURABLE", seen)
	}
	lines := refusalLines(f.logs.String())
	if len(lines) != 2 {
		t.Fatalf("run refused lines = %d, want 2:\n%s", len(lines), f.logs.String())
	}
	for _, want := range []string{"reason=BUDGET_EXCEEDED", "period=1h0m0s", "cpu_seconds_used=12",
		"cpu_seconds_budget=10", "gib_seconds_used=12", "gib_seconds_budget=0", "measured_at="} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("exceeded line %q lacks %q", lines[0], want)
		}
	}
	for _, want := range []string{"reason=BUDGET_UNMEASURABLE", "period=1h0m0s", "error="} {
		if !strings.Contains(lines[1], want) {
			t.Errorf("unmeasurable line %q lacks %q", lines[1], want)
		}
	}
}

func TestCreateNamespace_IdempotentRepeatComparesBudget(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	spec := func(period time.Duration) *orkstackv1.Namespace {
		return &orkstackv1.Namespace{Name: "ledger", Budget: &orkstackv1.Budget{
			CpuSeconds: 5000000, Period: durationpb.New(period), Enforce: true,
		}}
	}
	create := func(ns *orkstackv1.Namespace) (*connect.Response[orkstackv1.CreateNamespaceResponse], error) {
		return f.namespaceClient().CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: ns}))
	}
	if _, err := create(spec(720 * time.Hour)); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	again, err := create(spec(720 * time.Hour))
	if err != nil || again.Msg.GetCreated() {
		t.Fatalf("an identical repeat = %v / created %v, want the no-op", err, again.Msg.GetCreated())
	}
	_, err = create(spec(24 * time.Hour))
	want := "budget is {cpu_seconds=5000000 gib_seconds=0 period=720h0m0s enforce=true}, " +
		"not {cpu_seconds=5000000 gib_seconds=0 period=24h0m0s enforce=true}"
	if connect.CodeOf(err) != connect.CodeAlreadyExists || !strings.Contains(err.Error(), want) {
		t.Fatalf("a different period = %v, want AlreadyExists naming %q", err, want)
	}
	_, err = create(&orkstackv1.Namespace{Name: "ledger"})
	if connect.CodeOf(err) != connect.CodeAlreadyExists || !strings.Contains(err.Error(), "not none") {
		t.Fatalf("no budget against a budgeted namespace = %v, want AlreadyExists naming none", err)
	}
}
