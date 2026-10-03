package engine_test

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var usageBase = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

// heldRecord is one attempt record held for `held`, reserving res (nil =
// no reservation).
func heldRecord(attempt int32, held time.Duration, res *orkstackv1.ReservedResources) *orkstackv1.TaskAttempt {
	return &orkstackv1.TaskAttempt{
		Attempt:    attempt,
		Outcome:    orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		StartedAt:  timestamppb.New(usageBase),
		FinishedAt: timestamppb.New(usageBase.Add(held)),
		Resources:  res,
	}
}

func reserve(cpu float64, gib int64) *orkstackv1.ReservedResources {
	return &orkstackv1.ReservedResources{Cpu: cpu, MemoryBytes: gib << 30}
}

func TestAttemptUsage_ReservationTimesHeldTime(t *testing.T) {
	seconds, cpu, gib, timed, priced := engine.AttemptUsage(heldRecord(1, 90*time.Second, reserve(2, 4)))
	if seconds != 90 || cpu != 180 || gib != 360 || !timed || !priced {
		t.Fatalf("AttemptUsage = (%v, %v, %v, %v, %v), want (90, 180, 360, true, true)", seconds, cpu, gib, timed, priced)
	}
}

func TestAttemptUsage_NoReservationCountsTimeOnly(t *testing.T) {
	seconds, cpu, gib, timed, priced := engine.AttemptUsage(heldRecord(1, 90*time.Second, nil))
	if seconds != 90 || cpu != 0 || gib != 0 || !timed || priced {
		t.Fatalf("AttemptUsage = (%v, %v, %v, %v, %v), want (90, 0, 0, true, false)", seconds, cpu, gib, timed, priced)
	}
}

func TestAttemptUsage_UntimedContributesNothing(t *testing.T) {
	unfinished := heldRecord(1, time.Second, reserve(2, 4))
	unfinished.FinishedAt = nil
	backwards := heldRecord(1, -time.Second, reserve(2, 4))
	unstarted := heldRecord(1, time.Second, reserve(2, 4))
	unstarted.StartedAt = nil
	for name, record := range map[string]*orkstackv1.TaskAttempt{
		"finished_at unset": unfinished, "finished before started": backwards, "started_at unset": unstarted,
	} {
		seconds, cpu, gib, timed, priced := engine.AttemptUsage(record)
		if seconds != 0 || cpu != 0 || gib != 0 || timed || priced {
			t.Errorf("%s: AttemptUsage = (%v, %v, %v, %v, %v), want (0, 0, 0, false, false)", name, seconds, cpu, gib, timed, priced)
		}
	}
}

func TestUsageOfTask_EveryOutcomeCountsAndGapsAreUnreported(t *testing.T) {
	failed := heldRecord(1, 10*time.Second, reserve(1, 1))
	failed.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
	succeeded := heldRecord(3, 20*time.Second, reserve(2, 2))
	run := &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "etl"}
	task := &orkstackv1.TaskRun{TaskId: "load", Pool: "main", Attempt: 3}
	sample := engine.SampleTask(artifacts.RunIndexEntry{RunID: "ork-1-1", Time: usageBase}, run, task,
		[]*orkstackv1.TaskAttempt{failed, succeeded})
	got := engine.UsageOfTask(sample)
	if got.TaskID != "load" || got.Pool != "main" || !got.Observed {
		t.Fatalf("UsageOfTask keys = %+v", got)
	}
	if got.Recorded != 2 || got.Unreported != 1 || got.WithoutResources != 0 || got.Unmeasured != 0 {
		t.Fatalf("UsageOfTask counts = %+v, want 2 recorded, 1 unreported", got)
	}
	if got.TaskSeconds != 30 || got.CPUSeconds != 10+40 || got.GiBSeconds != 10+40 {
		t.Fatalf("UsageOfTask sums = %v / %v / %v, want 30 / 50 / 50", got.TaskSeconds, got.CPUSeconds, got.GiBSeconds)
	}
}

// usageCorpus is three runs across two orkflows, two pools and three tasks,
// with one record carrying no reservation and one task with no record.
func usageCorpus() []engine.UsageRun {
	return []engine.UsageRun{
		{RunID: "ork-1-1", CreatedAt: usageBase, Orkflow: "etl", Tasks: []engine.UsageTask{
			{TaskID: "extract", Pool: "main", Observed: true, Recorded: 1, TaskSeconds: 10, CPUSeconds: 10, GiBSeconds: 20},
			{TaskID: "load", Pool: "gpu", Observed: true, Recorded: 2, Unreported: 1, TaskSeconds: 30, CPUSeconds: 240, GiBSeconds: 480},
		}},
		{RunID: "ork-1-2", CreatedAt: usageBase.Add(time.Hour), Orkflow: "etl", Tasks: []engine.UsageTask{
			{TaskID: "extract", Pool: "main", Observed: true, Recorded: 1, WithoutResources: 1, TaskSeconds: 12},
			{TaskID: "load", Pool: "gpu", Observed: false},
		}},
		{RunID: "ork-1-3", CreatedAt: usageBase.Add(2 * time.Hour), Orkflow: "report", Tasks: []engine.UsageTask{
			{TaskID: "render", Pool: "main", Observed: true, Recorded: 1, Unmeasured: 1},
		}},
		{RunID: "ork-1-4", CreatedAt: usageBase.Add(3 * time.Hour), Orkflow: "report"},
	}
}

func near(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func TestFoldUsage_GroupsByOrkflowTaskAndPoolAndRowsSumToTheTotal(t *testing.T) {
	const (
		O = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW
		T = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK
		P = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL
	)
	total, rows, _ := engine.FoldUsage(usageCorpus(), nil)
	if len(rows) != 0 {
		t.Fatalf("no grouping produced %d rows, want none", len(rows))
	}
	if total.GetRuns() != 3 || total.GetTasks() != 5 || total.GetAttempts() != 5 ||
		total.GetTaskSeconds() != 52 || total.GetCpuSeconds() != 250 || total.GetGibSeconds() != 500 ||
		total.GetAttemptsWithoutResources() != 1 || total.GetAttemptsUnmeasured() != 1 || total.GetUnreportedAttempts() != 1 {
		t.Fatalf("total = %v", total)
	}
	if total.GetOrkflowName() != "" || total.GetTaskId() != "" || total.GetPool() != "" {
		t.Fatalf("the total row carries a key: %v", total)
	}
	groupings := [][]orkstackv1.FailureGroupBy{{O}, {P}, {O, T}, {O, T, P}, {P, O}, {O, P}}
	wantRows := map[int]int{0: 2, 1: 2, 2: 3, 3: 3, 4: 3, 5: 3}
	for i, groupBy := range groupings {
		gotTotal, rows, _ := engine.FoldUsage(usageCorpus(), groupBy)
		if !proto.Equal(gotTotal, total) {
			t.Errorf("%v: total = %v, want the ungrouped total %v", groupBy, gotTotal, total)
		}
		if len(rows) != wantRows[i] {
			t.Errorf("%v: %d rows, want %d: %v", groupBy, len(rows), wantRows[i], rows)
		}
		var sum orkstackv1.UsageRow
		for _, row := range rows {
			sum.Tasks += row.GetTasks()
			sum.Attempts += row.GetAttempts()
			sum.TaskSeconds += row.GetTaskSeconds()
			sum.CpuSeconds += row.GetCpuSeconds()
			sum.GibSeconds += row.GetGibSeconds()
			sum.AttemptsWithoutResources += row.GetAttemptsWithoutResources()
			sum.AttemptsUnmeasured += row.GetAttemptsUnmeasured()
			sum.UnreportedAttempts += row.GetUnreportedAttempts()
			grouped := map[orkstackv1.FailureGroupBy]bool{}
			for _, dim := range groupBy {
				grouped[dim] = true
			}
			if (row.GetOrkflowName() != "") != grouped[O] || (row.GetTaskId() != "") != grouped[T] || (row.GetPool() != "") != grouped[P] {
				t.Errorf("%v: row keys do not match the grouping: %v", groupBy, row)
			}
		}
		if sum.Tasks != total.GetTasks() || sum.Attempts != total.GetAttempts() ||
			!near(sum.TaskSeconds, total.GetTaskSeconds()) || !near(sum.CpuSeconds, total.GetCpuSeconds()) ||
			!near(sum.GibSeconds, total.GetGibSeconds()) || sum.AttemptsWithoutResources != total.GetAttemptsWithoutResources() ||
			sum.AttemptsUnmeasured != total.GetAttemptsUnmeasured() || sum.UnreportedAttempts != total.GetUnreportedAttempts() {
			t.Errorf("%v: Σ rows = %v, want the total %v", groupBy, &sum, total)
		}
	}
	// Runs counts DISTINCT runs per row: etl's two runs both reached the
	// extract row.
	_, rows, _ = engine.FoldUsage(usageCorpus(), []orkstackv1.FailureGroupBy{O, T})
	for _, row := range rows {
		if row.GetOrkflowName() == "etl" && row.GetTaskId() == "extract" && row.GetRuns() != 2 {
			t.Errorf("etl/extract runs = %d, want 2", row.GetRuns())
		}
	}
}

func TestFoldUsage_EvidenceMissingIsCountedNeverZeroed(t *testing.T) {
	runs := usageCorpus()[:2]
	total, _, evidence := engine.FoldUsage(runs, nil)
	if evidence.RunsDispatched != 2 || evidence.RunsEvidenceComplete != 1 || evidence.RunsEvidenceMissing != 1 {
		t.Fatalf("evidence = %+v, want 2 dispatched, 1 complete, 1 missing", evidence)
	}
	// ork-1-2 is missing load's records, and its extract's 12 s still count.
	if total.GetTaskSeconds() != 52 {
		t.Fatalf("task_seconds = %v, want 52: the observed sibling of a missing task still counts", total.GetTaskSeconds())
	}
}

func TestFoldUsage_CoverageSinceIsTheOldestRunWithARecord(t *testing.T) {
	t1, t2, t3 := usageBase, usageBase.Add(time.Hour), usageBase.Add(2*time.Hour)
	runs := []engine.UsageRun{
		{RunID: "ork-3-3", CreatedAt: t3, Orkflow: "x", Tasks: []engine.UsageTask{{TaskID: "a", Pool: "main", Observed: true, Recorded: 1}}},
		{RunID: "ork-3-1", CreatedAt: t1, Orkflow: "x", Tasks: []engine.UsageTask{{TaskID: "a", Pool: "main"}}},
		{RunID: "ork-3-2", CreatedAt: t2, Orkflow: "x", Tasks: []engine.UsageTask{{TaskID: "a", Pool: "main", Observed: true, Recorded: 1}}},
	}
	_, _, evidence := engine.FoldUsage(runs, nil)
	if !evidence.CoverageSince.Equal(t2) {
		t.Fatalf("coverage_since = %v, want %v", evidence.CoverageSince, t2)
	}
	_, _, none := engine.FoldUsage(runs[1:2], nil)
	if !none.CoverageSince.IsZero() {
		t.Fatalf("coverage_since with no record = %v, want zero", none.CoverageSince)
	}
}

func TestFoldUsage_OrderingIsTotalAndClockFree(t *testing.T) {
	task := func(id, pool string, cpu, gib, secs float64) engine.UsageTask {
		return engine.UsageTask{TaskID: id, Pool: pool, Observed: true, Recorded: 1, CPUSeconds: cpu, GiBSeconds: gib, TaskSeconds: secs}
	}
	runs := []engine.UsageRun{
		{RunID: "ork-4-1", CreatedAt: usageBase, Orkflow: "w", Tasks: []engine.UsageTask{
			task("a", "main", 10, 5, 1), // cpu tie with b, lower GiB
			task("b", "main", 10, 9, 1),
			task("c", "main", 7, 1, 9), // GiB tie with d, more time
			task("d", "main", 7, 1, 3),
			task("f", "main", 1, 1, 1), // full tie with e, broken by task id
			task("e", "main", 1, 1, 1),
		}},
	}
	groupBy := []orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW, orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK}
	_, rows, _ := engine.FoldUsage(runs, groupBy)
	var order []string
	for _, row := range rows {
		order = append(order, row.GetTaskId())
	}
	want := []string{"b", "a", "c", "d", "e", "f"}
	for i := range want {
		if i >= len(order) || order[i] != want[i] {
			t.Fatalf("row order = %v, want %v", order, want)
		}
	}
	rng := rand.New(rand.NewSource(7))
	for range 20 {
		shuffled := append([]engine.UsageTask(nil), runs[0].Tasks...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		_, again, _ := engine.FoldUsage([]engine.UsageRun{{RunID: "ork-4-1", CreatedAt: usageBase, Orkflow: "w", Tasks: shuffled}}, groupBy)
		if len(again) != len(rows) {
			t.Fatalf("a shuffled input produced %d rows, want %d", len(again), len(rows))
		}
		for i := range rows {
			if !proto.Equal(again[i], rows[i]) {
				t.Fatalf("a shuffled input produced a different table at row %d: %v vs %v", i, again[i], rows[i])
			}
		}
	}
}
