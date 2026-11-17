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

// measuredRecord is a record held 10 s reserving 2 cores whose container's
// cgroup reported cpu seconds and a peak against limit.
func measuredRecord(attempt int32, cpu float64, peak, limit int64, exact bool) *orkstackv1.TaskAttempt {
	r := heldRecord(attempt, 10*time.Second, reserve(2, 1))
	r.Measured = &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2, CpuSeconds: cpu,
		MemoryPeakBytes: peak, MemoryPeakExact: exact, MemoryLimitBytes: limit,
	}
	return r
}

// TestFoldUsage_MeasuredUsageBesideTheReservation: measured cpu-seconds are
// compared with the reservation of the SAME measured attempts (an unmeasured
// attempt's reservation is not in the denominator); the memory percentiles
// use exact peaks against a limit only; and every record that measured
// nothing is counted by why, one an older Orker wrote under UNSPECIFIED.
func TestFoldUsage_MeasuredUsageBesideTheReservation(t *testing.T) {
	notRun := heldRecord(5, 10*time.Second, reserve(2, 1))
	notRun.Measured = &orkstackv1.MeasuredUsage{
		Method:           orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED,
		UnmeasuredReason: orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NOT_RUN,
	}
	older := heldRecord(6, 10*time.Second, reserve(2, 1)) // no measured: a v0.3 Orker's
	sample := engine.TaskSample{RunID: "r1", Orkflow: "o", TaskID: "t", Pool: "main", Observed: true, Attempts: 6,
		Records: []*orkstackv1.TaskAttempt{
			measuredRecord(1, 5, 50, 100, true),  // 50 %
			measuredRecord(2, 10, 90, 100, true), // 90 %
			measuredRecord(3, 5, 99, 100, false), // a sampled peak: not in the percentiles
			measuredRecord(4, 0, 30, 0, true),    // no limit: not in the percentiles
			notRun, older,
		}}
	total, _, _ := engine.FoldUsage([]engine.UsageRun{{RunID: "r1", Orkflow: "o", Tasks: []engine.UsageTask{engine.UsageOfTask(sample)}}}, nil)

	if total.GetAttemptsMeasured() != 4 || total.GetMeasuredCpuSeconds() != 20 {
		t.Fatalf("measured %d attempts, %v cpu-s; want 4 and 20", total.GetAttemptsMeasured(), total.GetMeasuredCpuSeconds())
	}
	// 4 measured attempts × 2 cores × 10 s; the two unmeasured are not in it.
	if total.GetReservedCpuSecondsMeasured() != 80 || total.GetCpuEfficiency() != 0.25 {
		t.Fatalf("reserved %v, efficiency %v; want 80 and 0.25", total.GetReservedCpuSecondsMeasured(), total.GetCpuEfficiency())
	}
	if total.GetMemoryPeakBytesMax() != 99 {
		t.Fatalf("peak max = %d, want 99 (every measured peak counts for the max)", total.GetMemoryPeakBytesMax())
	}
	// Two exact utilizations, 50 % and 90 %: p50 is 50 % and p95 is 90 %, each
	// the upper edge of its 0.5 % bucket.
	if p50, p95 := total.GetMemoryUtilizationP50(), total.GetMemoryUtilizationP95(); math.Abs(p50-0.505) > 1e-9 || math.Abs(p95-0.905) > 1e-9 {
		t.Fatalf("p50 %v, p95 %v; want 0.505 and 0.905", p50, p95)
	}
	want := []*orkstackv1.UnmeasuredReasonCount{
		{Reason: orkstackv1.UnmeasuredReason_UNMEASURED_REASON_UNSPECIFIED, Attempts: 1},
		{Reason: orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NOT_RUN, Attempts: 1},
	}
	got := total.GetAttemptsUnmeasuredByReason()
	if len(got) != len(want) {
		t.Fatalf("unmeasured by reason = %v, want %v", got, want)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("unmeasured by reason = %v, want %v", got, want)
		}
	}
}

// TestFoldUsage_MemoryPercentilesAreOrderFree: the same attempts folded in
// any order give the same percentiles, and a row with no exact peak has none.
func TestFoldUsage_MemoryPercentilesAreOrderFree(t *testing.T) {
	var records []*orkstackv1.TaskAttempt
	for i := range 200 {
		records = append(records, measuredRecord(int32(i+1), 1, int64(i%100+1), 100, true))
	}
	fold := func(rs []*orkstackv1.TaskAttempt) *orkstackv1.UsageRow {
		s := engine.TaskSample{RunID: "r", Orkflow: "o", TaskID: "t", Pool: "main", Observed: true, Attempts: int32(len(rs)), Records: rs}
		total, _, _ := engine.FoldUsage([]engine.UsageRun{{RunID: "r", Orkflow: "o", Tasks: []engine.UsageTask{engine.UsageOfTask(s)}}}, nil)
		return total
	}
	first := fold(records)
	shuffled := append([]*orkstackv1.TaskAttempt(nil), records...)
	rand.New(rand.NewSource(7)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	again := fold(shuffled)
	if first.GetMemoryUtilizationP50() != again.GetMemoryUtilizationP50() || first.GetMemoryUtilizationP95() != again.GetMemoryUtilizationP95() {
		t.Fatalf("order changed the percentiles: %v/%v vs %v/%v", first.GetMemoryUtilizationP50(), first.GetMemoryUtilizationP95(),
			again.GetMemoryUtilizationP50(), again.GetMemoryUtilizationP95())
	}
	if p95 := first.GetMemoryUtilizationP95(); p95 < 0.95 || p95 > 0.96 {
		t.Fatalf("p95 of 1 %%..100 %% = %v, want within its bucket of 95 %%", p95)
	}
	sampled := fold([]*orkstackv1.TaskAttempt{measuredRecord(1, 1, 50, 100, false)})
	if sampled.GetMemoryUtilizationP50() != 0 || sampled.GetMemoryUtilizationP95() != 0 {
		t.Fatalf("a row with only sampled peaks has percentiles %v/%v", sampled.GetMemoryUtilizationP50(), sampled.GetMemoryUtilizationP95())
	}
}

// TestFoldUsage_MeasuredUsageNeverMisstates is the reviews' cases: a record
// read back with figures no sampler writes neither panics nor counts; an
// attempt stopped at its limit counts at it; efficiency compares only the
// measured attempts that carry a reservation; a bucket's edge lands in that
// bucket; a reason this engine does not know is counted under UNSPECIFIED;
// and each group's percentiles are its own.
func TestFoldUsage_MeasuredUsageNeverMisstates(t *testing.T) {
	fold := func(records ...*orkstackv1.TaskAttempt) *orkstackv1.UsageRow {
		s := engine.TaskSample{RunID: "r", Orkflow: "o", TaskID: "t", Pool: "main", Observed: true, Attempts: int32(len(records)), Records: records}
		total, _, _ := engine.FoldUsage([]engine.UsageRun{{RunID: "r", Orkflow: "o", Tasks: []engine.UsageTask{engine.UsageOfTask(s)}}}, nil)
		return total
	}

	t.Run("an enormous peak against a tiny limit is at the limit, no panic", func(t *testing.T) {
		row := fold(measuredRecord(1, 1, 1<<62, 1, true))
		if row.GetMemoryUtilizationP95() != 1 {
			t.Fatalf("p95 = %v, want 1", row.GetMemoryUtilizationP95())
		}
	})
	t.Run("figures no sampler writes count the attempt and add nothing", func(t *testing.T) {
		bad := measuredRecord(1, math.NaN(), -5, 100, true)
		row := fold(bad, measuredRecord(2, 2, 10, 100, true))
		if row.GetAttemptsMeasured() != 2 || row.GetMeasuredCpuSeconds() != 2 || row.GetMemoryPeakBytesMax() != 10 {
			t.Fatalf("row = %v", row)
		}
	})
	t.Run("an attempt stopped at its limit counts at it", func(t *testing.T) {
		oom := measuredRecord(1, 1, 60, 100, false) // its peak not the kernel's
		oom.Measured.OomKilled = true
		row := fold(oom, measuredRecord(2, 1, 30, 100, true))
		if row.GetMemoryUtilizationP95() != 1 || math.Abs(row.GetMemoryUtilizationP50()-0.305) > 1e-9 {
			t.Fatalf("p50 %v, p95 %v; want 0.305 and 1 (the OOM at its limit)", row.GetMemoryUtilizationP50(), row.GetMemoryUtilizationP95())
		}
	})
	t.Run("efficiency is one population", func(t *testing.T) {
		unpriced := measuredRecord(1, 50, 1, 100, true)
		unpriced.Resources = nil
		row := fold(unpriced, measuredRecord(2, 5, 1, 100, true)) // 5 cpu-s of 20 reserved
		if row.GetMeasuredCpuSeconds() != 55 || row.GetReservedCpuSecondsMeasured() != 20 || row.GetCpuEfficiency() != 0.25 {
			t.Fatalf("measured %v, reserved %v, efficiency %v; want 55, 20, 0.25",
				row.GetMeasuredCpuSeconds(), row.GetReservedCpuSecondsMeasured(), row.GetCpuEfficiency())
		}
	})
	t.Run("edges", func(t *testing.T) {
		for _, tc := range []struct {
			peak, limit int64
			want        float64
		}{
			{0, 100, 0.005},     // [0, 0.5 %)
			{95, 100, 0.955},    // 95 % exactly is the start of its bucket
			{90, 100, 0.905},    // so is 90 %
			{199, 200, 1},       // 99.5 %: the bucket below the limit
			{100, 100, 1},       // at the limit
			{130, 100, 1},       // over it: at or above the limit reads 1
			{1, 1 << 40, 0.005}, // a tiny share of a large limit
		} {
			if got := fold(measuredRecord(1, 1, tc.peak, tc.limit, true)).GetMemoryUtilizationP50(); math.Abs(got-tc.want) > 1e-12 {
				t.Errorf("%d of %d: p50 = %v, want %v", tc.peak, tc.limit, got, tc.want)
			}
		}
	})
	t.Run("an unknown reason counts under UNSPECIFIED", func(t *testing.T) {
		newer := heldRecord(1, time.Second, nil)
		newer.Measured = &orkstackv1.MeasuredUsage{Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED, UnmeasuredReason: 99}
		older := heldRecord(2, time.Second, nil)
		got := fold(newer, older).GetAttemptsUnmeasuredByReason()
		if len(got) != 1 || got[0].GetReason() != orkstackv1.UnmeasuredReason_UNMEASURED_REASON_UNSPECIFIED || got[0].GetAttempts() != 2 {
			t.Fatalf("unmeasured by reason = %v, want UNSPECIFIED: 2", got)
		}
	})
	t.Run("each group's percentiles are its own", func(t *testing.T) {
		low := engine.TaskSample{RunID: "r", Orkflow: "o", TaskID: "low", Pool: "main", Observed: true, Attempts: 1,
			Records: []*orkstackv1.TaskAttempt{measuredRecord(1, 1, 10, 100, true)}}
		high := engine.TaskSample{RunID: "r", Orkflow: "o", TaskID: "high", Pool: "main", Observed: true, Attempts: 1,
			Records: []*orkstackv1.TaskAttempt{measuredRecord(1, 1, 90, 100, true)}}
		total, rows, _ := engine.FoldUsage([]engine.UsageRun{{RunID: "r", Orkflow: "o", Tasks: []engine.UsageTask{engine.UsageOfTask(low), engine.UsageOfTask(high)}}},
			[]orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK})
		byTask := map[string]float64{}
		for _, row := range rows {
			byTask[row.GetTaskId()] = row.GetMemoryUtilizationP95()
		}
		if math.Abs(byTask["low"]-0.105) > 1e-12 || math.Abs(byTask["high"]-0.905) > 1e-12 || math.Abs(total.GetMemoryUtilizationP95()-0.905) > 1e-12 {
			t.Fatalf("p95 by task %v, total %v", byTask, total.GetMemoryUtilizationP95())
		}
	})
}

// TestFoldUsage_NothingToCompareIsUnsetNeverZero: a row with no measured
// record carrying a reservation has no efficiency, and one with no record
// the percentiles cover has no percentiles and says it covered none; unset
// is "nothing to compare", which a client renders as "—", never 0 %.
func TestFoldUsage_NothingToCompareIsUnsetNeverZero(t *testing.T) {
	older := heldRecord(1, 10*time.Second, reserve(2, 1)) // a v0.3 Orker's: nothing measured
	sampled := measuredRecord(2, 1, 50, 100, false)       // measured, but its peak sampled
	sampled.Resources = nil
	s := engine.TaskSample{RunID: "r", Orkflow: "o", TaskID: "t", Pool: "main", Observed: true, Attempts: 2,
		Records: []*orkstackv1.TaskAttempt{older, sampled}}
	total, _, _ := engine.FoldUsage([]engine.UsageRun{{RunID: "r", Orkflow: "o", Tasks: []engine.UsageTask{engine.UsageOfTask(s)}}}, nil)
	if total.CpuEfficiency != nil || total.MemoryUtilizationP50 != nil || total.MemoryUtilizationP95 != nil {
		t.Fatalf("efficiency %v, p50 %v, p95 %v; want all unset", total.CpuEfficiency, total.MemoryUtilizationP50, total.MemoryUtilizationP95)
	}
	if total.GetMemoryUtilizationAttempts() != 0 || total.GetAttemptsMeasured() != 1 {
		t.Fatalf("utilization attempts %d, measured %d; want 0 and 1", total.GetMemoryUtilizationAttempts(), total.GetAttemptsMeasured())
	}
	covered := engine.TaskSample{RunID: "r", Orkflow: "o", TaskID: "t", Pool: "main", Observed: true, Attempts: 1,
		Records: []*orkstackv1.TaskAttempt{measuredRecord(1, 0, 0, 100, true)}}
	total, _, _ = engine.FoldUsage([]engine.UsageRun{{RunID: "r", Orkflow: "o", Tasks: []engine.UsageTask{engine.UsageOfTask(covered)}}}, nil)
	if total.CpuEfficiency == nil || total.GetCpuEfficiency() != 0 || total.MemoryUtilizationP50 == nil || total.GetMemoryUtilizationAttempts() != 1 {
		t.Fatalf("a measured idle attempt: %v; want efficiency set to 0 and one covered attempt", total)
	}
}
