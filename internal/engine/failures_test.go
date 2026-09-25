package engine_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe what makes two failed runs the SAME failure. Every
// function under test is pure: no store, no clock, no Temporal.

var fingerprintEpoch = time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)

const (
	catActivity = orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
	catInfra    = orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	catContract = orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
	catTimedOut = orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT

	dimOrkflow  = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW
	dimTask     = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK
	dimCode     = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE
	dimCategory = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CATEGORY
	dimImage    = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE
	dimPool     = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL
	dimPhase    = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE
	dimOrker    = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER
)

var defaultDims = []orkstackv1.FailureGroupBy{dimOrkflow, dimTask, dimCode, dimCategory}

// taskRun is one task of a snapshot. finished is an offset from the epoch;
// a negative offset means "no finished_at".
func taskRun(id string, status orkstackv1.Status, finished time.Duration, code string,
	category orkstackv1.FailureCategory) *orkstackv1.TaskRun {
	task := &orkstackv1.TaskRun{TaskId: id, Status: status}
	if finished >= 0 {
		task.FinishedAt = timestamppb.New(fingerprintEpoch.Add(finished))
	}
	if code != "" || category != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		task.Error = &orkstackv1.Error{Code: code, Category: category}
	}
	return task
}

func failedTask(id string, finished time.Duration, code string, category orkstackv1.FailureCategory) *orkstackv1.TaskRun {
	return taskRun(id, orkstackv1.Status_STATUS_FAILED, finished, code, category)
}

func primaryID(run *orkstackv1.Run) string {
	return engine.PrimaryFailedTask(run).GetTaskId()
}

func TestPrimaryFailedTask_IsTheEarliestFinishedFailedTask(t *testing.T) {
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		taskRun("build", orkstackv1.Status_STATUS_SUCCEEDED, time.Minute, "", 0),
		failedTask("deploy", 5*time.Minute, "CONTAINER_EXIT_NONZERO", catActivity),
		failedTask("ingest", 3*time.Minute, "IMAGE_PULL_FAILED", catInfra),
		failedTask("report", 4*time.Minute, "VALUES_MISSING", catContract),
	}}
	if got := primaryID(run); got != "ingest" {
		t.Fatalf("primary failed task is %q, want the earliest-finished failed task %q", got, "ingest")
	}
}

func TestPrimaryFailedTask_TiesAndMissingFinishedAtBreakByDeclarationOrder(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		tasks []*orkstackv1.TaskRun
		want  string
	}{
		{"a tie goes to the task declared first", []*orkstackv1.TaskRun{
			failedTask("b", time.Minute, "X", catActivity),
			failedTask("a", time.Minute, "Y", catActivity),
		}, "b"},
		{"a task with no finished_at sorts after one that has it", []*orkstackv1.TaskRun{
			failedTask("unfinished", -1, "X", catActivity),
			failedTask("finished", 10*time.Minute, "Y", catActivity),
		}, "finished"},
		{"two tasks with no finished_at go by declaration order", []*orkstackv1.TaskRun{
			failedTask("first", -1, "X", catActivity),
			failedTask("second", -1, "Y", catActivity),
		}, "first"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := primaryID(&orkstackv1.Run{TaskRuns: testCase.tasks}); got != testCase.want {
				t.Errorf("primary failed task is %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestPrimaryFailedTask_IgnoresTasksCanceledAroundTheFailure(t *testing.T) {
	// The run failed on "migrate"; "backfill" and "notify" were in flight and
	// were canceled around the failure — earlier, even, by the clock. A
	// canceled task is a consequence, never the cause.
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		taskRun("backfill", orkstackv1.Status_STATUS_CANCELED, time.Second, "CANCELED", orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED),
		taskRun("notify", orkstackv1.Status_STATUS_CANCELED, 2*time.Second, "", 0),
		failedTask("migrate", time.Minute, "PIN_HOST_LOST", catInfra),
	}}
	if got := primaryID(run); got != "migrate" {
		t.Fatalf("primary failed task is %q, want %q", got, "migrate")
	}
	onlyCanceled := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		taskRun("backfill", orkstackv1.Status_STATUS_CANCELED, time.Second, "", 0),
	}}
	if got := engine.PrimaryFailedTask(onlyCanceled); got != nil {
		t.Fatalf("a run with no FAILED task has primary %q, want none", got.GetTaskId())
	}
}

func indexEntry(id, orkflow string, created time.Duration) artifacts.RunIndexEntry {
	return artifacts.RunIndexEntry{RunID: id, Namespace: "payments", Orkflow: orkflow,
		Status: "failed", Time: fingerprintEpoch.Add(created)}
}

func TestFingerprintOf_ARunWithNoFailedTaskIsRunLevelAndNeverEvidenceMissing(t *testing.T) {
	run := &orkstackv1.Run{
		Id: "ork-1", OrkflowName: "nightly", Status: orkstackv1.Status_STATUS_FAILED,
		Error: &orkstackv1.Error{Code: "RUN_START_FAILED", Category: catInfra},
	}
	// attemptRead is true: the caller asked for image. A run-level failure
	// has no attempt to be missing.
	got := engine.FingerprintOf(indexEntry("ork-1", "nightly", 0), run, nil, true)
	want := engine.FailureRecord{
		RunID: "ork-1", CreatedAt: fingerprintEpoch, Orkflow: "nightly",
		Code: "RUN_START_FAILED", Category: catInfra,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FingerprintOf = %+v, want %+v", got, want)
	}
}

func TestFingerprintOf_ReadsCodeCategoryPoolAndOrkerOffTheTaskRun(t *testing.T) {
	task := failedTask("ingest", time.Minute, "IMAGE_PULL_FAILED", catInfra)
	task.Pool, task.OrkerId = "gpu", "orker-7"
	run := &orkstackv1.Run{
		Id: "ork-2", OrkflowName: "payments", Status: orkstackv1.Status_STATUS_FAILED,
		// The run's own error is the generic wrapper; the task's is the reason.
		Error:    &orkstackv1.Error{Code: "RUN_FAILED"},
		TaskRuns: []*orkstackv1.TaskRun{task},
	}
	got := engine.FingerprintOf(indexEntry("ork-2", "payments", time.Minute), run, nil, false)
	want := engine.FailureRecord{
		RunID: "ork-2", CreatedAt: fingerprintEpoch.Add(time.Minute), Orkflow: "payments",
		TaskID: "ingest", Code: "IMAGE_PULL_FAILED", Category: catInfra,
		Pool: "gpu", OrkerID: "orker-7",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FingerprintOf = %+v, want %+v", got, want)
	}

	// A snapshot archived before a field existed groups under the empty value.
	old := &orkstackv1.Run{Id: "ork-3", OrkflowName: "payments",
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "ingest", Status: orkstackv1.Status_STATUS_FAILED}}}
	gotOld := engine.FingerprintOf(indexEntry("ork-3", "payments", 0), old, nil, false)
	if gotOld.Code != "" || gotOld.Pool != "" || gotOld.OrkerID != "" ||
		gotOld.Category != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		t.Fatalf("an old snapshot invented values: %+v", gotOld)
	}
}

func TestFingerprintOf_ImageAndPhaseComeFromTheFinalAttemptRecord(t *testing.T) {
	task := failedTask("reconcile", time.Minute, "CONTAINER_EXIT_NONZERO", catActivity)
	run := &orkstackv1.Run{Id: "ork-4", OrkflowName: "reconcile", TaskRuns: []*orkstackv1.TaskRun{task}}
	attempt := &orkstackv1.TaskAttempt{
		Attempt: 3, Phase: "exit",
		Image: &orkstackv1.ImageRef{Pinned: "ghcr.io/acme/reconcile@sha256:ab12"},
	}
	got := engine.FingerprintOf(indexEntry("ork-4", "reconcile", 0), run, attempt, true)
	if got.Image != "ghcr.io/acme/reconcile@sha256:ab12" || got.Phase != "exit" {
		t.Fatalf("image %q phase %q, want the attempt record's", got.Image, got.Phase)
	}
	if !got.AttemptRead || got.AttemptMissing {
		t.Fatalf("AttemptRead=%v AttemptMissing=%v, want read and present", got.AttemptRead, got.AttemptMissing)
	}
}

func TestFingerprintOf_AMissingAttemptRecordIsUnavailableNotEmpty(t *testing.T) {
	task := failedTask("reconcile", time.Minute, "CONTAINER_EXIT_NONZERO", catActivity)
	run := &orkstackv1.Run{Id: "ork-5", OrkflowName: "reconcile", TaskRuns: []*orkstackv1.TaskRun{task}}

	looked := engine.FingerprintOf(indexEntry("ork-5", "reconcile", 0), run, nil, true)
	if !looked.AttemptRead || !looked.AttemptMissing {
		t.Fatalf("a record looked for and not found reads AttemptRead=%v AttemptMissing=%v, want both true",
			looked.AttemptRead, looked.AttemptMissing)
	}
	notLooked := engine.FingerprintOf(indexEntry("ork-5", "reconcile", 0), run, nil, false)
	if notLooked.AttemptRead || notLooked.AttemptMissing {
		t.Fatalf("a record nobody looked for reads AttemptRead=%v AttemptMissing=%v, want neither",
			notLooked.AttemptRead, notLooked.AttemptMissing)
	}
}

func TestFinalAttemptRecord_PrefersTheTaskRunsAttemptThenTheHighest(t *testing.T) {
	records := []*orkstackv1.TaskAttempt{
		{Attempt: 1, Phase: "image"},
		{Attempt: 2, Phase: "run"},
		{Attempt: 3, Phase: "timeout"},
	}
	for _, testCase := range []struct {
		name    string
		attempt int32
		records []*orkstackv1.TaskAttempt
		want    string
	}{
		{"the task run's own attempt", 2, records, "run"},
		{"attempt 0 — a task Temporal timed out — takes the highest", 0, records, "timeout"},
		{"an attempt with no record of its own takes the highest", 7, records, "timeout"},
		{"no record at all is none", 2, nil, ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			task := &orkstackv1.TaskRun{TaskId: "t", Attempt: testCase.attempt}
			got := engine.FinalAttemptRecord(task, testCase.records)
			if got.GetPhase() != testCase.want {
				t.Errorf("FinalAttemptRecord picked phase %q, want %q", got.GetPhase(), testCase.want)
			}
		})
	}
}

// record is a fully-filled FailureRecord for grouping tests.
func record(id string, created time.Duration, orkflow, task, code string,
	category orkstackv1.FailureCategory) engine.FailureRecord {
	return engine.FailureRecord{
		RunID: id, CreatedAt: fingerprintEpoch.Add(created), Orkflow: orkflow,
		TaskID: task, Code: code, Category: category,
		Pool: "main", OrkerID: "orker-1", Image: "img@sha256:" + task, Phase: "exit",
	}
}

// corpus is the research narrative in miniature: seventeen image pulls on one
// pool against two reconcile failures and one run-level start failure.
func corpus() []engine.FailureRecord {
	var out []engine.FailureRecord
	for i := 0; i < 17; i++ {
		r := record(fmt.Sprintf("ork-ingest-%02d", i), time.Duration(i)*time.Minute,
			"payments", "ingest", "IMAGE_PULL_FAILED", catInfra)
		r.Pool, r.Phase = "gpu", "image"
		out = append(out, r)
	}
	out = append(out,
		record("ork-rec-a", 30*time.Minute, "reconcile", "reconcile", "CONTAINER_EXIT_NONZERO", catActivity),
		record("ork-rec-b", 40*time.Minute, "reconcile", "reconcile", "CONTAINER_EXIT_NONZERO", catActivity),
		engine.FailureRecord{RunID: "ork-night", CreatedAt: fingerprintEpoch.Add(-time.Hour),
			Orkflow: "nightly", Code: "RUN_START_FAILED", Category: catInfra},
	)
	return out
}

func TestGroupFailures_DefaultDimensionsAreOrkflowTaskCodeCategory(t *testing.T) {
	want := []orkstackv1.FailureGroupBy{dimOrkflow, dimTask, dimCode, dimCategory}
	if !reflect.DeepEqual(engine.DefaultFailureGroupBy(), want) {
		t.Fatalf("DefaultFailureGroupBy = %v, want %v", engine.DefaultFailureGroupBy(), want)
	}
	groups, totals := engine.GroupFailures(corpus(), nil, 0, 0)
	if len(groups) != 3 || totals.GroupsTotal != 3 {
		t.Fatalf("got %d groups (total %d), want 3", len(groups), totals.GroupsTotal)
	}
	first := groups[0]
	if first.GetOrkflowName() != "payments" || first.GetTaskId() != "ingest" ||
		first.GetCode() != "IMAGE_PULL_FAILED" || first.GetCategory() != catInfra || first.GetRunCount() != 17 {
		t.Fatalf("worst group = %v, want payments/ingest/IMAGE_PULL_FAILED/INFRASTRUCTURE_ERROR ×17", first)
	}
	// The pool is not a default dimension, so it is left empty.
	if first.GetPool() != "" || first.GetImagePinned() != "" || first.GetPhase() != "" || first.GetOrkerId() != "" {
		t.Fatalf("an ungrouped dimension carries a value: %v", first)
	}
}

func TestGroupFailures_UnrequestedDimensionsStayEmpty(t *testing.T) {
	groups, _ := engine.GroupFailures(corpus(), []orkstackv1.FailureGroupBy{dimPool}, 0, 0)
	if len(groups) != 3 {
		t.Fatalf("grouping by pool alone gave %d groups, want 3 (gpu, main, and none)", len(groups))
	}
	for _, group := range groups {
		if group.GetOrkflowName() != "" || group.GetTaskId() != "" || group.GetCode() != "" ||
			group.GetCategory() != 0 || group.GetImagePinned() != "" || group.GetPhase() != "" ||
			group.GetOrkerId() != "" {
			t.Errorf("a group by pool alone carries another dimension: %v", group)
		}
	}
	if groups[0].GetPool() != "gpu" || groups[0].GetRunCount() != 17 {
		t.Errorf("first group = %v, want gpu ×17", groups[0])
	}
	if groups[1].GetPool() != "main" || groups[1].GetRunCount() != 2 {
		t.Errorf("second group = %v, want main ×2", groups[1])
	}
	// The run-level failure has no pool: it groups under the empty value,
	// never under an invented one.
	if groups[2].GetPool() != "" || groups[2].GetRunCount() != 1 {
		t.Errorf("third group = %v, want the empty pool ×1", groups[2])
	}
}

func TestGroupFailures_OrderIsTotalAndClockFree(t *testing.T) {
	// Ties everywhere: equal counts, equal last_seen, so only the dimension
	// values can order them.
	base := []engine.FailureRecord{
		record("ork-a", time.Minute, "zeta", "t", "B", catActivity),
		record("ork-b", time.Minute, "alpha", "t", "B", catActivity),
		record("ork-c", time.Minute, "alpha", "t", "A", catActivity),
		record("ork-d", time.Minute, "alpha", "t", "A", catInfra),
		record("ork-e", 2*time.Minute, "mid", "t", "Z", catActivity),
	}
	orders := [][]int{{0, 1, 2, 3, 4}, {4, 3, 2, 1, 0}, {2, 4, 0, 3, 1}}
	var first []*orkstackv1.FailureGroup
	for n, order := range orders {
		records := make([]engine.FailureRecord, len(order))
		for i, j := range order {
			records[i] = base[j]
		}
		groups, _ := engine.GroupFailures(records, defaultDims, 0, 0)
		if n == 0 {
			first = groups
			var got []string
			for _, g := range groups {
				got = append(got, g.GetOrkflowName()+"/"+g.GetCode()+"/"+g.GetCategory().String())
			}
			want := []string{
				// last_seen desc puts the later run first.
				"mid/Z/FAILURE_CATEGORY_ACTIVITY_FAILED",
				"alpha/A/FAILURE_CATEGORY_ACTIVITY_FAILED",
				"alpha/A/FAILURE_CATEGORY_INFRASTRUCTURE_ERROR",
				"alpha/B/FAILURE_CATEGORY_ACTIVITY_FAILED",
				"zeta/B/FAILURE_CATEGORY_ACTIVITY_FAILED",
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("order = %v, want %v", got, want)
			}
			continue
		}
		if len(groups) != len(first) {
			t.Fatalf("input order %v changed the group count", order)
		}
		for i := range groups {
			if !proto.Equal(groups[i], first[i]) {
				t.Fatalf("input order %v put %v at %d, want %v", order, groups[i], i, first[i])
			}
		}
	}
}

func TestGroupFailures_CategoryFilterDropsRecordsBeforeGrouping(t *testing.T) {
	groups, totals := engine.GroupFailures(corpus(), defaultDims, catActivity, 0)
	if len(groups) != 1 || groups[0].GetRunCount() != 2 || groups[0].GetCategory() != catActivity {
		t.Fatalf("filtered groups = %v, want one ACTIVITY_FAILED group of 2", groups)
	}
	if totals.Grouped != 2 {
		t.Fatalf("failed_runs_grouped = %d after the filter, want 2", totals.Grouped)
	}
	// Grouping by a dimension that is NOT category still filters by it.
	groups, totals = engine.GroupFailures(corpus(), []orkstackv1.FailureGroupBy{dimOrkflow}, catTimedOut, 0)
	if len(groups) != 0 || totals.Grouped != 0 || totals.GroupsTotal != 0 {
		t.Fatalf("a category nothing has returned %v (%+v)", groups, totals)
	}
}

func TestGroupFailures_ExamplesAreNewestFirstAtMostThreeAndNeverCompareIds(t *testing.T) {
	// Ids are chosen so their lexical order is the OPPOSITE of their creation
	// order: an implementation that sorted ids would fail.
	records := []engine.FailureRecord{
		record("ork-z", 1*time.Minute, "o", "t", "C", catActivity),
		record("ork-y", 2*time.Minute, "o", "t", "C", catActivity),
		record("ork-x", 3*time.Minute, "o", "t", "C", catActivity),
		record("ork-w", 4*time.Minute, "o", "t", "C", catActivity),
		// Same instant as ork-w, later in the scan: the scan order breaks the tie.
		record("ork-a", 4*time.Minute, "o", "t", "C", catActivity),
	}
	groups, _ := engine.GroupFailures(records, defaultDims, 0, 0)
	if len(groups) != 1 {
		t.Fatalf("got %d groups, want 1", len(groups))
	}
	if got, want := groups[0].GetExampleRunIds(), []string{"ork-w", "ork-a", "ork-x"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("examples = %v, want %v", got, want)
	}
	if !groups[0].GetFirstSeen().AsTime().Equal(fingerprintEpoch.Add(time.Minute)) ||
		!groups[0].GetLastSeen().AsTime().Equal(fingerprintEpoch.Add(4*time.Minute)) {
		t.Fatalf("first/last seen = %s/%s", groups[0].GetFirstSeen().AsTime(), groups[0].GetLastSeen().AsTime())
	}
}

func TestGroupFailures_SumOfRunCountEqualsRecordsGroupedBeforeLimit(t *testing.T) {
	for _, dims := range [][]orkstackv1.FailureGroupBy{
		defaultDims,
		{dimOrker},
		{dimImage, dimPhase},
		{dimOrkflow, dimTask, dimCode, dimCategory, dimImage, dimPool, dimPhase, dimOrker},
	} {
		groups, totals := engine.GroupFailures(corpus(), dims, 0, 0)
		var sum int64
		for _, g := range groups {
			sum += g.GetRunCount()
		}
		if sum != int64(len(corpus())) || totals.Grouped != sum {
			t.Errorf("group by %v: sum(run_count)=%d grouped=%d, want both %d",
				dims, sum, totals.Grouped, len(corpus()))
		}
		if totals.GroupsTotal != int64(len(groups)) {
			t.Errorf("group by %v: groups_total=%d, want %d", dims, totals.GroupsTotal, len(groups))
		}
	}
}

func TestGroupFailures_LimitTruncatesGroupsButNotTotals(t *testing.T) {
	records := corpus()
	records[0].AttemptRead, records[0].AttemptMissing = true, true
	records[17].AttemptRead, records[17].AttemptMissing = true, true // reconcile, the second group

	groups, totals := engine.GroupFailures(records, defaultDims, 0, 1)
	if len(groups) != 1 || groups[0].GetRunCount() != 17 {
		t.Fatalf("limit 1 returned %v, want the worst group only", groups)
	}
	if totals.GroupsTotal != 3 || totals.Grouped != 20 {
		t.Fatalf("totals after limit = %+v, want 3 groups over 20 runs", totals)
	}
	if totals.AttemptEvidenceMissing != 2 {
		t.Fatalf("attempt_evidence_missing total = %d, want 2 (one of them in a truncated group)",
			totals.AttemptEvidenceMissing)
	}
	if groups[0].GetAttemptEvidenceMissing() != 1 {
		t.Fatalf("the worst group's own missing count = %d, want 1", groups[0].GetAttemptEvidenceMissing())
	}
}
