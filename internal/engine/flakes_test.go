package engine_test

import (
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe the flake index: what one dispatched task of one
// finished run says about retries, and how those samples fold into groups.
// Every function under test is pure: no store, no clock, no Temporal.

var flakeEpoch = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

const (
	outcomeFailed    = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
	outcomeSucceeded = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
	outcomeCanceled  = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED
)

// flakeTask is one dispatched task snapshot with its final attempt number.
func flakeTask(id, pool string, attempt int32) *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{TaskId: id, Pool: pool, Attempt: attempt, Status: orkstackv1.Status_STATUS_SUCCEEDED}
}

// attemptRecord is one stored TaskAttempt.
func attemptRecord(attempt int32, outcome orkstackv1.AttemptOutcome, orker, phase, code string,
	category orkstackv1.FailureCategory,
) *orkstackv1.TaskAttempt {
	record := &orkstackv1.TaskAttempt{Attempt: attempt, Outcome: outcome, OrkerId: orker, Phase: phase}
	if code != "" || category != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		record.Error = &orkstackv1.Error{Code: code, Category: category}
	}
	return record
}

// failedOn is a non-final attempt that exited non-zero on one Orker.
func failedOn(attempt int32, orker string) *orkstackv1.TaskAttempt {
	return attemptRecord(attempt, outcomeFailed, orker, "exit", "CONTAINER_EXIT_NONZERO", catActivity)
}

// succeededOn is a final attempt that succeeded on one Orker.
func succeededOn(attempt int32, orker string) *orkstackv1.TaskAttempt {
	return attemptRecord(attempt, outcomeSucceeded, orker, "commit", "", orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED)
}

// flakeSample builds one sample through SampleTask, the way the handler does.
func flakeSample(runID string, createdMinutes int, orkflow string, task *orkstackv1.TaskRun,
	records ...*orkstackv1.TaskAttempt,
) engine.TaskSample {
	entry := artifacts.RunIndexEntry{
		RunID:   runID,
		Orkflow: orkflow,
		Status:  "succeeded",
		Time:    flakeEpoch.Add(time.Duration(createdMinutes) * time.Minute),
	}
	run := &orkstackv1.Run{Id: runID, OrkflowName: orkflow, TaskRuns: []*orkstackv1.TaskRun{task}}
	return engine.SampleTask(entry, run, task, records)
}

func TestDispatched_MeansTheTaskRunsPoolIsSet(t *testing.T) {
	for _, testCase := range []struct {
		name string
		task *orkstackv1.TaskRun
		want bool
	}{
		{"a task that reached an Orker", flakeTask("build", "main", 1), true},
		{"a skipped task", &orkstackv1.TaskRun{TaskId: "notify", Status: orkstackv1.Status_STATUS_SKIPPED}, false},
		{"a task canceled before dispatch", &orkstackv1.TaskRun{TaskId: "later", Status: orkstackv1.Status_STATUS_CANCELED}, false},
		{"a task of a run whose start failed", &orkstackv1.TaskRun{TaskId: "first", Status: orkstackv1.Status_STATUS_PENDING}, false},
		// An await (docs/DSL.md "Awaited tasks") reaches no Orker: it is never
		// a flake sample and contributes no namespace-budget usage.
		{"a resolved await", &orkstackv1.TaskRun{TaskId: "approval", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Await: &orkstackv1.TaskAwait{ResolvedVia: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API}}, false},
		{"a nil task", nil, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := engine.Dispatched(testCase.task); got != testCase.want {
				t.Errorf("Dispatched = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestSampleTask_AttemptsIsTheHighestRecordOrTheSnapshotsAttemptWhicheverIsHigher(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		task    *orkstackv1.TaskRun
		records []*orkstackv1.TaskAttempt
		want    int32
	}{
		{"the snapshot alone", flakeTask("t", "main", 3), []*orkstackv1.TaskAttempt{succeededOn(3, "a")}, 3},
		{"a timed-out task carries attempt 0", flakeTask("t", "main", 0),
			[]*orkstackv1.TaskAttempt{failedOn(1, "a"), failedOn(2, "a")}, 2},
		{"an adopted commit names the committing attempt", flakeTask("t", "main", 1),
			[]*orkstackv1.TaskAttempt{failedOn(1, "a"), succeededOn(2, "b")}, 2},
		{"a snapshot ahead of its records", flakeTask("t", "main", 4),
			[]*orkstackv1.TaskAttempt{failedOn(1, "a")}, 4},
		{"no record is no attempts", flakeTask("t", "main", 2), nil, 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sample := flakeSample("ork-1", 0, "nightly", testCase.task, testCase.records...)
			if sample.Attempts != testCase.want {
				t.Errorf("Attempts = %d, want %d", sample.Attempts, testCase.want)
			}
		})
	}
}

func TestSampleTask_DropsZeroAndDuplicateAttemptRecordsAsTheTimelineDoes(t *testing.T) {
	first := failedOn(1, "a")
	sample := flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 2),
		attemptRecord(0, outcomeFailed, "x", "exit", "BOGUS", catActivity),
		first,
		failedOn(1, "duplicate"),
		succeededOn(2, "b"),
	)
	if len(sample.Records) != 2 {
		t.Fatalf("Records = %d, want 2 (attempt 0 and the duplicate of 1 dropped)", len(sample.Records))
	}
	if sample.Records[0] != first {
		t.Errorf("the first record for attempt 1 must win; got orker %q", sample.Records[0].GetOrkerId())
	}
	if sample.Records[1].GetAttempt() != 2 {
		t.Errorf("records must ascend by attempt; second is %d", sample.Records[1].GetAttempt())
	}
}

func TestSampleTask_NonFinalRecordsAreTheOnesBelowTheHighestAttempt(t *testing.T) {
	sample := flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 3),
		failedOn(1, "a"), failedOn(2, "b"), succeededOn(3, "c"))
	nonFinal := sample.NonFinal()
	if len(nonFinal) != 2 || nonFinal[0].GetAttempt() != 1 || nonFinal[1].GetAttempt() != 2 {
		t.Fatalf("NonFinal = %v, want attempts 1 and 2", nonFinal)
	}
	if !sample.Retried() {
		t.Error("a task that took three attempts retried")
	}
	once := flakeSample("ork-2", 0, "nightly", flakeTask("t", "main", 1), succeededOn(1, "a"))
	if once.Retried() || len(once.NonFinal()) != 0 {
		t.Errorf("a one-attempt task did not retry: Retried=%v NonFinal=%d", once.Retried(), len(once.NonFinal()))
	}
}

func TestSampleTask_UnreportedCountsAttemptsWithNoRecord(t *testing.T) {
	sample := flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 3), succeededOn(3, "c"))
	if got := sample.Unreported(); got != 2 {
		t.Errorf("Unreported = %d, want 2 (attempts 1 and 2 left no record)", got)
	}
	whole := flakeSample("ork-2", 0, "nightly", flakeTask("t", "main", 2), failedOn(1, "a"), succeededOn(2, "b"))
	if got := whole.Unreported(); got != 0 {
		t.Errorf("Unreported = %d, want 0", got)
	}
}

func TestSampleTask_ADispatchedTaskWithNoRecordIsUnobservedNotClean(t *testing.T) {
	sample := flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 1))
	if sample.Observed {
		t.Fatal("a dispatched task with no record is evidence missing, never observed")
	}
	if sample.Attempts != 0 || sample.Retried() || sample.Unreported() != 0 {
		t.Errorf("an unobserved sample claims attempts=%d retried=%v unreported=%d",
			sample.Attempts, sample.Retried(), sample.Unreported())
	}
}

func TestSampleTask_AnAdoptedCommitOnAttemptTwoIsARetry(t *testing.T) {
	adopted := succeededOn(2, "b")
	adopted.CommitAdopted = true
	// Attempt 1 wrote commit.json and died before reporting; the snapshot
	// names that committing attempt. Attempt 2 adopted the commit.
	sample := flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 1), adopted)
	if !sample.Retried() || sample.Attempts != 2 {
		t.Errorf("an adopted commit on attempt 2 is a retry: Attempts=%d Retried=%v", sample.Attempts, sample.Retried())
	}
	if sample.Unreported() != 1 {
		t.Errorf("attempt 1 left no record: Unreported=%d, want 1", sample.Unreported())
	}
}

// groupOf finds one group by orkflow and task, or fails.
func groupOf(t *testing.T, groups []*orkstackv1.FlakeGroup, orkflow, task string) *orkstackv1.FlakeGroup {
	t.Helper()
	for _, group := range groups {
		if group.GetOrkflowName() == orkflow && group.GetTaskId() == task {
			return group
		}
	}
	t.Fatalf("no group %s/%s in %v", orkflow, task, groups)
	return nil
}

func TestGroupFlakes_DefaultKeyIsOrkflowAndTask(t *testing.T) {
	samples := []engine.TaskSample{
		flakeSample("ork-1", 0, "nightly", flakeTask("fetch", "main", 1), succeededOn(1, "a")),
		flakeSample("ork-2", 0, "reconcile", flakeTask("fetch", "gpu", 1), succeededOn(1, "a")),
	}
	groups, totals := engine.GroupFlakes(samples, nil, 0)
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want one per (orkflow, task)", len(groups))
	}
	for _, group := range groups {
		if group.GetPool() != "" || group.GetOrkerId() != "" {
			t.Errorf("a dimension not grouped by must be empty: %v", group)
		}
	}
	if !totals.BodyKeyed {
		t.Error("the default key names no record-level dimension")
	}
	want := []orkstackv1.FailureGroupBy{dimOrkflow, dimTask}
	if !reflect.DeepEqual(totals.GroupBy, want) {
		t.Errorf("resolved group_by = %v, want %v", totals.GroupBy, want)
	}
}

func TestGroupFlakes_OrkflowIsAlwaysTheFirstKeyComponent(t *testing.T) {
	samples := []engine.TaskSample{
		flakeSample("ork-1", 0, "nightly", flakeTask("fetch", "main", 1), succeededOn(1, "a")),
		flakeSample("ork-2", 0, "reconcile", flakeTask("fetch", "main", 1), succeededOn(1, "a")),
	}
	groups, totals := engine.GroupFlakes(samples, []orkstackv1.FailureGroupBy{dimPool}, 0)
	if len(groups) != 2 {
		t.Fatalf("grouping by pool alone still separates orkflows: %d groups", len(groups))
	}
	want := []orkstackv1.FailureGroupBy{dimOrkflow, dimPool}
	if !reflect.DeepEqual(totals.GroupBy, want) {
		t.Errorf("resolved group_by = %v, want %v", totals.GroupBy, want)
	}
	// Named explicitly, in any position, it is kept where the caller put it
	// and not added twice.
	_, named := engine.GroupFlakes(samples, []orkstackv1.FailureGroupBy{dimTask, dimOrkflow}, 0)
	if !reflect.DeepEqual(named.GroupBy, []orkstackv1.FailureGroupBy{dimTask, dimOrkflow}) {
		t.Errorf("an explicit ORKFLOW is kept in the caller's order: %v", named.GroupBy)
	}
}

func TestGroupFlakes_CodeAndCategoryAreRefusedAsDimensions(t *testing.T) {
	for _, requested := range [][]orkstackv1.FailureGroupBy{
		{dimCode},
		{dimTask, dimCategory},
		{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED},
		{dimTask, dimTask},
		{orkstackv1.FailureGroupBy(99)},
	} {
		if _, err := engine.ResolveFlakeGroupBy(requested); err == nil {
			t.Errorf("ResolveFlakeGroupBy(%v) accepted what it must refuse", requested)
		}
	}
	resolved, err := engine.ResolveFlakeGroupBy(nil)
	if err != nil || !reflect.DeepEqual(resolved, []orkstackv1.FailureGroupBy{dimOrkflow, dimTask}) {
		t.Errorf("an empty request resolves to ORKFLOW, TASK: %v, %v", resolved, err)
	}
	// GroupFlakes itself never keys by them, whatever it is handed.
	samples := []engine.TaskSample{
		flakeSample("ork-1", 0, "nightly", flakeTask("fetch", "main", 2), failedOn(1, "a"), succeededOn(2, "a")),
		flakeSample("ork-2", 1, "nightly", flakeTask("fetch", "main", 2),
			attemptRecord(1, outcomeFailed, "a", "pull", "IMAGE_PULL_FAILED", catInfra), succeededOn(2, "a")),
	}
	groups, totals := engine.GroupFlakes(samples, []orkstackv1.FailureGroupBy{dimTask, dimCode, dimCategory}, 0)
	if len(groups) != 1 {
		t.Errorf("code and category split a group: %d groups", len(groups))
	}
	for _, dim := range totals.GroupBy {
		if dim == dimCode || dim == dimCategory {
			t.Errorf("resolved group_by carries %s", dim)
		}
	}
}

func TestGroupFlakes_ARunIsCountedOnceHoweverManyMemberTasksItHas(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-1", OrkflowName: "nightly"}
	entry := artifacts.RunIndexEntry{RunID: "ork-1", Time: flakeEpoch}
	fetch, parse := flakeTask("fetch", "main", 3), flakeTask("parse", "main", 1)
	samples := []engine.TaskSample{
		engine.SampleTask(entry, run, fetch, []*orkstackv1.TaskAttempt{failedOn(1, "a"), failedOn(2, "a"), succeededOn(3, "a")}),
		engine.SampleTask(entry, run, parse, []*orkstackv1.TaskAttempt{succeededOn(1, "a")}),
	}
	groups, _ := engine.GroupFlakes(samples, []orkstackv1.FailureGroupBy{dimOrkflow}, 0)
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want the one orkflow", len(groups))
	}
	group := groups[0]
	if group.GetRunsObserved() != 1 || group.GetRunsRetried() != 1 {
		t.Errorf("one run: observed=%d retried=%d, want 1 and 1", group.GetRunsObserved(), group.GetRunsRetried())
	}
	if group.GetP50Attempts() != 3 {
		t.Errorf("the per-run sample is the maximum over member tasks: p50=%d, want 3", group.GetP50Attempts())
	}
	if group.GetRetryAttempts() != 2 {
		t.Errorf("retry_attempts sums non-final records over member tasks: %d, want 2", group.GetRetryAttempts())
	}
}

func TestGroupFlakes_ABodyKeyedGroupWithNoObservedRunHasAnUndefinedRateAndAMissingCount(t *testing.T) {
	samples := []engine.TaskSample{
		flakeSample("ork-1", 0, "nightly", flakeTask("warm-cache", "main", 1)),
		flakeSample("ork-2", 1, "nightly", flakeTask("warm-cache", "main", 2)),
		flakeSample("ork-2", 1, "nightly", flakeTask("post", "main", 1), succeededOn(1, "a")),
	}
	groups, totals := engine.GroupFlakes(samples, nil, 0)
	warm := groupOf(t, groups, "nightly", "warm-cache")
	if warm.GetRetryRateDefined() || warm.GetRunsObserved() != 0 || warm.GetRetryRate() != 0 {
		t.Errorf("an unobservable task: defined=%v observed=%d rate=%v",
			warm.GetRetryRateDefined(), warm.GetRunsObserved(), warm.GetRetryRate())
	}
	if warm.GetRunsEvidenceMissing() != 2 {
		t.Errorf("runs_evidence_missing = %d, want 2", warm.GetRunsEvidenceMissing())
	}
	if warm.GetP50Attempts() != 0 || warm.GetP95Attempts() != 0 {
		t.Errorf("percentiles over nothing are unset: %d/%d", warm.GetP50Attempts(), warm.GetP95Attempts())
	}
	post := groupOf(t, groups, "nightly", "post")
	if !post.GetRetryRateDefined() || post.GetRetryRate() != 0 {
		t.Errorf("an observed clean task is a defined 0%%: %v", post)
	}
	if groups[len(groups)-1] != warm {
		t.Error("an undefined rate orders last")
	}
	if totals.RunsEvidenceMissing != 2 || totals.RunsEvidenceComplete != 0 {
		t.Errorf("totals: complete=%d missing=%d, want 0 and 2", totals.RunsEvidenceComplete, totals.RunsEvidenceMissing)
	}
}

func TestGroupFlakes_ARecordKeyedGroupCountsARunRetriedOnlyWhereANonFinalAttemptCarriesItsKey(t *testing.T) {
	// Attempt 1 failed on Orker A, attempt 2 succeeded on Orker B.
	samples := []engine.TaskSample{
		flakeSample("ork-1", 0, "reconcile", flakeTask("fetch-rates", "main", 2), failedOn(1, "orker-a"), succeededOn(2, "orker-b")),
	}
	groups, totals := engine.GroupFlakes(samples, []orkstackv1.FailureGroupBy{dimTask, dimOrker}, 0)
	if totals.BodyKeyed {
		t.Fatal("a grouping by orker is record-keyed")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want one per Orker", len(groups))
	}
	byOrker := map[string]*orkstackv1.FlakeGroup{}
	for _, group := range groups {
		byOrker[group.GetOrkerId()] = group
	}
	a, b := byOrker["orker-a"], byOrker["orker-b"]
	if a == nil || b == nil {
		t.Fatalf("groups by orker: %v", groups)
	}
	if a.GetRunsObserved() != 1 || a.GetRunsRetried() != 1 || a.GetRetryRate() != 1 {
		t.Errorf("Orker A failed the attempt that caused the retry: %v", a)
	}
	if b.GetRunsObserved() != 1 || b.GetRunsRetried() != 0 || b.GetRetryRate() != 0 || !b.GetRetryRateDefined() {
		t.Errorf("Orker B succeeded; it must not be blamed: %v", b)
	}
	if a.GetDominantCode() != "CONTAINER_EXIT_NONZERO" || b.GetDominantCode() != "" {
		t.Errorf("dominant causes: A=%q B=%q", a.GetDominantCode(), b.GetDominantCode())
	}
	if a.GetRunsEvidenceMissing() != 0 || a.GetUnreportedAttempts() != 0 {
		t.Errorf("record-keyed groups carry no per-group evidence counts: %v", a)
	}
	if groups[0] != a {
		t.Error("the retrying Orker orders first")
	}
}

func TestGroupFlakes_AttemptsPerRunAreNearestRankOverAttemptsCarryingTheKey(t *testing.T) {
	var samples []engine.TaskSample
	// Ten runs: attempts 1..10 is too many for a real policy, so the task's
	// attempt counts are 1,1,1,1,1,2,2,2,3,4.
	for i, attempts := range []int32{1, 1, 1, 1, 1, 2, 2, 2, 3, 4} {
		records := make([]*orkstackv1.TaskAttempt, 0, attempts)
		for attempt := int32(1); attempt < attempts; attempt++ {
			records = append(records, failedOn(attempt, "orker-a"))
		}
		records = append(records, succeededOn(attempts, "orker-b"))
		samples = append(samples, flakeSample("ork-"+string(rune('a'+i)), i, "nightly", flakeTask("t", "main", attempts), records...))
	}
	groups, _ := engine.GroupFlakes(samples, nil, 0)
	if groups[0].GetP50Attempts() != 1 || groups[0].GetP95Attempts() != 4 {
		t.Errorf("body-keyed p50/p95 = %d/%d, want 1/4", groups[0].GetP50Attempts(), groups[0].GetP95Attempts())
	}
	byOrker, _ := engine.GroupFlakes(samples, []orkstackv1.FailureGroupBy{dimOrker}, 0)
	for _, group := range byOrker {
		switch group.GetOrkerId() {
		case "orker-a":
			// Five runs had attempts on A: 1,1,1,2,3.
			if group.GetRunsObserved() != 5 || group.GetP50Attempts() != 1 || group.GetP95Attempts() != 3 {
				t.Errorf("orker-a: observed=%d p50=%d p95=%d, want 5/1/3",
					group.GetRunsObserved(), group.GetP50Attempts(), group.GetP95Attempts())
			}
		case "orker-b":
			if group.GetRunsObserved() != 10 || group.GetP50Attempts() != 1 || group.GetP95Attempts() != 1 {
				t.Errorf("orker-b: observed=%d p50=%d p95=%d, want 10/1/1",
					group.GetRunsObserved(), group.GetP50Attempts(), group.GetP95Attempts())
			}
		default:
			t.Errorf("unexpected group %v", group)
		}
	}
}

func TestGroupFlakes_DominantCauseIsTheModeOfOneTripleNeverThreeModes(t *testing.T) {
	// Codes: X twice (both in phase "exit"), Y once in "pull", once in "exit".
	// Phases: "exit" three times. Three independent modes would name
	// (X, ACTIVITY, exit) — which is right here — so the counter-example
	// needs a combination no attempt had: code mode X, category mode INFRA.
	records := [][]*orkstackv1.TaskAttempt{
		{attemptRecord(1, outcomeFailed, "a", "exit", "X", catActivity), succeededOn(2, "a")},
		{attemptRecord(1, outcomeFailed, "a", "exit", "X", catActivity), succeededOn(2, "a")},
		{attemptRecord(1, outcomeFailed, "a", "pull", "Y", catInfra), succeededOn(2, "a")},
		{attemptRecord(1, outcomeFailed, "a", "pull", "Z", catInfra), succeededOn(2, "a")},
		{attemptRecord(1, outcomeFailed, "a", "pull", "W", catInfra), succeededOn(2, "a")},
	}
	var samples []engine.TaskSample
	for i, set := range records {
		samples = append(samples, flakeSample("ork-"+string(rune('a'+i)), i, "nightly", flakeTask("t", "main", 2), set...))
	}
	groups, _ := engine.GroupFlakes(samples, nil, 0)
	group := groups[0]
	// Independent modes: code X (2), category INFRA (3), phase pull (3) — a
	// triple no attempt had. The mode of the triple is (X, ACTIVITY, exit).
	if group.GetDominantCode() != "X" || group.GetDominantCategory() != catActivity ||
		group.GetDominantPhase() != "exit" || group.GetDominantCount() != 2 {
		t.Errorf("dominant = (%s, %s, %s) x%d, want (X, ACTIVITY_FAILED, exit) x2",
			group.GetDominantCode(), group.GetDominantCategory(), group.GetDominantPhase(), group.GetDominantCount())
	}
	if group.GetRetryAttempts() != 5 {
		t.Errorf("retry_attempts = %d, want 5", group.GetRetryAttempts())
	}

	// A tie breaks by code ascending.
	tied := []engine.TaskSample{
		flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 2), attemptRecord(1, outcomeFailed, "a", "exit", "B", catActivity), succeededOn(2, "a")),
		flakeSample("ork-2", 1, "nightly", flakeTask("t", "main", 2), attemptRecord(1, outcomeFailed, "a", "exit", "A", catActivity), succeededOn(2, "a")),
	}
	tiedGroups, _ := engine.GroupFlakes(tied, nil, 0)
	if tiedGroups[0].GetDominantCode() != "A" || tiedGroups[0].GetDominantCount() != 1 {
		t.Errorf("a tie breaks by code ascending: %q x%d", tiedGroups[0].GetDominantCode(), tiedGroups[0].GetDominantCount())
	}

	// No retry, no cause.
	clean, _ := engine.GroupFlakes([]engine.TaskSample{
		flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 1), succeededOn(1, "a")),
	}, nil, 0)
	if clean[0].GetDominantCode() != "" || clean[0].GetDominantCount() != 0 ||
		clean[0].GetDominantCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED ||
		clean[0].GetLastRetriedAt() != nil || len(clean[0].GetExampleRunIds()) != 0 {
		t.Errorf("a group with no retry carries no cause: %v", clean[0])
	}
}

func TestGroupFlakes_ACanceledNonFinalRecordIsItsOwnTriple(t *testing.T) {
	canceled := attemptRecord(1, outcomeCanceled, "a", "exit", "", orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED)
	samples := []engine.TaskSample{
		flakeSample("ork-1", 0, "nightly", flakeTask("t", "main", 2), canceled, succeededOn(2, "a")),
		flakeSample("ork-2", 1, "nightly", flakeTask("t", "main", 2),
			attemptRecord(1, outcomeCanceled, "a", "exit", "", orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED), succeededOn(2, "a")),
		flakeSample("ork-3", 2, "nightly", flakeTask("t", "main", 2), failedOn(1, "a"), succeededOn(2, "a")),
	}
	groups, _ := engine.GroupFlakes(samples, nil, 0)
	group := groups[0]
	if group.GetRunsRetried() != 3 {
		t.Errorf("a canceled non-final attempt is still a retry: runs_retried=%d", group.GetRunsRetried())
	}
	if group.GetDominantCode() != "" || group.GetDominantCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED ||
		group.GetDominantPhase() != "exit" || group.GetDominantCount() != 2 {
		t.Errorf("dominant = (%q, %s, %q) x%d, want the canceled triple x2",
			group.GetDominantCode(), group.GetDominantCategory(), group.GetDominantPhase(), group.GetDominantCount())
	}
}

func TestGroupFlakes_OrderIsTotalAndClockFree(t *testing.T) {
	samples := []engine.TaskSample{
		// b: 1 of 2 retried.
		flakeSample("ork-1", 0, "nightly", flakeTask("b", "main", 2), failedOn(1, "a"), succeededOn(2, "a")),
		flakeSample("ork-2", 1, "nightly", flakeTask("b", "main", 1), succeededOn(1, "a")),
		// a: 1 of 2 retried — ties b on every count, broken by task id.
		flakeSample("ork-1", 0, "nightly", flakeTask("a", "main", 2), failedOn(1, "a"), succeededOn(2, "a")),
		flakeSample("ork-2", 1, "nightly", flakeTask("a", "main", 1), succeededOn(1, "a")),
		// c: 1 of 1 retried — the highest rate.
		flakeSample("ork-1", 0, "nightly", flakeTask("c", "main", 2), failedOn(1, "a"), succeededOn(2, "a")),
		// d: unobservable — last.
		flakeSample("ork-1", 0, "nightly", flakeTask("d", "main", 1)),
		// e: 0 of 3 — below a and b.
		flakeSample("ork-1", 0, "nightly", flakeTask("e", "main", 1), succeededOn(1, "a")),
		flakeSample("ork-2", 1, "nightly", flakeTask("e", "main", 1), succeededOn(1, "a")),
		flakeSample("ork-3", 2, "nightly", flakeTask("e", "main", 1), succeededOn(1, "a")),
		// f: 0 of 1 — below e on runs_observed.
		flakeSample("ork-3", 2, "nightly", flakeTask("f", "main", 1), succeededOn(1, "a")),
	}
	order := func(in []engine.TaskSample) []string {
		groups, _ := engine.GroupFlakes(in, nil, 0)
		out := make([]string, len(groups))
		for i, group := range groups {
			out[i] = group.GetTaskId()
		}
		return out
	}
	want := []string{"c", "a", "b", "e", "f", "d"}
	reversed := make([]engine.TaskSample, len(samples))
	for i := range samples {
		reversed[len(samples)-1-i] = samples[i]
	}
	rotated := append(append([]engine.TaskSample(nil), samples[4:]...), samples[:4]...)
	for name, in := range map[string][]engine.TaskSample{"as given": samples, "reversed": reversed, "rotated": rotated} {
		if got := order(in); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: order = %v, want %v", name, got, want)
		}
	}
	first, _ := engine.GroupFlakes(samples, nil, 0)
	second, _ := engine.GroupFlakes(reversed, nil, 0)
	for i := range first {
		// Examples may differ only where the scan order breaks a creation-time
		// tie; none does here.
		if !proto.Equal(first[i], second[i]) {
			t.Errorf("group %d differs by input order:\n%v\n%v", i, first[i], second[i])
		}
	}
}

func TestGroupFlakes_ExamplesAreTheNewestRetriedRunsAtMostThreeAndNeverCompareIds(t *testing.T) {
	var samples []engine.TaskSample
	// Ids deliberately sort opposite to creation time; the newest has the
	// smallest id.
	ids := []string{"ork-z", "ork-y", "ork-x", "ork-w", "ork-v"}
	for i, id := range ids {
		samples = append(samples, flakeSample(id, i, "nightly", flakeTask("t", "main", 2), failedOn(1, "a"), succeededOn(2, "a")))
	}
	samples = append(samples, flakeSample("ork-newest-clean", 99, "nightly", flakeTask("t", "main", 1), succeededOn(1, "a")))
	// Two runs created at one instant keep the scan's order.
	samples = append(samples,
		flakeSample("ork-tie-first", 50, "nightly", flakeTask("t", "main", 2), failedOn(1, "a"), succeededOn(2, "a")),
		flakeSample("ork-tie-second", 50, "nightly", flakeTask("t", "main", 2), failedOn(1, "a"), succeededOn(2, "a")))
	groups, _ := engine.GroupFlakes(samples, nil, 0)
	got := groups[0].GetExampleRunIds()
	want := []string{"ork-tie-first", "ork-tie-second", "ork-v"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("examples = %v, want %v (newest retried first, never the clean run)", got, want)
	}
	if last := groups[0].GetLastRetriedAt(); !last.AsTime().Equal(flakeEpoch.Add(50 * time.Minute)) {
		t.Errorf("last_retried_at = %v, want the newest retried run's creation", last.AsTime())
	}
}

func TestGroupFlakes_LimitTruncatesGroupsButNotTotals(t *testing.T) {
	var samples []engine.TaskSample
	for _, task := range []string{"a", "b", "c", "d"} {
		samples = append(samples, flakeSample("ork-1", 0, "nightly", flakeTask(task, "main", 1), succeededOn(1, "a")))
	}
	samples = append(samples, flakeSample("ork-2", 1, "nightly", flakeTask("e", "main", 1)))
	groups, totals := engine.GroupFlakes(samples, nil, 2)
	if len(groups) != 2 {
		t.Errorf("groups = %d, want the limit of 2", len(groups))
	}
	if totals.GroupsTotal != 5 || totals.RunsDispatched != 2 || totals.RunsEvidenceMissing != 1 {
		t.Errorf("totals are over every group: %+v", totals)
	}
}

func TestGroupFlakes_ResponseTotalsSumDispatchedIntoCompleteAndMissing(t *testing.T) {
	samples := []engine.TaskSample{
		// ork-1: complete.
		flakeSample("ork-1", 0, "nightly", flakeTask("a", "main", 1), succeededOn(1, "x")),
		flakeSample("ork-1", 0, "nightly", flakeTask("b", "main", 1), succeededOn(1, "x")),
		// ork-2: partially observed — missing for the response, its observed
		// task still counts.
		flakeSample("ork-2", 1, "nightly", flakeTask("a", "main", 2), failedOn(1, "x"), succeededOn(2, "x")),
		flakeSample("ork-2", 1, "nightly", flakeTask("b", "main", 1)),
		// ork-3: nothing observed.
		flakeSample("ork-3", 2, "nightly", flakeTask("a", "main", 1)),
	}
	for _, groupBy := range [][]orkstackv1.FailureGroupBy{nil, {dimOrker}} {
		groups, totals := engine.GroupFlakes(samples, groupBy, 0)
		if totals.RunsDispatched != 3 || totals.RunsEvidenceComplete != 1 || totals.RunsEvidenceMissing != 2 {
			t.Errorf("%v: dispatched=%d complete=%d missing=%d, want 3/1/2", groupBy,
				totals.RunsDispatched, totals.RunsEvidenceComplete, totals.RunsEvidenceMissing)
		}
		if totals.RunsEvidenceComplete+totals.RunsEvidenceMissing != totals.RunsDispatched {
			t.Errorf("%v: complete + missing != dispatched", groupBy)
		}
		if groupBy == nil {
			a := groupOf(t, groups, "nightly", "a")
			if a.GetRunsObserved() != 2 || a.GetRunsRetried() != 1 || a.GetRunsEvidenceMissing() != 1 {
				t.Errorf("task a: %v", a)
			}
		}
	}
}
