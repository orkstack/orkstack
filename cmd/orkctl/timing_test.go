package main

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// These are the task-timing-breakdown spec's CLI scenarios (C1-C9). The
// derivation table's rows T1-T8 are built once, as data, and every rendering
// test reads the same builders — the goldens and the fixture-driven command
// tests cannot drift apart.

var successPhases = []string{
	"start", "commit-check", "workspace", "stage-inputs", "image", "render",
	"create", "run", "exit", "validate", "commit",
}

func ms(n int64) time.Duration { return time.Duration(n) * time.Millisecond }

// spansFrom lays out contiguous spans from start with the given durations,
// and returns them with the instant the last one ends.
func spansFrom(start time.Time, names []string, durations ...time.Duration) ([]*orkstackv1.PhaseSpan, time.Time) {
	spans := make([]*orkstackv1.PhaseSpan, len(durations))
	at := start
	for i, d := range durations {
		spans[i] = &orkstackv1.PhaseSpan{
			Phase:      names[i],
			StartedAt:  timestamppb.New(at),
			FinishedAt: timestamppb.New(at.Add(d)),
		}
		at = at.Add(d)
	}
	return spans, at
}

func record(
	runID, taskID string, attempt int32, outcome orkstackv1.AttemptOutcome,
	start time.Time, names []string, durations ...time.Duration,
) *orkstackv1.TaskAttempt {
	spans, end := spansFrom(start, names, durations...)
	last := ""
	if len(spans) > 0 {
		last = spans[len(spans)-1].GetPhase()
	}
	return &orkstackv1.TaskAttempt{
		SchemaVersion: 1,
		RunId:         runID,
		TaskId:        taskID,
		Attempt:       attempt,
		Outcome:       outcome,
		StartedAt:     timestamppb.New(start),
		FinishedAt:    timestamppb.New(end),
		Phase:         last,
		Phases:        spans,
		OrkerId:       "orker-a1",
	}
}

func unreported(runID, taskID string, attempt int32) *orkstackv1.TaskAttempt {
	return &orkstackv1.TaskAttempt{
		RunId: runID, TaskId: taskID, Attempt: attempt,
		Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED,
	}
}

func at(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func ts(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }

// scenarioNightly is the spec's worked example — T1 (extract, load), T2
// (transform), T3 (verify) and T4 (report) in one run — exactly as §6.2
// renders it.
func scenarioNightly() (*orkstackv1.Run, []*orkstackv1.TaskAttempt) {
	const runID = "ork-20260922091401-1"
	succeeded := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
	failed := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED

	extractS := at("2026-09-22T09:14:02Z")
	extractB := extractS.Add(ms(2100))
	transformS := at("2026-09-22T09:15:07Z")
	transformB := transformS.Add(251 * time.Second)
	loadS := at("2026-09-22T09:20:10Z")
	loadB := loadS.Add(ms(1400))
	verifyS := at("2026-09-22T09:22:40Z")

	run := &orkstackv1.Run{
		Id: runID, Namespace: "data", OrkflowName: "nightly-etl", OrkflowVersion: "1.2.0",
		Status:     orkstackv1.Status_STATUS_FAILED,
		CreatedAt:  ts(at("2026-09-22T09:14:01Z")),
		StartedAt:  ts(at("2026-09-22T09:14:01Z")),
		FinishedAt: ts(at("2026-09-22T09:22:42Z")),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "extract", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, Pool: "main",
				ScheduledAt: ts(extractS), StartedAt: ts(extractB), FinishedAt: ts(extractB.Add(63 * time.Second))},
			{TaskId: "transform", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 3, Pool: "gpu",
				ScheduledAt: ts(transformS), StartedAt: ts(transformB), FinishedAt: ts(transformB.Add(52 * time.Second))},
			{TaskId: "load", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, Pool: "main",
				ScheduledAt: ts(loadS), StartedAt: ts(loadB), FinishedAt: ts(loadB.Add(150 * time.Second))},
			{TaskId: "verify", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 1, Pool: "main",
				ScheduledAt: ts(verifyS), FinishedAt: ts(verifyS.Add(2 * time.Second)),
				Error: &orkstackv1.Error{Code: "ORKER_HEARTBEAT_TIMEOUT", Message: "the orker stopped heartbeating"}},
			{TaskId: "report", Status: orkstackv1.Status_STATUS_SKIPPED},
		},
	}
	attempts := []*orkstackv1.TaskAttempt{
		record(runID, "extract", 1, succeeded, extractB, successPhases,
			ms(3), ms(40), ms(2), ms(105), ms(900), ms(4), ms(205), ms(61200), ms(1), ms(8), ms(500)),
		unreported(runID, "transform", 1),
		record(runID, "transform", 2, failed, at("2026-09-22T09:17:30Z"), successPhases[:9],
			ms(2), ms(38), ms(2), ms(97), ms(1100), ms(3), ms(190), ms(12400), ms(1)),
		record(runID, "transform", 3, succeeded, transformB, successPhases,
			ms(3), ms(41), ms(2), ms(118), ms(1200), ms(4), ms(210), ms(50100), ms(1), ms(9), ms(700)),
		record(runID, "load", 1, succeeded, loadB, successPhases,
			ms(3), ms(39), ms(2), ms(110), ms(800), ms(4), ms(200), ms(148400), ms(1), ms(8), ms(500)),
		// verify: an empty timeline — its records were reclaimed.
		// report: never scheduled, so nothing.
	}
	return run, attempts
}

// scenarioRender is §6.2's --task example: decided on attempt 2, and attempt
// 1 HAS a record, so the queue wait is a number.
func scenarioRender(runID string) (*orkstackv1.TaskRun, []*orkstackv1.TaskAttempt) {
	S := at("2026-09-22T10:02:00Z")
	B := S.Add(77 * time.Second)
	task := &orkstackv1.TaskRun{
		TaskId: "render", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2, Pool: "gpu",
		ScheduledAt: ts(S), StartedAt: ts(B), FinishedAt: ts(B.Add(48 * time.Second)),
	}
	return task, []*orkstackv1.TaskAttempt{
		record(runID, "render", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, S.Add(2*time.Second), successPhases[:9],
			ms(3), ms(39), ms(2), ms(99), ms(1100), ms(4), ms(201), ms(14900), ms(1)),
		record(runID, "render", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, B, successPhases,
			ms(3), ms(40), ms(2), ms(101), ms(1100), ms(4), ms(205), ms(46200), ms(1), ms(8), ms(600)),
	}
}

// scenarioPreContract is T5: a snapshot written before scheduled_at existed,
// whose started_at still holds the dispatch instant, and an attempt record
// written before phase spans existed.
func scenarioPreContract(runID string) (*orkstackv1.TaskRun, []*orkstackv1.TaskAttempt) {
	B := at("2026-09-22T11:00:03Z")
	task := &orkstackv1.TaskRun{
		TaskId: "legacy", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, Pool: "main",
		StartedAt: ts(B), FinishedAt: ts(B.Add(12 * time.Second)),
	}
	old := record(runID, "legacy", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, B.Add(ms(1500)), nil)
	old.Phase = "commit"
	old.FinishedAt = ts(B.Add(ms(11700)))
	return task, []*orkstackv1.TaskAttempt{old}
}

// scenarioSkew is T6: the Orker's clock is three seconds behind the
// workflow's, so started_at precedes scheduled_at.
func scenarioSkew(runID string) (*orkstackv1.TaskRun, []*orkstackv1.TaskAttempt) {
	S := at("2026-09-22T12:00:05Z")
	B := S.Add(-3 * time.Second)
	task := &orkstackv1.TaskRun{
		TaskId: "skewed", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2, Pool: "main",
		ScheduledAt: ts(S), StartedAt: ts(B), FinishedAt: ts(S.Add(13 * time.Second)),
	}
	return task, []*orkstackv1.TaskAttempt{
		record(runID, "skewed", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, S.Add(-7*time.Second),
			successPhases[:9], ms(3), ms(30), ms(2), ms(90), ms(700), ms(3), ms(150), ms(1200), ms(1)),
		record(runID, "skewed", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, B, successPhases,
			ms(3), ms(30), ms(2), ms(90), ms(700), ms(3), ms(150), ms(14500), ms(1), ms(7), ms(400)),
	}
}

// scenarioNoStore is T8: a healthy task on an engine with no attempt store.
func scenarioNoStore() *orkstackv1.TaskRun {
	S := at("2026-09-22T13:00:02Z")
	B := S.Add(ms(2100))
	return &orkstackv1.TaskRun{
		TaskId: "plain", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, Pool: "main",
		ScheduledAt: ts(S), StartedAt: ts(B), FinishedAt: ts(B.Add(63 * time.Second)),
	}
}

// oneTaskRun wraps one task in a run with distinct start and finish, so no
// rendered duration is a coincidental zero.
func oneTaskRun(runID string, task *orkstackv1.TaskRun) *orkstackv1.Run {
	start := task.GetScheduledAt()
	if start == nil {
		start = task.GetStartedAt()
	}
	return &orkstackv1.Run{
		Id: runID, OrkflowName: "timing", OrkflowVersion: "0.1.0",
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: ts(start.AsTime().Add(-time.Second)), StartedAt: ts(start.AsTime().Add(-time.Second)),
		FinishedAt: ts(task.GetFinishedAt().AsTime().Add(time.Second)),
		TaskRuns:   []*orkstackv1.TaskRun{task},
	}
}

// seedTimingRun puts a run and its attempt records into the CLI fixture.
func seedTimingRun(t *testing.T, f *fixture, attempts *cliAttemptStore, run *orkstackv1.Run, records []*orkstackv1.TaskAttempt) {
	t.Helper()
	f.runs.Apply(run)
	for _, rec := range records {
		if rec.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED {
			continue // absent from the store; the API synthesizes the row
		}
		if err := attempts.PutAttempt(context.Background(), rec); err != nil {
			t.Fatalf("PutAttempt: %v", err)
		}
	}
}

func renderHuman(t *testing.T, timing *runTiming, oneTask bool) string {
	t.Helper()
	var out strings.Builder
	if err := renderRunTiming(&out, timing, oneTask); err != nil {
		t.Fatalf("renderRunTiming: %v", err)
	}
	return out.String()
}

func renderJSON(t *testing.T, timing *runTiming) string {
	t.Helper()
	var out strings.Builder
	if err := writeTimingJSON(&out, timing); err != nil {
		t.Fatalf("writeTimingJSON: %v", err)
	}
	return out.String()
}

var timingNow = at("2026-09-22T23:00:00Z")

// TestRunTiming_GoldenTable is C1: T1-T4 in one run, as §6.2 renders it.
func TestRunTiming_GoldenTable(t *testing.T) {
	run, attempts := scenarioNightly()
	timing := computeRunTiming(run, attempts, timingOptions{AttemptStore: true, Now: timingNow})
	got := renderHuman(t, timing, false)
	requireGolden(t, "run-timing.golden", got)
	t.Logf("C1 run timing:\n%s", got)
}

// TestRunTiming_GoldenJSON is C2: the same run as the CLI-owned document.
func TestRunTiming_GoldenJSON(t *testing.T) {
	run, attempts := scenarioNightly()
	timing := computeRunTiming(run, attempts, timingOptions{AttemptStore: true, Now: timingNow})
	got := renderJSON(t, timing)
	requireGolden(t, "run-timing.json.golden", got)

	var doc struct {
		WaitSumSeconds       float64 `json:"waitSumSeconds"`
		WaitSumTasks         int     `json:"waitSumTasks"`
		WaitUnavailableTasks int     `json:"waitUnavailableTasks"`
		Tasks                []map[string]any
	}
	if err := json.Unmarshal([]byte(got), &doc); err != nil {
		t.Fatalf("the document is not JSON: %v", err)
	}
	if doc.WaitSumSeconds != 254.5 || doc.WaitSumTasks != 3 || doc.WaitUnavailableTasks != 1 {
		t.Errorf("wait sum = %v over %d (%d unavailable), want 254.5 over 3 (1)",
			doc.WaitSumSeconds, doc.WaitSumTasks, doc.WaitUnavailableTasks)
	}
	report := doc.Tasks[4]
	for _, key := range []string{"waitSeconds", "runSeconds", "totalSeconds", "queueWaitSeconds", "scheduledAt"} {
		if _, present := report[key]; present {
			t.Errorf("never-scheduled report carries %q; a quantity that does not apply is absent", key)
		}
	}
	verify := doc.Tasks[3]
	if v, present := verify["waitSeconds"]; !present || v != nil || verify["waitReason"] != reasonNoOrkerStart {
		t.Errorf("verify wait = %v (%v), want null with %q", v, verify["waitReason"], reasonNoOrkerStart)
	}
}

// TestRunTiming_TaskNarrowsToOneTaskAndPrintsItsPhases is C3, through the
// real command against the fixture engine.
func TestRunTiming_TaskNarrowsToOneTaskAndPrintsItsPhases(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	task, records := scenarioRender("run-timing-render")
	other := scenarioNoStore()
	run := oneTaskRun("run-timing-render", task)
	run.TaskRuns = append(run.TaskRuns, other)
	seedTimingRun(t, f, attempts, run, records)

	got := f.run("run", "timing", "run-timing-render", "--task", "render").requireExit(t, exitOK)
	requireGolden(t, "run-timing-task.golden", got.stdout)
	if strings.Contains(got.stdout, "plain") {
		t.Errorf("--task render printed another task:\n%s", got.stdout)
	}
	if !strings.Contains(got.stdout, "queue wait on pool gpu 2s (attempt 1 started 2026-09-22T10:02:02Z)") {
		t.Errorf("no numeric queue wait line:\n%s", got.stdout)
	}

	unknown := f.run("run", "timing", "run-timing-render", "--task", "nope")
	if unknown.code != exitError || !strings.Contains(unknown.stderr, `task "nope" not found`) {
		t.Errorf("unknown task: exit %d stderr %q", unknown.code, unknown.stderr)
	}
}

// standaloneZero finds a duration that is exactly zero ("0s"), not the tail of
// "10s" or "1m0s".
var standaloneZero = regexp.MustCompile(`(^|[^0-9.m])0s\b`)

// TestRunTiming_PreContractRunRendersUnavailableNeverZero is C4 (T5).
func TestRunTiming_PreContractRunRendersUnavailableNeverZero(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	task, records := scenarioPreContract("run-timing-legacy")
	seedTimingRun(t, f, attempts, oneTaskRun("run-timing-legacy", task), records)

	got := f.run("run", "timing", "run-timing-legacy").requireExit(t, exitOK)
	if standaloneZero.MatchString(got.stdout) {
		t.Errorf("a pre-contract run printed a zero duration:\n%s", got.stdout)
	}
	for _, want := range []string{
		"legacy  SUCCEEDED  main  1        -          unavailable  unavailable  12s",
		preContractFooter,
		"legacy  attempt 1  SUCCEEDED   phases unavailable (record predates phase spans)",
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("output lacks %q:\n%s", want, got.stdout)
		}
	}
	doc := f.run("--json", "run", "timing", "run-timing-legacy").requireExit(t, exitOK)
	if !strings.Contains(doc.stdout, `"waitReason": "`+reasonPreContract+`"`) {
		t.Errorf("JSON lacks the pre-contract wait reason:\n%s", doc.stdout)
	}
	t.Logf("C4 pre-contract:\n%s", got.stdout)
}

// TestRunTiming_ClockSkewIsUnavailableNotNegative is C5 (T6).
func TestRunTiming_ClockSkewIsUnavailableNotNegative(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	task, records := scenarioSkew("run-timing-skew")
	seedTimingRun(t, f, attempts, oneTaskRun("run-timing-skew", task), records)

	got := f.run("run", "timing", "run-timing-skew").requireExit(t, exitOK)
	if strings.Contains(got.stdout, " -3s") || strings.Contains(got.stdout, "-7s") {
		t.Errorf("a skewed pair rendered negative:\n%s", got.stdout)
	}
	for _, want := range []string{
		"skewed  SUCCEEDED  main  2        2026-09-22T12:00:05Z  unavailable  16s  13s",
		"skewed  queue wait on pool main: unavailable (clock skew)",
	} {
		if !strings.Contains(strings.Join(strings.Fields(got.stdout), " "), strings.Join(strings.Fields(want), " ")) {
			t.Errorf("output lacks %q:\n%s", want, got.stdout)
		}
	}
	doc := f.run("--json", "run", "timing", "run-timing-skew").requireExit(t, exitOK)
	if !strings.Contains(doc.stdout, `"waitReason": "clock skew"`) {
		t.Errorf("JSON lacks the clock-skew wait reason:\n%s", doc.stdout)
	}
}

// TestRunTiming_WithoutAnAttemptStoreStillPrintsTheSnapshotHalf is C6 (T8).
func TestRunTiming_WithoutAnAttemptStoreStillPrintsTheSnapshotHalf(t *testing.T) {
	f := newFixture(t)
	f.runs.Apply(oneTaskRun("run-timing-nostore", scenarioNoStore()))

	got := f.run("run", "timing", "run-timing-nostore").requireExit(t, exitOK)
	for _, want := range []string{"2.1s", "1m3s", "1m5.1s", noStoreBlock} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("output lacks %q:\n%s", want, got.stdout)
		}
	}
	doc := f.run("--json", "run", "timing", "run-timing-nostore").requireExit(t, exitOK)
	for _, want := range []string{
		`"attemptRecordsAvailable": false`,
		`"queueWaitReason": "no task attempt store"`,
		`"attemptsReason": "no task attempt store"`,
		`"waitSeconds": 2.1`,
	} {
		if !strings.Contains(doc.stdout, want) {
			t.Errorf("JSON lacks %s:\n%s", want, doc.stdout)
		}
	}
}

// TestRunTiming_EveryReasonStringIsPrintedByAScenario: a reason cannot be
// added without a scenario that shows it.
func TestRunTiming_EveryReasonStringIsPrintedByAScenario(t *testing.T) {
	var outputs []string
	render := func(run *orkstackv1.Run, attempts []*orkstackv1.TaskAttempt, opts timingOptions) {
		opts.Now = timingNow
		timing := computeRunTiming(run, attempts, opts)
		outputs = append(outputs, renderHuman(t, timing, opts.Task != ""), renderJSON(t, timing))
	}
	nightly, nightlyAttempts := scenarioNightly()
	render(nightly, nightlyAttempts, timingOptions{AttemptStore: true}) // C1, C2
	renderTask, renderRecords := scenarioRender("r")
	render(oneTaskRun("r", renderTask), renderRecords, timingOptions{AttemptStore: true, Task: "render"}) // C3
	for _, scenario := range []func(string) (*orkstackv1.TaskRun, []*orkstackv1.TaskAttempt){
		scenarioPreContract, // C4
		scenarioSkew,        // C5
	} {
		task, records := scenario("s")
		render(oneTaskRun("s", task), records, timingOptions{AttemptStore: true})
	}
	render(oneTaskRun("n", scenarioNoStore()), nil, timingOptions{}) // C6

	union := strings.Join(outputs, "\n")
	for _, reason := range timingReasons {
		if !strings.Contains(union, reason) {
			t.Errorf("no scenario prints the reason %q", reason)
		}
	}
	if len(timingReasons) != 7 {
		t.Errorf("there are %d reasons, the contract names seven", len(timingReasons))
	}
}

// TestRunTiming_ReclaimedRecordsOfAReportedTaskReadAsReclaimed: the engine
// lays a task's timeline out as UNREPORTED rows up to the attempt the
// snapshot names, so reclaimed records reach the CLI as rows, not as an empty
// list. A task whose snapshot proves an Orker reported (started_at) had its
// record written, so rows that are ALL unreported mean reclaimed, not crashed.
func TestRunTiming_ReclaimedRecordsOfAReportedTaskReadAsReclaimed(t *testing.T) {
	task := scenarioNoStore()
	timing := computeRunTiming(oneTaskRun("gone", task),
		[]*orkstackv1.TaskAttempt{unreported("gone", "plain", 1)},
		timingOptions{AttemptStore: true, Now: timingNow})
	got := renderHuman(t, timing, false)
	if !strings.Contains(got, "plain  attempt 1  unavailable (attempt records reclaimed)") {
		t.Errorf("reclaimed records did not read as reclaimed:\n%s", got)
	}
	if strings.Contains(got, "orker crashed") {
		t.Errorf("reclaimed records blamed a crash:\n%s", got)
	}
	if !strings.Contains(got, "2.1s") || !strings.Contains(got, "1m3s") {
		t.Errorf("the snapshot half is gone:\n%s", got)
	}
}

// TestRunTiming_RunningTaskShowsOnlySinceScheduled is T7: while a task runs
// there is no wait, run or total yet — only how long since it was scheduled.
func TestRunTiming_RunningTaskShowsOnlySinceScheduled(t *testing.T) {
	S := timingNow.Add(-75 * time.Second)
	run := &orkstackv1.Run{Id: "live", Status: orkstackv1.Status_STATUS_RUNNING, StartedAt: ts(S),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "busy", Status: orkstackv1.Status_STATUS_RUNNING, Attempt: 1, Pool: "main", ScheduledAt: ts(S)}}}
	timing := computeRunTiming(run, []*orkstackv1.TaskAttempt{unreported("live", "busy", 1)},
		timingOptions{AttemptStore: true, Now: timingNow})
	got := renderHuman(t, timing, false)
	row := strings.Join(strings.Fields(strings.Split(got, "\n")[1]), " ")
	if row != "busy RUNNING main 1 "+S.Format(time.RFC3339)+" - - since scheduled 1m15s" {
		t.Errorf("running row = %q", row)
	}
	if !strings.Contains(got, "busy  attempt 1  in progress") || strings.Contains(got, "orker crashed") {
		t.Errorf("the in-flight attempt is not shown as in progress:\n%s", got)
	}
}

// TestRunGet_TimingFlagAppendsTheTable is C7's first half.
func TestRunGet_TimingFlagAppendsTheTable(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	task, records := scenarioRender("run-get-timing")
	seedTimingRun(t, f, attempts, oneTaskRun("run-get-timing", task), records)

	plain := f.run("run", "get", "run-get-timing").requireExit(t, exitOK)
	combined := f.run("run", "get", "run-get-timing", "--timing").requireExit(t, exitOK)
	requireGolden(t, "run-get-timing.golden", combined.stdout)
	if !strings.HasPrefix(combined.stdout, plain.stdout) {
		t.Fatalf("--timing changed the run rendering above the table\n--- without ---\n%s\n--- with ---\n%s",
			plain.stdout, combined.stdout)
	}
	both := f.run("run", "get", "run-get-timing", "--attempts", "--timing").requireExit(t, exitOK)
	if !strings.Contains(both.stdout, "OUTCOME") ||
		strings.Index(both.stdout, "OUTCOME") > strings.Index(both.stdout, "WAIT ") {
		t.Errorf("--attempts --timing: the timing table must come after the attempts:\n%s", both.stdout)
	}
}

// TestRunGet_TimingIsRefusedWithJSON is C7's second half, on the precedent of
// --attempts.
func TestRunGet_TimingIsRefusedWithJSON(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	task, records := scenarioRender("run-get-timing-json")
	seedTimingRun(t, f, attempts, oneTaskRun("run-get-timing-json", task), records)

	refused := f.run("--json", "-n", "data", "run", "get", "run-get-timing-json", "--timing")
	if refused.code != exitError {
		t.Fatalf("exit code = %d, want %d", refused.code, exitError)
	}
	if refused.stdout != "" {
		t.Errorf("a refused command still wrote to stdout:\n%s", refused.stdout)
	}
	want := "--timing is a second response and cannot be added to --json output; " +
		"read the timing with: orkctl -n data run timing run-get-timing-json --json"
	if !strings.Contains(refused.stderr, want) {
		t.Errorf("stderr = %q, want it to contain %q", refused.stderr, want)
	}
	for _, path := range f.requests() {
		if strings.Contains(path, "GetRun") || strings.Contains(path, "ListTaskAttempts") {
			t.Errorf("the refused command still reached the engine: %v", f.requests())
		}
	}
}

// TestRenderRun_TaskTableHasScheduledBeforeStarted is C8: `run get` shows the
// dispatch instant beside the Orker's start.
func TestRenderRun_TaskTableHasScheduledBeforeStarted(t *testing.T) {
	task, _ := scenarioRender("r")
	var out strings.Builder
	if err := renderRun(&out, oneTaskRun("r", task)); err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	rows := taskTableRows(t, out.String())
	if rows[0] != "TASK STATUS POOL PIN ORKER ATTEMPT EXIT SCHEDULED STARTED FINISHED ERROR" {
		t.Fatalf("header = %q", rows[0])
	}
	if rows[1] != "render SUCCEEDED gpu - - 2 0 2026-09-22T10:02:00Z 2026-09-22T10:03:17Z 2026-09-22T10:04:05Z -" {
		t.Fatalf("row = %q", rows[1])
	}
}

// TestTaskStatusLabel_NeverStartedReadsScheduledAt is C9: a task canceled
// while it ran has scheduled_at and no started_at (no Orker reported), and it
// is NOT "never started".
func TestTaskStatusLabel_NeverStartedReadsScheduledAt(t *testing.T) {
	ran := &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_CANCELED, ScheduledAt: ts(fixedNow)}
	if got := taskStatusLabel(ran); got != "CANCELED" {
		t.Errorf("canceled-while-running = %q, want CANCELED", got)
	}
	never := &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_CANCELED}
	if got := taskStatusLabel(never); got != "CANCELED (never started)" {
		t.Errorf("never scheduled = %q, want CANCELED (never started)", got)
	}
}
