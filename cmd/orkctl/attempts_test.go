package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// cliAttemptStore is the shared bucket of attempt records as the CLI's engine
// sees it: the records an Orker filed, keyed the way the real store keys them.
type cliAttemptStore struct {
	mu      sync.Mutex
	records map[string][]*orkstackv1.TaskAttempt
}

func newCLIAttemptStore() *cliAttemptStore {
	return &cliAttemptStore{records: map[string][]*orkstackv1.TaskAttempt{}}
}

func (s *cliAttemptStore) PutAttempt(_ context.Context, attempt *orkstackv1.TaskAttempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := attempt.GetRunId() + "\x00" + attempt.GetTaskId()
	s.records[key] = append(s.records[key], proto.Clone(attempt).(*orkstackv1.TaskAttempt))
	return nil
}

// ListAttempts returns one task's records ascending, as the store's contract
// promises. The fixture writes them in order, so the order is preserved rather
// than re-derived.
func (s *cliAttemptStore) ListAttempts(
	_ context.Context, runID, taskID string, _ artifacts.Item,
) ([]*orkstackv1.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := s.records[runID+"\x00"+taskID]
	out := make([]*orkstackv1.TaskAttempt, 0, len(stored))
	for _, record := range stored {
		out = append(out, proto.Clone(record).(*orkstackv1.TaskAttempt))
	}
	return out, nil
}

// newAttemptFixture is the CLI fixture with an attempt store attached.
func newAttemptFixture(t *testing.T) (*fixture, *cliAttemptStore) {
	t.Helper()
	attempts := newCLIAttemptStore()
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Attempts = attempts
	})
	return f, attempts
}

// seedAttemptRun puts a run carrying the named tasks into the read model. The
// per-task attempt number is what the engine finally saw, which is one of the
// three sources the timeline's length comes from.
func seedAttemptRun(
	t *testing.T,
	f *fixture,
	runID, namespace string,
	tasks map[string]*orkstackv1.TaskRun,
	order ...string,
) {
	t.Helper()
	taskRuns := make([]*orkstackv1.TaskRun, 0, len(order))
	for _, id := range order {
		taskRun, ok := tasks[id]
		if !ok {
			t.Fatalf("seedAttemptRun: no task %q in the fixture", id)
		}
		taskRun.TaskId = id
		taskRuns = append(taskRuns, taskRun)
	}
	f.runs.Apply(&orkstackv1.Run{
		Id:             runID,
		Namespace:      namespace,
		OrkflowName:    "flaky-pipeline",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt:      timestamppb.New(fixedNow),
		StartedAt:      timestamppb.New(fixedNow),
		FinishedAt:     timestamppb.New(fixedNow),
		TaskRuns:       taskRuns,
	})
}

// seedRetriedRun is the fixture every rendering test below reads: one task that
// failed and was retried into a success, and one whose single attempt left no
// record at all. Between them they cover the three outcomes a timeline can
// carry — FAILED, SUCCEEDED and the synthesized UNREPORTED.
func seedRetriedRun(t *testing.T, f *fixture, attempts *cliAttemptStore, runID, namespace string) {
	t.Helper()
	seedAttemptRun(t, f, runID, namespace, map[string]*orkstackv1.TaskRun{
		"flaky": {
			Status:     orkstackv1.Status_STATUS_SUCCEEDED,
			Attempt:    2,
			StartedAt:  timestamppb.New(fixedNow),
			FinishedAt: timestamppb.New(fixedNow.Add(3 * time.Second)),
		},
		"build": {
			Status:    orkstackv1.Status_STATUS_FAILED,
			Attempt:   1,
			StartedAt: timestamppb.New(fixedNow),
			Error: &orkstackv1.Error{
				Code:     "ORKER_HEARTBEAT_TIMEOUT",
				Message:  "the orker stopped heartbeating",
				Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
			},
		},
	}, "flaky", "build")

	ctx := context.Background()
	if err := attempts.PutAttempt(ctx, &orkstackv1.TaskAttempt{
		SchemaVersion: 1,
		RunId:         runID,
		TaskId:        "flaky",
		Attempt:       1,
		Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
		Error: &orkstackv1.Error{
			Code:      "CONTAINER_EXIT_NONZERO",
			Message:   "wget: server returned error: HTTP/1.1 503 Service Unavailable",
			Details:   "probe 1: connection reset\nprobe 2: connection reset",
			Retryable: true,
			Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
		},
		ExitCode:   8,
		StartedAt:  timestamppb.New(fixedNow),
		FinishedAt: timestamppb.New(fixedNow.Add(1200 * time.Millisecond)),
		OrkerId:    "orker-a1",
		Pool:       "default",
		Phase:      "run",
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}
	if err := attempts.PutAttempt(ctx, &orkstackv1.TaskAttempt{
		SchemaVersion: 1,
		RunId:         runID,
		TaskId:        "flaky",
		Attempt:       2,
		Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		StartedAt:     timestamppb.New(fixedNow.Add(2 * time.Second)),
		FinishedAt:    timestamppb.New(fixedNow.Add(2900 * time.Millisecond)),
		OrkerId:       "orker-a1",
		Pool:          "default",
		Phase:         "upload",
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}
	// "build" gets no record: its Orker died before it could file one, which is
	// the hole the API fills with an UNREPORTED row.
}

// TestRunAttempts_GoldenTimelineOfAMixedRun pins the table an operator reads
// after a retry, with all three kinds of row in it.
func TestRunAttempts_GoldenTimelineOfAMixedRun(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	seedRetriedRun(t, f, attempts, "run-attempts-1", "")

	listed := f.run("run", "attempts", "run-attempts-1").requireExit(t, exitOK)
	requireGolden(t, "run-attempts.golden", listed.stdout)

	// The claim the golden is holding: a try that FAILED and was retried away
	// is on this surface, and it is not on `run get`'s.
	if !strings.Contains(listed.stdout, "FAILED") {
		t.Errorf("the retried-away attempt is missing from the timeline:\n%s", listed.stdout)
	}
	if strings.Contains(listed.stdout, "ATTEMPT_OUTCOME_") {
		t.Errorf("outcomes carry their enum prefix:\n%s", listed.stdout)
	}
}

// TestRunAttempts_UnreportedIsAnAbsentRecordNotAFailure is the rendering rule
// for the row nothing filed.
//
// An UNREPORTED attempt is not known to have failed — it is known to have left
// no record — so every cell but the three the reader synthesizes is a dash, and
// the reason is said once under the table rather than in the error column,
// where it would read as that attempt's error.
func TestRunAttempts_UnreportedIsAnAbsentRecordNotAFailure(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	seedRetriedRun(t, f, attempts, "run-attempts-2", "")

	listed := f.run("run", "attempts", "run-attempts-2", "--task", "build").requireExit(t, exitOK)
	if !strings.Contains(listed.stdout, "UNREPORTED") {
		t.Fatalf("the unreported attempt is missing:\n%s", listed.stdout)
	}
	if !strings.Contains(listed.stdout, "no record from the orker") {
		t.Errorf("nothing says what UNREPORTED means, so a dashed row reads as missing data:\n%s",
			listed.stdout)
	}
	row := attemptRow(t, listed.stdout, "build")
	if strings.Contains(row, "FAILED") {
		t.Errorf("an unreported attempt is rendered as a failure: %q", row)
	}
	// The run snapshot's own error for this task must not leak into the row:
	// the record is what an attempt row reports, and there is none.
	if strings.Contains(row, "ORKER_HEARTBEAT_TIMEOUT") {
		t.Errorf("the task's final error is rendered as the attempt's: %q", row)
	}
}

// attemptRow returns the table row for a task, so an assertion about one row
// cannot be satisfied by text elsewhere in the output.
func attemptRow(t *testing.T, stdout, taskID string) string {
	t.Helper()
	for _, line := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(line, taskID+" ") {
			return line
		}
	}
	t.Fatalf("no row for task %q in:\n%s", taskID, stdout)
	return ""
}

// TestRunAttempts_TaskDetailBlockCarriesTheFullFailure pins the block that
// exists because a table cell is one line and a failure is not.
//
// The namespace is a named one so the printed logs command is the command that
// actually reaches this run: a hint that omitted -n would read the default
// namespace and find nothing.
func TestRunAttempts_TaskDetailBlockCarriesTheFullFailure(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	seedRetriedRun(t, f, attempts, "run-attempts-3", "payments")

	detailed := f.run("-n", "payments", "run", "attempts", "run-attempts-3", "--task", "flaky").
		requireExit(t, exitOK)
	requireGolden(t, "run-attempts-task.golden", detailed.stdout)

	// The two halves a truncated cell cannot carry, and the copy-pasteable way
	// to read what that attempt printed.
	for _, want := range []string{
		"probe 2: connection reset",
		"orkctl -n payments run logs run-attempts-3 --task flaky --attempt 1",
	} {
		if !strings.Contains(detailed.stdout, want) {
			t.Errorf("detail block does not carry %q:\n%s", want, detailed.stdout)
		}
	}
	// Only the FAILED attempt gets a block; the successful retry has no error
	// to expand.
	if strings.Count(detailed.stdout, "\nAttempt ") != 1 {
		t.Errorf("want exactly one detail block, for the failed attempt:\n%s", detailed.stdout)
	}
}

// TestRunAttempts_JSONIsTheRawResponse — scripts get the response envelope,
// unreshaped, so a count they derive is theirs to define.
func TestRunAttempts_JSONIsTheRawResponse(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	seedRetriedRun(t, f, attempts, "run-attempts-4", "")

	machine := f.run("--json", "run", "attempts", "run-attempts-4").requireExit(t, exitOK)
	requireGolden(t, "run-attempts.json.golden", machine.stdout)

	var decoded struct {
		Attempts []struct {
			TaskID  string `json:"taskId"`
			Attempt int    `json:"attempt"`
			Outcome string `json:"outcome"`
		} `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(machine.stdout), &decoded); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, machine.stdout)
	}
	if len(decoded.Attempts) != 3 {
		t.Fatalf("want the run's three attempts, got %d:\n%s", len(decoded.Attempts), machine.stdout)
	}
	if strings.Contains(machine.stdout, "ATTEMPT_OUTCOME_") {
		t.Errorf("machine output carries the enum prefix:\n%s", machine.stdout)
	}
}

// TestRunAttempts_EmptyTimelineIsAnAnswerNotAnError — a task that was never
// dispatched, and a run whose records cleanup has reclaimed, both read this
// way.
func TestRunAttempts_EmptyTimelineIsAnAnswerNotAnError(t *testing.T) {
	f, _ := newAttemptFixture(t)
	seedAttemptRun(t, f, "run-attempts-5", "", map[string]*orkstackv1.TaskRun{
		"never": {Status: orkstackv1.Status_STATUS_PENDING},
	}, "never")

	listed := f.run("run", "attempts", "run-attempts-5").requireExit(t, exitOK)
	if listed.stdout != "No attempts.\n" {
		t.Fatalf("stdout = %q, want a plain empty answer", listed.stdout)
	}
}

// TestRunGet_AttemptsFlagAppendsTheTimelineAndChangesNothingElse is the
// regression bar: `run get` without the flag is byte for byte what it was, and
// with it the same output plus the timeline.
func TestRunGet_AttemptsFlagAppendsTheTimelineAndChangesNothingElse(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	seedRetriedRun(t, f, attempts, "run-attempts-6", "")

	plain := f.run("run", "get", "run-attempts-6").requireExit(t, exitOK)
	requireGolden(t, "run-get-plain.golden", plain.stdout)

	combined := f.run("run", "get", "run-attempts-6", "--attempts").requireExit(t, exitOK)
	requireGolden(t, "run-get-attempts.golden", combined.stdout)

	if !strings.HasPrefix(combined.stdout, plain.stdout) {
		t.Fatalf("--attempts changed the run rendering above the timeline\n"+
			"--- without ---\n%s\n--- with ---\n%s", plain.stdout, combined.stdout)
	}
	if strings.Contains(plain.stdout, "OUTCOME") {
		t.Errorf("`run get` prints the attempt table without being asked:\n%s", plain.stdout)
	}
}

// TestRunGet_AttemptsIsRefusedWithJSONRatherThanEmittingTwoDocuments.
//
// The two responses cannot share one document, and printing them back to back
// would hand a script two concatenated objects that no JSON parser accepts —
// silently, since both commands would still exit 0.
func TestRunGet_AttemptsIsRefusedWithJSONRatherThanEmittingTwoDocuments(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	seedRetriedRun(t, f, attempts, "run-attempts-7", "")

	refused := f.run("--json", "run", "get", "run-attempts-7", "--attempts")
	if refused.code != exitError {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s", refused.code, exitError, refused.stdout)
	}
	if refused.stdout != "" {
		t.Errorf("a refused command still wrote to stdout:\n%s", refused.stdout)
	}
	if !strings.Contains(refused.stderr, "run attempts run-attempts-7 --json") {
		t.Errorf("the refusal does not name the command that answers it:\n%s", refused.stderr)
	}
	// Refused locally: nothing was asked of the engine.
	for _, path := range f.requests() {
		if strings.Contains(path, "ListTaskAttempts") {
			t.Errorf("the refused command still called ListTaskAttempts: %v", f.requests())
		}
	}
}

// TestRunAttempts_EngineWithoutAnAttemptStoreSaysSoInWords.
//
// FailedPrecondition is the one failure that is not about the request: no run
// on this engine has a retry history. Reported as "failed_precondition" it
// sends an operator hunting for the run id they mistyped, so it is spelled out
// instead — and the gRPC code stays out of the sentence.
func TestRunAttempts_EngineWithoutAnAttemptStoreSaysSoInWords(t *testing.T) {
	f := newFixture(t)
	seedAttemptRun(t, f, "run-attempts-8", "", map[string]*orkstackv1.TaskRun{
		"build": {Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1},
	}, "build")

	outcome := f.run("run", "attempts", "run-attempts-8")
	if outcome.code != exitError {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s", outcome.code, exitError, outcome.stdout)
	}
	if !strings.Contains(outcome.stderr, "attempt history is not available") {
		t.Fatalf("stderr does not explain the missing store:\n%s", outcome.stderr)
	}
	for _, leak := range []string{"failed_precondition", "connect.", "*connect.Error"} {
		if strings.Contains(outcome.stderr, leak) {
			t.Errorf("stderr leaks the transport (%q):\n%s", leak, outcome.stderr)
		}
	}

	// The same engine's `run get --attempts` says the same thing, so the two
	// surfaces cannot disagree about why there is no history.
	combined := f.run("run", "get", "run-attempts-8", "--attempts")
	if combined.code != exitError {
		t.Fatalf("exit code = %d, want %d", combined.code, exitError)
	}
	if !strings.Contains(combined.stderr, "attempt history is not available") {
		t.Errorf("`run get --attempts` explains it differently:\n%s", combined.stderr)
	}
	// The run itself still printed: one missing store must not cost the read.
	if !strings.Contains(combined.stdout, "run-attempts-8") {
		t.Errorf("the run was not printed before the timeline failed:\n%s", combined.stdout)
	}
}

// TestRunAttempts_UnknownRunAndTaskAreReportedAsNotFound keeps the ordinary
// request errors on the shared rendering: they name the operation and the code,
// which is what tells an operator the id was wrong rather than the engine.
func TestRunAttempts_UnknownRunAndTaskAreReportedAsNotFound(t *testing.T) {
	f, attempts := newAttemptFixture(t)
	seedRetriedRun(t, f, attempts, "run-attempts-9", "")

	unknownRun := f.run("run", "attempts", "run-missing")
	if unknownRun.code != exitError {
		t.Fatalf("exit code = %d, want %d", unknownRun.code, exitError)
	}
	if !strings.Contains(unknownRun.stderr, "list task attempts: not_found:") {
		t.Errorf("an unknown run is not reported as not found:\n%s", unknownRun.stderr)
	}

	unknownTask := f.run("run", "attempts", "run-attempts-9", "--task", "no-such-task")
	if unknownTask.code != exitError {
		t.Fatalf("exit code = %d, want %d", unknownTask.code, exitError)
	}
	if !strings.Contains(unknownTask.stderr, "not_found") ||
		!strings.Contains(unknownTask.stderr, "no-such-task") {
		t.Errorf("an unknown task is not reported as not found:\n%s", unknownTask.stderr)
	}
}

// TestAttemptExitCell_ZeroIsOnlyAnExitCodeWhenTheAttemptSucceeded.
//
// The field is 0 for every attempt whose container never ran, so printing that
// 0 beside a FAILED outcome would say the process exited cleanly — the opposite
// of what happened.
func TestAttemptExitCell_ZeroIsOnlyAnExitCodeWhenTheAttemptSucceeded(t *testing.T) {
	cases := []struct {
		name    string
		attempt *orkstackv1.TaskAttempt
		want    string
	}{
		{"succeeded", &orkstackv1.TaskAttempt{
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED}, "0"},
		{"failed without a container", &orkstackv1.TaskAttempt{
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED}, dash},
		{"failed with an exit code", &orkstackv1.TaskAttempt{
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, ExitCode: 8}, "8"},
		{"unreported", &orkstackv1.TaskAttempt{
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED}, dash},
	}
	for _, tc := range cases {
		if got := attemptExitCell(tc.attempt); got != tc.want {
			t.Errorf("attemptExitCell(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestAttemptDuration_NeedsBothEnds — an attempt whose Orker vanished has a
// start and no end, and "now minus started" would invent a duration that grows
// every time the command is run.
func TestAttemptDuration_NeedsBothEnds(t *testing.T) {
	both := &orkstackv1.TaskAttempt{
		StartedAt:  timestamppb.New(fixedNow),
		FinishedAt: timestamppb.New(fixedNow.Add(1500 * time.Millisecond)),
	}
	if got := attemptDuration(both); got != "1.5s" {
		t.Errorf("attemptDuration(both ends) = %q, want %q", got, "1.5s")
	}
	openEnded := &orkstackv1.TaskAttempt{StartedAt: timestamppb.New(fixedNow)}
	if got := attemptDuration(openEnded); got != dash {
		t.Errorf("attemptDuration(no end) = %q, want %q", got, dash)
	}
	skewed := &orkstackv1.TaskAttempt{
		StartedAt:  timestamppb.New(fixedNow),
		FinishedAt: timestamppb.New(fixedNow.Add(-time.Second)),
	}
	if got := attemptDuration(skewed); got != dash {
		t.Errorf("attemptDuration(skewed pair) = %q, want %q", got, dash)
	}
}

// TestAttemptErrorLine_LeadsWithTheCategory: the category is what decides
// whether the attempt could be retried at all, and the code alone does not
// separate a task bug from a deployment one.
func TestAttemptErrorLine_LeadsWithTheCategory(t *testing.T) {
	withCategory := &orkstackv1.Error{
		Code:     "CONTAINER_EXIT_NONZERO",
		Message:  "exit status 8",
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
	}
	if got, want := attemptErrorLine(withCategory),
		"ACTIVITY_FAILED/CONTAINER_EXIT_NONZERO: exit status 8"; got != want {
		t.Errorf("attemptErrorLine = %q, want %q", got, want)
	}
	uncategorised := &orkstackv1.Error{Code: "SOMETHING", Message: "went wrong"}
	if got, want := attemptErrorLine(uncategorised), "SOMETHING: went wrong"; got != want {
		t.Errorf("attemptErrorLine(uncategorised) = %q, want %q", got, want)
	}
	if got := attemptErrorLine(nil); got != dash {
		t.Errorf("attemptErrorLine(nil) = %q, want %q", got, dash)
	}
}
