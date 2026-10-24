package api_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe the replica that did not run the task.
//
// Every attempt record below was written by "an Orker" — really just a Put
// into the shared store — and read back by a Service whose RunStore is empty
// and whose reader is the only thing that has ever heard of the run. That is
// the deployment a retry history has to survive: the process that watched the
// retries is gone, and the one being asked never saw them.

// attemptFixture is a Service reading a run out of the record, its attempt
// records out of the store, and (when a test wires one) its log index out of
// the same bucket.
type attemptFixture struct {
	*readerFixture
	attempts *artifacts.MemoryAttemptStore
	logs     *memoryLogStore
}

func newAttemptFixture(t *testing.T, run *orkstackv1.Run) *attemptFixture {
	t.Helper()
	attempts := artifacts.NewMemoryAttemptStore()
	logs := &memoryLogStore{}
	reader := &fakeReader{
		runs:   map[string]*orkstackv1.Run{run.GetId(): run},
		events: map[string][]*orkstackv1.Event{run.GetId(): {}},
	}
	fixture := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Attempts = attempts
		cfg.Logs = logs
	})
	return &attemptFixture{readerFixture: fixture, attempts: attempts, logs: logs}
}

// list is the whole RPC over the wire, which is also the assertion that the
// method is actually served and not merely implemented.
func (f *attemptFixture) list(
	t *testing.T,
	req *orkstackv1.ListTaskAttemptsRequest,
) ([]*orkstackv1.TaskAttempt, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := f.runClient().ListTaskAttempts(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return response.Msg.GetAttempts(), nil
}

// write files one record, the way the Orker that ran the attempt would.
func (f *attemptFixture) write(t *testing.T, record *orkstackv1.TaskAttempt) {
	t.Helper()
	if err := f.attempts.PutAttempt(context.Background(), record); err != nil {
		t.Fatalf("put attempt record %d of %q: %v", record.GetAttempt(), record.GetTaskId(), err)
	}
}

// attemptRecord is one stored attempt, with the fields every record carries.
func attemptRecord(
	runID, taskID string,
	attempt int32,
	outcome orkstackv1.AttemptOutcome,
) *orkstackv1.TaskAttempt {
	at := time.Date(2026, 9, 9, 17, 1, 0, 0, time.UTC).Add(time.Duration(attempt) * time.Second)
	record := &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion,
		RunId:         runID,
		TaskId:        taskID,
		Attempt:       attempt,
		Outcome:       outcome,
		OrkerId:       "orker-a1",
		Phase:         "run",
		StartedAt:     timestamppb.New(at),
		FinishedAt:    timestamppb.New(at.Add(time.Second)),
	}
	if outcome == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
		record.ExitCode = 1
		record.Error = &orkstackv1.Error{
			Code:    "CONTAINER_EXIT_NONZERO",
			Message: "container exited 1",
		}
	}
	return record
}

// retriedRun is a run whose one task failed twice and then succeeded, as the
// snapshot records it: only the FINAL attempt, which is the whole problem.
func retriedRun(runID, taskID string, finalAttempt int32) *orkstackv1.Run {
	at := time.Date(2026, 9, 9, 17, 0, 0, 0, time.UTC)
	return &orkstackv1.Run{
		Id:             runID,
		OrkflowName:    "flaky",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt:      timestamppb.New(at),
		StartedAt:      timestamppb.New(at),
		FinishedAt:     timestamppb.New(at.Add(10 * time.Second)),
		TaskRuns: []*orkstackv1.TaskRun{{
			TaskId:  taskID,
			Status:  orkstackv1.Status_STATUS_SUCCEEDED,
			Attempt: finalAttempt,
		}},
	}
}

// outcomesOf renders a timeline as "task#attempt=OUTCOME", which is exactly
// the three things this RPC's contract is about: which task, in what order,
// ending how.
func outcomesOf(attempts []*orkstackv1.TaskAttempt) []string {
	out := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		out = append(out, fmt.Sprintf("%s#%d=%s",
			attempt.GetTaskId(), attempt.GetAttempt(),
			strings.TrimPrefix(attempt.GetOutcome().String(), "ATTEMPT_OUTCOME_")))
	}
	return out
}

// The timeline of a retried task, in the order the retries happened — the
// store is a map and the records are written out of order on purpose.
func TestListTaskAttempts_OrdersOneTasksAttemptsAscending(t *testing.T) {
	run := retriedRun("ork-attempts-1", "flaky", 3)
	fixture := newAttemptFixture(t, run)
	for _, attempt := range []int32{3, 1, 2} {
		outcome := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
		if attempt == 3 {
			outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
		}
		fixture.write(t, attemptRecord(run.GetId(), "flaky", attempt, outcome))
	}

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	want := []string{"flaky#1=FAILED", "flaky#2=FAILED", "flaky#3=SUCCEEDED"}
	if got := outcomesOf(attempts); !slices.Equal(got, want) {
		t.Fatalf("timeline = %v, want %v", got, want)
	}
	if attempts[0].GetError().GetCode() != "CONTAINER_EXIT_NONZERO" {
		t.Fatalf("attempt 1 lost its error: %#v", attempts[0].GetError())
	}
}

// Tasks come back in the order the RUN lists them, not in whatever order the
// store's keys happen to sort: an operator reads a run top to bottom.
func TestListTaskAttempts_KeepsRunTaskOrderAcrossTasks(t *testing.T) {
	run := retriedRun("ork-attempts-2", "zulu", 1)
	run.TaskRuns = append(run.GetTaskRuns(), &orkstackv1.TaskRun{
		TaskId: "alpha", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2,
	})
	fixture := newAttemptFixture(t, run)
	fixture.write(t, attemptRecord(run.GetId(), "alpha", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))
	fixture.write(t, attemptRecord(run.GetId(), "alpha", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))
	fixture.write(t, attemptRecord(run.GetId(), "zulu", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	want := []string{"zulu#1=SUCCEEDED", "alpha#1=FAILED", "alpha#2=SUCCEEDED"}
	if got := outcomesOf(attempts); !slices.Equal(got, want) {
		t.Fatalf("timeline = %v, want %v", got, want)
	}
}

// task_id selects one task and nothing else.
func TestListTaskAttempts_TaskSelectorNarrowsToOneTask(t *testing.T) {
	run := retriedRun("ork-attempts-3", "zulu", 1)
	run.TaskRuns = append(run.GetTaskRuns(), &orkstackv1.TaskRun{
		TaskId: "alpha", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1,
	})
	fixture := newAttemptFixture(t, run)
	fixture.write(t, attemptRecord(run.GetId(), "alpha", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))
	fixture.write(t, attemptRecord(run.GetId(), "zulu", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{
		RunId: run.GetId(), TaskId: "alpha",
	})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	want := []string{"alpha#1=SUCCEEDED"}
	if got := outcomesOf(attempts); !slices.Equal(got, want) {
		t.Fatalf("timeline = %v, want %v", got, want)
	}
}

// The hole an Orker's death leaves is REPORTED. Attempt 2 wrote no record, so
// it comes back UNREPORTED rather than making attempt 3 look like attempt 2.
func TestListTaskAttempts_SynthesizesUnreportedAttemptsForMissingRecords(t *testing.T) {
	run := retriedRun("ork-attempts-4", "flaky", 3)
	fixture := newAttemptFixture(t, run)
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 3, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	want := []string{"flaky#1=FAILED", "flaky#2=UNREPORTED", "flaky#3=SUCCEEDED"}
	if got := outcomesOf(attempts); !slices.Equal(got, want) {
		t.Fatalf("timeline = %v, want %v", got, want)
	}
	gap := attempts[1]
	if gap.GetRunId() != run.GetId() || gap.GetTaskId() != "flaky" {
		t.Fatalf("synthesized row is not addressable: %#v", gap)
	}
	if gap.GetError() != nil || gap.GetStartedAt() != nil || gap.GetFinishedAt() != nil {
		t.Fatalf("synthesized row invented detail it does not have: %#v", gap)
	}
}

// TaskRun.attempt is 0 when Temporal timed the task out, and the records are
// then the only source of the attempt count. The timeline still runs 1..N.
func TestListTaskAttempts_RecordsExtendATimelineTheSnapshotUndercounts(t *testing.T) {
	run := retriedRun("ork-attempts-5", "flaky", 0)
	run.GetTaskRuns()[0].Status = orkstackv1.Status_STATUS_FAILED
	fixture := newAttemptFixture(t, run)
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	want := []string{"flaky#1=UNREPORTED", "flaky#2=FAILED"}
	if got := outcomesOf(attempts); !slices.Equal(got, want) {
		t.Fatalf("timeline = %v, want %v", got, want)
	}
}

// An attempt that wrote a log byte and then died leaves no record, but the log
// index still knows it happened. The timeline reaches it.
func TestListTaskAttempts_LogStoreWidensTheTimelinePastTheLastRecord(t *testing.T) {
	run := retriedRun("ork-attempts-6", "flaky", 1)
	run.GetTaskRuns()[0].Status = orkstackv1.Status_STATUS_RUNNING
	run.Status = orkstackv1.Status_STATUS_RUNNING
	fixture := newAttemptFixture(t, run)
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))
	fixture.logs.write(artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 2}, 1, "trying again\n")

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	want := []string{"flaky#1=FAILED", "flaky#2=UNREPORTED"}
	if got := outcomesOf(attempts); !slices.Equal(got, want) {
		t.Fatalf("timeline = %v, want %v", got, want)
	}
}

// A task that has not run yet has an EMPTY timeline, not an error and not a
// synthesized failure: nothing has been attempted, and saying "attempt 1,
// unreported" would accuse an Orker that was never asked to do anything.
func TestListTaskAttempts_TaskWithNoAttemptsIsEmptyNotAnError(t *testing.T) {
	run := retriedRun("ork-attempts-7", "pending", 0)
	run.Status = orkstackv1.Status_STATUS_RUNNING
	run.GetTaskRuns()[0].Status = orkstackv1.Status_STATUS_PENDING

	fixture := newAttemptFixture(t, run)
	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	if len(attempts) != 0 {
		t.Fatalf("a task that never ran reported %v", outcomesOf(attempts))
	}
}

// A run of another namespace is NotFound, records and all. The error of a
// failed attempt is as much of a tenant's business as its logs are.
func TestListTaskAttempts_AcrossNamespacesIsNotFound(t *testing.T) {
	run := retriedRun("ork-attempts-ns", "flaky", 2)
	run.Namespace = "payments"
	fixture := newAttemptFixture(t, run)
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED))

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err == nil {
		t.Fatalf("the default namespace read %v out of a payments run", outcomesOf(attempts))
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("err = %v (code %v), want NotFound", err, connect.CodeOf(err))
	}
}

// A task the run does not have is NotFound rather than an empty list: a typo
// must not look like a task that never retried.
func TestListTaskAttempts_UnknownTaskIsNotFound(t *testing.T) {
	run := retriedRun("ork-attempts-unknown", "flaky", 1)
	fixture := newAttemptFixture(t, run)

	if _, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{
		RunId: run.GetId(), TaskId: "nope",
	}); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("err = %v (code %v), want NotFound", err, connect.CodeOf(err))
	}
}

func TestListTaskAttempts_RequiresARun(t *testing.T) {
	run := retriedRun("ork-attempts-args", "flaky", 1)
	fixture := newAttemptFixture(t, run)

	if _, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{TaskId: "flaky"}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err = %v (code %v), want InvalidArgument", err, connect.CodeOf(err))
	}
}

// With no attempt store wired, the RPC says so instead of reporting a run
// whose every attempt is UNREPORTED — which would read as "every Orker died"
// when the truth is "this engine was never told where the records live".
func TestListTaskAttempts_WithoutAnAttemptStoreSaysSo(t *testing.T) {
	run := retriedRun("ork-attempts-none", "flaky", 2)
	reader := &fakeReader{
		runs:   map[string]*orkstackv1.Run{run.GetId(): run},
		events: map[string][]*orkstackv1.Event{run.GetId(): {}},
	}
	fixture := newReaderFixture(t, reader)

	_, err := fixture.runClient().ListTaskAttempts(context.Background(), connect.NewRequest(
		&orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("err = %v (code %v), want FailedPrecondition", err, connect.CodeOf(err))
	}
}

// TestListTaskAttempts_PassesPhasesThroughAndSynthesizedRowsHaveNone is A1:
// a stored record's phase spans reach the client as the Orker wrote them, and
// a synthesized UNREPORTED row carries none — nothing is invented for an
// attempt nobody recorded.
func TestListTaskAttempts_PassesPhasesThroughAndSynthesizedRowsHaveNone(t *testing.T) {
	run := retriedRun("ork-attempts-phases", "flaky", 3)
	fixture := newAttemptFixture(t, run)
	first := attemptRecord(run.GetId(), "flaky", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED)
	start := first.GetStartedAt().AsTime()
	first.Phase = "exit"
	first.Phases = []*orkstackv1.PhaseSpan{
		{Phase: "start", StartedAt: timestamppb.New(start), FinishedAt: timestamppb.New(start.Add(3 * time.Millisecond))},
		{Phase: "run", StartedAt: timestamppb.New(start.Add(3 * time.Millisecond)), FinishedAt: timestamppb.New(start.Add(900 * time.Millisecond))},
		{Phase: "exit", StartedAt: timestamppb.New(start.Add(900 * time.Millisecond)), FinishedAt: first.GetFinishedAt()},
	}
	fixture.write(t, first)
	fixture.write(t, attemptRecord(run.GetId(), "flaky", 3, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))

	attempts, err := fixture.list(t, &orkstackv1.ListTaskAttemptsRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("ListTaskAttempts: %v", err)
	}
	if len(attempts) != 3 {
		t.Fatalf("timeline = %v", outcomesOf(attempts))
	}
	got := attempts[0].GetPhases()
	if len(got) != 3 || got[0].GetPhase() != "start" || got[2].GetPhase() != "exit" {
		t.Fatalf("attempt 1 phases = %v, want the three stored spans", got)
	}
	if !got[2].GetFinishedAt().AsTime().Equal(first.GetFinishedAt().AsTime()) {
		t.Errorf("the last span ends at %v, want %v", got[2].GetFinishedAt().AsTime(), first.GetFinishedAt().AsTime())
	}
	if attempts[1].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_UNREPORTED {
		t.Fatalf("attempt 2 = %v, want UNREPORTED", attempts[1].GetOutcome())
	}
	if len(attempts[1].GetPhases()) != 0 {
		t.Errorf("the synthesized UNREPORTED row carries phases: %v", attempts[1].GetPhases())
	}
}
