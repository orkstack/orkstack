package orker

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// spanNames is the phase sequence a record's span list names, in order.
func spanNames(spans []*orkstackv1.PhaseSpan) []string {
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		out = append(out, span.GetPhase())
	}
	return out
}

// assertClosedSpanList holds the three invariants every recorded span list
// keeps, whatever path the attempt took: contiguous (each span ends where the
// next begins), never inverted, the last ending at the record's finished_at,
// and the record's terminal phase equal to the last span's — one sequence,
// two views of it.
func assertClosedSpanList(t *testing.T, record *orkstackv1.TaskAttempt) {
	t.Helper()
	spans := record.GetPhases()
	if len(spans) == 0 {
		t.Fatalf("attempt %d recorded no phase spans", record.GetAttempt())
	}
	for i, span := range spans {
		if span.GetStartedAt() == nil || span.GetFinishedAt() == nil {
			t.Fatalf("span %d (%s) is missing an end: %v .. %v",
				i, span.GetPhase(), span.GetStartedAt(), span.GetFinishedAt())
		}
		if span.GetFinishedAt().AsTime().Before(span.GetStartedAt().AsTime()) {
			t.Errorf("span %d (%s) is inverted: %v .. %v", i, span.GetPhase(),
				span.GetStartedAt().AsTime(), span.GetFinishedAt().AsTime())
		}
		if i+1 < len(spans) && !span.GetFinishedAt().AsTime().Equal(spans[i+1].GetStartedAt().AsTime()) {
			t.Errorf("span %d (%s) ends at %v but span %d (%s) begins at %v: not contiguous",
				i, span.GetPhase(), span.GetFinishedAt().AsTime(),
				i+1, spans[i+1].GetPhase(), spans[i+1].GetStartedAt().AsTime())
		}
	}
	last := spans[len(spans)-1]
	if !last.GetFinishedAt().AsTime().Equal(record.GetFinishedAt().AsTime()) {
		t.Errorf("last span ends at %v, want the record's finished_at %v",
			last.GetFinishedAt().AsTime(), record.GetFinishedAt().AsTime())
	}
	if record.GetPhase() != last.GetPhase() {
		t.Errorf("record.phase = %q but the last span is %q; they are one sequence", record.GetPhase(), last.GetPhase())
	}
}

func onlyAttempt(t *testing.T, store *artifacts.MemoryAttemptStore, in *orkstackv1.ExecuteTaskInput) *orkstackv1.TaskAttempt {
	t.Helper()
	records, err := store.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("stored %d attempt records, want 1", len(records))
	}
	return records[0]
}

func assertSpanNames(t *testing.T, got []*orkstackv1.PhaseSpan, want []string) {
	t.Helper()
	names := spanNames(got)
	if len(names) != len(want) {
		t.Fatalf("phase spans = %q, want %q", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("phase spans = %q, want %q", names, want)
		}
	}
}

// TestPhaseSpans_CoverTheSuccessSequenceContiguously is O1: a successful
// attempt's record lists the eleven §7.1 phases it walked, contiguous, the
// last closing at the record's finished_at.
func TestPhaseSpans_CoverTheSuccessSequenceContiguously(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	in := baseExportInput("spans-success")

	if _, err := dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	record := onlyAttempt(t, attempts, in)
	assertSpanNames(t, record.GetPhases(), wantPhaseSequence)
	assertClosedSpanList(t, record)
	if record.GetPhase() != "commit" {
		t.Errorf("record.phase = %q, want commit", record.GetPhase())
	}
	if first := record.GetPhases()[0].GetStartedAt().AsTime(); first.Before(record.GetStartedAt().AsTime()) {
		t.Errorf("the first span begins at %v, before the attempt's own start %v",
			first, record.GetStartedAt().AsTime())
	}
	t.Logf("O1 phase sequence: %q", spanNames(record.GetPhases()))
}

// TestPhaseSpans_AFailedAttemptEndsInTheFailingPhase is O2: a container that
// exits 8 ends its list at "exit" — the phase that classified the status —
// and never reaches validate or commit.
func TestPhaseSpans_AFailedAttemptEndsInTheFailingPhase(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 8}, attempts)
	in := baseExportInput("spans-exit8")

	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute: want a failure for exit 8, got nil")
	}
	record := onlyAttempt(t, attempts, in)
	assertSpanNames(t, record.GetPhases(), wantPhaseSequence[:9])
	assertClosedSpanList(t, record)
	if record.GetPhase() != "exit" {
		t.Errorf("record.phase = %q, want exit", record.GetPhase())
	}
}

// TestPhaseSpans_AreRecordedWithNoPhaseObserverWired is O3: the spans are
// the Orker's own record, not a by-product of telemetry, so a dispatcher
// with Phases == nil records them all the same.
func TestPhaseSpans_AreRecordedWithNoPhaseObserverWired(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	if dispatcher.Phases != nil {
		t.Fatal("test precondition: the dispatcher must have no PhaseObserver")
	}
	in := baseExportInput("spans-no-observer")
	if _, err := dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	record := onlyAttempt(t, attempts, in)
	if len(record.GetPhases()) != len(wantPhaseSequence) {
		t.Fatalf("with no PhaseObserver the record has %d spans, want %d", len(record.GetPhases()), len(wantPhaseSequence))
	}
}

// TestPhaseSpans_AnAdoptedCommitRecordsStartAndCommitCheckOnly is O4: the
// retry that finds a commit returns it at STEP 3, so its record is exactly
// [start, commit-check] — no container ran and the list says so.
func TestPhaseSpans_AnAdoptedCommitRecordsStartAndCommitCheckOnly(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	dispatcher.Commits = artifacts.NewMemoryCommitStore()
	in := baseExportInput("spans-adopt")
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + in.GetTaskId() + "/commit.json"

	first, err := dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if first.GetCommitAdopted() {
		t.Fatal("the first attempt adopted a commit that did not exist yet")
	}
	// A second attempt for the same activity: a fresh attempt-record store so
	// the (run, task, attempt) key does not collide with the first record.
	adoptStore := artifacts.NewMemoryAttemptStore()
	dispatcher.Attempts = adoptStore
	second, err := dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if !second.GetCommitAdopted() {
		t.Fatal("the second attempt did not adopt the first attempt's commit")
	}
	record := onlyAttempt(t, adoptStore, in)
	assertSpanNames(t, record.GetPhases(), []string{"start", "commit-check"})
	assertClosedSpanList(t, record)
}

// TestPhaseSpans_AValidateRefusalRecordsStartOnly is O5: an input Validate
// refuses never reaches the commit check, so its list is exactly [start].
func TestPhaseSpans_AValidateRefusalRecordsStartOnly(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	in := baseExportInput("spans-refused")
	in.Image.Pinned = ""

	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute: want a Validate refusal for an unpinned image, got nil")
	}
	record := onlyAttempt(t, attempts, in)
	assertSpanNames(t, record.GetPhases(), []string{"start"})
	assertClosedSpanList(t, record)
}

// cancelOnStartRuntime cancels the attempt's context the moment the
// container starts and then waits, like a real container, until it is
// signalled. It puts the cancellation exactly in the wait path's select, the
// place "cancel" is entered from.
type cancelOnStartRuntime struct {
	phaseCapturingRuntime
	cancel context.CancelFunc

	once     sync.Once
	signaled chan struct{}
}

func (r *cancelOnStartRuntime) Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error {
	r.cancel()
	return nil
}

func (r *cancelOnStartRuntime) Signal(context.Context, ContainerID, Signal) error {
	r.once.Do(func() { close(r.signaled) })
	return nil
}

func (r *cancelOnStartRuntime) Wait(ctx context.Context, _ ContainerID) (ExitStatus, error) {
	select {
	case <-r.signaled:
		return ExitStatus{Code: 143}, nil
	case <-ctx.Done():
		return ExitStatus{}, ctx.Err()
	}
}

// TestPhaseSpans_CancellationAppendsCancelAndStillCloses is O6 (extending
// TestAttemptRecords_CancellationIsStillRecorded): a cancellation that reaches
// a running container appends "cancel", and the list is still closed at the
// record's finished_at with no inverted span.
func TestPhaseSpans_CancellationAppendsCancelAndStillCloses(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &cancelOnStartRuntime{cancel: cancel, signaled: make(chan struct{})}
	dispatcher := newAttemptDispatcher(t, runtime, attempts)
	in := baseExportInput("spans-cancel")

	if _, err := dispatcher.Execute(ctx, in); err == nil {
		t.Fatal("Execute on a cancelled attempt returned no error")
	}
	record := onlyAttempt(t, attempts, in)
	if record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED {
		t.Fatalf("outcome = %v, want CANCELED", record.GetOutcome())
	}
	names := spanNames(record.GetPhases())
	if len(names) == 0 || names[len(names)-1] != "cancel" {
		t.Errorf("phase spans = %q, want the list to end in cancel", names)
	}
	assertClosedSpanList(t, record)
}

// TestHeartbeater_PhaseLogIsSafeUnderConcurrentPhaseCalls is O7: the log is
// written from more than one goroutine (the wait path's select is its own
// step), so it is locked; run under -race. A mark recorded after the end the
// list is closed at is dropped rather than rendered backwards.
func TestHeartbeater_PhaseLogIsSafeUnderConcurrentPhaseCalls(t *testing.T) {
	beat := &Heartbeater{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				beat.Phase("run")
				_ = beat.PhaseSpans(time.Now().UTC())
			}
		}()
	}
	wg.Wait()

	spans := beat.PhaseSpans(time.Now().UTC())
	if len(spans) != 8*50 {
		t.Fatalf("recorded %d spans, want %d", len(spans), 8*50)
	}
	for i, span := range spans {
		if span.GetFinishedAt().AsTime().Before(span.GetStartedAt().AsTime()) {
			t.Fatalf("span %d is inverted", i)
		}
	}

	late := &Heartbeater{}
	late.Phase("run")
	end := time.Now().UTC()
	// A mark AFTER end: the cancel path's Phase racing the record.
	late.marks = append(late.marks, phaseMark{name: "cancel", at: end.Add(time.Second)})
	spans = late.PhaseSpans(end)
	assertSpanNames(t, spans, []string{"run"})
	if !spans[0].GetFinishedAt().AsTime().Equal(end) {
		t.Errorf("the surviving span ends at %v, want %v", spans[0].GetFinishedAt().AsTime(), end)
	}

	if (&Heartbeater{}).PhaseSpans(end) != nil {
		t.Error("a heartbeater that never saw a Phase call rendered spans")
	}
}

// TestFailure_StampStartFillsADetailOnEveryFailurePath is O8: whether or not
// a detail was attached, the ApplicationError the Orker returns carries the
// attempt's start, so the engine projects TaskRun.started_at for a failed
// task exactly as for a succeeded one.
func TestFailure_StampStartFillsADetailOnEveryFailurePath(t *testing.T) {
	start := time.Date(2026, 9, 22, 9, 14, 4, 0, time.UTC)
	cases := map[string]*Failure{
		"no detail (a Validate refusal)": Fail(ReasonImageNotPinned, "task %q: image.pinned is empty", "a"),
		"a detail from the failure path": Fail(ReasonContainerExitNonzero, "exit 8").
			WithDetail(&orkstackv1.ActivityFailureDetail{ExitCode: 8, OrkerId: "orker-1", Attempt: 2}),
	}
	for name, failure := range cases {
		t.Run(name, func(t *testing.T) {
			hadDetail := failure.Detail != nil
			failure.StampStart(timestamppb.New(start))
			var appErr *temporal.ApplicationError
			if !errors.As(failure.TemporalError(), &appErr) {
				t.Fatal("TemporalError is not an ApplicationError")
			}
			var detail *orkstackv1.ActivityFailureDetail
			if err := appErr.Details(&detail); err != nil {
				t.Fatalf("decode the detail: %v", err)
			}
			if got := detail.GetStartedAt(); got == nil || !got.AsTime().Equal(start) {
				t.Fatalf("detail.started_at = %v, want %v", got, start)
			}
			if detail.GetReasonCode() != failure.Reason {
				t.Errorf("detail.reason_code = %q, want %q", detail.GetReasonCode(), failure.Reason)
			}
			if detail.GetCategory() != CategoryEnum(failure.Category()) {
				t.Errorf("detail.category = %v", detail.GetCategory())
			}
			if hadDetail && (detail.GetExitCode() != 8 || detail.GetOrkerId() != "orker-1" || detail.GetAttempt() != 2) {
				t.Errorf("stamping the start lost the existing detail: %v", detail)
			}
		})
	}
	var absent *Failure
	if absent.StampStart(timestamppb.New(start)) != nil {
		t.Error("a nil Failure stamped into something")
	}
}

// TestExecute_AFailedAttemptReturnsItsStart holds O8 at the Execute seam: the
// error a failing container produces carries the same start the attempt
// record does.
func TestExecute_AFailedAttemptReturnsItsStart(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 3}, attempts)
	in := baseExportInput("failed-start")
	_, err := dispatcher.Execute(context.Background(), in)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("Execute error = %v, want an ApplicationError", err)
	}
	var detail *orkstackv1.ActivityFailureDetail
	if err := appErr.Details(&detail); err != nil {
		t.Fatalf("decode the detail: %v", err)
	}
	record := onlyAttempt(t, attempts, in)
	if detail.GetStartedAt() == nil || !detail.GetStartedAt().AsTime().Equal(record.GetStartedAt().AsTime()) {
		t.Fatalf("detail.started_at = %v, want the attempt's start %v", detail.GetStartedAt(), record.GetStartedAt())
	}
}
