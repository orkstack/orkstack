package engine

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
)

const testRunContextSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// runContextState is a run whose plan carries a run context, with one
// plain task "t" whose row was scheduled.
func runContextState(t *testing.T, retry *orkstackv1.RetryPolicy) (*runState, *orkstackv1.Task) {
	t.Helper()
	tk := &orkstackv1.Task{Id: "t", Retry: retry}
	created := time.Date(2026, 11, 19, 5, 0, 0, 412_000_000, time.UTC)
	s := &runState{
		runID: "ork-rc",
		scope: payloadcrypt.Unsealed(),
		wf:    &orkstackv1.Orkflow{Name: "load", Version: "7", Namespace: "default", Tasks: []*orkstackv1.Task{tk}},
		plan: RunPlan{
			RunContextSHA256:   testRunContextSHA,
			RunContextMaxBytes: DefaultRunContextMaxBytes,
		},
		createdAt: timestamppb.New(created),
		startedAt: timestamppb.New(created.Add(128 * time.Millisecond)),
		taskRuns: map[string]*orkstackv1.TaskRun{
			"t": {TaskId: "t", ScheduledAt: timestamppb.New(created.Add(time.Second))},
		},
	}
	return s, tk
}

func TestExecuteTaskInput_CarriesRunContext(t *testing.T) {
	s, tk := runContextState(t, &orkstackv1.RetryPolicy{MaximumAttempts: 3})
	in, err := s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	rc := in.GetRunContext()
	want := &orkstackv1.RunContext{
		ObjectSha256:        testRunContextSHA,
		CreatedAt:           s.createdAt,
		StartedAt:           s.startedAt,
		TaskScheduledAt:     s.taskRuns["t"].GetScheduledAt(),
		DeclaredMaxAttempts: 3,
	}
	if !proto.Equal(rc, want) {
		t.Fatalf("run context = %v, want %v", rc, want)
	}
}

func TestExecuteTaskInput_NoRunContextWithoutPlanSHA(t *testing.T) {
	s, tk := runContextState(t, nil)
	s.plan.RunContextSHA256 = ""
	in, err := s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	if in.GetRunContext() != nil {
		t.Fatalf("a plan without a run context sent %v", in.GetRunContext())
	}
}

func TestRunContext_ChildCarriesItsParentAndRoot(t *testing.T) {
	s, tk := runContextState(t, nil)
	s.plan.RunContextIndex = 2
	s.plan.Origin = &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-parent", RootRunID: "ork-root"}
	in, err := s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	rc := in.GetRunContext()
	if rc.GetParentRunId() != "ork-parent" || rc.GetRootRunId() != "ork-root" || rc.GetTreeIndex() != 2 {
		t.Fatalf("child run context = %v", rc)
	}
	// A top-level run names neither: its root is itself.
	s.plan.Origin = &RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER.String(), ID: "trg-1"}
	s.plan.RunContextIndex = 0
	in, err = s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rc := in.GetRunContext(); rc.GetParentRunId() != "" || rc.GetRootRunId() != "" || rc.GetTreeIndex() != 0 {
		t.Fatalf("top-level run context = %v", rc)
	}
}

func TestRunContext_DeclaredAndOwnAttempts(t *testing.T) {
	cases := []struct {
		name      string
		retry     *orkstackv1.RetryPolicy
		ownBefore int32
		wantMax   int32
	}{
		{"no retry", nil, 0, 1},
		{"maximum 0 is one attempt", &orkstackv1.RetryPolicy{MaximumAttempts: 0}, 0, 1},
		{"maximum 1", &orkstackv1.RetryPolicy{MaximumAttempts: 1}, 0, 1},
		{"maximum 5, two spent", &orkstackv1.RetryPolicy{MaximumAttempts: 5}, 2, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, tk := runContextState(t, tc.retry)
			if tc.ownBefore > 0 {
				s.taskRuns["t"].Interruptions = &orkstackv1.TaskInterruptions{OwnAttemptsUsed: tc.ownBefore}
			}
			in, err := s.taskDispatchInput(tk, "t", "main", nil)
			if err != nil {
				t.Fatal(err)
			}
			rc := in.GetRunContext()
			if rc.GetDeclaredMaxAttempts() != tc.wantMax || rc.GetOwnAttemptsBefore() != tc.ownBefore {
				t.Fatalf("declared %d own before %d, want %d and %d",
					rc.GetDeclaredMaxAttempts(), rc.GetOwnAttemptsBefore(), tc.wantMax, tc.ownBefore)
			}
		})
	}
}

func TestRunContext_DeadlineOnlyWithRunTimeout(t *testing.T) {
	s, tk := runContextState(t, nil)
	in, err := s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	if in.GetRunContext().GetRunDeadline() != nil {
		t.Fatal("a run without timeout: sent a deadline")
	}
	deadline := time.Date(2026, 11, 19, 6, 0, 0, 999_999_999, time.UTC)
	s.runTimeout = &runTimeout{seconds: 3600, deadline: deadline}
	in, err = s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := in.GetRunContext().GetRunDeadline().AsTime(); !got.Equal(deadline) {
		t.Fatalf("deadline = %v, want %v", got, deadline)
	}
}

func TestRunContext_ZeroDeadlineAtDispatchFails(t *testing.T) {
	s, tk := runContextState(t, nil)
	s.runTimeout = &runTimeout{seconds: 3600}
	_, err := s.taskDispatchInput(tk, "t", "main", nil)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != RunTimeoutStateInvalidCode || !app.NonRetryable() {
		t.Fatalf("err = %v, want a non-retryable %s", err, RunTimeoutStateInvalidCode)
	}
}

// maximalRunContext is the largest RunContext workflow code can build: every
// id at its 30-character shape, timestamps with nanoseconds, the largest
// counts.
func maximalRunContext() *orkstackv1.RunContext {
	ts := timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999_999_999, time.UTC))
	id := "ork-" + strings.Repeat("z", 26)
	return &orkstackv1.RunContext{
		ObjectSha256:        strings.Repeat("f", 64),
		RootRunId:           id,
		ParentRunId:         id,
		TreeIndex:           2147483647,
		CreatedAt:           ts,
		StartedAt:           ts,
		TaskScheduledAt:     ts,
		DeclaredMaxAttempts: 2147483647,
		OwnAttemptsBefore:   2147483647,
		RunDeadline:         ts,
	}
}

func TestRunContext_MaximalMessageWithinPinnedBound(t *testing.T) {
	size, err := runContextBytes(maximalRunContext())
	if err != nil {
		t.Fatal(err)
	}
	if size > DefaultRunContextMaxBytes {
		t.Fatalf("the largest run context is %d bytes, over the pinned %d", size, DefaultRunContextMaxBytes)
	}
	t.Logf("largest run context: %d of %d bytes", size, DefaultRunContextMaxBytes)
}

// inputAtLimit sets tk's with: so that its input, without the run context,
// serializes to exactly limit bytes.
func inputAtLimit(t *testing.T, s *runState, tk *orkstackv1.Task, limit int64) {
	t.Helper()
	pad := ""
	for range 4 {
		with, err := structpb.NewStruct(map[string]any{"pad": pad})
		if err != nil {
			t.Fatal(err)
		}
		tk.With = with
		in, err := s.taskDispatchInput(tk, "t", "main", nil)
		if err != nil {
			t.Fatal(err)
		}
		in.RunContext = nil
		size, err := task.PayloadSize(in)
		if err != nil {
			t.Fatal(err)
		}
		if size == limit {
			return
		}
		if n := len(pad) + int(limit-size); n >= 0 {
			pad = strings.Repeat("x", n)
		}
	}
	t.Fatal("could not size the input to the limit")
}

func TestCheckInputSize_RunContextBeyondUserBound(t *testing.T) {
	s, tk := runContextState(t, nil)
	inputAtLimit(t, s, tk, task.MaxActivityInputBytes)
	got, err := s.taskDispatch(tk, "t", "main", nil)
	if err != nil {
		t.Fatalf("an input at exactly 256 KiB without its run context was refused: %v", err)
	}
	if got.size <= task.MaxActivityInputBytes {
		t.Fatalf("size %d: the run context should take the input past 256 KiB", got.size)
	}
	// One user byte more is refused, run context or not.
	tk.With.Fields["pad"] = structpb.NewStringValue(tk.With.Fields["pad"].GetStringValue() + "x")
	if _, err := s.taskDispatch(tk, "t", "main", nil); err == nil || !strings.Contains(err.Error(), task.ReasonInputTooLarge) {
		t.Fatalf("one byte over the user bound: err = %v, want %s", err, task.ReasonInputTooLarge)
	}
}

func TestCheckInputSize_RunContextUnderThePlansLimit(t *testing.T) {
	s, tk := runContextState(t, nil)
	s.plan.Limits = WorkspaceLimits{ValuesMaxBytes: 4096}
	inputAtLimit(t, s, tk, 4096)
	if _, err := s.taskDispatch(tk, "t", "main", nil); err != nil {
		t.Fatalf("an input at the plan's limit without its run context was refused: %v", err)
	}
	tk.With.Fields["pad"] = structpb.NewStringValue(tk.With.Fields["pad"].GetStringValue() + "x")
	if _, err := s.taskDispatch(tk, "t", "main", nil); err == nil {
		t.Fatal("one byte over the plan's limit was accepted")
	}
}

func TestDispatchInput_RunContextOverItsBoundFails(t *testing.T) {
	s, tk := runContextState(t, nil)
	s.plan.RunContextMaxBytes = 16
	_, err := s.taskDispatch(tk, "t", "main", nil)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || !app.NonRetryable() || !strings.Contains(err.Error(), "run context") {
		t.Fatalf("err = %v, want a non-retryable refusal naming the run context", err)
	}
}

func TestHistoryGuard_RunContextBound(t *testing.T) {
	s, _ := runContextState(t, nil)
	if got := s.runContextBound(); got != DefaultRunContextMaxBytes {
		t.Fatalf("runContextBound = %d, want %d", got, DefaultRunContextMaxBytes)
	}
	if got := s.valuesBound(); got != task.MaxActivityInputBytes+DefaultRunContextMaxBytes {
		t.Fatalf("valuesBound = %d, want 256 KiB + %d", got, DefaultRunContextMaxBytes)
	}
	plain := &orkstackv1.Task{Id: "h"}
	if got := s.handlerStartBytesOf(plain, HistoryReserve{}); got != task.MaxActivityInputBytes+DefaultRunContextMaxBytes {
		t.Fatalf("handlerStartBytesOf = %d, want 256 KiB + %d", got, DefaultRunContextMaxBytes)
	}
	s.plan.RunContextSHA256 = ""
	if s.runContextBound() != 0 || s.valuesBound() != task.MaxActivityInputBytes ||
		s.handlerStartBytesOf(plain, HistoryReserve{}) != task.MaxActivityInputBytes {
		t.Fatal("a plan without a run context is charged for one")
	}
}

func TestIdempotencyKey_UnchangedByRunContext(t *testing.T) {
	s, tk := runContextState(t, nil)
	with, err := s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	s.plan.RunContextSHA256 = ""
	without, err := s.taskDispatchInput(tk, "t", "main", nil)
	if err != nil {
		t.Fatal(err)
	}
	if with.GetIdempotencyKey() == "" || with.GetIdempotencyKey() != without.GetIdempotencyKey() {
		t.Fatalf("idempotency key %q with a run context, %q without", with.GetIdempotencyKey(), without.GetIdempotencyKey())
	}
}

func TestChildStartPlan_CopiesTheRunContext(t *testing.T) {
	s, _ := runContextState(t, nil)
	planned := &ChildRunPlan{Plan: RunPlan{RunContextIndex: 3}}
	got := s.childStartPlan("release", planned)
	if got.RunContextSHA256 != testRunContextSHA || got.RunContextMaxBytes != DefaultRunContextMaxBytes || got.RunContextIndex != 3 {
		t.Fatalf("child start plan's run context = %q, %d, %d", got.RunContextSHA256, got.RunContextMaxBytes, got.RunContextIndex)
	}
}

func TestSnapshot_RunContextAvailable(t *testing.T) {
	s, _ := runContextState(t, nil)
	if got := s.runContextState().GetStatus(); got != orkstackv1.RunContextStatus_RUN_CONTEXT_STATUS_AVAILABLE {
		t.Fatalf("status = %v, want AVAILABLE", got)
	}
	s.plan.RunContextSHA256 = ""
	if s.runContextState() != nil {
		t.Fatal("a run without a run context says it has one")
	}
}
