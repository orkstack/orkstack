package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// recordedAwaits is a run whose results the engine records, and what each
// read of an await's record answers, in order; the last answer stands.
type recordedAwaits struct {
	*harness
	mu      sync.Mutex
	answers map[string][]any // a ResolutionRead, or an error, by await
	reads   []string         // the awaits read, in order: one a read, its retries aside
	at      []time.Time
	last    map[string]int // each await's read that its retries repeat
	// held is called by a read of the await before it answers.
	held map[string]func()
}

var errReadUnanswered = temporal.NewApplicationError("unavailable: down", engine.RecordStoreUnansweredType)

func newRecordedAwaits(t *testing.T, answers map[string][]any) *recordedAwaits {
	t.Helper()
	h := &recordedAwaits{answers: answers, last: map[string]int{}}
	h.harness = newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "work" {
			return nil, temporal.NewNonRetryableApplicationError("broke", "ActivityFailed", nil)
		}
		return okTask(in)
	})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, req engine.ResolutionReadRequest) (engine.ResolutionRead, error) {
		if wait := h.held[req.TaskID]; wait != nil {
			wait()
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if activity.GetInfo(ctx).Attempt == 1 {
			h.last[req.TaskID] = 0
			for _, id := range h.reads {
				if id == req.TaskID {
					h.last[req.TaskID]++
				}
			}
			h.reads = append(h.reads, req.TaskID)
			h.at = append(h.at, h.env.Now())
		}
		list := h.answers[req.TaskID]
		if len(list) == 0 {
			return engine.ResolutionRead{Outcome: engine.ResolutionNone}, nil
		}
		switch a := list[min(h.last[req.TaskID], len(list)-1)].(type) {
		case error:
			return engine.ResolutionRead{}, a
		case engine.ResolutionRead:
			return a, nil
		}
		return engine.ResolutionRead{}, errors.New("no answer")
	}, activity.RegisterOptions{Name: engine.ActivityReadResolution})
	return h
}

// recordedPlan is the plan of a run whose results the engine records.
func recordedPlan() engine.RunPlan {
	plan := defaultPlan()
	budget := engine.DefaultRecordBudget
	plan.CommitRoot = "mem://store"
	plan.Identity = &engine.IdentityPlan{AdmissionSHA256: strings.Repeat("a", 64), ValueRecords: 1, RecordBudget: &budget,
		ResolutionsPerPass: engine.DefaultResolutionsPerPass, NonesPerAwait: engine.DefaultNonesPerAwait}
	return plan
}

func (h *recordedAwaits) runs(t *testing.T, doc string, edit ...func(*engine.RunPlan)) *orkstackv1.Run {
	t.Helper()
	plan := recordedPlan()
	for _, fn := range edit {
		fn(&plan)
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-1", compile(t, doc), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	return runOf(t, h.harness, "ork-1")
}

// tell is a message that names await id and, as any sender's may, claims more.
func tell(id string) *orkstackv1.AwaitSignal {
	return &orkstackv1.AwaitSignal{TaskId: id, Reject: true, Reason: "forged", Principal: "mallory"}
}

func onRecord(values string) engine.ResolutionRead {
	return engine.ResolutionRead{Outcome: engine.ResolutionFound, RecordSHA256: strings.Repeat("d", 64), Via: "AWAIT_RESOLVED_VIA_API", Principal: "alice", PrincipalVerified: true,
		Values: &engine.ArtifactRefPlan{Name: "values.json", URI: "mem://record", SHA256: strings.Repeat("e", 64), SizeBytes: int64(len(values))}, ValuesJSON: []byte(values)}
}

func rejectedOnRecord() engine.ResolutionRead {
	return engine.ResolutionRead{Outcome: engine.ResolutionFound, Rejected: true, Reason: "too risky", Via: "AWAIT_RESOLVED_VIA_API", Principal: "alice"}
}

func outcome(o string) engine.ResolutionRead { return engine.ResolutionRead{Outcome: o} }

func (h *recordedAwaits) types(id string) string {
	var out []string
	for _, name := range eventTypes(eventsFor(h.harness, "ork-1", id)) {
		if strings.HasPrefix(name, "TASK_") {
			out = append(out, name)
		}
	}
	return strings.Join(out, ",")
}

// TestRecordedAwait_ATellingIsOnlyAWakeUp: what is applied is what the record
// says, and nothing the message claims.
func TestRecordedAwait_ATellingIsOnlyAWakeUp(t *testing.T) {
	h := newRecordedAwaits(t, map[string][]any{"b": {onRecord(`{"model_uri":"s3://m"}`)}})
	h.signalAt(time.Minute, tell("b"))
	run := h.runs(t, awaitedDocument)
	must.OK(t, h.env.GetWorkflowError())
	b := taskRunOf(t, run, "b")
	if b.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || b.GetAwait().GetResolvedBy() != "alice" || !b.GetAwait().GetResolvedByVerified() ||
		b.GetValuesRef().GetUri() != "mem://record" || b.GetRecord().GetSha256() != strings.Repeat("d", 64) || len(h.reads) != 1 {
		t.Fatalf("%v, reads %v", b, h.reads)
	}
	if h.types("b") != "TASK_AWAITING,TASK_RESOLVED,TASK_SUCCEEDED" {
		t.Fatal(h.types("b"))
	}
	for _, in := range h.calls {
		for _, up := range in.GetUpstream() {
			if up.GetTaskId() == "b" && (up.GetCommitUri() != "" || up.GetRecordUri() != "mem://store/metadata/attested/default/ork-1/b/resolution.json") {
				t.Fatalf("c is given %v", up)
			}
		}
	}
	for _, e := range eventsOf(h.harness, "ork-1") {
		if strings.Contains(e.GetMessage(), "mallory") || strings.Contains(e.GetMessage(), "forged") {
			t.Fatalf("an event quotes the message: %s", e.GetMessage())
		}
	}
}

// TestRecordedAwait_WhatATellingsReadAnswers: the await is closed by a record
// and left open by anything else, with its deadline's timer pending.
func TestRecordedAwait_WhatATellingsReadAnswers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		read   any
		status orkstackv1.Status
		code   string
		types  string
		// then is what the deadline's read answers for one left open.
		then any
	}{
		{"a rejection", rejectedOnRecord(), orkstackv1.Status_STATUS_FAILED, engine.AwaitRejectedCode, "TASK_AWAITING,TASK_RESOLVED,TASK_FAILED", nil},
		{"invalid", engine.ResolutionRead{Outcome: engine.ResolutionInvalid, Object: "detail", Why: "missing"}, orkstackv1.Status_STATUS_FAILED, engine.ResolutionRecordInvalidCode, "TASK_AWAITING,TASK_FAILED", nil},
		{"values that are no object, as of any run", onRecord(`[1]`), orkstackv1.Status_STATUS_SUCCEEDED, "", "TASK_AWAITING,TASK_RESOLVED,TASK_SUCCEEDED", nil},
		{"none", outcome(engine.ResolutionNone), orkstackv1.Status_STATUS_FAILED, engine.AwaitTimedOutCode, "TASK_AWAITING,TASK_LOG,TASK_FAILED", outcome(engine.ResolutionNone)},
		{"late", outcome(engine.ResolutionLate), orkstackv1.Status_STATUS_FAILED, engine.AwaitTimedOutCode, "TASK_AWAITING,TASK_LOG,TASK_FAILED", outcome(engine.ResolutionLate)},
		{"unanswered", errReadUnanswered, orkstackv1.Status_STATUS_FAILED, engine.AwaitTimedOutCode, "TASK_AWAITING,TASK_FAILED", outcome(engine.ResolutionNone)},
		{"unsupported", outcome(engine.ResolutionUnsupported), orkstackv1.Status_STATUS_FAILED, engine.AwaitTimedOutCode, "TASK_AWAITING,TASK_FAILED", outcome(engine.ResolutionNone)},
		{"unanswered, and found at the deadline", errReadUnanswered, orkstackv1.Status_STATUS_SUCCEEDED, "", "TASK_AWAITING,TASK_RESOLVED,TASK_SUCCEEDED", onRecord(`{"model_uri":"s3://m"}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answers := []any{tc.read}
			if tc.then != nil {
				answers = append(answers, tc.then)
			}
			h := newRecordedAwaits(t, map[string][]any{"b": answers})
			h.signalAt(time.Minute, tell("b"))
			run := h.runs(t, awaitedDocument)
			b := taskRunOf(t, run, "b")
			if b.GetStatus() != tc.status || b.GetError().GetCode() != tc.code || h.types("b") != tc.types || len(h.reads) != len(answers) {
				t.Fatalf("%v %v, events %s, %d reads", b.GetStatus(), b.GetError(), h.types("b"), len(h.reads))
			}
		})
	}
}

// TestRecordedAwait_TheRulesOfATelling: what is not read.
func TestRecordedAwait_TheRulesOfATelling(t *testing.T) {
	t.Run("past the number that found nothing", func(t *testing.T) {
		h := newRecordedAwaits(t, nil)
		for i := range 6 {
			h.signalAt(time.Duration(i+1)*time.Minute, tell("b"))
		}
		h.runs(t, awaitedDocument)
		// Three tellings, and the deadline's.
		if len(h.reads) != 4 || strings.Count(h.types("b"), "TASK_LOG") != 3 {
			t.Fatalf("%d reads, %s", len(h.reads), h.types("b"))
		}
	})
	t.Run("queued when a read went unanswered", func(t *testing.T) {
		h := newRecordedAwaits(t, map[string][]any{"b": {errReadUnanswered, onRecord(`{"model_uri":"s3://m"}`)}})
		h.env.RegisterDelayedCallback(func() {
			for range 5 {
				h.env.SignalWorkflow(engine.ResolveAwaitSignal, tell("b"))
			}
		}, time.Minute)
		h.runs(t, awaitedDocument)
		must.OK(t, h.env.GetWorkflowError())
		if len(h.reads) != 2 || h.at[1].Sub(h.at[0]) < 58*time.Minute {
			t.Fatalf("read %d times, at %v: the stale ones are dropped and the deadline adopts", len(h.reads), h.at)
		}
	})
	t.Run("of a task that awaits nothing", func(t *testing.T) {
		h := newRecordedAwaits(t, map[string][]any{"b": {onRecord(`{"model_uri":"s3://m"}`)}})
		h.env.RegisterDelayedCallback(func() {
			h.env.SignalWorkflow(engine.ResolveAwaitSignal, tell("b"))
			h.env.SignalWorkflow(engine.ResolveAwaitSignal, tell("b"))
			h.env.SignalWorkflow(engine.ResolveAwaitSignal, tell("nobody"))
		}, time.Minute)
		h.runs(t, awaitedDocument)
		must.OK(t, h.env.GetWorkflowError())
		if len(h.reads) != 1 || strings.Count(h.types("b"), "TASK_LOG") != 1 {
			t.Fatalf("%d reads, %s", len(h.reads), h.types("b"))
		}
	})
}

// TestRecordedAwait_ItsDeadline: the record is read before the run says that
// nobody answered, and once more when that read was not answered.
func TestRecordedAwait_ItsDeadline(t *testing.T) {
	busy := temporal.NewApplicationError("busy: no slot", engine.RecordStoreUnansweredType)
	const again = 5 * time.Minute
	for _, tc := range []struct {
		name    string
		answers []any
		code    string // "" is adopted
		reads   int
		says    string
	}{
		{"nobody answered", []any{outcome(engine.ResolutionNone)}, engine.AwaitTimedOutCode, 1, "was not resolved within"},
		{"an answer that came late", []any{outcome(engine.ResolutionLate)}, engine.AwaitTimedOutCode, 1, "was not resolved within"},
		{"an answer the run was not told of", []any{onRecord(`{"model_uri":"s3://m"}`)}, "", 1, ""},
		{"a rejection", []any{rejectedOnRecord()}, engine.AwaitRejectedCode, 1, "too risky"},
		{"invalid", []any{outcome(engine.ResolutionInvalid)}, engine.ResolutionRecordInvalidCode, 1, "cannot be used"},
		{"read the second time", []any{errReadUnanswered, onRecord(`{"model_uri":"s3://m"}`)}, "", 2, ""},
		{"not there the second time", []any{errReadUnanswered, outcome(engine.ResolutionNone)}, engine.AwaitTimedOutCode, 2, ""},
		{"unread twice", []any{errReadUnanswered}, engine.ResolutionRecordUnavailableCode, 2, "a decision may be on record"},
		{"a newer format, twice", []any{outcome(engine.ResolutionUnsupported)}, engine.ResolutionRecordUnavailableCode, 2, "newer format"},
		{"busy the second time", []any{errReadUnanswered, busy, onRecord(`{"model_uri":"s3://m"}`)}, "", 3, ""},
		{"busy the second and the third time", []any{errReadUnanswered, busy, busy}, engine.ResolutionRecordUnavailableCode, 3, "a decision may be on record"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecordedAwaits(t, map[string][]any{"b": tc.answers})
			run := h.runs(t, awaitedDocument)
			b := taskRunOf(t, run, "b")
			if tc.code == "" {
				must.OK(t, h.env.GetWorkflowError())
			}
			if run.GetError().GetTaskCode() != tc.code || run.GetError().GetCategory() != b.GetError().GetCategory() {
				t.Fatalf("the run failed %v, its task %v", run.GetError(), b.GetError())
			}
			if b.GetError().GetCode() != tc.code || !strings.Contains(b.GetError().GetMessage(), tc.says) || len(h.reads) != tc.reads {
				t.Fatalf("%v %v after %d reads", b.GetStatus(), b.GetError(), len(h.reads))
			}
			for i := 1; i < len(h.at); i++ {
				if gap := h.at[i].Sub(h.at[i-1]); gap < again || gap > again+2*time.Minute {
					t.Fatalf("read %d came %v after the one before", i+1, gap)
				}
			}
		})
	}
}

// awaitsDocument awaits five approvals whose deadlines fall together.
const awaitsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: awaits
tasks:
  - {id: a, await: {timeout_seconds: 600}}
  - {id: b, await: {timeout_seconds: 600}}
  - {id: c, await: {timeout_seconds: 600}}
  - {id: d, await: {timeout_seconds: 600}}
`

// TestRecordedAwait_DeadlinesThatFallTogether cost one read that is not
// answered, and one more at the set's timer, never one each.
func TestRecordedAwait_DeadlinesThatFallTogether(t *testing.T) {
	t.Run("a store that does not answer", func(t *testing.T) {
		h := newRecordedAwaits(t, map[string][]any{"a": {errReadUnanswered}, "b": {errReadUnanswered}, "c": {errReadUnanswered}, "d": {errReadUnanswered}})
		run := h.runs(t, awaitsDocument)
		if len(h.reads) != 2 || run.GetStatus() != orkstackv1.Status_STATUS_FAILED || taskRunOf(t, run, "a").GetError().GetCode() != engine.ResolutionRecordUnavailableCode {
			t.Fatalf("read %v, %v", h.reads, run.GetError())
		}
	})
	t.Run("a store that answers the second time", func(t *testing.T) {
		ok := onRecord(`{}`)
		h := newRecordedAwaits(t, map[string][]any{"a": {errReadUnanswered, ok}, "b": {ok}, "c": {ok}, "d": {ok}})
		run := h.runs(t, awaitsDocument)
		must.OK(t, h.env.GetWorkflowError())
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || strings.Join(h.reads, "") != "aabcd" {
			t.Fatalf("read %v, %v", h.reads, run.GetStatus())
		}
	})
	t.Run("a telling of one that is confirming", func(t *testing.T) {
		ok := onRecord(`{}`)
		h := newRecordedAwaits(t, map[string][]any{"a": {errReadUnanswered, ok}, "b": {ok}, "c": {ok}, "d": {ok}})
		h.signalAt(12*time.Minute, tell("a"))
		h.runs(t, awaitsDocument)
		must.OK(t, h.env.GetWorkflowError())
		if strings.Join(h.reads, "") != "aabcd" {
			t.Fatalf("read %v", h.reads)
		}
	})
}

// finishedAt is when task id ended, which a run that ran it says.
func finishedAt(t *testing.T, run *orkstackv1.Run, id string) time.Time {
	t.Helper()
	at := taskRunOf(t, run, id).GetFinishedAt()
	if at == nil {
		t.Fatalf("task %s has no finish time", id)
	}
	return at.AsTime()
}

// repeatedDocument has a failure handler that must not run.
const repeatedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: repeated
tasks:
  - {id: gate, await: {timeout_seconds: 600}}
  - {id: nap, sleep: {seconds: 120}}
  - id: after
    needs: [nap]
    uses: shell
    with: {command: "echo a"}
  - id: last
    needs: [gate]
    uses: shell
    with: {command: "echo a"}
  - id: handler
    needs: [gate]
    join: any_failed
    uses: shell
    with: {command: "echo a"}
`

// TestRecordedAwait_ToldManyTimes: an answer repeated all the while its record
// cannot be read delays neither a sleep nor the deadline, which is read twice
// and takes it; nothing ends the run, and no failure handler runs.
func TestRecordedAwait_ToldManyTimes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		until time.Duration // when the record can be read again
	}{
		{"and read at the deadline", 9 * time.Minute},
		{"and read at its second read", 12 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecordedAwaits(t, nil)
			began := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			h.env.SetStartTime(began)
			h.env.OnActivity(engine.ActivityReadResolution, mock.Anything, mock.Anything).Return(
				func(context.Context, engine.ResolutionReadRequest) (engine.ResolutionRead, error) {
					if h.env.Now().Sub(began) < tc.until {
						return engine.ResolutionRead{}, errReadUnanswered
					}
					return onRecord(`{}`), nil
				})
			for i := range 48 {
				h.signalAt(time.Duration(i+1)*10*time.Second, tell("gate"))
			}
			run := h.runs(t, repeatedDocument)
			must.OK(t, h.env.GetWorkflowError())
			at := func(id string) time.Duration { return finishedAt(t, run, id).Sub(began) }
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || taskRunOf(t, run, "handler").GetStatus() != orkstackv1.Status_STATUS_SKIPPED {
				t.Fatalf("the run %v, its handler %v", run.GetStatus(), taskRunOf(t, run, "handler").GetStatus())
			}
			if got := at("after"); got < 2*time.Minute || got > 150*time.Second {
				t.Fatalf("a task behind a two-minute sleep ended %s in", got)
			}
			want := 10 * time.Minute
			if tc.until > want {
				want += 5 * time.Minute
			}
			if got := at("last"); got < want || got > want+2*time.Minute {
				t.Fatalf("the answer was taken %s in, want about %s", got, want)
			}
		})
	}
}

// siblingDocument awaits three approvals beside a sleep another task needs.
const siblingDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: siblings
tasks:
  - {id: a, await: {timeout_seconds: 600}}
  - {id: b, await: {timeout_seconds: 600}}
  - {id: c, await: {timeout_seconds: 600}}
  - {id: nap, sleep: {seconds: 120}}
  - id: after
    needs: [nap]
    uses: shell
    with: {command: "echo a"}
`

// TestRecordedAwait_ManyToldOfWhatCannotBeRead: no await fails or is applied
// unread, a sibling that ends meanwhile is taken and its dependent started,
// and each answer is taken at its deadline.
func TestRecordedAwait_ManyToldOfWhatCannotBeRead(t *testing.T) {
	ok := onRecord(`{}`)
	h := newRecordedAwaits(t, map[string][]any{"a": {errReadUnanswered, ok}, "b": {errReadUnanswered, ok}, "c": {errReadUnanswered, ok}})
	for i, id := range []string{"a", "b", "c"} {
		h.signalAt(time.Minute+time.Duration(i)*time.Second, tell(id))
	}
	run := h.runs(t, siblingDocument)
	must.OK(t, h.env.GetWorkflowError())
	first := finishedAt(t, run, "a")
	for _, id := range []string{"a", "b", "c"} {
		tr := taskRunOf(t, run, id)
		if tr.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || tr.GetAwait().GetResolvedBy() != "alice" {
			t.Fatalf("%s: %v, %v", id, tr.GetStatus(), tr.GetError())
		}
		if at := finishedAt(t, run, id); at.Before(first) {
			first = at
		}
	}
	after := taskRunOf(t, run, "after")
	if at := finishedAt(t, run, "after"); after.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !at.Before(first.Add(-5*time.Minute)) {
		t.Fatalf("after ended %v, the first answer was taken %v", at, first)
	}
}

// handlerAwaitDocument fails, and then awaits in two failure handlers.
const handlerAwaitDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: handlers
tasks:
  - id: work
    uses: shell
    with: {command: "echo a"}
  - {id: h1, needs: [work], join: any_failed, await: {timeout_seconds: 600}}
  - {id: h2, needs: [work], join: any_failed, await: {timeout_seconds: 600}}
`

// TestRecordedAwait_InAFailureHandler: each ends for itself, stops nothing
// else, and the run keeps its own failure.
func TestRecordedAwait_InAFailureHandler(t *testing.T) {
	for _, tc := range []struct {
		name    string
		answers map[string][]any
		h1, h2  string // each handler's code; "" is SUCCEEDED
		reads   string
	}{
		{"adopted at the deadline", map[string][]any{"h1": {onRecord(`{}`)}, "h2": {outcome(engine.ResolutionNone)}}, "", engine.AwaitTimedOutCode, "h1h2"},
		{"unread twice", map[string][]any{"h1": {errReadUnanswered}, "h2": {errReadUnanswered}}, engine.ResolutionRecordUnavailableCode, engine.ResolutionRecordUnavailableCode, "h1h1"},
		{"a newer format, and one adopted", map[string][]any{"h1": {errReadUnanswered, outcome(engine.ResolutionUnsupported)}, "h2": {onRecord(`{}`)}}, engine.ResolutionRecordUnavailableCode, "", "h1h1h2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecordedAwaits(t, tc.answers)
			run := h.runs(t, handlerAwaitDocument)
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "broke") {
				t.Fatalf("%v", run.GetError())
			}
			if a, b := taskRunOf(t, run, "h1").GetError().GetCode(), taskRunOf(t, run, "h2").GetError().GetCode(); a != tc.h1 || b != tc.h2 || strings.Join(h.reads, "") != tc.reads {
				t.Fatalf("h1 %q, h2 %q, read %v", a, b, h.reads)
			}
		})
	}
}

// TestRecordedAwait_AnEndingReadyAtAHandlersDeadline: two handlers' awaits reach
// their deadlines, and while the first one's record is read an ending of the
// run becomes ready. The second is not read before that ending: it ends with
// it, or, where an unpause wins over the pause's expiry, is read after all.
// The run ends in every case.
func TestRecordedAwait_AnEndingReadyAtAHandlersDeadline(t *testing.T) {
	const due = 600 * time.Second // the awaits' deadline, and the ending a second after it
	pause := func(h *recordedAwaits, on bool) {
		h.env.SignalWorkflow(engine.PauseSignal, &orkstackv1.RunPauseSignal{Format: 1, Pause: on, Principal: "alice", Reason: "db",
			TimeoutSeconds: 541, RequestId: fmt.Sprintf("req-%v", on)})
	}
	pauses := func(p *engine.RunPlan) {
		p.Pause = &engine.PausePlan{DefaultTimeoutSeconds: 3600, MaxTimeoutSeconds: 7200, ApplyReserveEvents: 20, CascadeEventsPerChild: 4,
			InheritedGraceSeconds: 600, InheritedGraceMaxLevels: 8}
	}
	for _, tc := range []struct {
		name, doc string
		plan      func(*engine.RunPlan)
		during    func(*recordedAwaits)
		h2        orkstackv1.Status
		says      string
		reads     string
	}{
		{"the run timeout's hard end", strings.Replace(handlerAwaitDocument, "name: handlers\n", "name: handlers\ntimeout: 300s\n", 1),
			func(p *engine.RunPlan) {
				p.RunTimeout = &engine.RunTimeoutPlan{HandlerGraceSeconds: 301, ResolutionsPerPass: engine.DefaultResolutionsPerPass}
			},
			func(*recordedAwaits) {}, orkstackv1.Status_STATUS_CANCELED, "hard end", "h1"},
		{"a pause's expiry", handlerAwaitDocument, pauses,
			func(h *recordedAwaits) { h.env.RegisterDelayedCallback(func() { pause(h, true) }, time.Minute) },
			orkstackv1.Status_STATUS_CANCELED, "pause ended", "h1"},
		{"an unpause that wins over the pause's expiry", handlerAwaitDocument, pauses,
			func(h *recordedAwaits) {
				h.env.RegisterDelayedCallback(func() { pause(h, true) }, time.Minute)
				h.env.RegisterDelayedCallback(func() { pause(h, false) }, due+1200*time.Millisecond)
			}, orkstackv1.Status_STATUS_SUCCEEDED, "", "h1h2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecordedAwaits(t, map[string][]any{"h1": {onRecord(`{}`)}, "h2": {onRecord(`{}`)}})
			began := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			h.env.SetStartTime(began)
			h.env.SetTestTimeout(time.Minute)
			// The environment's clock runs as the wall's while a read is held.
			read := make(chan struct{})
			h.env.RegisterDelayedCallback(func() { close(read) }, due+1500*time.Millisecond)
			h.held = map[string]func(){"h1": func() { <-read }}
			tc.during(h)
			run := h.runs(t, tc.doc, tc.plan)
			h1, h2 := taskRunOf(t, run, "h1"), taskRunOf(t, run, "h2")
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "broke") {
				t.Fatalf("the run %v: %v", run.GetStatus(), run.GetError())
			}
			if h1.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || h2.GetStatus() != tc.h2 || strings.Join(h.reads, "") != tc.reads {
				t.Fatalf("h1 %v, h2 %v %v, read %v", h1.GetStatus(), h2.GetStatus(), h2.GetError(), h.reads)
			}
			said := ""
			for _, e := range eventsFor(h.harness, "ork-1", "h2") {
				said = e.GetMessage()
			}
			if !strings.Contains(said, tc.says) {
				t.Fatalf("h2 ended %q, want %q", said, tc.says)
			}
		})
	}
}

// TestRecordedAwait_ASealedRun: a message that does not open is an event of the
// run and reads nothing; one that opens has the record read and applied.
func TestRecordedAwait_ASealedRun(t *testing.T) {
	s := newSealedRun(t, "ork-await")
	h := newRecordedAwaits(t, map[string][]any{"b": {onRecord(`{"model_uri":"s3://m"}`)}})
	h.env.SetDataConverter(payloadcrypt.NewConverter(engine.DataConverter(), s.keys).WithReaderKeys(treeReader(s.keys, map[string]any{payloadcrypt.MemoField: s.memo})))
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{payloadcrypt.WorkflowInterceptor(s.keys)}}))
	h.env.SetStartWorkflowOptions(clientOptions("ork-await"))
	wf := compile(t, awaitedDocument)
	memo := engine.RunStartMemo(wf, recordedPlan())
	memo[payloadcrypt.MemoField] = s.memo
	must.OK(t, h.env.SetMemoOnStart(memo))
	sealedOrker(h.harness, s.scope)
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(engine.ResolveAwaitSignal, payloadcrypt.Seal(s.scope.For(payloadcrypt.RunContext("ork-sibling")), tell("b")))
	}, time.Minute)
	h.env.RegisterDelayedCallback(func() {
		if len(h.reads) != 0 {
			t.Errorf("a message that does not open had %v read", h.reads)
		}
		h.env.SignalWorkflow(engine.ResolveAwaitSignal, payloadcrypt.Seal(s.scope, tell("b")))
	}, 2*time.Minute)
	args := []any{"ork-await", wf, payload(t, nil), recordedPlan()}
	for i := range args {
		args[i] = payloadcrypt.Seal(s.scope, args[i])
	}
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflowRaw, rawOptions())
	h.env.ExecuteWorkflow(rawRunWorkflow, args...)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if got := unreadableEvents(h.harness, "ork-await"); len(got) != 1 {
		t.Fatalf("RESOLUTION_UNREADABLE events = %v, want one", got)
	}
	resolved := eventsFor(h.harness, "ork-await", "b")
	if types := strings.Join(eventTypes(resolved), ","); types != "TASK_AWAITING,TASK_RESOLVED,TASK_SUCCEEDED" ||
		!strings.Contains(resolved[1].GetMessage(), "alice") || len(h.reads) != 1 {
		t.Fatalf("b's events = %s (%q), read %v", types, resolved[1].GetMessage(), h.reads)
	}
}

// guardedHandlerDocument fails, awaits in a failure handler, and has another
// behind it.
const guardedHandlerDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: guarded
tasks:
  - id: work
    uses: shell
    with: {command: "echo a"}
  - {id: h1, needs: [work], join: any_failed, await: {timeout_seconds: 600}}
  - id: h2
    needs: [work, h1]
    join: all_done
    uses: shell
    with: {command: "echo a"}
`

// TestRecordedAwait_InAFailureHandlerAtTheHistoryGuard: a handler's await whose
// deadline comes with no history left for its read closes with it, the handler
// not yet started is canceled, and the run ends with its own failure; with
// history left, the same record is adopted.
func TestRecordedAwait_InAFailureHandlerAtTheHistoryGuard(t *testing.T) {
	for name, atLimit := range map[string]bool{"at the limit": true, "with history left": false} {
		t.Run(name, func(t *testing.T) {
			h := newRecordedAwaits(t, map[string][]any{"h1": {onRecord(`{}`)}})
			if atLimit {
				h.env.RegisterDelayedCallback(func() { h.env.SetCurrentHistoryLength(int(engine.DefaultHistoryMaxEvents) - 10) }, 5*time.Minute)
			}
			run := h.runs(t, guardedHandlerDocument, func(p *engine.RunPlan) {
				p.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents, MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
			})
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "broke") {
				t.Fatalf("%v %v", run.GetStatus(), run.GetError())
			}
			want, reads := orkstackv1.Status_STATUS_SUCCEEDED, 1
			if atLimit {
				want, reads = orkstackv1.Status_STATUS_CANCELED, 0
			}
			for _, id := range []string{"h1", "h2"} {
				if got := taskRunOf(t, run, id); got.GetStatus() != want {
					t.Errorf("%s: %v %v, want %v", id, got.GetStatus(), got.GetError(), want)
				}
			}
			if len(h.reads) != reads {
				t.Errorf("read %v", h.reads)
			}
		})
	}
}

// confirmingThenFailing has an approval past its deadline when its sibling
// fails, and an approval in the failure handler.
const confirmingThenFailing = `
apiVersion: orkstack/v1
kind: Orkflow
name: confirming
tasks:
  - {id: first, await: {timeout_seconds: 60}}
  - {id: wait, sleep: {seconds: 120}}
  - id: work
    needs: [wait]
    uses: shell
    with: {command: "echo a"}
  - {id: h1, needs: [work], join: any_failed, await: {timeout_seconds: 60}}
`

// TestRecordedAwait_AConfirmingSetTheRunsFailureEnded leaves nothing for a
// handler's own: its record is read at its deadline and once more.
func TestRecordedAwait_AConfirmingSetTheRunsFailureEnded(t *testing.T) {
	h := newRecordedAwaits(t, map[string][]any{"first": {errReadUnanswered}, "h1": {errReadUnanswered, onRecord(`{}`)}})
	run := h.runs(t, confirmingThenFailing)
	if tr := taskRunOf(t, run, "h1"); tr.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("h1 %v %v, read %v", tr.GetStatus(), tr.GetError(), h.reads)
	}
	if strings.Join(h.reads, " ") != "first h1 h1" || taskRunOf(t, run, "first").GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("read %v, first %v", h.reads, taskRunOf(t, run, "first").GetStatus())
	}
}

// TestRecordedRun_APlanWithNoBounds is refused before it starts anything.
func TestRecordedRun_APlanWithNoBounds(t *testing.T) {
	h := newRecordedAwaits(t, nil)
	plan := defaultPlan()
	plan.Identity = &engine.IdentityPlan{AdmissionSHA256: strings.Repeat("a", 64), ValueRecords: 1}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-1", compile(t, awaitedDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if run := runOf(t, h.harness, "ork-1"); run.GetError().GetCode() != engine.RunDefinitionUnsupportedCode || len(h.taskOrder()) != 0 {
		t.Fatalf("%v, started %v", run.GetError(), h.taskOrder())
	}
}

// TestRecordedAwait_ACancelDuringARead ends the run canceled, and the await
// with it.
func TestRecordedAwait_ACancelDuringARead(t *testing.T) {
	h := newRecordedAwaits(t, map[string][]any{"b": {errReadUnanswered}})
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 62*time.Minute)
	run := h.runs(t, awaitedDocument)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || taskRunOf(t, run, "b").GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("%v %v", run.GetStatus(), taskRunOf(t, run, "b"))
	}
}

// TestRecordedAwait_TheEndingDrainIsBounded: one event an await, and one line
// for what is left.
func TestRecordedAwait_TheEndingDrainIsBounded(t *testing.T) {
	h := newRecordedAwaits(t, map[string][]any{"b": {rejectedOnRecord()}})
	h.env.RegisterDelayedCallback(func() {
		for range 40 {
			h.env.SignalWorkflow(engine.ResolveAwaitSignal, tell("b"))
		}
	}, time.Minute)
	h.runs(t, awaitedDocument)
	if got := strings.Count(h.types("b"), "TASK_LOG"); got != 1 {
		t.Fatalf("%d events for 39 resolutions left queued", got)
	}
}
