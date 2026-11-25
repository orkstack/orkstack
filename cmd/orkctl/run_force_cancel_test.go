package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// stubForceCancels answers a force cancel's requests as it is set to; each
// status is answered once, the last kept.
type stubForceCancels struct {
	mu       sync.Mutex
	asks     []engine.ForceCancelAsk
	answer   engine.ForceCancelAnswer
	err      error
	statuses []engine.ForceCancelStatus
	failing  []error
	below    engine.FCSubtree
	stopped  []string
	read     []string
}

func (s *stubForceCancels) Request(_ context.Context, ask engine.ForceCancelAsk) (engine.ForceCancelAnswer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asks = append(s.asks, ask)
	return s.answer, s.err
}

func (s *stubForceCancels) Status(_ context.Context, _, _, requestID string) (engine.ForceCancelStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.read = append(s.read, requestID)
	if len(s.failing) > 0 {
		err := s.failing[0]
		s.failing = s.failing[1:]
		return engine.ForceCancelStatus{}, err
	}
	if len(s.statuses) == 0 {
		return engine.ForceCancelStatus{}, engine.ErrForceCancelNotFound
	}
	next := s.statuses[0]
	if len(s.statuses) > 1 {
		s.statuses = s.statuses[1:]
	}
	return next, nil
}

func (s *stubForceCancels) Stop(_ context.Context, _, runID string) (string, error) {
	s.stopped = append(s.stopped, runID)
	return "fc-1", s.err
}

func (s *stubForceCancels) Below(context.Context, string, string) (engine.FCSubtree, error) {
	return s.below, s.err
}

func forceCancelCLI(t *testing.T, s *stubForceCancels) *fixture {
	t.Helper()
	poll := forceCancelPoll
	forceCancelPoll = time.Millisecond
	t.Cleanup(func() { forceCancelPoll = poll })
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.ForceCancels = s })
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, OrkflowName: "hello-ork"})
	return f
}

func decided(cascade bool) engine.ForceCancelDecision {
	return engine.ForceCancelDecision{RequestID: "fc-1", Principal: "ada", PrincipalVerified: true, Reason: "stuck", GraceSeconds: 90, Cascade: cascade}
}

// cancel --force asks for exactly what its flags say, and says what became of
// it.
func TestRunForceCancel(t *testing.T) {
	s := &stubForceCancels{answer: engine.ForceCancelAnswer{State: engine.FCStateRequested, RequestID: "fc-1", Decision: decided(false), Squatters: 2, JoinSkipped: true}}
	f := forceCancelCLI(t, s)
	f.run("run", "cancel", "ork-r", "--force", "--reason", "stuck", "--grace", "90s", "--request-id", "mine").requireExit(t, 0).
		requireContains(t, "Force cancel requested for run ork-r", "fc-1", "1m30s", "2 that claimed", "could not be told")
	if a := s.asks[0]; a.Reason != "stuck" || a.Grace != 90*time.Second || a.ClientRequestID != "mine" || a.Cascade {
		t.Errorf("asked %+v", a)
	}
	s.answer = engine.ForceCancelAnswer{State: engine.FCStateWaiting, RequestID: "fc-1", Attached: true, Decision: decided(false)}
	f.run("run", "cancel", "ork-r", "--force", "--reason", "mine").requireExit(t, 0).requireContains(t, "by ada (verified) is already in progress; its reason and grace apply", "stuck")
	s.answer = engine.ForceCancelAnswer{State: engine.FCStateRequested, RequestID: "fc-1", Direct: true}
	f.run("run", "cancel", "ork-r", "--force", "--reason", "x").requireExit(t, 0).requireContains(t, "Run ork-r was ended")
	s.answer = engine.ForceCancelAnswer{State: engine.FCStateAlreadyEnded}
	f.run("run", "cancel", "ork-r", "--force", "--reason", "x", "--wait").requireExit(t, 0).requireContains(t, "had already ended")
	// The same request id, which asked for no cascade before.
	for _, state := range []string{engine.FCStateWaiting, engine.FCStateAlreadyEnded} {
		s.answer = engine.ForceCancelAnswer{State: state, RequestID: "fc-1", Attached: true, Decision: decided(false)}
		f.run("run", "cancel", "ork-r", "--force", "--cascade", "--wait", "--reason", "x", "--request-id", "mine").requireExit(t, 1).requireStderrContains(t, "no cascade was started")
	}
	s.answer = engine.ForceCancelAnswer{State: engine.FCStateAlreadyEnded, RequestID: "fc-1", Attached: true, Decision: decided(true)}
	s.statuses = []engine.ForceCancelStatus{{Decision: decided(true), State: engine.FCStateEnded, Cascade: &engine.ForceCancelCascade{State: engine.FCCascadePartial}}}
	f.run("run", "cancel", "ork-r", "--force", "--cascade", "--wait", "--reason", "x", "--request-id", "mine").requireExit(t, 1).requireStderrContains(t, "the cascade is partial")
	s.answer = engine.ForceCancelAnswer{State: engine.FCStateRequested, RequestID: "fc-1", Decision: decided(true)}
	f.run("run", "cancel", "ork-r", "--force", "--cascade", "--reason", "x").requireExit(t, 6).requireStderrContains(t, "the cascade is still running")
	if !s.asks[len(s.asks)-1].Cascade {
		t.Error("no cascade was asked for")
	}
	if got := f.run("run", "cancel", "ork-r", "--force", "--cascade", "--reason", "x", "-j"); got.code != 6 || !strings.Contains(got.stdout, `"requestId"`) {
		t.Errorf("%d: %s", got.code, got.stdout)
	}
}

// A refusal is told from a failure, and flags that say nothing together ask
// for nothing.
func TestRunForceCancel_Refused(t *testing.T) {
	s := &stubForceCancels{}
	f := forceCancelCLI(t, s)
	f.run("run", "cancel", "ork-r", "--force").requireExit(t, 5).requireStderrContains(t, "reason")
	f.run("run", "cancel", "ork-x", "--force", "--reason", "x").requireExit(t, 5).requireStderrContains(t, "not found")
	for err, code := range map[error]int{engine.ErrForceCancelGraceTooShort: 5, engine.ErrForceCancelRetriesExhausted: 5, engine.ErrForceCancelStale: 5,
		serviceerror.NewUnavailable("down"): 1, errors.New("what"): 1} {
		s.err = err
		f.run("run", "cancel", "ork-r", "--force", "--reason", "x").requireExit(t, code)
	}
	asked := len(s.asks)
	for _, args := range [][]string{{"--cascade"}, {"--wait"}, {"--grace", "1m"}, {"--stop"}, {"--dry-run"}, {"--request-id", "x"}, {"--force", "--dry-run"}, {"--force", "--stop", "--cascade"},
		{"--force", "--stop", "--wait"}, {"--force", "--stop", "--grace", "1m"}, {"--force", "--cascade", "--dry-run", "--wait"}} {
		f.run(append([]string{"run", "cancel", "ork-r", "--reason", "x"}, args...)...).requireExit(t, 1)
	}
	if len(s.asks) != asked || len(s.stopped) != 0 {
		t.Errorf("asked %+v, stopped %v", s.asks[asked:], s.stopped)
	}
}

// --cascade --dry-run lists, and --stop stops, and neither asks for more.
func TestRunForceCancel_PreviewAndStop(t *testing.T) {
	s := &stubForceCancels{below: engine.FCSubtree{Open: []engine.FCBelow{{RunID: "ork-a", ParentRunID: "ork-r", Orkflow: "billing/pay", Depth: 1}}, Truncated: "scan", Excluded: map[string]string{"ork-y": "unreadable", "ork-z": "other_namespace"}}}
	f := forceCancelCLI(t, s)
	f.run("run", "cancel", "ork-r", "--force", "--cascade", "--dry-run").requireExit(t, 0).
		requireContains(t, "1 open runs below run ork-r; nothing was done", "ork-a", "billing/pay", "cut short (scan)", "Left out: ork-y (unreadable)", "Left out: - (other_namespace)")
	f.run("run", "cancel", "ork-r", "--force", "--stop").requireExit(t, 0).requireContains(t, "asked to stop (request fc-1)")
	if len(s.asks) != 0 || len(s.stopped) != 1 {
		t.Errorf("asked %v, stopped %v", s.asks, s.stopped)
	}
	s.err = engine.ErrForceCancelNotFound
	f.run("run", "cancel", "ork-r", "--force", "--stop").requireExit(t, 5).requireStderrContains(t, "no cascade")
}

// TestRunForceCancel_WaitExit: --wait is over when the force cancel and its
// cascade are, and exits 0 only when nothing is left.
func TestRunForceCancel_WaitExit(t *testing.T) {
	row := func(id, state, cause string) engine.FCBelowResult {
		return engine.FCBelowResult{FCBelow: engine.FCBelow{RunID: id, Orkflow: "pay"}, State: state, Cause: cause}
	}
	cascade := func(c engine.ForceCancelCascade) engine.ForceCancelStatus {
		return engine.ForceCancelStatus{Decision: decided(true), State: engine.FCStateForceCanceled, Cascade: &c}
	}
	running := engine.ForceCancelStatus{Decision: decided(false), Running: true, State: engine.FCStateRequested}
	manyOpen := &engine.FCAfter{Count: 70}
	for i := range 70 {
		manyOpen.RunIDs = append(manyOpen.RunIDs, "ork-open-"+strings.Repeat("x", i+1))
	}
	for _, tt := range []struct {
		name     string
		statuses []engine.ForceCancelStatus
		failing  []error
		below    []engine.FCBelow
		code     int
		says     []string
		not      string
	}{
		{name: "ended", statuses: []engine.ForceCancelStatus{running, running, {Decision: decided(false), State: engine.FCStateForceCanceled}}, says: []string{"Run ork-r: force_canceled"}},
		{name: "ended, and what it left open", statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: engine.FCStateForceCanceled}}, below: []engine.FCBelow{{RunID: "ork-a", Orkflow: "pay"}},
			says: []string{"1 open runs below it were only asked to cancel", "orkctl run cancel ork-a --force"}},
		{name: "the run's own cancel", statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: engine.FCStateCanceled}}, says: []string{"canceled"}},
		{name: "unaudited", statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: engine.FCStateForceCanceled, Unaudited: true}}, says: []string{"a record of it could not be written"}},
		{name: "why it was left as it is", statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: engine.FCStateAlreadyEnded, Cause: "unverified_record"}}, says: []string{"Cause: unverified_record"}},
		{name: "a state this orkctl does not know", statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: "new"}}, code: 1, says: []string{"not known to this orkctl"}},
		{name: "failed", statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: engine.FCStateFailed, Cause: "clock_skew"}}, code: 1, says: []string{"failed (clock_skew); the run may still be open"}},
		{name: "incomplete", statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: engine.FCStateForceCanceled, Incomplete: true, Cause: "event_unpublished"}}, code: 1, says: []string{"incomplete (event_unpublished)"}},
		{name: "not there yet, then an engine restarting", failing: []error{engine.ErrForceCancelNotFound, serviceerror.NewUnavailable("down")}, statuses: []engine.ForceCancelStatus{{Decision: decided(false), State: engine.FCStateEnded}}, says: []string{"ended"}},
		{name: "cannot be read", failing: []error{errors.New("what")}, code: 1, says: []string{"what"}},
		{name: "a clean cascade", statuses: []engine.ForceCancelStatus{{Decision: decided(true), Running: true, State: engine.FCStateRequested, Cascade: &engine.ForceCancelCascade{State: engine.FCCascadeRunning, Results: []engine.FCBelowResult{row("ork-a", engine.FCStateCanceled, ""), row("ork-b", engine.FCBelowWaiting, "")}}},
			cascade(engine.ForceCancelCascade{State: engine.FCCascadeComplete, Covered: 1, Truncated: "scan", Rerun: true, Results: []engine.FCBelowResult{row("ork-a", engine.FCStateCanceled, ""), row("ork-b", engine.FCStateForceCanceled, "")}, After: &engine.FCAfter{}})},
			says: []string{"ork-a  pay  canceled", "ork-b  pay  force_canceled", "1 deeper runs"}, not: "may remain open"},
		{name: "one left open", statuses: []engine.ForceCancelStatus{cascade(engine.ForceCancelCascade{State: engine.FCCascadePartial, After: &engine.FCAfter{Count: 1, RunIDs: []string{"ork-q"}}})}, code: 1,
			says: []string{"Some runs under ork-r may still be open.", "orkctl run cancel ork-q --force", "the cascade is partial"}},
		{name: "more than are listed", statuses: []engine.ForceCancelStatus{cascade(engine.ForceCancelCascade{State: engine.FCCascadePartial, After: manyOpen})}, code: 1, says: []string{"and 20 more"}},
		{name: "unconfirmed", statuses: []engine.ForceCancelStatus{cascade(engine.ForceCancelCascade{State: engine.FCCascadePartial, After: &engine.FCAfter{Unconfirmed: "too_many_runs"}})}, code: 1, says: []string{"couldn't be checked (too_many_runs)"}},
		{name: "failed rows", statuses: []engine.ForceCancelStatus{cascade(engine.ForceCancelCascade{State: engine.FCCascadePartial, NotStarted: 2, Rerun: true, Excluded: map[string]string{"ork-z": "unreadable"}, Results: []engine.FCBelowResult{row("ork-a", engine.FCStateFailed, "not_below")}})}, code: 1,
			says: []string{"ork-a  pay  failed (not_below)", "2 runs were not force-canceled", "Left out: ork-z (unreadable)", "may remain open"}},
		{name: "stopped", statuses: []engine.ForceCancelStatus{cascade(engine.ForceCancelCascade{State: engine.FCCascadeStopped})}, code: 1, says: []string{"the cascade is stopped"}},
		{name: "not over", statuses: []engine.ForceCancelStatus{running}, code: 4, says: []string{"not over after 50ms; it goes on"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &stubForceCancels{answer: engine.ForceCancelAnswer{State: engine.FCStateRequested, RequestID: "fc-1", Decision: decided(true)}, statuses: tt.statuses, failing: tt.failing, below: engine.FCSubtree{Open: tt.below}}
			wait := "10s"
			if tt.code == 4 {
				wait = "50ms"
			}
			got := forceCancelCLI(t, s).run("run", "cancel", "ork-r", "--force", "--reason", "x", "--wait", "--wait-timeout", wait)
			if got.code != tt.code {
				t.Fatalf("exit %d, want %d: %s%s", got.code, tt.code, got.stdout, got.stderr)
			}
			for _, want := range tt.says {
				if !strings.Contains(got.stdout+got.stderr, want) {
					t.Errorf("%q is not in: %s%s", want, got.stdout, got.stderr)
				}
			}
			if tt.not != "" && strings.Contains(got.stdout+got.stderr, tt.not) {
				t.Errorf("%q is in: %s", tt.not, got.stdout)
			}
			for _, id := range s.read {
				if id != "fc-1" {
					t.Errorf("read %v", s.read)
				}
			}
		})
	}
}

// run get says what a force-canceled run's force cancel lacks.
func TestRunGet_ForceCancel(t *testing.T) {
	for _, tt := range []struct {
		status engine.ForceCancelStatus
		err    []error
		want   string
	}{
		{engine.ForceCancelStatus{State: engine.FCStateForceCanceled, Incomplete: true}, nil, "Force cancel: incomplete"},
		{engine.ForceCancelStatus{State: engine.FCStateForceCanceled}, nil, ""},
		{engine.ForceCancelStatus{State: engine.FCStateForceCanceled, Cascade: &engine.ForceCancelCascade{State: engine.FCCascadeRunning}}, nil, "its cascade is in progress"},
		{engine.ForceCancelStatus{State: engine.FCStateForceCanceled, Cascade: &engine.ForceCancelCascade{State: engine.FCCascadePartial}}, nil, "its cascade left runs below open"},
		{engine.ForceCancelStatus{State: engine.FCStateForceCanceled, Cascade: &engine.ForceCancelCascade{State: engine.FCCascadeStopped}}, nil, "its cascade was stopped"},
		{engine.ForceCancelStatus{}, []error{errors.New("down")}, "Force cancel: could not be read"},
		{engine.ForceCancelStatus{}, []error{engine.ErrForceCancelNotFound}, ""},
		{engine.ForceCancelStatus{State: engine.FCStateForceCanceled, Unaudited: true}, nil, "could not be written to the audit trail"},
	} {
		s := &stubForceCancels{statuses: []engine.ForceCancelStatus{tt.status}, failing: tt.err}
		f := forceCancelCLI(t, s)
		f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_CANCELED, OrkflowName: "hello-ork", Error: &orkstackv1.Error{Code: "RUN_FORCE_CANCELED"}})
		got := f.run("run", "get", "ork-r").requireExit(t, 0)
		if tt.want == "" && strings.Contains(got.stdout, "Force cancel:") || !strings.Contains(got.stdout, tt.want) {
			t.Errorf("%+v: %s", tt.status, got.stdout)
		}
	}
	s := &stubForceCancels{}
	f := forceCancelCLI(t, s)
	f.run("run", "get", "ork-r").requireExit(t, 0)
	if len(s.read) != 0 {
		t.Error("read for a run that was not force-canceled")
	}
}
