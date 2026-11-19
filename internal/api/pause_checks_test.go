package api

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

var pauseNow = time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

func pauseRun(mutate func(*orkstackv1.Run)) *orkstackv1.Run {
	run := &orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true}
	if mutate != nil {
		mutate(run)
	}
	return run
}

func handler(id string, status orkstackv1.Status) *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{TaskId: id, Status: status, Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED}
}

// Every refusal of design §5.1's checks 3–9, in order, and the requests
// that pass them.
func TestPauseRequestRefusal(t *testing.T) {
	ownPause := &orkstackv1.RunPause{
		PausedAt: timestamppb.New(pauseNow.Add(-time.Hour)), ExpiresAt: timestamppb.New(pauseNow.Add(time.Hour)),
		PausedBy: "alice",
	}
	for _, tc := range []struct {
		name  string
		pause bool
		run   *orkstackv1.Run
		code  string // "" passes
		says  string
	}{
		{"pause a running run", true, pauseRun(nil), "", ""},
		{"unpause an own pause", false, pauseRun(func(r *orkstackv1.Run) {
			r.Status, r.Pause = orkstackv1.Status_STATUS_PAUSED, ownPause
		}), "", ""},
		{"unpause a run that is not paused is a no-op the run answers", false, pauseRun(nil), "", ""},
		{"an ended run", true, pauseRun(func(r *orkstackv1.Run) { r.Status = orkstackv1.Status_STATUS_SUCCEEDED }),
			"", "has ended (SUCCEEDED)"},
		{"a run without the plan", true, pauseRun(func(r *orkstackv1.Run) { r.Pausable = false }),
			engine.RunPauseUnsupportedCode, "started without support for pausing"},
		{"a run not started", true, pauseRun(func(r *orkstackv1.Run) { r.Status = orkstackv1.Status_STATUS_PENDING }),
			engine.RunPauseNotRunningCode, "has not started yet"},
		{"a queued run", true, pauseRun(func(r *orkstackv1.Run) { r.Queued = &orkstackv1.ConcurrencyQueued{} }),
			engine.RunPauseNotRunningCode, "waiting for its concurrency slot"},
		{"a paused run", true, pauseRun(func(r *orkstackv1.Run) {
			r.Status, r.Pause = orkstackv1.Status_STATUS_PAUSED, ownPause
		}), engine.RunAlreadyPausedCode, "by alice (asserted), until 2026-11-17T11:00:00Z"},
		{"a run paused by its parent", true, pauseRun(func(r *orkstackv1.Run) {
			r.Status = orkstackv1.Status_STATUS_PAUSED
			r.PausedByParent = &orkstackv1.InheritedPause{ParentRunId: "ork-p"}
		}), engine.RunAlreadyPausedCode, `because its parent run "ork-p" is paused`},
		{"unpause a run paused by its parent", false, pauseRun(func(r *orkstackv1.Run) {
			r.Status = orkstackv1.Status_STATUS_PAUSED
			r.PausedByParent = &orkstackv1.InheritedPause{ParentRunId: "ork-p"}
		}), engine.RunPauseInheritedCode, `unpause "ork-p"`},
		{"pause a failing run with a handler to start", true, pauseRun(func(r *orkstackv1.Run) {
			r.Error = &orkstackv1.Error{Code: "X"}
			r.TaskRuns = []*orkstackv1.TaskRun{handler("h", orkstackv1.Status_STATUS_PENDING)}
		}), "", ""},
		{"pause a failing run whose handlers all started", true, pauseRun(func(r *orkstackv1.Run) {
			r.Error = &orkstackv1.Error{Code: "X"}
			r.TaskRuns = []*orkstackv1.TaskRun{handler("h", orkstackv1.Status_STATUS_RUNNING),
				{TaskId: "plain", Status: orkstackv1.Status_STATUS_PENDING}}
		}), engine.RunEndingCode, "all its failure handlers have started"},
		{"unpause an expired pause", false, pauseRun(func(r *orkstackv1.Run) {
			r.Status = orkstackv1.Status_STATUS_PAUSED
			r.Pause = &orkstackv1.RunPause{PausedBy: "alice", ExpiresAt: timestamppb.New(pauseNow)}
		}), engine.RunEndingCode, "pause expired at 2026-11-17T10:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := pauseRequestRefusal(tc.run, tc.pause, pauseNow)
			if tc.code == "" && tc.says == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var cerr *connect.Error
			if !errors.As(err, &cerr) || cerr.Code() != connect.CodeFailedPrecondition {
				t.Fatalf("err = %v, want FailedPrecondition", err)
			}
			if tc.code != "" && !strings.HasPrefix(cerr.Message(), tc.code+": ") {
				t.Fatalf("message %q does not begin with %s", cerr.Message(), tc.code)
			}
			if !strings.Contains(cerr.Message(), tc.says) {
				t.Fatalf("message %q, want it to say %q", cerr.Message(), tc.says)
			}
		})
	}
}

// The warnings say what the pause will not hold back: a failing run kept
// open, deadlines that end before the pause, child runs paused with it, and
// a schedule that cancels it.
func TestPauseWarnings(t *testing.T) {
	expires := pauseNow.Add(7 * 24 * time.Hour)
	run := pauseRun(func(r *orkstackv1.Run) {
		r.Error = &orkstackv1.Error{Code: "X"}
		r.Timeout = &orkstackv1.RunTimeout{
			Deadline: timestamppb.New(pauseNow.Add(time.Hour)), HardEnd: timestamppb.New(pauseNow.Add(2 * time.Hour)),
		}
		r.TaskRuns = []*orkstackv1.TaskRun{
			{TaskId: "approve", Status: orkstackv1.Status_STATUS_AWAITING,
				Await: &orkstackv1.TaskAwait{Deadline: timestamppb.New(pauseNow.Add(3 * time.Hour))}},
			{TaskId: "late", Status: orkstackv1.Status_STATUS_AWAITING,
				Await: &orkstackv1.TaskAwait{Deadline: timestamppb.New(expires.Add(time.Hour))}},
			{TaskId: "child", Status: orkstackv1.Status_STATUS_RUNNING, Child: &orkstackv1.TaskChild{RunId: "ork-c"}},
			{TaskId: "done-child", Status: orkstackv1.Status_STATUS_SUCCEEDED, Child: &orkstackv1.TaskChild{RunId: "ork-d"}},
		}
	})
	got := strings.Join(pauseWarnings(run, 7*24*time.Hour, expires, true), "\n")
	for _, want := range []string{
		"the run is failing; pausing holds its failure handlers and keeps it open, counted toward max_running, for up to 168h0m0s",
		"this run's timeout ends at 2026-11-17T11:00:00Z, before this pause does; it will then fail, and its failure handlers will wait for the unpause until 2026-11-17T12:00:00Z",
		"awaited task approve's deadline is 2026-11-17T13:00:00Z, before this pause ends; it will then time out",
		"1 running child run is paused too",
		"the schedule that started this run cancels it at its next fire while it is paused (overlap policy CANCEL_OTHER)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("warnings\n%s\nlack %q", got, want)
		}
	}
	if strings.Contains(got, "late") {
		t.Errorf("warns of a deadline after the pause ends:\n%s", got)
	}
	if w := pauseWarnings(pauseRun(nil), time.Hour, pauseNow.Add(time.Hour), false); len(w) != 0 {
		t.Errorf("a plain running run warns %q", w)
	}
}

// The pause's length: the request's, or the deployment's default, never
// more than its maximum, and at least one second.
func TestPauseTimeoutOf(t *testing.T) {
	plan := engine.PausePlan{DefaultTimeoutSeconds: 3600, MaxTimeoutSeconds: 7200}
	for _, tc := range []struct {
		asked time.Duration
		want  time.Duration
		ok    bool
	}{
		{0, time.Hour, true},
		{time.Second, time.Second, true},
		{2 * time.Hour, 2 * time.Hour, true},
		{2*time.Hour + time.Second, 0, false},
		{500 * time.Millisecond, 0, false},
		{-time.Second, 0, false},
		{1500 * time.Millisecond, 0, false},
	} {
		got, err := pauseTimeoutOf(tc.asked, plan)
		if (err == nil) != tc.ok || (tc.ok && got != tc.want) {
			t.Errorf("pauseTimeoutOf(%s) = %s, %v; want %s ok=%t", tc.asked, got, err, tc.want, tc.ok)
		}
	}
}

// FuzzPauseRunRequest holds the request bounds to what they claim: an
// accepted timeout is whole seconds within [1 s, max] (or the default for
// none), and an accepted reason is valid UTF-8 within its bound.
func FuzzPauseRunRequest(f *testing.F) {
	for _, seed := range []struct {
		nanos  int64
		reason string
	}{{0, ""}, {int64(time.Second), "db"}, {int64(500 * time.Millisecond), "x"}, {int64(2 * time.Hour), "\xff"},
		{int64(time.Hour) + 1, strings.Repeat("r", 1025)}, {-1, ""}} {
		f.Add(seed.nanos, seed.reason)
	}
	plan := engine.PausePlan{DefaultTimeoutSeconds: 3600, MaxTimeoutSeconds: 7200}
	f.Fuzz(func(t *testing.T, nanos int64, reason string) {
		got, err := pauseTimeoutOf(time.Duration(nanos), plan)
		if err == nil {
			if nanos == 0 && got != time.Hour {
				t.Fatalf("no timeout gave %s, want the default", got)
			}
			if got < time.Second || got > 2*time.Hour || got%time.Second != 0 {
				t.Fatalf("accepted %s", got)
			}
		}
		if checkPauseRequestText("ork-r", reason) == nil && (len(reason) > maxRunPauseReasonBytes || !utf8.ValidString(reason)) {
			t.Fatalf("accepted reason of %d bytes, valid %t", len(reason), utf8.ValidString(reason))
		}
	})
}

// An unset timestamp reads as unknown, never as the epoch.
func TestInstant_UnsetIsUnknown(t *testing.T) {
	var unset *timestamppb.Timestamp
	if got := instant(unset); got != "an unknown time" {
		t.Fatalf("instant(nil) = %q", got)
	}
	run := pauseRun(func(r *orkstackv1.Run) {
		r.Timeout = &orkstackv1.RunTimeout{Deadline: timestamppb.New(pauseNow.Add(time.Hour))}
	})
	got := strings.Join(pauseWarnings(run, 2*time.Hour, pauseNow.Add(2*time.Hour), false), "\n")
	if strings.Contains(got, "1970") || !strings.Contains(got, "until an unknown time") {
		t.Fatalf("warning with no hard end = %q", got)
	}
}
