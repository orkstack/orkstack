package main

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The Paused and Last pause lines of run get: who and when, why, until
// when, what still runs, and how the last pause ended.
func TestPauseLines(t *testing.T) {
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	own := &orkstackv1.RunPause{
		PausedAt: timestamppb.New(at), ExpiresAt: timestamppb.New(at.Add(7 * 24 * time.Hour)),
		PausedBy: "alice", Reason: "db maintenance",
	}
	running := []*orkstackv1.TaskRun{
		{TaskId: "a", Status: orkstackv1.Status_STATUS_RUNNING},
		{TaskId: "b", Status: orkstackv1.Status_STATUS_RUNNING},
		{TaskId: "c", Status: orkstackv1.Status_STATUS_PENDING},
	}
	for _, tc := range []struct {
		name string
		run  *orkstackv1.Run
		want []string
	}{
		{"own pause", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_PAUSED, Pause: own, TaskRuns: running}, []string{
			"Paused:\tsince 2026-11-12T10:00:00Z by alice (asserted): db maintenance; ends as failed at 2026-11-19T10:00:00Z unless unpaused; 2 tasks still running",
		}},
		{"own pause of a failing run", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_PAUSED, Pause: own,
			Error: &orkstackv1.Error{Code: "X"}}, []string{
			"Paused:\tsince 2026-11-12T10:00:00Z by alice (asserted): db maintenance; ends at 2026-11-19T10:00:00Z unless unpaused; failure handlers wait until it is unpaused",
		}},
		{"paused by its parent", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_PAUSED,
			PausedByParent: &orkstackv1.InheritedPause{ParentRunId: "ork-p", Since: timestamppb.New(at)}}, []string{
			"Paused:\tbecause parent run ork-p is paused (since 2026-11-12T10:00:00Z)",
		}},
		{"a last pause unpaused", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED, LastPause: &orkstackv1.RunPause{
			PausedAt: timestamppb.New(at), PausedBy: "alice", End: orkstackv1.PauseEnd_PAUSE_END_UNPAUSED,
			EndedAt: timestamppb.New(at.Add(time.Hour)), EndedBy: "bob",
		}}, []string{
			"Last pause:\t2026-11-12T10:00:00Z by alice (asserted); unpaused at 2026-11-12T11:00:00Z by bob (asserted)",
		}},
		{"a last pause timed out", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED, LastPause: &orkstackv1.RunPause{
			PausedAt: timestamppb.New(at), PausedBy: "alice", Reason: "x", End: orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT,
			EndedAt: timestamppb.New(at.Add(time.Hour)),
		}}, []string{
			"Last pause:\t2026-11-12T10:00:00Z by alice (asserted): x; timed out at 2026-11-12T11:00:00Z",
		}},
		{"a last pause the run's end closed", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_CANCELED, LastPause: &orkstackv1.RunPause{
			PausedAt: timestamppb.New(at), PausedBy: "alice", End: orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED,
			EndedAt: timestamppb.New(at.Add(time.Hour)),
		}}, []string{
			"Last pause:\t2026-11-12T10:00:00Z by alice (asserted); ended with the run at 2026-11-12T11:00:00Z",
		}},
		{"a verified pause", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_PAUSED, Pause: &orkstackv1.RunPause{
			PausedAt: timestamppb.New(at), ExpiresAt: timestamppb.New(at.Add(time.Hour)), PausedBy: "user:corp:ada", PausedByVerified: true,
		}}, []string{
			"Paused:\tsince 2026-11-12T10:00:00Z by user:corp:ada (verified); ends as failed at 2026-11-12T11:00:00Z unless unpaused",
		}},
		{"never paused", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pauseLines(tc.run)
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("lines\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// A pause that did not reach a child run says so: under run get's Paused
// line, and on stderr once a waited pause applied (Run.pause_warnings).
func TestPauseWarnings(t *testing.T) {
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	run := &orkstackv1.Run{
		Status: orkstackv1.Status_STATUS_PAUSED,
		Pause:  &orkstackv1.RunPause{PausedAt: timestamppb.New(at), ExpiresAt: timestamppb.New(at.Add(time.Hour)), PausedBy: "alice"},
		PauseWarnings: []*orkstackv1.PauseWarning{
			{TaskId: "build", ChildRunId: "ork-c1", Code: "CHILD_NOT_PAUSABLE",
				Message: "child run ork-c1 was started without support for pausing; it keeps running"},
			{TaskId: "ship", ChildRunId: "ork-c2", Code: "CHILD_PAUSE_FAILED",
				Message: "could not pause child run ork-c2: unavailable\x1b[2J; it keeps running"},
		},
	}
	want := []string{
		"Paused:\tsince 2026-11-12T10:00:00Z by alice (asserted); ends as failed at 2026-11-12T11:00:00Z unless unpaused",
		"Not paused:\ttask build: child run ork-c1 was started without support for pausing; it keeps running",
		"Not paused:\ttask ship: could not pause child run ork-c2: unavailable [2J; it keeps running",
	}
	if got := pauseLines(run); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("pauseLines =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	var out strings.Builder
	printPauseWarnings(&out, run)
	wantOut := "warning: task build: child run ork-c1 was started without support for pausing; it keeps running\n" +
		"warning: task ship: could not pause child run ork-c2: unavailable [2J; it keeps running\n"
	if out.String() != wantOut {
		t.Errorf("printPauseWarnings =\n%q\nwant\n%q", out.String(), wantOut)
	}
	out.Reset()
	printPauseWarnings(&out, &orkstackv1.Run{})
	if out.String() != "" {
		t.Errorf("printPauseWarnings of a run with none = %q, want nothing", out.String())
	}
}
