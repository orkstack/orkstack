package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestRunTimeoutLine: `run get`'s Timeout line says the declared limit and
// where the run's clock stands, in the words docs/CLI.md and the UI use.
func TestRunTimeoutLine(t *testing.T) {
	now := time.Date(2026, 11, 12, 14, 48, 0, 0, time.UTC)
	at := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(d)) }
	const (
		running   = orkstackv1.Status_STATUS_RUNNING
		paused    = orkstackv1.Status_STATUS_PAUSED
		failed    = orkstackv1.Status_STATUS_FAILED
		succeeded = orkstackv1.Status_STATUS_SUCCEEDED
		pending   = orkstackv1.Status_STATUS_PENDING
		canceled  = orkstackv1.Status_STATUS_CANCELED
	)
	tests := []struct {
		name    string
		status  orkstackv1.Status
		timeout *orkstackv1.RunTimeout
		want    string
	}{
		{"no timeout declared", running, nil, ""},
		{"queued, clock not started", pending, &orkstackv1.RunTimeout{Seconds: 21600},
			"6h (starts when the run starts)"},
		{"ended before its clock started", canceled, &orkstackv1.RunTimeout{Seconds: 21600},
			"6h (the run never started)"},
		{"running with time left", running, &orkstackv1.RunTimeout{Seconds: 21600, Deadline: at(72 * time.Minute)},
			"6h (deadline 2026-11-12T16:00:00Z, 1h12m left)"},
		{"under a minute left", running, &orkstackv1.RunTimeout{Seconds: 60, Deadline: at(42*time.Second + 300*time.Millisecond)},
			"1m (deadline 2026-11-12T14:48:42Z, 43s left)"},
		{"paused with time left", paused, &orkstackv1.RunTimeout{Seconds: 5400, Deadline: at(30 * time.Minute)},
			"1h30m (deadline 2026-11-12T15:18:00Z, 30m left)"},
		{"under a second left", running, &orkstackv1.RunTimeout{Seconds: 60, Deadline: at(300 * time.Millisecond)},
			"1m (deadline 2026-11-12T14:48:00Z, 1s left)"},
		{"exactly at the deadline", running, &orkstackv1.RunTimeout{Seconds: 60, Deadline: at(0)},
			"1m (deadline 2026-11-12T14:48:00Z, passed)"},
		{"stopped at the hard end, hard end unrecorded", failed, &orkstackv1.RunTimeout{Seconds: 60, Deadline: at(-time.Hour), TimedOut: true, HardEndReached: true},
			"1m (timed out at 2026-11-12T13:48:00Z)"},
		{"a value no duration holds", pending, &orkstackv1.RunTimeout{Seconds: 1 << 62},
			"4611686018427387904s (starts when the run starts)"},
		{"timed out, deadline unrecorded", failed, &orkstackv1.RunTimeout{Seconds: 60, TimedOut: true},
			"1m (timed out, deadline unknown)"},
		{"no seconds, yet stopped at the hard end", failed, &orkstackv1.RunTimeout{HardEndReached: true},
			"0s (the run never started)"},
		{"live past the deadline, ending not yet recorded", running, &orkstackv1.RunTimeout{Seconds: 21600, Deadline: at(-time.Second)},
			"6h (deadline 2026-11-12T14:47:59Z, passed)"},
		{"finished before its deadline", succeeded, &orkstackv1.RunTimeout{Seconds: 21600, Deadline: at(time.Hour)},
			"6h (deadline 2026-11-12T15:48:00Z)"},
		{"timed out, handlers still running", running, &orkstackv1.RunTimeout{Seconds: 21600, Deadline: at(-time.Minute), HardEnd: at(9 * time.Minute), TimedOut: true},
			"6h (timed out at 2026-11-12T14:47:00Z; failure handlers stop at 2026-11-12T14:57:00Z)"},
		{"timed out, handlers finished in time", failed, &orkstackv1.RunTimeout{Seconds: 21600, Deadline: at(-time.Hour), HardEnd: at(-50 * time.Minute), TimedOut: true},
			"6h (timed out at 2026-11-12T13:48:00Z)"},
		{"timed out, handlers stopped at the hard end", failed, &orkstackv1.RunTimeout{Seconds: 21600, Deadline: at(-time.Hour), HardEnd: at(-50 * time.Minute), TimedOut: true, HardEndReached: true},
			"6h (timed out at 2026-11-12T13:48:00Z; failure handlers stopped at 2026-11-12T13:58:00Z)"},
		{"failing for another reason past the deadline", running, &orkstackv1.RunTimeout{Seconds: 21600, Deadline: at(-time.Minute), HardEnd: at(9 * time.Minute)},
			"6h (deadline 2026-11-12T14:47:00Z, passed; failure handlers stop at 2026-11-12T14:57:00Z)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &orkstackv1.Run{Status: tt.status, Timeout: tt.timeout}
			if got := runTimeoutLine(run, now); got != tt.want {
				t.Fatalf("runTimeoutLine = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRunTimeoutLine_FinishedRunSaysWhetherTheDeadlinePassed: a finished
// run that ended at or past its deadline without the timeout as its cause
// says the deadline passed; one that ended before it does not.
func TestRunTimeoutLine_FinishedRunSaysWhetherTheDeadlinePassed(t *testing.T) {
	now := time.Date(2026, 11, 12, 14, 48, 0, 0, time.UTC)
	deadline := timestamppb.New(now.Add(-time.Hour))
	run := &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED,
		Timeout:    &orkstackv1.RunTimeout{Seconds: 21600, Deadline: deadline},
		FinishedAt: timestamppb.New(now.Add(-time.Minute))}
	if got := runTimeoutLine(run, now); got != "6h (deadline 2026-11-12T13:48:00Z, passed)" {
		t.Fatalf("ended past its deadline: %q", got)
	}
	run.FinishedAt = timestamppb.New(now.Add(-2 * time.Hour))
	if got := runTimeoutLine(run, now); got != "6h (deadline 2026-11-12T13:48:00Z)" {
		t.Fatalf("ended before its deadline: %q", got)
	}
}

// TestRenderRun_PrintsTheTimeoutLine: the line sits in the header, and a run
// whose Orkflow declares no timeout prints none.
func TestRenderRun_PrintsTheTimeoutLine(t *testing.T) {
	restore := nowFunc
	nowFunc = func() time.Time { return fixedNow }
	t.Cleanup(func() { nowFunc = restore })

	run := &orkstackv1.Run{Id: "ork-1", OrkflowName: "deploy", OrkflowVersion: "1",
		Status:  orkstackv1.Status_STATUS_PENDING,
		Timeout: &orkstackv1.RunTimeout{Seconds: 3600}}
	var out bytes.Buffer
	must.OKf(t, renderRun(&out, run), "renderRun")
	if got := collapseSpaces(detailLine(t, out.String(), "Timeout:")); got != "Timeout: 1h (starts when the run starts)" {
		t.Fatalf("Timeout line = %q", got)
	}

	out.Reset()
	run.Timeout = nil
	must.OKf(t, renderRun(&out, run), "renderRun")
	if strings.Contains(out.String(), "Timeout:") {
		t.Fatalf("a run with no timeout prints a Timeout line:\n%s", out.String())
	}
}

// TestRenderOrkflow_ShowsItsTimeout: `orkflow get` prints the declared run
// timeout as it is written, and nothing for an Orkflow with none.
func TestRenderOrkflow_ShowsItsTimeout(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", TimeoutSeconds: 5400,
		Tasks: []*orkstackv1.Task{{Id: "ship", Uses: "shell"}}}
	var out bytes.Buffer
	must.OKf(t, renderOrkflow(&out, wf, ""), "renderOrkflow")
	if got := collapseSpaces(detailLine(t, out.String(), "Timeout:")); got != "Timeout: 1h30m" {
		t.Fatalf("Timeout line = %q", got)
	}
	out.Reset()
	wf.TimeoutSeconds = 0
	must.OKf(t, renderOrkflow(&out, wf, ""), "renderOrkflow")
	if strings.Contains(out.String(), "Timeout:") {
		t.Fatalf("an Orkflow with no timeout prints a Timeout line:\n%s", out.String())
	}
}

// TestRenderPlan_NamesTheRunTimeout: `run plan` names the planned
// definition's run timeout under its input, as written, and a definition
// with none prints no line, so every existing plan reads as before.
func TestRenderPlan_NamesTheRunTimeout(t *testing.T) {
	plan := &orkstackv1.PlanRunResponse{OrkflowName: "deploy", OrkflowVersion: "1", Namespace: "default",
		DefinitionDigest: "sha256:abc", TimeoutSeconds: 21600}
	var out bytes.Buffer
	must.OK(t, renderPlan(&out, plan, "", nil))
	if got := detailLine(t, out.String(), "timeout"); got != "timeout     6h" {
		t.Fatalf("timeout line = %q", got)
	}
	out.Reset()
	plan.TimeoutSeconds = 0
	must.OK(t, renderPlan(&out, plan, "", nil))
	if strings.Contains(out.String(), "timeout") {
		t.Fatalf("a plan of a definition with no timeout names one:\n%s", out.String())
	}
}
