package orker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestDockerRuntime_Wait_AWatcherThatCannotStartSaysWhy: a stream that is
// never opened says so, with the step that failed, beside a cause it leaves
// unknown, so it is never read as a stream that heard nothing.
func TestDockerRuntime_Wait_AWatcherThatCannotStartSaysWhy(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("createdfail", "")
	fake.set("events", "start \ndie \n")
	status, err := startAndWait(t, fake, CLIDocker, 200*time.Millisecond)
	must.OKf(t, err, "Wait")
	if !status.StopCauseUnknown {
		t.Errorf("cause unknown = false, want true")
	}
	if status.StopWatchFailure == nil {
		t.Fatal("the status does not say the stream was never opened")
	}
	if status.StopWatchFailure.Stage != StopWatchAnchor || status.StopWatchFailure.Err == nil {
		t.Errorf("failure = %+v, want stage %q with its error", status.StopWatchFailure, StopWatchAnchor)
	}

	opened := newCountingDocker(t)
	opened.set("events", "start \ndie \n")
	status, err = startAndWait(t, opened, CLIDocker, 200*time.Millisecond)
	must.OKf(t, err, "Wait")
	if status.StopWatchFailure != nil {
		t.Errorf("a stream that opened reads as one that failed: %+v", status.StopWatchFailure)
	}
}

// TestDispatcher_AWatcherThatCannotStartIsWarnedAndCounted: the dispatcher
// warns, naming the attempt and why, counts the failure, and the attempt's
// failure says the events could not be followed rather than that they ended.
func TestDispatcher_AWatcherThatCannotStartIsWarnedAndCounted(t *testing.T) {
	for _, code := range []int32{0, 143} {
		failure := &StopWatchFailure{Stage: StopWatchAnchor, Err: errors.New("inspect exited 1")}
		dispatcher := newAttemptDispatcher(t, &statusRuntime{status: ExitStatus{
			Code: code, StopCauseUnknown: true, StopWatchFailure: failure,
		}}, artifacts.NewMemoryAttemptStore())
		var logs bytes.Buffer
		dispatcher.Logger = slog.New(slog.NewTextHandler(&logs, nil))
		var counted []string
		dispatcher.StopWatchFailures = func(stage string) { counted = append(counted, stage) }
		dispatcher.Commits = artifacts.NewMemoryCommitStore()
		in := baseExportInput("watch-failed")
		key, err := artifacts.CommitKey("default", in.GetRunId(), in.GetTaskId())
		must.OK(t, err)
		in.CommitUri = "mem://commits/" + key

		_, err = dispatcher.Execute(context.Background(), in)

		logged := logs.String()
		if !strings.Contains(logged, "level=WARN") || !strings.Contains(logged, "stage=anchor") ||
			!strings.Contains(logged, in.GetRunId()) || !strings.Contains(logged, "inspect exited 1") {
			t.Errorf("exit %d: the log does not warn of the stream that never opened, naming the attempt and why:\n%s", code, logged)
		}
		if len(counted) != 1 || counted[0] != StopWatchAnchor {
			t.Errorf("exit %d: counted %v, want one %q", code, counted, StopWatchAnchor)
		}
		if code == 0 {
			must.OKf(t, err, "a clean exit with no stream")
			continue
		}
		if err == nil || !strings.Contains(err.Error(), "could not be followed") || strings.Contains(err.Error(), "events ended") {
			t.Errorf("exit %d: failure = %v, want it to say the events could not be followed", code, err)
		}
	}
}

// TestDockerRuntime_Wait_OnPodmanAFailedStreamStillSaysSoWhenTheEngineAnswers:
// on the Podman CLI the engine's own record answers what the stream could not,
// so the cause is known, and the status still says the stream never opened.
func TestDockerRuntime_Wait_OnPodmanAFailedStreamStillSaysSoWhenTheEngineAnswers(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("createdfail", "")
	fake.set("events", "start \ndie \n")
	status, err := startAndWait(t, fake, CLIPodman, 200*time.Millisecond)
	must.OKf(t, err, "Wait")
	if status.StopCauseUnknown {
		t.Errorf("cause unknown = true, want the engine's record to answer it")
	}
	if status.StopWatchFailure == nil || status.StopWatchFailure.Stage != StopWatchAnchor {
		t.Errorf("failure = %+v, want stage %q", status.StopWatchFailure, StopWatchAnchor)
	}
}

// TestDockerRuntime_Wait_AStreamWhoseCLIExitsAtOnceSaysWhy: a stream whose CLI
// started but ended with an error before saying anything (a flag it refused,
// an engine that refused it) never followed the stream: the status says so,
// with what the CLI said, rather than reading as a stream that heard nothing.
func TestDockerRuntime_Wait_AStreamWhoseCLIExitsAtOnceSaysWhy(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("eventsfail", "")
	status, err := startAndWait(t, fake, CLIDocker, 2*time.Second)
	must.OKf(t, err, "Wait")
	if !status.StopCauseUnknown {
		t.Errorf("cause unknown = false, want true")
	}
	if status.StopWatchFailure == nil || status.StopWatchFailure.Stage != StopWatchExit ||
		!strings.Contains(status.StopWatchFailure.Err.Error(), "cannot read events") {
		t.Fatalf("failure = %+v, want stage %q carrying the CLI's own words", status.StopWatchFailure, StopWatchExit)
	}
}
