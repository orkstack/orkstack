package orker

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// statusRuntime is retryingRuntime reporting one exit status.
type statusRuntime struct {
	retryingRuntime
	status ExitStatus
}

func (r *statusRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return r.status, nil
}

// TestDispatcher_TellsAContainerStoppedFromOutsideFromOneThatExited is the
// classification: a container the runtime reports stopped from outside did not
// end on its own, whatever its code, so it is an infrastructure failure that
// retries and its output is never committed. Every other status stays the
// workload's.
func TestDispatcher_TellsAContainerStoppedFromOutsideFromOneThatExited(t *testing.T) {
	for _, tt := range []struct {
		name       string
		status     ExitStatus
		wantReason string
		wantInMsg  string
		notInMsg   string
	}{
		{
			name:       "stopped, exit 143",
			status:     ExitStatus{Code: 143, StoppedFromOutside: true},
			wantReason: ReasonContainerStoppedByRuntime,
			wantInMsg:  "exited 143",
		},
		{
			name:       "stopped past its grace, exit 137",
			status:     ExitStatus{Code: 137, StoppedFromOutside: true},
			wantReason: ReasonContainerStoppedByRuntime,
		},
		{
			// Valid output and a clean exit, written while it was being stopped:
			// committing it would make what a daemon interrupted a durable result.
			name:       "stopped, its handler exits 0",
			status:     ExitStatus{Code: 0, StoppedFromOutside: true},
			wantReason: ReasonContainerStoppedByRuntime,
		},
		{
			name:       "stopped and killed by the kernel too",
			status:     ExitStatus{Code: 137, OOMKilled: true, StoppedFromOutside: true},
			wantReason: ReasonContainerStoppedByRuntime,
		},
		{
			name:       "exits 143 by itself",
			status:     ExitStatus{Code: 143},
			wantReason: ReasonContainerExitNonzero,
			wantInMsg:  "exited 143",
			notInMsg:   "is unknown",
		},
		{
			name:       "exits non-zero and the runtime's events said nothing",
			status:     ExitStatus{Code: 143, StopCauseUnknown: true},
			wantReason: ReasonContainerExitNonzero,
			wantInMsg:  "events ended before they said how the container ended, so whether the workload exited on its own or the runtime stopped it is unknown",
			notInMsg:   "does not record",
		},
		{
			name:       "exits non-zero on a runtime that keeps no record of a stop",
			status:     ExitStatus{Code: 143, StopCauseUnknown: true, StopCauseUnrecorded: true},
			wantReason: ReasonContainerExitNonzero,
			wantInMsg:  "does not record whether it stopped a container, so whether the workload exited on its own or the runtime stopped it is unknown",
			notInMsg:   "events ended",
		},
		{
			name:       "killed by the kernel",
			status:     ExitStatus{Code: 137, OOMKilled: true},
			wantReason: ReasonContainerExitNonzero,
			wantInMsg:  "memory limit",
		},
		{name: "exits 0", status: ExitStatus{Code: 0}},
		{name: "exits 0 and the runtime's events said nothing", status: ExitStatus{Code: 0, StopCauseUnknown: true}},
		{name: "exits 0 on a runtime that keeps no record of a stop", status: ExitStatus{Code: 0, StopCauseUnknown: true, StopCauseUnrecorded: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			commits := artifacts.NewMemoryCommitStore()
			dispatcher := newAttemptDispatcher(t, &statusRuntime{status: tt.status}, artifacts.NewMemoryAttemptStore())
			dispatcher.Commits = commits
			in := baseExportInput("stopped")
			key, err := artifacts.CommitKey("default", in.GetRunId(), in.GetTaskId())
			must.OK(t, err)
			in.CommitUri = "mem://commits/" + key

			_, err = dispatcher.Execute(context.Background(), in)

			if tt.wantReason == "" {
				if err != nil {
					t.Fatalf("Execute = %v, want success", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Execute succeeded, want %s", tt.wantReason)
			}
			message := err.Error()
			if !strings.Contains(message, tt.wantReason) {
				t.Errorf("Execute = %q, want %s", message, tt.wantReason)
			}
			if tt.wantInMsg != "" && !strings.Contains(message, tt.wantInMsg) {
				t.Errorf("Execute = %q, want it to say %q", message, tt.wantInMsg)
			}
			if tt.notInMsg != "" && strings.Contains(message, tt.notInMsg) {
				t.Errorf("Execute = %q, must not say %q", message, tt.notInMsg)
			}
			if tt.wantReason == ReasonContainerStoppedByRuntime {
				if commit, err := commits.Get(context.Background(), in.GetCommitUri()); err == nil {
					t.Errorf("a stopped container's output was committed: %+v", commit)
				}
			}
		})
	}
}

// TestReasonContainerStoppedByRuntime_IsAnInfrastructureFailureThatRetries: §4
// keeps it apart from ActivityFailed, and it is what makes a task's retry policy
// cover a daemon restart.
func TestReasonContainerStoppedByRuntime_IsAnInfrastructureFailureThatRetries(t *testing.T) {
	if got := CategoryFor(ReasonContainerStoppedByRuntime); got != CategoryInfrastructureError {
		t.Errorf("category = %s, want %s", got, CategoryInfrastructureError)
	}
	if !RetryableReason(ReasonContainerStoppedByRuntime) {
		t.Error("it is not retried")
	}
}

// startAndWait runs one container on the fake CLI, as the dispatcher does.
func startAndWait(t *testing.T, fake *countingDocker, cli string, settle time.Duration) (ExitStatus, error) {
	t.Helper()
	fake.set("startquick", "")
	runtime := &DockerRuntime{Bin: fake.bin, CLI: cli, stopSettle: settle}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	must.OKf(t, runtime.Start(ctx, "cid-0123", &stdout, &stderr), "Start")
	return runtime.Wait(ctx, "cid-0123")
}

// TestDockerRuntime_Wait_ReadsWhatTheEngineSaidHappened is the stream, word by
// word, as Docker 29.9.3 and Podman 5.8.9 were measured to say it. "kill" and
// "stop" come from someone asking the engine, and a process that exits by itself
// or signals itself makes neither.
func TestDockerRuntime_Wait_ReadsWhatTheEngineSaidHappened(t *testing.T) {
	for _, tt := range []struct {
		name string
		// events is what the engine's stream says. An empty one says nothing and
		// stays open, as an engine does that has nothing to tell.
		events string
		// ends makes the stream end after that, as a daemon going away does.
		ends  bool
		fails bool
		// hangs is a stream that never answers, which only the settle ends. Every
		// other case keeps the default, so a slow host cannot read as a stream
		// that said nothing.
		hangs bool
		// late is a stream whose CLI connects only after the attach has ended.
		late             bool
		wantStopped      bool
		wantCauseUnknown bool
	}{
		{name: "Docker: a daemon's clean shutdown", events: "create \nstart \nkill 15\nstop \ndie \n", wantStopped: true},
		{name: "Docker: past the stop's grace", events: "start \nkill 15\nkill 9\nstop \ndie \n", wantStopped: true},
		{name: "Docker: an image's own stop signal", events: "start \nkill 3\nstop \ndie \n", wantStopped: true},
		{name: "Docker: an operator's stop", events: "start \nkill 15\nstop \ndie \ndestroy \n", wantStopped: true},
		{name: "Docker: an operator's kill", events: "start \nkill 9\ndie \n", wantStopped: true},
		{name: "Docker: the daemon goes as soon as it said so", events: "start \nkill 15\n", ends: true, wantStopped: true},
		{name: "Docker: exits by itself", events: "create \nattach \nstart \ndie \n"},
		{name: "Docker: an exec's end is not the container's", events: "start \nexec_die \nexec_die \ndie \n"},
		{name: "Docker: a signal that stops nothing", events: "start \nkill 10\nkill 1\ndie \n"},
		{name: "Podman: an operator's stop", events: "create\ninit\nstart\nstop\ndied\ncleanup\n", wantStopped: true},
		{name: "Podman: an operator's kill, whichever signal", events: "create\ninit\nstart\nkill\ndied\n", wantStopped: true},
		{name: "Podman: exits by itself", events: "create\ninit\nstart\ndied\ncleanup\n"},
		{name: "a stop that came after it ended is not why it ended", events: "start \ndie \nkill 9\nstop \n"},
		{name: "a stream that connects after the container ended still hears why", events: "start \nkill 15\nstop \ndie \n", late: true, wantStopped: true},
		{name: "the stream ends before it says", events: "start \n", ends: true, wantCauseUnknown: true},
		{name: "the stream never says", events: "start \n", hangs: true, wantCauseUnknown: true},
		{name: "the stream says nothing at all", events: "", hangs: true, wantCauseUnknown: true},
		{name: "the CLI refuses to stream", fails: true, wantCauseUnknown: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("events", tt.events)
			if tt.ends {
				fake.set("eventsend", "")
			}
			if tt.fails {
				fake.set("eventsfail", "")
			}
			if tt.late {
				fake.set("eventsconnect", "0.3")
			}
			var settle time.Duration
			if tt.hangs {
				settle = 200 * time.Millisecond
			}
			status, err := startAndWait(t, fake, CLIDocker, settle)
			must.OKf(t, err, "Wait")
			if status.StopCauseUnrecorded {
				t.Errorf("a stream that said too little reads as a runtime that keeps no record")
			}
			if status.StoppedFromOutside != tt.wantStopped || status.StopCauseUnknown != tt.wantCauseUnknown {
				t.Errorf("stopped = %t, cause unknown = %t; want %t, %t",
					status.StoppedFromOutside, status.StopCauseUnknown, tt.wantStopped, tt.wantCauseUnknown)
			}
		})
	}
}

// TestDockerRuntime_Wait_OnThePodmanCLIAsksTheEngineWhetherItWasStopped: libpod
// writes a stop's `stop` event only if the container is still stopping once its
// process is gone, which it almost never is (conmon has reaped it and written
// its exit file by then, Podman 5.7.0 and 5.8.9's libpod/container_internal.go),
// so a stop says create, init, start, died, cleanup: word for word what a
// container that exits by itself says. What libpod records instead is
// State.StoppedByUser, set by every stop and kill through the engine before the
// container ends, which the podman CLI's inspect answers. The docker CLI's
// inspect, against Docker or Podman's socket, has no such field and is never
// asked it.
func TestDockerRuntime_Wait_OnThePodmanCLIAsksTheEngineWhetherItWasStopped(t *testing.T) {
	const podmanStop = "create\ninit\nstart\ndied\ncleanup\n"
	for _, tt := range []struct {
		name             string
		cli              string
		events           string
		flag             string
		wantStopped      bool
		wantCauseUnknown bool
		wantAsked        int
	}{
		{name: "podman: an operator's stop", cli: CLIPodman, events: podmanStop, flag: "stoppedbyuser", wantStopped: true, wantAsked: 1},
		{name: "podman: exits by itself", cli: CLIPodman, events: podmanStop, wantAsked: 1},
		// The record is the engine's to give, and it gave none.
		{name: "podman: the engine will not say", cli: CLIPodman, events: podmanStop, flag: "stoppedbyuserfail", wantCauseUnknown: true, wantAsked: 1},
		{name: "podman: a kill the stream heard needs no asking", cli: CLIPodman, events: "create\ninit\nstart\nkill\ndied\n", wantStopped: true},
		{name: "docker: never asked", cli: CLIDocker, events: "create \nstart \ndie \n", flag: "stoppedbyuser"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("events", tt.events)
			if tt.flag != "" {
				fake.set(tt.flag, "")
			}
			status, err := startAndWait(t, fake, tt.cli, 0)
			must.OKf(t, err, "Wait")
			if status.StoppedFromOutside != tt.wantStopped || status.StopCauseUnknown != tt.wantCauseUnknown {
				t.Errorf("stopped = %t, cause unknown = %t; want %t, %t",
					status.StoppedFromOutside, status.StopCauseUnknown, tt.wantStopped, tt.wantCauseUnknown)
			}
			if status.StopCauseUnrecorded != tt.wantCauseUnknown {
				t.Errorf("unrecorded = %t, want %t", status.StopCauseUnrecorded, tt.wantCauseUnknown)
			}
			if got := countPrefix(fake.calls(), "inspect --format {{.State.StoppedByUser}}"); got != tt.wantAsked {
				t.Errorf("the engine was asked whether it was stopped %d times, want %d", got, tt.wantAsked)
			}
		})
	}
}

// TestDockerRuntime_Wait_TheDockerCLIOnPodmanKeepsNoRecordOfAStop: behind Podman's
// Docker-compatible socket the stream may or may not say `stop` (libpod writes
// it only if the container is still stopping once its process is gone), and the
// docker CLI's inspect reads Docker's state, which has no StoppedByUser. So
// there an end with nothing heard says nothing about why it ended, and it is
// reported as untold, never as the workload's; a stop it did say is a stop. A
// kill comes before the end and is heard. Against Docker an end with no stop heard stays the workload's, and
// an engine that cannot be identified leaves it untold.
func TestDockerRuntime_Wait_TheDockerCLIOnPodmanKeepsNoRecordOfAStop(t *testing.T) {
	const podmanEnd = "create \ninit \nstart \ndie \ncleanup \n"
	for _, tt := range []struct {
		name             string
		events           string
		flags            []string
		wantStopped      bool
		wantCauseUnknown bool
	}{
		{name: "Podman's socket: an end with no kill heard", events: podmanEnd, flags: []string{"podmanengine"}, wantCauseUnknown: true},
		{name: "Podman's socket: a stop it did say", events: "start \nstop \ndie \n", flags: []string{"podmanengine"}, wantStopped: true},
		{name: "Podman's socket: a kill, whichever signal", events: "start \nkill \ndie \n", flags: []string{"podmanengine"}, wantStopped: true},
		{name: "Docker: an end with no stop heard", events: "create \nstart \ndie \n"},
		{name: "an engine that cannot be identified", events: "create \nstart \ndie \n", flags: []string{"versionfail"}, wantCauseUnknown: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("events", tt.events)
			for _, flag := range tt.flags {
				fake.set(flag, "")
			}
			status, err := startAndWait(t, fake, CLIDocker, 0)
			must.OKf(t, err, "Wait")
			if status.StoppedFromOutside != tt.wantStopped || status.StopCauseUnknown != tt.wantCauseUnknown {
				t.Errorf("stopped = %t, cause unknown = %t; want %t, %t",
					status.StoppedFromOutside, status.StopCauseUnknown, tt.wantStopped, tt.wantCauseUnknown)
			}
			if status.StopCauseUnrecorded != tt.wantCauseUnknown {
				t.Errorf("unrecorded = %t, want %t", status.StopCauseUnrecorded, tt.wantCauseUnknown)
			}
			if got := countPrefix(fake.calls(), "inspect --format {{.State.StoppedByUser}}"); got != 0 {
				t.Errorf("the docker CLI's inspect was asked for StoppedByUser %d times, which it has not", got)
			}
		})
	}
}

// TestDockerRuntime_Wait_HearsAnEngineWhoseClockDiffers: a --since worked out on
// the Orker's clock asks a remote engine for its future if that engine's clock is
// behind, and it answers with nothing (Docker 29.9.2 measured). A container stopped
// from outside whose handler exits 0 would then be committed as a success.
func TestDockerRuntime_Wait_HearsAnEngineWhoseClockDiffers(t *testing.T) {
	for _, cli := range []string{CLIDocker, CLIPodman} {
		for _, skew := range []string{"-3600", "-120", "-61", "0", "120", "3600"} {
			t.Run(cli+"/"+skew, func(t *testing.T) {
				fake := newCountingDocker(t)
				fake.set("daemonskew", skew)
				fake.set("events", "start \nkill 15\nstop \ndie \n")
				status, err := startAndWait(t, fake, cli, 300*time.Millisecond)
				must.OKf(t, err, "Wait")
				if !status.StoppedFromOutside || status.StopCauseUnknown {
					t.Errorf("stopped = %t, cause unknown = %t with the engine's clock %ss from the Orker's; want a stop heard",
						status.StoppedFromOutside, status.StopCauseUnknown, skew)
				}
			})
		}
	}
}

// TestDockerRuntime_Wait_AStreamWithNoAnchorObservesNothing: a created time the
// engine will not give, or gives in a form nothing can use, opens no stream. It is
// what a stream that said nothing is, its cause unknown, and never a verdict.
func TestDockerRuntime_Wait_AStreamWithNoAnchorObservesNothing(t *testing.T) {
	for name, set := range map[string]func(*countingDocker){
		"refused":  func(fake *countingDocker) { fake.set("createdfail", "") },
		"not JSON": func(fake *countingDocker) { fake.set("createdat", "soon\n") },
		"no time":  func(fake *countingDocker) { fake.set("createdat", `"0001-01-01T00:00:00Z"`+"\n") },
		"a flag":   func(fake *countingDocker) { fake.set("createdat", `"--help"`+"\n") },
	} {
		t.Run(name, func(t *testing.T) {
			fake := newCountingDocker(t)
			set(fake)
			fake.set("events", "start \nkill 15\nstop \ndie \n")
			status, err := startAndWait(t, fake, CLIDocker, 200*time.Millisecond)
			must.OKf(t, err, "Wait")
			if status.StoppedFromOutside || !status.StopCauseUnknown {
				t.Errorf("stopped = %t, cause unknown = %t, want nothing observed", status.StoppedFromOutside, status.StopCauseUnknown)
			}
			if got := countPrefix(fake.calls(), "events "); got != 0 {
				t.Errorf("a stream with no anchor was opened %d times", got)
			}
		})
	}
}

// TestDockerRuntime_Wait_AsksEachCLIInItsOwnWords: the two CLIs name an event's
// word differently, and neither understands the other's template. Both look back
// to the instant the engine says it created the container, which is what covers one
// that ran before the stream's CLI connected: the two processes race, whichever is
// forked first.
func TestDockerRuntime_Wait_AsksEachCLIInItsOwnWords(t *testing.T) {
	for _, tt := range []struct{ cli, want string }{
		{CLIDocker, `events --since 2026-11-07T06:11:16.727408016Z --filter container=cid-0123 --format {{.Action}} {{index .Actor.Attributes "signal"}}`},
		{CLIPodman, "events --since 2026-11-07T06:11:16.727408016Z --filter container=cid-0123 --format {{.Status}}"},
	} {
		t.Run(tt.cli, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("createdat", `"2026-11-07T06:11:16.727408016Z"`+"\n")
			if _, err := startAndWait(t, fake, tt.cli, 0); err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if got := countPrefix(fake.calls(), tt.want); got != 1 {
				t.Errorf("the stream was asked for %d times as %q", got, tt.want)
			}
		})
	}
}

// awaitGone waits until the process the fake CLI recorded under flag is gone.
func awaitGone(t *testing.T, fake *countingDocker, flag string) {
	t.Helper()
	pid := awaitRecorded(t, fake, flag)
	deadline := time.Now().Add(20 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("the stream's process %d outlived what it watched", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitRecorded waits, by deadline, until the fake CLI's process has recorded
// its pid under flag, and returns it.
func awaitRecorded(t *testing.T, fake *countingDocker, flag string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var pid int
	for {
		raw, err := os.ReadFile(filepath.Join(fake.dir, flag))
		if err == nil {
			if pid, err = strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stream never recorded its pid: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return pid
}

// TestDockerRuntime_TheStreamEndsWithTheContainer: a CLI following one
// container's events never ends on its own, so each way an attempt stops looking
// has to end it, or an Orker leaks one process per attempt.
func TestDockerRuntime_TheStreamEndsWithTheContainer(t *testing.T) {
	t.Run("Wait", func(t *testing.T) {
		fake := newCountingDocker(t)
		if _, err := startAndWait(t, fake, CLIDocker, 0); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		awaitGone(t, fake, "eventspid")
	})
	t.Run("Remove, when nothing waited", func(t *testing.T) {
		fake := newCountingDocker(t)
		fake.set("startquick", "")
		runtime := &DockerRuntime{Bin: fake.bin, CLI: CLIDocker}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var stdout, stderr bytes.Buffer
		must.OKf(t, runtime.Start(ctx, "cid-0123", &stdout, &stderr), "Start")
		// The stream must be running before Remove, or a Remove that killed it
		// before its first line ran would leave nothing to observe: what is
		// proven is that Remove ends a stream that follows the container.
		awaitRecorded(t, fake, "eventspid")
		must.OKf(t, runtime.Remove(ctx, "cid-0123"), "Remove")
		awaitGone(t, fake, "eventspid")
	})
}

// TestDispatcher_AStoppedAttemptsRecordCarriesTheReason guards the seam between
// the two: the attempt's record carries the reason, so `orkctl run attempts` says
// it.
func TestDispatcher_AStoppedAttemptsRecordCarriesTheReason(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &statusRuntime{status: ExitStatus{Code: 143, StoppedFromOutside: true}}, attempts)
	in := baseExportInput("stopped-record")
	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute succeeded")
	}
	records, err := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	must.OK(t, err)
	if len(records) != 1 {
		t.Fatalf("records = %+v", records)
	}
	record := records[0]
	if got := record.GetError().GetCode(); got != ReasonContainerStoppedByRuntime {
		t.Errorf("error code = %s", got)
	}
	if got := record.GetError().GetCategory(); got != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Errorf("category = %s", got)
	}
}
