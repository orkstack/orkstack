//go:build integration

package orker_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
)

// These tests hold the line E47 draws on a real engine: a number says nothing
// about why a container ended, and what the engine was asked says everything.
// The daemon-restart half is in
// TestIntegration_ContainerRemoval_ADaemonRestartFailsTheAttemptAndItsContainerIsRemoved,
// which needs a daemon of its own; these need nothing the host's cannot share,
// and run on every engine.

// unrecordedWords is what a failure says behind Podman's socket when nothing
// was heard before the end.
const unrecordedWords = "container runtime does not record whether it stopped a container, so " +
	"whether the workload exited on its own or the runtime stopped it is unknown"

// onPodmanSocket reports the docker CLI against Podman's Docker-compatible
// socket. There a stop may or may not be heard before the end, and an end with
// nothing heard has no record of why (docs/OCI_ACTIVITY_CONTRACT.md §8.2).
func onPodmanSocket(t *testing.T) bool {
	t.Helper()
	info := runtimeInfo(t)
	return info.Name == orker.CLIPodman && info.CLI == orker.CLIDocker
}

// TestIntegration_ContainerExit_AWorkloadThatExitsByItselfIsItsOwnFailure: each
// of these reports what a SIGTERM's death reports, or exactly the signal itself,
// and nobody asked the engine for anything. Behind Podman's socket the failure
// says why it ended is unknown, because there nothing heard is no record.
func TestIntegration_ContainerExit_AWorkloadThatExitsByItselfIsItsOwnFailure(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)
	socket := onPodmanSocket(t)
	for _, tt := range []struct{ name, command string }{
		{name: "exits-143", command: `exit 143`},
		{name: "exits-137", command: `exit 137`},
		{name: "signals-itself-and-dies", command: `kill -TERM $$; sleep 30`},
		{name: "signals-itself-and-its-handler-exits-143", command: `trap 'exit 143' TERM; kill -TERM $$; sleep 30`},
		{name: "kills-itself", command: `kill -KILL $$; sleep 30`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			in := removalInput(t, "self-"+tt.name, "/bin/sh", "-c", tt.command)
			_, err := dispatcher.Execute(ctx, in)
			if err == nil {
				t.Fatal("the attempt succeeded")
			}
			if !strings.Contains(err.Error(), orker.ReasonContainerExitNonzero) {
				t.Errorf("Execute = %v, want %s", err, orker.ReasonContainerExitNonzero)
			}
			if says := strings.Contains(err.Error(), unrecordedWords); says != socket {
				t.Errorf("the failure says the runtime keeps no record = %t, want %t: %v", says, socket, err)
			}
			if !socket && strings.Contains(err.Error(), "is unknown") {
				t.Errorf("the engine's events said how it ended, and the failure says it is unknown: %v", err)
			}
		})
	}
}

// TestIntegration_ContainerExit_AContainerStoppedThroughItsEngineIsNotTheWorkloads:
// what a daemon's shutdown does to a container, asked for by an operator here on
// an engine that stays up: the same words on the stream, and a workload that
// did nothing wrong, whatever it exits with. Behind Podman's socket a stop may
// or may not be heard (Podman 5.8.9 nearly always says it, CI's 5.7.0 may not),
// so there either is right: the stop, or an end with no record that says so
// (or a clean exit that commits). A workload failure whose cause is known is
// never right. A kill is heard there too.
func TestIntegration_ContainerExit_AContainerStoppedThroughItsEngineIsNotTheWorkloads(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)
	cli := containerCLI(t)
	socket := onPodmanSocket(t)
	const ticking = `while :; do echo tick; sleep 1; done`
	for _, tt := range []struct {
		name, command string
		ask           []string
		// heard: the engine says this before the end behind every CLI and engine.
		heard bool
		// exitsZero: what the workload exits with once stopped.
		exitsZero bool
	}{
		{name: "stop", command: ticking, ask: []string{"stop", "--time", "1"}},
		// The handler leaves a valid result, so what decides the outcome is the
		// stop alone: heard, that result is never committed; unheard, the clean
		// exit stands as the workload's and commits it (§8.2). A handler that
		// left none would fail VALUES_MISSING there, which is the workload's
		// own contract, not what this test asks.
		{name: "stop-handler-exits-0", command: `trap 'echo {} > /orkstack/output/values.json; exit 0' TERM; ` + ticking, ask: []string{"stop", "--time", "5"}, exitsZero: true},
		{name: "kill", command: ticking, ask: []string{"kill"}, heard: true},
		{name: "kill-term", command: ticking, ask: []string{"kill", "--signal", "TERM"}, heard: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			in := removalInput(t, "outside-"+tt.name, "/bin/sh", "-c", tt.command)
			done := make(chan error, 1)
			go func() {
				_, err := dispatcher.Execute(ctx, in)
				done <- err
			}()
			container := awaitLabelled(ctx, t, "ork.run="+in.GetRunId())
			awaitTicks(ctx, t, container, 1)
			args := append(append([]string{}, tt.ask...), container)
			if out, err := exec.CommandContext(ctx, cli, args...).CombinedOutput(); err != nil {
				t.Fatalf("%s %v: %v: %s", cli, args, err, out)
			}
			select {
			case err := <-done:
				stopped := err != nil && strings.Contains(err.Error(), orker.ReasonContainerStoppedByRuntime)
				switch {
				case !socket || tt.heard:
					if !stopped {
						t.Errorf("Execute = %v, want %s", err, orker.ReasonContainerStoppedByRuntime)
					}
				case stopped:
					t.Logf("observed: the stop was heard behind Podman's socket")
				case tt.exitsZero:
					// Nothing heard and nothing recorded: a clean exit stands
					// as the workload's (§8.2).
					if err != nil {
						t.Errorf("Execute = %v, want %s, or a clean exit with no record to stand as a success",
							err, orker.ReasonContainerStoppedByRuntime)
					}
				default:
					if err == nil || !strings.Contains(err.Error(), orker.ReasonContainerExitNonzero) ||
						!strings.Contains(err.Error(), unrecordedWords) {
						t.Errorf("Execute = %v, want %s, or %s saying the runtime keeps no record",
							err, orker.ReasonContainerStoppedByRuntime, orker.ReasonContainerExitNonzero)
					}
				}
			case <-ctx.Done():
				t.Fatalf("the attempt never ended: %v", ctx.Err())
			}
		})
	}
}

// TestIntegration_ContainerExit_ASignalThatStopsNothingIsNotAStop: an operator's
// USR1 to a workload that handles it, which then fails or succeeds on its own.
// Docker's events name the signal. Podman's do not, so there any kill reads as a
// stop (docs/DEPLOY.md, "What removes a task's container").
func TestIntegration_ContainerExit_ASignalThatStopsNothingIsNotAStop(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)
	cli := containerCLI(t)
	wantReason := orker.ReasonContainerExitNonzero
	if onPodman(t) {
		wantReason = orker.ReasonContainerStoppedByRuntime
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	in := removalInput(t, "outside-usr1", "/bin/sh", "-c", `trap 'exit 3' USR1; while :; do echo tick; sleep 1; done`)
	done := make(chan error, 1)
	go func() {
		_, err := dispatcher.Execute(ctx, in)
		done <- err
	}()
	container := awaitLabelled(ctx, t, "ork.run="+in.GetRunId())
	awaitTicks(ctx, t, container, 1)
	if out, err := exec.CommandContext(ctx, cli, "kill", "--signal", "USR1", container).CombinedOutput(); err != nil {
		t.Fatalf("signal the container: %v: %s", err, out)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), wantReason) {
			t.Errorf("Execute = %v, want %s", err, wantReason)
		}
	case <-ctx.Done():
		t.Fatalf("the attempt never ended: %v", ctx.Err())
	}
}

// TestIntegration_ContainerExit_TheStreamOfAKilledOrkerEndsWithItsContainer: the
// event stream has no reason of its own to end, so a killed Orker's would run for
// as long as the daemon, one per task it was running. Its pipe has no reader once
// the Orker is gone, so the next word about the container ends it. The stream was
// there to begin with, which is what makes its absence mean something.
func TestIntegration_ContainerExit_TheStreamOfAKilledOrkerEndsWithItsContainer(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	in := removalInput(t, "stream-orphan", "/bin/sh", "-c", `while :; do echo tick; sleep 1; done`)
	container, kill := startHeldOrker(ctx, t, "", removalWorkRoot(t, "stream-orphan"), in)
	stream := func() string {
		// A pattern naming this test's own container alone (AGENTS.md §11.1).
		out, err := exec.CommandContext(ctx, "pgrep", "-af", "events .*container="+container).Output()
		// pgrep's status 1 alone says nothing matched. A pgrep that could not run,
		// on this test's expired deadline above all, has seen nothing, which must
		// not read as the stream being gone.
		var exit *exec.ExitError
		if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1) {
			t.Fatalf("look for the event stream: %v", err)
		}
		return strings.TrimSpace(string(out))
	}
	awaitCondition(ctx, t, "the event stream is running", func() bool { return stream() != "" })
	kill()
	awaitTicks(ctx, t, container, 2)
	if got := stream(); got == "" {
		t.Logf("observed: the stream ended with the Orker, before its container did")
	}

	removeByLabel(t, "ork.run="+in.GetRunId())
	awaitCondition(ctx, t, "the event stream ended with its container", func() bool { return stream() == "" })
}

// TestIntegration_ContainerExit_AStreamThatConnectsLateStillHearsTheStop: the
// engine replays what it said since the container's creation, so a stream whose
// CLI connects after the container has been stopped still hears why. Behind
// Podman's socket the stop may or may not be said, so there an end with no
// record of why is right too; a known cause that is not the stop never is.
func TestIntegration_ContainerExit_AStreamThatConnectsLateStillHearsTheStop(t *testing.T) {
	requireDocker(t)
	socket := onPodmanSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cli := containerCLI(t)
	wrapper := filepath.Join(t.TempDir(), "late-stream-cli")
	script := "#!/bin/sh\nif [ \"$1\" = events ]; then sleep 2; fi\nexec " + cli + " \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	label := "ork.run=" + removalRunID("late-stream")
	t.Cleanup(func() { removeByLabel(t, label) })
	out, err := exec.CommandContext(ctx, cli, "create", "--label", label, orker.BindProbeImage,
		"sleep", "300").Output()
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := orker.ContainerID(strings.TrimSpace(string(out)))

	runtime := &orker.DockerRuntime{Bin: wrapper}
	if filepath.Base(cli) == orker.CLIPodman {
		runtime.CLI = orker.CLIPodman
	}
	if err := runtime.Start(ctx, id, io.Discard, io.Discard); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if out, err := exec.CommandContext(ctx, cli, "stop", "--time", "1", string(id)).CombinedOutput(); err != nil {
		t.Fatalf("stop: %v: %s", err, out)
	}
	status, err := runtime.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	heard := status.StoppedFromOutside && !status.StopCauseUnknown && !status.StopCauseUnrecorded
	if socket {
		unrecorded := !status.StoppedFromOutside && status.StopCauseUnknown && status.StopCauseUnrecorded
		if !heard && !unrecorded {
			t.Errorf("stopped = %t, cause unknown = %t, unrecorded = %t, want the stop heard, or an end with "+
				"no record behind Podman's socket",
				status.StoppedFromOutside, status.StopCauseUnknown, status.StopCauseUnrecorded)
		}
		if unrecorded {
			t.Logf("observed: nothing was heard behind Podman's socket, and the end has no record")
		}
		return
	}
	if !heard {
		t.Errorf("stopped = %t, cause unknown = %t, want the stop heard from a stream that connected late",
			status.StoppedFromOutside, status.StopCauseUnknown)
	}
}

// awaitCondition polls a condition on a test's own deadline.
func awaitCondition(ctx context.Context, t *testing.T, what string, holds func() bool) {
	t.Helper()
	for !holds() {
		select {
		case <-ctx.Done():
			t.Fatalf("%s: never held: %v", what, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
