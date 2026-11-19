//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// pauseProbeYAML is a task that runs until its marker time passes, and one
// that needs it: a pause taken while the first runs holds the second.
func pauseProbeYAML(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: pause probe.
tasks:
  - id: slow
    uses: shell
    timeout_seconds: 300
    with:
      op: script
      interpreter: sh
      script: |
        sleep 6
  - id: after
    uses: shell
    needs: [slow]
    with:
      op: script
      interpreter: sh
      script: |
        echo done
`, name)
}

// cliRefused runs orkctl against one replica, requires exit 1 and returns
// its stderr.
func (s *originStack) cliRefused(t *testing.T, replica *engineReplica, args ...string) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(principalEnvVar, s.principal)
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	full := append([]string{"-n", s.namespace}, args...)
	var stdout, stderr bytes.Buffer
	code := Execute(append(full, "--server", "http://"+replica.addr), &stdout, &stderr)
	fmt.Fprintf(&s.transcript, "$ orkctl %s    # via %s\n%s%s(exit %d)\n\n",
		strings.Join(quoteArgs(full), " "), replica.name, stdout.String(), stderr.String(), code)
	if code != exitError {
		t.Fatalf("orkctl %v via %s exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			full, replica.name, code, exitError, stdout.String(), stderr.String())
	}
	return stderr.String()
}

// taskStatus is one task's status as a replica reads the run.
func (s *originStack) taskStatus(ctx context.Context, t *testing.T, replica *engineReplica, runID, taskID string) orkstackv1.Status {
	t.Helper()
	got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: s.namespace}))
	if err != nil {
		t.Fatalf("get run %s via %s: %v", runID, replica.name, err)
	}
	for _, task := range got.Msg.GetRun().GetTaskRuns() {
		if task.GetTaskId() == taskID {
			return task.GetStatus()
		}
	}
	return orkstackv1.Status_STATUS_UNSPECIFIED
}

// TestIntegration_PauseThroughTheCLI pauses a run on real `ork` replicas
// with `orkctl run pause --wait`, and requires: the running task finishes
// while the one that needs it is held (observed while the first one's end
// shows the run making progress); run get and the PAUSED list on the other
// replica show the pause, and status=RUNNING leaves it out; a second pause,
// an unpause of a run paused by nobody else, and an Orkflow delete are
// answered as design §5.1 says; and an unpause lets the held task run, the
// run succeed, and run get show the last pause (design §8, integration 11
// and 27).
func TestIntegration_PauseThroughTheCLI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newOriginStack(ctx, t, "pause")
	const orkflow = "pause-probe"
	if _, err := s.a.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: pauseProbeYAML(orkflow), Namespace: s.namespace,
	})); err != nil {
		t.Fatalf("ApplyOrkflow on replica A: %v", err)
	}
	runID := decodeSubmittedID(t, s.cli(t, s.a, exitOK, "--json", "run", "submit", orkflow))
	s.awaitTaskRunning(ctx, t, s.a, runID)

	s.principal = "dana@acme"
	paused := s.cli(t, s.b, exitOK, "run", "pause", runID, "--reason", "db maintenance", "--timeout", "1h", "--wait", "--wait-timeout", "2m")
	if !strings.Contains(paused, "run "+runID+" paused until ") {
		t.Fatalf("run pause --wait printed %q", paused)
	}
	again := s.cliRefused(t, s.a, "run", "pause", runID)
	if !strings.Contains(again, "RUN_ALREADY_PAUSED") || !strings.Contains(again, "by dana@acme (asserted)") {
		t.Fatalf("a second pause said %q, want RUN_ALREADY_PAUSED naming dana@acme as asserted", again)
	}
	s.principal = ""

	// The running task finishes; the one that needs it is held. Its end is
	// the progress the held window is observed against.
	deadline := time.Now().Add(2 * time.Minute)
	for s.taskStatus(ctx, t, s.a, runID, "slow") != orkstackv1.Status_STATUS_SUCCEEDED {
		if time.Now().After(deadline) {
			t.Fatalf("task slow of the paused run never finished")
		}
		time.Sleep(250 * time.Millisecond)
	}
	held := time.Now().Add(3 * time.Second)
	for time.Now().Before(held) {
		if got := s.taskStatus(ctx, t, s.b, runID, "after"); got != orkstackv1.Status_STATUS_PENDING && got != orkstackv1.Status_STATUS_UNSPECIFIED {
			t.Fatalf("task after of the paused run is %s, want held PENDING", got)
		}
		time.Sleep(250 * time.Millisecond)
	}

	human := collapseSpaces(s.cli(t, s.a, exitOK, "run", "get", runID))
	for _, want := range []string{"Status: PAUSED", "Paused: since ", "by dana@acme (asserted): db maintenance; ends as failed at "} {
		if !strings.Contains(human, want) {
			t.Fatalf("run get lacks %q:\n%s", want, human)
		}
	}
	s.awaitListed(t, s.b, func(ids []string) bool { return containsID(ids, runID) }, "--filter", "status=PAUSED")
	if running := s.cli(t, s.b, exitOK, "run", "list", "--filter", "status=RUNNING"); strings.Contains(running, runID) {
		t.Fatalf("status=RUNNING lists the paused run:\n%s", running)
	}
	if refused := s.cliRefused(t, s.a, "orkflow", "delete", orkflow, "--yes"); !strings.Contains(refused, "1 active") {
		t.Fatalf("deleting the paused run's Orkflow said %q, want it refused for 1 active run", refused)
	}

	unpaused := s.cli(t, s.a, exitOK, "run", "unpause", runID, "--wait", "--wait-timeout", "2m")
	if !strings.Contains(unpaused, "run "+runID+" unpaused") {
		t.Fatalf("run unpause --wait printed %q", unpaused)
	}
	if run := s.awaitTerminal(ctx, t, s.b, runID); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the unpaused run ended %s, want SUCCEEDED", run.GetStatus())
	}
	last := collapseSpaces(s.cli(t, s.b, exitOK, "run", "get", runID))
	if !strings.Contains(last, "Last pause: ") || !strings.Contains(last, "; unpaused at ") {
		t.Fatalf("run get of the finished run lacks its last pause:\n%s", last)
	}
	if ended := s.cliRefused(t, s.a, "run", "pause", runID); !strings.Contains(ended, "has ended (SUCCEEDED)") {
		t.Fatalf("pausing the finished run said %q", ended)
	}

	proof.Write(t, "orkctl", "run-pause-two-replica.txt", []byte(s.transcript.String()))
	t.Logf("EVIDENCE pause_through_cli run=%s", runID)
}
