//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The join-policy CLI scenarios (docs/DSL.md "Joins and failure handlers"): real `ork` and
// `orker` binaries, real Temporal and MinIO, and the BUILT orkctl, on
// examples/branch-and-recover.ork.yaml as shipped (applied under a stamped
// name so parallel suites never share a definition).

// branchAndRecoverDoc is the shipped example under name.
func branchAndRecoverDoc(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("../../examples/branch-and-recover.ork.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Replace(string(raw), "name: branch-and-recover\n", "name: "+name+"\n", 1)
}

// awaitRunTerminal is awaitTerminalOnB with room for the example's retries
// and handlers.
func awaitRunTerminal(ctx context.Context, t *testing.T, s *taskRunStack, runID string) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(6 * time.Minute)
	for time.Now().Before(deadline) {
		got, err := s.replicas[0].runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
		if err == nil {
			switch got.Msg.GetRun().GetStatus() {
			case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED:
				return got.Msg.GetRun()
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %s", runID)
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("run %s never ended", runID)
	return nil
}

func joinStack(t *testing.T) (*taskRunStack, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	s := newTaskRunStack(ctx, t)
	name := fmt.Sprintf("branch-and-recover-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(branchAndRecoverDoc(t, name))
	return s, name
}

func codeOf(s *taskRunStack, args ...string) (string, string, int) {
	return s.cli(0, args...)
}

// Plan the failure path, assert it, and hold a real run to the plan.
func TestIntegration_CLIPlanAssumeFailedAndTheRunAgree(t *testing.T) {
	s, name := joinStack(t)
	// The decision is decide's value; the plan is handed it as an assumed
	// output, or every branch would be planned skipped.
	base := []string{"run", "plan", name, "--input", "route=canary", "--output", `decide={"value":"canary"}`,
		"--assume-failed", "deploy"}
	if _, stderr, code := codeOf(s, append(base, "--assert-runs", "rollback", "--assert-runs", "notify",
		"--assert-skipped", "escalate")...); code != 0 {
		t.Fatalf("handler assertions exit %d:\n%s", code, stderr)
	}
	if _, _, code := codeOf(s, append(base, "--assert-runs", "announce")...); code != exitPlanAssertionFailed {
		t.Fatalf("--assert-runs announce exit %d, want 4", code)
	}
	_, stderr, code := codeOf(s, append(base, "--assert-runs", "deploy")...)
	if code != exitPlanAssertionFailed || !strings.Contains(stderr, "assert-runs deploy: planned ASSUMED_FAILED (assumed to fail (--assume-failed))") {
		t.Fatalf("--assert-runs deploy exit %d:\n%s", code, stderr)
	}
	if _, stderr, code := codeOf(s, append(base, "--assume-failed", "rollback", "--assert-runs", "escalate")...); code != 0 {
		t.Fatalf("an escalation under two assumptions exit %d:\n%s", code, stderr)
	}
	planned, _, _ := codeOf(s, append([]string{"--json"}, base...)...)

	runID := s.submit(name, "--input", "route=canary", "--input", "fail_deploy=true", "--input", "smoke_seconds=1")
	run := awaitRunTerminal(s.ctx, t, s, runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	// The join rule: a failure handler is held to its plan exactly; any other
	// task may also have been canceled once the run started failing.
	want := map[string][]orkstackv1.Status{
		"decide":   {orkstackv1.Status_STATUS_SUCCEEDED},
		"canary":   {orkstackv1.Status_STATUS_SUCCEEDED},
		"full":     {orkstackv1.Status_STATUS_SKIPPED},
		"verify":   {orkstackv1.Status_STATUS_SUCCEEDED},
		"deploy":   {orkstackv1.Status_STATUS_FAILED},
		"smoke":    {orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_CANCELED},
		"announce": {orkstackv1.Status_STATUS_CANCELED},
		"rollback": {orkstackv1.Status_STATUS_SUCCEEDED},
		"notify":   {orkstackv1.Status_STATUS_SUCCEEDED},
		"escalate": {orkstackv1.Status_STATUS_SKIPPED},
	}
	for id, allowed := range want {
		got := taskRunNamed(run, id).GetStatus()
		ok := false
		for _, a := range allowed {
			ok = ok || got == a
		}
		if !ok {
			t.Errorf("%s = %v, plan allows %v", id, got, allowed)
		}
	}
	fmt.Fprintf(&s.transcript, "plan --json:\n%s\nrun %s: %s\n", planned, runID, run.GetStatus())
	s.writeTranscript("join-C1.txt")
}

// The failing run through `run get` and `run watch`.
func TestIntegration_CLIRunGetShowsAFailingRun(t *testing.T) {
	s, name := joinStack(t)
	runID := s.submit(name, "--input", "route=full", "--input", "fail_deploy=true",
		"--input", "smoke_seconds=1", "--input", "rollback_seconds=30")
	var failing string
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		out, _, code := codeOf(s, "run", "get", runID)
		if code == 0 && strings.Contains(out, "RUNNING (failing)") && strings.Contains(out, "rollback  RUNNING") {
			failing = out
			break
		}
		select {
		case <-s.ctx.Done():
			t.Fatal("context ended")
		case <-time.After(500 * time.Millisecond):
		}
	}
	if failing == "" {
		t.Fatal("run get never showed RUNNING (failing) while rollback ran")
	}
	for _, want := range []string{"JOIN", "verify    none_failed", "rollback  any_failed", "notify    all_done"} {
		if !strings.Contains(failing, want) {
			t.Errorf("run get lacks %q:\n%s", want, failing)
		}
	}
	machine := s.mustCLI(0, "--json", "run", "get", runID)
	if !strings.Contains(machine, `"join": "ANY_FAILED"`) {
		t.Errorf("--json run get carries no join:\n%s", machine)
	}
	// run watch follows the run to its end, and a FAILED run exits
	// exitRunFailed (2) — the run is failing, so that is the right code.
	watched, watchErr, watchCode := codeOf(s, "run", "watch", runID)
	if watchCode != exitRunFailed {
		t.Errorf("run watch exit = %d, want %d (the run ends FAILED); stderr:\n%s", watchCode, exitRunFailed, watchErr)
	}
	noted := false
	for _, line := range strings.Split(watched, "\n") {
		if strings.Contains(line, "run failing after task") && strings.Contains(line, " - ") {
			noted = true
		}
	}
	if !noted {
		t.Errorf("run watch printed no failing note with - for its task:\n%s", watched)
	}
	awaitRunTerminal(s.ctx, t, s, runID)
	s.writeTranscript("join-C2.txt")
}

// Replay a failure handler locally.
func TestIntegration_CLITaskRunReplaysAFailureHandler(t *testing.T) {
	s, name := joinStack(t)
	runID := s.submit(name, "--input", "route=canary", "--input", "fail_deploy=true", "--input", "smoke_seconds=1")
	run := awaitRunTerminal(s.ctx, t, s, runID)
	if taskRunNamed(run, "rollback").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("rollback = %v", taskRunNamed(run, "rollback"))
	}
	stdout, stderr, code := s.taskRun("--from-run", runID, "--task", "rollback")
	if code != 0 {
		t.Fatalf("task run rollback exit %d:\n%s\n%s", code, stdout, stderr)
	}
	for _, want := range []string{`"policy":"any_failed"`, `"taskId":"deploy","status":"FAILED"`, "deploy failed: the health check",
		"maximum 2 attempts — not applied here: a local run is one attempt", "deploy/deploy.log"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("task run stdout lacks %q:\n%s", want, stdout)
		}
	}
	overridden, stderr, code := s.taskRun("--from-run", runID, "--task", "rollback", "--render-only", "--need", "deploy=succeeded")
	if code != 0 || !strings.Contains(overridden, `"status": "SUCCEEDED"`) || strings.Contains(overridden, `"status": "FAILED"`) {
		t.Fatalf("--need deploy=succeeded exit %d:\n%s\n%s", code, overridden, stderr)
	}
	_, stderr, code = s.taskRun("--from-run", runID, "--task", "rollback", "--need", "verify=failed")
	if code == 0 || !strings.Contains(stderr, `"verify" is not a direct need of "rollback"`) {
		t.Fatalf("--need verify=failed exit %d:\n%s", code, stderr)
	}
	s.writeTranscript("join-C3.txt")
}
