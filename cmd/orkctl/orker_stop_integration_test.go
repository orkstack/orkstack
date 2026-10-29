//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// An Orker stopped with SIGTERM (a pod deletion, `systemctl stop`, `docker
// stop`) cancels its running tasks. These scenarios hold it to what a
// cancellation owes each of them before the process exits (docs/DEPLOY.md
// "Removing an Orker safely"): the container is sent SIGTERM and, after the
// task's own grace, SIGKILL; the attempt's record names ORKER_STOPPED; and the
// lines the workload printed after SIGTERM reach the task's log. Temporal is
// told nothing, exactly as for an Orker that died: the task ends
// ORKER_HEARTBEAT_TIMEOUT, or is retried on another Orker under its policy.

// stubbornGraceSeconds is how long the task that ignores SIGTERM keeps its
// container before the SIGKILL. The Orker cannot exit sooner.
const stubbornGraceSeconds = 3

// stoppableOrkflow is two tasks that trap SIGTERM: one exits on it, one
// ignores it and must be killed. An attempt after the first exits 0 at once,
// so a retry on another Orker finishes the run.
func stoppableOrkflow(name, retry string) string {
	task := func(id, onTerm string, grace int) string {
		return fmt.Sprintf(`  - id: %[1]s
    uses: shell
    network: none
    timeout_seconds: 300
    cancellation_grace_period_seconds: %[3]d
%[4]s    with:
      op: script
      interpreter: sh
      script: |
        if ! grep -q '"attempt": 1,' /orkstack/input/task.json; then
          echo "%[1]s: retried, done"
          exit 0
        fi
        trap 'echo "%[1]s: SIGTERM, %[2]s"%[5]s' TERM
        echo "%[1]s: started"
        while :; do sleep 1; done
`, id, onTerm, grace, retry, map[bool]string{true: "; exit 0", false: ""}[onTerm == "cleaning up"])
	}
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: two tasks an Orker stops with SIGTERM.
tasks:
%s%s`, name, task("graceful", "cleaning up", 20), task("stubborn", "ignoring it", stubbornGraceSeconds))
}

// stoppedAttempt is one `run attempts --json` row with its error.
type stoppedAttempt struct {
	TaskID  string `json:"taskId"`
	Attempt int    `json:"attempt"`
	Outcome string `json:"outcome"`
	OrkerID string `json:"orkerId"`
	Error   struct {
		Code      string `json:"code"`
		Category  string `json:"category"`
		Retryable bool   `json:"retryable"`
		Message   string `json:"message"`
	} `json:"error"`
}

func (s *timingStack) stoppedAttempts(runID string) []stoppedAttempt {
	s.t.Helper()
	var doc struct {
		Attempts []stoppedAttempt `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(s.mustCLI(0, "--json", "run", "attempts", runID)), &doc); err != nil {
		s.t.Fatalf("run attempts --json: %v", err)
	}
	return doc.Attempts
}

// runningContainers lists this run's containers that are still running,
// by its own label (AGENTS.md §11.1: never an unfiltered docker ps).
func runningContainers(t *testing.T, runID string) []string {
	t.Helper()
	out, err := exec.Command("docker", "ps", "--filter", "label=ork.run="+runID,
		"--filter", "status=running", "--format", "{{.Names}}").Output()
	if err != nil {
		t.Fatalf("docker ps for run %s: %v", runID, err)
	}
	return strings.Fields(string(out))
}

// removeRunContainers removes whatever an Orker left of one run, so a failing
// assertion leaks no container.
func removeRunContainers(t *testing.T, runID string) {
	t.Helper()
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=ork.run="+runID).Output()
	if err != nil {
		t.Logf("docker ps for run %s: %v", runID, err)
		return
	}
	for _, id := range strings.Fields(string(out)) {
		removeContainer(t, id)
	}
}

// TestIntegration_SIGTERMedOrkerRecordsTheAttemptsItStops.
func TestIntegration_SIGTERMedOrkerRecordsTheAttemptsItStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	for _, variant := range []struct {
		name  string
		file  string
		retry string
	}{
		{"no retry policy", "orker-sigterm-no-retry.txt", "    retry:\n      maximum_attempts: 1\n"},
		{"retry policy", "orker-sigterm-retry.txt", "    retry:\n      maximum_attempts: 2\n      initial_interval_seconds: 1\n"},
	} {
		t.Run(variant.name, func(t *testing.T) {
			s := newTimingStack(ctx, t, 1)
			defer s.writeTranscript(variant.file)
			firstID := uniqueName("orker-sigterm")
			orker := startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
				"ORK_ORKER_ID="+firstID, "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
			awaitActivityPoller(ctx, t, s.temporal, s.queue)
			name := uniqueName("orker-sigterm")
			s.apply(stoppableOrkflow(name, variant.retry))
			runID := s.submit(name)
			t.Cleanup(func() { removeRunContainers(t, runID) })

			// Both workloads are live, and have trapped SIGTERM, once each has
			// printed its first line.
			deadline := time.Now().Add(2 * time.Minute)
			for {
				out, _, _ := s.cli(0, "run", "logs", runID)
				if strings.Contains(out, "graceful: started") && strings.Contains(out, "stubborn: started") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the two tasks never both started within 2m; logs:\n%s", out)
				}
				time.Sleep(250 * time.Millisecond)
			}

			exited := make(chan error, 1)
			go func() { exited <- orker.Wait() }()
			if err := orker.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatalf("SIGTERM the orker: %v", err)
			}
			stoppedAt := time.Now()
			select {
			case err := <-exited:
				if err != nil {
					t.Fatalf("the orker exited with %v after SIGTERM, want status 0", err)
				}
			case <-time.After(90 * time.Second):
				_ = orker.Process.Kill()
				t.Fatalf("the orker did not exit within 90s of SIGTERM")
			}
			took := time.Since(stoppedAt)
			t.Logf("(%s) the orker exited %s after SIGTERM", variant.name, took.Round(time.Millisecond))

			// What the Orker owed its tasks before exiting is checked whole, so
			// one failure shows every debt left unpaid.
			//
			// The SIGKILL after stubborn's grace is sent by the Orker, so an
			// Orker that exits before the grace has passed leaves stubborn
			// running for ever: no Orker with this id boots again to reap it.
			if running := runningContainers(t, runID); len(running) != 0 {
				t.Errorf("containers %v of run %s still run after their orker exited", running, runID)
				removeRunContainers(t, runID)
			}
			if took < stubbornGraceSeconds*time.Second {
				t.Errorf("the orker exited %s after SIGTERM, before stubborn's %ds grace could pass",
					took, stubbornGraceSeconds)
			}

			// Each attempt it stopped has a record, written before it exited.
			byTask := map[string]stoppedAttempt{}
			for _, row := range s.stoppedAttempts(runID) {
				if row.Attempt == 1 {
					byTask[row.TaskID] = row
				}
			}
			for _, task := range []string{"graceful", "stubborn"} {
				row := byTask[task]
				if row.Outcome != "FAILED" || row.Error.Code != "ORKER_STOPPED" ||
					row.Error.Category != "INFRASTRUCTURE_ERROR" || !row.Error.Retryable || row.OrkerID != firstID {
					t.Errorf("%s attempt 1 = %+v, want FAILED ORKER_STOPPED (INFRASTRUCTURE_ERROR, retryable) on %s",
						task, row, firstID)
					continue
				}
				t.Logf("(%s) %s attempt 1: %s %s %q", variant.name, task, row.Outcome, row.Error.Code, row.Error.Message)
			}

			// And the lines each workload printed after SIGTERM reached its log.
			logs := s.mustCLI(0, "run", "logs", runID)
			for _, line := range []string{"graceful: SIGTERM, cleaning up", "stubborn: SIGTERM, ignoring it"} {
				if !strings.Contains(logs, line) {
					t.Errorf("run logs lack %q, printed after SIGTERM:\n%s", line, logs)
				}
			}
			s.mustCLI(0, "run", "attempts", runID)

			// Temporal was told nothing: the outcome is the one a dead Orker
			// gets.
			if variant.retry == "    retry:\n      maximum_attempts: 1\n" {
				awaitTerminalOnB(ctx, t, s.replicas[0], runID)
				got := s.mustCLI(0, "--json", "run", "get", runID)
				var doc struct {
					Run struct {
						Status   string `json:"status"`
						TaskRuns []struct {
							TaskID string `json:"taskId"`
							Error  struct {
								Code string `json:"code"`
							} `json:"error"`
						} `json:"taskRuns"`
					} `json:"run"`
				}
				if err := json.Unmarshal([]byte(got), &doc); err != nil {
					t.Fatalf("run get --json: %v\n%s", err, got)
				}
				if doc.Run.Status != "FAILED" || len(doc.Run.TaskRuns) != 2 {
					t.Fatalf("run = %s, want FAILED with two tasks:\n%s", doc.Run.Status, got)
				}
				for _, taskRun := range doc.Run.TaskRuns {
					if taskRun.Error.Code != "ORKER_HEARTBEAT_TIMEOUT" {
						t.Fatalf("task %s ended %q, want ORKER_HEARTBEAT_TIMEOUT:\n%s",
							taskRun.TaskID, taskRun.Error.Code, got)
					}
				}
				t.Logf("(%s) the run FAILED with both tasks ORKER_HEARTBEAT_TIMEOUT", variant.name)
				return
			}

			secondID := uniqueName("orker-sigterm-2")
			startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
				"ORK_ORKER_ID="+secondID, "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
			final := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
			if final.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("run ended %s, want SUCCEEDED once attempt 2 ran on %s", final.GetStatus(), secondID)
			}
			for _, row := range s.stoppedAttempts(runID) {
				if row.Attempt == 2 && (row.Outcome != "SUCCEEDED" || row.OrkerID != secondID) {
					t.Fatalf("%s attempt 2 = %+v, want SUCCEEDED on %s", row.TaskID, row, secondID)
				}
			}
			s.mustCLI(0, "run", "attempts", runID)
			t.Logf("(%s) attempt 2 of both tasks SUCCEEDED on %s", variant.name, secondID)
		})
	}
}
