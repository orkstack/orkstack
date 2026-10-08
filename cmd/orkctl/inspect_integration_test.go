//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"

	"github.com/orkstack/orkstack/internal/task"
)

// The live-attempt-introspection integration scenarios (docs/UI_CLI_PARITY.md
// "Live attempt introspection"): real `ork`
// and `orker` binaries, a real Temporal cluster and the MinIO bucket, and
// `orkctl run inspect` read back through the built CLI. Each test owns a
// private Temporal namespace and a private Orker channel (the timingStack
// topology, AGENTS.md §11.1).

// startOrkerWith is startOrker with extra environment and a returned process
// handle, for the one test that must kill its Orker by handle. It copies
// startProcess's body rather than calling it, so the shared helpers keep
// their signatures and every other integration test is untouched.
//
// Its base environment is the one the timingStack's Orker gets
// (startIdempotencyOrker): the replicas it pairs with store artifacts,
// attempt records and logs in the MinIO bucket, and an Orker on another
// backend would file its records where no engine reads them.
func startOrkerWith(ctx context.Context, t *testing.T,
	orkerPath, temporalAddr, temporalNamespace, queue string,
	extra ...string) *exec.Cmd {
	t.Helper()
	env := append([]string{
		"ORK_TEMPORAL_ADDRESS=" + temporalAddr,
		"ORK_TEMPORAL_NAMESPACE=" + temporalNamespace,
		"ORK_ORKER_QUEUE=" + queue,
		"ORK_ORKER_WORKDIR=" + bindableOrkerWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=s3",
	}, extra...)
	if filepath.Base(orkerPath) == "ork" {
		env = append(env, localActivityCatalogEnv(t))
	}
	cmd := exec.CommandContext(ctx, orkerPath)
	cmd.Env = append(os.Environ(), env...)
	var log strings.Builder
	cmd.Stdout = &log
	cmd.Stderr = &log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start orker: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		t.Logf("orker log:\n%s", log.String())
	})
	return cmd
}

// liveAttemptDoc is one LiveAttempt as `run inspect --json` prints it.
type liveAttemptDoc struct {
	TaskID                         string `json:"taskId"`
	Attempt                        int    `json:"attempt"`
	State                          string `json:"state"`
	AttemptScheduledAt             string `json:"attemptScheduledAt"`
	AttemptStartedAt               string `json:"attemptStartedAt"`
	NextAttemptAt                  string `json:"nextAttemptAt"`
	TimeoutSeconds                 int    `json:"timeoutSeconds"`
	CancellationGracePeriodSeconds int    `json:"cancellationGracePeriodSeconds"`
	ExpiresAt                      string `json:"expiresAt"`
	HeartbeatAvailable             bool   `json:"heartbeatAvailable"`
	HeartbeatUnavailableReason     string `json:"heartbeatUnavailableReason"`
	LastHeartbeatAt                string `json:"lastHeartbeatAt"`
	HeartbeatStale                 bool   `json:"heartbeatStale"`
	HeartbeatDeadlineAt            string `json:"heartbeatDeadlineAt"`
	Phase                          string `json:"phase"`
	ContainerID                    string `json:"containerId"`
	BytesLogged                    int64  `json:"bytesLogged,string"`
	OrkerID                        string `json:"orkerId"`
}

// inspectDoc is `run inspect --json`.
type inspectDoc struct {
	RunStatus  string           `json:"runStatus"`
	Attempts   []liveAttemptDoc `json:"attempts"`
	ComputedAt string           `json:"computedAt"`
}

func (d *inspectDoc) attempt(taskID string) *liveAttemptDoc {
	for i := range d.Attempts {
		if d.Attempts[i].TaskID == taskID {
			return &d.Attempts[i]
		}
	}
	return nil
}

func (d *inspectDoc) terminal() bool {
	switch d.RunStatus {
	case "SUCCEEDED", "FAILED", "CANCELED", "SKIPPED":
		return true
	}
	return false
}

// inspect runs `run inspect --json` against one replica and decodes it.
func (s *timingStack) inspect(replica int, runID string) *inspectDoc {
	s.t.Helper()
	stdout := s.mustCLI(replica, "--json", "run", "inspect", runID)
	var doc inspectDoc
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		s.t.Fatalf("run inspect --json is not JSON: %v\n%s", err, stdout)
	}
	return &doc
}

// awaitInspect polls `run inspect --json` until done accepts a response, in a
// deadline loop of waitForPoolLive's shape.
func (s *timingStack) awaitInspect(replica int, runID string, within time.Duration, what string,
	done func(*inspectDoc) bool) *inspectDoc {
	s.t.Helper()
	deadline := time.Now().Add(within)
	var last *inspectDoc
	for time.Now().Before(deadline) {
		last = s.inspect(replica, runID)
		if done(last) {
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.t.Fatalf("run inspect never reported %s within %s; last %+v", what, within, last)
	return nil
}

// liveAttemptRow is one `run attempts --json` row.
type liveAttemptRow struct {
	TaskID  string `json:"taskId"`
	Attempt int    `json:"attempt"`
	Outcome string `json:"outcome"`
	OrkerID string `json:"orkerId"`
}

func (s *timingStack) attempts(runID string) []liveAttemptRow {
	s.t.Helper()
	var doc struct {
		Attempts []liveAttemptRow `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(s.mustCLI(0, "--json", "run", "attempts", runID)), &doc); err != nil {
		s.t.Fatalf("run attempts --json: %v", err)
	}
	return doc.Attempts
}

// awaitAttempt polls `run attempts --json` until attempt n of taskID has the
// wanted outcome, and returns it.
func (s *timingStack) awaitAttempt(runID, taskID string, n int, outcome string, within time.Duration) liveAttemptRow {
	s.t.Helper()
	deadline := time.Now().Add(within)
	var last []liveAttemptRow
	for time.Now().Before(deadline) {
		last = s.attempts(runID)
		for _, row := range last {
			if row.TaskID == taskID && row.Attempt == n && row.Outcome == outcome {
				return row
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.t.Fatalf("run attempts never showed %s attempt %d %s within %s; last %+v", taskID, n, outcome, within, last)
	return liveAttemptRow{}
}

func parseTime(t *testing.T, value string) time.Time {
	t.Helper()
	if value == "" {
		t.Fatal("a timestamp that must be set is unset")
	}
	return mustTime(t, value)
}

// chattyOrkflow is one task that writes a line every 200 ms until it is
// stopped, with the task's own bounds explicit.
func chattyOrkflow(name, retry string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: live attempt introspection probe — a task that keeps talking.
tasks:
  - id: talk
    uses: shell
    timeout_seconds: 60
%s    with:
      op: exec
      command: ["sh", "-c", "while :; do echo tick; sleep 0.2; done"]
`, name, retry)
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// removeContainer removes a container an Orker we killed left behind: a
// SIGKILLed Orker cannot stop its own workload, and no Orker with its id
// boots again to reap it.
func removeContainer(t *testing.T, id string) {
	t.Helper()
	if id == "" {
		return
	}
	out, err := exec.Command("docker", "rm", "-f", id).CombinedOutput()
	t.Logf("docker rm -f %s: %v %s", id, err, strings.TrimSpace(string(out)))
}

// TestIntegration_InspectRunReadsTheLiveHeartbeat.
func TestIntegration_InspectRunReadsTheLiveHeartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("run-inspect-live-heartbeat.txt")

	orkerID := uniqueName("orker-inspect-1")
	startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+orkerID, "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
	awaitActivityPoller(ctx, t, s.temporal, s.queue)

	name := uniqueName("inspect-live")
	s.apply(chattyOrkflow(name, ""))
	runID := s.submit(name)

	var phases []string
	first := s.awaitInspect(0, runID, 2*time.Minute, "a STARTED attempt in phase run", func(d *inspectDoc) bool {
		a := d.attempt("talk")
		if a == nil {
			return false
		}
		phases = append(phases, a.State+"/"+a.Phase)
		return a.State == "STARTED" && a.HeartbeatAvailable && a.Phase == "run"
	}).attempt("talk")
	t.Logf("I1 states/phases seen while polling: %v", phases)
	doc := s.inspect(0, runID)
	a := doc.attempt("talk")
	if a == nil || !a.HeartbeatAvailable {
		t.Fatalf("the attempt lost its heartbeat between two polls: %+v", a)
	}
	if a.Phase != "run" || a.ContainerID == "" || a.OrkerID != orkerID || a.Attempt != 1 ||
		a.TimeoutSeconds != 60 || a.CancellationGracePeriodSeconds != 30 || a.HeartbeatStale {
		t.Fatalf("live attempt = %+v, want phase run, a container, orker %s, attempt 1, timeout 60, grace 30, not stale", a, orkerID)
	}
	computed, lastBeat := parseTime(t, doc.ComputedAt), parseTime(t, a.LastHeartbeatAt)
	if age := computed.Sub(lastBeat); age > 3*time.Second {
		t.Errorf("computedAt - lastHeartbeatAt = %s, want <= 3s at a 1s tick", age)
	}
	if window := parseTime(t, a.HeartbeatDeadlineAt).Sub(lastBeat); window != task.HeartbeatTimeout {
		t.Errorf("heartbeatDeadlineAt - lastHeartbeatAt = %s, want %s", window, task.HeartbeatTimeout)
	}
	t.Logf("I1 phase=%q containerId=%s orkerId=%s attempt=%d timeoutSeconds=%d grace=%d stale=%v age=%s deadline-window=%s bytesLogged=%d",
		a.Phase, a.ContainerID, a.OrkerID, a.Attempt, a.TimeoutSeconds, a.CancellationGracePeriodSeconds,
		a.HeartbeatStale, computed.Sub(lastBeat), parseTime(t, a.HeartbeatDeadlineAt).Sub(lastBeat), a.BytesLogged)
	_ = first

	firstBytes := a.BytesLogged
	firstAt := computed
	grown := s.awaitInspect(0, runID, 20*time.Second, "a larger bytesLogged at least 1s later", func(d *inspectDoc) bool {
		b := d.attempt("talk")
		return b != nil && b.HeartbeatAvailable && b.BytesLogged > firstBytes &&
			parseTime(t, d.ComputedAt).Sub(firstAt) >= time.Second
	})
	t.Logf("I1 bytesLogged %d at %s -> %d at %s", firstBytes, doc.ComputedAt,
		grown.attempt("talk").BytesLogged, grown.ComputedAt)

	s.mustCLI(0, "run", "inspect", runID)
	s.mustCLI(0, "run", "cancel", runID)
	sawCancelRequested := false
	final := s.awaitInspect(0, runID, 90*time.Second, "a terminal run after cancel", func(d *inspectDoc) bool {
		if b := d.attempt("talk"); b != nil && b.State == "CANCEL_REQUESTED" {
			sawCancelRequested = true
			t.Logf("I1 CANCEL_REQUESTED observed: phase=%q available=%v", b.Phase, b.HeartbeatAvailable)
		}
		return d.terminal()
	})
	if final.RunStatus != "CANCELED" {
		t.Fatalf("run = %s after cancel, want CANCELED", final.RunStatus)
	}
	if len(final.Attempts) != 0 {
		t.Fatalf("a canceled run reports %d live attempts", len(final.Attempts))
	}
	t.Logf("I1 intermediate CANCEL_REQUESTED observed: %v; run reached %s", sawCancelRequested, final.RunStatus)
	human := s.mustCLI(0, "run", "inspect", runID)
	if !strings.Contains(human, "run CANCELED; nothing is live") {
		t.Fatalf("run inspect after cancel:\n%s", human)
	}
	row := s.awaitAttempt(runID, "talk", 1, "CANCELED", 60*time.Second)
	if row.OrkerID != a.OrkerID {
		t.Fatalf("run attempts orkerId = %q, the heartbeat carried %q", row.OrkerID, a.OrkerID)
	}
	t.Logf("I1 run attempts attempt 1 orkerId=%s == heartbeat orkerId=%s", row.OrkerID, a.OrkerID)
}

// TestIntegration_InspectRunShowsAnAttemptWaitingForAnOrker.
func TestIntegration_InspectRunShowsAnAttemptWaitingForAnOrker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("run-inspect-waiting.txt")

	name := uniqueName("inspect-wait")
	s.apply(fmt.Sprintf(`name: %s
version: "0.1.0"
description: live attempt introspection probe — waits for an orker.
tasks:
  - id: talk
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "for i in 1 2 3 4 5 6 7 8 9 10; do echo tick; sleep 0.5; done"]
`, name))
	runID := s.submit(name, "--allow-empty-pool")

	waiting := s.awaitInspect(0, runID, 60*time.Second, "a SCHEDULED attempt", func(d *inspectDoc) bool {
		a := d.attempt("talk")
		return a != nil && a.State == "SCHEDULED"
	}).attempt("talk")
	if waiting.HeartbeatUnavailableReason != "NOT_STARTED" || waiting.HeartbeatAvailable ||
		waiting.AttemptScheduledAt == "" || waiting.AttemptStartedAt != "" {
		t.Fatalf("waiting attempt = %+v, want NOT_STARTED, scheduled set, started unset", waiting)
	}
	t.Logf("I2 state=%s reason=%s attemptScheduledAt=%s attemptStartedAt=%q",
		waiting.State, waiting.HeartbeatUnavailableReason, waiting.AttemptScheduledAt, waiting.AttemptStartedAt)
	human := s.mustCLI(0, "run", "inspect", runID)
	if !strings.Contains(human, "talk: waiting for an orker on pool ") {
		t.Fatalf("human output does not say it is waiting:\n%s", human)
	}

	startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+uniqueName("orker-inspect-2"), "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
	started := s.awaitInspect(0, runID, 2*time.Minute, "STARTED with a heartbeat", func(d *inspectDoc) bool {
		a := d.attempt("talk")
		return a != nil && a.State == "STARTED" && a.HeartbeatAvailable
	}).attempt("talk")
	t.Logf("I2 after the orker started: state=%s phase=%s orkerId=%s", started.State, started.Phase, started.OrkerID)
	final := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	t.Logf("I2 run finished %s", final.GetStatus())
}

// TestIntegration_InspectRunNeverShowsAPreviousAttemptsDetail.
func TestIntegration_InspectRunNeverShowsAPreviousAttemptsDetail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	defer s.writeTranscript("run-inspect-previous-attempt.txt")
	startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
		"ORK_ORKER_ID="+uniqueName("orker-inspect-3"), "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
	awaitActivityPoller(ctx, t, s.temporal, s.queue)

	name := uniqueName("inspect-retry")
	s.apply(fmt.Sprintf(`name: %s
version: "0.1.0"
description: live attempt introspection probe — fails, then backs off.
tasks:
  - id: flaky
    uses: shell
    timeout_seconds: 60
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 20
      backoff_coefficient: 1.0
    with:
      op: exec
      command: ["sh", "-c", "echo partial; sleep 2; exit 1"]
`, name))
	runID := s.submit(name)
	s.awaitAttempt(runID, "flaky", 1, "FAILED", 2*time.Minute)

	backoff := s.awaitInspect(0, runID, 30*time.Second, "attempt 2 waiting out its backoff", func(d *inspectDoc) bool {
		a := d.attempt("flaky")
		return a != nil && a.NextAttemptAt != ""
	}).attempt("flaky")
	if backoff.State != "SCHEDULED" || backoff.Attempt != 2 || backoff.HeartbeatAvailable ||
		backoff.BytesLogged != 0 || backoff.Phase != "" {
		t.Fatalf("during the backoff InspectRun reported %+v, want SCHEDULED attempt 2 with no heartbeat, 0 bytes, no phase", backoff)
	}
	t.Logf("I3 InspectRun during the backoff: state=%s attempt=%d heartbeatAvailable=%v bytesLogged=%d phase=%q reason=%s nextAttemptAt=%s",
		backoff.State, backoff.Attempt, backoff.HeartbeatAvailable, backoff.BytesLogged, backoff.Phase,
		backoff.HeartbeatUnavailableReason, backoff.NextAttemptAt)

	// Temporal itself still holds attempt 1's detail for the pending activity.
	described, err := s.temporal.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatalf("DescribeWorkflowExecution: %v", err)
	}
	var carried *task.HeartbeatProgress
	for _, pa := range described.GetPendingActivities() {
		if pa.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		var progress task.HeartbeatProgress
		if err := converter.GetDefaultDataConverter().FromPayloads(pa.GetHeartbeatDetails(), &progress); err != nil {
			t.Fatalf("decode the cluster's heartbeat detail: %v", err)
		}
		carried = &progress
		t.Logf("I3 Temporal's pending activity: attempt=%d state=%s heartbeat_details=%+v last_heartbeat=%s last_started=%s",
			pa.GetAttempt(), pa.GetState(), progress, pa.GetLastHeartbeatTime().AsTime().Format(time.RFC3339Nano),
			pa.GetLastStartedTime().AsTime().Format(time.RFC3339Nano))
	}
	if carried == nil || carried.BytesLogged <= 0 {
		t.Fatalf("Temporal does not hold attempt 1's detail with bytes_logged > 0 (%+v); the scenario did not arise", carried)
	}

	next := s.awaitInspect(0, runID, 90*time.Second, "attempt 2 STARTED with its own heartbeat", func(d *inspectDoc) bool {
		a := d.attempt("flaky")
		return a != nil && a.Attempt == 2 && a.State == "STARTED" && a.HeartbeatAvailable
	}).attempt("flaky")
	if beat, started := parseTime(t, next.LastHeartbeatAt), parseTime(t, next.AttemptStartedAt); beat.Before(started) {
		t.Fatalf("attempt 2's first available heartbeat %s precedes its start %s", beat, started)
	}
	t.Logf("I3 attempt 2 first available heartbeat: lastHeartbeatAt=%s >= attemptStartedAt=%s phase=%q",
		next.LastHeartbeatAt, next.AttemptStartedAt, next.Phase)
	s.mustCLI(0, "run", "cancel", runID)
	awaitTerminalOnB(ctx, t, s.replicas[0], runID)
}

// TestIntegration_InspectRunMarksALostOrkerStale.
func TestIntegration_InspectRunMarksALostOrkerStale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()

	for _, variant := range []struct {
		name  string
		file  string
		retry string
	}{
		{"no retry policy", "run-inspect-lost-orker-no-retry.txt", "    retry:\n      maximum_attempts: 1\n"},
		{"retry policy", "run-inspect-lost-orker-retry.txt", "    retry:\n      maximum_attempts: 2\n      initial_interval_seconds: 1\n"},
	} {
		t.Run(variant.name, func(t *testing.T) {
			s := newTimingStack(ctx, t, 1)
			defer s.writeTranscript(variant.file)
			orker := startOrkerWith(ctx, t, s.orkerPath, s.address, s.namespace, s.queue,
				"ORK_ORKER_ID="+uniqueName("orker-inspect-4"), "ORK_ORKER_HEARTBEAT_INTERVAL=1s")
			awaitActivityPoller(ctx, t, s.temporal, s.queue)
			name := uniqueName("inspect-lost")
			s.apply(chattyOrkflow(name, variant.retry))
			runID := s.submit(name)

			live := s.awaitInspect(0, runID, 2*time.Minute, "a heartbeat in phase run", func(d *inspectDoc) bool {
				a := d.attempt("talk")
				return a != nil && a.HeartbeatAvailable && a.Phase == "run"
			}).attempt("talk")
			t.Cleanup(func() { removeContainer(t, live.ContainerID) })
			// The attempt must have left a durable trace before its Orker dies:
			// with no record ever filed, its log chunks are what tell `run
			// attempts` that attempt 1 happened at all (UNREPORTED rather than
			// no row). Killing before the first chunk lands would test the log
			// sink's latency, not this read.
			logDeadline := time.Now().Add(30 * time.Second)
			for {
				out, _, code := s.cli(0, "run", "logs", runID, "--task", "talk")
				if code == 0 && strings.Contains(out, "tick") {
					break
				}
				if time.Now().After(logDeadline) {
					t.Fatalf("attempt 1 wrote no retained log chunk within 30s")
				}
				time.Sleep(250 * time.Millisecond)
			}
			if err := orker.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatalf("SIGKILL the orker: %v", err)
			}
			killedAt := time.Now()
			t.Logf("I4 (%s) SIGKILLed orker pid %d holding container %s", variant.name, orker.Process.Pid, live.ContainerID)

			var previous *liveAttemptDoc
			stale := s.awaitInspect(0, runID, task.HeartbeatTimeout, "a stale heartbeat unchanged across two polls", func(d *inspectDoc) bool {
				a := d.attempt("talk")
				if a == nil || !a.HeartbeatAvailable || a.Attempt != 1 {
					previous = nil
					return false
				}
				ok := a.HeartbeatStale && previous != nil && previous.LastHeartbeatAt == a.LastHeartbeatAt &&
					parseTime(t, a.HeartbeatDeadlineAt).After(parseTime(t, d.ComputedAt))
				previous = a
				return ok
			})
			a := stale.attempt("talk")
			t.Logf("I4 (%s) %s after the kill: heartbeatStale=%v lastHeartbeatAt=%s (unchanged across two polls) heartbeatDeadlineAt=%s computedAt=%s",
				variant.name, time.Since(killedAt).Round(time.Second), a.HeartbeatStale, a.LastHeartbeatAt, a.HeartbeatDeadlineAt, stale.ComputedAt)
			s.mustCLI(0, "run", "inspect", runID)

			if variant.retry == "    retry:\n      maximum_attempts: 1\n" {
				final := s.awaitInspect(0, runID, 90*time.Second, "the run failing on the heartbeat timeout", func(d *inspectDoc) bool {
					return d.terminal()
				})
				if final.RunStatus != "FAILED" || len(final.Attempts) != 0 {
					t.Fatalf("after the heartbeat timeout: %+v, want FAILED with no attempts", final)
				}
				got := s.mustCLI(0, "--json", "run", "get", runID)
				if !strings.Contains(got, "ORKER_HEARTBEAT_TIMEOUT") {
					t.Fatalf("run get does not carry the heartbeat timeout category:\n%s", got)
				}
				t.Logf("I4 (%s) the attempt disappeared and the run FAILED with ORKER_HEARTBEAT_TIMEOUT", variant.name)
			} else {
				retried := s.awaitInspect(0, runID, 90*time.Second, "attempt 2 after the timeout", func(d *inspectDoc) bool {
					b := d.attempt("talk")
					return b != nil && b.Attempt == 2 && (b.State == "SCHEDULED" || b.State == "STARTED")
				}).attempt("talk")
				if retried.BytesLogged != 0 || retried.HeartbeatAvailable {
					t.Fatalf("attempt 2 = %+v, want its byte count reset and no heartbeat (no orker is alive)", retried)
				}
				t.Logf("I4 (%s) attempt 2 reappeared: state=%s bytesLogged=%d heartbeatAvailable=%v reason=%s",
					variant.name, retried.State, retried.BytesLogged, retried.HeartbeatAvailable, retried.HeartbeatUnavailableReason)
			}
			row := s.awaitAttempt(runID, "talk", 1, "UNREPORTED", 60*time.Second)
			t.Logf("I4 (%s) run attempts: attempt %d %s", variant.name, row.Attempt, row.Outcome)
			s.mustCLI(0, "run", "attempts", runID)
			if variant.retry != "    retry:\n      maximum_attempts: 1\n" {
				s.mustCLI(0, "run", "cancel", runID)
				awaitTerminalOnB(ctx, t, s.replicas[0], runID)
			}
		})
	}
}
