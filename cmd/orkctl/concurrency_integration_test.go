//go:build integration

package main

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// E88 P4 through the real binaries: a refuse or skip limit at a full key,
// at submit, at a schedule's fire and as `orkctl concurrency` reads it.

// limitedOrkflowYAML declares a run-level limit of one on its own key, with
// strategy and a task that holds the slot for holdSeconds.
func limitedOrkflowYAML(name, strategy string, holdSeconds int) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
concurrency:
  key: "%s-key"
  limit: 1
  on_limit: %s
tasks:
  - id: hold
    sleep:
      seconds: %d
`, name, name, strategy, holdSeconds)
}

// TestIntegration_ConcurrencySkipScheduleFire: under a skip limit, a fire
// that meets the key held by the schedule's earlier run starts no run and
// completes FIRE_SKIPPED_CONCURRENCY: the schedule, though it pauses on
// failure, records no failure, and `schedule get` says why in NOTE.
func TestIntegration_ConcurrencySkipScheduleFire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "conc-skip", 1)
	s.apply(t, limitedOrkflowYAML("skipper", "skip", 240))
	id := s.create(t, "nightly", "--orkflow", "skipper", "--cron", "0 2 * * *", "--paused", "--pause-on-failure", "--overlap", "allow-all")

	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	first := s.awaitFireRun(t, "nightly", 1)
	holder := first.Schedule.RecentFires[0].RunID
	s.awaitRunStatus(t, holder, 90*time.Second, "RUNNING")

	// A manual fire is one per second: wait out the first one's second.
	time.Sleep(1100 * time.Millisecond)
	s.cli(t, s.a, exitOK, "schedule", "fire", "nightly")
	skipped := s.awaitSchedule(t, "nightly", 90*time.Second, func(g scheduleJSON) bool {
		return len(g.Schedule.RecentFires) == 2 && g.Schedule.RecentFires[1].SkipCode != ""
	})
	fire := skipped.Schedule.RecentFires[1]
	if fire.SkipCode != "FIRE_SKIPPED_CONCURRENCY" || fire.RunID != "" || fire.Refusal != "" ||
		!strings.Contains(fire.SkipMessage, "skipper-key was full") {
		t.Fatalf("second fire = %+v; want skipped by the full key, with no run and no refusal", fire)
	}
	// The schedule was created paused, to be fired by hand; a fire that
	// failed under pause_on_failure would have written why into its note.
	if skipped.Schedule.Note != "" {
		t.Fatalf("a skipped fire was taken for a failure: the schedule's note is %q", skipped.Schedule.Note)
	}
	human := s.cli(t, s.a, exitOK, "schedule", "get", "nightly")
	if !strings.Contains(human, "FIRE_SKIPPED_CONCURRENCY: key skipper-key was full") {
		t.Fatalf("schedule get does not say why the fire was skipped:\n%s", human)
	}
	if runs := s.runsOf(t, id); len(runs) != 1 {
		t.Fatalf("the schedule started %d runs, want the one holder", len(runs))
	}
	t.Logf("EVIDENCE skip_code=%s skip_message=%q", fire.SkipCode, fire.SkipMessage)
	s.cli(t, s.a, exitOK, "run", "cancel", holder)
}

// TestIntegration_ConcurrencyRefuseAtSubmitAndShowTheHolder: a refuse
// limit's second submit is refused CONCURRENCY_LIMIT_REACHED, naming the run
// that holds the key, and starts nothing; `orkctl concurrency list` and
// `get` show that run holding and nobody waiting.
func TestIntegration_ConcurrencyRefuseAtSubmitAndShowTheHolder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "conc-refuse", 1)
	s.apply(t, limitedOrkflowYAML("refuser", "refuse", 240))
	var submitted struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if out := s.cli(t, s.a, exitOK, "--json", "run", "submit", "refuser"); json.Unmarshal([]byte(out), &submitted) != nil || submitted.Run.ID == "" {
		t.Fatalf("submit printed %s; want the run", out)
	}
	holder := submitted.Run.ID
	s.awaitRunStatus(t, holder, 90*time.Second, "RUNNING")

	// The holder runs its task, so it holds the key: the gate granted it.
	_, stderr, code := s.cliRaw(t, s.a, "run", "submit", "refuser")
	if code == exitOK || !strings.Contains(stderr, "CONCURRENCY_LIMIT_REACHED") || !strings.Contains(stderr, holder) {
		t.Fatalf("second submit exited %d: %s; want CONCURRENCY_LIMIT_REACHED naming %s", code, stderr, holder)
	}

	var list struct {
		Limits []struct {
			Key          string `json:"key"`
			Answering    bool   `json:"answering"`
			HoldersCount int    `json:"holdersCount"`
			QueuedCount  int    `json:"queuedCount"`
		} `json:"limits"`
	}
	out := s.cli(t, s.a, exitOK, "concurrency", "list", "--filter", "key=refuser-key", "--json")
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		t.Fatalf("decode concurrency list: %v (%s)", err, out)
	}
	if len(list.Limits) != 1 || !list.Limits[0].Answering || list.Limits[0].HoldersCount != 1 || list.Limits[0].QueuedCount != 0 {
		t.Fatalf("concurrency list = %+v; want refuser-key with one holder and nobody waiting", list.Limits)
	}
	got := s.cli(t, s.a, exitOK, "concurrency", "get", "refuser-key")
	if !strings.Contains(got, holder) || !strings.Contains(got, "Waiting (0)") {
		t.Fatalf("concurrency get does not show the holder:\n%s", got)
	}
	t.Logf("EVIDENCE refusal=%q", strings.TrimSpace(stderr))
	s.cli(t, s.a, exitOK, "run", "cancel", holder)
}

// TestIntegration_ConcurrencyResetExitCodes: on a real deployment a dry run
// and a reset exit 0 and the run keeps its slot, a reset at an epoch the limit
// is not at exits 5, the other engine answers the same request as completed,
// and the audit trail says who asked and what the limit did.
func TestIntegration_ConcurrencyResetExitCodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "conc-reset", 2)
	s.apply(t, limitedOrkflowYAML("refuser", "refuse", 240))
	var submitted struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if out := s.cli(t, s.a, exitOK, "--json", "run", "submit", "refuser"); json.Unmarshal([]byte(out), &submitted) != nil || submitted.Run.ID == "" {
		t.Fatalf("submit printed %s; want the run", out)
	}
	holder := submitted.Run.ID
	s.awaitRunStatus(t, holder, 90*time.Second, "RUNNING")

	dry := s.cli(t, s.a, exitOK, "admin", "concurrency", "reset", "refuser-key", "--dry-run")
	if !strings.Contains(dry, "1 running and 0 queued runs keep their slots") || !strings.Contains(dry, "Dry run: nothing was changed.") {
		t.Fatalf("dry run printed:\n%s", dry)
	}
	if _, stderr, code := s.cliRaw(t, s.a, "admin", "concurrency", "reset", "refuser-key", "--expected-epoch", "99"); code != exitResetRefused || !strings.Contains(stderr, "CONCURRENCY_RESET_CONFLICT") {
		t.Fatalf("a reset at another epoch exited %d: %s", code, stderr)
	}
	issued := time.Now().UTC().Format(time.RFC3339Nano)
	args := []string{"admin", "concurrency", "reset", "refuser-key", "--request-id", "rst-exit", "--issued-at", issued}
	out := s.cli(t, s.a, exitOK, args...)
	if !strings.Contains(out, "The limit's record is rebuilt.") {
		t.Fatalf("reset printed:\n%s", out)
	}
	if again := s.cli(t, s.b, exitOK, args...); !strings.Contains(again, "The limit's record is rebuilt.") {
		t.Fatalf("the other engine, asked the same, printed:\n%s", again)
	}
	if got := s.cli(t, s.a, exitOK, "concurrency", "get", "refuser-key"); !strings.Contains(got, holder) {
		t.Fatalf("the holder lost its slot:\n%s", got)
	}
	s.refusedByHolder(t, "refuser", holder)
	trail := auditCLI(t, s.b.engineReplica, "", "admin", "audit", "--filter", "action^concurrency.reset.", "--filter", "namespace="+s.namespace)
	for _, action := range []string{"concurrency.reset.requested", "concurrency.reset.applied", "concurrency.reset.completed"} {
		if !strings.Contains(trail, action) {
			t.Errorf("the audit trail has no %s:\n%s", action, trail)
		}
	}
	if got := strings.Count(trail, "concurrency.reset.requested"); got != 2 {
		t.Errorf("%d requests audited; want the one that started it and the one refused:\n%s", got, trail)
	}
	t.Logf("EVIDENCE %s", strings.TrimSpace(out))
	s.cli(t, s.a, exitOK, "run", "cancel", holder)
}

// TestIntegration_ConcurrencyResetOrkflowsOwnKey: the limit of an Orkflow that
// declares no key is read and reset under its own key, which get and the run
// name, and its holder keeps its slot.
func TestIntegration_ConcurrencyResetOrkflowsOwnKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newScheduleStack(ctx, t, "conc-own-key", 1)
	s.apply(t, strings.Replace(limitedOrkflowYAML("unkeyed", "refuse", 240), "  key: \"unkeyed-key\"\n", "", 1))
	var submitted struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if out := s.cli(t, s.a, exitOK, "--json", "run", "submit", "unkeyed"); json.Unmarshal([]byte(out), &submitted) != nil || submitted.Run.ID == "" {
		t.Fatalf("submit printed %s; want the run", out)
	}
	holder := submitted.Run.ID
	s.awaitRunStatus(t, holder, 90*time.Second, "RUNNING")
	key := engine.DefaultRunConcurrencyKey(s.namespace, "unkeyed")
	if out := s.cli(t, s.a, exitOK, "concurrency", "list", "-F", "key="+key); !strings.Contains(out, key) {
		t.Fatalf("list printed no %s:\n%s", key, out)
	}
	if out := s.cli(t, s.a, exitOK, "admin", "concurrency", "reset", key); !strings.Contains(out, "The limit's record is rebuilt.") {
		t.Fatalf("reset printed:\n%s", out)
	}
	if out := s.cli(t, s.a, exitOK, "concurrency", "get", key); !strings.Contains(out, holder) {
		t.Fatalf("the holder lost its slot:\n%s", out)
	}
	s.refusedByHolder(t, "unkeyed", holder)
	s.cli(t, s.a, exitOK, "run", "cancel", holder)
}

// refusedByHolder fails unless a submit of a refuse Orkflow whose limit was
// reset is refused, naming holder. A limit whose record was just rebuilt waits
// for what the cluster's index may still be adding, and says it cannot tell
// yet; that submit is sent again.
func (s *scheduleStack) refusedByHolder(t *testing.T, name, holder string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		_, stderr, code := s.cliRaw(t, s.a, "run", "submit", name)
		switch {
		case code == exitOK:
			t.Fatalf("a submit after the reset was accepted; want refused by %s", holder)
		case strings.Contains(stderr, holder):
			return
		case !strings.Contains(stderr, "CONCURRENCY_CHECK_UNAVAILABLE") || time.Now().After(deadline):
			t.Fatalf("a submit after the reset exited %d: %s; want refused by %s", code, stderr, holder)
		}
		select {
		case <-t.Context().Done():
			t.Fatal("the test ended while it waited for the limit to settle")
		case <-time.After(250 * time.Millisecond):
		}
	}
}
