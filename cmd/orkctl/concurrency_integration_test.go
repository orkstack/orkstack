//go:build integration

package main

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"strings"
	"testing"
	"time"
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
