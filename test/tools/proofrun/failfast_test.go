package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const failFastSuite = modulePath + "/test/tools/proofrun/testdata/failfast"

// runFailFastSuite runs testdata/failfast on two slots: TestFailsWhileTheBlockerRuns
// and TestBlocksUntilStopped start together, TestNeverStarts is queued behind.
func runFailFastSuite(t *testing.T, extra []string, env map[string]string) (int, map[string]entry, time.Duration) {
	t.Helper()
	quiet(t)
	out := t.TempDir()
	costs := filepath.Join(out, "costs.tsv")
	text := failFastSuite + "\tTestFailsWhileTheBlockerRuns\t30\n" +
		failFastSuite + "\tTestBlocksUntilStopped\t30\n" +
		failFastSuite + "\tTestNeverStarts\t20\n"
	if err := os.WriteFile(costs, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAILFAST_DIR", t.TempDir())
	for k, v := range env {
		t.Setenv(k, v)
	}
	args := append([]string{"-out", out, "-jobs", "2", "-costs", costs, "-allow", ""}, extra...)
	args = append(args, "./testdata/failfast")
	started := time.Now()
	code := cmdIntegration(args)
	took := time.Since(started)
	var l ledger
	if err := readJSON(filepath.Join(out, "integration-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	got := map[string]entry{}
	for _, e := range l.Tests {
		got[e.Test] = e
	}
	return code, got, took
}

// With -fail-fast the first failure ends the run: the test still running is
// asked to end and is recorded as unfinished, the test behind it never starts
// and is recorded as not run, and neither is a pass. The ledger is complete
// and fails the proof.
func TestIntegrationRunner_FailFastStopsTheRunAndRecordsWhatDidNotFinish(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	code, got, took := runFailFastSuite(t, []string{"-fail-fast"}, nil)
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if got["TestFailsWhileTheBlockerRuns"].Outcome != outcomeFail {
		t.Errorf("the failing test = %+v", got["TestFailsWhileTheBlockerRuns"])
	}
	block := got["TestBlocksUntilStopped"]
	if block.Outcome != outcomeNone || !strings.Contains(block.Reason, "not finished") || !strings.Contains(block.Reason, "fail-fast: ") {
		t.Errorf("the test that was running = %+v, want no outcome, not finished, naming the failure", block)
	}
	never := got["TestNeverStarts"]
	if never.Outcome != outcomeNone || !strings.Contains(never.Reason, "not run") {
		t.Errorf("the queued test = %+v, want no outcome, not run", never)
	}
	if took > time.Minute {
		t.Errorf("the run took %s: the blocked test was not stopped", took)
	}
}

// Without -fail-fast the same failure stops nothing.
func TestIntegrationRunner_WithoutFailFastEveryTestRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	code, got, _ := runFailFastSuite(t, nil, map[string]string{"FAILFAST_NO_BLOCK": "1"})
	if code != 1 {
		t.Fatalf("exit %d, want 1 (a test fails)", code)
	}
	for name, want := range map[string]string{
		"TestFailsWhileTheBlockerRuns": outcomeFail, "TestBlocksUntilStopped": outcomePass, "TestNeverStarts": outcomePass,
	} {
		if got[name].Outcome != want {
			t.Errorf("%s = %+v, want %s", name, got[name], want)
		}
	}
}

func failFastRunner(excused map[testKey]quarantineEntry) (*runner, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{failFast: true, excused: excused, c: newCollector(), log: &strings.Builder{}, cancel: cancel}
	return r, ctx
}

// A failure the quarantine list excuses does not fail the proof, so it must
// not stop it; any other failure does.
func TestRunner_FailFastIgnoresAQuarantinedFailureOnly(t *testing.T) {
	top := testKey{"p", "TestTop"}
	leaf := testKey{"p", "TestTop/leaf"}
	local := newCollector()
	local.failedSubs = []testKey{leaf}

	r, ctx := failFastRunner(map[testKey]quarantineEntry{leaf: {Suite: "integration", Package: "p", Test: "TestTop/leaf"}})
	r.onFailure(local)(top)
	if r.stopped() != "" || ctx.Err() != nil {
		t.Fatalf("a quarantined failure stopped the run: %q", r.stopped())
	}

	r, ctx = failFastRunner(nil)
	r.onFailure(local)(top)
	if !strings.Contains(r.stopped(), "p.TestTop failed") || ctx.Err() == nil {
		t.Fatalf("an unexcused failure did not stop the run: %q, ctx %v", r.stopped(), ctx.Err())
	}
	first := r.stopped()
	r.onFailure(local)(testKey{"p", "TestOther"})
	if r.stopped() != first {
		t.Errorf("the stop reason changed from %q to %q: the first failure is the one recorded", first, r.stopped())
	}
}

// A stopped run records a job it never started as tests with no outcome, and a
// test that already passed keeps its pass.
func TestRunner_ASettledJobOfAStoppedRunIsNeverAPass(t *testing.T) {
	r, _ := failFastRunner(nil)
	r.c.results[testKey{"p", "TestDone"}] = &entry{Package: "p", Test: "TestDone", Outcome: outcomePass}
	r.stop("fail-fast: p.TestBad failed")
	rest := r.settle(job{ID: "p-001", Package: "p", Tests: []string{"TestDone", "TestQueued"}}, jobResult{})
	if len(rest) != 0 {
		t.Errorf("a stopped run asked to run %v again", rest)
	}
	if e := r.c.results[testKey{"p", "TestDone"}]; e.Outcome != outcomePass {
		t.Errorf("a finished test lost its outcome: %+v", e)
	}
	e := r.c.results[testKey{"p", "TestQueued"}]
	if e == nil || e.Outcome != outcomeNone || !strings.Contains(e.Reason, "not run") || !strings.Contains(e.Reason, "p.TestBad failed") {
		t.Errorf("the queued test = %+v", e)
	}
}

// A binary that ends while a test runs is a failure, and under -fail-fast it
// stops the run; the tests behind it are recorded as not run, never re-run.
func TestRunner_FailFastStopsOnACrashAndDoesNotRerunTheRest(t *testing.T) {
	r, ctx := failFastRunner(nil)
	r.c.started[testKey{"p", "TestCrash"}] = true
	rest := r.settle(job{ID: "p-001", Package: "p", Tests: []string{"TestBefore", "TestCrash", "TestAfter"}}, jobResult{exitErr: os.ErrProcessDone, started: 1})
	if len(rest) != 0 || ctx.Err() == nil {
		t.Fatalf("rest %v, ctx %v: a crash must stop the run", rest, ctx.Err())
	}
	for name, want := range map[string]string{"TestCrash": outcomeFail, "TestBefore": outcomeNone, "TestAfter": outcomeNone} {
		if e := r.c.results[testKey{"p", name}]; e == nil || e.Outcome != want {
			t.Errorf("%s = %+v, want %s", name, e, want)
		}
	}
}

// A signal (the proof script stopping this gate because another failed) ends
// the run the same way, with or without -fail-fast: nothing starts, and every
// test is recorded as not run.
func TestRunner_ASignalStopsTheRunAndLeavesACompleteLedger(t *testing.T) {
	quiet(t)
	r := &runner{slots: 2, c: newCollector(), log: &strings.Builder{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.runAll(ctx, []job{
		{ID: "p-001", Package: "p", Tests: []string{"TestA"}},
		{ID: "p-002", Package: "p", Tests: []string{"TestB", "TestC"}, Heavy: true},
	})
	if !strings.Contains(r.stopped(), "signal") {
		t.Fatalf("stop reason = %q", r.stopped())
	}
	l := buildLedger("integration", []testKey{{"p", "TestA"}, {"p", "TestB"}, {"p", "TestC"}}, nil, r.c, nil)
	if l.Summary.NoOutcome != 3 || l.Summary.Pass != 0 || l.ok() {
		t.Errorf("ledger = %+v, problems %v: every test must be without an outcome and the ledger must fail", l.Summary, l.Problems)
	}
	if !strings.Contains(strings.Join(l.Problems, "\n"), "not run") {
		t.Errorf("problems do not say the tests were not run: %v", l.Problems)
	}
}

// The heavy-done marker is written once the heavy jobs have ended and before
// any ordinary job starts, and it is written for a plan with no heavy job at
// all, so the gate that waits for it (scripts/proof.sh starts the activity
// fixture on it) never waits on a plan that has none.
func TestRunner_WritesTheHeavyDoneMarkerBeforeTheOrdinaryJobs(t *testing.T) {
	quiet(t)
	for _, tc := range []struct {
		name string
		jobs []job
	}{
		{"with a heavy job", []job{
			{ID: "p-001", Package: "p", Tests: []string{"TestA"}},
			{ID: "p-002", Package: "p", Tests: []string{"TestB"}, Heavy: true},
		}},
		{"without one", []job{{ID: "p-001", Package: "p", Tests: []string{"TestA"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "integration-heavy-done")
			r := &runner{slots: 2, c: newCollector(), log: &strings.Builder{}, heavyDone: marker}
			// A cancelled context runs no process, so this exercises the
			// scheduling order alone: the marker must still appear.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			r.runAll(ctx, tc.jobs)
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("heavy-done marker: %v", err)
			}
		})
	}
	t.Run("no path, no file", func(t *testing.T) {
		r := &runner{slots: 1, c: newCollector(), log: &strings.Builder{}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r.runAll(ctx, nil) // must not panic or write anywhere
	})
}
