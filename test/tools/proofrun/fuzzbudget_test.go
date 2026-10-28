package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The fuzz lane's time (docs/PROOFS.md §2.3) is a budget for the whole lane,
// shared by its targets, so the lane ends inside its CI step however many
// targets the module has. These tests drive the allocation with an injected
// clock and an injected runner: nothing here sleeps or fuzzes.

// fakeLane is a lane host whose clock moves only when a target runs: a
// target takes the fuzz time it was given plus its overhead (the build the
// engine does before it fuzzes).
type fakeLane struct {
	t        *testing.T
	start    time.Time
	now      time.Time
	overhead func(k testKey) time.Duration
	outcome  func(k testKey) string
	// fuzzed is the fuzz time each target was given, in the order they ran.
	fuzzed []time.Duration
	ran    []testKey
	log    bytes.Buffer
}

func newFakeLane(t *testing.T, overhead time.Duration) *fakeLane {
	start := time.Date(2026, 10, 28, 5, 17, 0, 0, time.UTC)
	return &fakeLane{
		t: t, start: start, now: start,
		overhead: func(testKey) time.Duration { return overhead },
		outcome:  func(testKey) string { return outcomePass },
	}
}

func (f *fakeLane) clock() time.Time { return f.now }

func (f *fakeLane) run(k testKey, fuzztime string) entry {
	f.t.Helper()
	d, err := time.ParseDuration(fuzztime)
	if err != nil {
		f.t.Fatalf("the lane gave %s a fuzz time %q that go test would refuse: %v", k, fuzztime, err)
	}
	f.fuzzed = append(f.fuzzed, d)
	f.ran = append(f.ran, k)
	f.now = f.now.Add(d + f.overhead(k))
	e := entry{Package: k.Package, Test: k.Test, Job: suiteFuzz, Outcome: f.outcome(k)}
	if e.Outcome != outcomePass {
		e.Reason = "crasher kept at crashers/x"
	}
	return e
}

func fuzzKeys(n int) []testKey {
	keys := make([]testKey, n)
	for i := range keys {
		keys[i] = testKey{"example.com/p", fmt.Sprintf("FuzzT%03d", i)}
	}
	return keys
}

// withTuning pins the lane's tunables for one test and restores them.
func withTuning(t *testing.T, min, overhead time.Duration) {
	t.Helper()
	prev := fuzzTuning
	fuzzTuning = fuzzTunables{Min: min, Overhead: overhead}
	t.Cleanup(func() { fuzzTuning = prev })
}

func TestFuzzLane_TheFirstTargetGetsAnEqualShareLessTheExpectedOverhead(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	for _, c := range []struct {
		name    string
		budget  time.Duration
		targets int
		want    time.Duration
	}{
		{"84 targets in 105m", 105 * time.Minute, 84, 30 * time.Second},
		{"one target takes the whole budget less its overhead", 10 * time.Minute, 1, 10*time.Minute - 45*time.Second},
		{"a share below the minimum is raised to it", 80 * time.Minute, 84, 15 * time.Second},
		{"a share is cut to whole seconds", 101 * time.Minute, 84, 27 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := newFuzzPlanner(fuzzTuning, c.budget, time.Time{})
			got, ok := p.allot(time.Time{}, c.targets)
			if !ok || got != c.want {
				t.Errorf("allotted %v (ok %v), want %v", got, ok, c.want)
			}
		})
	}
}

func TestFuzzLane_EndsInsideItsBudgetWhateverTheNumberOfTargets(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	budget := 105 * time.Minute
	for _, n := range []int{1, 3, 20, 84, 100} {
		for _, overhead := range []time.Duration{0, 10 * time.Second, 45 * time.Second} {
			t.Run(fmt.Sprintf("%d targets, %v overhead", n, overhead), func(t *testing.T) {
				f := newFakeLane(t, overhead)
				l := runFuzzLane(fuzzKeys(n), fuzzPlan{Budget: budget}, f.start, f.clock, f.run, &f.log)
				if !l.ok() {
					t.Fatalf("the ledger has problems: %v", l.Problems)
				}
				if len(f.ran) != n {
					t.Fatalf("ran %d of %d targets", len(f.ran), n)
				}
				if took := f.now.Sub(f.start); took > budget {
					t.Errorf("the lane took %v, over its budget %v", took, budget)
				}
				for i, d := range f.fuzzed {
					if d < fuzzTuning.Min {
						t.Errorf("target %d fuzzed %v, under the minimum %v", i, d, fuzzTuning.Min)
					}
				}
			})
		}
	}
}

func TestFuzzLane_ASlowRunnerStillEndsInsideTheBudgetAndNamesWhatItNeverFuzzed(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	// 84 targets at 90s of overhead and the 15s minimum need 147 minutes.
	budget := 105 * time.Minute
	f := newFakeLane(t, 90*time.Second)
	l := runFuzzLane(fuzzKeys(84), fuzzPlan{Budget: budget}, f.start, f.clock, f.run, &f.log)
	if took := f.now.Sub(f.start); took > budget {
		t.Errorf("the lane took %v, over its budget %v: the overhead it measured must stop it starting what it cannot finish", took, budget)
	}
	if len(f.ran) == 0 || len(f.ran) >= 84 {
		t.Fatalf("ran %d of 84 targets, want some but not all", len(f.ran))
	}
	if l.ok() || l.Summary.Expected != 84 || l.Summary.Pass != len(f.ran) || l.Summary.Fail != 84-len(f.ran) {
		t.Errorf("summary = %+v for %d targets run", l.Summary, len(f.ran))
	}
	if len(l.Problems) != 84-len(f.ran) {
		t.Errorf("%d problems for %d targets never fuzzed", len(l.Problems), 84-len(f.ran))
	}
}

func TestFuzzLane_UsesWhatTheBudgetLeavesInsteadOfLeavingItUnspent(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	budget := 105 * time.Minute
	f := newFakeLane(t, 10*time.Second)
	l := runFuzzLane(fuzzKeys(84), fuzzPlan{Budget: budget}, f.start, f.clock, f.run, &f.log)
	if !l.ok() {
		t.Fatal(l.Problems)
	}
	took := f.now.Sub(f.start)
	// The overhead the lane expected at first was longer than the real one;
	// the targets after the first few share what that left, so little of the
	// budget is wasted (the last target keeps only the difference).
	if took < budget-time.Minute {
		t.Errorf("the lane took %v of a %v budget: the time its early overestimate left was not shared out", took, budget)
	}
	// Equal shares of what remains: a target is never given less than the one
	// before it when the real overhead is below the estimate.
	for i := 1; i < len(f.fuzzed); i++ {
		if f.fuzzed[i] < f.fuzzed[i-1] {
			t.Errorf("target %d fuzzed %v, less than target %d (%v) although the overhead was below the estimate", i, f.fuzzed[i], i-1, f.fuzzed[i-1])
		}
	}
}

func TestFuzzLane_TheOverheadItMeasuresReplacesTheInitialEstimate(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	// A real overhead of 20s, a budget of 20 targets x 2m.
	budget := 40 * time.Minute
	f := newFakeLane(t, 20*time.Second)
	runFuzzLane(fuzzKeys(20), fuzzPlan{Budget: budget}, f.start, f.clock, f.run, &f.log)
	// After three measured targets the estimate is the median of them, so the
	// next allotment is the equal share of what remains less 20s, not less 45s.
	remaining := budget
	for i := 0; i < 3; i++ {
		remaining -= f.fuzzed[i] + 20*time.Second
	}
	want := (remaining / 17).Truncate(time.Second) - 20*time.Second
	if got := f.fuzzed[3]; got < want-time.Second || got > want+time.Second {
		t.Errorf("the fourth target fuzzed %v, want about %v (an equal share of %v over 17 targets, less the measured 20s)", got, want, remaining)
	}
	// The first three ran on the initial estimate.
	first := budget/20 - 45*time.Second
	if got := f.fuzzed[0]; got != first {
		t.Errorf("the first target fuzzed %v, want %v", got, first)
	}
}

func TestFuzzLane_OneColdBuildDoesNotStarveTheTargetsAfterIt(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	budget := 105 * time.Minute
	f := newFakeLane(t, 10*time.Second)
	f.overhead = func(k testKey) time.Duration {
		if k.Test == "FuzzT000" {
			return 4 * time.Minute // the first build with a cold cache
		}
		return 10 * time.Second
	}
	l := runFuzzLane(fuzzKeys(84), fuzzPlan{Budget: budget}, f.start, f.clock, f.run, &f.log)
	if !l.ok() {
		t.Fatal(l.Problems)
	}
	for i, d := range f.fuzzed[4:] {
		if d <= fuzzTuning.Min {
			t.Errorf("target %d fuzzed %v: one slow build is the median's outlier and must not push the others to the minimum", i+4, d)
		}
	}
	if took := f.now.Sub(f.start); took > budget {
		t.Errorf("the lane took %v, over its budget %v", took, budget)
	}
}

func TestFuzzLane_ABudgetSpentBeforeAllTargetsStartedFailsTheOnesNeverFuzzed(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	// Three targets' minimum plus overhead is 3m; the budget is 2m.
	budget := 2 * time.Minute
	f := newFakeLane(t, 45*time.Second)
	l := runFuzzLane(fuzzKeys(3), fuzzPlan{Budget: budget}, f.start, f.clock, f.run, &f.log)
	if len(f.ran) != 2 {
		t.Fatalf("ran %d targets, want 2 (a third would start with less than its minimum and overhead left)", len(f.ran))
	}
	if l.ok() || l.Summary.Expected != 3 || l.Summary.Pass != 2 || l.Summary.Fail != 1 {
		t.Fatalf("summary = %+v, problems = %v", l.Summary, l.Problems)
	}
	last := l.Tests[2]
	if last.Test != "FuzzT002" || last.Outcome != outcomeFail || !strings.Contains(last.Reason, "not fuzzed") || !strings.Contains(last.Reason, "2m0s") {
		t.Errorf("the unreached target's entry = %+v, want a failure that says it was not fuzzed and names the budget", last)
	}
	if last.Seconds != 0 || last.FuzzSeconds != 0 {
		t.Errorf("the unreached target has fuzz time %v and seconds %v, want none", last.FuzzSeconds, last.Seconds)
	}
	if len(l.Problems) != 1 || !strings.Contains(l.Problems[0], "example.com/p.FuzzT002") {
		t.Errorf("problems = %v, want the unreached target named", l.Problems)
	}
	if took := f.now.Sub(f.start); took > budget+time.Minute {
		t.Errorf("the lane took %v, far past its %v budget", took, budget)
	}
}

func TestFuzzLane_ACrasherIsReportedAsBeforeAndItsEarlyEndIsNotAnOverheadSample(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	f := newFakeLane(t, 10*time.Second)
	crash := testKey{"example.com/p", "FuzzT001"}
	f.outcome = func(k testKey) string {
		if k == crash {
			return outcomeFail
		}
		return outcomePass
	}
	l := runFuzzLane(fuzzKeys(5), fuzzPlan{Budget: 30 * time.Minute}, f.start, f.clock, f.run, &f.log)
	if l.ok() || l.Summary.Fail != 1 || l.Summary.Pass != 4 || l.Summary.Expected != 5 {
		t.Fatalf("summary = %+v, problems = %v", l.Summary, l.Problems)
	}
	if len(f.ran) != 5 {
		t.Errorf("ran %d targets: a crasher must not end the lane, the others still fuzz", len(f.ran))
	}
	if len(l.Problems) != 1 || !strings.Contains(l.Problems[0], "example.com/p.FuzzT001 failed: crasher kept") {
		t.Errorf("problems = %v", l.Problems)
	}
}

func TestFuzzLane_RecordsEachTargetsFuzzTimeInTheLedgerAndTheLog(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	f := newFakeLane(t, 20*time.Second)
	l := runFuzzLane(fuzzKeys(4), fuzzPlan{Budget: 10 * time.Minute}, f.start, f.clock, f.run, &f.log)
	if len(l.Tests) != 4 {
		t.Fatalf("ledger has %d entries", len(l.Tests))
	}
	for i, e := range l.Tests {
		if want := f.fuzzed[i].Seconds(); e.FuzzSeconds != want {
			t.Errorf("%s: fuzz_seconds = %v, want the %v it was given", e.Test, e.FuzzSeconds, want)
		}
		if want := (f.fuzzed[i] + 20*time.Second).Seconds(); e.Seconds != want {
			t.Errorf("%s: seconds = %v, want its whole %v", e.Test, e.Seconds, want)
		}
		line := fmt.Sprintf("fuzz: example.com/p.%s: pass (fuzz time %s, took %s", e.Test, f.fuzzed[i], f.fuzzed[i]+20*time.Second)
		if !strings.Contains(f.log.String(), line) {
			t.Errorf("the lane's log lacks %q:\n%s", line, f.log.String())
		}
	}
}

func TestFuzzLane_AFixedFuzzTimeGivesEveryTargetThatTimeAndNoBudgetApplies(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	for _, fixed := range []string{"30s", "2m", "100x"} {
		t.Run(fixed, func(t *testing.T) {
			var given []string
			clock := func() time.Time { return time.Time{} }
			run := func(k testKey, fuzztime string) entry {
				given = append(given, fuzztime)
				return entry{Package: k.Package, Test: k.Test, Job: suiteFuzz, Outcome: outcomePass}
			}
			var log bytes.Buffer
			l := runFuzzLane(fuzzKeys(3), fuzzPlan{Fixed: fixed}, time.Time{}, clock, run, &log)
			if !l.ok() || len(given) != 3 {
				t.Fatalf("problems %v, ran %d", l.Problems, len(given))
			}
			for i, g := range given {
				if g != fixed {
					t.Errorf("target %d was given %q, want %q", i, g, fixed)
				}
			}
			if d, err := time.ParseDuration(fixed); err == nil && l.Tests[0].FuzzSeconds != d.Seconds() {
				t.Errorf("fuzz_seconds = %v, want %v", l.Tests[0].FuzzSeconds, d.Seconds())
			}
		})
	}
}

func TestFuzzLane_NoTargetIsAProblemNotAPass(t *testing.T) {
	f := newFakeLane(t, 0)
	l := runFuzzLane(nil, fuzzPlan{Budget: time.Hour}, f.start, f.clock, f.run, &f.log)
	if l.ok() || !strings.Contains(strings.Join(l.Problems, "\n"), "no Fuzz target") {
		t.Errorf("ledger = %+v", l)
	}
}

func TestFuzzBudget_IsAPositiveDurationThatCoversATarget(t *testing.T) {
	withTuning(t, 15*time.Second, 45*time.Second)
	for _, c := range []struct {
		in string
		ok bool
	}{
		{"105m", true}, {"2h", true}, {"90m30s", true}, {"1m", true},
		{"59s", false}, {"0", false}, {"0s", false}, {"-5m", false}, {"100x", false}, {"abc", false}, {"", false}, {"1.5", false},
	} {
		_, err := parseFuzzBudget(c.in)
		if (err == nil) != c.ok {
			t.Errorf("budget %q: err = %v, want ok = %v", c.in, err, c.ok)
		}
	}
}
