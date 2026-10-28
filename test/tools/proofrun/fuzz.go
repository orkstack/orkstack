package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The fuzz lane (AGENTS.md §11.2, docs/PROOFS.md §2.3): every Fuzz target of
// the module, each for a bounded time, in the nightly tier. A target the
// engine cannot break passes; a crasher fails the lane, and the input that
// broke it is kept under the lane's output directory, so a nightly's artifact
// carries what a person needs to reproduce it. The engine writes a crasher
// into the fuzzed package's testdata/fuzz/<Target>/ — a file that would turn
// the proof's tree into one it did not start with — so the runner moves it out.

// fuzzWorkDir is the directory the lane's go commands run in; empty is the
// process's own. Tests point it at a throwaway module, because a crasher is
// written into the tree that was fuzzed.
var fuzzWorkDir string

// The lane's time is a budget for the whole lane, shared by its targets, so
// the lane ends inside its CI step however many targets the module has
// (docs/PROOFS.md §2.3). A target's share is an equal part of what remains,
// less the overhead the lane expects for starting a target: the build and
// link of its instrumented test binary, which `go test -fuzztime` does not
// count. The lane measures that overhead as it goes.
const (
	// fuzzMinTime is the least a target is ever fuzzed for. A target that
	// cannot be given it before the budget is spent is not run, and fails
	// the lane by name rather than being skipped quietly.
	fuzzMinTime = 15 * time.Second
	// fuzzOverheadEstimate is the overhead the lane assumes for a target
	// until it has measured fuzzOverheadSamples of them, and the allowance
	// the budget rule (fuzzbudget_workflow_test.go) holds the target count to.
	// It is conservative: a warm local build is a few seconds, a cold one on a
	// small runner is tens.
	fuzzOverheadEstimate = 45 * time.Second
	// fuzzOverheadSamples is how many targets are measured before the median
	// of their overheads replaces the estimate; the median, so that the one
	// cold build of a run does not starve every target after it.
	fuzzOverheadSamples = 3
	// fuzzTargetTail is how long past its fuzz time one target may run, for
	// the build before it and the minimization after a crasher, before the
	// lane stops it (fuzzDeadline).
	fuzzTargetTail = 10 * time.Minute
	// fuzzLaneMargin is the room a lane's step needs beyond its budget: the
	// last target's tail and the part's own work around the lane. The CI
	// budget plus this margin must fit the fuzz step's timeout.
	fuzzLaneMargin = fuzzTargetTail + 5*time.Minute
)

// fuzzTunables are the numbers the allocation uses; tests lower them.
type fuzzTunables struct {
	Min      time.Duration
	Overhead time.Duration
}

var fuzzTuning = fuzzTunables{Min: fuzzMinTime, Overhead: fuzzOverheadEstimate}

// parseFuzzBudget reads a lane budget: a positive duration that covers at
// least one target at its minimum fuzz time and expected overhead.
func parseFuzzBudget(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("-budget %q must be a positive duration such as 105m", s)
	}
	if need := fuzzTuning.Min + fuzzTuning.Overhead; d < need {
		return 0, fmt.Errorf("-budget %q cannot fuzz even one target: it needs %s (the minimum fuzz time %s and the expected overhead %s)", s, need, fuzzTuning.Min, fuzzTuning.Overhead)
	}
	return d, nil
}

// fuzzPlanner gives each target its fuzz time out of the lane's budget.
type fuzzPlanner struct {
	tun       fuzzTunables
	budget    time.Duration
	start     time.Time
	overheads []time.Duration
}

func newFuzzPlanner(tun fuzzTunables, budget time.Duration, start time.Time) *fuzzPlanner {
	return &fuzzPlanner{tun: tun, budget: budget, start: start}
}

// remaining is what is left of the budget at now.
func (p *fuzzPlanner) remaining(now time.Time) time.Duration {
	return p.start.Add(p.budget).Sub(now)
}

// overhead is the per-target overhead the next allotment allows for: the
// estimate until enough targets have been measured, then their median.
func (p *fuzzPlanner) overhead() time.Duration {
	if len(p.overheads) < fuzzOverheadSamples {
		return p.tun.Overhead
	}
	sorted := append([]time.Duration(nil), p.overheads...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

// allot is the fuzz time of the next target, when targetsLeft (it included)
// remain at now: an equal share of what remains, less the overhead, in whole
// seconds and never under the minimum. It is false when the budget left does
// not cover the minimum and the overhead, and then the target is not run:
// starting it would end the lane past its budget.
func (p *fuzzPlanner) allot(now time.Time, targetsLeft int) (time.Duration, bool) {
	rest, overhead := p.remaining(now), p.overhead()
	if targetsLeft < 1 || rest < p.tun.Min+overhead {
		return 0, false
	}
	fuzz := (rest/time.Duration(targetsLeft) - overhead).Truncate(time.Second)
	if fuzz < p.tun.Min {
		fuzz = p.tun.Min
	}
	return fuzz, true
}

// observe records what a target that ran to its end cost beyond its fuzz time.
func (p *fuzzPlanner) observe(elapsed, fuzzed time.Duration) {
	if elapsed < fuzzed {
		return
	}
	p.overheads = append(p.overheads, elapsed-fuzzed)
}

// fuzzPlan says how the lane spends its time: a budget shared by the targets,
// or a fixed -fuzztime for each (the local knob, and an iteration count).
type fuzzPlan struct {
	Budget time.Duration
	Fixed  string
}

// runFuzzLane fuzzes the targets in order and returns the lane's ledger. The
// clock and the runner are injected: the lane itself neither sleeps nor
// fuzzes. start is when the lane began, so the budget includes what the lane
// did before its first target. Each entry records the fuzz time the target
// was given (fuzz_seconds) beside the seconds it took; the log line says the
// same and what is left of the budget.
func runFuzzLane(targets []testKey, plan fuzzPlan, start time.Time, clock func() time.Time, run func(k testKey, fuzztime string) entry, log io.Writer) ledger {
	l := ledger{Suite: suiteFuzz, Problems: []string{}}
	if len(targets) == 0 {
		l.Problems = append(l.Problems, "no Fuzz target found: a lane that fuzzed nothing proves nothing")
	}
	var planner *fuzzPlanner
	if plan.Fixed == "" {
		planner = newFuzzPlanner(fuzzTuning, plan.Budget, start)
		fmt.Fprintf(log, "fuzz: %d targets share a budget of %s: an equal part of what remains each, less %s of overhead, never under %s\n",
			len(targets), plan.Budget, fuzzTuning.Overhead, fuzzTuning.Min)
	}
	for i, k := range targets {
		fuzztime := plan.Fixed
		var allotted time.Duration
		if planner != nil {
			var ok bool
			if allotted, ok = planner.allot(clock(), len(targets)-i); !ok {
				e := entry{Package: k.Package, Test: k.Test, Job: suiteFuzz, Outcome: outcomeFail,
					Reason: fmt.Sprintf("not fuzzed: the lane's budget of %s was spent before this target could start with its minimum %s and the expected overhead %s", plan.Budget, fuzzTuning.Min, planner.overhead())}
				l.Summary.Expected++
				l.Summary.Fail++
				l.Problems = append(l.Problems, fmt.Sprintf("%s failed: %s", k, e.Reason))
				fmt.Fprintf(log, "fuzz: %s: %s (%s)\n", k, e.Outcome, e.Reason)
				l.Tests = append(l.Tests, e)
				continue
			}
			fuzztime = allotted.String()
		} else if d, err := time.ParseDuration(fuzztime); err == nil {
			allotted = d
		}
		began := clock()
		e := run(k, fuzztime)
		elapsed := clock().Sub(began)
		e.Seconds = elapsed.Seconds()
		e.FuzzSeconds = allotted.Seconds()
		l.Summary.Expected++
		switch e.Outcome {
		case outcomePass:
			l.Summary.Pass++
			if planner != nil {
				planner.observe(elapsed, allotted)
			}
		default:
			l.Summary.Fail++
			l.Problems = append(l.Problems, fmt.Sprintf("%s failed: %s", k, e.Reason))
		}
		line := fmt.Sprintf("fuzz: %s: %s (fuzz time %s, took %s", k, e.Outcome, fuzztime, elapsed.Round(time.Second))
		if planner != nil {
			line += fmt.Sprintf("; %d left, %s of the budget remain, overhead taken as %s", len(targets)-i-1, planner.remaining(clock()).Round(time.Second), planner.overhead())
		}
		fmt.Fprintln(log, line+")")
		l.Tests = append(l.Tests, e)
	}
	return l
}

var fuzzIterations = regexp.MustCompile(`^[1-9][0-9]*x$`)

// checkFuzzTime accepts what go test -fuzztime accepts and the lane can bound:
// a positive duration, or an iteration count such as 100x.
func checkFuzzTime(s string) error {
	if fuzzIterations.MatchString(s) {
		return nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("-fuzztime %q is neither a duration (2m, 30s) nor an iteration count (100x)", s)
	}
	if d <= 0 {
		return fmt.Errorf("-fuzztime %q must be positive", s)
	}
	return nil
}

// fuzzDeadline bounds one target's whole run: the fuzzing itself, the build
// that precedes it and the minimization that follows a crasher.
func fuzzDeadline(fuzztime string) time.Duration {
	if d, err := time.ParseDuration(fuzztime); err == nil {
		return d + fuzzTargetTail
	}
	return 30 * time.Minute
}

// parseFuzzTargets reads `go test -json -list ^Fuzz` output: one entry per
// Fuzz function of each package, in listing order, once.
func parseFuzzTargets(out []byte) []testKey {
	_, all := parseJSONListing(out)
	var targets []testKey
	for _, k := range all {
		if strings.HasPrefix(k.Test, "Fuzz") {
			targets = append(targets, k)
		}
	}
	return targets
}

// fuzzSlug names a package for a file path.
func fuzzSlug(pkg string) string {
	return strings.NewReplacer("/", "_", ".", "_", ":", "_").Replace(pkg)
}

// fuzzCorpus is the files of a target's testdata/fuzz directory.
func fuzzCorpus(dir string) map[string]bool {
	files := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return files
	}
	for _, e := range entries {
		if !e.IsDir() {
			files[e.Name()] = true
		}
	}
	return files
}

// cmdFuzz is `proofrun fuzz`: the fuzz lane. It spends either a budget for the
// whole lane (-budget, the proofs' way) or a fixed time on each target
// (-fuzztime, the local knob and an iteration count), and exactly one of them.
func cmdFuzz(args []string) int {
	began := time.Now()
	fs := flag.NewFlagSet("fuzz", flag.ContinueOnError)
	out := fs.String("out", "", "directory for the ledger, each target's log and the crashers (required)")
	budget := fs.String("budget", "", "the lane's whole time, shared equally by its targets (a duration such as 105m); discovery counts against it")
	fuzztime := fs.String("fuzztime", "", "instead of -budget, how long each target fuzzes (go test -fuzztime: a duration or Nx)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "proofrun fuzz: -out is required")
		return 2
	}
	var plan fuzzPlan
	switch {
	case *budget != "" && *fuzztime != "":
		fmt.Fprintln(os.Stderr, "proofrun fuzz: give -budget or -fuzztime, not both")
		return 2
	case *budget == "" && *fuzztime == "":
		fmt.Fprintln(os.Stderr, "proofrun fuzz: give the lane's time: -budget DURATION, or -fuzztime for each target")
		return 2
	case *budget != "":
		d, err := parseFuzzBudget(*budget)
		if err != nil {
			fmt.Fprintln(os.Stderr, "proofrun fuzz:", err)
			return 2
		}
		plan.Budget = d
	default:
		if err := checkFuzzTime(*fuzztime); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun fuzz:", err)
			return 2
		}
		plan.Fixed = *fuzztime
	}
	patterns := fs.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	if err := os.MkdirAll(filepath.Join(*out, "fuzz"), 0o755); err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}

	targets, dirs, err := discoverFuzzTargets(patterns)
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Package != targets[j].Package {
			return targets[i].Package < targets[j].Package
		}
		return targets[i].Test < targets[j].Test
	})
	run := func(k testKey, fuzztime string) entry { return fuzzTarget(k, dirs[k.Package], fuzztime, *out) }
	l := runFuzzLane(targets, plan, began, time.Now, run, stdout)
	return writeLedgerAndReport(*out, l)
}

// discoverFuzzTargets lists the Fuzz functions of the packages and the
// directory of each package that has one.
func discoverFuzzTargets(patterns []string) ([]testKey, map[string]string, error) {
	list := exec.Command("go", append([]string{"test", "-json", "-list", "^Fuzz"}, patterns...)...)
	list.Dir = fuzzWorkDir
	var listErr bytes.Buffer
	list.Stderr = &listErr
	listOut, err := list.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("go test -list: %v\n%s%s", err, listOut, listErr.String())
	}
	targets := parseFuzzTargets(listOut)
	dirsCmd := exec.Command("go", append([]string{"list", "-f", "{{.ImportPath}}\t{{.Dir}}"}, patterns...)...)
	dirsCmd.Dir = fuzzWorkDir
	var dirsErr bytes.Buffer
	dirsCmd.Stderr = &dirsErr
	dirsOut, err := dirsCmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("go list: %v\n%s", err, dirsErr.String())
	}
	dirs := map[string]string{}
	for _, line := range strings.Split(string(dirsOut), "\n") {
		if pkg, dir, ok := strings.Cut(strings.TrimSpace(line), "\t"); ok {
			dirs[pkg] = dir
		}
	}
	return targets, dirs, nil
}

// fuzzTarget fuzzes one target and reports it as a ledger entry. Its output
// goes to <out>/fuzz/<package>.<target>.log; a crasher the engine wrote into
// the package is copied to <out>/crashers/<package>/<target>/ and removed.
func fuzzTarget(k testKey, dir, fuzztime, out string) entry {
	e := entry{Package: k.Package, Test: k.Test, Job: suiteFuzz, Outcome: outcomePass}
	logPath := filepath.Join(out, "fuzz", fuzzSlug(k.Package)+"."+k.Test+".log")
	if dir == "" {
		// Without the directory the crasher cannot be found, and a lane that
		// cannot keep one must not report a pass.
		e.Outcome, e.Reason = outcomeFail, "go list gave no directory for its package"
		return e
	}
	corpus := filepath.Join(dir, "testdata", "fuzz", k.Test)
	before := fuzzCorpus(corpus)

	ctx, cancel := context.WithTimeout(context.Background(), fuzzDeadline(fuzztime))
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-run", "^$", "-fuzz", "^"+k.Test+"$", "-fuzztime", fuzztime, k.Package)
	cmd.Dir = fuzzWorkDir
	logFile, err := os.Create(logPath)
	if err != nil {
		e.Outcome, e.Reason = outcomeFail, "cannot create its log: "+err.Error()
		return e
	}
	defer logFile.Close()
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.WaitDelay = 30 * time.Second
	runErr := cmd.Run()

	var crashers []string
	for name := range fuzzCorpus(corpus) {
		if !before[name] {
			crashers = append(crashers, name)
		}
	}
	sort.Strings(crashers)
	var kept []string
	for _, name := range crashers {
		dst, err := keepCrasher(filepath.Join(corpus, name), filepath.Join(out, "crashers", fuzzSlug(k.Package), k.Test, name))
		if err != nil {
			e.Outcome = outcomeFail
			e.Reason = "a crasher was found and could not be kept: " + err.Error()
			return e
		}
		kept = append(kept, dst)
	}
	if len(crashers) > 0 {
		// Only what this run made: the directories go if they are empty.
		for _, d := range []string{corpus, filepath.Dir(corpus), filepath.Dir(filepath.Dir(corpus))} {
			_ = os.Remove(d)
		}
	}
	switch {
	case ctx.Err() != nil:
		e.Outcome = outcomeFail
		e.Reason = fmt.Sprintf("the target did not finish within %s of its %s", fuzzDeadline(fuzztime), fuzztime)
	case len(kept) > 0:
		e.Outcome = outcomeFail
		e.Reason = fmt.Sprintf("crasher kept at %s (the log is %s)", strings.Join(kept, ", "), logPath)
	case runErr != nil:
		e.Outcome = outcomeFail
		e.Reason = fmt.Sprintf("go test -fuzz: %v (the log is %s)", runErr, logPath)
	}
	return e
}

// keepCrasher copies src to dst and removes src.
func keepCrasher(src, dst string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	f, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, in); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(src); err != nil {
		return "", err
	}
	return dst, nil
}
