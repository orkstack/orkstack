package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Planning turns "every test of these packages" into jobs: a test that took
// slowThreshold or more in the last measured full proof is a job of its own,
// faster tests are batched up to batchTarget seconds of work, and the jobs run
// longest-first so the slowest test starts at once and bounds the phase.

const (
	// slowThreshold is the measured duration at which a test gets a process
	// of its own, and the default for the slow list.
	slowThreshold = 10.0
	// batchTarget is how much measured work one batch process carries.
	batchTarget = 150.0
	// unmeasuredCost is what a test with no measurement, declared in an
	// integration-tagged file, is assumed to take: the conservative default,
	// above all but a handful of measured integration tests. It runs in a
	// process of its own, so a new slow test can never stretch a batch.
	unmeasuredCost = 300.0
	// unmeasuredUnitCost is the same for a test declared in a file the unit
	// build compiles too: the integration binary lists those, and they are
	// fast as a rule.
	unmeasuredUnitCost = 30.0
	// minJobTimeout is the floor of every job's -test.timeout.
	minJobTimeout = 10 * time.Minute
	// unknownTimeout bounds a job whose test was never measured.
	unknownTimeout = 30 * time.Minute
)

// costs maps a test to its measured seconds.
type costs map[testKey]float64

// readCosts parses scripts/proof-test-costs.tsv: "package  test  seconds",
// '#' comments ignored; the source proof is named in its header. A missing file is no measurements at all.
func readCosts(file string) (costs, error) {
	c := costs{}
	if file == "" {
		return c, nil
	}
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			return nil, fmt.Errorf("%s:%d: want \"package<TAB>test<TAB>seconds\"", file, n)
		}
		s, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: seconds: %v", file, n, err)
		}
		c[testKey{fields[0], fields[1]}] = s
	}
	return c, sc.Err()
}

// job is one test-binary process.
type job struct {
	ID      string        `json:"id"`
	Package string        `json:"package"`
	Tests   []string      `json:"tests"`
	Cost    float64       `json:"estimated_seconds"`
	Known   bool          `json:"measured"`
	Timeout time.Duration `json:"-"`
	// TimeoutSeconds is Timeout as the plan file records it.
	TimeoutSeconds float64 `json:"timeout_seconds"`
	// Shard is the 1-based shard that runs the job.
	Shard int `json:"shard"`
	// Heavy marks a test whose assertions are absolute wall-clock or
	// throughput bounds. Heavy jobs run one at a time even on a multi-slot
	// shard: they share one Temporal and one object store, and their own load
	// moves the GetRun latency thresholds they hold (plan §2.3, §3.6).
	Heavy bool `json:"heavy,omitempty"`
	// Attempt counts the processes this job's tests needed: a crash runs the
	// tests it had not started in a fresh process (attempt 2, 3, ...).
	Attempt int `json:"-"`
}

// plan builds the jobs for tests (package -> names in listing order). kinds
// says, per package and test, whether an unmeasured test is a unit test
// ("unit") or an integration test (anything else), which decides its cost.
func plan(tests map[string][]string, measured costs, kinds map[string]map[string]string) []job {
	var jobs []job
	pkgs := make([]string, 0, len(tests))
	for p := range tests {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)
	for _, p := range pkgs {
		var batch []string
		var batchCost float64
		flush := func() {
			if len(batch) > 0 {
				jobs = append(jobs, job{Package: p, Tests: batch, Cost: batchCost, Known: true})
			}
			batch, batchCost = nil, 0
		}
		for _, name := range tests[p] {
			s, known := measured[testKey{p, name}]
			switch {
			case !known:
				cost := unmeasuredCost
				if kinds[p][name] == "unit" {
					cost = unmeasuredUnitCost
				}
				jobs = append(jobs, job{Package: p, Tests: []string{name}, Cost: cost,
					Heavy: heavyByName(name)})
			case s >= slowThreshold:
				jobs = append(jobs, job{Package: p, Tests: []string{name}, Cost: s, Known: true, Heavy: heavyByName(name)})
			default:
				if len(batch) > 0 && batchCost+s > batchTarget {
					flush()
				}
				batch = append(batch, name)
				batchCost += s
			}
		}
		flush()
	}
	sortJobs(jobs)
	for i := range jobs {
		jobs[i].ID = fmt.Sprintf("%s-%03d", path.Base(jobs[i].Package), i+1)
		jobs[i].Timeout = jobTimeout(jobs[i])
		jobs[i].TimeoutSeconds = jobs[i].Timeout.Seconds()
		jobs[i].Attempt = 1
		jobs[i].Shard = 1
	}
	return jobs
}

// heavyTests are the tests whose assertions are absolute wall-clock or
// throughput bounds, named explicitly. A test that asserts such a bound is
// heavy whether or not it is slow — the reduced-ceiling copies of a moved
// soak run the same E4/E7 harness and are heavy for that reason. A test that
// is merely slow, or that waits on a deadline for progress, is not: duration
// says nothing about what it asserts, and serialising every slow test turned
// the plan into hours of one-slot work. A test that needs isolation for
// another reason is added here with that reason.
var heavyTests = map[string]bool{
	// The six the user moved to the nightly tier, each with GetRun latency,
	// snapshot-byte or E7/E4 thresholds measured on a quiet host.
	"TestIntegration_SoakTasks":                              true,
	"TestIntegration_ThreeThousandTaskDefinitionRuns":        true,
	"TestIntegration_SnapshotOverlayBytesForAThousandTasks":  true,
	"TestIntegration_SoakItemCeilingByReferenceWithOverlays": true,
	"TestIntegration_SoakFanOutMaxParallel":                  true,
	"TestIntegration_SoakFanOut":                             true,
	// Their merge-gate equivalents: the reduced-ceiling copies run the same
	// E4/E7 thresholds through the soak helpers.
	"TestIntegration_ATaskCeilingRunCarriesOnAtAReducedCeiling":      true,
	"TestIntegration_AnItemCeilingRunCarriesOnAtAReducedCeiling":     true,
	"TestIntegration_AFanOutAtAReducedItemCeilingMeetsItsThresholds": true,
}

func heavyByName(name string) bool { return heavyTests[name] }

// jobTimeout is max(10m, 3x the measured cost), or 30m for a test never
// measured: generous enough that load never trips it, tight enough that a hung
// test cannot hold a slot for the whole proof.
func jobTimeout(j job) time.Duration {
	if !j.Known {
		return unknownTimeout
	}
	d := time.Duration(3 * j.Cost * float64(time.Second))
	if d < minJobTimeout {
		return minJobTimeout
	}
	return d.Round(time.Second)
}

// runPattern is the -test.run value that selects exactly names: anchored, so
// TestA never also runs TestAB, and with every name quoted.
func runPattern(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = regexp.QuoteMeta(n)
	}
	return "^(?:" + strings.Join(q, "|") + ")$"
}

// costLine is one row of the cost ledger.
type costLine struct {
	Package string
	Test    string
	Seconds float64
}

// costsFromLedger reads a full proof's integration ledger. A skipped test is
// measured too (it takes no time), so it is batched rather than given a
// process of its own as an unknown would be.
func costsFromLedger(l ledger) []costLine {
	var out []costLine
	for _, e := range l.Tests {
		if e.Outcome == outcomePass || e.Outcome == outcomeFail || e.Outcome == outcomeSkip {
			out = append(out, costLine{e.Package, e.Test, e.Seconds})
		}
	}
	return out
}

var (
	goTestTop    = regexp.MustCompile(`^--- (PASS|FAIL|SKIP): (\S+) \(([0-9.]+)s\)`)
	goTestResult = regexp.MustCompile(`^(ok|FAIL)\s+(\S+)\s`)
)

// costsFromGoTestLog reads the text of `go test -p 1 -v ./...` — the format of
// every proof before ledgers existed — attributing each top-level result to
// the package whose "ok"/"FAIL" line follows it. With -p 1 the packages'
// output is never interleaved, which is what makes that attribution exact.
func costsFromGoTestLog(r io.Reader) ([]costLine, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	var pending []costLine
	var out []costLine
	for sc.Scan() {
		line := sc.Text()
		if m := goTestTop.FindStringSubmatch(line); m != nil {
			s, _ := strconv.ParseFloat(m[3], 64)
			pending = append(pending, costLine{Test: m[2], Seconds: s})
			continue
		}
		if m := goTestResult.FindStringSubmatch(line); m != nil && strings.Contains(m[2], "/") {
			for _, c := range pending {
				c.Package = m[2]
				out = append(out, c)
			}
			pending = nil
		}
	}
	return out, sc.Err()
}

// writeCosts writes the cost ledger, sorted by package then test.
func writeCosts(w io.Writer, lines []costLine, source string) {
	sort.Slice(lines, func(i, j int) bool {
		if lines[i].Package != lines[j].Package {
			return lines[i].Package < lines[j].Package
		}
		return lines[i].Test < lines[j].Test
	})
	fmt.Fprintf(w, "# Measured duration of every integration test.\n")
	fmt.Fprintf(w, "# source: %s\n", source)
	fmt.Fprintf(w, "# Regenerated by `make proof-slow-list FROM=proof/<name>`; read by test/tools/proofrun.\n")
	fmt.Fprintf(w, "# package\ttest\tseconds\n")
	for _, c := range lines {
		fmt.Fprintf(w, "%s\t%s\t%.2f\n", c.Package, c.Test, c.Seconds)
	}
}

// writeSlowList writes every test at or above threshold.
func writeSlowList(w io.Writer, lines []costLine, threshold float64, source string) {
	sort.Slice(lines, func(i, j int) bool {
		if lines[i].Package != lines[j].Package {
			return lines[i].Package < lines[j].Package
		}
		return lines[i].Test < lines[j].Test
	})
	fmt.Fprintf(w, "# Integration tests a feature proof defers to the merge proof: every test that\n")
	fmt.Fprintf(w, "# took %.0fs or more in the source proof.\n", threshold)
	fmt.Fprintf(w, "# source: %s\n", source)
	fmt.Fprintf(w, "# Regenerated by `make proof-slow-list FROM=proof/<name>`. A test the diff changes\n")
	fmt.Fprintf(w, "# or --tests names always runs; every deferred test is named in the README.\n")
	fmt.Fprintf(w, "# package\ttest\tseconds\n")
	for _, c := range lines {
		if c.Seconds >= threshold {
			fmt.Fprintf(w, "%s\t%s\t%.2f\n", c.Package, c.Test, c.Seconds)
		}
	}
}

// slowEntry is one line of the slow list.
type slowEntry struct {
	Seconds float64
	Line    int
}

func readSlowList(file string) (map[testKey]slowEntry, string, error) {
	out := map[testKey]slowEntry{}
	f, err := os.Open(file)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	var source string
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		if strings.HasPrefix(line, "# source: ") {
			source = strings.TrimPrefix(line, "# source: ")
		}
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			return nil, "", fmt.Errorf("%s:%d: want \"package<TAB>test<TAB>seconds\"", file, n)
		}
		s, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return nil, "", fmt.Errorf("%s:%d: %v", file, n, err)
		}
		out[testKey{fields[0], fields[1]}] = slowEntry{s, n}
	}
	return out, source, sc.Err()
}

// totalCost is the estimated work of a plan.
func totalCost(jobs []job) float64 {
	var t float64
	for _, j := range jobs {
		t += j.Cost
	}
	return t
}

// makespan simulates the plan on slots: each job, in plan order, takes the
// slot that frees first. It is the estimate the phase is measured against.
func makespan(jobs []job, slots int) float64 {
	if slots < 1 {
		slots = 1
	}
	free := make([]float64, slots)
	var end float64
	for _, j := range jobs {
		k := 0
		for i := range free {
			if free[i] < free[k] {
				k = i
			}
		}
		free[k] += j.Cost
		if free[k] > end {
			end = free[k]
		}
	}
	return end
}
