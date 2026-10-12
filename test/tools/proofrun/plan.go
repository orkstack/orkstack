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
	// unknownCost is what a test with no measurement is assumed to take when
	// ordering jobs. It runs in a process of its own, so a new slow test can
	// never stretch a batch.
	unknownCost = 30.0
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
	// Attempt counts the processes this job's tests needed: a crash runs the
	// tests it had not started in a fresh process (attempt 2, 3, ...).
	Attempt int `json:"-"`
}

// plan builds the jobs for tests (package -> names in listing order).
func plan(tests map[string][]string, measured costs) []job {
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
				jobs = append(jobs, job{Package: p, Tests: []string{name}, Cost: unknownCost})
			case s >= slowThreshold:
				jobs = append(jobs, job{Package: p, Tests: []string{name}, Cost: s, Known: true})
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
	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Cost != jobs[j].Cost {
			return jobs[i].Cost > jobs[j].Cost
		}
		if jobs[i].Package != jobs[j].Package {
			return jobs[i].Package < jobs[j].Package
		}
		return jobs[i].Tests[0] < jobs[j].Tests[0]
	})
	for i := range jobs {
		jobs[i].ID = fmt.Sprintf("%s-%03d", path.Base(jobs[i].Package), i+1)
		jobs[i].Timeout = jobTimeout(jobs[i])
		jobs[i].Attempt = 1
	}
	return jobs
}

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

// costsFromLedger reads a full proof's integration ledger.
func costsFromLedger(l ledger) []costLine {
	var out []costLine
	for _, e := range l.Tests {
		if e.Outcome == outcomePass || e.Outcome == outcomeFail {
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
			if m[1] == "SKIP" {
				continue
			}
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
