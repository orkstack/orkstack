package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// A ledger is the evidence that a suite ran what it claims to have run: one
// entry per test the suite was asked to run, with the job that ran it, its
// outcome and its duration, checked against the test binary's own listing
// (AGENTS.md §11.2). It is built from test2json events, never from scraping
// the human log, so a test that printed "--- PASS" inside its own output can
// not be mistaken for one that passed.

// Outcomes a ledger entry can carry. "none" is the absence of any outcome: the
// test was listed and asked for, and nothing reported it.
const (
	outcomePass = "pass"
	outcomeFail = "fail"
	outcomeSkip = "skip"
	outcomeNone = "none"
)

// event is one test2json record (go doc cmd/test2json).
type event struct {
	Time    string  `json:"Time,omitempty"`
	Action  string  `json:"Action"`
	Package string  `json:"Package,omitempty"`
	Test    string  `json:"Test,omitempty"`
	Elapsed float64 `json:"Elapsed,omitempty"`
	Output  string  `json:"Output,omitempty"`
}

// testKey names one test of one package. Test may be a subtest ("TestX/sub").
type testKey struct {
	Package string
	Test    string
}

func (k testKey) String() string { return k.Package + "." + k.Test }

// entry is one test's line in a ledger.
type entry struct {
	Package string  `json:"package"`
	Test    string  `json:"test"`
	Job     string  `json:"job,omitempty"`
	Outcome string  `json:"outcome"`
	Seconds float64 `json:"seconds"`
	// Reason says why an entry is a failure the test itself did not report:
	// its binary crashed while it ran, or nothing ever reported it.
	Reason string `json:"reason,omitempty"`
	// AllowedSkip is the allowlist's reason when the outcome is an allowed skip.
	AllowedSkip string `json:"allowed_skip,omitempty"`
}

// collector accumulates outcomes from test2json events.
type collector struct {
	results map[testKey]*entry
	started map[testKey]bool
	// order keeps first-seen order so a ledger reads like the run.
	order []testKey
	// subSkips are skipped subtests: a skip anywhere is a skip.
	subSkips []testKey
	// packageOutput keeps a package's own output (no Test), for diagnosis
	// when a binary dies before any test starts.
	packageOutput map[string]*strings.Builder
}

func newCollector() *collector {
	return &collector{
		results:       map[testKey]*entry{},
		started:       map[testKey]bool{},
		packageOutput: map[string]*strings.Builder{},
	}
}

// add records one event. job names the process that produced it.
func (c *collector) add(ev event, job string) {
	if ev.Test == "" {
		if ev.Action == "output" {
			b := c.packageOutput[ev.Package]
			if b == nil {
				b = &strings.Builder{}
				c.packageOutput[ev.Package] = b
			}
			if b.Len() < 64<<10 {
				b.WriteString(ev.Output)
			}
		}
		return
	}
	k := testKey{ev.Package, ev.Test}
	switch ev.Action {
	case "run":
		c.started[k] = true
		if strings.Contains(ev.Test, "/") {
			return
		}
		if _, ok := c.results[k]; !ok {
			c.results[k] = &entry{Package: ev.Package, Test: ev.Test, Job: job, Outcome: outcomeNone}
			c.order = append(c.order, k)
		}
	case "pass", "fail", "skip":
		if strings.Contains(ev.Test, "/") {
			if ev.Action == "skip" {
				c.subSkips = append(c.subSkips, k)
			}
			return
		}
		e, ok := c.results[k]
		if !ok {
			e = &entry{Package: ev.Package, Test: ev.Test}
			c.results[k] = e
			c.order = append(c.order, k)
		}
		e.Job = job
		e.Outcome = ev.Action
		e.Seconds = ev.Elapsed
		e.Reason = ""
	}
}

// readEvents feeds every JSON line of r to c, tagging each with job, and
// copies each line to raw when raw is not nil. A line that is not a JSON
// object is kept as package output: test2json passes such lines through only
// when its input was not a test binary's.
func readEvents(r io.Reader, c *collector, job string, raw io.Writer, text io.Writer, progress io.Writer) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if raw != nil {
			_, _ = raw.Write(append(append([]byte{}, line...), '\n'))
		}
		var ev event
		if len(line) == 0 || line[0] != '{' || json.Unmarshal(line, &ev) != nil {
			if text != nil {
				_, _ = text.Write(append(append([]byte{}, line...), '\n'))
			}
			continue
		}
		if text != nil && ev.Action == "output" {
			_, _ = io.WriteString(text, ev.Output)
		}
		if progress != nil && ev.Test != "" && !strings.Contains(ev.Test, "/") {
			switch ev.Action {
			case "run":
				fmt.Fprintf(progress, "proofrun: start %s %s\n", ev.Package, ev.Test)
			case "pass", "fail", "skip":
				fmt.Fprintf(progress, "proofrun: outcome %s %s %s %.2f\n", ev.Action, ev.Package, ev.Test, ev.Elapsed)
			}
		}
		c.add(ev, job)
	}
	return sc.Err()
}

// allowedSkip is one line of scripts/proof-allowed-skips.txt.
type allowedSkip struct {
	Suite   string // unit, integration or any
	Package string
	Test    string
	Reason  string
	Line    int
}

// readAllowlist parses the allowed-skip file: tab-separated
// "suite  package  test  reason", '#' comments and blank lines ignored. Every
// entry must carry a reason, because an allowed skip is a claim that this
// test is not evidence of anything — and that claim has to say why.
func readAllowlist(path string) ([]allowedSkip, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseAllowlist(f, path)
}

func parseAllowlist(r io.Reader, name string) ([]allowedSkip, error) {
	var out []allowedSkip
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(sc.Text(), "\t")
		if len(fields) < 4 || strings.TrimSpace(fields[3]) == "" {
			return nil, fmt.Errorf("%s:%d: want \"suite<TAB>package<TAB>test<TAB>reason\", got %q", name, n, sc.Text())
		}
		suite := strings.TrimSpace(fields[0])
		switch suite {
		case "unit", "integration", "any":
		default:
			return nil, fmt.Errorf("%s:%d: suite %q is not unit, integration or any", name, n, suite)
		}
		out = append(out, allowedSkip{
			Suite: suite, Package: strings.TrimSpace(fields[1]), Test: strings.TrimSpace(fields[2]),
			Reason: strings.TrimSpace(strings.Join(fields[3:], " ")), Line: n,
		})
	}
	return out, sc.Err()
}

// binaryRecord is one compiled test binary's digest and listing digest, as a
// shard records them (plan §2.4).
type binaryRecord struct {
	SHA256        string `json:"sha256"`
	ListingSHA256 string `json:"listing_sha256"`
}

// ledger is the checked result of one suite.
type ledger struct {
	Suite      string                  `json:"suite"`
	Shard      int                     `json:"shard,omitempty"`
	PlanSHA256 string                  `json:"plan_sha256,omitempty"`
	Binaries   map[string]binaryRecord `json:"binaries,omitempty"`
	Tests      []entry                 `json:"tests"`
	Problems   []string                `json:"problems"`
	Summary    summary                 `json:"summary"`
}

type summary struct {
	Expected     int `json:"expected"`
	Pass         int `json:"pass"`
	Fail         int `json:"fail"`
	AllowedSkip  int `json:"allowed_skip"`
	Skip         int `json:"skip"`
	NoOutcome    int `json:"no_outcome"`
	SubtestSkips int `json:"subtest_skips"`
	Deferred     int `json:"deferred"`
}

// ok reports whether the suite is evidence: every expected test reported a
// pass or an allowed skip, and nothing else is wrong.
func (l ledger) ok() bool { return len(l.Problems) == 0 }

// buildLedger checks the collected outcomes against what was expected.
//
// expected is every test the suite was asked to run, in order. listed is every
// test each compiled package reports (go test -list), which an allowlist entry
// is checked against: an entry naming a test its package does not have is
// stale, and a stale allowance is a failure, because it would silently allow
// the next test that happens to take its name.
func buildLedger(suite string, expected []testKey, listed map[string]map[string]bool, c *collector, allow []allowedSkip) ledger {
	l := ledger{Suite: suite, Problems: []string{}}
	allowed := map[testKey]allowedSkip{}
	for _, a := range allow {
		if a.Suite != suite && a.Suite != "any" {
			continue
		}
		allowed[testKey{a.Package, a.Test}] = a
		names, compiled := listed[a.Package]
		top := strings.SplitN(a.Test, "/", 2)[0]
		if compiled && !names[top] {
			l.Problems = append(l.Problems, fmt.Sprintf(
				"allowed-skip entry (line %d) names %s.%s, which its package does not list: remove the stale entry",
				a.Line, a.Package, a.Test))
		}
	}
	seen := map[testKey]bool{}
	for _, k := range expected {
		if seen[k] {
			continue
		}
		seen[k] = true
		l.Summary.Expected++
		e := entry{Package: k.Package, Test: k.Test, Outcome: outcomeNone, Reason: "no outcome: the test was listed and asked for, and nothing reported it"}
		if got, ok := c.results[k]; ok {
			e = *got
			if e.Outcome == outcomeNone && e.Reason == "" {
				e.Reason = "no outcome: the test started and never reported a result"
			}
		}
		switch e.Outcome {
		case outcomePass:
			l.Summary.Pass++
		case outcomeFail:
			l.Summary.Fail++
			why := "failed"
			if e.Reason != "" {
				why = e.Reason
			}
			l.Problems = append(l.Problems, fmt.Sprintf("%s %s (job %s)", k, why, orDash(e.Job)))
		case outcomeSkip:
			if a, ok := allowed[k]; ok {
				l.Summary.AllowedSkip++
				e.AllowedSkip = a.Reason
			} else {
				l.Summary.Skip++
				l.Problems = append(l.Problems, fmt.Sprintf(
					"%s skipped (job %s); a skip is never a pass — fix the prerequisite, or allow it in scripts/proof-allowed-skips.txt with its reason",
					k, orDash(e.Job)))
			}
		default:
			l.Summary.NoOutcome++
			l.Problems = append(l.Problems, fmt.Sprintf("%s %s (job %s)", k, e.Reason, orDash(e.Job)))
		}
		l.Tests = append(l.Tests, e)
	}
	for _, k := range c.subSkips {
		top := testKey{k.Package, strings.SplitN(k.Test, "/", 2)[0]}
		if !seen[top] {
			continue
		}
		if _, ok := allowed[k]; ok {
			continue
		}
		l.Summary.SubtestSkips++
		l.Problems = append(l.Problems, fmt.Sprintf(
			"%s skipped; a skip is never a pass — fix the prerequisite, or allow it in scripts/proof-allowed-skips.txt with its reason", k))
	}
	return l
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// listingName matches the names `-test.list` prints that are run as tests.
// Benchmarks are listed too but never run without -test.bench.
var listingName = regexp.MustCompile(`^(Test|Example|Fuzz)[\p{L}\p{N}_]*$`)

// parseListing reads `-test.list` output: one name per line, then the
// package's "ok" line, which is not a name.
func parseListing(out string) []string {
	var names []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if listingName.MatchString(line) {
			names = append(names, line)
		}
	}
	return names
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
