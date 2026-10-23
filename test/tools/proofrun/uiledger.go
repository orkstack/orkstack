package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The Playwright ledger is the UI suite's equivalent of the Go ledgers: the
// JSON reporter's outcomes checked against `playwright test --list`. Every
// listed test needs an outcome, and a skip, a flaky result or a failure fails
// the gate (retries stay 0). It also carries how many navigations the
// network-change guard (ui/e2e/test.ts) repeated, which has a ceiling.

type pwReport struct {
	Suites []pwSuite  `json:"suites"`
	Errors []pwError  `json:"errors"`
	Stats  *pwStatsJS `json:"stats,omitempty"`
}

type pwStatsJS struct {
	Expected   int `json:"expected"`
	Unexpected int `json:"unexpected"`
	Flaky      int `json:"flaky"`
	Skipped    int `json:"skipped"`
}

type pwError struct {
	Message string `json:"message"`
}

type pwSuite struct {
	Title  string    `json:"title"`
	File   string    `json:"file"`
	Specs  []pwSpec  `json:"specs"`
	Suites []pwSuite `json:"suites"`
}

type pwSpec struct {
	Title string   `json:"title"`
	File  string   `json:"file"`
	Tests []pwTest `json:"tests"`
}

type pwTest struct {
	ProjectName string     `json:"projectName"`
	Status      string     `json:"status"`
	Results     []pwResult `json:"results"`
}

type pwResult struct {
	ParallelIndex int      `json:"parallelIndex"`
	Status        string   `json:"status"`
	Duration      float64  `json:"duration"`
	StartTime     string   `json:"startTime,omitempty"`
	Error         *pwError `json:"error,omitempty"`
}

type uiEntry struct {
	File    string  `json:"file"`
	Title   string  `json:"title"`
	Project string  `json:"project"`
	Outcome string  `json:"outcome"`
	Seconds float64 `json:"seconds"`
	// Start is when the test began, in seconds from the earliest test of the
	// report; zero for a test the report has no start time for.
	Start  float64 `json:"start"`
	Detail string  `json:"detail,omitempty"`

	// abs and endAbs are the test's first start and last end as Unix seconds,
	// zero when the report carries no start time.
	abs, endAbs float64
	// lane is the Playwright worker lane (the result's parallelIndex) the
	// test ran in: the gap before a test is measured against the tests of
	// its own lane only, so a wall stays a file's own with several workers.
	lane int
}

// uiFileEntry is one spec file's measured cost: its tests' seconds, and its
// wall, which adds the gap before each of its tests (plan §1.3 F15).
type uiFileEntry struct {
	File        string  `json:"file"`
	Shard       int     `json:"shard"`
	Tests       int     `json:"tests"`
	Seconds     float64 `json:"seconds"`
	WallSeconds float64 `json:"wall_seconds"`
}

type uiLedger struct {
	Suite             string             `json:"suite"`
	Tests             []uiEntry          `json:"tests"`
	Problems          []string           `json:"problems"`
	Listed            int                `json:"listed"`
	Pass              int                `json:"pass"`
	GuardRetries      int                `json:"network_change_retries"`
	GuardRetryCeiling int                `json:"network_change_retry_ceiling"`
	Files             []uiFileEntry      `json:"files,omitempty"`
	HostSpeedFailures []hostSpeedFailure `json:"host_speed_failures,omitempty"`
	KnownGaps         []string           `json:"known_gaps,omitempty"`
}

// mergeReports joins two JSON reports of one run made as two invocations:
// the suites and errors of both, and the stats of the first that has any (a
// proof reads start times from each result, never from the stats).
func mergeReports(a, b pwReport) pwReport {
	a.Suites = append(a.Suites, b.Suites...)
	a.Errors = append(a.Errors, b.Errors...)
	if a.Stats == nil {
		a.Stats = b.Stats
	}
	return a
}

// flattenPW walks a report into "file › describe › title [project]" keys.
func flattenPW(r pwReport) (keys []string, tests map[string]uiEntry) {
	tests = map[string]uiEntry{}
	var walk func(s pwSuite, file string, titles []string)
	walk = func(s pwSuite, file string, titles []string) {
		if s.File != "" {
			file = s.File
		}
		for _, sp := range s.Specs {
			path := append(append([]string{}, titles...), sp.Title)
			for _, t := range sp.Tests {
				key := fmt.Sprintf("%s › %s [%s]", file, strings.Join(path, " › "), t.ProjectName)
				e := uiEntry{File: file, Title: strings.Join(path, " › "), Project: t.ProjectName, Outcome: outcomeNone}
				if len(t.Results) > 0 {
					var ms float64
					for _, res := range t.Results {
						ms += res.Duration
					}
					e.Seconds = ms / 1000
					e.lane = t.Results[0].ParallelIndex
					if first, err := time.Parse(time.RFC3339Nano, t.Results[0].StartTime); err == nil {
						last := t.Results[len(t.Results)-1]
						e.abs = float64(first.UnixNano()) / 1e9
						e.endAbs = e.abs + ms/1000
						if lastStart, err := time.Parse(time.RFC3339Nano, last.StartTime); err == nil {
							e.endAbs = float64(lastStart.UnixNano())/1e9 + last.Duration/1000
						}
					}
					switch t.Status {
					case "expected":
						e.Outcome = outcomePass
						if t.Results[len(t.Results)-1].Status == "skipped" {
							e.Outcome = outcomeSkip
						}
					case "skipped":
						e.Outcome = outcomeSkip
					case "flaky":
						e.Outcome = outcomeFail
						e.Detail = "flaky: it failed before it passed"
						if msg := t.Results[0].Error; msg != nil && msg.Message != "" {
							e.Detail = "flaky: " + strings.ReplaceAll(msg.Message, "\n", "; ")
						}
					default:
						e.Outcome = outcomeFail
						e.Detail = t.Results[len(t.Results)-1].Status
						if msg := t.Results[len(t.Results)-1].Error; msg != nil && msg.Message != "" {
							e.Detail = strings.ReplaceAll(msg.Message, "\n", "; ")
						}
					}
				} else if t.Status == "skipped" {
					e.Outcome = outcomeSkip
				}
				if _, dup := tests[key]; !dup {
					keys = append(keys, key)
				}
				tests[key] = e
			}
		}
		for _, sub := range s.Suites {
			walk(sub, file, append(append([]string{}, titles...), sub.Title))
		}
	}
	for _, s := range r.Suites {
		walk(s, s.File, nil)
	}
	return keys, tests
}

// buildUILedger checks a report against the listing.
func buildUILedger(list, report pwReport, guardRetries, ceiling int) uiLedger {
	l := uiLedger{Suite: "ui-e2e", Problems: []string{}, GuardRetries: guardRetries, GuardRetryCeiling: ceiling}
	listedKeys, listed := flattenPW(list)
	_, got := flattenPW(report)
	l.Listed = len(listedKeys)
	for _, e := range report.Errors {
		l.Problems = append(l.Problems, "Playwright reported an error outside any test: "+firstLine(e.Message))
	}
	for _, k := range listedKeys {
		e, ok := got[k]
		if !ok {
			// The listing names the test's file, title and project; the key's
			// "[project]" suffix is not part of the title.
			le := listed[k]
			e = uiEntry{File: le.File, Title: le.Title, Project: le.Project, Outcome: outcomeNone}
		}
		switch e.Outcome {
		case outcomePass:
			l.Pass++
		case outcomeSkip:
			l.Problems = append(l.Problems, k+" skipped; a skip is never a pass")
		case outcomeFail:
			l.Problems = append(l.Problems, k+" failed ("+orDash(e.Detail)+")")
		default:
			l.Problems = append(l.Problems, k+" has no outcome")
		}
		l.Tests = append(l.Tests, e)
	}
	setStarts(l.Tests)
	l.Files = fileWalls(l.Tests)
	if guardRetries > ceiling {
		l.Problems = append(l.Problems, fmt.Sprintf(
			"network churn above ceiling; evidence taken on a noisy host: the network-change guard repeated %d navigations, the ceiling is %d",
			guardRetries, ceiling))
	}
	return l
}

// setStarts gives each timed entry its start in seconds from the earliest.
func setStarts(entries []uiEntry) {
	var min float64
	for _, e := range entries {
		if e.abs > 0 && (min == 0 || e.abs < min) {
			min = e.abs
		}
	}
	for i := range entries {
		if entries[i].abs > 0 {
			entries[i].Start = entries[i].abs - min
		}
	}
}

// fileWalls computes each spec file's wall. Within each worker lane, tests
// are taken in start order, and the gap before a test — its start minus the
// end of the latest-ending test before it in that lane (never negative, none
// for the lane's first) — is the file of the test that follows it. Page
// start, per-file fixtures and beforeAll happen in those gaps, so a file
// costs more than the sum of its tests. Lanes are kept apart because with
// several workers another lane's test is what usually ended last, and a
// gap measured against it would be most of a file's beforeAll lost.
func fileWalls(entries []uiEntry) []uiFileEntry {
	byFile := map[string]*uiFileEntry{}
	lanes := map[int][]uiEntry{}
	for _, e := range entries {
		f := byFile[e.File]
		if f == nil {
			f = &uiFileEntry{File: e.File}
			byFile[e.File] = f
		}
		f.Tests++
		f.Seconds += e.Seconds
		f.WallSeconds += e.Seconds
		if e.abs > 0 {
			lanes[e.lane] = append(lanes[e.lane], e)
		}
	}
	for _, timed := range lanes {
		sort.SliceStable(timed, func(i, j int) bool { return timed[i].abs < timed[j].abs })
		var latestEnd float64
		for i, e := range timed {
			if i > 0 && e.abs > latestEnd {
				byFile[e.File].WallSeconds += e.abs - latestEnd
			}
			if e.endAbs > latestEnd {
				latestEnd = e.endAbs
			}
		}
	}
	out := make([]uiFileEntry, 0, len(byFile))
	for _, f := range byFile {
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].File < out[j].File })
	return out
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

var ceilingRE = regexp.MustCompile(`NETWORK_CHANGE_RETRY_CEILING\s*=\s*(\d+)`)

// retryCeiling reads the ceiling from the Playwright config.
func retryCeiling(config string) (int, error) {
	b, err := os.ReadFile(config)
	if err != nil {
		return 0, err
	}
	m := ceilingRE.FindSubmatch(b)
	if m == nil {
		return 0, fmt.Errorf("%s declares no NETWORK_CHANGE_RETRY_CEILING", config)
	}
	return strconv.Atoi(string(m[1]))
}

// guardMarker is what ui/e2e/test.ts prints for each repeated navigation.
const guardMarker = "navigating again (attempt"

func countGuardRetries(log string) (int, error) {
	f, err := os.Open(log)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		if strings.Contains(sc.Text(), guardMarker) {
			n++
		}
	}
	return n, sc.Err()
}

func cmdUILedger(args []string) int {
	fs := flag.NewFlagSet("ui-ledger", flag.ContinueOnError)
	listFile := fs.String("list", "", "`playwright test --list --reporter=json` output")
	var reportFiles []string
	fs.Func("report", "the JSON reporter's output of the run; repeatable, when a proof ran the suite as several invocations (the parallel project, then the solo projects) that together are one run", func(v string) error {
		reportFiles = append(reportFiles, v)
		return nil
	})
	logFile := fs.String("log", "", "the run's log, where the guard prints its repeats")
	config := fs.String("config", "ui/playwright.config.ts", "where NETWORK_CHANGE_RETRY_CEILING is declared")
	out := fs.String("out", "", "the proof directory")
	shard := fs.Int("shard", 1, "which shard of the UI plan this run is, recorded in files[].shard")
	hostSpeed := fs.String("host-speed", "scripts/proof-host-speed.tsv", "declared runner-size budgets")
	hostSpeedBase := fs.String("host-speed-base", "", "the base's copy of the host-speed table (only rows it carries excuse)")
	part := fs.String("part", "local", "the part this ledger belongs to: only a CI ui-e2e:K part may excuse a runner-size miss")
	if err := fs.Parse(args); err != nil || *listFile == "" || len(reportFiles) == 0 || *out == "" {
		fmt.Fprintln(os.Stderr, "proofrun ui-ledger -list FILE -report FILE [-report FILE]... -log FILE -out DIR")
		return 2
	}
	var list, report pwReport
	if err := readJSON(*listFile, &list); err != nil {
		fmt.Println("ui-e2e ledger: the listing is unreadable:", err)
		return 1
	}
	for _, f := range reportFiles {
		var part pwReport
		if err := readJSON(f, &part); err != nil {
			fmt.Println("ui-e2e ledger: the report is unreadable:", err)
			return 1
		}
		report = mergeReports(report, part)
	}
	ceiling, err := retryCeiling(*config)
	if err != nil {
		fmt.Println("ui-e2e ledger:", err)
		return 1
	}
	retries := 0
	if *logFile != "" {
		if retries, err = countGuardRetries(*logFile); err != nil {
			fmt.Println("ui-e2e ledger:", err)
			return 1
		}
	}
	l := buildUILedger(list, report, retries, ceiling)
	for i := range l.Files {
		l.Files[i].Shard = *shard
	}
	// A declared runner-size miss of a CI UI shard exits the shard cleanly and
	// is recorded in this part's ledger; the merger re-derives every one of
	// them and never takes the shard's word for it. Only a failed timing
	// assertion of a bound the base also declares is excused here; a skip, a
	// missing outcome or any other failure stays a shard failure, and the
	// local part never excuses at all (hostspeed.go).
	tree, err := readHostSpeed(*hostSpeed)
	if err != nil {
		fmt.Println("ui-e2e ledger:", err)
		return 1
	}
	base, err := readHostSpeed(*hostSpeedBase)
	if err != nil {
		fmt.Println("ui-e2e ledger: the base's host-speed table excuses nothing:", err)
		base = emptyHostSpeed()
	}
	declared := anchoredHostSpeed(tree, base)
	cpus := runtime.NumCPU()
	excused := map[string]hostSpeedFailure{}
	for _, e := range l.Tests {
		if f, ok := declared.uiRunnerSizeMiss(*part, cpus, e); ok {
			excused[f.Message] = f
		}
	}
	var kept []string
	for _, p := range l.Problems {
		if f, ok := excused[p]; ok {
			l.HostSpeedFailures = append(l.HostSpeedFailures, f)
			l.KnownGaps = append(l.KnownGaps, p)
			fmt.Printf("ui-e2e known gap: %s on a %d-CPU runner (bound %.0fs, reference %d CPUs)\n", p, cpus, f.Bound, f.ReferenceCPUs)
			continue
		}
		kept = append(kept, p)
	}
	l.Problems = kept
	if l.Problems == nil {
		l.Problems = []string{}
	}
	if err := writeJSON(filepath.Join(*out, "ui-e2e-tests.json"), l); err != nil {
		fmt.Println(err)
		return 1
	}
	fmt.Printf("ui-e2e ledger: %d listed, %d pass, network-change guard repeated %d navigations (ceiling %d)\n",
		l.Listed, l.Pass, l.GuardRetries, l.GuardRetryCeiling)
	for _, p := range l.Problems {
		fmt.Println("  -", p)
	}
	if len(l.Problems) > 0 {
		return 1
	}
	return 0
}
