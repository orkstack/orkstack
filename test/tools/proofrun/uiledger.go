package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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
	Status   string  `json:"status"`
	Duration float64 `json:"duration"`
}

type uiEntry struct {
	File    string  `json:"file"`
	Title   string  `json:"title"`
	Project string  `json:"project"`
	Outcome string  `json:"outcome"`
	Seconds float64 `json:"seconds"`
	Detail  string  `json:"detail,omitempty"`
}

type uiLedger struct {
	Suite             string    `json:"suite"`
	Tests             []uiEntry `json:"tests"`
	Problems          []string  `json:"problems"`
	Listed            int       `json:"listed"`
	Pass              int       `json:"pass"`
	GuardRetries      int       `json:"network_change_retries"`
	GuardRetryCeiling int       `json:"network_change_retry_ceiling"`
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
					default:
						e.Outcome = outcomeFail
						e.Detail = t.Results[len(t.Results)-1].Status
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
	listedKeys, _ := flattenPW(list)
	_, got := flattenPW(report)
	l.Listed = len(listedKeys)
	for _, e := range report.Errors {
		l.Problems = append(l.Problems, "Playwright reported an error outside any test: "+firstLine(e.Message))
	}
	for _, k := range listedKeys {
		e, ok := got[k]
		if !ok {
			e = uiEntry{Outcome: outcomeNone}
			parts := strings.SplitN(k, " › ", 2)
			e.File = parts[0]
			if len(parts) > 1 {
				e.Title = parts[1]
			}
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
	if guardRetries > ceiling {
		l.Problems = append(l.Problems, fmt.Sprintf(
			"network churn above ceiling; evidence taken on a noisy host: the network-change guard repeated %d navigations, the ceiling is %d",
			guardRetries, ceiling))
	}
	return l
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
	reportFile := fs.String("report", "", "the JSON reporter's output of the run")
	logFile := fs.String("log", "", "the run's log, where the guard prints its repeats")
	config := fs.String("config", "ui/playwright.config.ts", "where NETWORK_CHANGE_RETRY_CEILING is declared")
	out := fs.String("out", "", "the proof directory")
	if err := fs.Parse(args); err != nil || *listFile == "" || *reportFile == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "proofrun ui-ledger -list FILE -report FILE -log FILE -out DIR")
		return 2
	}
	var list, report pwReport
	if err := readJSON(*listFile, &list); err != nil {
		fmt.Println("ui-e2e ledger: the listing is unreadable:", err)
		return 1
	}
	if err := readJSON(*reportFile, &report); err != nil {
		fmt.Println("ui-e2e ledger: the report is unreadable:", err)
		return 1
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
