package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// pkgInfo is one package as `go list` reports it.
type pkgInfo struct {
	ImportPath string
	Dir        string
}

// goList runs `go list` with format and returns its non-empty lines.
func goList(tags string, format string, patterns ...string) ([]string, error) {
	return goListIn("", tags, format, patterns...)
}

// goListIn runs `go list` in dir ("" is the working directory).
func goListIn(dir, tags string, format string, patterns ...string) ([]string, error) {
	args := []string{"list"}
	if tags != "" {
		args = append(args, "-tags="+tags)
	}
	args = append(args, "-f", format)
	args = append(args, patterns...)
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	var lines []string
	for _, l := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	return lines, nil
}

// integrationPackages lists the packages whose build under -tags=integration
// differs from their unit build: the ones whose integration suite is not
// already the unit suite the unit gate ran.
func integrationPackages(patterns []string) ([]pkgInfo, error) {
	const format = `{{.ImportPath}}{{"\t"}}{{.Dir}}{{"\t"}}{{join .GoFiles ","}}|{{join .TestGoFiles ","}}|{{join .XTestGoFiles ","}}`
	tagged, err := goList("integration", format, patterns...)
	if err != nil {
		return nil, err
	}
	plain, err := goList("", format, patterns...)
	if err != nil {
		return nil, err
	}
	unitFiles := map[string]string{}
	for _, l := range plain {
		f := strings.SplitN(l, "\t", 3)
		if len(f) == 3 {
			unitFiles[f[0]] = f[2]
		}
	}
	var out []pkgInfo
	for _, l := range tagged {
		f := strings.SplitN(l, "\t", 3)
		if len(f) != 3 {
			continue
		}
		if f[2] == "||" || unitFiles[f[0]] == f[2] {
			continue
		}
		out = append(out, pkgInfo{ImportPath: f[0], Dir: f[1]})
	}
	return out, nil
}

// packageDirs maps import paths to directories.
func packageDirs(tags string, pkgs []string) (map[string]string, error) {
	lines, err := goList(tags, `{{.ImportPath}}{{"\t"}}{{.Dir}}`, pkgs...)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, l := range lines {
		f := strings.SplitN(l, "\t", 2)
		if len(f) == 2 {
			out[f[0]] = f[1]
		}
	}
	return out, nil
}

// testBinaryName is the file a package's test binary is compiled to.
var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func testBinaryName(importPath string) string {
	return unsafeName.ReplaceAllString(strings.TrimPrefix(importPath, modulePath+"/"), "_") + ".test"
}

// modulePath is this repository's module.
const modulePath = "github.com/orkstack/orkstack"

// compile builds every package's integration test binary into binDir, a few
// at a time (each `go test -c` is itself parallel).
func compile(pkgs []pkgInfo, binDir string, logw io.Writer) (map[string]string, error) {
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return nil, err
	}
	bins := map[string]string{}
	var mu sync.Mutex
	var errs []error
	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for _, p := range pkgs {
		wg.Add(1)
		sem <- struct{}{}
		go func(p pkgInfo) {
			defer wg.Done()
			defer func() { <-sem }()
			bin := filepath.Join(binDir, testBinaryName(p.ImportPath))
			started := time.Now()
			cmd := exec.Command("go", "test", "-c", "-tags=integration", "-o", bin, p.ImportPath)
			out, err := cmd.CombinedOutput()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("compile %s: %v\n%s", p.ImportPath, err, out))
				return
			}
			fmt.Fprintf(logw, "compiled %s in %.1fs -> %s\n", p.ImportPath, time.Since(started).Seconds(), bin)
			bins[p.ImportPath] = bin
		}(p)
	}
	wg.Wait()
	return bins, errors.Join(errs...)
}

// list runs a compiled test binary's -test.list in its package directory.
func list(bin, dir string) ([]string, error) {
	cmd := exec.Command(bin, "-test.list", ".*")
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s -test.list: %v\n%s", bin, err, stderr.String())
	}
	return parseListing(string(out)), nil
}

// runner executes jobs on a fixed number of slots.
type runner struct {
	slots    int
	nsPrefix string
	baseEnv  []string
	bins     map[string]string
	dirs     map[string]string
	jobDir   string

	mu     sync.Mutex
	c      *collector
	events io.Writer
	log    io.Writer
	done   int
	total  int
}

// jobResult is what one process reported.
type jobResult struct {
	exitErr error
	started int
}

// runAll runs jobs longest-first on r.slots slots. A job whose process died
// with tests it had not started yet runs those tests again in a fresh
// process, so one crash cannot hide the outcomes of the tests behind it; the
// test that was running when it died is recorded as failed and never re-run.
func (r *runner) runAll(ctx context.Context, jobs []job) {
	r.total = len(jobs)
	free := make(chan int, r.slots)
	for i := 1; i <= r.slots; i++ {
		free <- i
	}
	var wg sync.WaitGroup
	for _, j := range jobs {
		slot := <-free
		wg.Add(1)
		go func(j job, slot int) {
			defer wg.Done()
			defer func() { free <- slot }()
			for {
				res := r.runJob(ctx, j, slot)
				rest := r.settle(j, res)
				if len(rest) == 0 {
					break
				}
				j = job{ID: j.ID, Package: j.Package, Tests: rest, Cost: j.Cost, Known: j.Known, Timeout: j.Timeout, Attempt: j.Attempt + 1}
			}
			r.mu.Lock()
			r.done++
			r.mu.Unlock()
		}(j, slot)
	}
	wg.Wait()
}

// jobLabel names one process of a job.
func jobLabel(j job) string {
	if j.Attempt > 1 {
		return fmt.Sprintf("%s.%d", j.ID, j.Attempt)
	}
	return j.ID
}

// runJob runs one test-binary process: in the package's directory (tests
// resolve paths relative to it), with an anchored -test.run, its output
// converted by test2json.
func (r *runner) runJob(ctx context.Context, j job, slot int) jobResult {
	label := jobLabel(j)
	started := time.Now()
	r.logf("start %s slot %d: %s %s (estimated %.0fs, timeout %s)", label, slot, j.Package, strings.Join(j.Tests, ","), j.Cost, j.Timeout)

	logFile, err := os.Create(filepath.Join(r.jobDir, label+".log"))
	if err != nil {
		r.logf("%s: %v", label, err)
		return jobResult{exitErr: err}
	}
	defer logFile.Close()

	// The outer deadline only catches a binary that ignores its own
	// -test.timeout; the binary's timeout panic is what normally fires.
	jctx, cancel := context.WithTimeout(ctx, j.Timeout+2*time.Minute)
	defer cancel()
	args := []string{
		"-test.run", runPattern(j.Tests),
		"-test.v=test2json",
		"-test.paniconexit0",
		"-test.count=1",
		"-test.timeout=" + j.Timeout.String(),
	}
	cmd := exec.CommandContext(jctx, r.bins[j.Package], args...)
	cmd.Dir = r.dirs[j.Package]
	cmd.Env = append([]string{}, r.baseEnv...)
	if r.nsPrefix != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("ORK_TEMPORAL_NAMESPACE=%s-w%d", r.nsPrefix, slot))
	}
	cmd.Env = append(cmd.Env, "ORK_PROOF_JOB="+label)

	pr, pw, err := os.Pipe()
	if err != nil {
		return jobResult{exitErr: err}
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	conv := exec.Command("go", "tool", "test2json", "-t", "-p", j.Package)
	conv.Stdin = pr
	convOut, err := conv.StdoutPipe()
	if err != nil {
		return jobResult{exitErr: err}
	}
	conv.Stderr = logFile
	if err := conv.Start(); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return jobResult{exitErr: fmt.Errorf("start test2json: %w", err)}
	}
	_ = pr.Close()

	local := newCollector()
	var raw bytes.Buffer
	readDone := make(chan error, 1)
	go func() { readDone <- readEvents(convOut, local, label, &raw, logFile) }()

	runErr := cmd.Start()
	_ = pw.Close()
	if runErr == nil {
		runErr = cmd.Wait()
	}
	select {
	case <-readDone:
	case <-time.After(30 * time.Second):
		_ = conv.Process.Kill()
		<-readDone
	}
	_ = conv.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events != nil {
		_, _ = r.events.Write(raw.Bytes())
	}
	for _, k := range local.order {
		r.c.results[k] = local.results[k]
		r.c.order = append(r.c.order, k)
	}
	for k := range local.started {
		r.c.started[k] = true
	}
	r.c.subSkips = append(r.c.subSkips, local.subSkips...)
	n := 0
	for _, t := range j.Tests {
		if local.started[testKey{j.Package, t}] {
			n++
		}
	}
	outcome := "exit 0"
	if runErr != nil {
		outcome = runErr.Error()
	}
	fmt.Fprintf(r.log, "end   %s after %.1fs (%s), %d of %d tests started\n", label, time.Since(started).Seconds(), outcome, n, len(j.Tests))
	return jobResult{exitErr: runErr, started: n}
}

// settle records the job's tests that have no outcome and returns the ones
// to run again in a fresh process.
func (r *runner) settle(j job, res jobResult) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var rest []string
	for _, t := range j.Tests {
		k := testKey{j.Package, t}
		e, ok := r.c.results[k]
		if ok && e.Outcome != outcomeNone {
			continue
		}
		switch {
		case r.c.started[k]:
			// It was running when the process ended without reporting it:
			// the crash is this test's, and it is never re-run.
			if !ok {
				e = &entry{Package: j.Package, Test: t}
				r.c.results[k] = e
				r.c.order = append(r.c.order, k)
			}
			e.Outcome = outcomeFail
			e.Job = jobLabel(j)
			e.Reason = fmt.Sprintf("its test binary ended while it ran (%s); see %s.log", exitText(res.exitErr), jobLabel(j))
		case res.exitErr != nil && res.started > 0:
			rest = append(rest, t)
		case res.exitErr != nil:
			e = &entry{Package: j.Package, Test: t, Job: jobLabel(j), Outcome: outcomeFail,
				Reason: fmt.Sprintf("its test binary ended before starting it (%s); see %s.log", exitText(res.exitErr), jobLabel(j))}
			r.c.results[k] = e
			r.c.order = append(r.c.order, k)
		default:
			e = &entry{Package: j.Package, Test: t, Job: jobLabel(j), Outcome: outcomeNone,
				Reason: "no outcome: the process exited 0 without running it (does -test.run match it?)"}
			r.c.results[k] = e
			r.c.order = append(r.c.order, k)
		}
	}
	return rest
}

func exitText(err error) string {
	if err == nil {
		return "exit 0"
	}
	return err.Error()
}

func (r *runner) logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintf(r.log, format+"\n", args...)
}

// cmdIntegration is `proofrun integration`: compile, list, plan, run, ledger.
func cmdIntegration(args []string) int {
	fs := flag.NewFlagSet("integration", flag.ContinueOnError)
	out := fs.String("out", "", "directory for the ledger, plan, events and per-job logs (required)")
	binDir := fs.String("bin", "", "directory to compile test binaries into (default <out>/bin/test)")
	jobs := fs.Int("jobs", envInt("PROOF_JOBS", 4), "test processes at a time (PROOF_JOBS)")
	nsPrefix := fs.String("namespace-prefix", "", "each slot k runs with ORK_TEMPORAL_NAMESPACE=<prefix>-w<k>")
	costsFile := fs.String("costs", "scripts/proof-test-costs.tsv", "measured per-test durations")
	allowFile := fs.String("allow", "scripts/proof-allowed-skips.txt", "allowed skips")
	selectFile := fs.String("select", "", "a feature selection (proofrun select): run only its integration tests")
	only := fs.String("run", "", "run only the tests matching this regexp (by hand; never a proof)")
	planOnly := fs.Bool("plan-only", false, "compile, list and plan, print the estimated duration, run nothing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" || *jobs < 1 {
		fmt.Fprintln(os.Stderr, "proofrun integration: -out is required and -jobs must be at least 1")
		return 2
	}
	patterns := fs.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	if *binDir == "" {
		*binDir = filepath.Join(*out, "bin", "test")
	}
	allow, err := readAllowlist(*allowFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	measured, err := readCosts(*costsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	var onlyRE *regexp.Regexp
	if *only != "" {
		if onlyRE, err = regexp.Compile(*only); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun integration: -run:", err)
			return 2
		}
	}

	// Which packages, and which of their tests.
	var pkgs []pkgInfo
	var chosen map[string][]string
	whole := map[string]bool{}
	if *selectFile != "" {
		var sel selection
		if err := readJSON(*selectFile, &sel); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun integration:", err)
			return 2
		}
		chosen = map[string][]string{}
		var names []string
		for p, ps := range sel.Integration {
			if ps.Whole != "" {
				whole[p] = true
				names = append(names, p)
			} else if tests := ps.run(); len(tests) > 0 {
				chosen[p] = tests
				names = append(names, p)
			}
		}
		sort.Strings(names)
		if len(names) == 0 {
			fmt.Fprintln(stdout, "proofrun integration: the selection runs no integration test")
			return writeLedgerAndReport(*out, ledger{Suite: "integration", Problems: []string{}})
		}
		dirs, err := packageDirs("integration", names)
		if err != nil {
			fmt.Fprintln(os.Stderr, "proofrun integration:", err)
			return 2
		}
		for _, n := range names {
			pkgs = append(pkgs, pkgInfo{ImportPath: n, Dir: dirs[n]})
		}
	} else {
		if pkgs, err = integrationPackages(patterns); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun integration:", err)
			return 2
		}
	}
	if err := os.MkdirAll(filepath.Join(*out, "integration"), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 1
	}

	started := time.Now()
	bins, err := compile(pkgs, *binDir, stdout)
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	fmt.Fprintf(stdout, "compiled %d integration packages in %.1fs\n", len(pkgs), time.Since(started).Seconds())

	listed := map[string]map[string]bool{}
	run := map[string][]string{}
	dirs := map[string]string{}
	var expected []testKey
	var problems []string
	for _, p := range pkgs {
		dirs[p.ImportPath] = p.Dir
		names, err := list(bins[p.ImportPath], p.Dir)
		if err != nil {
			fmt.Fprintln(stdout, err)
			return 1
		}
		listed[p.ImportPath] = map[string]bool{}
		for _, n := range names {
			listed[p.ImportPath][n] = true
		}
		want := names
		if chosen != nil && !whole[p.ImportPath] {
			want = chosen[p.ImportPath]
		}
		for _, n := range want {
			if onlyRE != nil && !onlyRE.MatchString(n) {
				continue
			}
			expected = append(expected, testKey{p.ImportPath, n})
			if !listed[p.ImportPath][n] {
				problems = append(problems, fmt.Sprintf("%s.%s was selected, and its test binary does not list it", p.ImportPath, n))
				continue
			}
			run[p.ImportPath] = append(run[p.ImportPath], n)
		}
		fmt.Fprintf(stdout, "listed %s: %d tests, running %d\n", p.ImportPath, len(names), len(run[p.ImportPath]))
	}

	planned := plan(run, measured)
	if err := writeJSON(filepath.Join(*out, "integration-plan.json"), planned); err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	fmt.Fprintf(stdout, "planned %d jobs on %d slots; estimated %.0fs of work, %.0fs on %d slots (before contention)\n",
		len(planned), *jobs, totalCost(planned), makespan(planned, *jobs), *jobs)
	if *planOnly {
		return 0
	}

	events, err := os.Create(filepath.Join(*out, "integration.events.jsonl"))
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	defer events.Close()
	r := &runner{
		slots: *jobs, nsPrefix: *nsPrefix, baseEnv: os.Environ(), bins: bins, dirs: dirs,
		jobDir: filepath.Join(*out, "integration"), c: newCollector(), events: events, log: stdout,
	}
	r.runAll(context.Background(), planned)

	l := buildLedger("integration", expected, listed, r.c, allow)
	l.Problems = append(problems, l.Problems...)
	fmt.Fprintf(stdout, "integration phase took %.0fs\n", time.Since(started).Seconds())
	return writeLedgerAndReport(*out, l)
}

// writeLedgerAndReport writes <suite>-tests.json and prints its verdict.
func writeLedgerAndReport(out string, l ledger) int {
	if err := writeJSON(filepath.Join(out, l.Suite+"-tests.json"), l); err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	s := l.Summary
	fmt.Fprintf(stdout, "%s ledger: %d expected, %d pass, %d fail, %d allowed skip, %d skip, %d without outcome, %d subtest skips\n",
		l.Suite, s.Expected, s.Pass, s.Fail, s.AllowedSkip, s.Skip, s.NoOutcome, s.SubtestSkips)
	for _, p := range l.Problems {
		fmt.Fprintln(stdout, "  -", p)
	}
	if !l.ok() {
		return 1
	}
	return 0
}

// cmdUnit is `proofrun unit`: the unit suites, with a ledger.
func cmdUnit(args []string) int {
	fs := flag.NewFlagSet("unit", flag.ContinueOnError)
	out := fs.String("out", "", "directory for the ledger and events (required)")
	allowFile := fs.String("allow", "scripts/proof-allowed-skips.txt", "allowed skips")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "proofrun unit: -out is required")
		return 2
	}
	allow, err := readAllowlist(*allowFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun unit:", err)
		return 2
	}
	patterns := fs.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	// The listing: every test each package's unit binary reports.
	listCmd := exec.Command("go", append([]string{"test", "-json", "-list", ".*"}, patterns...)...)
	var listErr bytes.Buffer
	listCmd.Stderr = &listErr
	listOut, err := listCmd.Output()
	if err != nil {
		fmt.Fprintf(stdout, "go test -list: %v\n%s%s", err, listOut, listErr.String())
		return 1
	}
	listed, expected := parseJSONListing(listOut)

	events, err := os.Create(filepath.Join(*out, "unit.events.jsonl"))
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	defer events.Close()
	cmd := exec.Command("go", append([]string{"test", "-json", "-count=1"}, patterns...)...)
	cmd.Stderr = stdout
	goOut, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	c := newCollector()
	readErr := readEvents(goOut, c, "unit", events, stdout)
	runErr := cmd.Wait()

	l := buildLedger("unit", expected, listed, c, allow)
	if readErr != nil {
		l.Problems = append(l.Problems, "reading go test -json: "+readErr.Error())
	}
	if runErr != nil {
		l.Problems = append(l.Problems, "go test exited: "+runErr.Error())
	}
	return writeLedgerAndReport(*out, l)
}

// parseJSONListing reads `go test -json -list .*` output.
func parseJSONListing(out []byte) (map[string]map[string]bool, []testKey) {
	listed := map[string]map[string]bool{}
	var expected []testKey
	for _, line := range bytes.Split(out, []byte("\n")) {
		var ev event
		if json.Unmarshal(line, &ev) != nil || ev.Action != "output" || ev.Test != "" {
			continue
		}
		name := strings.TrimSpace(ev.Output)
		if !listingName.MatchString(name) {
			continue
		}
		if listed[ev.Package] == nil {
			listed[ev.Package] = map[string]bool{}
		}
		if !listed[ev.Package][name] {
			listed[ev.Package][name] = true
			expected = append(expected, testKey{ev.Package, name})
		}
	}
	return listed, expected
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// stdout is where the runners report; tests replace it.
var stdout io.Writer = os.Stdout
