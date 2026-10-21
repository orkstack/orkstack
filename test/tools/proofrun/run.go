package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	mu         sync.Mutex
	progressMu sync.Mutex
	c          *collector
	events     io.Writer
	log        io.Writer
	progress   io.Writer
	done       int
	total      int
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
//
// Heavy jobs (tests whose assertions are absolute wall-clock or throughput
// bounds) run first, one at a time, and no ordinary job starts until they are
// done: a soak measures a quiet machine, which is the local equivalent of a
// CI heavy shard's own Temporal. The ordinary jobs then fill every slot. The
// added local wall clock is recorded in docs/PROOFS.md.
func (r *runner) runAll(ctx context.Context, jobs []job) {
	r.total = len(jobs)
	var heavy, light []job
	for _, j := range jobs {
		if j.Heavy {
			heavy = append(heavy, j)
		} else {
			light = append(light, j)
		}
	}
	for _, j := range heavy {
		r.runOne(ctx, j, 1)
	}
	ch := make(chan job)
	var wg sync.WaitGroup
	for slot := 1; slot <= r.slots; slot++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			for j := range ch {
				r.runOne(ctx, j, slot)
			}
		}(slot)
	}
	for _, j := range light {
		ch <- j
	}
	close(ch)
	wg.Wait()
}

// runOne runs one job, re-running in a fresh process the tests behind a crash.
func (r *runner) runOne(ctx context.Context, j job, slot int) {
	for {
		res := r.runJob(ctx, j, slot)
		rest := r.settle(j, res)
		if len(rest) == 0 {
			break
		}
		j = job{ID: j.ID, Package: j.Package, Tests: rest, Cost: j.Cost, Known: j.Known, Heavy: j.Heavy, Timeout: j.Timeout, Attempt: j.Attempt + 1}
	}
	r.mu.Lock()
	r.done++
	r.mu.Unlock()
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
	progress := r.progressWriter()
	go func() { readDone <- readEvents(convOut, local, label, &raw, logFile, progress) }()

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
	r.c.failedSubs = append(r.c.failedSubs, local.failedSubs...)
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

// progressWriter is a writer that serialises the proofrun: lines a shard's
// log carries, so a cancelled job's log still says which test was running.
func (r *runner) progressWriter() io.Writer {
	if r.progress == nil {
		return nil
	}
	return lockedWriter{w: r.progress, mu: &r.progressMu}
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// integrationPlanOptions is `proofrun integration`'s invocation.
type integrationPlanOptions struct {
	out, binDir                           string
	jobs, shards, uiShards                int
	nsPrefix, costsFile, allowFile        string
	selectFile, only                      string
	quarantineFile, quarantineBase        string
	hostSpeedFile, hostSpeedBase          string
	planOnly                              bool
	planFile                              string
	shard                                 int
	matrixOut                             string
	nightlyFile, nightlyMode, nightlyBase string
	tier, proofID                         string
	baseCommit, mergeBase                 string
	observabilityRequired                 bool
	printProgress                         bool
	ci                                    bool
	listsChanged                          string
	prNumber                              int
	prHead, prBase, prContains            string
	pr                                    *prRecord
}

func integrationFlags() (*flag.FlagSet, *integrationPlanOptions) {
	o := &integrationPlanOptions{}
	fs := flag.NewFlagSet("integration", flag.ContinueOnError)
	fs.StringVar(&o.out, "out", "", "directory for the ledger, plan, events and per-job logs (required)")
	fs.StringVar(&o.binDir, "bin", "", "directory to compile test binaries into, or a directory of binaries the plan shipped (default <out>/bin/test)")
	fs.IntVar(&o.jobs, "jobs", envInt("PROOF_JOBS", 4), "test processes at a time (PROOF_JOBS)")
	fs.StringVar(&o.nsPrefix, "namespace-prefix", "", "each slot k runs with ORK_TEMPORAL_NAMESPACE=<prefix>-w<k>")
	fs.StringVar(&o.costsFile, "costs", "scripts/proof-test-costs.tsv", "measured per-test durations")
	fs.StringVar(&o.allowFile, "allow", "scripts/proof-allowed-skips.txt", "allowed skips")
	fs.StringVar(&o.quarantineFile, "quarantine", "scripts/proof-quarantine.txt", "the quarantine list")
	fs.StringVar(&o.quarantineBase, "quarantine-base", "", "the base's copy of the quarantine list (only entries it carries excuse)")
	fs.StringVar(&o.hostSpeedFile, "host-speed", "scripts/proof-host-speed.tsv", "the host-speed table, hashed into the plan")
	fs.StringVar(&o.hostSpeedBase, "host-speed-base", "", "the base's copy of the host-speed table, hashed into the plan (only rows it carries excuse)")
	fs.StringVar(&o.selectFile, "select", "", "a feature selection (proofrun select): run only its integration tests")
	fs.StringVar(&o.only, "run", "", "run only the tests matching this regexp (by hand; never a proof)")
	fs.BoolVar(&o.planOnly, "plan-only", false, "compile, list and plan, write integration-plan.json, run nothing")
	fs.StringVar(&o.planFile, "plan", "", "run the shard a plan names, with no re-planning")
	fs.IntVar(&o.shard, "shard", 0, "with -plan: run only this shard (1-based)")
	fs.IntVar(&o.shards, "shards", 1, "with -plan-only: split the planned jobs into this many shards")
	fs.StringVar(&o.matrixOut, "matrix-out", "", "with -plan-only: write the shard indices as a JSON array")
	fs.StringVar(&o.nightlyFile, "nightly", "scripts/proof-nightly-tests.txt", "the nightly list")
	fs.StringVar(&o.nightlyMode, "nightly-mode", nightlyInclude, "include (default), defer (leave out entries the base also lists) or only")
	fs.StringVar(&o.nightlyBase, "nightly-base", "", "the base's copy of the nightly list (required with -nightly-mode defer)")
	fs.StringVar(&o.tier, "tier", tierFull, "full, nightly, release or feature")
	fs.StringVar(&o.proofID, "proof-id", "", "the proof's id: the plan's image tag and namespace prefix")
	fs.StringVar(&o.baseCommit, "base-commit", "", "the commit the proof diffs against")
	fs.StringVar(&o.mergeBase, "merge-base", "", "the merge base of -base-commit and HEAD (computed when absent)")
	fs.BoolVar(&o.observabilityRequired, "observability-required", false, "the tree changes a telemetry path")
	fs.BoolVar(&o.printProgress, "print-progress", false, "print proofrun: start/outcome lines as tests run")
	fs.BoolVar(&o.ci, "ci", false, "this plan is a CI proof's (parts_expected is the job graph, not local)")
	fs.StringVar(&o.listsChanged, "lists-changed", "", "JSON file of gate-file changes (proofrun touches -gate-files)")
	fs.IntVar(&o.prNumber, "pr-number", 0, "pull request number")
	fs.StringVar(&o.prHead, "pr-head-sha", "", "the pull request's head")
	fs.StringVar(&o.prBase, "pr-base-sha", "", "the pull request's base")
	fs.StringVar(&o.prContains, "pr-contains-base", "", "the merge commit's second parent contains the first (true or false)")
	fs.IntVar(&o.uiShards, "ui-shards", 0, "how many UI shards the plan part wrote (recorded in parts_expected)")
	return fs, o
}

// cmdIntegration is `proofrun integration`: compile, list, plan and run, or
// plan once and run one shard of it (`-plan`), or plan only (`-plan-only`).
func cmdIntegration(args []string) int {
	fs, o := integrationFlags()
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if o.prNumber > 0 {
		o.pr = &prRecord{Number: o.prNumber, HeadSHA: o.prHead, BaseSHA: o.prBase, ContainsBase: o.prContains == "true"}
	}
	if o.pr != nil && !o.pr.ContainsBase {
		// A pull request's merge commit must contain its base: otherwise the
		// tree tested is not the one that would land (plan §3.2).
		fmt.Fprintf(os.Stderr, "proofrun integration: pull request %d's tested commit does not contain its base %s: update the branch and run again\n", o.pr.Number, orDash(o.pr.BaseSHA))
		return 2
	}
	if o.out == "" || o.jobs < 1 || o.shards < 1 {
		fmt.Fprintln(os.Stderr, "proofrun integration: -out is required, -jobs and -shards must be at least 1")
		return 2
	}
	switch o.nightlyMode {
	case nightlyInclude, nightlyDefer, nightlyOnly:
	default:
		fmt.Fprintln(os.Stderr, "proofrun integration: -nightly-mode must be include, defer or only")
		return 2
	}
	if o.nightlyMode == nightlyDefer && o.nightlyBase == "" {
		fmt.Fprintln(os.Stderr, "proofrun integration: -nightly-mode defer requires -nightly-base: a proof defers only the entries its base also lists")
		return 2
	}
	switch o.tier {
	case tierFull, tierNightly, tierRelease, tierFeature:
	default:
		fmt.Fprintln(os.Stderr, "proofrun integration: -tier must be full, nightly, release or feature")
		return 2
	}
	if o.planFile != "" {
		if o.planOnly || o.shard < 1 {
			fmt.Fprintln(os.Stderr, "proofrun integration: -plan runs one shard: give -shard K (1-based) and no -plan-only")
			return 2
		}
		return runShard(o)
	}
	if o.shard > 0 {
		fmt.Fprintln(os.Stderr, "proofrun integration: -shard runs a plan: give -plan FILE")
		return 2
	}
	return planAndMaybeRun(o, fs.Args())
}

// planAndMaybeRun compiles and lists the packages, writes the plan, and runs
// it unless -plan-only.
func planAndMaybeRun(o *integrationPlanOptions, patterns []string) int {
	// Only a run that plans every integration package can say a nightly
	// entry names no integration package: a selection, -run or explicit
	// package patterns (scripts/integration.sh) compile a subset.
	complete := o.selectFile == "" && o.only == "" && len(patterns) == 0
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	binDir := o.binDir
	if binDir == "" {
		binDir = filepath.Join(o.out, "bin", "test")
	}
	allow, err := readAllowlist(o.allowFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	measured, err := readCosts(o.costsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	var onlyRE *regexp.Regexp
	if o.only != "" {
		if onlyRE, err = regexp.Compile(o.only); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun integration: -run:", err)
			return 2
		}
	}
	var sel *selection
	if o.selectFile != "" {
		sel = &selection{}
		if err := readJSON(o.selectFile, sel); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun integration:", err)
			return 2
		}
	}
	nightly, err := readNightlyList(o.nightlyFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	var nightlyBase []nightlyEntry
	if o.nightlyMode == nightlyDefer || o.nightlyBase != "" {
		if nightlyBase, err = readNightlyList(o.nightlyBase); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun integration:", err)
			return 2
		}
	}

	var pkgs []pkgInfo
	var chosen map[string][]string
	whole := map[string]bool{}
	if sel != nil {
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
			plan := &integrationPlan{Format: planFormat, Tier: tierFeature, Expected: []testRef{}, Jobs: []job{}, Shards: []planShard{}, Packages: []planPackage{}, Problems: []string{}}
			return writePlan(*o, plan, measured, nightly, nightlyBase, nil)
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
	if err := os.MkdirAll(filepath.Join(o.out, "integration"), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 1
	}

	started := time.Now()
	bins, err := compile(pkgs, binDir, stdout)
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	fmt.Fprintf(stdout, "compiled %d integration packages in %.1fs\n", len(pkgs), time.Since(started).Seconds())

	listed := map[string]map[string]bool{}
	run := map[string][]string{}
	dirs := map[string]string{}
	var packages []planPackage
	var problems []string
	var unlisted []testKey
	root, _ := repoRoot()
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
			if !listed[p.ImportPath][n] {
				problems = append(problems, fmt.Sprintf("%s.%s was selected, and its test binary does not list it", p.ImportPath, n))
				unlisted = append(unlisted, testKey{p.ImportPath, n})
				continue
			}
			run[p.ImportPath] = append(run[p.ImportPath], n)
		}
		var sha string
		if b, err := os.ReadFile(bins[p.ImportPath]); err == nil {
			sum := sha256.Sum256(b)
			sha = hex.EncodeToString(sum[:])
		}
		rel := p.Dir
		if root != "" {
			if r, err := filepath.Rel(root, p.Dir); err == nil {
				rel = r
			}
		}
		packages = append(packages, planPackage{
			ImportPath: p.ImportPath, Dir: rel, Binary: filepath.Base(bins[p.ImportPath]),
			BinarySHA256: sha, ListingSHA256: sha256String(strings.Join(names, "\n")), Listed: names,
		})
		fmt.Fprintf(stdout, "listed %s: %d tests, running %d\n", p.ImportPath, len(names), len(run[p.ImportPath]))
	}
	checkedOn := todayUTC()
	quarantine, qProblems, qExpiring, err := quarantineProof(o.quarantineFile, o.quarantineBase, listed, allow, checkedOn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	printQuarantineWarnings(stdout, qExpiring)
	problems = append(nightlyProblems(nightly, listed, complete), problems...)
	problems = append(qProblems, problems...)

	// The test sets: expected = run ∪ deferred, and the mode decides both.
	deferred := map[testKey]nightlyEntry{}
	switch {
	case o.nightlyMode == nightlyDefer:
		if sel == nil {
			deferred = deferredSet(nightly, nightlyBase)
		}
	case o.nightlyMode == nightlyOnly && sel == nil:
		// expected is the nightly list's tests only; everything else is out of tier.
	}
	runSet := map[testKey]bool{}
	for p, names := range run {
		for _, n := range names {
			k := testKey{p, n}
			if _, def := deferred[k]; def {
				continue
			}
			runSet[k] = true
		}
	}
	if o.nightlyMode == nightlyOnly && sel == nil {
		runSet = map[testKey]bool{}
		for _, e := range nightly {
			if listed[e.Package][e.Test] {
				runSet[e.key()] = true
			}
		}
	}
	runTests := map[string][]string{}
	for p := range run {
		runTests[p] = nil
	}
	for p, names := range run {
		for _, n := range names {
			if runSet[testKey{p, n}] {
				runTests[p] = append(runTests[p], n)
			}
		}
	}
	kinds := map[string]map[string]string{}
	for _, p := range pkgs {
		kinds[p.ImportPath] = testKinds(p.Dir)
	}
	jobs := plan(runTests, measured, kinds)
	shards := splitShards(jobs, o.shards, o.jobs)
	plan := &integrationPlan{
		Format: planFormat, Tier: o.tier, ProofID: o.proofID,
		BaseRef: mainBranch, BaseCommit: o.baseCommit, MergeBase: o.mergeBase,
		ObservabilityRequired: o.observabilityRequired, PR: o.pr,
		Packages: packages, Jobs: jobs, Shards: shards,
		RequestedShards: o.shards, SlotsPerShard: o.jobs, Problems: problems,
		CheckedOn: checkedOn.Format("2006-01-02"),
	}
	if code := writePlan(*o, plan, measured, nightly, nightlyBase, nil); code != 0 {
		return code
	}
	for _, s := range shards {
		fmt.Fprintf(stdout, "  shard %d: %d jobs, estimated %.0fs, makespan %.0fs\n", s.Index, len(s.Jobs), s.EstimatedSeconds, s.EstimatedMakespan)
	}
	fmt.Fprintf(stdout, "planned %d jobs on %d shards (%d slots each); estimated %.0fs of work, floor %.0fs\n",
		len(jobs), len(shards), o.jobs, totalCost(jobs), floorSeconds(jobs))
	if o.planOnly {
		if o.matrixOut != "" {
			idx := make([]int, len(shards))
			for i := range idx {
				idx[i] = i + 1
			}
			b, _ := json.Marshal(idx)
			if err := os.WriteFile(o.matrixOut, append(b, '\n'), 0o644); err != nil {
				fmt.Fprintln(os.Stderr, "proofrun integration:", err)
				return 1
			}
		}
		return 0
	}
	events, err := os.Create(filepath.Join(o.out, "integration.events.jsonl"))
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	defer events.Close()
	r := &runner{
		slots: o.jobs, nsPrefix: o.nsPrefix, baseEnv: os.Environ(), bins: bins, dirs: dirs,
		jobDir: filepath.Join(o.out, "integration"), c: newCollector(), events: events, log: stdout,
		progress: nil,
	}
	if o.printProgress {
		r.progress = stdout
	}
	r.runAll(context.Background(), jobs)
	var expected []testKey
	for _, e := range plan.Expected {
		expected = append(expected, testKey{e.Package, e.Test})
	}
	l := buildLedgerQ("integration", expected, listed, r.c, allow, quarantine)
	for _, k := range unlisted {
		l.Tests = append(l.Tests, entry{Package: k.Package, Test: k.Test, Outcome: outcomeNone,
			Reason: "it was selected, and its test binary does not list it"})
		l.Summary.Expected++
		l.Summary.NoOutcome++
	}
	l.Problems = append(problems, l.Problems...)
	l.PlanSHA256, _ = fieldSHA256(filepath.Join(o.out, "integration-plan.json"))
	// The local part records each binary's listing as a shard does, so the
	// merger checks a local proof's listings too (rule 7).
	l.Binaries = map[string]binaryRecord{}
	for _, pk := range packages {
		l.Binaries[pk.ImportPath] = binaryRecord{SHA256: pk.BinarySHA256, ListingSHA256: pk.ListingSHA256}
	}
	fmt.Fprintf(stdout, "integration phase took %.0fs\n", time.Since(started).Seconds())
	return writeLedgerAndReport(o.out, l)
}

// writePlan records the expected set and every input's hash, then writes the
// plan to <out>/integration-plan.json.
func writePlan(o integrationPlanOptions, p *integrationPlan, measured costs, nightly, nightlyBase []nightlyEntry, _ []testKey) int {
	p.Expected = []testRef{}
	p.Deferred = []deferredRef{}
	p.Unmeasured = []unmeasuredRef{}
	runSet := map[testKey]bool{}
	for _, j := range p.Jobs {
		for _, t := range j.Tests {
			runSet[testKey{j.Package, t}] = true
		}
	}
	deferred := map[testKey]nightlyEntry{}
	if o.nightlyMode == nightlyDefer && o.selectFile == "" {
		deferred = deferredSet(nightly, nightlyBase)
	}
	var keys []testKey
	if o.nightlyMode == nightlyOnly && o.selectFile == "" {
		for _, e := range nightly {
			if runSet[e.key()] {
				keys = append(keys, e.key())
			}
		}
	} else {
		for _, j := range p.Jobs {
			for _, t := range j.Tests {
				keys = append(keys, testKey{j.Package, t})
			}
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i].Package != keys[j].Package {
			return keys[i].Package < keys[j].Package
		}
		return keys[i].Test < keys[j].Test
	})
	for k, e := range deferred {
		if runSet[k] {
			continue
		}
		p.Deferred = append(p.Deferred, deferredRef{Package: k.Package, Test: k.Test, Entry: e.Text})
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i].Package != keys[j].Package {
			return keys[i].Package < keys[j].Package
		}
		return keys[i].Test < keys[j].Test
	})
	p.Expected = sortedTestRefs(keys)
	sort.SliceStable(p.Deferred, func(i, j int) bool {
		if p.Deferred[i].Package != p.Deferred[j].Package {
			return p.Deferred[i].Package < p.Deferred[j].Package
		}
		return p.Deferred[i].Test < p.Deferred[j].Test
	})
	for _, k := range keys {
		if _, ok := measured[k]; ok {
			continue
		}
		kind := "integration"
		for _, pkg := range p.Packages {
			if pkg.ImportPath == k.Package && testKindsFor(pkg.Dir)[k.Test] == "unit" {
				kind = "unit"
			}
		}
		p.Unmeasured = append(p.Unmeasured, unmeasuredRef{Package: k.Package, Test: k.Test, Kind: kind})
	}
	if o.nightlyMode == nightlyOnly && o.selectFile == "" {
		p.OutOfTier = 0
		for _, pkg := range p.Packages {
			for _, t := range pkg.Listed {
				if !runSet[testKey{pkg.ImportPath, t}] {
					p.OutOfTier++
				}
			}
		}
	}
	p.Tree, _ = workingTreeHash(mustRepoRoot())
	p.Commit = strings.TrimSpace(mustGit("rev-parse", "HEAD"))
	p.Nightly = nightlyRef{
		File: o.nightlyFile, Mode: o.nightlyMode,
		Entries: entryLines(nightly), BaseCommit: o.baseCommit,
		BaseEntries: entryLines(nightlyBase),
	}
	p.Nightly.SHA256, _ = fileSHA256(o.nightlyFile)
	if o.nightlyBase != "" {
		p.Nightly.BaseSHA256, _ = fileSHA256(o.nightlyBase)
	}
	p.Costs = fileRef{File: o.costsFile}
	p.Costs.SHA256, _ = fileSHA256(o.costsFile)
	// Every input that decides what runs or what is excused is pinned by its
	// hash, so the merger can refuse a proof judged on other bytes.
	p.AllowedSkips = fileRef{File: o.allowFile}
	p.AllowedSkips.SHA256, _ = fileSHA256(o.allowFile)
	p.Quarantine = quarantineRef{File: o.quarantineFile}
	p.Quarantine.SHA256, _ = fileSHA256(o.quarantineFile)
	p.Quarantine.BaseSHA256, _ = fileSHA256(o.quarantineBase)
	p.HostSpeed = quarantineRef{File: o.hostSpeedFile}
	p.HostSpeed.SHA256, _ = fileSHA256(o.hostSpeedFile)
	p.HostSpeed.BaseSHA256, _ = fileSHA256(o.hostSpeedBase)
	p.PartsExpected = partsExpected(o.tier, !o.ci, len(p.Shards), 0, o.observabilityRequired)
	if o.ci {
		uiShards := o.uiShards
		p.PartsExpected = partsExpected(o.tier, false, len(p.Shards), uiShards, o.observabilityRequired)
	}
	if p.CheckedOn == "" {
		p.CheckedOn = time.Now().UTC().Format("2006-01-02")
	}
	if o.listsChanged != "" {
		if err := readJSON(o.listsChanged, &p.ListsChanged); err != nil {
			// An unreadable list of gate-file changes is recorded as such,
			// never as "no gate file changed" (plan §6.4).
			p.ListsChanged = []listChange{{Error: "the gate-file changes are unreadable: " + err.Error()}}
		}
	}
	if o.mergeBase == "" && o.baseCommit != "" {
		if mb, err := gitOut("merge-base", o.baseCommit, "HEAD"); err == nil {
			p.MergeBase = strings.TrimSpace(mb)
		}
	}
	if err := writeJSON(filepath.Join(o.out, "integration-plan.json"), p); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 1
	}
	return 0
}

func testKindsFor(dir string) map[string]string { return testKinds(dir) }

// runShard runs one shard of a written plan.
func runShard(o *integrationPlanOptions) int {
	plan, err := readPlan(o.planFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	root, err := repoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	tree, err := checkoutTree(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	if plan.Tree == "" || tree != plan.Tree {
		fmt.Fprintf(os.Stderr, "proofrun integration: this plan was made for tree %s, and this checkout is %s\n", plan.Tree, tree)
		return 2
	}
	if o.shard > len(plan.Shards) {
		fmt.Fprintf(os.Stderr, "proofrun integration: -shard %d, and the plan holds %d shards\n", o.shard, len(plan.Shards))
		return 2
	}
	if err := os.MkdirAll(filepath.Join(o.out, "integration"), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 1
	}
	jobs := plan.shardJobs(o.shard)
	if len(jobs) == 0 {
		fmt.Fprintf(os.Stderr, "proofrun integration: shard %d holds no job\n", o.shard)
		return 2
	}
	binDir := o.binDir
	if binDir == "" {
		binDir = filepath.Join(o.out, "bin", "test")
	}
	allow, err := readAllowlist(o.allowFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	var pkgs []pkgInfo
	seen := map[string]bool{}
	for _, j := range jobs {
		if seen[j.Package] {
			continue
		}
		seen[j.Package] = true
		pk := plan.pkg(j.Package)
		if pk == nil {
			fmt.Fprintf(os.Stderr, "proofrun integration: the plan holds a job for %s and no package for it\n", j.Package)
			return 2
		}
		pkgs = append(pkgs, pkgInfo{ImportPath: pk.ImportPath, Dir: filepath.Join(root, pk.Dir)})
	}
	bins := map[string]string{}
	binaries := map[string]binaryRecord{}
	if st, err := os.Stat(binDir); err == nil && st.IsDir() {
		if _, err := os.Stat(filepath.Join(binDir, plan.pkg(pkgs[0].ImportPath).Binary)); err == nil {
			for _, p := range pkgs {
				pk := plan.pkg(p.ImportPath)
				bin := filepath.Join(binDir, pk.Binary)
				sha, err := fileSHA256(bin)
				if err != nil {
					fmt.Fprintf(os.Stderr, "proofrun integration: shipped binary %s: %v\n", bin, err)
					return 2
				}
				if pk.BinarySHA256 != "" && sha != pk.BinarySHA256 {
					fmt.Fprintf(os.Stderr, "proofrun integration: shipped binary %s hashes to %s, and the plan says %s\n", bin, sha, pk.BinarySHA256)
					return 2
				}
				bins[p.ImportPath] = bin
			}
		}
	}
	if len(bins) == 0 {
		var err error
		bins, err = compile(pkgs, binDir, stdout)
		if err != nil {
			fmt.Fprintln(stdout, err)
			return 1
		}
	}
	listed := map[string]map[string]bool{}
	dirs := map[string]string{}
	for _, p := range pkgs {
		dirs[p.ImportPath] = p.Dir
		names, err := list(bins[p.ImportPath], p.Dir)
		if err != nil {
			fmt.Fprintln(stdout, err)
			return 1
		}
		pk := plan.pkg(p.ImportPath)
		if !slicesEqual(names, pk.Listed) {
			missing, extra := listingDiff(pk.Listed, names)
			if len(missing) > 0 {
				fmt.Fprintf(os.Stderr, "proofrun integration: binary %s misses %s, which the plan listed\n", p.ImportPath, strings.Join(missing, ", "))
			}
			if len(extra) > 0 {
				fmt.Fprintf(os.Stderr, "proofrun integration: binary %s lists %s, which the plan did not\n", p.ImportPath, strings.Join(extra, ", "))
			}
			return 2
		}
		listed[p.ImportPath] = map[string]bool{}
		for _, n := range names {
			listed[p.ImportPath][n] = true
		}
		sha, _ := fileSHA256(bins[p.ImportPath])
		lsha := sha256String(strings.Join(names, "\n"))
		binaries[p.ImportPath] = binaryRecord{SHA256: sha, ListingSHA256: lsha}
		if pk.BinarySHA256 != "" && sha != pk.BinarySHA256 {
			fmt.Fprintf(stdout, "note: a binary compiled here hashes to %s; the plan's compile hashed to %s (separate compiles need not agree)\n", sha, pk.BinarySHA256)
		}
	}
	// A shard judges every quarantine date on the proof's start date, the
	// plan's, so every part and the merger decide alike.
	checkedOn, err := time.Parse("2006-01-02", plan.CheckedOn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "proofrun integration: the plan records no start date (checked_on %q): quarantine dates cannot be judged\n", plan.CheckedOn)
		return 2
	}
	quarantine, qProblems, qExpiring, err := quarantineProof(o.quarantineFile, o.quarantineBase, listed, allow, checkedOn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun integration:", err)
		return 2
	}
	printQuarantineWarnings(stdout, qExpiring)
	events, err := os.Create(filepath.Join(o.out, "integration.events.jsonl"))
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	defer events.Close()
	r := &runner{
		slots: o.jobs, nsPrefix: o.nsPrefix, baseEnv: os.Environ(), bins: bins, dirs: dirs,
		jobDir: filepath.Join(o.out, "integration"), c: newCollector(), events: events, log: stdout,
	}
	if o.printProgress {
		r.progress = stdout
	}
	started := time.Now()
	r.runAll(context.Background(), jobs)
	shardSet := map[testKey]bool{}
	for _, j := range jobs {
		for _, t := range j.Tests {
			shardSet[testKey{j.Package, t}] = true
		}
	}
	var expected []testKey
	for _, e := range plan.Expected {
		if shardSet[testKey{e.Package, e.Test}] {
			expected = append(expected, testKey{e.Package, e.Test})
		}
	}
	l := buildLedgerQ("integration", expected, listed, r.c, allow, quarantine)
	l.Problems = append(qProblems, l.Problems...)
	l.Shard = o.shard
	l.PlanSHA256, _ = fieldSHA256(o.planFile)
	l.Binaries = binaries
	fmt.Fprintf(stdout, "shard %d of %d took %.0fs\n", o.shard, len(plan.Shards), time.Since(started).Seconds())
	return writeLedgerAndReport(o.out, l)
}

// checkoutTree is the tree a shard compares with its plan's: the working
// tree's hash. Tests replace it, because other work may change the checkout
// a test runs in while it runs.
var checkoutTree = workingTreeHash

func listingDiff(want, got []string) (missing, extra []string) {
	w := map[string]bool{}
	g := map[string]bool{}
	for _, s := range want {
		w[s] = true
	}
	for _, s := range got {
		g[s] = true
	}
	for _, s := range want {
		if !g[s] {
			missing = append(missing, s)
		}
	}
	for _, s := range got {
		if !w[s] {
			extra = append(extra, s)
		}
	}
	return missing, extra
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sha256String(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func fieldSHA256(file string) (string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func mustRepoRoot() string {
	r, _ := repoRoot()
	return r
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
	quarantineFile := fs.String("quarantine", "scripts/proof-quarantine.txt", "the quarantine list")
	quarantineBase := fs.String("quarantine-base", "", "the base's copy of the quarantine list (only entries it carries excuse)")
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
	checkedOn := todayUTC()
	quarantine, qProblems, qExpiring, err := quarantineProof(*quarantineFile, *quarantineBase, listed, allow, checkedOn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun unit:", err)
		return 2
	}
	printQuarantineWarnings(stdout, qExpiring)

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
	readErr := readEvents(goOut, c, "unit", events, stdout, nil)
	runErr := cmd.Wait()

	l := buildLedgerQ("unit", expected, listed, c, allow, quarantine)
	l.Problems = append(qProblems, l.Problems...)
	if readErr != nil {
		l.Problems = append(l.Problems, "reading go test -json: "+readErr.Error())
	}
	if runErr != nil && !unitExitExcused(l, c.pkgEnds, listed) {
		l.Problems = append(l.Problems, "go test exited: "+runErr.Error())
	}
	return writeLedgerAndReport(*out, l)
}

// unitExitExcused reports whether go test's non-zero exit says nothing the
// ledger does not: every failing test is a quarantined failure, and every
// package reported its own terminal event, failing only where a quarantined
// test failed. A build failure, a TestMain or package-level failure, or a
// crash keeps the exit a problem (plan §2.1, `unit`).
func unitExitExcused(l ledger, pkgEnds map[string]string, listed map[string]map[string]bool) bool {
	if l.Summary.Quarantined == 0 || l.Summary.Fail > 0 || l.Summary.NoOutcome > 0 {
		return false
	}
	quarantined := map[string]bool{}
	for _, e := range l.Tests {
		if e.Outcome == outcomeQuarantinedFail {
			quarantined[e.Package] = true
		}
	}
	for pkg := range listed {
		if _, ok := pkgEnds[pkg]; !ok {
			return false
		}
	}
	for pkg, end := range pkgEnds {
		if end == outcomeFail && !quarantined[pkg] {
			return false
		}
	}
	return true
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

// todayUTC is the proof's date: a quarantine review-by date is judged against
// a date, never a time of day, so every part and the merger agree.
func todayUTC() time.Time {
	t, _ := time.Parse("2006-01-02", time.Now().UTC().Format("2006-01-02"))
	return t
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
