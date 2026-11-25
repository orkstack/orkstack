package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The GPU spec list (scripts/proof-gpu-specs.txt) is the Playwright twin of
// the GPU-host test list (gpu.go): spec files that need the GPU host. It has
// the nightly spec list's rows and is parsed as strictly (nightlyspecs.go),
// and the same two-list anchor applies: a proof without ORK_PROOF_GPU_HOST=1
// records each test of a listed spec that both the tree's list and the base's
// copy carry as host-deferred in the Playwright ledger, and a spec only the
// tree carries is planned with no outcome, so its change lands through a
// GPU-host-tier proof. A GPU-host-tier proof runs the listed specs in its GPU
// gate, after the gpuhost Go tests, one file at a time, into a ledger of its
// own (gpu-specs).
//
// There is no paths: header: the covered paths of the merge-check rule
// (`make merge-check GPU=`) are the list itself, gpuspecs.go and every spec
// the tree's or main's list names. A spec's helpers are not covered; that
// would widen the rule to the whole UI suite, the user's decision to make.

const (
	gpuSpecFile         = "scripts/proof-gpu-specs.txt"
	gpuSpecBaseFileName = "gpu-specs-base.txt"
	suiteGPUSpecs       = "gpu-specs"
)

// readGPUSpecs parses the list; an absent file is an empty list, because the
// base of a change may predate it.
func readGPUSpecs(file string) ([]nightlySpecEntry, error) {
	return readNightlySpecs(file)
}

// parseGPUSpecs parses the rows as strictly as the nightly spec list. A list
// with no row is valid.
func parseGPUSpecs(b []byte, name string) ([]nightlySpecEntry, error) {
	return parseNightlySpecs(bytes.NewReader(b), name)
}

// gpuSpecOverlapProblems refuses a spec on both the GPU and the nightly spec
// list: the nightly tier could not run it, and the GPU host runs it.
func gpuSpecOverlapProblems(gpu, nightly []nightlySpecEntry) []string {
	on := map[string]bool{}
	for _, e := range nightly {
		on[e.Spec] = true
	}
	var problems []string
	for _, e := range gpu {
		if on[e.Spec] {
			problems = append(problems, fmt.Sprintf("GPU spec list line %d: %s is on both the GPU spec list and the nightly spec list", e.Line, e.Spec))
		}
	}
	return problems
}

// gpuSpecListProblems checks the GPU spec list against a listing's spec
// files: a row whose spec the listing does not hold is stale, a spec may not
// also be on the nightly list, and, with complete set (a listing of every spec
// file), a covering spec must be a listed file on neither list.
func gpuSpecListProblems(gpu, nightly []nightlySpecEntry, listed map[string]bool, complete bool) []string {
	elsewhere := map[string]string{}
	for _, e := range nightly {
		elsewhere[e.Spec] = "nightly spec list"
	}
	return append(gpuSpecOverlapProblems(gpu, nightly), specListProblems("GPU spec list", gpu, listed, complete, elsewhere)...)
}

// gpuSpecPaths are the specs the lists name, sorted and unique: paths a
// change to which needs a GPU-host-tier proof.
func gpuSpecPaths(lists ...[]nightlySpecEntry) []string {
	seen := map[string]bool{}
	var out []string
	for _, rows := range lists {
		for _, e := range rows {
			if !seen[e.Spec] {
				seen[e.Spec] = true
				out = append(out, e.Spec)
			}
		}
	}
	sort.Strings(out)
	return out
}

// gpuSpecAnchorProblems is what makes the UI plan's host-deferral the one the
// two lists give: the plan read the tree's list as it is now, and, unless
// it ran on the GPU host or in the nightly tier, the base's copy beside it,
// byte for byte the list at the base commit, and deferred exactly the rows
// both carry. A plan that recorded no list fails closed while the tree's has
// rows: their deferral could not be checked. mode is the plan's nightly mode.
func gpuSpecAnchorProblems(p uiPlan, mode, baseFile, baseCommit string) []string {
	tree, err := readGPUSpecs(gpuSpecFile)
	if err != nil {
		return []string{"the GPU spec list cannot be read: " + err.Error()}
	}
	treeSHA, _ := fileSHA256(gpuSpecFile)
	g := p.GPUSpecs
	if g == nil {
		if len(tree) > 0 {
			return []string{fmt.Sprintf("the UI plan recorded no GPU spec list while %s lists %d spec file(s): their host-deferral cannot be checked", gpuSpecFile, len(tree))}
		}
		return nil
	}
	var problems []string
	if g.SHA256 != treeSHA {
		problems = append(problems, fmt.Sprintf("the GPU spec list %s is not the one the UI plan read (plan sha256 %s, tree %s)", orDash(g.File), short(g.SHA256), orDash(short(treeSHA))))
	}
	switch {
	case mode == nightlyOnly:
		for _, d := range p.HostDeferred {
			problems = append(problems, fmt.Sprintf("%s was host-deferred, and the UI plan's nightly mode is only: the nightly listing holds no GPU spec", d.File))
		}
		return problems
	case g.Host:
		for _, d := range p.HostDeferred {
			problems = append(problems, fmt.Sprintf("%s was host-deferred, and the plan ran on the GPU host: its GPU gate runs the spec", d.File))
		}
		return problems
	}
	if got, _ := fileSHA256(baseFile); got != g.BaseSHA256 {
		problems = append(problems, fmt.Sprintf("the base's GPU spec list beside the UI plan is not the one it hashed (plan sha256 %s, %s %s)", short(g.BaseSHA256), baseFile, orDash(short(got))))
	}
	base, err := readGPUSpecs(baseFile)
	if err != nil {
		return append(problems, "the base's GPU spec list cannot be read to check the deferral: "+err.Error())
	}
	if len(tree) > 0 || len(base) > 0 {
		problems = append(problems, listBaseCopyProblems("GPU spec list", gpuSpecFile, baseFile, baseCommit)...)
	}
	inTree, inBase := map[string]bool{}, map[string]bool{}
	for _, e := range tree {
		inTree[e.file()] = true
	}
	for _, e := range base {
		inBase[e.file()] = true
	}
	want := deferredSpecSet(tree, base)
	deferred := map[string]bool{}
	for _, d := range p.HostDeferred {
		deferred[d.File] = true
		if _, ok := want[d.File]; !ok {
			problems = append(problems, fmt.Sprintf("%s was host-deferred, but both GPU spec lists do not carry it: a row the change adds runs on the GPU host", d.File))
		}
	}
	files := make([]string, 0, len(want))
	for f := range want {
		files = append(files, f)
	}
	sort.Strings(files)
	for _, f := range files {
		if !deferred[f] {
			problems = append(problems, fmt.Sprintf("%s is in both GPU spec lists and was not host-deferred", want[f].Spec))
		}
	}
	return problems
}

// checkGPUSpecsLedger holds the GPU spec gate's ledger to the tree's list,
// re-derived from the file: the hash it recorded, a gate that ran on the GPU
// host, no problem of its own, a passing test only, none host-deferred (this
// ledger exists only where the gate ran), and exactly the listed spec files.
func checkGPUSpecsLedger(l uiLedger, tree []nightlySpecEntry, treeSHA, tier string) []string {
	var p []string
	if l.Suite != suiteGPUSpecs {
		p = append(p, fmt.Sprintf("the gpu-specs ledger is of suite %q", l.Suite))
	}
	if l.ListSHA256 != treeSHA {
		p = append(p, fmt.Sprintf("the gpu-specs ledger read the GPU spec list %s, and this tree's is %s", orDash(short(l.ListSHA256)), orDash(short(treeSHA))))
	}
	if !l.GPUHost || !l.GPUGateRan {
		p = append(p, "the gpu-specs ledger's gate did not run on the GPU host")
	}
	p = append(p, l.Problems...)
	inTree := map[string]bool{}
	for _, e := range tree {
		inTree[e.file()] = true
	}
	has := map[string]bool{}
	for _, e := range l.Tests {
		if !inTree[e.File] {
			p = append(p, fmt.Sprintf("%s is in the gpu-specs ledger and not on this tree's GPU spec list", e.File))
			continue
		}
		has[e.File] = true
		switch e.Outcome {
		case outcomePass:
			if !l.GPUHost || !l.GPUGateRan {
				p = append(p, fmt.Sprintf("%s passed in a gpu-specs ledger whose gate did not run on the GPU host", uiKey(e)))
			}
		case outcomeHostDeferred:
			p = append(p, fmt.Sprintf("%s is host-deferred in the gpu-specs ledger (%s proof): the GPU spec gate runs it", uiKey(e), tier))
		default:
			p = append(p, fmt.Sprintf("%s has outcome %q in the gpu-specs ledger", uiKey(e), orDash(e.Outcome)))
		}
	}
	for _, e := range tree {
		if !has[e.file()] {
			p = append(p, fmt.Sprintf("%s has no test in the gpu-specs ledger", e.Spec))
		}
	}
	return p
}

// readGPUSpecsLedger reads the spec gate's ledger from a part's directory and
// judges it against the tree's list; a gate that ran on the GPU host and left
// none is a problem. It returns the ledger, with its problems, or nil.
func readGPUSpecsLedger(dir, tier string) (*uiLedger, []string) {
	tree, err := readGPUSpecs(gpuSpecFile)
	if err != nil {
		return nil, []string{"the GPU spec list cannot be read: " + err.Error()}
	}
	treeSHA, err := fileSHA256(gpuSpecFile)
	if err != nil {
		return nil, []string{"the GPU spec list cannot be hashed: " + err.Error()}
	}
	var l uiLedger
	if err := readJSON(filepath.Join(dir, suiteGPUSpecs+"-tests.json"), &l); err != nil {
		return nil, []string{"the GPU gate ran on the GPU host and left no gpu-specs ledger: " + err.Error()}
	}
	problems := dedupe(checkGPUSpecsLedger(l, tree, treeSHA, tier))
	var out []string
	for _, pr := range problems {
		out = append(out, "gpu-specs: "+pr)
	}
	l.Problems = problems
	return &l, out
}

// mergeGPUSpecs judges the GPU spec gate: where the GPU gate ran on the GPU
// host, the gpu-specs ledger its part wrote; elsewhere nothing runs, and the
// host-deferral stands in the Playwright ledger (mergeUI), which the UI plan
// must agree with: its Host flag is whether the gate ran.
func (m *merger) mergeGPUSpecs() {
	host := m.gpu != nil && m.gpu.GPUHost
	if m.uiPlan != nil && m.uiPlan.GPUSpecs != nil && m.uiPlan.GPUSpecs.Host != host {
		m.problems = append(m.problems, fmt.Sprintf("the UI plan was made for GPU host %t and the gpu ledger's gate ran on it %t", m.uiPlan.GPUSpecs.Host, host))
	}
	if !host {
		return
	}
	l, problems := readGPUSpecsLedger(m.gpuDir, m.plan.Tier)
	m.problems = append(m.problems, problems...)
	m.gpuSpecs = l
	m.proof.GPUSpecsGateRan = l != nil
}

// buildGPUSpecsLedger judges the reports of the GPU spec gate against the
// listing of the listed spec files: every listed test passed, every row is a
// file the listing holds, and the listing holds no other file.
func buildGPUSpecsLedger(list, report pwReport, rows []nightlySpecEntry, guardRetries, ceiling int) uiLedger {
	l := buildUILedger(list, report, guardRetries, ceiling)
	l.Suite = suiteGPUSpecs
	listed := map[string]bool{}
	for _, s := range list.Suites {
		listed[s.File] = true
	}
	l.Problems = append(l.Problems, specListProblems("GPU spec list", rows, listed, false, nil)...)
	on := map[string]bool{}
	for _, e := range rows {
		on[e.file()] = true
	}
	files := make([]string, 0, len(listed))
	for f := range listed {
		files = append(files, f)
	}
	sort.Strings(files)
	for _, f := range files {
		if !on[f] {
			l.Problems = append(l.Problems, fmt.Sprintf("%s is listed but not on the GPU spec list", f))
		}
	}
	return l
}

// cmdGPUSpecs is `proofrun gpu-specs`: the GPU gate's Playwright half, run
// only on the GPU host after the Go tests. scripts/proof.sh has run each listed
// spec file on its own and listed them; this reads that listing and the
// reports and writes gpu-specs-tests.json. With no row it needs neither and
// writes an empty ledger, so a proof on the GPU host always has one.
func cmdGPUSpecs(args []string) int {
	fs := flag.NewFlagSet("gpu-specs", flag.ContinueOnError)
	out := fs.String("out", "", "the proof directory: gpu-specs-tests.json goes here (required)")
	specs := fs.String("specs", gpuSpecFile, "this tree's GPU spec list")
	listFile := fs.String("list", "", "`playwright test --list --reporter=json` output of the listed spec files (required with rows)")
	var reportFiles []string
	fs.Func("report", "the JSON reporter's output of one spec file's run; repeatable", func(v string) error {
		reportFiles = append(reportFiles, v)
		return nil
	})
	tier := fs.String("tier", tierFull, "full, nightly, release or gpu")
	logFile := fs.String("log", "", "the Playwright runs' log, where the network-change guard prints its repeats")
	config := fs.String("config", "ui/playwright.config.ts", "where NETWORK_CHANGE_RETRY_CEILING is declared")
	if err := fs.Parse(args); err != nil || fs.NArg() > 0 || *out == "" {
		fmt.Fprintln(os.Stderr, "proofrun gpu-specs -out DIR [-specs FILE] [-list FILE -report FILE...] [-log FILE -config FILE] [-tier TIER]")
		return 2
	}
	switch *tier {
	case tierFull, tierNightly, tierRelease, tierGPU:
	default:
		fmt.Fprintln(os.Stderr, "proofrun gpu-specs: -tier must be full, nightly, release or gpu")
		return 2
	}
	rows, err := readGPUSpecs(*specs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gpu-specs:", err)
		return 2
	}
	treeSHA, err := fileSHA256(*specs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gpu-specs:", err)
		return 2
	}
	l := uiLedger{Suite: suiteGPUSpecs, Problems: []string{}, Tests: []uiEntry{}}
	if len(rows) > 0 {
		if *listFile == "" || len(reportFiles) == 0 {
			fmt.Fprintln(os.Stderr, "proofrun gpu-specs: the list has rows: give -list and a -report for each spec file run")
			return 2
		}
		var list, report pwReport
		if err := readJSON(*listFile, &list); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun gpu-specs: the listing is unreadable:", err)
			return 1
		}
		for _, f := range reportFiles {
			var part pwReport
			if err := readJSON(f, &part); err != nil {
				fmt.Fprintln(os.Stderr, "proofrun gpu-specs: a report is unreadable:", err)
				return 1
			}
			report = mergeReports(report, part)
		}
		retries, ceiling := 0, 0
		if *logFile == "" && len(rows) > 0 {
			fmt.Fprintln(os.Stderr, "proofrun gpu-specs: -log is required when the list has rows: the network-change guard is read from it")
			return 2
		}
		if *logFile != "" {
			if ceiling, err = retryCeiling(*config); err != nil {
				fmt.Fprintln(os.Stderr, "proofrun gpu-specs:", err)
				return 1
			}
			if retries, err = countGuardRetries(*logFile); err != nil {
				fmt.Fprintln(os.Stderr, "proofrun gpu-specs:", err)
				return 1
			}
		}
		l = buildGPUSpecsLedger(list, report, rows, retries, ceiling)
	}
	l.ListSHA256, l.GPUHost, l.GPUGateRan = treeSHA, true, true
	checked := l
	checked.Problems = nil
	l.Problems = dedupe(append(l.Problems, checkGPUSpecsLedger(checked, rows, treeSHA, *tier)...))
	if l.Problems == nil {
		l.Problems = []string{}
	}
	if err := writeJSON(filepath.Join(*out, suiteGPUSpecs+"-tests.json"), l); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gpu-specs:", err)
		return 1
	}
	fmt.Fprintf(stdout, "gpu-specs ledger: %d spec file(s) listed, %d listed tests, %d pass\n", len(rows), l.Listed, l.Pass)
	for _, p := range l.Problems {
		fmt.Fprintln(stdout, "  -", strings.TrimSpace(p))
	}
	if len(l.Problems) > 0 {
		return 1
	}
	return 0
}
