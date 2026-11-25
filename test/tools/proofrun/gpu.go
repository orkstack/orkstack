package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// The GPU-host tier (AGENTS.md §11, docs/PROOFS.md, "The GPU-host tier"):
// the tests scripts/proof-gpu-tests.txt lists need the GPU host, so a proof
// without ORK_PROOF_GPU_HOST=1 records each entry both the base's and the
// tree's list carry as host-deferred, and an entry only the tree carries as
// having no outcome. A GPU-host-tier proof — a full proof with the flag, or
// the GPU gate alone (`--tier gpu`, `make proof-gpu`) — runs them, one at a
// time, compiled with -tags=integration,gpuhost. Its ledger is the "gpu"
// suite's, and a change touching a path the list's `paths:` header names
// merges only with a passing GPU-host-tier proof of its tree (verify -gpu,
// `make merge-check GPU=`).

const (
	suiteGPU            = "gpu"
	tierGPU             = "gpu"
	outcomeHostDeferred = "host-deferred"
	gpuListFile         = "scripts/proof-gpu-tests.txt"
	gpuBaseFileName     = "gpu-base.txt"
	gpuBuildTags        = "integration,gpuhost"
	gpuTestTimeout      = 20 * time.Minute
)

// gpuList is the parsed GPU-host list: its rows, in the nightly list's
// format, and the paths its `paths:` header names.
type gpuList struct {
	Entries []nightlyEntry
	Paths   []string
}

// readGPUList parses file; an absent file is an empty list, because the base
// of a change may predate the list.
func readGPUList(file string) (gpuList, error) {
	if file == "" {
		return gpuList{}, nil
	}
	b, err := os.ReadFile(file)
	if os.IsNotExist(err) {
		return gpuList{}, nil
	}
	if err != nil {
		return gpuList{}, err
	}
	return parseGPUList(b, file)
}

// parseGPUList parses the rows as strictly as the nightly list, and the one
// `# paths:` header: its patterns, on its line and on the comment lines that
// follow it indented by at least two spaces after the `#`. A pattern is a
// relative, clean, slash-separated path.Match pattern with no `..`. A list
// with rows must name its paths.
func parseGPUList(b []byte, name string) (gpuList, error) {
	entries, err := parseNightlyList(bytes.NewReader(b), name)
	if err != nil {
		return gpuList{}, err
	}
	var paths []string
	header := 0
	in := false
	sc := bufio.NewScanner(bytes.NewReader(b))
	n := 0
	for sc.Scan() {
		n++
		text := sc.Text()
		var fields string
		switch {
		case strings.HasPrefix(text, "# paths:"):
			if header != 0 {
				return gpuList{}, fmt.Errorf("%s:%d: a second paths: header (the first is on line %d)", name, n, header)
			}
			header, in = n, true
			fields = strings.TrimPrefix(text, "# paths:")
		case in && strings.HasPrefix(text, "#  "):
			fields = strings.TrimPrefix(text, "#")
		default:
			in = false
			continue
		}
		for _, p := range strings.Fields(fields) {
			if err := checkGPUPathPattern(p); err != nil {
				return gpuList{}, fmt.Errorf("%s:%d: paths: %q %v", name, n, p, err)
			}
			paths = append(paths, p)
		}
	}
	if err := sc.Err(); err != nil {
		return gpuList{}, err
	}
	if header != 0 && len(paths) == 0 {
		return gpuList{}, fmt.Errorf("%s:%d: the paths: header names no path", name, header)
	}
	if len(entries) > 0 && header == 0 {
		return gpuList{}, fmt.Errorf("%s: a list with entries names the paths a change must prove on the GPU host in a \"# paths:\" header", name)
	}
	return gpuList{Entries: entries, Paths: paths}, nil
}

func checkGPUPathPattern(p string) error {
	// A pattern ending in "/" names a directory and everything under it.
	if dir, ok := strings.CutSuffix(p, "/"); ok {
		if dir == "" || strings.ContainsAny(dir, "*?[") {
			return fmt.Errorf("is not a directory prefix")
		}
		p = dir
	}
	switch {
	case strings.HasPrefix(p, "/"):
		return fmt.Errorf("is absolute")
	case strings.Contains(p, `\`):
		return fmt.Errorf("has a backslash")
	case path.Clean(p) != p:
		return fmt.Errorf("is not a clean path")
	case p == ".." || strings.HasPrefix(p, "../") || strings.Contains(p, "/../") || strings.HasSuffix(p, "/.."):
		return fmt.Errorf("leaves the repository")
	}
	if _, err := path.Match(p, ""); err != nil {
		return fmt.Errorf("is not a pattern: %v", err)
	}
	return nil
}

// gpuPathMatches is whether pattern p covers file f: a directory prefix (p
// ends in "/") covers everything under it, any other p is a path.Match
// pattern over the whole path.
func gpuPathMatches(p, f string) bool {
	if strings.HasSuffix(p, "/") {
		return strings.HasPrefix(f, p)
	}
	ok, _ := path.Match(p, f)
	return ok
}

// touchesGPUPaths returns the changed files a paths: pattern matches.
func touchesGPUPaths(patterns, changed []string) []string {
	var hit []string
	for _, f := range changed {
		for _, p := range patterns {
			if gpuPathMatches(p, f) {
				hit = append(hit, f)
				break
			}
		}
	}
	return hit
}

// gpuHarnessPaths are covered whatever any list's header says: the list
// itself and the code that judges it, so a change can never remove its own
// obligation by editing either.
var gpuHarnessPaths = []string{
	gpuListFile,
	gpuSpecFile,
	"test/tools/proofrun/gpu*.go",
	"scripts/proof.sh",
	"scripts/proof-part.sh",
	"scripts/gpu-host-setup.sh",
}

// gitFileAt is file's bytes at commit, and whether the commit has it. A
// commit the repository does not hold is an error, never an absent file.
func gitFileAt(commit, file string) ([]byte, bool, error) {
	if commit == "" {
		return nil, false, fmt.Errorf("no commit is known")
	}
	if err := exec.Command("git", "cat-file", "-e", commit+"^{commit}").Run(); err != nil {
		return nil, false, fmt.Errorf("commit %s is not in this repository: %v", commit, err)
	}
	if exec.Command("git", "cat-file", "-e", commit+":"+file).Run() != nil {
		return nil, false, nil
	}
	b, err := exec.Command("git", "show", commit+":"+file).Output()
	if err != nil {
		return nil, false, fmt.Errorf("git show %s:%s: %v", commit, file, err)
	}
	return b, true, nil
}

// gpuBaseCopyProblems holds the base's copy of the list a proof read to the
// base commit itself: the copy must be byte for byte the list at that commit,
// or empty where the commit has none (scripts/proof.sh writes it so).
func gpuBaseCopyProblems(baseFile, baseCommit string) []string {
	return listBaseCopyProblems("GPU-host list", gpuListFile, baseFile, baseCommit)
}

// listBaseCopyProblems is gpuBaseCopyProblems for any list file, named label.
func listBaseCopyProblems(label, listFile, baseFile, baseCommit string) []string {
	want, _, err := gitFileAt(baseCommit, listFile)
	if err != nil {
		return []string{"the base's " + label + " cannot be read from the base commit to check the proof's copy: " + err.Error()}
	}
	got, err := os.ReadFile(baseFile)
	if os.IsNotExist(err) {
		got, err = nil, nil
	}
	if err != nil {
		return []string{"the proof's copy of the base's " + label + " cannot be read: " + err.Error()}
	}
	if !bytes.Equal(got, want) {
		return []string{fmt.Sprintf("the proof's copy of the base's %s (%s) is not %s at the base commit %s", label, baseFile, listFile, short(baseCommit))}
	}
	return nil
}

// gpuDeferredLedger is the gpu ledger of a proof that did not run the gate:
// each entry both lists carry is host-deferred, and each entry only the tree
// carries has no outcome, which fails the proof (a change adding an entry
// lands through a GPU-host-tier proof).
func gpuDeferredLedger(tree, base []nightlyEntry) ledger {
	inBase := map[testKey]bool{}
	for _, e := range base {
		inBase[e.key()] = true
	}
	l := ledger{Suite: suiteGPU, Tests: []entry{}, Problems: []string{}}
	for _, e := range tree {
		l.Summary.Expected++
		if inBase[e.key()] {
			l.Tests = append(l.Tests, entry{Package: e.Package, Test: e.Test, Outcome: outcomeHostDeferred, Reason: e.Text})
			l.Summary.Deferred++
			continue
		}
		l.Tests = append(l.Tests, entry{Package: e.Package, Test: e.Test, Outcome: outcomeNone,
			Reason: "the change adds it to " + gpuListFile + ": only a GPU-host-tier proof (ORK_PROOF_GPU_HOST=1) runs it"})
		l.Summary.NoOutcome++
		l.Problems = append(l.Problems, fmt.Sprintf("%s is on this tree's GPU-host list and not on the base's: it needs a GPU-host-tier proof", e.key()))
	}
	return l
}

// checkGPULedger holds a gpu ledger to the two lists it was judged on,
// re-derived from the files themselves: the hashes it recorded, exactly the
// tree's entries, a host-deferred entry only where both lists carry it and
// the gate did not run (never in the gpu tier), and a pass only from a gate
// that ran on the GPU host.
func checkGPULedger(l ledger, tree, base []nightlyEntry, treeSHA, baseSHA, tier string) []string {
	var p []string
	if l.Suite != suiteGPU {
		p = append(p, fmt.Sprintf("the gpu ledger is of suite %q", l.Suite))
	}
	if l.ListSHA256 != treeSHA {
		p = append(p, fmt.Sprintf("the gpu ledger read the GPU-host list %s, and this tree's is %s", orDash(short(l.ListSHA256)), orDash(short(treeSHA))))
	}
	if l.BaseSHA256 != baseSHA {
		p = append(p, fmt.Sprintf("the gpu ledger read the base's GPU-host list %s, and the base's is %s", orDash(short(l.BaseSHA256)), orDash(short(baseSHA))))
	}
	if tier == tierGPU && !l.GPUHost {
		p = append(p, "a gpu-tier proof whose gate did not run on the GPU host")
	}
	if l.GPUHost && !l.GPUGateRan {
		p = append(p, "the GPU-host flag was set and the GPU gate did not run")
	}
	inTree, inBase := map[testKey]bool{}, map[testKey]bool{}
	for _, e := range tree {
		inTree[e.key()] = true
	}
	for _, e := range base {
		inBase[e.key()] = true
	}
	seen := map[testKey]bool{}
	for _, e := range l.Tests {
		k := testKey{e.Package, e.Test}
		if seen[k] {
			p = append(p, fmt.Sprintf("%s is in the gpu ledger twice", k))
			continue
		}
		seen[k] = true
		if !inTree[k] {
			p = append(p, fmt.Sprintf("%s is in the gpu ledger and not on this tree's GPU-host list", k))
			continue
		}
		switch e.Outcome {
		case outcomePass:
			if !l.GPUHost || !l.GPUGateRan {
				p = append(p, fmt.Sprintf("%s passed in a gpu ledger whose gate did not run on the GPU host", k))
			}
		case outcomeHostDeferred:
			switch {
			case tier == tierGPU:
				p = append(p, fmt.Sprintf("%s is host-deferred in a GPU-host-tier proof", k))
			case l.GPUHost:
				p = append(p, fmt.Sprintf("%s is host-deferred in a proof that ran on the GPU host", k))
			case !inBase[k]:
				p = append(p, fmt.Sprintf("%s is host-deferred, and the base's GPU-host list does not carry it: an entry the change adds runs on the GPU host", k))
			}
		default:
			p = append(p, fmt.Sprintf("%s has outcome %q in the gpu ledger", k, orDash(e.Outcome)))
		}
	}
	keys := make([]testKey, 0, len(inTree))
	for k := range inTree {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, k := range keys {
		if !seen[k] {
			p = append(p, fmt.Sprintf("%s is on this tree's GPU-host list and not in the gpu ledger", k))
		}
	}
	return p
}

// gpuListProblems checks the list against the gpuhost binaries' listings:
// every entry is a listed test of its package, and every covering test is a
// listed test of the same package not itself on the list.
func gpuListProblems(entries []nightlyEntry, listed map[string]map[string]bool) []string {
	var problems []string
	on := map[testKey]bool{}
	for _, e := range entries {
		on[e.key()] = true
	}
	for _, e := range entries {
		names, compiled := listed[e.Package]
		if !compiled {
			problems = append(problems, fmt.Sprintf("GPU-host list line %d names package %s, which was not compiled", e.Line, e.Package))
			continue
		}
		if !names[e.Test] {
			problems = append(problems, fmt.Sprintf("GPU-host list line %d names %s, which its package built with -tags=%s does not list", e.Line, e.key(), gpuBuildTags))
		}
		for _, c := range e.Covering {
			switch {
			case !names[c]:
				problems = append(problems, fmt.Sprintf("GPU-host list line %d: covering test %s.%s is not a listed test of that package", e.Line, e.Package, c))
			case on[testKey{e.Package, c}]:
				problems = append(problems, fmt.Sprintf("GPU-host list line %d: covering test %s.%s is itself on the GPU-host list", e.Line, e.Package, c))
			}
		}
	}
	return problems
}

// cmdGPU is `proofrun gpu`: the GPU gate. Without -host it writes the
// host-deferred ledger; with -host (ORK_PROOF_GPU_HOST=1, whose preflight
// scripts/proof.sh ran) it compiles the listed packages with
// -tags=integration,gpuhost, checks the list against their listings, and runs
// every listed test, one process at a time.
func cmdGPU(args []string) int {
	fs := flag.NewFlagSet("gpu", flag.ContinueOnError)
	out := fs.String("out", "", "the proof directory: gpu-tests.json goes here (required)")
	listFile := fs.String("list", gpuListFile, "this tree's GPU-host list")
	baseFile := fs.String("base", "", "the base's copy of the GPU-host list (required)")
	host := fs.Bool("host", false, "run the listed tests: this host is the GPU host (ORK_PROOF_GPU_HOST=1)")
	tier := fs.String("tier", tierFull, "full, nightly, release or gpu")
	binDir := fs.String("bin", "", "where the gpuhost test binaries are compiled (default <out>/bin/test-gpu)")
	printProgress := fs.Bool("print-progress", false, "print proofrun: start/outcome lines as tests run")
	if err := fs.Parse(args); err != nil || fs.NArg() > 0 || *out == "" || *baseFile == "" {
		fmt.Fprintln(os.Stderr, "proofrun gpu -out DIR -base FILE [-list FILE] [-host] [-tier TIER]")
		return 2
	}
	switch *tier {
	case tierFull, tierNightly, tierRelease:
	case tierGPU:
		if !*host {
			fmt.Fprintln(os.Stderr, "proofrun gpu: -tier gpu runs the gate on the GPU host: give -host")
			return 2
		}
	default:
		fmt.Fprintln(os.Stderr, "proofrun gpu: -tier must be full, nightly, release or gpu")
		return 2
	}
	tree, err := readGPUList(*listFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gpu:", err)
		return 2
	}
	base, err := readGPUList(*baseFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gpu:", err)
		return 2
	}
	treeSHA, err := fileSHA256(*listFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gpu:", err)
		return 2
	}
	baseSHA, err := fileSHA256(*baseFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun gpu:", err)
		return 2
	}
	var l ledger
	if !*host {
		l = gpuDeferredLedger(tree.Entries, base.Entries)
	} else {
		dir := *binDir
		if dir == "" {
			dir = filepath.Join(*out, "bin", "test-gpu")
		}
		var progress io.Writer
		if *printProgress {
			progress = stdout
		}
		l = runGPUGate(*out, dir, tree.Entries, progress)
		l.GPUHost, l.GPUGateRan = true, true
	}
	l.ListSHA256, l.BaseSHA256 = treeSHA, baseSHA
	l.Problems = append(l.Problems, checkGPULedger(l, tree.Entries, base.Entries, treeSHA, baseSHA, *tier)...)
	l.Problems = dedupe(l.Problems)
	return writeLedgerAndReport(*out, l)
}

// runGPUGate compiles, lists and runs the GPU-host tests, serially.
func runGPUGate(out, binDir string, entries []nightlyEntry, progress io.Writer) ledger {
	l := ledger{Suite: suiteGPU, Tests: []entry{}, Problems: []string{}}
	if len(entries) == 0 {
		return l
	}
	var importPaths []string
	seen := map[string]bool{}
	for _, e := range entries {
		if !seen[e.Package] {
			seen[e.Package] = true
			importPaths = append(importPaths, e.Package)
		}
	}
	lines, err := goList(gpuBuildTags, `{{.ImportPath}}{{"\t"}}{{.Dir}}`, importPaths...)
	if err != nil {
		l.Problems = append(l.Problems, err.Error())
		return l
	}
	var pkgs []pkgInfo
	dirs := map[string]string{}
	for _, line := range lines {
		f := strings.SplitN(line, "\t", 2)
		if len(f) == 2 {
			pkgs = append(pkgs, pkgInfo{ImportPath: f[0], Dir: f[1]})
			dirs[f[0]] = f[1]
		}
	}
	bins, err := compileTags(pkgs, binDir, gpuBuildTags, stdout)
	if err != nil {
		l.Problems = append(l.Problems, err.Error())
		return l
	}
	listed := map[string]map[string]bool{}
	for _, p := range pkgs {
		names, err := list(bins[p.ImportPath], p.Dir)
		if err != nil {
			l.Problems = append(l.Problems, err.Error())
			return l
		}
		listed[p.ImportPath] = map[string]bool{}
		for _, n := range names {
			listed[p.ImportPath][n] = true
		}
	}
	problems := gpuListProblems(entries, listed)
	var expected []testKey
	var jobs []job
	for i, e := range entries {
		expected = append(expected, e.key())
		if listed[e.Package][e.Test] {
			jobs = append(jobs, job{ID: fmt.Sprintf("gpu-%02d", i+1), Package: e.Package, Tests: []string{e.Test},
				Timeout: gpuTestTimeout, TimeoutSeconds: gpuTestTimeout.Seconds(), Shard: 1, Attempt: 1})
		}
	}
	if err := os.MkdirAll(filepath.Join(out, suiteGPU), 0o755); err != nil {
		l.Problems = append(l.Problems, err.Error())
		return l
	}
	events, err := os.Create(filepath.Join(out, "gpu.events.jsonl"))
	if err != nil {
		l.Problems = append(l.Problems, err.Error())
		return l
	}
	// One process at a time, in list order: the tests share the host's GPUs
	// and its GPU host lock, so no two may run at once.
	r := &runner{
		slots: 1, baseEnv: os.Environ(), bins: bins, dirs: dirs,
		jobDir: filepath.Join(out, suiteGPU), c: newCollector(), events: events, log: stdout,
		progress: progress,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	runSerially(ctx, r, jobs)
	stop()
	if err := events.Close(); err != nil {
		problems = append(problems, "the gpu gate's event log could not be closed: "+err.Error())
	}
	built := buildLedgerQ(suiteGPU, expected, listed, r.c, nil, nil)
	built.Problems = append(problems, built.Problems...)
	return built
}

// runSerially runs jobs one after another, and none once ctx ends: a test a
// signal kept from running has no result, so its ledger entry has no outcome
// and the gate fails (buildLedgerQ).
func runSerially(ctx context.Context, r *runner, jobs []job) {
	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		r.runAll(ctx, []job{j})
	}
}

func dedupe(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]bool{}
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// mergeGPU is the gpu ledger of a merged proof: the one a part's GPU gate
// wrote (the gpu part, or the local part), or, when no gate ran, the
// host-deferred ledger of the two lists. Either way it is judged against the
// tree's list and the base's copy beside the plan, read from the files.
func (m *merger) mergeGPU() {
	tree, err := readGPUList(gpuListFile)
	if err != nil {
		m.problems = append(m.problems, "the GPU-host list cannot be read: "+err.Error())
		return
	}
	baseFile := filepath.Join(filepath.Dir(m.planPath), gpuBaseFileName)
	if _, err := os.Stat(baseFile); err != nil && len(tree.Entries) > 0 {
		m.problems = append(m.problems, "the base's copy of the GPU-host list ("+gpuBaseFileName+") is not beside the plan, so no entry can be host-deferred: "+err.Error())
	}
	base, err := readGPUList(baseFile)
	if err != nil {
		m.problems = append(m.problems, "the base's GPU-host list cannot be read: "+err.Error())
		return
	}
	treeSHA, err := fileSHA256(gpuListFile)
	if err != nil {
		m.problems = append(m.problems, "the GPU-host list cannot be hashed: "+err.Error())
		return
	}
	baseSHA, err := fileSHA256(baseFile)
	if err != nil {
		m.problems = append(m.problems, "the base's GPU-host list cannot be hashed: "+err.Error())
		return
	}
	// The two lists are judged only as the plan read them, and the base's
	// copy only as the base commit holds it: a copy swapped into the plan
	// artifact would otherwise host-defer the entries a change adds.
	m.problems = append(m.problems, gpuAnchorProblems(m.plan.GPU, treeSHA, baseSHA, len(tree.Entries) > 0)...)
	if len(tree.Entries) > 0 || len(base.Entries) > 0 {
		m.problems = append(m.problems, gpuBaseCopyProblems(baseFile, m.plan.BaseCommit)...)
	}
	var l *ledger
	for _, part := range []string{suiteGPU, "local"} {
		for _, reading := range m.resultReadings(part) {
			var got ledger
			if readJSON(filepath.Join(reading.Dir, suiteGPU+"-tests.json"), &got) == nil {
				l = &got
				m.gpuDir = reading.Dir
				break
			}
			for _, g := range reading.Record.Gates {
				if g.Gate == suiteGPU {
					m.problems = append(m.problems, "part "+part+" ran the gpu gate and left no gpu ledger")
				}
			}
		}
		if l != nil {
			break
		}
	}
	if l == nil {
		d := gpuDeferredLedger(tree.Entries, base.Entries)
		d.ListSHA256, d.BaseSHA256 = treeSHA, baseSHA
		l = &d
	}
	l.Problems = dedupe(append(l.Problems, checkGPULedger(*l, tree.Entries, base.Entries, treeSHA, baseSHA, m.plan.Tier)...))
	for _, p := range l.Problems {
		m.problems = append(m.problems, "gpu: "+p)
	}
	m.gpu = l
	m.proof.GPUHost, m.proof.GPUGateRan = l.GPUHost, l.GPUGateRan
}

// mergeGPUTierProof is the verdict of a GPU-host-tier proof (`--tier gpu`,
// `make proof-gpu`): no integration plan, one local part, the gpu gate's
// ledger, judged against the tree's list and the base's copy in the proof
// directory. It is never merge evidence; it is what `verify -gpu` reads.
func mergeGPUTierProof(partsDir, out, name string) int {
	parts, _, err := readParts(partsDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	m := &merger{parts: parts}
	proof := &proofRecord{Name: name, Tier: tierGPU, Gates: []gateRecord{}, Ledgers: map[string]any{}, Ineligible: []string{}}
	m.proof = proof
	var problems []string
	readings := m.resultReadings("local")
	if len(readings) != 1 {
		problems = append(problems, "the proof left no readable local part")
	} else {
		rec := readings[0].Record
		proof.Commit, proof.Branch = rec.Commit, rec.Branch
		proof.Tree, proof.TreeEnd, proof.CleanStart, proof.CleanEnd = rec.Tree, rec.TreeEnd, rec.CleanStart, rec.CleanEnd
		proof.BaseRef, proof.BaseCommit, proof.MergeBase = rec.BaseRef, rec.BaseCommit, rec.MergeBase
		proof.Gates, proof.GatesOverride = rec.Gates, rec.GatesOverride
		proof.Parts = []string{"local"}
		if rec.Tier != tierGPU {
			problems = append(problems, "the local part is of tier "+orDash(rec.Tier)+", not "+tierGPU)
		}
		ran := false
		for _, g := range rec.Gates {
			ran = ran || g.Gate == suiteGPU
		}
		if !ran {
			problems = append(problems, "gate gpu did not run")
		}
		tree, terr := readGPUList(gpuListFile)
		base, berr := readGPUList(filepath.Join(out, gpuBaseFileName))
		var l ledger
		switch {
		case terr != nil:
			problems = append(problems, "the GPU-host list cannot be read: "+terr.Error())
		case berr != nil:
			problems = append(problems, "the base's GPU-host list cannot be read: "+berr.Error())
		case readJSON(filepath.Join(readings[0].Dir, suiteGPU+"-tests.json"), &l) != nil:
			problems = append(problems, "the local part left no gpu ledger")
		default:
			treeSHA, terr := fileSHA256(gpuListFile)
			baseSHA, berr := fileSHA256(filepath.Join(out, gpuBaseFileName))
			if terr != nil || berr != nil {
				problems = append(problems, fmt.Sprintf("the GPU-host lists cannot be hashed: %v %v", terr, berr))
			}
			problems = append(problems, gpuBaseCopyProblems(filepath.Join(out, gpuBaseFileName), rec.BaseCommit)...)
			l.Problems = dedupe(append(l.Problems, checkGPULedger(l, tree.Entries, base.Entries, treeSHA, baseSHA, tierGPU)...))
			m.gpu = &l
			proof.GPUHost, proof.GPUGateRan = l.GPUHost, l.GPUGateRan
			proof.Ledgers.(map[string]any)[suiteGPU] = map[string]any{"summary": l.Summary, "problems": len(l.Problems)}
			for _, p := range l.Problems {
				problems = append(problems, "gpu: "+p)
			}
			sl, sp := readGPUSpecsLedger(readings[0].Dir, tierGPU)
			problems = append(problems, sp...)
			m.gpuSpecs = sl
			proof.GPUSpecsGateRan = sl != nil
			if sl != nil {
				if ledgers, ok := proof.Ledgers.(map[string]any); ok {
					ledgers[suiteGPUSpecs] = map[string]any{"listed": sl.Listed, "pass": sl.Pass, "problems": len(sl.Problems)}
				} else {
					problems = append(problems, "the proof record's ledgers are not a map")
				}
			}
		}
	}
	status := 0
	if len(problems) > 0 {
		status = 1
		proof.Ineligible = append(proof.Ineligible, problems...)
	}
	ledgersOK := map[string]bool{}
	if m.gpu != nil {
		ledgersOK[suiteGPU] = m.gpu.ok()
	}
	if m.gpuSpecs != nil {
		ledgersOK[suiteGPUSpecs] = len(m.gpuSpecs.Problems) == 0
	}
	verdict(proof, status, ledgersOK)
	if err := os.MkdirAll(out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	if err := writeJSON(filepath.Join(out, "proof.json"), proof); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	if m.gpu != nil {
		if err := writeJSON(filepath.Join(out, suiteGPU+"-tests.json"), m.gpu); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun merge:", err)
			return 2
		}
	}
	if m.gpuSpecs != nil {
		if err := writeJSON(filepath.Join(out, suiteGPUSpecs+"-tests.json"), m.gpuSpecs); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun merge:", err)
			return 2
		}
	}
	if err := writeMergeREADME(filepath.Join(out, "README.md"), proof, m); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun merge:", err)
		return 2
	}
	if proof.Result != "pass" {
		return 1
	}
	return 0
}

// changedFiles is every path HEAD changes against base, with renames
// listed as a deletion and an addition, so a file moved away from a covered
// path is still a covered path changed.
func changedFiles(base string) ([]string, error) {
	if base == "" {
		return nil, fmt.Errorf("main's tip is not known")
	}
	out, err := exec.Command("git", "diff", "--no-renames", "--name-only", "-z", base, "HEAD").Output()
	if err != nil {
		return nil, fmt.Errorf("git diff --no-renames --name-only -z %s HEAD: %v", base, err)
	}
	return splitNUL(out), nil
}

func splitNUL(b []byte) []string {
	var files []string
	for _, f := range bytes.Split(b, []byte{0}) {
		if len(f) > 0 {
			files = append(files, string(f))
		}
	}
	return files
}

// mergeGPUProblems is merge-check's GPU rule: a candidate whose changed
// files touch a path that main's tip's GPU-host list, the tree's list or the
// harness itself covers needs a passing GPU-host-tier proof of its tree
// (GPU=), and one that is named is checked whatever the candidate touches.
// Whatever cannot be read — main's tip, its list, the diff — requires it.
func mergeGPUProblems(gpuDir, tree, mainTip string) []string {
	var problems, required []string
	treeList, err := readGPUList(gpuListFile)
	if err != nil {
		problems = append(problems, "the GPU-host list cannot be read: "+err.Error())
	}
	_, treeHas := os.Stat(gpuListFile)
	paths := append(append([]string{}, gpuHarnessPaths...), treeList.Paths...)
	tipBytes, tipHas, err := gitFileAt(mainTip, gpuListFile)
	switch {
	case err != nil:
		required = append(required, "main's tip's GPU-host list cannot be read ("+err.Error()+")")
	case tipHas:
		tipList, err := parseGPUList(tipBytes, mainBranch+":"+gpuListFile)
		if err != nil {
			required = append(required, "main's tip's GPU-host list cannot be parsed ("+err.Error()+")")
		}
		paths = append(paths, tipList.Paths...)
		if os.IsNotExist(treeHas) {
			problems = append(problems, "the change deletes "+gpuListFile+", which main's tip carries: the GPU-host list is never removed by a merge")
		}
	}
	// The spec twin (gpuspecs.go): its list and every spec either list names
	// are covered paths too, so a listed spec cannot change without a proof.
	treeSpecs, err := readGPUSpecs(gpuSpecFile)
	if err != nil {
		problems = append(problems, "the GPU spec list cannot be read: "+err.Error())
	}
	var tipSpecs []nightlySpecEntry
	specBytes, specHas, err := gitFileAt(mainTip, gpuSpecFile)
	switch {
	case err != nil:
		required = append(required, "main's tip's GPU spec list cannot be read ("+err.Error()+")")
	case specHas:
		if tipSpecs, err = parseGPUSpecs(specBytes, mainBranch+":"+gpuSpecFile); err != nil {
			required = append(required, "main's tip's GPU spec list cannot be parsed ("+err.Error()+")")
		}
		if _, statErr := os.Stat(gpuSpecFile); statErr != nil {
			if os.IsNotExist(statErr) {
				problems = append(problems, "the change deletes "+gpuSpecFile+", which main's tip carries: the GPU spec list is never removed by a merge")
			} else {
				problems = append(problems, "the GPU spec list cannot be examined: "+statErr.Error())
			}
		}
	}
	paths = append(paths, gpuSpecPaths(treeSpecs, tipSpecs)...)
	changed, err := changedFiles(mainTip)
	if err != nil {
		required = append(required, "the changed files cannot be listed ("+err.Error()+")")
	}
	return append(problems, gpuMergeRule(paths, gpuDir, tree, changed, required)...)
}

// gpuMergeRule is mergeGPUProblems over the covered paths: pure but for
// reading the named proof. required are reasons, besides a touched path,
// that a GPU-host proof is needed.
func gpuMergeRule(paths []string, gpuDir, tree string, changed, required []string) []string {
	touched := append(touchesGPUPaths(paths, changed), required...)
	if gpuDir == "" {
		if len(touched) == 0 {
			return nil
		}
		shown := touched
		if len(shown) > 5 {
			shown = append(append([]string{}, shown[:5]...), fmt.Sprintf("and %d more", len(touched)-5))
		}
		return []string{fmt.Sprintf("the change touches the GPU-host paths %s covers (%s): it merges only with a passing GPU-host-tier proof of its tree, GPU=proof/<name> (make proof-gpu, or a full proof with ORK_PROOF_GPU_HOST=1)",
			gpuListFile, strings.Join(shown, ", "))}
	}
	return gpuProofProblems(gpuDir, tree)
}

// gpuProofProblems is what makes dir a GPU-host-tier proof of tree: a full
// proof run with ORK_PROOF_GPU_HOST=1, or a gpu-tier proof, whose GPU gate
// ran on the GPU host, that passed, held every identity rule (its tier's own
// aside, for the gpu tier) and measured exactly tree, clean.
func gpuProofProblems(dir, tree string) []string {
	var r proofRecord
	if err := readJSON(filepath.Join(dir, "proof.json"), &r); err != nil {
		return []string{fmt.Sprintf("the GPU-host proof %s is unreadable: %v", dir, err)}
	}
	var p []string
	switch r.Tier {
	case tierFull:
		if !r.MergeEligible {
			p = append(p, "the GPU-host proof is a full proof that is not merge-eligible: "+orDash(strings.Join(r.Ineligible, "; ")))
		}
	case tierGPU, tierRelease:
		if !r.EligibleButForTier {
			var reasons []string
			for _, reason := range r.Ineligible {
				if reason != tierIneligible(r.Tier) {
					reasons = append(reasons, reason)
				}
			}
			p = append(p, "the GPU-host proof is not eligible evidence: "+orDash(strings.Join(reasons, "; ")))
		}
	default:
		p = append(p, "the GPU-host proof is tier "+orDash(r.Tier)+": only a full or release proof with ORK_PROOF_GPU_HOST=1, or a gpu-tier proof, is one")
	}
	if !r.GPUHost || !r.GPUGateRan {
		p = append(p, "the GPU-host proof's GPU gate did not run on the GPU host (gpu_host and gpu_gate_ran)")
	}
	if r.GPUGateRan && !r.GPUSpecsGateRan {
		p = append(p, "the GPU-host proof's GPU spec gate did not run (gpu_specs_gate_ran): its Playwright specs were not run on the GPU host")
	}
	if r.Result != "pass" {
		p = append(p, "the GPU-host proof did not pass")
	}
	if r.Tree == "" || r.Tree != tree {
		p = append(p, fmt.Sprintf("the GPU-host proof measured tree %s, and this one is %s", orDash(short(r.Tree)), orDash(short(tree))))
	}
	if !r.CleanStart || !r.CleanEnd || r.Tree != r.TreeEnd {
		p = append(p, "the GPU-host proof's tree was not clean")
	}
	return p
}

// gpuListRef records the GPU-host lists a plan was made with, so the merger
// judges the gpu ledger on exactly those bytes.
type gpuListRef struct {
	File       string `json:"file"`
	SHA256     string `json:"sha256"`
	BaseSHA256 string `json:"base_sha256"`
}

// gpuAnchorProblems checks the lists the merger read against the hashes the
// plan recorded. A plan that recorded none fails closed while the tree's list
// has entries: their host-deferral could not be checked.
func gpuAnchorProblems(ref gpuListRef, treeSHA, baseSHA string, treeHasEntries bool) []string {
	if ref.SHA256 == "" || ref.BaseSHA256 == "" {
		if treeHasEntries {
			return []string{"the plan recorded no hash of the GPU-host list or of the base's copy: the gpu ledger's host-deferral cannot be checked"}
		}
		return nil
	}
	var p []string
	if ref.SHA256 != treeSHA {
		p = append(p, fmt.Sprintf("the GPU-host list is not the one the plan read (plan sha256 %s, tree %s)", short(ref.SHA256), orDash(short(treeSHA))))
	}
	if ref.BaseSHA256 != baseSHA {
		p = append(p, fmt.Sprintf("the base's GPU-host list beside the plan is not the one the plan hashed (plan sha256 %s, %s %s)", short(ref.BaseSHA256), gpuBaseFileName, orDash(short(baseSHA))))
	}
	return p
}
