package main

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The GPU spec list (scripts/proof-gpu-specs.txt) is the Playwright twin of
// scripts/proof-gpu-tests.txt: its rows have the nightly spec list's format.

const gpuSpecRow = "ui/e2e/orker-gpu.spec.ts\tui/e2e/orker-pool.spec.ts\t2026-11-19 Daniel Santos\tneeds a real GPU behind the Orker"

const gpuSpecHeader = "# the GPU-host spec list\n#\n#   spec<TAB>covering<TAB>approved<TAB>reason\n"

func gpuSpecEntries(t *testing.T, rows ...string) []nightlySpecEntry {
	t.Helper()
	out, err := parseGPUSpecs([]byte(gpuSpecHeader+strings.Join(rows, "\n")+"\n"), "list")
	must.OK(t, err)
	return out
}

func TestGPUSpecs_ParsesTheListStrictly(t *testing.T) {
	cases := []struct {
		name, text, err string
		rows            int
	}{
		{name: "the header alone is an empty list", text: gpuSpecHeader},
		{name: "an empty file is an empty list", text: ""},
		{name: "a row", text: gpuSpecHeader + gpuSpecRow + "\n", rows: 1},
		{name: "five columns", text: "a\t" + gpuSpecRow + "\n", err: "got 5 columns"},
		{name: "an empty column", text: "ui/e2e/a.spec.ts\t\t2026-11-19 Daniel Santos\tr\n", err: "column 2 is empty"},
		{name: "not a spec path", text: "e2e/a.spec.ts\tui/e2e/b.spec.ts\t2026-11-19 Daniel Santos\tr\n", err: "is not a spec file under ui/e2e/"},
		{name: "a covering path that is not a spec", text: "ui/e2e/a.spec.ts\tui/e2e/b.ts\t2026-11-19 Daniel Santos\tr\n", err: "is not a spec file under ui/e2e/"},
		{name: "no approval date", text: "ui/e2e/a.spec.ts\tui/e2e/b.spec.ts\tDaniel\tr\n", err: "is not \"YYYY-MM-DD <name>\""},
		{name: "a duplicate", text: gpuSpecRow + "\n" + gpuSpecRow + "\n", err: "is already listed on line 1"},
		{name: "a spec covering itself", text: "ui/e2e/a.spec.ts\tui/e2e/a.spec.ts\t2026-11-19 Daniel Santos\tr\n", err: "covers itself"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseGPUSpecs([]byte(c.text), "list")
			if c.err != "" {
				if err == nil || !strings.Contains(err.Error(), c.err) {
					t.Fatalf("err = %v, want %q", err, c.err)
				}
				return
			}
			if err != nil || len(got) != c.rows {
				t.Fatalf("entries %v, err %v, want %d rows", got, err, c.rows)
			}
		})
	}
}

func TestGPUSpecs_AnAbsentFileIsAnEmptyList(t *testing.T) {
	got, err := readGPUSpecs(filepath.Join(t.TempDir(), "nope.txt"))
	if err != nil || len(got) != 0 {
		t.Fatalf("entries %v, err %v", got, err)
	}
	if got, err := readGPUSpecs(""); err != nil || len(got) != 0 {
		t.Fatalf("no file: entries %v, err %v", got, err)
	}
}

// A listed spec must be a file the listing holds, its covering specs listed
// files on neither this list nor the nightly spec list, and no spec may be on
// both lists.
func TestGPUSpecs_ProblemsAgainstTheListing(t *testing.T) {
	gpu := gpuSpecEntries(t, gpuSpecRow,
		"ui/e2e/gone.spec.ts\tui/e2e/orker-gpu.spec.ts,ui/e2e/missing.spec.ts,ui/e2e/slow.spec.ts\t2026-11-19 Daniel Santos\tr")
	nightly, err := parseNightlySpecs(strings.NewReader(specRow+"\nui/e2e/slow.spec.ts\tui/e2e/orker-pool.spec.ts\t2026-10-26 Daniel Santos\tr\n"), "nightly")
	must.OK(t, err)
	listed := map[string]bool{
		"orker-gpu.spec.ts": true, "orker-pool.spec.ts": true, "slow.spec.ts": true,
		"run-page-ceiling.spec.ts": true, "run-page-reduced-ceiling.spec.ts": true,
	}
	got := strings.Join(gpuSpecListProblems(gpu, nightly, listed, true), "\n")
	for _, want := range []string{
		"GPU spec list line 5 names ui/e2e/gone.spec.ts, which the listing does not hold: remove the stale entry",
		"GPU spec list line 5: covering spec ui/e2e/orker-gpu.spec.ts is itself on the GPU spec list",
		"GPU spec list line 5: covering spec ui/e2e/missing.spec.ts is not a listed spec file",
		"GPU spec list line 5: covering spec ui/e2e/slow.spec.ts is on the nightly spec list",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("problems lack %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "line 4") {
		t.Errorf("the valid row is a problem:\n%s", got)
	}
	both := gpuSpecEntries(t, "ui/e2e/run-page-ceiling.spec.ts\tui/e2e/orker-pool.spec.ts\t2026-11-19 Daniel Santos\tr")
	if got := strings.Join(gpuSpecListProblems(both, nightly, listed, true), "\n"); !strings.Contains(got, "ui/e2e/run-page-ceiling.spec.ts is on both the GPU spec list and the nightly spec list") {
		t.Errorf("a spec on both lists is not refused:\n%s", got)
	}
	// A partial listing is no concern of the covering specs.
	if got := gpuSpecListProblems(gpu[:1], nil, map[string]bool{"orker-gpu.spec.ts": true}, false); len(got) != 0 {
		t.Errorf("a partial listing's problems = %v", got)
	}
	// An empty list is valid against any listing.
	if got := gpuSpecListProblems(nil, nightly, listed, true); len(got) != 0 {
		t.Errorf("an empty list's problems = %v", got)
	}
	// The nightly list's own check refuses a covering spec that the GPU list holds.
	elsewhere := map[string]string{"ui/e2e/orker-pool.spec.ts": "GPU spec list"}
	if got := strings.Join(specListProblems("nightly spec list", nightly[1:], listed, true, elsewhere), "\n"); !strings.Contains(got, "covering spec ui/e2e/orker-pool.spec.ts is on the GPU spec list") {
		t.Errorf("a nightly covering spec on the GPU list is not refused:\n%s", got)
	}
}

func TestGPUSpecs_CoveredPathsAreTheListAndItsSpecs(t *testing.T) {
	a := gpuSpecEntries(t, gpuSpecRow)
	b := gpuSpecEntries(t, "ui/e2e/other-gpu.spec.ts\tui/e2e/orker-pool.spec.ts\t2026-11-19 Daniel Santos\tr")
	got := gpuSpecPaths(a, b, a)
	want := []string{"ui/e2e/orker-gpu.spec.ts", "ui/e2e/other-gpu.spec.ts"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("paths = %v, want %v", got, want)
	}
	if got := gpuSpecPaths(nil); len(got) != 0 {
		t.Errorf("an empty list covers %v", got)
	}
}

// The spec list and the proof code are covered whatever any list says.
func TestGPUSpecs_HarnessPathsCoverTheListAndTheCode(t *testing.T) {
	for _, p := range []string{gpuSpecFile, "test/tools/proofrun/gpuspecs.go", "test/tools/proofrun/gpuspecs_test.go"} {
		if len(touchesGPUPaths(gpuHarnessPaths, []string{p})) != 1 {
			t.Errorf("%s is not a covered harness path", p)
		}
	}
}

func hostDeferredFiles(p uiPlan) []string {
	var out []string
	for _, d := range p.HostDeferred {
		out = append(out, d.File)
	}
	return out
}

// The Playwright phase never holds a GPU spec: it is left out of the run in
// every mode, recorded host-deferred only where both lists carry it, and
// planned (so it has no outcome) where only the tree's does.
func TestUIPlan_GPUSpecsAreKeptOutOfThePlaywrightPhase(t *testing.T) {
	listing := fixtureReport(
		uiTestSpec{"a.spec.ts", "one", 0, 10},
		uiTestSpec{"orker-gpu.spec.ts", "binds a unit", 0, 100},
		uiTestSpec{"orker-pool.spec.ts", "pool", 0, 5},
	)
	gpu := gpuSpecEntries(t, gpuSpecRow)
	cases := []struct {
		name         string
		mode         string
		base         []nightlySpecEntry
		host         bool
		files, tests []string
		hostDeferred []string
	}{
		{name: "defer, both lists", mode: nightlyDefer, base: gpu,
			files: []string{"a.spec.ts", "orker-pool.spec.ts"}, tests: []string{"a.spec.ts", "orker-gpu.spec.ts", "orker-pool.spec.ts"}, hostDeferred: []string{"orker-gpu.spec.ts"}},
		{name: "defer, only the tree's list: no outcome", mode: nightlyDefer, base: nil,
			files: []string{"a.spec.ts", "orker-pool.spec.ts"}, tests: []string{"a.spec.ts", "orker-gpu.spec.ts", "orker-pool.spec.ts"}},
		{name: "include (pre-release), both lists", mode: nightlyInclude, base: gpu,
			files: []string{"a.spec.ts", "orker-pool.spec.ts"}, tests: []string{"a.spec.ts", "orker-gpu.spec.ts", "orker-pool.spec.ts"}, hostDeferred: []string{"orker-gpu.spec.ts"}},
		{name: "the GPU host: the gate's ledger holds them", mode: nightlyDefer, base: gpu, host: true,
			files: []string{"a.spec.ts", "orker-pool.spec.ts"}, tests: []string{"a.spec.ts", "orker-pool.spec.ts"}},
		{name: "include on the GPU host", mode: nightlyInclude, base: gpu, host: true,
			files: []string{"a.spec.ts", "orker-pool.spec.ts"}, tests: []string{"a.spec.ts", "orker-pool.spec.ts"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, problems := buildUIPlanNightly(listing, []byte("x"), nil, "costs", "sha", 2, uiNightlyInput{
				Mode: c.mode, File: "tree.txt", Base: nil,
				GPU: &uiGPUInput{File: "gpu.txt", BaseFile: "gpu-base.txt", Tree: gpu, Base: c.base, Host: c.host}})
			if len(problems) != 0 {
				t.Fatalf("problems = %v", problems)
			}
			var files, tests, sharded []string
			for _, f := range p.Files {
				files = append(files, f.File)
			}
			for _, s := range p.Shards {
				sharded = append(sharded, s.Files...)
			}
			seen := map[string]bool{}
			for _, x := range p.Tests {
				if !seen[x.File] {
					seen[x.File] = true
					tests = append(tests, x.File)
				}
			}
			sort.Strings(sharded)
			sort.Strings(tests)
			if !reflect.DeepEqual(files, c.files) || !reflect.DeepEqual(sharded, c.files) {
				t.Errorf("files = %v, sharded %v, want %v", files, sharded, c.files)
			}
			if !reflect.DeepEqual(tests, c.tests) {
				t.Errorf("planned tests' files = %v, want %v", tests, c.tests)
			}
			if got := hostDeferredFiles(p); !reflect.DeepEqual(orEmpty(got), orEmpty(c.hostDeferred)) {
				t.Errorf("host-deferred = %v, want %v", got, c.hostDeferred)
			}
			for _, d := range p.HostDeferred {
				if d.Entry != gpuSpecRow {
					t.Errorf("host-deferred %s carries entry %q", d.File, d.Entry)
				}
			}
			if len(p.Deferred) != 0 {
				t.Errorf("a GPU spec is in the nightly deferral: %v", p.Deferred)
			}
			if p.GPUSpecs == nil || p.GPUSpecs.Host != c.host || p.GPUSpecs.File != "gpu.txt" || !reflect.DeepEqual(p.GPUSpecs.Files, []string{"orker-gpu.spec.ts"}) {
				t.Errorf("gpu specs record = %+v", p.GPUSpecs)
			}
		})
	}
	// The nightly tier lists the nightly specs alone: nothing of the GPU list
	// is planned, deferred or stale there.
	nightlyListing := fixtureReport(uiTestSpec{"run-page-ceiling.spec.ts", "at 4,000", 0, 400})
	nightlyRows, _ := parseNightlySpecs(strings.NewReader(specRow+"\n"), "n")
	p, problems := buildUIPlanNightly(nightlyListing, []byte("x"), nil, "costs", "sha", 1, uiNightlyInput{
		Mode: nightlyOnly, Tree: nightlyRows, GPU: &uiGPUInput{File: "gpu.txt", Tree: gpu}})
	if len(problems) != 0 || len(p.HostDeferred) != 0 || len(p.Files) != 1 {
		t.Errorf("nightly tier: problems %v, host-deferred %v, files %v", problems, p.HostDeferred, p.Files)
	}
	// A GPU spec the listing does not hold is stale; one on both lists is refused.
	stale := gpuSpecEntries(t, "ui/e2e/gone.spec.ts\tui/e2e/a.spec.ts\t2026-11-19 Daniel Santos\tr")
	if _, problems := buildUIPlanNightly(listing, []byte("x"), nil, "costs", "sha", 1, uiNightlyInput{
		Mode: nightlyDefer, GPU: &uiGPUInput{Tree: stale, Base: stale}}); len(problems) != 1 || !strings.Contains(problems[0], "gone.spec.ts") {
		t.Errorf("a stale GPU row's problems = %v", problems)
	}
	overlap := gpuSpecEntries(t, "ui/e2e/orker-pool.spec.ts\tui/e2e/a.spec.ts\t2026-11-19 Daniel Santos\tr")
	nightlyOverlap, _ := parseNightlySpecs(strings.NewReader("ui/e2e/orker-pool.spec.ts\tui/e2e/a.spec.ts\t2026-10-26 Daniel Santos\tr\n"), "n")
	if _, problems := buildUIPlanNightly(listing, []byte("x"), nil, "costs", "sha", 1, uiNightlyInput{
		Mode: nightlyDefer, Tree: nightlyOverlap, Base: nightlyOverlap, GPU: &uiGPUInput{Tree: overlap, Base: overlap}}); !slices.ContainsFunc(problems, func(s string) bool { return strings.Contains(s, "on both the GPU spec list and the nightly spec list") }) {
		t.Errorf("a spec on both lists: problems = %v", problems)
	}
}

// The ledger records a host-deferred spec's tests with that outcome and the
// row; a test of the file that ran all the same, or a tree-only row, fails.
func TestUILedger_RecordsHostDeferredSpecs(t *testing.T) {
	listing := fixtureReport(uiTestSpec{"a.spec.ts", "one", 0, 10}, uiTestSpec{"orker-gpu.spec.ts", "binds", 0, 100})
	run := fixtureReport(uiTestSpec{"a.spec.ts", "one", 0, 10})
	l := buildUILedgerPlanned(listing, run, 0, 0, nil, map[string]string{"orker-gpu.spec.ts": gpuSpecRow})
	if len(l.Problems) != 0 || l.Pass != 1 || l.HostDeferred != 1 || l.Deferred != 0 || l.Listed != 2 {
		t.Fatalf("pass %d host-deferred %d of %d, problems %v", l.Pass, l.HostDeferred, l.Listed, l.Problems)
	}
	for _, e := range l.Tests {
		if e.File == "orker-gpu.spec.ts" && (e.Outcome != outcomeHostDeferred || e.Detail != gpuSpecRow) {
			t.Errorf("host-deferred entry = %+v", e)
		}
	}
	ran := buildUILedgerPlanned(listing, listing, 0, 0, nil, map[string]string{"orker-gpu.spec.ts": gpuSpecRow})
	if got := strings.Join(ran.Problems, "\n"); !strings.Contains(got, "orker-gpu.spec.ts › binds [chromium] ran, and the UI plan leaves its spec file to the GPU host") {
		t.Errorf("a host-deferred spec that ran is not a problem: %v", ran.Problems)
	}
	// A tree-only row is not in the plan's deferral: the listed test has no outcome.
	only := buildUILedgerPlanned(listing, run, 0, 0, nil, nil)
	if got := strings.Join(only.Problems, "\n"); !strings.Contains(got, "orker-gpu.spec.ts › binds [chromium] has no outcome") {
		t.Errorf("a tree-only GPU spec row has an outcome: %v", only.Problems)
	}
}

func TestUIPlan_CommandKeepsGPUSpecsOutAndNamesTheFilesToRun(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	listFile := filepath.Join(dir, "list.json")
	must.OK(t, writeJSON(listFile, listingOf(map[string]int{"a.spec.ts": 1, "orker-gpu.spec.ts": 1, "orker-pool.spec.ts": 1})))
	gpu, gpuBase, nightly := filepath.Join(dir, "gpu.txt"), filepath.Join(dir, "gpu-base.txt"), filepath.Join(dir, "nightly.txt")
	must.OK(t, os.WriteFile(gpu, []byte(gpuSpecHeader+gpuSpecRow+"\n"), 0o644))
	must.OK(t, os.WriteFile(gpuBase, []byte(gpuSpecHeader+gpuSpecRow+"\n"), 0o644))
	must.OK(t, os.WriteFile(nightly, []byte(""), 0o644))
	out, run := filepath.Join(dir, "ui-plan.json"), filepath.Join(dir, "run.txt")
	plan := func(extra ...string) (int, string, uiPlan) {
		args := append([]string{"-list", listFile, "-out", out, "-shards", "1", "-nightly-specs", nightly, "-nightly-specs-base", nightly, "-gpu-specs", gpu, "-run-files-out", run}, extra...)
		code := cmdUIPlan(args)
		b, _ := os.ReadFile(run)
		var p uiPlan
		_ = readJSON(out, &p)
		return code, string(b), p
	}
	code, files, p := plan("-nightly-mode", nightlyDefer, "-gpu-specs-base", gpuBase)
	if code != 0 || files != "e2e/a.spec.ts\ne2e/orker-pool.spec.ts\n" || len(p.HostDeferred) != 1 || p.GPUSpecs == nil || p.GPUSpecs.BaseSHA256 == "" || p.GPUSpecs.SHA256 == "" {
		t.Errorf("defer: exit %d, run files %q, plan %+v", code, files, p)
	}
	if code, files, p := plan("-nightly-mode", nightlyInclude, "-gpu-specs-base", gpuBase); code != 0 || files != "e2e/a.spec.ts\ne2e/orker-pool.spec.ts\n" || len(p.HostDeferred) != 1 {
		t.Errorf("include: exit %d, run files %q, host-deferred %v", code, files, p.HostDeferred)
	}
	if code, files, p := plan("-nightly-mode", nightlyDefer, "-gpu-host"); code != 0 || files != "e2e/a.spec.ts\ne2e/orker-pool.spec.ts\n" || len(p.HostDeferred) != 0 || !p.GPUSpecs.Host {
		t.Errorf("gpu host: exit %d, run files %q, plan %+v", code, files, p)
	}
	if code, _, _ := plan("-nightly-mode", nightlyDefer); code != 2 {
		t.Errorf("without the base's copy: exit %d, want 2", code)
	}
	// A listed spec the listing does not hold fails the plan.
	must.OK(t, os.WriteFile(gpu, []byte(gpuSpecHeader+"ui/e2e/gone.spec.ts\tui/e2e/a.spec.ts\t2026-11-19 Daniel Santos\tr\n"), 0o644))
	if code, _, _ := plan("-nightly-mode", nightlyDefer, "-gpu-specs-base", gpuBase); code != 1 {
		t.Errorf("a stale GPU row: exit %d, want 1", code)
	}
	// An empty list leaves everything in.
	must.OK(t, os.WriteFile(gpu, []byte(gpuSpecHeader), 0o644))
	if code, files, p := plan("-nightly-mode", nightlyDefer, "-gpu-specs-base", gpuBase); code != 0 || files != "" || len(p.HostDeferred) != 0 {
		t.Errorf("empty list: exit %d, run files %q, plan %+v", code, files, p)
	}
}

// The base's copy anchors the host-deferral, checked by the merger from the
// files, and a plan that read no list fails closed while the tree's has rows.
func TestGPUSpecAnchorProblems(t *testing.T) {
	repo := t.TempDir()
	baseCommit := gitRepo(t, repo, map[string]string{gpuSpecFile: gpuSpecHeader + gpuSpecRow + "\n"})
	t.Chdir(repo)
	tree := []byte(gpuSpecHeader + gpuSpecRow + "\n")
	baseCopy := filepath.Join(repo, gpuSpecBaseFileName)
	must.OK(t, os.WriteFile(baseCopy, tree, 0o644))
	sha := bytesSHA256(tree)
	good := func() uiPlan {
		return uiPlan{GPUSpecs: &uiPlanGPUSpecs{File: gpuSpecFile, SHA256: sha, BaseSHA256: sha, Files: []string{"orker-gpu.spec.ts"}},
			HostDeferred: []uiDeferredSpec{{File: "orker-gpu.spec.ts", Entry: gpuSpecRow}}}
	}
	cases := []struct {
		name   string
		mutate func(p *uiPlan)
		mode   string
		want   string
	}{
		{name: "anchored", mode: nightlyDefer},
		{name: "include", mode: nightlyInclude},
		{name: "no list recorded while the tree has rows", mode: nightlyDefer, mutate: func(p *uiPlan) { p.GPUSpecs = nil }, want: "recorded no GPU spec list"},
		{name: "a swapped tree list", mode: nightlyDefer, mutate: func(p *uiPlan) { p.GPUSpecs.SHA256 = "ffff" }, want: "is not the one the UI plan read"},
		{name: "a swapped base copy", mode: nightlyDefer, mutate: func(p *uiPlan) { p.GPUSpecs.BaseSHA256 = "ffff" }, want: "base's GPU spec list beside the UI plan is not the one it hashed"},
		{name: "a row deferred that the plan should have run", mode: nightlyDefer, mutate: func(p *uiPlan) {
			p.HostDeferred = append(p.HostDeferred, uiDeferredSpec{File: "other.spec.ts"})
		}, want: "other.spec.ts was host-deferred, but both GPU spec lists do not carry it"},
		{name: "a row both lists carry that was not deferred", mode: nightlyDefer, mutate: func(p *uiPlan) { p.HostDeferred = nil }, want: "is in both GPU spec lists and was not host-deferred"},
		{name: "the nightly tier defers none", mode: nightlyOnly, want: "host-deferred, and the UI plan's nightly mode is only"},
		{name: "a plan that ran on the GPU host defers none", mode: nightlyDefer, mutate: func(p *uiPlan) { p.GPUSpecs.Host = true }, want: "was host-deferred, and the plan ran on the GPU host"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := good()
			if c.mutate != nil {
				c.mutate(&p)
			}
			got := strings.Join(gpuSpecAnchorProblems(p, c.mode, baseCopy, baseCommit), "\n")
			if c.want == "" {
				if got != "" {
					t.Fatalf("problems = %s", got)
				}
				return
			}
			if !strings.Contains(got, c.want) {
				t.Fatalf("problems = %q, want %q", got, c.want)
			}
		})
	}
	// A base copy that is not the list at the base commit cannot host-defer.
	forged := filepath.Join(repo, "forged.txt")
	must.OK(t, os.WriteFile(forged, []byte(gpuSpecHeader+"ui/e2e/x.spec.ts\tui/e2e/a.spec.ts\t2026-11-19 Daniel Santos\tr\n"), 0o644))
	p := good()
	p.GPUSpecs.BaseSHA256 = fileSHAMust(t, forged)
	if got := strings.Join(gpuSpecAnchorProblems(p, nightlyDefer, forged, baseCommit), "\n"); !strings.Contains(got, "is not "+gpuSpecFile+" at the base commit") {
		t.Errorf("a forged base copy: %q", got)
	}
	// An empty tree list needs nothing recorded.
	must.OK(t, os.WriteFile(gpuSpecFile, []byte(gpuSpecHeader), 0o644))
	if got := gpuSpecAnchorProblems(uiPlan{}, nightlyDefer, baseCopy, baseCommit); len(got) != 0 {
		t.Errorf("an empty list with no record: %v", got)
	}
}

func fileSHAMust(t *testing.T, file string) string {
	t.Helper()
	s, err := fileSHA256(file)
	must.OK(t, err)
	return s
}

func gpuSpecsLedgerFor(host bool, tests ...uiEntry) uiLedger {
	l := uiLedger{Suite: suiteGPUSpecs, Problems: []string{}, ListSHA256: "tree", GPUHost: host, GPUGateRan: host}
	for _, e := range tests {
		l.Listed++
		if e.Outcome == outcomePass {
			l.Pass++
		}
		l.Tests = append(l.Tests, e)
	}
	return l
}

func TestCheckGPUSpecsLedger(t *testing.T) {
	tree := gpuSpecEntries(t, gpuSpecRow)
	pass := uiEntry{File: "orker-gpu.spec.ts", Title: "binds", Project: "chromium", Outcome: outcomePass}
	for _, tt := range []struct {
		name   string
		ledger func() uiLedger
		tier   string
		want   string // "" is none
	}{
		{"a passing gate on the GPU host", func() uiLedger { return gpuSpecsLedgerFor(true, pass) }, tierFull, ""},
		{"the GPU-host tier", func() uiLedger { return gpuSpecsLedgerFor(true, pass) }, tierGPU, ""},
		{"an empty list, an empty ledger", func() uiLedger { return gpuSpecsLedgerFor(true) }, tierFull, "has no test in the gpu-specs ledger"},
		{"host-deferred in the GPU-host tier", func() uiLedger {
			e := pass
			e.Outcome = outcomeHostDeferred
			return gpuSpecsLedgerFor(true, e)
		}, tierGPU, "host-deferred"},
		{"a pass from a gate that did not run on the GPU host", func() uiLedger { return gpuSpecsLedgerFor(false, pass) }, tierFull, "did not run on the GPU host"},
		{"another list's hash", func() uiLedger { l := gpuSpecsLedgerFor(true, pass); l.ListSHA256 = "other"; return l }, tierFull, "read the GPU spec list"},
		{"a problem the gate wrote", func() uiLedger {
			l := gpuSpecsLedgerFor(true, pass)
			l.Problems = []string{"orker-gpu.spec.ts › binds [chromium] failed (boom)"}
			return l
		}, tierFull, "failed (boom)"},
		{"a test that did not pass", func() uiLedger {
			e := pass
			e.Outcome = outcomeFail
			return gpuSpecsLedgerFor(true, e)
		}, tierFull, "has outcome \"fail\""},
		{"a spec not on the list", func() uiLedger {
			e := pass
			e.File = "stray.spec.ts"
			return gpuSpecsLedgerFor(true, pass, e)
		}, tierFull, "stray.spec.ts is in the gpu-specs ledger and not on this tree's GPU spec list"},
		{"the wrong suite", func() uiLedger { l := gpuSpecsLedgerFor(true, pass); l.Suite = "ui-e2e"; return l }, tierFull, "is of suite"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := strings.Join(checkGPUSpecsLedger(tt.ledger(), tree, "tree", tt.tier), "\n")
			if tt.want == "" {
				if got != "" {
					t.Fatalf("problems = %s", got)
				}
				return
			}
			if !strings.Contains(got, tt.want) {
				t.Fatalf("problems = %q, want %q", got, tt.want)
			}
		})
	}
	// With no rows, an empty ledger from the gate is the whole proof.
	if got := checkGPUSpecsLedger(gpuSpecsLedgerFor(true), nil, "tree", tierFull); len(got) != 0 {
		t.Errorf("an empty list's ledger: %v", got)
	}
}

func TestCmdGPUSpecs_BuildsTheLedgerFromTheListingAndReports(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	list := fixtureReport(uiTestSpec{"orker-gpu.spec.ts", "binds", 0, 5})
	listFile, passFile, failFile, specs := filepath.Join(dir, "list.json"), filepath.Join(dir, "pass.json"), filepath.Join(dir, "none.json"), filepath.Join(dir, "specs.txt")
	must.OK(t, writeJSON(listFile, list))
	must.OK(t, writeJSON(passFile, list))
	must.OK(t, writeJSON(failFile, pwReport{}))
	must.OK(t, os.WriteFile(specs, []byte(gpuSpecHeader+gpuSpecRow+"\n"), 0o644))
	cfg, logFile := filepath.Join(dir, "pw.config.ts"), filepath.Join(dir, "pw.log")
	must.OK(t, os.WriteFile(cfg, []byte("export const NETWORK_CHANGE_RETRY_CEILING = 2;\n"), 0o644))
	must.OK(t, os.WriteFile(logFile, nil, 0o644))
	out := filepath.Join(dir, "out")
	must.OK(t, os.MkdirAll(out, 0o755))
	// A list with rows needs the Playwright log: the guard is read from it.
	if code := cmdGPUSpecs([]string{"-list", listFile, "-report", passFile, "-specs", specs, "-out", out, "-tier", tierGPU}); code != 2 {
		t.Fatalf("rows and no -log: exit %d, want 2", code)
	}
	run := func(report string) (int, uiLedger) {
		code := cmdGPUSpecs([]string{"-list", listFile, "-report", report, "-specs", specs, "-out", out, "-tier", tierGPU, "-log", logFile, "-config", cfg})
		var l uiLedger
		_ = readJSON(filepath.Join(out, suiteGPUSpecs+"-tests.json"), &l)
		return code, l
	}
	code, l := run(passFile)
	if code != 0 || l.Pass != 1 || !l.GPUHost || !l.GPUGateRan || l.ListSHA256 == "" || l.Suite != suiteGPUSpecs {
		t.Fatalf("a passing run: exit %d, ledger %+v", code, l)
	}
	if code, l := run(failFile); code != 1 || len(l.Problems) == 0 {
		t.Errorf("a run with no outcome: exit %d, problems %v", code, l.Problems)
	}
	// No rows: no listing, no report, an empty ledger that passes.
	must.OK(t, os.WriteFile(specs, []byte(gpuSpecHeader), 0o644))
	if code := cmdGPUSpecs([]string{"-specs", specs, "-out", out, "-tier", tierFull}); code != 0 {
		t.Errorf("an empty list: exit %d, want 0", code)
	}
	var empty uiLedger
	must.OK(t, readJSON(filepath.Join(out, suiteGPUSpecs+"-tests.json"), &empty))
	if len(empty.Tests) != 0 || !empty.GPUHost || !empty.GPUGateRan || len(empty.Problems) != 0 {
		t.Errorf("an empty list's ledger = %+v", empty)
	}
	// A row whose spec the listing does not hold is stale.
	must.OK(t, os.WriteFile(specs, []byte(gpuSpecHeader+"ui/e2e/gone.spec.ts\tui/e2e/a.spec.ts\t2026-11-19 Daniel Santos\tr\n"), 0o644))
	if code, l := run(passFile); code != 1 || !strings.Contains(strings.Join(l.Problems, "\n"), "gone.spec.ts") {
		t.Errorf("a stale row: exit %d, problems %v", code, l.Problems)
	}
	if code := cmdGPUSpecs([]string{"-specs", specs}); code != 2 {
		t.Errorf("no -out: exit %d, want 2", code)
	}
	if code := cmdGPUSpecs([]string{"-specs", specs, "-out", out, "-tier", "bogus"}); code != 2 {
		t.Errorf("a bad -tier: exit %d, want 2", code)
	}
}

// merge-check's GPU rule covers a change to the spec list and to a listed
// spec, from main's tip's list or the tree's.
func TestMergeGPUProblems_CoversTheGPUSpecs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(t *testing.T, dir string)
		want   string
	}{
		{"an untouched change", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("y"), 0o644))
		}, ""},
		{"a listed spec changed", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "ui/e2e/orker-gpu.spec.ts"), []byte("y"), 0o644))
		}, "ui/e2e/orker-gpu.spec.ts"},
		{"a spec the tree's list adds", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "ui/e2e/new.spec.ts"), []byte("y"), 0o644))
			must.OK(t, os.WriteFile(filepath.Join(dir, gpuSpecFile), []byte(gpuSpecHeader+gpuSpecRow+"\nui/e2e/new.spec.ts\tui/e2e/orker-pool.spec.ts\t2026-11-19 Daniel Santos\tr\n"), 0o644))
		}, "ui/e2e/new.spec.ts"},
		{"the row removed with its spec", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "ui/e2e/orker-gpu.spec.ts"), []byte("y"), 0o644))
			must.OK(t, os.WriteFile(filepath.Join(dir, gpuSpecFile), []byte(gpuSpecHeader), 0o644))
		}, "ui/e2e/orker-gpu.spec.ts"},
		{"the list itself changed", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, gpuSpecFile), []byte(gpuSpecHeader+"# more\n"+gpuSpecRow+"\n"), 0o644))
		}, gpuSpecFile},
		{"the list deleted", func(t *testing.T, dir string) {
			must.OK(t, os.Remove(filepath.Join(dir, gpuSpecFile)))
		}, "deletes " + gpuSpecFile},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tip := gitRepo(t, dir, map[string]string{
				gpuSpecFile: gpuSpecHeader + gpuSpecRow + "\n", "ui/e2e/orker-gpu.spec.ts": "x", "README.md": "x"})
			git(t, dir, "checkout", "-q", "-b", "candidate")
			tt.change(t, dir)
			git(t, dir, "add", "-A")
			git(t, dir, "commit", "-q", "-m", "change")
			t.Chdir(dir)
			got := mergeGPUProblems("", "tree", tip)
			if tt.want == "" {
				if len(got) != 0 {
					t.Fatalf("problems = %v, want none", got)
				}
				return
			}
			if !slices.ContainsFunc(got, func(p string) bool { return strings.Contains(p, tt.want) }) {
				t.Fatalf("problems = %v, want one containing %q", got, tt.want)
			}
		})
	}
}

// A GPU-host-tier proof must record that the spec gate ran with the Go gate.
func TestGPUProofProblems_RequiresTheSpecGate(t *testing.T) {
	r := passingGPUProof(tierGPU, "tree1")
	r.GPUSpecsGateRan = false
	got := gpuProofProblems(writeGPUProof(t, r), "tree1")
	if !slices.ContainsFunc(got, func(p string) bool { return strings.Contains(p, "gpu_specs_gate_ran") }) {
		t.Errorf("problems = %v, want one naming gpu_specs_gate_ran", got)
	}
	r.GPUSpecsGateRan = true
	if got := gpuProofProblems(writeGPUProof(t, r), "tree1"); len(got) != 0 {
		t.Errorf("a proof whose spec gate ran: %v", got)
	}
}

// The repository's own list is valid, today with no rows, and every row it
// ever holds names real specs that no merge-gate lane leaves out.
func TestRepositoryGPUSpecs_ParseAndSpecsExist(t *testing.T) {
	root, err := repoRoot()
	must.OK(t, err)
	entries, err := readGPUSpecs(filepath.Join(root, gpuSpecFile))
	must.OK(t, err)
	if _, err := os.Stat(filepath.Join(root, gpuSpecFile)); err != nil {
		t.Fatalf("%s is missing: %v", gpuSpecFile, err)
	}
	nightly, err := readNightlySpecs(filepath.Join(root, nightlySpecFile))
	must.OK(t, err)
	listed := map[string]bool{}
	files, _ := filepath.Glob(filepath.Join(root, "ui", "e2e", "*.spec.ts"))
	for _, f := range files {
		listed[filepath.Base(f)] = true
	}
	for _, p := range gpuSpecListProblems(entries, nightly, listed, true) {
		t.Error(p)
	}
}

// FuzzParseGPUSpecs states the parser's claims: it never panics, and a list it
// accepts holds only spec paths under ui/e2e/, each listed once, with covering
// specs that are neither the row's own spec nor outside ui/e2e/.
func FuzzParseGPUSpecs(f *testing.F) {
	for _, seed := range []string{"", gpuSpecHeader, gpuSpecHeader + gpuSpecRow + "\n", "a\tb\tc\n", gpuSpecRow + "\n" + gpuSpecRow} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		rows, err := parseGPUSpecs(b, "fuzz")
		if err != nil {
			return
		}
		seen := map[string]bool{}
		for _, e := range rows {
			if !specPath.MatchString(e.Spec) || seen[e.Spec] || len(e.Covering) == 0 {
				t.Fatalf("accepted row %+v", e)
			}
			seen[e.Spec] = true
			for _, c := range e.Covering {
				if !specPath.MatchString(c) || c == e.Spec {
					t.Fatalf("accepted covering spec %q of %s", c, e.Spec)
				}
			}
		}
	})
}

// A tip whose spec list cannot be read requires a GPU-host proof; it is
// never ignored.
func TestMergeGPUProblems_AnUnreadableTipSpecListRequiresAProof(t *testing.T) {
	dir := t.TempDir()
	gitRepo(t, dir, map[string]string{"README.md": "x"})
	t.Chdir(dir)
	got := mergeGPUProblems("", "tree", "0123456789abcdef0123456789abcdef01234567")
	if !slices.ContainsFunc(got, func(p string) bool { return strings.Contains(p, "main's tip's GPU spec list cannot be read") }) {
		t.Errorf("problems = %v", got)
	}
}

// The network-change guard holds the GPU specs as it holds the merge gate's:
// the log's repeated navigations are counted against the config's ceiling.
func TestCmdGPUSpecs_EnforcesTheNetworkChangeGuard(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	list := fixtureReport(uiTestSpec{"orker-gpu.spec.ts", "binds", 0, 5})
	listFile, specs, cfg, logFile := filepath.Join(dir, "list.json"), filepath.Join(dir, "specs.txt"), filepath.Join(dir, "pw.config.ts"), filepath.Join(dir, "pw.log")
	must.OK(t, writeJSON(listFile, list))
	must.OK(t, os.WriteFile(specs, []byte(gpuSpecHeader+gpuSpecRow+"\n"), 0o644))
	must.OK(t, os.WriteFile(cfg, []byte("export const NETWORK_CHANGE_RETRY_CEILING = 2;\n"), 0o644))
	for _, c := range []struct {
		repeats int
		want    int
	}{{2, 0}, {3, 1}} {
		must.OK(t, os.WriteFile(logFile, []byte(strings.Repeat("x "+guardMarker+" 2)\n", c.repeats)), 0o644))
		out := t.TempDir()
		code := cmdGPUSpecs([]string{"-list", listFile, "-report", listFile, "-specs", specs, "-out", out, "-log", logFile, "-config", cfg})
		var l uiLedger
		must.OK(t, readJSON(filepath.Join(out, suiteGPUSpecs+"-tests.json"), &l))
		if code != c.want || l.GuardRetries != c.repeats || l.GuardRetryCeiling != 2 {
			t.Errorf("%d repeats: exit %d, ledger retries %d ceiling %d, problems %v", c.repeats, code, l.GuardRetries, l.GuardRetryCeiling, l.Problems)
		}
	}
	if code := cmdGPUSpecs([]string{"-list", listFile, "-report", listFile, "-specs", specs, "-out", t.TempDir(), "-log", logFile, "-config", filepath.Join(dir, "none")}); code != 1 {
		t.Errorf("an unreadable config: exit %d, want 1", code)
	}
}
