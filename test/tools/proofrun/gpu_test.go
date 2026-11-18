package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const gpuPkg = "github.com/orkstack/orkstack/internal/orker"

func gpuRow(test, covers string) string {
	return gpuPkg + "\t" + test + "\t" + covers + "\t2026-11-18 Daniel Santos\tbecause"
}

const gpuHeader = "# the GPU-host list\n#\n# paths: internal/orker/gpu_boot.go internal/orker/*_gpuhost_test.go\n#   scripts/gpu-host-setup.sh\n"

func TestParseGPUList(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		wantPaths []string
		wantRows  int
		wantErr   string
	}{
		{"empty", "", nil, 0, ""},
		{"comments only", "# nothing yet\n", nil, 0, ""},
		{"header and continuation", gpuHeader + gpuRow("TestIntegrationGPU_A", "TestCover") + "\n",
			[]string{"internal/orker/gpu_boot.go", "internal/orker/*_gpuhost_test.go", "scripts/gpu-host-setup.sh"}, 1, ""},
		{"a blank comment ends the continuation", "# paths: a.go\n#\n#   b.go\n", []string{"a.go"}, 0, ""},
		{"rows need a header", gpuRow("TestIntegrationGPU_A", "TestCover") + "\n", nil, 0, "paths:"},
		{"an empty header", "# paths:\n", nil, 0, "names no path"},
		{"two headers", "# paths: a.go\n# paths: b.go\n", nil, 0, "second paths:"},
		{"absolute path", "# paths: /etc/cdi\n", nil, 0, "absolute"},
		{"leaves the repository", "# paths: ../x.go\n", nil, 0, "leaves the repository"},
		{"dot-dot inside", "# paths: a/../../x.go\n", nil, 0, "not a clean path"},
		{"a bad pattern", "# paths: a[.go\n", nil, 0, "not a pattern"},
		{"a malformed row", gpuHeader + gpuPkg + "\tTestX\n", nil, 0, "columns"},
		{"an approval without a name", gpuHeader + gpuPkg + "\tTestX\tTestC\t2026-11-18\tr\n", nil, 0, "approved"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGPUList([]byte(tt.text), "list")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Paths, tt.wantPaths) || len(got.Entries) != tt.wantRows {
				t.Fatalf("paths %v, %d rows; want %v, %d", got.Paths, len(got.Entries), tt.wantPaths, tt.wantRows)
			}
		})
	}
}

// TestGPUList_TheTreesListParses holds scripts/proof-gpu-tests.txt itself to
// the parser: every row approved, a paths: header, no covering test listed.
func TestGPUList_TheTreesListParses(t *testing.T) {
	list, err := readGPUList(filepath.Join("..", "..", "..", gpuListFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Entries) == 0 || len(list.Paths) == 0 {
		t.Fatalf("the tree's GPU-host list has %d rows and %d paths", len(list.Entries), len(list.Paths))
	}
	on := map[string]bool{}
	for _, e := range list.Entries {
		on[e.Test] = true
	}
	for _, e := range list.Entries {
		for _, c := range e.Covering {
			if on[c] {
				t.Errorf("line %d: covering test %s is itself on the list", e.Line, c)
			}
		}
	}
	// A pattern naming a file that is not there covers nothing: every
	// literal path and directory prefix of the header exists in the tree.
	root := filepath.Join("..", "..", "..")
	for _, p := range list.Paths {
		if strings.ContainsAny(p, "*?[") {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(p))); err != nil {
			t.Errorf("paths: %s names nothing in the tree: %v", p, err)
		}
	}
}

func FuzzParseGPUList(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte(gpuHeader + gpuRow("TestIntegrationGPU_A", "TestCover") + "\n"))
	f.Add([]byte("# paths: a.go\n#   b/*.go\n"))
	f.Add([]byte("# paths: /x\n"))
	f.Add([]byte("# paths:\n# paths: a\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := parseGPUList(b, "fuzz")
		if err != nil {
			return
		}
		if len(got.Entries) > 0 && len(got.Paths) == 0 {
			t.Fatalf("a list with %d rows parsed with no paths", len(got.Entries))
		}
		for _, p := range got.Paths {
			if err := checkGPUPathPattern(p); err != nil {
				t.Fatalf("accepted path %q: %v", p, err)
			}
		}
		again, err := parseGPUList(b, "fuzz")
		if err != nil || !slices.Equal(again.Paths, got.Paths) || len(again.Entries) != len(got.Entries) {
			t.Fatalf("a second parse differs: %v, %v", again, err)
		}
	})
}

func TestTouchesGPUPaths(t *testing.T) {
	patterns := []string{"internal/orker/gpu_boot.go", "internal/orker/*_gpuhost_test.go", "deploy/gpu/"}
	got := touchesGPUPaths(patterns, []string{
		"internal/orker/gpu_boot.go", "internal/orker/units_gpuhost_test.go",
		"internal/orker/capacity.go", "internal/orker/sub/x_gpuhost_test.go", "README.md",
		"deploy/gpu/a/b.yaml", "deploy/gpux.yaml",
	})
	want := []string{"internal/orker/gpu_boot.go", "internal/orker/units_gpuhost_test.go", "deploy/gpu/a/b.yaml"}
	if !slices.Equal(got, want) {
		t.Fatalf("touched = %v, want %v", got, want)
	}
}

func gpuEntries(t *testing.T, tests ...string) []nightlyEntry {
	t.Helper()
	var b strings.Builder
	b.WriteString(gpuHeader)
	for _, n := range tests {
		b.WriteString(gpuRow(n, "TestCover") + "\n")
	}
	l, err := parseGPUList([]byte(b.String()), "list")
	if err != nil {
		t.Fatal(err)
	}
	return l.Entries
}

func outcomes(l ledger) map[string]string {
	out := map[string]string{}
	for _, e := range l.Tests {
		out[e.Test] = e.Outcome
	}
	return out
}

// TestGPUDeferredLedger: without the GPU host an entry both lists carry is
// host-deferred, and one the change adds has no outcome and fails.
func TestGPUDeferredLedger(t *testing.T) {
	tree := gpuEntries(t, "TestIntegrationGPU_A", "TestIntegrationGPU_New")
	base := gpuEntries(t, "TestIntegrationGPU_A", "TestIntegrationGPU_Removed")
	l := gpuDeferredLedger(tree, base)
	want := map[string]string{"TestIntegrationGPU_A": outcomeHostDeferred, "TestIntegrationGPU_New": outcomeNone}
	if got := outcomes(l); !mapsEqual(got, want) {
		t.Fatalf("outcomes = %v, want %v", got, want)
	}
	if l.ok() || len(l.Problems) != 1 || !strings.Contains(l.Problems[0], "TestIntegrationGPU_New") {
		t.Fatalf("problems = %v, want one naming the added entry", l.Problems)
	}
	if ok := gpuDeferredLedger(base[:1], base); !ok.ok() || ok.Summary.Deferred != 1 {
		t.Fatalf("a list the base carries whole: %+v", ok)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestCheckGPULedger(t *testing.T) {
	tree := gpuEntries(t, "TestIntegrationGPU_A", "TestIntegrationGPU_B")
	base := gpuEntries(t, "TestIntegrationGPU_A", "TestIntegrationGPU_B")
	led := func(host bool, outcome ...string) ledger {
		l := ledger{Suite: suiteGPU, GPUHost: host, GPUGateRan: host, ListSHA256: "t", BaseSHA256: "b"}
		for i, o := range outcome {
			l.Tests = append(l.Tests, entry{Package: gpuPkg, Test: tree[i].Test, Outcome: o})
		}
		return l
	}
	tests := []struct {
		name string
		l    ledger
		base []nightlyEntry
		tier string
		want string // a problem contains it; "" is none
	}{
		{"host-deferred in the full tier", led(false, outcomeHostDeferred, outcomeHostDeferred), base, tierFull, ""},
		{"passed on the GPU host", led(true, outcomePass, outcomePass), base, tierFull, ""},
		{"passed on the GPU host, gpu tier", led(true, outcomePass, outcomePass), base, tierGPU, ""},
		{"host-deferred in the gpu tier", led(true, outcomePass, outcomeHostDeferred), base, tierGPU, "host-deferred in a GPU-host-tier proof"},
		{"the gpu tier without the host", led(false, outcomeHostDeferred, outcomeHostDeferred), base, tierGPU, "did not run on the GPU host"},
		{"host-deferred on the GPU host", led(true, outcomePass, outcomeHostDeferred), base, tierFull, "proof that ran on the GPU host"},
		{"a pass without the gate", led(false, outcomePass, outcomeHostDeferred), base, tierFull, "gate did not run"},
		{"host-deferred, added by the change", led(false, outcomeHostDeferred, outcomeHostDeferred), base[:1], tierFull, "base's GPU-host list does not carry it"},
		{"a failure", led(true, outcomePass, outcomeFail), base, tierFull, `outcome "fail"`},
		{"a missing entry", led(true, outcomePass), base, tierFull, "not in the gpu ledger"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkGPULedger(tt.l, tree, tt.base, "t", "b", tt.tier)
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
	if got := checkGPULedger(led(false, outcomeHostDeferred, outcomeHostDeferred), tree, base, "other", "b", tierFull); len(got) == 0 {
		t.Fatal("a ledger judged on another list's bytes passed")
	}
	extra := led(true, outcomePass, outcomePass)
	extra.Tests = append(extra.Tests, entry{Package: gpuPkg, Test: "TestIntegrationGPU_Gone", Outcome: outcomePass})
	if got := checkGPULedger(extra, tree, base, "t", "b", tierFull); len(got) == 0 {
		t.Fatal("a ledger entry the tree's list does not carry passed")
	}
}

func writeGPUProof(t *testing.T, r proofRecord) string {
	t.Helper()
	dir := t.TempDir()
	if err := writeJSON(filepath.Join(dir, "proof.json"), r); err != nil {
		t.Fatal(err)
	}
	return dir
}

func passingGPUProof(tier, tree string) proofRecord {
	r := proofRecord{Tier: tier, Tree: tree, TreeEnd: tree, CleanStart: true, CleanEnd: true, Result: "pass",
		GPUHost: true, GPUGateRan: true}
	switch tier {
	case tierFull:
		r.MergeEligible = true
	default:
		r.Ineligible = []string{tierIneligible(tier)}
		r.EligibleButForTier = true
	}
	return r
}

func TestGPUProofProblems(t *testing.T) {
	const tree = "aaaa1111"
	tests := []struct {
		name string
		r    func() proofRecord
		want string
	}{
		{"a gpu-tier proof", func() proofRecord { return passingGPUProof(tierGPU, tree) }, ""},
		{"a full proof with the flag", func() proofRecord { return passingGPUProof(tierFull, tree) }, ""},
		{"a full proof without the flag", func() proofRecord {
			r := passingGPUProof(tierFull, tree)
			r.GPUHost, r.GPUGateRan = false, false
			return r
		}, "did not run on the GPU host"},
		{"another tree", func() proofRecord { return passingGPUProof(tierGPU, "bbbb2222") }, "measured tree"},
		{"a failing proof", func() proofRecord {
			r := passingGPUProof(tierGPU, tree)
			r.Result = "fail"
			return r
		}, "did not pass"},
		{"a nightly proof", func() proofRecord { return passingGPUProof(tierNightly, tree) }, "tier nightly"},
		{"not eligible", func() proofRecord {
			r := passingGPUProof(tierGPU, tree)
			r.EligibleButForTier = false
			r.Ineligible = append(r.Ineligible, "the tree changed")
			return r
		}, "the tree changed"},
		{"not clean", func() proofRecord {
			r := passingGPUProof(tierGPU, tree)
			r.CleanEnd = false
			return r
		}, "not clean"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gpuProofProblems(writeGPUProof(t, tt.r()), tree)
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
	if got := gpuProofProblems(filepath.Join(t.TempDir(), "absent"), tree); len(got) != 1 || !strings.Contains(got[0], "unreadable") {
		t.Fatalf("an absent proof: %v", got)
	}
}

// TestGPUMergeRule: a change touching a listed path needs GPU=, one that
// does not needs none, and a GPU= named is checked either way — also when it
// is the same directory as the full proof (PROOF=).
func TestGPUMergeRule(t *testing.T) {
	const tree = "aaaa1111"
	paths := []string{"internal/orker/gpu_boot.go"}
	touching := []string{"internal/orker/gpu_boot.go", "README.md"}
	if got := gpuMergeRule(paths, "", tree, []string{"README.md"}, nil); len(got) != 0 {
		t.Fatalf("an untouched change: %v", got)
	}
	if got := gpuMergeRule(paths, "", tree, touching, nil); len(got) != 1 || !strings.Contains(got[0], "GPU=proof/<name>") {
		t.Fatalf("a touching change without GPU=: %v", got)
	}
	full := writeGPUProof(t, passingGPUProof(tierFull, tree))
	if got := gpuMergeRule(paths, full, tree, touching, nil); len(got) != 0 {
		t.Fatalf("GPU= the full proof itself, run with the flag: %v", got)
	}
	other := writeGPUProof(t, passingGPUProof(tierGPU, "bbbb2222"))
	if got := gpuMergeRule(paths, other, tree, []string{"README.md"}, nil); len(got) == 0 {
		t.Fatal("a named GPU= proof of another tree passed an untouched change")
	}
	if got := gpuMergeRule(paths, "", tree, nil, []string{"the changed files cannot be listed (boom)"}); len(got) != 1 {
		t.Fatalf("unknown changed files must ask for GPU=: %v", got)
	}
}

// TestMergeGPUTierProof: a gpu-tier proof's verdict is its local part's gpu
// ledger, judged against the lists, never merge-eligible but for its tier.
func TestMergeGPUTierProof(t *testing.T) {
	for _, tt := range []struct {
		name     string
		host     bool
		gate     bool
		wantPass bool
	}{
		{"the gate ran on the GPU host", true, true, true},
		{"a ledger that did not run on the host", false, true, false},
		{"no gpu gate", true, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			base := gitRepo(t, root, nil)
			t.Chdir(root)
			parts := filepath.Join(root, "parts")
			local := filepath.Join(parts, "local-a1")
			must.OK(t, os.MkdirAll(local, 0o755))
			rec := partRecord{Part: "local", Attempt: 1, Tier: tierGPU, Name: "g", Commit: "c", Tree: "aaaa", TreeEnd: "aaaa",
				CleanStart: true, CleanEnd: true, BaseRef: mainBranch, BaseCommit: base, Gates: []gateRecord{}}
			if tt.gate {
				rec.Gates = []gateRecord{{Gate: suiteGPU, Result: "pass", Log: "gpu.log", Command: "gpu_gate"}}
			}
			must.OK(t, writeJSON(filepath.Join(local, "part.json"), rec))
			emptySHA, _ := fileSHA256("")
			l := ledger{Suite: suiteGPU, Tests: []entry{}, Problems: []string{}, GPUHost: tt.host, GPUGateRan: tt.host,
				ListSHA256: emptySHA, BaseSHA256: emptySHA}
			must.OK(t, writeJSON(filepath.Join(local, suiteGPU+"-tests.json"), l))
			out := filepath.Join(root, "out")
			must.OK(t, os.MkdirAll(out, 0o755))
			must.OK(t, os.WriteFile(filepath.Join(out, gpuBaseFileName), nil, 0o644))
			code := mergeGPUTierProof(parts, out, "g")
			var r proofRecord
			must.OK(t, readJSON(filepath.Join(out, "proof.json"), &r))
			if (r.Result == "pass") != tt.wantPass || (code == 0) != tt.wantPass {
				t.Fatalf("result %q, exit %d, ineligible %v; want pass %v", r.Result, code, r.Ineligible, tt.wantPass)
			}
			if r.Tier != tierGPU || r.MergeEligible {
				t.Fatalf("tier %q, merge-eligible %v: a gpu-tier proof is never merge evidence", r.Tier, r.MergeEligible)
			}
			if tt.wantPass && (!r.EligibleButForTier || !r.GPUHost || !r.GPUGateRan) {
				t.Fatalf("a passing gpu-tier proof: %+v", r)
			}
		})
	}
}

// TestMergeGPU: the merger's gpu ledger is the local part's gate's when it
// ran, and otherwise the host-deferred ledger of the tree's list and the
// base's copy beside the plan, which fails for an entry the change adds or a
// base copy that is missing.
func TestMergeGPU(t *testing.T) {
	list := gpuHeader + gpuRow("TestIntegrationGPU_A", "TestCover") + "\n"
	added := list + gpuRow("TestIntegrationGPU_New", "TestCover") + "\n"
	for _, tt := range []struct {
		name     string
		tree     string
		base     *string
		hostRan  bool
		wantHost bool
		want     string // a merge problem contains it; "" is none
	}{
		{"host-deferred", list, &list, false, false, ""},
		{"an entry the change adds", added, &list, false, false, "TestIntegrationGPU_New"},
		{"no base copy beside the plan", list, nil, false, false, gpuBaseFileName},
		{"the local part ran the gate", added, &list, true, true, ""},
		// The plan artifact's base copy swapped for the tree's list would
		// host-defer the entry the change adds: the base commit refuses it.
		{"a base copy that is not the base commit's", added, &added, false, false, "is not scripts/proof-gpu-tests.txt at the base commit"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			baseCommit := gitRepo(t, dir, map[string]string{gpuListFile: list})
			must.OK(t, os.WriteFile(filepath.Join(dir, gpuListFile), []byte(tt.tree), 0o644))
			if tt.base != nil {
				must.OK(t, os.WriteFile(filepath.Join(dir, gpuBaseFileName), []byte(*tt.base), 0o644))
			}
			t.Chdir(dir)
			treeSHA, _ := fileSHA256(gpuListFile)
			baseSHA, _ := fileSHA256(gpuBaseFileName)
			m := &merger{plan: &integrationPlan{Tier: tierFull, BaseCommit: baseCommit,
				GPU: gpuListRef{File: gpuListFile, SHA256: treeSHA, BaseSHA256: baseSHA}},
				planPath: filepath.Join(dir, "integration-plan.json"),
				parts:    map[string]map[int]partReading{}, proof: &proofRecord{}}
			if tt.hostRan {
				local := filepath.Join(dir, "local-a1")
				must.OK(t, os.MkdirAll(local, 0o755))
				treeSHA, _ := fileSHA256(gpuListFile)
				baseSHA, _ := fileSHA256(gpuBaseFileName)
				l := ledger{Suite: suiteGPU, Problems: []string{}, GPUHost: true, GPUGateRan: true, ListSHA256: treeSHA, BaseSHA256: baseSHA,
					Tests: []entry{{Package: gpuPkg, Test: "TestIntegrationGPU_A", Outcome: outcomePass}, {Package: gpuPkg, Test: "TestIntegrationGPU_New", Outcome: outcomePass}}}
				must.OK(t, writeJSON(filepath.Join(local, suiteGPU+"-tests.json"), l))
				m.parts["local"] = map[int]partReading{1: {Part: "local", Attempt: 1, Dir: local, Record: &partRecord{Part: "local", Attempt: 1}}}
			}
			m.mergeGPU()
			if m.gpu == nil || m.proof.GPUHost != tt.wantHost {
				t.Fatalf("gpu ledger %v, gpu_host %v; want gpu_host %v", m.gpu, m.proof.GPUHost, tt.wantHost)
			}
			if tt.want == "" {
				if len(m.problems) != 0 {
					t.Fatalf("problems = %v, want none", m.problems)
				}
				return
			}
			if !slices.ContainsFunc(m.problems, func(p string) bool { return strings.Contains(p, tt.want) }) {
				t.Fatalf("problems = %v, want one containing %q", m.problems, tt.want)
			}
		})
	}
}

// gitRepo makes dir a repository whose main branch holds one commit of
// files, and returns that commit.
func gitRepo(t *testing.T, dir string, files map[string]string) string {
	t.Helper()
	git(t, dir, "init", "-q", "-b", mainBranch)
	for name, body := range files {
		must.OK(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755))
		must.OK(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "--allow-empty", "-m", "base")
	return strings.TrimSpace(git(t, dir, "rev-parse", "HEAD"))
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@example.invalid",
		"-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// TestMergeGPUProblems: merge-check's GPU rule reads main's tip's list too,
// so a change cannot remove its own obligation by trimming the header or
// deleting the list, a file moved away from a covered path still counts, and
// whatever cannot be read asks for GPU=.
func TestMergeGPUProblems(t *testing.T) {
	const list = "# paths: internal/orker/gpu_boot.go internal/orker/cdi/\n" +
		gpuPkg + "\tTestIntegrationGPU_A\tTestCover\t2026-11-18 Daniel Santos\tbecause\n"
	const trimmed = "# paths: internal/orker/cdi/\n" +
		gpuPkg + "\tTestIntegrationGPU_A\tTestCover\t2026-11-18 Daniel Santos\tbecause\n"
	for _, tt := range []struct {
		name   string
		change func(t *testing.T, dir string)
		tip    func(tip string) string
		want   string // a problem contains it; "" is none
	}{
		{"an untouched change", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("y"), 0o644))
		}, nil, ""},
		{"a covered path changed", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "internal/orker/gpu_boot.go"), []byte("y"), 0o644))
		}, nil, "internal/orker/gpu_boot.go"},
		{"the header trimmed in the same change", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "internal/orker/gpu_boot.go"), []byte("y"), 0o644))
			must.OK(t, os.WriteFile(filepath.Join(dir, gpuListFile), []byte(trimmed), 0o644))
		}, nil, "internal/orker/gpu_boot.go"},
		{"the list deleted", func(t *testing.T, dir string) {
			must.OK(t, os.Remove(filepath.Join(dir, gpuListFile)))
		}, nil, "deletes " + gpuListFile},
		{"a covered file renamed away", func(t *testing.T, dir string) {
			git(t, dir, "mv", "internal/orker/gpu_boot.go", "internal/orker/boot.go")
		}, nil, "internal/orker/gpu_boot.go"},
		{"a file under a covered directory", func(t *testing.T, dir string) {
			must.OK(t, os.MkdirAll(filepath.Join(dir, "internal/orker/cdi"), 0o755))
			must.OK(t, os.WriteFile(filepath.Join(dir, "internal/orker/cdi/new file.go"), []byte("y"), 0o644))
		}, nil, "internal/orker/cdi/new file.go"},
		{"a harness file changed", func(t *testing.T, dir string) {
			must.OK(t, os.MkdirAll(filepath.Join(dir, "test/tools/proofrun"), 0o755))
			must.OK(t, os.WriteFile(filepath.Join(dir, "test/tools/proofrun/gpu.go"), []byte("y"), 0o644))
		}, nil, "test/tools/proofrun/gpu.go"},
		{"main's tip unknown", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("y"), 0o644))
		}, func(string) string { return "" }, "main's tip"},
		{"main's tip not in the repository", func(t *testing.T, dir string) {
			must.OK(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("y"), 0o644))
		}, func(string) string { return "0123456789abcdef0123456789abcdef01234567" }, "not in this repository"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tip := gitRepo(t, dir, map[string]string{gpuListFile: list, "internal/orker/gpu_boot.go": "x", "README.md": "x"})
			git(t, dir, "checkout", "-q", "-b", "candidate")
			tt.change(t, dir)
			git(t, dir, "add", "-A")
			git(t, dir, "commit", "-q", "-m", "change")
			t.Chdir(dir)
			if tt.tip != nil {
				tip = tt.tip(tip)
			}
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

// TestGPUAnchorProblems: the merger judges the lists only as the plan
// hashed them, and a plan with no hash fails closed while the list has rows.
func TestGPUAnchorProblems(t *testing.T) {
	ref := gpuListRef{File: gpuListFile, SHA256: "tree", BaseSHA256: "base"}
	for _, tt := range []struct {
		name          string
		ref           gpuListRef
		tree, base    string
		entries, want bool
	}{
		{"matching", ref, "tree", "base", true, false},
		{"the tree's list changed", ref, "other", "base", true, true},
		{"the base copy swapped", ref, "tree", "tree", true, true},
		{"no hash, rows", gpuListRef{}, "tree", "base", true, true},
		{"no hash, no rows", gpuListRef{}, "tree", "base", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := gpuAnchorProblems(tt.ref, tt.tree, tt.base, tt.entries); (len(got) > 0) != tt.want {
				t.Fatalf("problems = %v, want some %v", got, tt.want)
			}
		})
	}
}

// TestGPUProofProblems_ReleaseTier: a release proof run with the flag whose
// GPU gate ran and passed is a GPU-host proof.
func TestGPUProofProblems_ReleaseTier(t *testing.T) {
	const tree = "aaaa1111"
	if got := gpuProofProblems(writeGPUProof(t, passingGPUProof(tierRelease, tree)), tree); len(got) != 0 {
		t.Fatalf("a passing release proof with the GPU gate: %v", got)
	}
	r := passingGPUProof(tierRelease, tree)
	r.GPUGateRan = false
	if got := gpuProofProblems(writeGPUProof(t, r), tree); len(got) == 0 {
		t.Fatal("a release proof whose GPU gate did not run passed")
	}
}

// TestRunSerially_ASignalLeavesTestsWithoutOutcome: once the gate's context
// ends no further test starts, and the tests it kept from running have no
// outcome, so the gpu ledger fails rather than passing or skipping them.
func TestRunSerially_ASignalLeavesTestsWithoutOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &runner{slots: 1, c: newCollector(), log: os.Stderr}
	jobs := []job{{ID: "gpu-01", Package: gpuPkg, Tests: []string{"TestIntegrationGPU_A"}, Shard: 1, Attempt: 1}}
	runSerially(ctx, r, jobs)
	k := testKey{gpuPkg, "TestIntegrationGPU_A"}
	l := buildLedgerQ(suiteGPU, []testKey{k}, map[string]map[string]bool{gpuPkg: {"TestIntegrationGPU_A": true}}, r.c, nil, nil)
	if l.ok() || l.Summary.NoOutcome != 1 {
		t.Fatalf("ledger ok %v, summary %+v: a test a signal kept from running must have no outcome", l.ok(), l.Summary)
	}
}
