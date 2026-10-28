package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The fuzz lane (AGENTS.md §11.2, docs/PROOFS.md §2.1) runs every Fuzz target
// of the module for a bounded time and fails on a crasher, keeping the input.
// The runner writes into the package it fuzzes (a crasher lands in the
// package's testdata/fuzz) and removes it again, so these tests run it in a
// throwaway module and never in the repository's tree.

const fuzzModule = `module example.com/fz

go 1.27
`

const fuzzOKSource = `package fz

import "testing"

func FuzzOK(f *testing.F) {
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, b []byte) {})
}
`

// FuzzCrash fails on any input but its seed, so the engine finds a crasher at
// once and writes it to testdata/fuzz/FuzzCrash.
const fuzzCrashSource = `package fz

import "testing"

func FuzzCrash(f *testing.F) {
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if string(b) != "seed" {
			t.Fatalf("crash on %q", b)
		}
	})
}
`

// fuzzModuleDir writes a throwaway module with the given files and points the
// fuzz runner at it.
func fuzzModuleDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	files["go.mod"] = fuzzModule
	for name, text := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	prev := fuzzWorkDir
	fuzzWorkDir = dir
	t.Cleanup(func() { fuzzWorkDir = prev })
	return dir
}

func TestParseFuzzTargets_KeepsOnlyFuzzFunctions(t *testing.T) {
	out := []byte(`{"Action":"output","Package":"ex.com/a","Output":"FuzzA\n"}
{"Action":"output","Package":"ex.com/a","Output":"FuzzB_withUnderscore\n"}
{"Action":"output","Package":"ex.com/a","Output":"TestNotFuzz\n"}
{"Action":"output","Package":"ex.com/a","Output":"ok  \tex.com/a\t0.002s\n"}
{"Action":"pass","Package":"ex.com/a"}
{"Action":"output","Package":"ex.com/b","Output":"FuzzA\n"}
{"Action":"output","Package":"ex.com/b","Output":"FuzzA\n"}
`)
	got := parseFuzzTargets(out)
	want := []testKey{{"ex.com/a", "FuzzA"}, {"ex.com/a", "FuzzB_withUnderscore"}, {"ex.com/b", "FuzzA"}}
	if len(got) != len(want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("target %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestFuzzTime_IsAPositiveDurationOrAnIterationCount(t *testing.T) {
	for _, ok := range []string{"2m", "30s", "1h", "90s", "1500ms", "100x", "1x"} {
		if err := checkFuzzTime(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "0", "0s", "-5s", "2", "x", "0x", "1.5x", "abc", "2 m", "5m;rm"} {
		if err := checkFuzzTime(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestFuzzTarget_APackageWithNoDirectoryFailsRatherThanPasses(t *testing.T) {
	e := fuzzTarget(testKey{"example.com/x", "FuzzX"}, "", "1s", t.TempDir())
	if e.Outcome != outcomeFail || !strings.Contains(e.Reason, "no directory") {
		t.Errorf("entry = %+v", e)
	}
}

func TestFuzzRunner_FlagsAreValidatedBeforeAnythingIsWritten(t *testing.T) {
	quiet(t)
	out := filepath.Join(t.TempDir(), "out")
	for name, args := range map[string][]string{
		"no -out":                      {"-fuzztime", "1s"},
		"a bad fuzztime":               {"-out", out, "-fuzztime", "forever"},
		"neither a budget nor a time":  {"-out", out},
		"both a budget and a time":     {"-out", out, "-budget", "30m", "-fuzztime", "10s"},
		"a bad budget":                 {"-out", out, "-budget", "forever"},
		"a budget of no time":          {"-out", out, "-budget", "0s"},
		"a negative budget":            {"-out", out, "-budget", "-5m"},
		"an iteration count as budget": {"-out", out, "-budget", "100x"},
		"a budget below one target":    {"-out", out, "-budget", "10s"},
	} {
		if code := cmdFuzz(args); code != 2 {
			t.Errorf("%s: exit %d, want 2", name, code)
		}
	}
	if _, err := os.Stat(out); err == nil {
		t.Error("a refused invocation created its output directory")
	}
}

func TestFuzzRunner_APassingTargetAndACrashingOne(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -fuzz")
	}
	quiet(t)
	// Each target is fuzzed for a fixed 3s, so neither outcome depends on how
	// long this host takes to build the instrumented binaries: under a 12s
	// budget, a cold build on a loaded CI runner (26s for the first target)
	// once spent it all and left FuzzOK never fuzzed. How a budget is shared
	// is proven with an injected clock (fuzzbudget_test.go). The crasher a
	// short fuzz time finds is kept and fails the lane as it does for a longer
	// one.
	mod := fuzzModuleDir(t, map[string]string{"ok_test.go": fuzzOKSource, "crash_test.go": fuzzCrashSource})
	out := filepath.Join(t.TempDir(), "out")
	code := cmdFuzz([]string{"-out", out, "-fuzztime", "3s"})
	if code != 1 {
		t.Fatalf("exit %d, want 1: FuzzCrash has a crasher", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "fuzz-tests.json"), &l); err != nil {
		t.Fatalf("the lane's ledger is fuzz-tests.json: %v", err)
	}
	if l.Suite != suiteFuzz || l.Summary.Expected != 2 || l.Summary.Pass != 1 || l.Summary.Fail != 1 {
		t.Fatalf("ledger = %+v", l)
	}
	by := map[string]entry{}
	for _, e := range l.Tests {
		by[e.Test] = e
	}
	for _, name := range []string{"FuzzOK", "FuzzCrash"} {
		if e := by[name]; e.FuzzSeconds != 3 || e.Seconds < e.FuzzSeconds && e.Outcome == outcomePass {
			t.Errorf("%s: fuzz_seconds %v, seconds %v: the ledger must record the fuzz time it was given (3s here)", name, e.FuzzSeconds, e.Seconds)
		}
	}
	if by["FuzzOK"].Outcome != outcomePass || by["FuzzCrash"].Outcome != outcomeFail {
		t.Errorf("outcomes: FuzzOK %q, FuzzCrash %q", by["FuzzOK"].Outcome, by["FuzzCrash"].Outcome)
	}
	if !strings.Contains(by["FuzzCrash"].Reason, "crasher") {
		t.Errorf("the failing target's reason = %q, want it to name the crasher", by["FuzzCrash"].Reason)
	}
	problems := strings.Join(l.Problems, "\n")
	if !strings.Contains(problems, "example.com/fz.FuzzCrash") || strings.Contains(problems, "FuzzOK") {
		t.Errorf("problems = %s", problems)
	}

	// The crasher is kept under the output directory, as the input the engine
	// wrote, and removed from the tree it was fuzzed in.
	kept, _ := filepath.Glob(filepath.Join(out, "crashers", "*", "FuzzCrash", "*"))
	if len(kept) != 1 {
		t.Fatalf("kept crashers = %v, want exactly one", kept)
	}
	body, err := os.ReadFile(kept[0])
	if err != nil || !strings.HasPrefix(string(body), "go test fuzz v1") {
		t.Errorf("the kept crasher %s = %q (%v), want a go test fuzz corpus file", kept[0], body, err)
	}
	if _, err := os.Stat(filepath.Join(mod, "testdata")); err == nil {
		t.Error("the crasher's directory was left in the fuzzed tree")
	}
	// Each target keeps its own log.
	logs, _ := filepath.Glob(filepath.Join(out, "fuzz", "*.log"))
	if len(logs) != 2 {
		t.Errorf("logs = %v, want one per target", logs)
	}
}

func TestFuzzRunner_ATrackedCorpusIsNeitherMovedNorMistakenForACrasher(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -fuzz")
	}
	quiet(t)
	// FuzzOK has a committed corpus entry; fuzzing it must leave that entry
	// alone and report no crasher.
	mod := fuzzModuleDir(t, map[string]string{
		"ok_test.go":                     fuzzOKSource,
		"testdata/fuzz/FuzzOK/committed": "go test fuzz v1\n[]byte(\"kept\")\n",
	})
	out := filepath.Join(t.TempDir(), "out")
	if code := cmdFuzz([]string{"-out", out, "-fuzztime", "2s"}); code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
	if _, err := os.Stat(filepath.Join(mod, "testdata", "fuzz", "FuzzOK", "committed")); err != nil {
		t.Errorf("the committed corpus entry is gone: %v", err)
	}
	if kept, _ := filepath.Glob(filepath.Join(out, "crashers", "*")); len(kept) != 0 {
		t.Errorf("crashers kept for a passing target: %v", kept)
	}
}

func TestFuzzRunner_NoTargetIsAProblemNotAPass(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -list")
	}
	quiet(t)
	fuzzModuleDir(t, map[string]string{"plain_test.go": "package fz\n\nimport \"testing\"\n\nfunc TestPlain(t *testing.T) {}\n"})
	out := filepath.Join(t.TempDir(), "out")
	if code := cmdFuzz([]string{"-out", out, "-fuzztime", "1s"}); code != 1 {
		t.Fatalf("exit %d, want 1: a lane that fuzzed nothing proves nothing", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "fuzz-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	if l.ok() || !strings.Contains(strings.Join(l.Problems, "\n"), "no Fuzz target") {
		t.Errorf("ledger = %+v", l)
	}
}
