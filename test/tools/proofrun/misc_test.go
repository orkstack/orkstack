package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestWriteGates(t *testing.T) {
	sel := &selection{Integration: map[string]*pkgSelection{pEngine: {Evidence: map[string]string{"TestX": "named"}}},
		Specs: []selectedSpec{{File: "ui/e2e/admin.spec.ts"}, {File: "ui/e2e/budgets.spec.ts"}}}
	sel.Gates.UICheck, sel.Gates.UIUnit = true, true
	var b strings.Builder
	writeGates(&b, sel)
	want := "integration=yes\nui_check=yes\nui_unit=yes\nsite=no\nui_e2e=yes\nactivity_fixture=no\nobservability=no\nrace_packages=\nspecs=e2e/admin.spec.ts e2e/budgets.spec.ts\n"
	if b.String() != want {
		t.Errorf("got\n%s\nwant\n%s", b.String(), want)
	}
	b.Reset()
	writeGates(&b, &selection{Integration: map[string]*pkgSelection{pEngine: {Evidence: map[string]string{}}}, AllSpecs: "ui/e2e/harness.ts changed"})
	if !strings.Contains(b.String(), "integration=no\n") || !strings.Contains(b.String(), "specs=ALL\n") || !strings.Contains(b.String(), "ui_e2e=yes\n") {
		t.Errorf("got\n%s", b.String())
	}
}

// gatesOutput runs `proofrun gates -select` over sel, as scripts/proof.sh
// does, and returns its stdout.
func gatesOutput(t *testing.T, sel *selection) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "selection.json")
	if err := writeJSON(file, sel); err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	prev := os.Stdout
	os.Stdout = w
	code := cmdGates([]string{"-select", file})
	os.Stdout = prev
	_ = w.Close()
	out, _ := io.ReadAll(r)
	_ = r.Close()
	if code != 0 {
		t.Fatalf("proofrun gates exited %d", code)
	}
	return string(out)
}

// scripts/proof.sh reads the gates from KEY=VALUE lines: the site gate is
// `site=yes` for a selection that carries it, `site=no` for one that does not,
// and the key is in the selection's JSON so a selection written by one process
// is read by another.
func TestGates_PrintTheSiteGate(t *testing.T) {
	site := &selection{}
	site.Gates.Site = true
	if out := gatesOutput(t, site); !strings.Contains(out, "\nsite=yes\n") {
		t.Errorf("a site selection printed\n%s", out)
	}
	if out := gatesOutput(t, &selection{}); !strings.Contains(out, "\nsite=no\n") {
		t.Errorf("a selection without the site printed\n%s", out)
	}
	b, err := json.Marshal(site.Gates)
	if err != nil || !strings.Contains(string(b), `"site":true`) {
		t.Errorf("the selection's gates marshal as %s (%v)", b, err)
	}
	var back selection
	if err := json.Unmarshal([]byte(`{"gates":{"site":true}}`), &back); err != nil || !back.Gates.Site {
		t.Errorf("a selection written with gates.site reads back as %+v (%v)", back.Gates, err)
	}
}

// The gate lines scripts/proof.sh reads are exactly the ones it has a case for
// (plus specs): a gate printed here and unread there would be selected and never run.
func TestGates_EveryPrintedGateIsOneProofShReads(t *testing.T) {
	var sel selection
	sel.Gates.UICheck, sel.Gates.UIUnit, sel.Gates.Site, sel.Gates.Fixture, sel.Gates.Observability = true, true, true, true, true
	sel.RacePackages = []string{"./internal/dsl"}
	script := proofScript(t)
	i := strings.Index(script, "integration | ui_check")
	if i < 0 {
		t.Fatal("proof.sh has no case arm reading the gate lines")
	}
	// The loop that reads them: every case arm up to its closing esac.
	start := strings.LastIndex(script[:i], "\n") + 1
	loop := script[start : i+strings.Index(script[i:], "esac")]
	arms := regexp.MustCompile(`(?m)^\t\t([a-z0-9_ |]+)\)$`).FindAllStringSubmatch(loop, -1)
	read := map[string]bool{}
	for _, a := range arms {
		for _, key := range strings.Split(a[1], "|") {
			read[strings.TrimSpace(key)] = true
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(gatesOutput(t, &sel)), "\n") {
		key, _, _ := strings.Cut(line, "=")
		if !read[key] {
			t.Errorf("proofrun gates prints %s, which proof.sh's case arms %v do not read", key, read)
		}
	}
}

func TestTouching(t *testing.T) {
	scope, err := parseScope(strings.NewReader("internal/telemetry/**.go\tobservability,go\ninternal/**\tgo\ndocs/**\tnone\n"), "t")
	if err != nil {
		t.Fatal(err)
	}
	paths := []changedPath{{Path: "docs/x.md"}, {Path: "internal/telemetry/sink.go"}, {Path: "internal/engine/a.go"}}
	if got := touching(scope, paths, actObservability); strings.Join(got, ",") != "internal/telemetry/sink.go" {
		t.Errorf("got %v", got)
	}
	if got := touching(scope, paths[:1], actObservability); len(got) != 0 {
		t.Errorf("got %v", got)
	}
}

// §6.4's lists_changed, computed from the change set: every changed gate file
// is named, and the three lists that excuse a test carry the rows added and
// removed.
func TestTouches_GateFilesListAddedAndRemovedRows(t *testing.T) {
	const nightly = "scripts/proof-nightly-tests.txt"
	base := "# a comment\n" +
		"example.com/m\tTestSoak\tTestCover\t2026-10-18 Daniel Santos\tkept\n" +
		"example.com/m\tTestOld\tTestCover\t2026-10-18 Daniel Santos\tremoved\n"
	tree := "# a changed comment is not a row\n" +
		"example.com/m\tTestSoak\tTestCover\t2026-10-18 Daniel Santos\tkept\n" +
		"example.com/m\tTestNew\tTestCover\t2026-10-18 Daniel Santos\tadded\n"
	paths := []changedPath{
		{Path: nightly},
		{Path: "scripts/proof-quarantine.txt"},
		{Path: "internal/engine/a.go"},
		{Path: "docs/PROOFS.md"},
	}
	changes := gateFileChanges(paths, func(path string) (string, string) {
		switch path {
		case nightly:
			return base, tree
		case "scripts/proof-quarantine.txt":
			return "", "unit\texample.com/m\tTestX\tDaniel Santos <danlsgiga@gmail.com>\t2026-10-19\t2026-11-18\twhy\n"
		}
		return "", ""
	})
	if len(changes) != 2 {
		t.Fatalf("changes = %+v, want only the two gate files of the diff", changes)
	}
	if changes[0].File != nightly || changes[1].File != "scripts/proof-quarantine.txt" {
		t.Fatalf("changes = %+v", changes)
	}
	if got := strings.Join(changes[0].Added, "|"); !strings.Contains(got, "TestNew") || strings.Contains(got, "TestSoak") {
		t.Errorf("added = %v", changes[0].Added)
	}
	if got := strings.Join(changes[0].Removed, "|"); !strings.Contains(got, "TestOld") || strings.Contains(got, "TestSoak") {
		t.Errorf("removed = %v", changes[0].Removed)
	}
	if len(changes[1].Added) != 1 || !strings.Contains(changes[1].Added[0], "TestX") {
		t.Errorf("a new quarantine row was not reported: %+v", changes[1])
	}
}

func TestTouches_GateFilesAreTheOnesCodeownersMustCover(t *testing.T) {
	for _, p := range []string{
		"scripts/proof.sh", "scripts/proof-nightly-tests.txt", "scripts/github/ruleset-main.json",
		".github/workflows/proof.yml", "test/tools/proofrun/merge.go", "Makefile", "ui/playwright.config.ts",
	} {
		if !isGateFile(p) {
			t.Errorf("%s is not a gate file", p)
		}
	}
	for _, p := range []string{"ui/src/routes/+page.svelte", "internal/engine/workflow.go", "docs/PROOFS.md"} {
		if isGateFile(p) {
			t.Errorf("%s is a gate file", p)
		}
	}
}

// The host-speed table excuses a failure too, so a change to it is itemised
// row by row like the other excusing lists (R2-M1).
func TestTouches_TheHostSpeedTableIsItemisedRowByRow(t *testing.T) {
	const table = "scripts/proof-host-speed.tsv"
	row := "ui\te2e/a.spec.ts\tt\tlessThan 5000\t5\t16\tnote"
	changes := gateFileChanges([]changedPath{{Path: table}}, func(string) (string, string) {
		return "# header\n" + row + "\n", "# header\n" + strings.Replace(row, "\t5\t", "\t9\t", 1) + "\n"
	})
	if len(changes) != 1 || len(changes[0].Added) != 1 || len(changes[0].Removed) != 1 ||
		!strings.Contains(changes[0].Added[0], "\t9\t") || changes[0].Removed[0] != row {
		t.Errorf("changes = %+v, want the loosened row added and the base's row removed", changes)
	}
}

// A failure to list the gate files is written as an error entry, never as
// an empty list a caller would read as "no gate file changed".
func TestTouches_AFailureIsAnErrorEntryNotAnEmptyList(t *testing.T) {
	quiet(t)
	var b strings.Builder
	prev := os.Stderr
	devnull, _ := os.Open(os.DevNull)
	os.Stderr = devnull
	code := touchesGateFiles(&b, "no-such-ref-for-this-test")
	os.Stderr = prev
	var got []listChange
	if err := json.Unmarshal([]byte(b.String()), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, b.String())
	}
	if code != 1 || len(got) != 1 || !strings.Contains(got[0].Error, "is not a commit") {
		t.Errorf("exit %d, %+v", code, got)
	}
}
