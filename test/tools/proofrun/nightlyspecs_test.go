package main

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const specRow = "ui/e2e/run-page-ceiling.spec.ts\tui/e2e/run-page-reduced-ceiling.spec.ts\t2026-10-26 Daniel Santos\tthe default ceiling's 4,000 tasks"

func TestNightlySpecs_ParsesTheListStrictly(t *testing.T) {
	cases := []struct {
		name, text, err string
	}{
		{name: "a row", text: specRow + "\n"},
		{name: "comments and blank lines", text: "# header\n\n" + specRow + "\n"},
		{name: "five columns", text: "a\t" + specRow + "\n", err: `want "spec<TAB>covering specs<TAB>approved<TAB>reason", got 5 columns`},
		{name: "an empty column", text: "ui/e2e/a.spec.ts\t\t2026-10-26 Daniel Santos\tr\n", err: "column 2 is empty"},
		{name: "not a spec path", text: "e2e/a.spec.ts\tui/e2e/b.spec.ts\t2026-10-26 Daniel Santos\tr\n", err: `"e2e/a.spec.ts" is not a spec file under ui/e2e/`},
		{name: "a covering path that is not a spec", text: "ui/e2e/a.spec.ts\tui/e2e/b.ts\t2026-10-26 Daniel Santos\tr\n", err: `covering spec "ui/e2e/b.ts" is not a spec file under ui/e2e/`},
		{name: "no approval date", text: "ui/e2e/a.spec.ts\tui/e2e/b.spec.ts\tDaniel\tr\n", err: `approved column "Daniel" is not "YYYY-MM-DD <name>"`},
		{name: "a duplicate", text: specRow + "\n" + specRow + "\n", err: "is already listed on line 1"},
		{name: "a spec covering itself", text: "ui/e2e/a.spec.ts\tui/e2e/a.spec.ts\t2026-10-26 Daniel Santos\tr\n", err: "covers itself"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseNightlySpecs(strings.NewReader(c.text), "list")
			if c.err == "" {
				if err != nil || len(got) != 1 {
					t.Fatalf("entries %v, err %v", got, err)
				}
				if got[0].file() != "run-page-ceiling.spec.ts" || !reflect.DeepEqual(got[0].coveringFiles(), []string{"run-page-reduced-ceiling.spec.ts"}) {
					t.Errorf("entry = %+v", got[0])
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("err = %v, want %q", err, c.err)
			}
		})
	}
}

// A listed spec must be a file the listing holds, and its covering specs
// listed files that are not themselves on the list.
func TestNightlySpecs_ProblemsAgainstTheListing(t *testing.T) {
	entries, err := parseNightlySpecs(strings.NewReader(specRow+"\n"+
		"ui/e2e/gone.spec.ts\tui/e2e/run-page-ceiling.spec.ts,ui/e2e/missing.spec.ts\t2026-10-26 Daniel Santos\tr\n"), "list")
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{"run-page-ceiling.spec.ts": true, "run-page-reduced-ceiling.spec.ts": true}
	got := strings.Join(nightlySpecProblems(entries, listed, true), "\n")
	for _, want := range []string{
		"nightly spec list line 2 names ui/e2e/gone.spec.ts, which the listing does not hold: remove the stale entry",
		"nightly spec list line 2: covering spec ui/e2e/run-page-ceiling.spec.ts is itself on the nightly spec list",
		"nightly spec list line 2: covering spec ui/e2e/missing.spec.ts is not a listed spec file",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("problems lack %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "line 1") {
		t.Errorf("the valid row is a problem:\n%s", got)
	}
	// The nightly tier lists only the specs it runs: there the covering
	// specs are no one's concern.
	if got := nightlySpecProblems(entries[:1], map[string]bool{"run-page-ceiling.spec.ts": true}, false); len(got) != 0 {
		t.Errorf("a nightly listing's problems = %v", got)
	}
}

// The full tier defers the rows both the tree's list and its base's list
// carry: a row the change adds is run, one last time.
func TestNightlySpecs_DeferredSetIsAnchoredToTheBase(t *testing.T) {
	tree, _ := parseNightlySpecs(strings.NewReader(specRow+"\nui/e2e/new.spec.ts\tui/e2e/old.spec.ts\t2026-10-26 Daniel Santos\tr\n"), "tree")
	base, _ := parseNightlySpecs(strings.NewReader(specRow+"\n"), "base")
	got := deferredSpecSet(tree, base)
	if len(got) != 1 || got["run-page-ceiling.spec.ts"].Spec != "ui/e2e/run-page-ceiling.spec.ts" {
		t.Errorf("deferred = %v, want run-page-ceiling.spec.ts alone", got)
	}
	if len(deferredSpecSet(tree, nil)) != 0 {
		t.Error("a base without the list defers something")
	}
}

// A merge needs a nightly proof of main's tip while either nightly list is
// non-empty: a spec list alone makes it due.
func TestNightlySpecs_ASpecListAloneMakesTheNightlyDue(t *testing.T) {
	if p := nightlyProofProblemsWith("", specRow+"\n", "", "main-tree"); len(p) != 1 || !strings.Contains(p[0], "0 test(s) and 1 spec file(s)") || !strings.Contains(p[0], "NIGHTLY=proof/<name>") {
		t.Errorf("problems = %v", p)
	}
	if p := nightlyProofProblemsWith("", "", "", "main-tree"); len(p) != 0 {
		t.Errorf("two empty lists ask for a nightly proof: %v", p)
	}
	if p := nightlyProofProblemsWith("", "not\ta\trow\n", "", "main-tree"); len(p) != 1 || !strings.Contains(p[0], "nightly spec list cannot be read") {
		t.Errorf("an unreadable spec list does not fail closed: %v", p)
	}
}

// The repository's list: every spec it names and every covering spec exists,
// and no covering spec is itself on the list.
func TestRepositoryNightlySpecs_SpecsExistAndCoveringSpecsAreNotListed(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := readNightlySpecs(filepath.Join(root, nightlySpecFile))
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	files, _ := filepath.Glob(filepath.Join(root, "ui", "e2e", "*.spec.ts"))
	for _, f := range files {
		listed[filepath.Base(f)] = true
	}
	for _, p := range nightlySpecProblems(entries, listed, true) {
		t.Error(p)
	}
}
