package main

import (
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// A miniature repository: two integration packages (engine, orkctl), one
// plain package (dsl), the UI's specs and unit tests, a test tool and an
// example. The dependency index is written by hand to match it, so these
// tests exercise the selection rules and nothing of `go list`.

const (
	pEngine = modulePath + "/internal/engine"
	pOrker  = modulePath + "/internal/orker"
	pDSL    = modulePath + "/internal/dsl"
	pOrkctl = modulePath + "/cmd/orkctl"
	pOrk    = modulePath + "/cmd/ork"
	pOrkerB = modulePath + "/cmd/orker"
)

var baseTree = map[string]string{
	"internal/engine/run.go":      "package engine\n\nfunc Run() int { return 1 }\n",
	"internal/engine/run_test.go": "package engine\n\nimport \"testing\"\n\nfunc TestRunUnit(t *testing.T) { _ = Run() }\n",
	"internal/engine/live_integration_test.go": `//go:build integration

package engine

import "testing"

func boot(t *testing.T) int { return Run() }

func TestIntegration_Fast(t *testing.T) { _ = boot(t) }

func TestIntegration_Slow(t *testing.T) { _ = boot(t) }

func TestIntegration_Uses(t *testing.T) { _ = Run() }
`,
	"internal/orker/activity.go":   "package orker\n\nfunc Boot() {}\n",
	"internal/dsl/parse.go":        "package dsl\n\nfunc Parse() int { return 1 }\n",
	"internal/dsl/parse_test.go":   "package dsl\n\nimport \"testing\"\n\nfunc TestParse(t *testing.T) { _ = Parse() }\n",
	"internal/dsl/fuzz_test.go":    dslFuzzFile,
	"internal/dsl/testdata/a.yaml": "a: 1\n",
	"cmd/orkctl/main.go":           "package main\n\nfunc main() {}\n",
	"cmd/orkctl/cli_integration_test.go": `//go:build integration

package main

import "testing"

func sign(t *testing.T) string { return "./test/tools/cosign-sign" }

func TestIntegration_CLI(t *testing.T) {}

func TestIntegration_Signs(t *testing.T) { _ = sign(t) }

func TestIntegration_Examples(t *testing.T) { _ = "examples/hello.ork.yaml" }
`,
	"ui/e2e/admin.spec.ts":            "test('admin', () => {});\n",
	"ui/e2e/examples-gallery.spec.ts": "test('gallery', () => {});\n",
	"ui/e2e/signing.spec.ts":          "execFileSync('go', ['run', './test/tools/cosign-sign']);\n",
	"ui/test/flakes.test.ts":          "test('flakes', () => {});\n",
	"ui/src/lib/flakes.ts":            "export const x = 1;\n",
	"ui/src/routes/page.svelte":       "<p>hi</p>\n",
	"test/tools/cosign-sign/main.go":  "package main\n\nfunc main() {}\n",
	"test/tools/lonely/main.go":       "package main\n\nfunc main() {}\n",
	"examples/hello.ork.yaml":         "name: hello\n",
	"images/activity-shell/entry.sh":  "#!/bin/sh\n",
	"docs/DSL.md":                     "# DSL\n",
	"README.md":                       "# OrkStack\n",
	"deploy/orkstack.defaults.yaml":   "engine:\n  orkflow_max_tasks: 4000\n",
	"proto/orkstack/v1/run.proto":     "syntax = \"proto3\";\n",
	"site/package.json":               "{\"name\": \"orkstack-site\"}\n",
	"site/src/pages/index.astro":      "<h1>OrkStack</h1>\n",
	"ui/src/tokens.css":               ":root { --color-bg: #fff; }\n",
	"ui/src/lib/icons.ts":             "export const ICONS = {};\n",
	"ui/package.json":                 "{\"name\": \"ui\"}\n",
}

func fakeDeps() depIndex {
	set := func(p ...string) map[string]bool {
		m := map[string]bool{}
		for _, x := range p {
			m[x] = true
		}
		return m
	}
	return depIndex{
		test: map[string]map[string]map[string]bool{
			cfgUnit: {
				pEngine: set(pEngine, pDSL),
				pDSL:    set(pDSL),
				pOrkctl: set(pOrkctl, pDSL),
			},
			cfgIntegration: {
				pEngine: set(pEngine, pDSL),
				pDSL:    set(pDSL),
				pOrkctl: set(pOrkctl, pDSL),
			},
		},
		bins: map[string][]binaryUse{pOrkctl: {{Package: pOrk}, {Package: pOrkerB}}},
		binDeps: map[string]map[string]bool{
			pOrk:    set(pOrk, pEngine, pDSL),
			pOrkerB: set(pOrkerB, pOrker, pEngine),
		},
	}
}

type change struct {
	path    string
	content string // "" deletes the file
}

// fakeSelect writes the base tree with changes applied to a temp dir (the
// new side), keeps the base tree as the old side, and selects.
func fakeSelect(t *testing.T, changes []change, tests, specs []string) *selection {
	t.Helper()
	return fakeSelectSlow(t, changes, tests, specs, map[testKey]slowEntry{{pEngine, "TestIntegration_Slow"}: {Seconds: 120}})
}

// fakeSelectSlow is fakeSelect with a slow list of the caller's own.
func fakeSelectSlow(t *testing.T, changes []change, tests, specs []string, slow map[testKey]slowEntry) *selection {
	t.Helper()
	root := t.TempDir()
	newTree := map[string]string{}
	for p, c := range baseTree {
		newTree[p] = c
	}
	var paths []changedPath
	for _, c := range changes {
		status := "M"
		if _, existed := baseTree[c.path]; !existed {
			status = "A"
		}
		if c.content == "" {
			delete(newTree, c.path)
			status = "D"
		} else {
			newTree[c.path] = c.content
		}
		paths = append(paths, changedPath{Path: c.path, Status: status})
	}
	for p, c := range newTree {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	scope, err := readScope(filepath.Join("..", "..", "..", "scripts", "proof-scope.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	pkgs := func(names ...string) []pkgInfo {
		var out []pkgInfo
		for _, n := range names {
			out = append(out, pkgInfo{ImportPath: n, Dir: filepath.Join(root, relDir(n))})
		}
		return out
	}
	in := selectInput{
		root: root, paths: paths, scope: scope, tests: tests, specs: specs,
		slow: slow,
		readNew: func(p string) []byte {
			if c, ok := newTree[p]; ok {
				return []byte(c)
			}
			return nil
		},
		readOld: func(p string) []byte {
			if c, ok := baseTree[p]; ok {
				return []byte(c)
			}
			return nil
		},
		listOld: func(dir string) []string {
			var out []string
			for p := range baseTree {
				if path.Dir(p) == dir {
					out = append(out, path.Base(p))
				}
			}
			return out
		},
		deps:            func() (depIndex, []string, error) { return fakeDeps(), nil, nil },
		integrationPkgs: func() ([]pkgInfo, error) { return pkgs(pEngine, pOrkctl), nil },
		allPkgs:         func() ([]pkgInfo, error) { return pkgs(pEngine, pDSL, pOrkctl, pOrker), nil },
	}
	sel := &selection{Integration: map[string]*pkgSelection{}}
	if err := selectInto(sel, in); err != nil {
		t.Fatal(err)
	}
	return sel
}

func evidenceNames(sel *selection, pkg string) []string {
	ps := sel.Integration[pkg]
	if ps == nil {
		return []string{}
	}
	return keys(ps.Evidence)
}

func regressionNames(sel *selection, pkg string) []string {
	ps := sel.Integration[pkg]
	if ps == nil {
		return []string{}
	}
	return orEmpty(ps.Regression)
}

func unitNames(sel *selection) []string {
	out := []string{}
	for _, u := range sel.UnitEvidence {
		out = append(out, relDir(u.Package)+"."+u.Test)
	}
	sort.Strings(out)
	return out
}

func specFiles(sel *selection) []string {
	out := []string{}
	for _, s := range sel.Specs {
		out = append(out, s.File)
	}
	return out
}

func requireErrors(t *testing.T, sel *selection, fragments ...string) {
	t.Helper()
	joined := strings.Join(sel.Errors, "\n")
	if len(fragments) == 0 && len(sel.Errors) > 0 {
		t.Fatalf("unexpected errors:\n%s", joined)
	}
	for _, f := range fragments {
		if !strings.Contains(joined, f) {
			t.Errorf("errors lack %q:\n%s", f, joined)
		}
	}
}

var engineChange = change{"internal/engine/run.go", "package engine\n\nfunc Run() int { return 2 }\n"}

func TestSelect_ProductionChangeWithoutEvidenceIsRefused(t *testing.T) {
	sel := fakeSelect(t, []change{engineChange}, nil, nil)
	requireErrors(t, sel, "internal/engine changed (func Run) and no evidence test depends on it",
		"integration packages that depend on it: internal/engine, cmd/orkctl")
}

func TestSelect_NamedIntegrationEvidenceAndTheRegressionSet(t *testing.T) {
	sel := fakeSelect(t, []change{engineChange}, []string{"internal/engine:TestIntegration_Uses"}, nil)
	requireErrors(t, sel)
	if got, want := evidenceNames(sel, pEngine), []string{"TestIntegration_Uses"}; !reflect.DeepEqual(got, want) {
		t.Errorf("engine evidence = %v, want %v", got, want)
	}
	// The slow test is left out and named; the evidence is not repeated.
	if got, want := regressionNames(sel, pEngine), []string{"TestIntegration_Fast"}; !reflect.DeepEqual(got, want) {
		t.Errorf("engine regression = %v, want %v", got, want)
	}
	if d := sel.Integration[pEngine].SlowOmitted; len(d) != 1 || d[0].Test != "TestIntegration_Slow" || d[0].Seconds != 120 {
		t.Errorf("engine slow-omitted = %v", d)
	}
	// orkctl links no engine, but runs cmd/ork, which does: a binary edge.
	if got, want := regressionNames(sel, pOrkctl), []string{"TestIntegration_CLI", "TestIntegration_Examples", "TestIntegration_Signs"}; !reflect.DeepEqual(got, want) {
		t.Errorf("orkctl regression = %v, want %v", got, want)
	}
	if !strings.Contains(sel.Integration[pOrkctl].Why, "runs cmd/ork, which links it") {
		t.Errorf("orkctl why = %q", sel.Integration[pOrkctl].Why)
	}
	// The regression set is never evidence.
	if len(sel.Evidence) != 1 || !reflect.DeepEqual(sel.Evidence[0].Evidence, []string{"internal/engine.TestIntegration_Uses"}) {
		t.Errorf("evidence = %+v", sel.Evidence)
	}
	// And the UI suite, which runs ork and orker, ran no spec for it.
	if !strings.Contains(strings.Join(sel.Notes, "\n"), "no Playwright spec ran for the Go changes in internal/engine; only the merge proof runs them") {
		t.Errorf("notes = %v", sel.Notes)
	}
}

func TestSelect_TemporalFacingPackagesNeedIntegrationEvidence(t *testing.T) {
	sel := fakeSelect(t, []change{engineChange}, []string{"internal/engine:TestRunUnit"}, nil)
	requireErrors(t, sel, "internal/engine is Temporal- or storage-facing and its evidence has no integration test")
	if got := unitNames(sel); !reflect.DeepEqual(got, []string{"internal/engine.TestRunUnit"}) {
		t.Errorf("unit evidence = %v", got)
	}
}

func TestSelect_UnitEvidenceIsEnoughForAPlainPackage(t *testing.T) {
	sel := fakeSelect(t, []change{{"internal/dsl/parse.go", "package dsl\n\nfunc Parse() int { return 2 }\n"}},
		[]string{"./internal/dsl:TestParse"}, nil)
	requireErrors(t, sel)
	if got := unitNames(sel); !reflect.DeepEqual(got, []string{"internal/dsl.TestParse"}) {
		t.Errorf("unit evidence = %v", got)
	}
	// Both integration packages link dsl: both run their regression set.
	if len(regressionNames(sel, pEngine)) == 0 || len(regressionNames(sel, pOrkctl)) == 0 {
		t.Errorf("regression = engine %v, orkctl %v", regressionNames(sel, pEngine), regressionNames(sel, pOrkctl))
	}
}

func TestSelect_TestdataIsAChangeToItsPackage(t *testing.T) {
	sel := fakeSelect(t, []change{{"internal/dsl/testdata/a.yaml", "a: 2\n"}}, nil, nil)
	requireErrors(t, sel, "internal/dsl changed (a.yaml) and no evidence test depends on it")
}

func TestSelect_AChangedTestIsItsOwnEvidenceAndRunsEvenWhenSlow(t *testing.T) {
	sel := fakeSelect(t, []change{{"internal/engine/live_integration_test.go",
		strings.Replace(baseTree["internal/engine/live_integration_test.go"], "func TestIntegration_Slow(t *testing.T) { _ = boot(t) }", "func TestIntegration_Slow(t *testing.T) { _ = boot(t); t.Log(1) }", 1)}}, nil, nil)
	requireErrors(t, sel)
	if got := evidenceNames(sel, pEngine); !reflect.DeepEqual(got, []string{"TestIntegration_Slow"}) {
		t.Errorf("evidence = %v", got)
	}
	if d := sel.Integration[pEngine].SlowOmitted; len(d) != 0 {
		t.Errorf("a changed slow test was left out: %v", d)
	}
	if got := sel.Integration[pEngine].run(); !reflect.DeepEqual(got, []string{"TestIntegration_Fast", "TestIntegration_Slow", "TestIntegration_Uses"}) {
		t.Errorf("run = %v", got)
	}
}

func TestSelect_AChangedHelperSelectsItsCallers(t *testing.T) {
	sel := fakeSelectSlow(t, []change{bootChange}, nil, nil, map[testKey]slowEntry{})
	requireErrors(t, sel)
	if got := evidenceNames(sel, pEngine); !reflect.DeepEqual(got, []string{"TestIntegration_Fast", "TestIntegration_Slow"}) {
		t.Errorf("evidence = %v", got)
	}
}

// bootChange edits boot, the helper TestIntegration_Fast and
// TestIntegration_Slow reach; TestIntegration_Uses does not.
var bootChange = change{"internal/engine/live_integration_test.go",
	strings.Replace(baseTree["internal/engine/live_integration_test.go"], "func boot(t *testing.T) int { return Run() }", "func boot(t *testing.T) int { return Run() + 1 }", 1)}

// A test that is evidence only because it reaches a changed helper is held
// to the slow list, as the regression set is: the merge proof runs it. A
// test whose own source changed, and every --tests name, always runs; and
// when every caller a package's helper change reaches is slow, the cheapest
// one stays, so the change still has a test that exercises it.
func TestSelect_TheSlowListAppliesToHelperReachedEvidence(t *testing.T) {
	slowTest := map[testKey]slowEntry{{pEngine, "TestIntegration_Slow"}: {Seconds: 120}}
	bothSlow := map[testKey]slowEntry{{pEngine, "TestIntegration_Slow"}: {Seconds: 120}, {pEngine, "TestIntegration_Fast"}: {Seconds: 30}}
	slowAlsoChanged := change{bootChange.path, strings.Replace(bootChange.content,
		"func TestIntegration_Slow(t *testing.T) { _ = boot(t) }", "func TestIntegration_Slow(t *testing.T) { _ = boot(t); t.Log(1) }", 1)}
	cases := []struct {
		name         string
		changes      []change
		tests        []string
		slow         map[testKey]slowEntry
		evidence     []string
		slowOmitted  []string
		run          []string
		keptCheapest string
	}{
		{
			name: "a slow caller of a changed helper is left to the merge proof", changes: []change{bootChange}, slow: slowTest,
			evidence: []string{"TestIntegration_Fast"}, slowOmitted: []string{"TestIntegration_Slow"},
			run: []string{"TestIntegration_Fast", "TestIntegration_Uses"},
		},
		{
			name: "when every caller is slow the cheapest stays as evidence", changes: []change{bootChange}, slow: bothSlow,
			evidence: []string{"TestIntegration_Fast"}, slowOmitted: []string{"TestIntegration_Slow"},
			run: []string{"TestIntegration_Fast", "TestIntegration_Uses"}, keptCheapest: "TestIntegration_Fast",
		},
		{
			name: "a slow caller named by --tests always runs", changes: []change{bootChange}, slow: slowTest,
			tests:    []string{"internal/engine:TestIntegration_Slow"},
			evidence: []string{"TestIntegration_Fast", "TestIntegration_Slow"}, slowOmitted: []string{},
			run: []string{"TestIntegration_Fast", "TestIntegration_Slow", "TestIntegration_Uses"},
		},
		{
			name: "a slow caller whose own source changed always runs", changes: []change{slowAlsoChanged}, slow: slowTest,
			evidence: []string{"TestIntegration_Fast", "TestIntegration_Slow"}, slowOmitted: []string{},
			run: []string{"TestIntegration_Fast", "TestIntegration_Slow", "TestIntegration_Uses"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sel := fakeSelectSlow(t, c.changes, c.tests, nil, c.slow)
			requireErrors(t, sel)
			ps := sel.Integration[pEngine]
			if got := evidenceNames(sel, pEngine); !reflect.DeepEqual(got, c.evidence) {
				t.Errorf("evidence = %v, want %v", got, c.evidence)
			}
			omitted := []string{}
			for _, d := range ps.SlowOmitted {
				omitted = append(omitted, d.Test)
			}
			if !reflect.DeepEqual(omitted, c.slowOmitted) {
				t.Errorf("slow omitted = %v, want %v", omitted, c.slowOmitted)
			}
			for _, d := range ps.SlowOmitted {
				if !strings.Contains(d.Reached, "reaches changed boot") {
					t.Errorf("%s is left out without the reach that selected it: %+v", d.Test, d)
				}
			}
			if got := ps.run(); !reflect.DeepEqual(got, c.run) {
				t.Errorf("run = %v, want %v", got, c.run)
			}
			if c.keptCheapest != "" && !strings.Contains(ps.Evidence[c.keptCheapest], "the cheapest") {
				t.Errorf("%s stays without saying why: %q", c.keptCheapest, ps.Evidence[c.keptCheapest])
			}
			// The README says why each one was left out.
			sel.SlowSource = "proof/x (commit abc)"
			var r strings.Builder
			featureReport(&r, sel, outcomes{}, nil)
			for _, d := range c.slowOmitted {
				want := "left out: on the slow list (120s in proof/x (commit abc)): " + d + " — evidence only because it reaches changed boot; the merge proof runs it"
				if !strings.Contains(r.String(), want) {
					t.Errorf("the README lacks %q:\n%s", want, r.String())
				}
			}
		})
	}
}

func TestSelect_NamedSlowTestRunsAndAnchoredPatternsMatchWholeNames(t *testing.T) {
	sel := fakeSelect(t, []change{engineChange}, []string{"internal/engine:TestIntegration_Slow", "internal/engine:TestIntegration_Fa"}, nil)
	requireErrors(t, sel, `--tests "internal/engine:TestIntegration_Fa" matches no test declared in internal/engine`)
	if got := evidenceNames(sel, pEngine); !reflect.DeepEqual(got, []string{"TestIntegration_Slow"}) {
		t.Errorf("evidence = %v", got)
	}
}

func TestSelect_SpecsAndUISource(t *testing.T) {
	sel := fakeSelect(t, []change{{"ui/src/routes/page.svelte", "<p>bye</p>\n"}}, nil, nil)
	requireErrors(t, sel, "ui/src changed (ui/src/routes/page.svelte) and no spec proves it")

	sel = fakeSelect(t, []change{{"ui/src/routes/page.svelte", "<p>bye</p>\n"}}, nil, []string{"admin.spec.ts"})
	requireErrors(t, sel)
	if got := specFiles(sel); !reflect.DeepEqual(got, []string{"ui/e2e/admin.spec.ts"}) {
		t.Errorf("specs = %v", got)
	}
	if !sel.Gates.UICheck || !sel.Gates.UIUnit {
		t.Error("a UI change must run the UI type-check and unit suites")
	}

	sel = fakeSelect(t, nil, nil, []string{"ui/e2e/missing.spec.ts"})
	requireErrors(t, sel, `--specs "ui/e2e/missing.spec.ts": no such file`)
}

func TestSelect_UILibraryChangeMayBeProvenByAUnitTest(t *testing.T) {
	sel := fakeSelect(t, []change{{"ui/src/lib/flakes.ts", "export const x = 2;\n"}, {"ui/test/flakes.test.ts", "test('flakes', () => { x; });\n"}}, nil, nil)
	requireErrors(t, sel)
	if len(sel.Specs) != 0 {
		t.Errorf("specs = %v", sel.Specs)
	}
}

func TestSelect_HarnessChangeRunsEverySpec(t *testing.T) {
	sel := fakeSelect(t, []change{{"ui/e2e/harness.ts", "export {};\n"}}, nil, nil)
	requireErrors(t, sel)
	if sel.AllSpecs == "" {
		t.Error("a harness change must run every spec")
	}
}

func TestSelect_SuiteWidePathsAndUnmappedPathsAreRefused(t *testing.T) {
	sel := fakeSelect(t, []change{{"go.mod", "module x\n"}, {"Dockerfile", "FROM scratch\n"}}, nil, nil)
	requireErrors(t, sel, "go.mod: this path changes how every suite runs; run make proof",
		"Dockerfile: unmapped path; add a rule to scripts/proof-scope.tsv or run make proof")
}

func TestSelect_DocsOnlyRunsNothingButTheFastGates(t *testing.T) {
	sel := fakeSelect(t, []change{{"docs/DSL.md", "# DSL, revised\n"}}, nil, nil)
	requireErrors(t, sel)
	if len(sel.Integration) != 0 || len(sel.Specs) != 0 || sel.Gates.UICheck || sel.Gates.Fixture {
		t.Errorf("a docs change selected %+v", sel)
	}
	// The site renders the documentation, so its gate is the one fast gate a
	// docs change adds.
	if !sel.Gates.Site {
		t.Error("a docs change must run the site gate: the site renders it")
	}
}

func TestSelect_ToolChangeFindsItsUsersBySearch(t *testing.T) {
	sel := fakeSelect(t, []change{{"test/tools/cosign-sign/main.go", "package main\n\nfunc main() { println() }\n"}}, nil, nil)
	requireErrors(t, sel)
	if got := evidenceNames(sel, pOrkctl); !reflect.DeepEqual(got, []string{"TestIntegration_Signs"}) {
		t.Errorf("orkctl evidence = %v", got)
	}
	if got := specFiles(sel); !reflect.DeepEqual(got, []string{"ui/e2e/signing.spec.ts"}) {
		t.Errorf("specs = %v", got)
	}

	sel = fakeSelect(t, []change{{"test/tools/lonely/main.go", "package main\n\nfunc main() { println() }\n"}}, nil, nil)
	requireErrors(t, sel, "test/tools/lonely changed, and no test or spec runs it")
}

func TestSelect_ExampleChangeRunsTheExampleTestsAndTheGallery(t *testing.T) {
	sel := fakeSelect(t, []change{{"examples/hello.ork.yaml", "name: hello\nversion: 2\n"}}, nil, nil)
	requireErrors(t, sel)
	if got := evidenceNames(sel, pOrkctl); !reflect.DeepEqual(got, []string{"TestIntegration_Examples"}) {
		t.Errorf("orkctl evidence = %v", got)
	}
	if got := specFiles(sel); !reflect.DeepEqual(got, []string{"ui/e2e/examples-gallery.spec.ts"}) {
		t.Errorf("specs = %v", got)
	}
}

func TestSelect_ImageChangeRunsTheFixtureAndTheOrkerRegression(t *testing.T) {
	sel := fakeSelect(t, []change{{"images/activity-shell/entry.sh", "#!/bin/sh\nexit 0\n"}}, nil, nil)
	requireErrors(t, sel)
	if !sel.Gates.Fixture {
		t.Error("an image change must run the activity contract fixture")
	}
	// Only orkctl reaches internal/orker (through cmd/orker).
	if _, ok := sel.Integration[pOrkctl]; !ok {
		t.Errorf("integration = %v", keys2(sel.Integration))
	}
	if _, ok := sel.Integration[pEngine]; ok {
		t.Error("internal/engine does not link internal/orker in this tree")
	}
}

func TestSelect_DeletedTestsAreListedAsRemoved(t *testing.T) {
	sel := fakeSelect(t, []change{{"internal/engine/live_integration_test.go",
		strings.Replace(baseTree["internal/engine/live_integration_test.go"], "func TestIntegration_Uses(t *testing.T) { _ = Run() }\n", "", 1)}}, nil, nil)
	requireErrors(t, sel)
	if !reflect.DeepEqual(sel.Removed, []string{pEngine + ".TestIntegration_Uses"}) {
		t.Errorf("removed = %v", sel.Removed)
	}
}

func keys2(m map[string]*pkgSelection) []string { return sortedKeys(m) }

// ---- the site gate ----------------------------------------------------------

// A change to the site itself runs the site gate and nothing else: the gate is
// its evidence, so no Go test, spec or UI gate is asked for on its account.
func TestSelect_ASiteChangeRunsTheSiteGateAndNeedsNoGoEvidence(t *testing.T) {
	sel := fakeSelect(t, []change{{"site/src/pages/index.astro", "<h1>OrkStack, revised</h1>\n"}, {"site/package.json", "{\"name\": \"orkstack-site\", \"private\": true}\n"}}, nil, nil)
	requireErrors(t, sel)
	if !sel.Gates.Site {
		t.Error("a change under site/ must run the site gate")
	}
	if sel.Gates.UICheck || sel.Gates.UIUnit || sel.Gates.Fixture || sel.Gates.Observability {
		t.Errorf("a site change ran other gates: %+v", sel.Gates)
	}
	if len(sel.Integration) != 0 || len(sel.Specs) != 0 || len(sel.UnitEvidence) != 0 || sel.AllSpecs != "" {
		t.Errorf("a site change asked for Go or Playwright evidence: %+v", sel)
	}
	// The README states which gate proves which paths.
	var site *evidenceFor
	for i := range sel.Evidence {
		if strings.HasPrefix(sel.Evidence[i].Target, "site (") {
			site = &sel.Evidence[i]
		}
	}
	if site == nil {
		t.Fatalf("no evidence entry for the site paths: %+v", sel.Evidence)
	}
	if want := "site (site/package.json, site/src/pages/index.astro)"; site.Target != want {
		t.Errorf("evidence target = %q, want %q", site.Target, want)
	}
	if !reflect.DeepEqual(site.Evidence, []string{"the site gate"}) {
		t.Errorf("site evidence = %v", site.Evidence)
	}
}

// The design tokens are the UI's source and the site's whole style: a change
// to them runs the site gate and keeps every UI requirement it always had.
func TestSelect_TheTokensFileRunsTheSiteGateAndStillTheUIGates(t *testing.T) {
	tokens := change{"ui/src/tokens.css", ":root { --color-bg: #eee; }\n"}
	sel := fakeSelect(t, []change{tokens}, nil, nil)
	requireErrors(t, sel, "ui/src changed (ui/src/tokens.css) and no spec proves it")
	if !sel.Gates.Site {
		t.Error("a tokens.css change must run the site gate")
	}

	sel = fakeSelect(t, []change{tokens}, nil, []string{"admin.spec.ts"})
	requireErrors(t, sel)
	if !sel.Gates.Site || !sel.Gates.UICheck || !sel.Gates.UIUnit {
		t.Errorf("a tokens.css change must run the site gate and the UI type-check and unit suites: %+v", sel.Gates)
	}
	if got := specFiles(sel); !reflect.DeepEqual(got, []string{"ui/e2e/admin.spec.ts"}) {
		t.Errorf("specs = %v", got)
	}

	// The rest of ui/src is not the site's input.
	sel = fakeSelect(t, []change{{"ui/src/routes/page.svelte", "<p>bye</p>\n"}}, nil, []string{"admin.spec.ts"})
	requireErrors(t, sel)
	if sel.Gates.Site {
		t.Error("a change to a UI page ran the site gate")
	}
}

// Every input the site renders or takes its style from can break it, so a
// change to any of them runs the gate. The change needs no Go evidence on that
// account.
func TestSelect_EveryInputOfTheSiteRunsTheSiteGate(t *testing.T) {
	for _, c := range []struct {
		name   string
		change change
		specs  []string
	}{
		{"a changed doc", change{"docs/DSL.md", "# DSL, revised\n"}, nil},
		{"a new doc", change{"docs/NEW.md", "# New\n"}, nil},
		{"a deleted doc", change{"docs/DSL.md", ""}, nil},
		{"the root README", change{"README.md", "# OrkStack, revised\n"}, nil},
		{"the generated defaults", change{"deploy/orkstack.defaults.yaml", "engine:\n  orkflow_max_tasks: 5000\n"}, nil},
		{"a proto file", change{"proto/orkstack/v1/run.proto", "syntax = \"proto3\";\n// revised\n"}, nil},
		{"an example", change{"examples/hello.ork.yaml", "name: hello\nversion: 2\n"}, nil},
		{"the tokens", change{"ui/src/tokens.css", ":root { --color-bg: #eee; }\n"}, []string{"admin.spec.ts"}},
		{"the icons", change{"ui/src/lib/icons.ts", "export const ICONS = { x: 1 };\n"}, []string{"admin.spec.ts"}},
		{"the UI's dependencies", change{"ui/package.json", "{\"name\": \"ui\", \"private\": true}\n"}, nil},
		{"the site", change{"site/package.json", "{\"name\": \"orkstack-site\", \"private\": true}\n"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			sel := fakeSelect(t, []change{c.change}, nil, c.specs)
			requireErrors(t, sel)
			if !sel.Gates.Site {
				t.Errorf("%s did not run the site gate (paths %+v)", c.change.path, sel.Paths)
			}
			if len(sel.UnitEvidence) != 0 {
				t.Errorf("%s asked for Go unit evidence: %v", c.change.path, unitNames(sel))
			}
		})
	}
}

// What the site does not read does not run its gate: a Go change, a UI page,
// the Playwright harness, an activity image, a test tool.
func TestSelect_NoSiteGateWhenNothingTheSiteReadsChanged(t *testing.T) {
	for _, c := range []struct {
		name    string
		changes []change
		tests   []string
		specs   []string
	}{
		{"a Go change", []change{engineChange}, []string{"internal/engine:TestIntegration_Uses"}, nil},
		{"a plain Go change", []change{{"internal/dsl/parse.go", "package dsl\n\nfunc Parse() int { return 2 }\n"}}, []string{"./internal/dsl:TestParse"}, nil},
		{"a UI library file", []change{{"ui/src/lib/flakes.ts", "export const x = 2;\n"}, {"ui/test/flakes.test.ts", "test('flakes', () => { x; });\n"}}, nil, nil},
		{"a UI page", []change{{"ui/src/routes/page.svelte", "<p>bye</p>\n"}}, nil, []string{"admin.spec.ts"}},
		{"the Playwright harness", []change{{"ui/e2e/harness.ts", "export {};\n"}}, nil, nil},
		{"an activity image", []change{{"images/activity-shell/entry.sh", "#!/bin/sh\nexit 0\n"}}, nil, nil},
		{"a test tool", []change{{"test/tools/cosign-sign/main.go", "package main\n\nfunc main() { println() }\n"}}, nil, nil},
		{"no change at all", nil, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			sel := fakeSelect(t, c.changes, c.tests, c.specs)
			requireErrors(t, sel)
			if sel.Gates.Site {
				t.Errorf("the site gate ran for %+v", sel.Paths)
			}
			for _, e := range sel.Evidence {
				if strings.HasPrefix(e.Target, "site") {
					t.Errorf("the README would name the site gate: %+v", e)
				}
			}
		})
	}
}

// A change to many paths does not make the README's evidence line unreadable:
// it names the first few, and "Changed paths" above it lists them all.
func TestSelect_ALongSiteEvidenceLineIsCut(t *testing.T) {
	var changes []change
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		changes = append(changes, change{"docs/" + n + ".md", "# " + n + "\n"})
	}
	sel := fakeSelect(t, changes, nil, nil)
	requireErrors(t, sel)
	var target string
	for _, e := range sel.Evidence {
		if strings.HasPrefix(e.Target, "site") {
			target = e.Target
		}
	}
	want := "site (docs/a.md, docs/b.md, docs/c.md, docs/d.md, docs/e.md, docs/f.md, docs/g.md, docs/h.md, and 2 more)"
	if target != want {
		t.Errorf("target = %q, want %q", target, want)
	}
	if len(sel.Paths) != 10 {
		t.Errorf("the selection lists %d changed paths, want all 10", len(sel.Paths))
	}
}

// The feature README says which gate ran for which paths.
func TestSelect_TheFeatureReportNamesTheSiteGateForItsPaths(t *testing.T) {
	sel := fakeSelect(t, []change{{"docs/DSL.md", "# DSL, revised\n"}, {"site/src/pages/index.astro", "<h1>OrkStack, revised</h1>\n"}}, nil, nil)
	requireErrors(t, sel)
	var b strings.Builder
	featureReport(&b, sel, outcomes{}, nil)
	got := b.String()
	for _, want := range []string{
		"- `M` docs/DSL.md — rule `docs/**` (scripts/proof-scope.tsv:",
		"→ none, site",
		"- **site (docs/DSL.md, site/src/pages/index.astro)**:\n  - the site gate — see the gates\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the README lacks %q:\n%s", want, got)
		}
	}
	var term strings.Builder
	printSelection(&term, sel)
	if !strings.Contains(term.String(), "M   docs/DSL.md -> none,site") {
		t.Errorf("the terminal selection does not show the site action:\n%s", term.String())
	}
}

func TestSelect_UserFacingChangeWithoutDocsWarns(t *testing.T) {
	paths := []pathScope{{Path: "cmd/orkctl/root.go", Docs: true}}
	warnings := docsWarning(paths)
	if len(warnings) != 2 || !strings.Contains(warnings[0], "no documentation change") ||
		!strings.Contains(warnings[0], "cmd/orkctl/root.go") || !strings.Contains(warnings[1], "CHANGELOG") {
		t.Fatalf("warnings = %v", warnings)
	}
	withDocs := append(paths, pathScope{Path: "docs/CLI.md"}, pathScope{Path: "CHANGELOG.md"})
	if got := docsWarning(withDocs); len(got) != 0 {
		t.Errorf("a change with docs and a CHANGELOG entry warned: %v", got)
	}
	if got := docsWarning([]pathScope{{Path: "cmd/orkctl/root_test.go", Docs: true}}); len(got) != 0 {
		t.Errorf("a test-only change warned: %v", got)
	}
}

// The warning names every user-facing path that changed without a doc
// change, so a reviewer sees which ones (plan §8.4), and none that did not.
func TestSelect_TheDocsWarningListsEveryUndocumentedPath(t *testing.T) {
	paths := []pathScope{
		{Path: "internal/dsl/parse.go", Docs: true},
		{Path: "cmd/orkctl/root.go", Docs: true},
		{Path: "cmd/orkctl/root_test.go", Docs: true},
		{Path: "internal/engine/run.go"},
	}
	warnings := docsWarning(paths)
	if len(warnings) == 0 || !strings.HasSuffix(warnings[0], ": cmd/orkctl/root.go, internal/dsl/parse.go") {
		t.Fatalf("warnings = %v, want the two user-facing paths, sorted", warnings)
	}
	for _, not := range []string{"root_test.go", "internal/engine/run.go"} {
		if strings.Contains(warnings[0], not) {
			t.Errorf("the warning names %s, which needs no doc: %q", not, warnings[0])
		}
	}
	var b strings.Builder
	featureReport(&b, &selection{DocsWarning: warnings}, outcomes{}, nil)
	if !strings.Contains(b.String(), "WARNING: user-facing source files changed with no documentation change") ||
		!strings.Contains(b.String(), "cmd/orkctl/root.go, internal/dsl/parse.go") {
		t.Errorf("the README does not list the paths:\n%s", b.String())
	}
}

func TestSelect_SlowListTestsAreSlowOmittedNotDeferred(t *testing.T) {
	sel := fakeSelect(t, []change{engineChange}, []string{"internal/engine:TestIntegration_Uses"}, nil)
	sel.SlowSource = "proof/x (commit abc)"
	if d := sel.Integration[pEngine].SlowOmitted; len(d) != 1 || d[0].Test != "TestIntegration_Slow" {
		t.Fatalf("slow_omitted = %+v", d)
	}
	b, err := json.Marshal(sel.Integration[pEngine])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"slow_omitted":[{"test":"TestIntegration_Slow"`) || strings.Contains(string(b), "deferred") {
		t.Errorf("the selection JSON does not carry slow_omitted alone: %s", b)
	}
	var r strings.Builder
	featureReport(&r, sel, outcomes{}, nil)
	got := r.String()
	if !strings.Contains(got, "left out: on the slow list (120s in proof/x (commit abc)): TestIntegration_Slow") {
		t.Errorf("the README lacks the slow-list wording:\n%s", got)
	}
	// "deferred" means only a nightly-listed test the full tier did not run.
	if strings.Contains(got, "deferred") {
		t.Errorf("the feature README says deferred:\n%s", got)
	}
}

func TestFeatureReport_PrintsTheDocsWarning(t *testing.T) {
	var b strings.Builder
	featureReport(&b, &selection{DocsWarning: []string{"no CHANGELOG.md [Unreleased] entry"}}, outcomes{}, nil)
	got := b.String()
	if !strings.HasPrefix(got, "## Documentation") || !strings.Contains(got, "WARNING: no CHANGELOG.md") {
		t.Fatalf("report = %q", got)
	}
}
