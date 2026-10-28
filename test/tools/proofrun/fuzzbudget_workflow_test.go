package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// The fuzz lane's budget (docs/PROOFS.md §2.3) is what keeps the nightly's
// fuzz part inside its CI step however many Fuzz targets the module grows to
// have. Nothing here runs the lane or reads the clock: it reads the step's
// limit from proof.yml, the budget every place that sets one names, and the
// module's Fuzz targets from the source, so a change that lets the lane
// outgrow its step fails a unit test and not a nightly.

// fuzzLaneFacts are what the budget rule judges.
type fuzzLaneFacts struct {
	// StepMinutes is the fuzz step's timeout-minutes in proof.yml.
	StepMinutes int
	// Budgets are the lane budgets the repository sets, by where.
	Budgets map[string]string
	// Targets is the number of Fuzz targets of the module.
	Targets int
}

// fuzzBudgetProblems holds the lane to its step: every budget the repository
// sets is one value, that value and the lane's margin fit the step's limit,
// and the budget covers every target at its minimum fuzz time and the
// overhead the lane allows for one, so the allocation never has to leave a
// target out.
func fuzzBudgetProblems(f fuzzLaneFacts) []string {
	var p []string
	if f.StepMinutes <= 0 {
		return []string{"the fuzz step of proof.yml has no readable timeout-minutes"}
	}
	step := time.Duration(f.StepMinutes) * time.Minute
	var sources []string
	for src := range f.Budgets {
		sources = append(sources, src)
	}
	sort.Strings(sources)
	if len(sources) == 0 {
		return []string{"no fuzz budget is set anywhere"}
	}
	first := f.Budgets[sources[0]]
	var budget time.Duration
	for _, src := range sources {
		text := f.Budgets[src]
		d, err := time.ParseDuration(text)
		switch {
		case err != nil || d <= 0:
			p = append(p, fmt.Sprintf("%s sets the fuzz budget %q, which is not a positive duration", src, text))
			continue
		case text != first:
			p = append(p, fmt.Sprintf("%s sets the fuzz budget %q and %s sets %q: the repository names one budget", src, text, sources[0], first))
		}
		if d > budget {
			budget = d
		}
	}
	if budget == 0 {
		return p
	}
	if budget+fuzzLaneMargin > step {
		p = append(p, fmt.Sprintf("the fuzz budget %v and the lane's margin %v (the last target's build and minimization, the part's own work and the ledger write) exceed the fuzz step's %v: lower the budget, or raise the step's timeout and the job's with it", budget, fuzzLaneMargin, step))
	}
	per := fuzzMinTime + fuzzOverheadEstimate
	if need := time.Duration(f.Targets) * per; need > budget {
		p = append(p, fmt.Sprintf("%d Fuzz targets need %v at the minimum %v each plus the overhead %v the lane expects for one, and the budget is %v: the lane could not fuzz every target; raise the budget and the step's timeout, or the lane has outgrown its step", f.Targets, need, fuzzMinTime, fuzzOverheadEstimate, budget))
	}
	return p
}

// fuzzTargetsOfTheModule counts the module's Fuzz targets the way go test
// -list '^Fuzz' ./... does for a build without tags — and counts a file
// whatever its build constraint, so the count is never an underestimate.
// A target is a top-level function named Fuzz... in a _test.go file that takes
// one *testing.F.
func fuzzTargetsOfTheModule(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	skip := map[string]bool{".git": true, "node_modules": true, "testdata": true, "vendor": true, "proof": true, ".data": true, ".claude": true, "ui": true, "site": true}
	var targets []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (skip[d.Name()] || strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_")) {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil && path != root {
				return filepath.SkipDir // another module
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Fuzz") || !takesTestingF(fn) {
				continue
			}
			targets = append(targets, filepath.ToSlash(filepath.Dir(path))+"."+fn.Name.Name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(targets)
	return targets
}

// takesTestingF reports whether the function's one parameter is *testing.F.
func takesTestingF(fn *ast.FuncDecl) bool {
	params := fn.Type.Params
	if params == nil || len(params.List) != 1 || len(params.List[0].Names) > 1 {
		return false
	}
	star, ok := params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "testing" && sel.Sel.Name == "F"
}

var budgetFallback = regexp.MustCompile(`\|\|\s*'([^']+)'`)

// budgetOf is the fallback literal of a `${{ inputs.fuzz_budget || '105m' }}`
// expression, and whether the expression reads the input at all.
func budgetOf(expr string) (string, bool) {
	if !strings.Contains(expr, "inputs.fuzz_budget") {
		return "", false
	}
	m := budgetFallback.FindStringSubmatch(expr)
	if m == nil {
		return "", false
	}
	return m[1], true
}

type wfTop struct {
	On   map[string]any `yaml:"on"`
	Env  map[string]any `yaml:"env"`
	Jobs map[string]struct {
		With map[string]any `yaml:"with"`
	} `yaml:"jobs"`
}

func loadWfTop(t *testing.T, name string) wfTop {
	t.Helper()
	var w wfTop
	if err := yaml.Unmarshal([]byte(workflowFile(t, name)), &w); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return w
}

// inputDefault is the default of one input of one trigger.
func inputDefault(w wfTop, trigger, input string) (string, bool) {
	trig, _ := w.On[trigger].(map[string]any)
	inputs, _ := trig["inputs"].(map[string]any)
	in, _ := inputs[input].(map[string]any)
	v, ok := in["default"]
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// fuzzStepMinutes is the timeout-minutes of the step that runs the lane.
func fuzzStepMinutes(t *testing.T) int {
	t.Helper()
	w := loadWorkflow(t, "proof.yml")
	job := w.Jobs["fuzz"]
	if job == nil {
		t.Fatal("proof.yml has no fuzz job")
	}
	for _, s := range job.Steps {
		if strings.Contains(s.Run, "--part fuzz") {
			n, set, err := timeoutMinutes(s.Timeout)
			if err != nil || !set {
				t.Fatalf("the fuzz step's timeout-minutes: set %v, err %v", set, err)
			}
			return n
		}
	}
	t.Fatal("the fuzz job has no step that runs the fuzz part")
	return 0
}

var proofShBudget = regexp.MustCompile(`(?m)^FUZZ_BUDGET="\$\{FUZZ_BUDGET:-([^}]+)\}"`)

// fuzzBudgetsOfTheRepository are the lane budgets the workflows and the proof
// script set by default, keyed by where.
func fuzzBudgetsOfTheRepository(t *testing.T) map[string]string {
	t.Helper()
	budgets := map[string]string{}
	proof := loadWfTop(t, "proof.yml")
	if v, ok := budgetOf(fmt.Sprint(proof.Env["FUZZ_BUDGET"])); ok {
		budgets["proof.yml env FUZZ_BUDGET"] = v
	} else {
		t.Errorf("proof.yml env FUZZ_BUDGET = %v, want an expression reading inputs.fuzz_budget with a literal fallback", proof.Env["FUZZ_BUDGET"])
	}
	if v, ok := inputDefault(proof, "workflow_call", "fuzz_budget"); ok {
		budgets["proof.yml workflow_call input fuzz_budget"] = v
	} else {
		t.Error("proof.yml's workflow_call has no fuzz_budget input with a default")
	}
	nightly := loadWfTop(t, "nightly.yml")
	if v, ok := inputDefault(nightly, "workflow_dispatch", "fuzz_budget"); ok {
		budgets["nightly.yml workflow_dispatch input fuzz_budget"] = v
	} else {
		t.Error("nightly.yml's workflow_dispatch has no fuzz_budget input with a default")
	}
	if v, ok := budgetOf(fmt.Sprint(nightly.Jobs["nightly"].With["fuzz_budget"])); ok {
		budgets["nightly.yml jobs.nightly.with.fuzz_budget"] = v
	} else {
		t.Errorf("nightly.yml passes fuzz_budget = %v, want an expression reading inputs.fuzz_budget with a literal fallback", nightly.Jobs["nightly"].With["fuzz_budget"])
	}
	script, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if m := proofShBudget.FindSubmatch(script); m != nil {
		budgets["scripts/proof.sh FUZZ_BUDGET default"] = string(m[1])
	} else {
		t.Error(`scripts/proof.sh has no FUZZ_BUDGET="${FUZZ_BUDGET:-<budget>}" default`)
	}
	return budgets
}

func TestFuzzLane_TheBudgetAndTheModulesTargetsFitTheFuzzStep(t *testing.T) {
	targets := fuzzTargetsOfTheModule(t)
	if len(targets) < 80 {
		t.Fatalf("found %d Fuzz targets, want at least the 80 the module had when this rule was written: the count is wrong", len(targets))
	}
	facts := fuzzLaneFacts{StepMinutes: fuzzStepMinutes(t), Budgets: fuzzBudgetsOfTheRepository(t), Targets: len(targets)}
	expectNoProblems(t, fmt.Sprintf("%d Fuzz targets, budgets %v, fuzz step %d min", facts.Targets, facts.Budgets, facts.StepMinutes), fuzzBudgetProblems(facts))
}

func TestFuzzLane_TheBudgetRuleRefusesEachWeakening(t *testing.T) {
	good := func() fuzzLaneFacts {
		return fuzzLaneFacts{
			StepMinutes: 135,
			Budgets:     map[string]string{"a": "105m", "b": "105m"},
			Targets:     84,
		}
	}
	if p := fuzzBudgetProblems(good()); len(p) > 0 {
		t.Fatalf("the reference facts fail the rule: %v", p)
	}
	for _, c := range []struct {
		name   string
		mutate func(f *fuzzLaneFacts)
		want   string
	}{
		{"a budget that leaves no margin", func(f *fuzzLaneFacts) { f.Budgets = map[string]string{"a": "130m"} }, "exceed the fuzz step"},
		{"a budget longer than the step", func(f *fuzzLaneFacts) { f.Budgets = map[string]string{"a": "150m"} }, "exceed the fuzz step"},
		{"a step limit cut under the budget", func(f *fuzzLaneFacts) { f.StepMinutes = 110 }, "exceed the fuzz step"},
		{"targets grown past what the budget covers", func(f *fuzzLaneFacts) { f.Targets = 200 }, "could not fuzz every target"},
		{"a budget lowered under the targets' minimum", func(f *fuzzLaneFacts) {
			f.Budgets = map[string]string{"a": "60m"}
		}, "could not fuzz every target"},
		{"two places naming different budgets", func(f *fuzzLaneFacts) { f.Budgets["b"] = "90m" }, "the repository names one budget"},
		{"a budget that is not a duration", func(f *fuzzLaneFacts) { f.Budgets["b"] = "soon" }, "not a positive duration"},
		{"a zero budget", func(f *fuzzLaneFacts) { f.Budgets = map[string]string{"a": "0s"} }, "not a positive duration"},
		{"no budget anywhere", func(f *fuzzLaneFacts) { f.Budgets = nil }, "no fuzz budget"},
		{"an unreadable step limit", func(f *fuzzLaneFacts) { f.StepMinutes = 0 }, "no readable timeout-minutes"},
	} {
		f := good()
		c.mutate(&f)
		p := fuzzBudgetProblems(f)
		if len(p) == 0 || !strings.Contains(strings.Join(p, "\n"), c.want) {
			t.Errorf("%s: problems = %v, want one containing %q", c.name, p, c.want)
		}
	}
}

func TestFuzzLane_TheModulesFuzzTargetsAreCountedFromSource(t *testing.T) {
	targets := fuzzTargetsOfTheModule(t)
	joined := strings.Join(targets, "\n")
	// A target of a package and its sibling in another, and not the Fuzz
	// functions the proofrun tests hold in string literals.
	for _, want := range []string{"internal/listfilter.FuzzParse_AcceptedTextIsAFilterThatFormatsBackToItself", "cmd/orkctl.", "pkg/webhooks."} {
		if !strings.Contains(joined, want) {
			t.Errorf("the count lacks %q", want)
		}
	}
	for _, not := range []string{"FuzzOK", "FuzzCrash"} {
		if strings.Contains(joined, "."+not) {
			t.Errorf("the count holds %s, which is Go source inside a string literal", not)
		}
	}
}
