package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// `proofrun select` decides what a feature proof runs (AGENTS.md §11.2):
// the change's evidence — the tests and specs that prove it — and a
// regression set that is never evidence. It refuses, with exit 2, a change
// it cannot account for: a path the scope table does not map or maps to the
// full proof, a changed package with no evidence test, a --tests pattern
// that matches nothing, a --specs file that does not exist.

// selection is the plan a feature proof runs, and the record its README is
// written from.
type selection struct {
	Tier       string `json:"tier"`
	BaseRef    string `json:"base_ref"`
	BaseCommit string `json:"base_commit"`
	MergeBase  string `json:"merge_base"`
	Head       string `json:"head"`
	Inputs     struct {
		Tests []string `json:"tests"`
		Specs []string `json:"specs"`
	} `json:"inputs"`
	Paths []pathScope `json:"paths"`
	Gates struct {
		UICheck       bool `json:"ui_check"`
		UIUnit        bool `json:"ui_unit"`
		Fixture       bool `json:"activity_fixture"`
		Observability bool `json:"observability"`
	} `json:"gates"`
	Integration  map[string]*pkgSelection `json:"integration"`
	UnitEvidence []selectedTest           `json:"unit_evidence"`
	Specs        []selectedSpec           `json:"specs"`
	AllSpecs     string                   `json:"all_specs,omitempty"`
	UIUnitTests  []string                 `json:"ui_unit_evidence"`
	Evidence     []evidenceFor            `json:"evidence"`
	Removed      []string                 `json:"removed_tests"`
	NotRun       []string                 `json:"integration_packages_not_run"`
	Notes        []string                 `json:"notes"`
	Errors       []string                 `json:"errors"`
	SlowSource   string                   `json:"slow_list_source"`
}

// pathScope is how one changed path was classified.
type pathScope struct {
	Path    string   `json:"path"`
	Status  string   `json:"status"`
	Rule    string   `json:"rule"`
	Line    int      `json:"rule_line"`
	Actions []string `json:"actions"`
}

// pkgSelection is what runs of one integration package.
type pkgSelection struct {
	Whole      string            `json:"whole,omitempty"`
	Evidence   map[string]string `json:"evidence"`
	Regression []string          `json:"regression"`
	Deferred   []deferredTest    `json:"deferred_slow"`
	Declared   int               `json:"declared_integration_tests"`
	Why        string            `json:"why"`
}

type deferredTest struct {
	Test    string  `json:"test"`
	Seconds float64 `json:"seconds"`
}

// run lists the tests to run; nil with Whole set means every listed test.
func (p *pkgSelection) run() []string {
	if p == nil || p.Whole != "" {
		return nil
	}
	set := map[string]bool{}
	for t := range p.Evidence {
		set[t] = true
	}
	for _, t := range p.Regression {
		set[t] = true
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

type selectedTest struct {
	Package string `json:"package"`
	Test    string `json:"test"`
	Why     string `json:"why"`
}

type selectedSpec struct {
	File string `json:"file"`
	Why  string `json:"why"`
}

// evidenceFor is what proves one changed package or path.
type evidenceFor struct {
	Target   string   `json:"target"`
	Evidence []string `json:"evidence"`
}

// gitOut runs git and returns stdout.
func gitOut(args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// changedPath is one path of the diff.
type changedPath struct {
	Path   string
	Status string // A, M, D, ?? (untracked)
}

// diffPaths lists every path that differs between base and the working tree,
// staged and untracked (not ignored) files included.
func diffPaths(base string) ([]changedPath, error) {
	out, err := gitOut("diff", "--no-renames", "--name-status", "-z", base, "--")
	if err != nil {
		return nil, err
	}
	var paths []changedPath
	f := strings.Split(out, "\x00")
	for i := 0; i+1 < len(f); i += 2 {
		if f[i] == "" {
			break
		}
		paths = append(paths, changedPath{Path: f[i+1], Status: f[i][:1]})
	}
	untracked, err := gitOut("ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	for _, p := range strings.Split(untracked, "\x00") {
		if p != "" {
			paths = append(paths, changedPath{Path: p, Status: "??"})
		}
	}
	sort.Slice(paths, func(i, j int) bool { return paths[i].Path < paths[j].Path })
	return paths, nil
}

// gitShow returns a file's content at rev, or nil when it did not exist.
func gitShow(rev, p string) []byte {
	cmd := exec.Command("git", "show", rev+":"+p)
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	return out
}

// importPathOf is a repository-relative directory's import path.
func importPathOf(dir string) string {
	if dir == "." || dir == "" {
		return modulePath
	}
	return modulePath + "/" + dir
}

func relDir(importPath string) string {
	if importPath == modulePath {
		return "."
	}
	return strings.TrimPrefix(importPath, modulePath+"/")
}

// selectOptions are the inputs of a selection.
type selectOptions struct {
	Base      string
	Tests     []string
	Specs     []string
	ScopeFile string
	SlowFile  string
}

// multiFlag collects a repeatable flag.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, " ") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdSelect(args []string) int {
	fs := flag.NewFlagSet("select", flag.ContinueOnError)
	var o selectOptions
	var tests, specs multiFlag
	fs.StringVar(&o.Base, "base", "main", "the ref the change is measured from (its merge base with HEAD)")
	fs.Var(&tests, "tests", "PKG:REGEX naming evidence tests (repeatable)")
	fs.Var(&specs, "specs", "a Playwright spec or UI unit test file naming evidence (repeatable)")
	fs.StringVar(&o.ScopeFile, "scope", "scripts/proof-scope.tsv", "the scope table")
	fs.StringVar(&o.SlowFile, "slow", "scripts/proof-slow-tests.txt", "the slow list")
	outFile := fs.String("out", "", "write the selection JSON here")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	o.Tests, o.Specs = tests, specs
	sel, err := computeSelection(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun select:", err)
		return 2
	}
	if *outFile != "" {
		if err := writeJSON(*outFile, sel); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun select:", err)
			return 2
		}
	}
	printSelection(os.Stdout, sel)
	if len(sel.Errors) > 0 {
		fmt.Fprintln(os.Stderr, "proofrun select: this is not a valid feature proof:")
		for _, e := range sel.Errors {
			fmt.Fprintln(os.Stderr, "  -", e)
		}
		return 2
	}
	return 0
}

// computeSelection runs against the repository in the working directory.
func computeSelection(o selectOptions) (*selection, error) {
	sel := &selection{Tier: "feature", BaseRef: o.Base, Integration: map[string]*pkgSelection{},
		Errors: []string{}, Notes: []string{}, Removed: []string{}, UnitEvidence: []selectedTest{}, Specs: []selectedSpec{}}
	sel.Inputs.Tests = append([]string{}, o.Tests...)
	sel.Inputs.Specs = append([]string{}, o.Specs...)
	if sel.Inputs.Tests == nil {
		sel.Inputs.Tests = []string{}
	}
	if sel.Inputs.Specs == nil {
		sel.Inputs.Specs = []string{}
	}

	rootOut, err := gitOut("rev-parse", "--show-toplevel")
	if err != nil {
		return nil, err
	}
	root := strings.TrimSpace(rootOut)
	if bc, err := gitOut("rev-parse", "--verify", "--quiet", o.Base+"^{commit}"); err != nil {
		return nil, fmt.Errorf("base %q is not a commit: %v", o.Base, err)
	} else {
		sel.BaseCommit = strings.TrimSpace(bc)
	}
	head, err := gitOut("rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	sel.Head = strings.TrimSpace(head)
	mb, err := gitOut("merge-base", sel.BaseCommit, "HEAD")
	if err != nil {
		return nil, fmt.Errorf("no merge base between %s and HEAD: %v", o.Base, err)
	}
	sel.MergeBase = strings.TrimSpace(mb)

	scope, err := readScope(o.ScopeFile)
	if err != nil {
		return nil, err
	}
	slow, slowSource, err := readSlowList(o.SlowFile)
	if err != nil {
		return nil, err
	}
	sel.SlowSource = slowSource
	paths, err := diffPaths(sel.MergeBase)
	if err != nil {
		return nil, err
	}
	in := selectInput{
		root: root, mergeBase: sel.MergeBase, paths: paths, scope: scope, slow: slow,
		tests: o.Tests, specs: o.Specs,
		readNew: func(p string) []byte {
			b, err := os.ReadFile(filepath.Join(root, p))
			if err != nil {
				return nil
			}
			return b
		},
		readOld: func(p string) []byte { return gitShow(sel.MergeBase, p) },
		listOld: func(dir string) []string {
			out, err := gitOut("ls-tree", "--name-only", sel.MergeBase, "--", dir+"/")
			if err != nil {
				return nil
			}
			var names []string
			for _, l := range strings.Split(out, "\n") {
				if l != "" {
					names = append(names, path.Base(l))
				}
			}
			return names
		},
	}
	in.deps = func() (depIndex, []string, error) { return loadDeps(root) }
	in.integrationPkgs = func() ([]pkgInfo, error) { return integrationPackages([]string{"./..."}) }
	in.allPkgs = func() ([]pkgInfo, error) {
		lines, err := goList("integration", `{{.ImportPath}}{{"\t"}}{{.Dir}}`, "./...")
		if err != nil {
			return nil, err
		}
		var out []pkgInfo
		for _, l := range lines {
			f := strings.SplitN(l, "\t", 2)
			if len(f) == 2 {
				out = append(out, pkgInfo{f[0], f[1]})
			}
		}
		return out, nil
	}
	if err := selectInto(sel, in); err != nil {
		return nil, err
	}
	return sel, nil
}

// selectInput is everything a selection reads, so tests can supply it.
type selectInput struct {
	root      string
	mergeBase string
	paths     []changedPath
	scope     scopeTable
	slow      map[testKey]slowEntry
	tests     []string
	specs     []string
	readNew   func(p string) []byte
	readOld   func(p string) []byte
	listOld   func(dir string) []string
	deps      func() (depIndex, []string, error)
	// integrationPkgs lists the packages with an integration suite.
	integrationPkgs func() ([]pkgInfo, error)
	// allPkgs lists every package of the module, with its directory.
	allPkgs func() ([]pkgInfo, error)
}

// newDirFiles reads every .go file of a repository-relative directory on the
// new side.
func (in selectInput) newDirFiles(dir string) []srcFile {
	files, _ := readDirGo(filepath.Join(in.root, dir))
	return files
}

// selectInto fills sel. It is the whole of the selection logic.
func selectInto(sel *selection, in selectInput) error {
	type dirWork struct {
		old, new map[string]srcFile
		nonGo    []string
	}
	work := map[string]*dirWork{}
	dirOf := func(dir string) *dirWork {
		w := work[dir]
		if w == nil {
			w = &dirWork{old: map[string]srcFile{}, new: map[string]srcFile{}}
			work[dir] = w
		}
		return w
	}
	virtualChanged := map[string]string{} // import path -> why
	var uiSource []string
	examplesChanged := []string{}
	toolsChanged := map[string]bool{}

	for _, cp := range in.paths {
		rule := in.scope.match(cp.Path)
		if rule == nil {
			sel.Errors = append(sel.Errors, fmt.Sprintf("%s: unmapped path; add a rule to scripts/proof-scope.tsv or run make proof", cp.Path))
			sel.Paths = append(sel.Paths, pathScope{Path: cp.Path, Status: cp.Status, Rule: "(none)"})
			continue
		}
		sel.Paths = append(sel.Paths, pathScope{Path: cp.Path, Status: cp.Status, Rule: rule.Pattern, Line: rule.Line, Actions: rule.Actions})
		if strings.HasPrefix(cp.Path, "ui/") || strings.HasPrefix(cp.Path, "proto/") {
			sel.Gates.UICheck = true
			sel.Gates.UIUnit = true
		}
		for _, a := range rule.Actions {
			switch a {
			case actFull:
				sel.Errors = append(sel.Errors, fmt.Sprintf("%s: this path changes how every suite runs; run make proof", cp.Path))
			case actGo:
				if strings.HasSuffix(cp.Path, ".go") {
					dir := path.Dir(cp.Path)
					w := dirOf(dir)
					name := path.Base(cp.Path)
					if b := in.readOld(cp.Path); b != nil {
						w.old[name] = srcFile{Name: name, Content: b}
					}
					if b := in.readNew(cp.Path); b != nil {
						w.new[name] = srcFile{Name: name, Content: b}
					}
				} else {
					dir := in.goPackageDir(path.Dir(cp.Path))
					if dir == "" {
						sel.Notes = append(sel.Notes, fmt.Sprintf("%s belongs to no Go package; nothing to prove for it", cp.Path))
						continue
					}
					dirOf(dir).nonGo = append(dirOf(dir).nonGo, cp.Path)
				}
			case actUI:
				if cp.Status != "D" {
					uiSource = append(uiSource, cp.Path)
				}
			case actSpec:
				if cp.Status == "D" {
					sel.Removed = append(sel.Removed, cp.Path+" (spec)")
				} else {
					sel.Specs = append(sel.Specs, selectedSpec{File: cp.Path, Why: "changed"})
				}
			case actUIUnit:
				if cp.Status != "D" {
					sel.UIUnitTests = append(sel.UIUnitTests, cp.Path)
				}
			case actAllSpecs:
				if sel.AllSpecs == "" {
					sel.AllSpecs = cp.Path + " changed (the Playwright harness)"
				}
			case actProto:
			case actImages:
				sel.Gates.Fixture = true
				virtualChanged[modulePath+"/internal/orker"] = cp.Path + " (activity images run on the Orker)"
			case actExamples:
				examplesChanged = append(examplesChanged, cp.Path)
			case actTool:
				parts := strings.SplitN(cp.Path, "/", 4)
				if len(parts) >= 3 {
					toolsChanged[parts[2]] = true
				}
			case actObservability:
				sel.Gates.Observability = true
			case actNone:
			}
		}
	}

	// ---- Go: what the diff did to each package --------------------------
	type pkgChange struct {
		dir        string
		production []string
		changed    bool
	}
	var changedPkgs []pkgChange
	dirs := make([]string, 0, len(work))
	for d := range work {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	declaredCache := map[string][2]map[string]bool{}
	declared := func(dir string) (map[string]bool, map[string]bool) {
		if c, ok := declaredCache[dir]; ok {
			return c[0], c[1]
		}
		u, i := testsDeclared(dir, in.newDirFiles(dir))
		declaredCache[dir] = [2]map[string]bool{u, i}
		return u, i
	}
	integEvidence := map[string]map[string]string{} // import path -> test -> why
	addInteg := func(pkg, test, why string) {
		if integEvidence[pkg] == nil {
			integEvidence[pkg] = map[string]string{}
		}
		if _, ok := integEvidence[pkg][test]; !ok {
			integEvidence[pkg][test] = why
		}
	}
	unitSeen := map[testKey]bool{}
	addUnit := func(pkg, test, why string) {
		k := testKey{pkg, test}
		if !unitSeen[k] {
			unitSeen[k] = true
			sel.UnitEvidence = append(sel.UnitEvidence, selectedTest{Package: pkg, Test: test, Why: why})
		}
	}
	wholeIntegration := map[string]string{}
	for _, dir := range dirs {
		w := work[dir]
		pkg := importPathOf(dir)
		pc := pkgChange{dir: dir}
		for _, p := range w.nonGo {
			pc.production = append(pc.production, path.Base(p))
		}
		newAll := in.newDirFiles(dir)
		unitDecl, integDecl := declared(dir)
		for _, cfg := range configs {
			ch := analyzeDir(cfg, dir, w.old, w.new, newAll)
			if ch.Production {
				pc.production = append(pc.production, ch.ProductionWhy...)
			}
			if ch.Production || ch.TestFilesChanged || ch.Whole != "" || len(ch.Tests) > 0 || len(ch.Removed) > 0 {
				pc.changed = true
			}
			for _, r := range ch.Removed {
				sel.Removed = append(sel.Removed, pkg+"."+r)
			}
			if ch.Whole != "" {
				if cfg == cfgIntegration && len(integDecl) > 0 {
					wholeIntegration[pkg] = ch.Whole
					sel.Notes = append(sel.Notes, fmt.Sprintf("%s: %s, so the whole integration package runs, slow tests included; a change to TestMain is better proven by make proof", pkg, ch.Whole))
				}
			}
			for t, why := range ch.Tests {
				switch {
				case unitDecl[t]:
					addUnit(pkg, t, why)
				case integDecl[t]:
					addInteg(pkg, t, why)
				}
			}
		}
		if len(pc.production) > 0 {
			pc.changed = true
		}
		if pc.changed {
			changedPkgs = append(changedPkgs, pc)
		}
	}
	sel.Removed = dedupe(sel.Removed)

	// ---- named tests -----------------------------------------------------
	for _, spec := range in.tests {
		i := strings.LastIndex(spec, ":")
		if i <= 0 || i == len(spec)-1 {
			sel.Errors = append(sel.Errors, fmt.Sprintf("--tests %q: want PKG:REGEX", spec))
			continue
		}
		pkgArg, pattern := spec[:i], spec[i+1:]
		dir := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(pkgArg, modulePath+"/"), "./"), "/")
		re, err := regexp.Compile("^(?:" + pattern + ")$")
		if err != nil {
			sel.Errors = append(sel.Errors, fmt.Sprintf("--tests %q: %v", spec, err))
			continue
		}
		unitDecl, integDecl := declared(dir)
		pkg := importPathOf(dir)
		var matched []string
		for _, set := range []map[string]bool{unitDecl, integDecl} {
			for t := range set {
				if re.MatchString(t) {
					matched = append(matched, t)
				}
			}
		}
		sort.Strings(matched)
		if len(matched) == 0 {
			sel.Errors = append(sel.Errors, fmt.Sprintf("--tests %q matches no test declared in %s", spec, dir))
			continue
		}
		for _, t := range matched {
			if integDecl[t] {
				addInteg(pkg, t, "named by --tests "+spec)
			} else {
				addUnit(pkg, t, "named by --tests "+spec)
			}
		}
	}

	// ---- named specs -----------------------------------------------------
	for _, s := range in.specs {
		p := strings.TrimPrefix(path.Clean(s), "./")
		candidates := []string{p, "ui/" + p, "ui/e2e/" + p, "ui/test/" + p}
		found := ""
		for _, c := range candidates {
			if st, err := os.Stat(filepath.Join(in.root, c)); err == nil && !st.IsDir() {
				found = c
				break
			}
		}
		switch {
		case found == "":
			sel.Errors = append(sel.Errors, fmt.Sprintf("--specs %q: no such file", s))
		case strings.HasPrefix(found, "ui/e2e/") && strings.HasSuffix(found, ".spec.ts"):
			sel.Specs = append(sel.Specs, selectedSpec{File: found, Why: "named by --specs"})
		case strings.HasPrefix(found, "ui/test/"):
			sel.UIUnitTests = append(sel.UIUnitTests, found)
			sel.Gates.UIUnit = true
			sel.Gates.UICheck = true
		default:
			sel.Errors = append(sel.Errors, fmt.Sprintf("--specs %q: %s is neither a Playwright spec (ui/e2e/*.spec.ts) nor a UI unit test (ui/test/)", s, found))
		}
	}
	if len(sel.Specs) > 0 {
		sel.Gates.UICheck = true
		sel.Gates.UIUnit = true
	}

	// ---- automatic rules: examples and tools, by search ------------------
	needDeps := len(changedPkgs) > 0 || len(virtualChanged) > 0 || len(examplesChanged) > 0 || len(toolsChanged) > 0
	var allPkgs []pkgInfo
	if len(examplesChanged) > 0 || len(toolsChanged) > 0 {
		var err error
		if allPkgs, err = in.allPkgs(); err != nil {
			return err
		}
	}
	searchTests := func(label string, needle func(string) bool) int {
		n := 0
		for _, p := range allPkgs {
			files, _ := readDirGo(p.Dir)
			hits := testsReferencing(p.Dir, files, needle)
			if len(hits) == 0 {
				continue
			}
			unitDecl, integDecl := testsDeclared(p.Dir, files)
			for t, why := range hits {
				w := label + ": " + why
				switch {
				case integDecl[t]:
					addInteg(p.ImportPath, t, w)
					n++
				case unitDecl[t]:
					addUnit(p.ImportPath, t, w)
					n++
				}
			}
		}
		return n
	}
	searchSpecs := func(label string, needle func(string) bool) int {
		matches, _ := filepath.Glob(filepath.Join(in.root, "ui", "e2e", "*.spec.ts"))
		sort.Strings(matches)
		n := 0
		for _, m := range matches {
			b, err := os.ReadFile(m)
			if err != nil {
				continue
			}
			if needle(string(b)) {
				rel, _ := filepath.Rel(in.root, m)
				sel.Specs = append(sel.Specs, selectedSpec{File: filepath.ToSlash(rel), Why: label})
				n++
			}
		}
		return n
	}
	for _, p := range examplesChanged {
		base := path.Base(p)
		label := p + " changed"
		searchTests(label, func(s string) bool {
			return s == "examples" || s == "examples/" || strings.Contains(s, p) || (base != "" && strings.Contains(s, base))
		})
	}
	if len(examplesChanged) > 0 {
		gallery := "ui/e2e/examples-gallery.spec.ts"
		if _, err := os.Stat(filepath.Join(in.root, gallery)); err == nil {
			sel.Specs = append(sel.Specs, selectedSpec{File: gallery, Why: "examples/ changed"})
		}
		sel.Gates.UICheck, sel.Gates.UIUnit = true, true
	}
	tools := make([]string, 0, len(toolsChanged))
	for t := range toolsChanged {
		tools = append(tools, t)
	}
	sort.Strings(tools)
	for _, tool := range tools {
		needle := "test/tools/" + tool
		label := needle + " changed"
		n := searchTests(label, func(s string) bool { return strings.Contains(s, needle) })
		n += searchSpecs(label, func(s string) bool { return strings.Contains(s, needle) })
		if n == 0 {
			sel.Errors = append(sel.Errors, fmt.Sprintf("%s changed, and no test or spec runs it: nothing can prove the change", needle))
		}
	}
	sel.Specs = dedupeSpecs(sel.Specs)
	if len(sel.Specs) > 0 {
		sel.Gates.UICheck, sel.Gates.UIUnit = true, true
	}

	// ---- regression set and evidence requirement -------------------------
	var deps depIndex
	var integPkgs []pkgInfo
	if needDeps || len(integEvidence) > 0 {
		var unresolved []string
		var err error
		deps, unresolved, err = in.deps()
		if err != nil {
			return err
		}
		for _, u := range unresolved {
			sel.Errors = append(sel.Errors, "cannot resolve a binary a test builds: "+u)
		}
		if integPkgs, err = in.integrationPkgs(); err != nil {
			return err
		}
	}
	changedSet := map[string]string{}
	for _, pc := range changedPkgs {
		changedSet[importPathOf(pc.dir)] = "changed"
	}
	for p, why := range virtualChanged {
		changedSet[p] = why
	}
	changedList := make([]string, 0, len(changedSet))
	for p := range changedSet {
		changedList = append(changedList, p)
	}
	sort.Strings(changedList)

	for _, ip := range integPkgs {
		dir := relDir(ip.ImportPath)
		_, integDecl := declared(dir)
		ps := &pkgSelection{Evidence: map[string]string{}, Regression: []string{}, Deferred: []deferredTest{}, Declared: len(integDecl)}
		for t, why := range integEvidence[ip.ImportPath] {
			ps.Evidence[t] = why
		}
		if w, ok := wholeIntegration[ip.ImportPath]; ok {
			ps.Whole = w
		}
		var because []string
		for _, c := range changedList {
			if ok, how := deps.dependsOn(cfgIntegration, ip.ImportPath, c); ok {
				because = append(because, fmt.Sprintf("%s (%s)", relDir(c), how))
			}
		}
		if len(because) > 0 {
			ps.Why = "its test binary depends on " + strings.Join(because, ", ")
			names := make([]string, 0, len(integDecl))
			for t := range integDecl {
				names = append(names, t)
			}
			sort.Strings(names)
			for _, t := range names {
				if _, ev := ps.Evidence[t]; ev {
					continue
				}
				if s, slow := in.slow[testKey{ip.ImportPath, t}]; slow && ps.Whole == "" {
					ps.Deferred = append(ps.Deferred, deferredTest{Test: t, Seconds: s.Seconds})
					continue
				}
				ps.Regression = append(ps.Regression, t)
			}
		}
		if len(ps.Evidence) == 0 && len(ps.Regression) == 0 && ps.Whole == "" {
			sel.NotRun = append(sel.NotRun, fmt.Sprintf("%s: its test binary depends on no changed package", relDir(ip.ImportPath)))
			continue
		}
		if ps.Why == "" {
			ps.Why = "it holds evidence tests"
		}
		sel.Integration[ip.ImportPath] = ps
		// A slow-list entry naming a test that is no longer declared is stale.
		unitDecl, _ := declared(dir)
		for k := range in.slow {
			if k.Package == ip.ImportPath && !integDecl[k.Test] && !unitDecl[k.Test] {
				sel.Notes = append(sel.Notes, fmt.Sprintf("the slow list names %s.%s, which is no longer declared: regenerate it with make proof-slow-list", relDir(k.Package), k.Test))
			}
		}
	}
	// Evidence named in a package that is not an integration package (or
	// the package listing failed to find) must still run.
	for p, tests := range integEvidence {
		if _, ok := sel.Integration[p]; ok {
			continue
		}
		ps := &pkgSelection{Evidence: map[string]string{}, Regression: []string{}, Deferred: []deferredTest{}, Why: "it holds evidence tests"}
		for t, why := range tests {
			ps.Evidence[t] = why
		}
		sel.Integration[p] = ps
	}
	sort.Strings(sel.NotRun)

	// Evidence per changed package.
	evidenceIn := func(cfg string, target string) (all []string, integ int) {
		if cfg == cfgUnit {
			for _, u := range sel.UnitEvidence {
				if ok, _ := deps.dependsOn(cfgUnit, u.Package, target); ok {
					all = append(all, relDir(u.Package)+"."+u.Test)
				}
			}
			return all, 0
		}
		pkgs := make([]string, 0, len(sel.Integration))
		for p := range sel.Integration {
			pkgs = append(pkgs, p)
		}
		sort.Strings(pkgs)
		for _, p := range pkgs {
			ps := sel.Integration[p]
			ok, _ := deps.dependsOn(cfgIntegration, p, target)
			if !ok {
				continue
			}
			if ps.Whole != "" {
				all = append(all, relDir(p)+" (whole package: "+ps.Whole+")")
				integ++
			}
			names := make([]string, 0, len(ps.Evidence))
			for t := range ps.Evidence {
				names = append(names, t)
			}
			sort.Strings(names)
			for _, t := range names {
				all = append(all, relDir(p)+"."+t)
				integ++
			}
		}
		return all, integ
	}
	for _, pc := range changedPkgs {
		if len(pc.production) == 0 {
			continue
		}
		target := importPathOf(pc.dir)
		if len(in.newDirFiles(pc.dir)) == 0 {
			sel.Notes = append(sel.Notes, fmt.Sprintf("%s was deleted; nothing links it any more", pc.dir))
			continue
		}
		unitEv, _ := evidenceIn(cfgUnit, target)
		integEv, nInteg := evidenceIn(cfgIntegration, target)
		ev := append(append([]string{}, integEv...), unitEv...)
		sel.Evidence = append(sel.Evidence, evidenceFor{Target: pc.dir, Evidence: ev})
		temporal := in.scope.temporalFacing(pc.dir)
		switch {
		case len(ev) == 0:
			sel.Errors = append(sel.Errors, fmt.Sprintf("%s changed (%s) and no evidence test depends on it; name one with --tests PKG:REGEX%s",
				pc.dir, strings.Join(dedupe(pc.production), ", "), candidates(deps, integPkgs, target)))
		case temporal && nInteg == 0:
			sel.Errors = append(sel.Errors, fmt.Sprintf("%s is Temporal- or storage-facing and its evidence has no integration test; name one with --tests PKG:REGEX%s",
				pc.dir, candidates(deps, integPkgs, target)))
		}
	}
	// Evidence per non-Go path.
	if len(uiSource) > 0 {
		var ev []string
		for _, s := range sel.Specs {
			ev = append(ev, s.File)
		}
		if sel.AllSpecs != "" {
			ev = append(ev, "every spec ("+sel.AllSpecs+")")
		}
		libOnly := true
		for _, p := range uiSource {
			if !(strings.HasPrefix(p, "ui/src/lib/") && strings.HasSuffix(p, ".ts")) {
				libOnly = false
			}
		}
		if libOnly {
			ev = append(ev, sel.UIUnitTests...)
		}
		sel.Evidence = append(sel.Evidence, evidenceFor{Target: "ui/src (" + strings.Join(uiSource, ", ") + ")", Evidence: ev})
		if len(ev) == 0 {
			msg := "ui/src changed (" + strings.Join(uiSource, ", ") + ") and no spec proves it; name one with --specs ui/e2e/<spec>.spec.ts"
			if libOnly {
				msg += ", or a UI unit test with --specs ui/test/<file>"
			}
			sel.Errors = append(sel.Errors, msg)
		}
	}
	if sel.Gates.Fixture {
		sel.Evidence = append(sel.Evidence, evidenceFor{Target: "images/, test/fixtures/", Evidence: []string{"the activity contract fixture"}})
	}
	if sel.Gates.Observability {
		sel.Evidence = append(sel.Evidence, evidenceFor{Target: "telemetry", Evidence: []string{"the observability gate"}})
	}
	for _, tool := range tools {
		needle := "test/tools/" + tool
		var ev []string
		for p, ps := range sel.Integration {
			for t, why := range ps.Evidence {
				if strings.HasPrefix(why, needle+" changed") {
					ev = append(ev, relDir(p)+"."+t)
				}
			}
		}
		for _, u := range sel.UnitEvidence {
			if strings.HasPrefix(u.Why, needle+" changed") {
				ev = append(ev, relDir(u.Package)+"."+u.Test)
			}
		}
		for _, s := range sel.Specs {
			if strings.HasPrefix(s.Why, needle+" changed") {
				ev = append(ev, s.File)
			}
		}
		sort.Strings(ev)
		sel.Evidence = append(sel.Evidence, evidenceFor{Target: needle, Evidence: ev})
	}
	if len(examplesChanged) > 0 {
		var ev []string
		for p, ps := range sel.Integration {
			for t, why := range ps.Evidence {
				if strings.Contains(why, "examples/") {
					ev = append(ev, relDir(p)+"."+t)
				}
			}
		}
		for _, s := range sel.Specs {
			if s.Why == "examples/ changed" {
				ev = append(ev, s.File)
			}
		}
		sort.Strings(ev)
		sel.Evidence = append(sel.Evidence, evidenceFor{Target: "examples/", Evidence: ev})
	}

	// Go changes the UI suite would exercise run no spec here.
	var uiPkgs []string
	for _, pc := range changedPkgs {
		if len(pc.production) == 0 {
			continue
		}
		target := importPathOf(pc.dir)
		for _, b := range []string{modulePath + "/cmd/ork", modulePath + "/cmd/orker"} {
			if b == target || deps.binDepsOf(b)[target] {
				uiPkgs = append(uiPkgs, pc.dir)
				break
			}
		}
	}
	if len(uiPkgs) > 0 {
		sel.Notes = append(sel.Notes, fmt.Sprintf("no Playwright spec ran for the Go changes in %s; only the merge proof runs them", strings.Join(uiPkgs, ", ")))
	}
	if sel.UIUnitTests == nil {
		sel.UIUnitTests = []string{}
	}
	return nil
}

// binDepsOf returns what a main package links, loading it on demand.
func (d depIndex) binDepsOf(main string) map[string]bool {
	if s, ok := d.binDeps[main]; ok {
		return s
	}
	lines, err := goList("", `{{.ImportPath}}{{"\t"}}{{range .Deps}}{{.}}|{{end}}`, main)
	set := map[string]bool{}
	if err == nil {
		for _, l := range lines {
			f := strings.SplitN(l, "\t", 2)
			if len(f) == 2 {
				for _, dep := range strings.Split(f[1], "|") {
					if dep != "" {
						set[dep] = true
					}
				}
			}
		}
	}
	if d.binDeps != nil {
		d.binDeps[main] = set
	}
	return set
}

// candidates names the integration packages that could hold evidence.
func candidates(d depIndex, integ []pkgInfo, target string) string {
	var c []string
	for _, p := range integ {
		if ok, _ := d.dependsOn(cfgIntegration, p.ImportPath, target); ok {
			c = append(c, relDir(p.ImportPath))
		}
	}
	if len(c) == 0 {
		return ""
	}
	return " (integration packages that depend on it: " + strings.Join(c, ", ") + ")"
}

// goPackageDir is the nearest directory at or above dir that holds Go files.
func (in selectInput) goPackageDir(dir string) string {
	for d := dir; d != "." && d != "/" && d != ""; d = path.Dir(d) {
		if len(in.newDirFiles(d)) > 0 {
			return d
		}
		for _, n := range in.listOld(d) {
			if strings.HasSuffix(n, ".go") {
				return d
			}
		}
	}
	return ""
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func dedupeSpecs(in []selectedSpec) []selectedSpec {
	seen := map[string]bool{}
	out := []selectedSpec{}
	for _, s := range in {
		if !seen[s.File] {
			seen[s.File] = true
			out = append(out, s)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].File < out[j].File })
	return out
}
