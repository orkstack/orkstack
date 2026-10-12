package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// A test binary depends on a package through its imports — what
// `go list -test -deps` reports — and through every binary its suite builds
// and runs: an orkctl integration test that starts `ork` exercises
// internal/engine although it never imports it. Those binary edges are
// derived mechanically from the test sources: every binary a test builds goes
// through testbin.Path with a literal package path, and a `go build` or
// `go run` spelled any other way is refused, because a dependency this tool
// cannot see is a regression a feature proof would never run.

// testbinPath is the one helper integration tests build binaries with.
const testbinPkg = modulePath + "/internal/testsupport/testbin"

// binaryUse is one binary a package's tests build.
type binaryUse struct {
	Package string // import path of the main package
	At      string // file:line of the call
}

// scanBinaries finds every binary the _test.go files of dir build, and every
// build it cannot resolve.
func scanBinaries(dir string, files []srcFile) (uses []binaryUse, unresolved []string) {
	for _, f := range files {
		if !strings.HasSuffix(f.Name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(dir, f.Name), f.Content, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		testbinName := ""
		for _, imp := range file.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if p == testbinPkg {
				testbinName = "testbin"
				if imp.Name != nil {
					testbinName = imp.Name.Name
				}
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			at := fset.Position(call.Pos()).String()
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				if x, ok := sel.X.(*ast.Ident); ok && testbinName != "" && x.Name == testbinName && sel.Sel.Name == "Path" {
					if len(call.Args) != 2 {
						unresolved = append(unresolved, at+": testbin.Path takes (t, \"./cmd/<name>\")")
						return true
					}
					lit, ok := call.Args[1].(*ast.BasicLit)
					p := ""
					if ok && lit.Kind == token.STRING {
						p, _ = strconv.Unquote(lit.Value)
					}
					if !strings.HasPrefix(p, "./cmd/") && !strings.HasPrefix(p, "./test/tools/") && !strings.HasPrefix(p, "./examples/") {
						unresolved = append(unresolved, at+": testbin.Path needs a literal \"./cmd/<name>\" or \"./test/tools/<name>\"")
						return true
					}
					uses = append(uses, binaryUse{Package: modulePath + "/" + strings.TrimPrefix(p, "./"), At: at})
					return true
				}
				if x, ok := sel.X.(*ast.Ident); ok && x.Name == "exec" && (sel.Sel.Name == "Command" || sel.Sel.Name == "CommandContext") {
					args := call.Args
					if sel.Sel.Name == "CommandContext" && len(args) > 0 {
						args = args[1:]
					}
					if len(args) >= 2 && isLit(args[0], "go") && (isLit(args[1], "build") || isLit(args[1], "run") || isLit(args[1], "install")) {
						unresolved = append(unresolved, at+": builds a Go binary with exec; use testbin.Path(t, \"./cmd/<name>\") so its dependencies are known")
					}
				}
			}
			return true
		})
	}
	sort.Slice(uses, func(i, j int) bool { return uses[i].Package < uses[j].Package })
	return uses, unresolved
}

func isLit(e ast.Expr, want string) bool {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	s, err := strconv.Unquote(lit.Value)
	return err == nil && s == want
}

// depIndex is every package's test-binary dependencies, per configuration.
type depIndex struct {
	// test[cfg][pkg] is the set of packages pkg's test binary links.
	test map[string]map[string]map[string]bool
	// bins[pkg] is the binaries pkg's tests build.
	bins map[string][]binaryUse
	// binDeps[main] is the set of packages a built binary links.
	binDeps map[string]map[string]bool
}

// dependsOn reports whether pkg's test binary in cfg depends on target,
// through its imports or a binary its tests build, and how.
func (d depIndex) dependsOn(cfg, pkg, target string) (bool, string) {
	if pkg == target {
		return true, "is the package"
	}
	if d.test[cfg][pkg][target] {
		return true, "imports it"
	}
	for _, b := range d.bins[pkg] {
		if b.Package == target || d.binDeps[b.Package][target] {
			return true, "runs " + strings.TrimPrefix(b.Package, modulePath+"/") + ", which links it"
		}
	}
	return false, ""
}

// loadDeps builds the index for every package in the module.
func loadDeps(root string) (depIndex, []string, error) {
	d := depIndex{
		test:    map[string]map[string]map[string]bool{},
		bins:    map[string][]binaryUse{},
		binDeps: map[string]map[string]bool{},
	}
	const format = `{{.ImportPath}}{{"\t"}}{{.ForTest}}{{"\t"}}{{range .Deps}}{{.}}|{{end}}`
	for _, cfg := range configs {
		tags := ""
		if cfg == cfgIntegration {
			tags = "integration"
		}
		lines, err := goList(tags, format, "-test", "./...")
		if err != nil {
			return d, nil, err
		}
		m := map[string]map[string]bool{}
		for _, l := range lines {
			f := strings.SplitN(l, "\t", 3)
			if len(f) != 3 || !strings.HasSuffix(f[0], ".test") {
				continue
			}
			pkg := strings.TrimSuffix(f[0], ".test")
			set := map[string]bool{pkg: true}
			for _, dep := range strings.Split(f[2], "|") {
				if dep = strings.TrimSpace(dep); dep == "" {
					continue
				}
				if i := strings.Index(dep, " ["); i >= 0 {
					dep = dep[:i]
				}
				set[dep] = true
			}
			m[pkg] = set
		}
		d.test[cfg] = m
	}

	// Binary edges, from every package's test sources.
	dirs, err := goList("integration", `{{.ImportPath}}{{"\t"}}{{.Dir}}`, "./...")
	if err != nil {
		return d, nil, err
	}
	var unresolved []string
	mains := map[string]bool{}
	for _, l := range dirs {
		f := strings.SplitN(l, "\t", 2)
		if len(f) != 2 {
			continue
		}
		files, err := readDirGo(f[1])
		if err != nil {
			return d, nil, err
		}
		uses, bad := scanBinaries(f[1], files)
		for _, b := range bad {
			if rel, err := filepath.Rel(root, b); err == nil && !strings.HasPrefix(rel, "..") {
				b = rel
			}
			unresolved = append(unresolved, b)
		}
		if len(uses) > 0 {
			d.bins[f[0]] = uses
			for _, u := range uses {
				mains[u.Package] = true
			}
		}
	}
	if len(mains) > 0 {
		var list []string
		for m := range mains {
			list = append(list, m)
		}
		sort.Strings(list)
		lines, err := goList("", `{{.ImportPath}}{{"\t"}}{{range .Deps}}{{.}}|{{end}}`, list...)
		if err != nil {
			return d, nil, err
		}
		for _, l := range lines {
			f := strings.SplitN(l, "\t", 2)
			if len(f) != 2 {
				continue
			}
			set := map[string]bool{f[0]: true}
			for _, dep := range strings.Split(f[1], "|") {
				if dep != "" {
					set[dep] = true
				}
			}
			d.binDeps[f[0]] = set
		}
	}
	sort.Strings(unresolved)
	return d, unresolved, nil
}

// readDirGo reads every .go file of a directory.
func readDirGo(dir string) ([]srcFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []srcFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		out = append(out, srcFile{Name: e.Name(), Content: b})
	}
	return out, nil
}
