package main

import (
	"bytes"
	"go/ast"
	"go/build"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"sort"
	"strconv"
	"strings"
)

// Mapping a Go diff onto tests works on declarations, not lines. Both sides
// of every changed file are parsed — the merge base's revision and the
// working tree's — without comments, and each top-level declaration is
// printed in canonical form. A declaration whose canonical form differs, or
// that exists on one side only, is changed. So a hunk that only touched
// comments, or only moved code, changes nothing and needs no evidence, and a
// deleted line still maps to the declaration it was deleted from.
//
// A changed test is selected. A changed helper — a func, method, var, const
// or type declared in a _test.go file — selects every test of the package
// (in the same build configuration) that reaches it transitively through
// the package's top-level declarations. Matching is by identifier name, so
// it can only over-select. A changed TestMain or init selects the whole
// package. A changed build constraint or package clause selects every test
// the file declares, on either side.

// Build configurations a test file can compile in.
const (
	cfgUnit        = "unit"
	cfgIntegration = "integration"
)

var configs = []string{cfgUnit, cfgIntegration}

// srcFile is one Go file's content on one side of the diff.
type srcFile struct {
	Name    string // base name
	Content []byte
}

// fileMatches reports whether a Go file compiles in cfg, with build
// constraints and GOOS/GOARCH suffixes evaluated by go/build itself.
func fileMatches(cfg, dir string, f srcFile) bool {
	ctx := build.Default
	ctx.BuildTags = nil
	if cfg == cfgIntegration {
		ctx.BuildTags = []string{"integration"}
	}
	content := f.Content
	ctx.OpenFile = func(string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(content)), nil
	}
	ok, err := ctx.MatchFile(dir, f.Name)
	return err == nil && ok
}

// decl is one top-level declaration.
type decl struct {
	Key   string          // "func Name", "method T.Name", "var X", "const X", "type T", "init#3"
	Name  string          // the identifier other declarations reference it by
	Canon string          // canonical source, comments removed
	Refs  map[string]bool // identifiers it mentions
	File  string
	Test  bool // a top-level func TestXxx
}

// parsedFile is one parsed file.
type parsedFile struct {
	Name    string
	Package string
	Header  string // package clause + build constraint, canonical
	Decls   []decl
	Tests   []string
	TestGo  bool
	Literal []string // every string literal, unquoted
	OK      bool
}

var testName = func(name string) bool {
	if !strings.HasPrefix(name, "Test") || name == "TestMain" {
		return false
	}
	rest := name[len("Test"):]
	return rest == "" || !(rest[0] >= 'a' && rest[0] <= 'z')
}

// parseGo parses one file without comments into its declarations.
func parseGo(f srcFile) parsedFile {
	pf := parsedFile{Name: f.Name, TestGo: strings.HasSuffix(f.Name, "_test.go")}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, f.Name, f.Content, parser.SkipObjectResolution)
	if err != nil {
		return pf
	}
	pf.OK = true
	pf.Package = file.Name.Name
	pf.Header = "package " + file.Name.Name + "\n" + buildConstraint(f.Content)
	inits := 0
	canon := func(n any) string {
		var b bytes.Buffer
		_ = printer.Fprint(&b, token.NewFileSet(), n)
		return b.String()
	}
	for _, d := range file.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			name := d.Name.Name
			key := "func " + name
			if d.Recv != nil && len(d.Recv.List) > 0 {
				key = "method " + recvName(d.Recv.List[0].Type) + "." + name
			} else if name == "init" {
				inits++
				key = "init#" + strconv.Itoa(inits)
			}
			dd := decl{Key: key, Name: name, Canon: canon(d), Refs: refs(d), File: f.Name}
			if d.Recv == nil && testName(name) && pf.TestGo {
				dd.Test = true
				pf.Tests = append(pf.Tests, name)
			}
			pf.Decls = append(pf.Decls, dd)
		case *ast.GenDecl:
			if d.Tok == token.IMPORT {
				continue
			}
			for _, s := range d.Specs {
				switch s := s.(type) {
				case *ast.TypeSpec:
					pf.Decls = append(pf.Decls, decl{Key: "type " + s.Name.Name, Name: s.Name.Name,
						Canon: canon(&ast.GenDecl{Tok: d.Tok, Specs: []ast.Spec{s}}), Refs: refs(s), File: f.Name})
				case *ast.ValueSpec:
					c := canon(&ast.GenDecl{Tok: d.Tok, Specs: []ast.Spec{s}})
					r := refs(s)
					for _, n := range s.Names {
						if n.Name == "_" {
							continue
						}
						pf.Decls = append(pf.Decls, decl{Key: d.Tok.String() + " " + n.Name, Name: n.Name, Canon: c, Refs: r, File: f.Name})
					}
				}
			}
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING {
			if s, err := strconv.Unquote(bl.Value); err == nil {
				pf.Literal = append(pf.Literal, s)
			}
		}
		return true
	})
	return pf
}

// buildConstraint returns the file's //go:build line, if any.
func buildConstraint(src []byte) string {
	for _, line := range strings.Split(string(src), "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "//go:build ") {
			return t
		}
		if strings.HasPrefix(t, "package ") {
			break
		}
	}
	return ""
}

func recvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvName(t.X)
	case *ast.IndexExpr:
		return recvName(t.X)
	case *ast.IndexListExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

// refs collects every identifier a node mentions, selector names included.
func refs(n ast.Node) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(n, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok {
			out[id.Name] = true
		}
		return true
	})
	return out
}

// dirChange is what a diff did to one package directory, for one build
// configuration.
type dirChange struct {
	// Tests are tests that changed or reach a changed helper, on the new side.
	Tests map[string]string // test -> why
	// Whole is set when TestMain or init changed: the whole binary.
	Whole string
	// Removed are tests that exist only on the old side.
	Removed []string
	// Production is set when a non-test declaration, a non-test file's
	// header, or a non-Go file of the package changed.
	Production bool
	// ProductionWhy names what changed outside the tests.
	ProductionWhy []string
	// TestFilesChanged is set when any non-comment change touched a
	// _test.go file.
	TestFilesChanged bool
}

// analyzeDir compares one package directory's changed files across the
// diff, for one build configuration. oldFiles and newFiles hold the changed
// files' content on each side (a file absent on a side is missing from its
// map); newAll is every Go file of the directory on the new side, which is
// what reachability is computed over.
func analyzeDir(cfg, dir string, oldFiles, newFiles map[string]srcFile, newAll []srcFile) dirChange {
	ch := dirChange{Tests: map[string]string{}}
	parseSide := func(files map[string]srcFile) map[string]parsedFile {
		out := map[string]parsedFile{}
		for name, f := range files {
			if !strings.HasSuffix(name, ".go") || !fileMatches(cfg, dir, f) {
				continue
			}
			out[name] = parseGo(f)
		}
		return out
	}
	oldP, newP := parseSide(oldFiles), parseSide(newFiles)

	// Declarations by key on each side, across the changed files, so code
	// moved between two changed files is not a change.
	type side struct {
		canon  map[string]string
		isTest map[string]bool
		name   map[string]string
		test   map[string]bool
	}
	index := func(ps map[string]parsedFile) side {
		s := side{canon: map[string]string{}, isTest: map[string]bool{}, name: map[string]string{}, test: map[string]bool{}}
		for _, pf := range ps {
			for _, d := range pf.Decls {
				k := d.Key
				if pf.TestGo {
					k = "test:" + k
				}
				s.canon[k] += d.Canon
				s.isTest[k] = pf.TestGo
				s.name[k] = d.Name
				s.test[k] = d.Test
			}
		}
		return s
	}
	o, n := index(oldP), index(newP)
	changedHelpers := map[string]bool{}
	keys := map[string]bool{}
	for k := range o.canon {
		keys[k] = true
	}
	for k := range n.canon {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		oc, inOld := o.canon[k]
		nc, inNew := n.canon[k]
		if inOld && inNew && oc == nc {
			continue
		}
		isTest := o.isTest[k] || n.isTest[k]
		name := n.name[k]
		if name == "" {
			name = o.name[k]
		}
		if !isTest {
			ch.Production = true
			ch.ProductionWhy = append(ch.ProductionWhy, strings.TrimPrefix(k, "test:"))
			continue
		}
		ch.TestFilesChanged = true
		switch {
		case n.test[k] || (o.test[k] && !inNew):
			if inNew {
				ch.Tests[name] = "changed"
			} else {
				ch.Removed = append(ch.Removed, name)
			}
		case name == "TestMain" || strings.HasPrefix(k, "test:init#"):
			ch.Whole = strings.TrimPrefix(k, "test:") + " changed"
		default:
			changedHelpers[name] = true
		}
	}

	// A changed header (package clause, build constraint) changes which
	// tests compile where: every test the file declares, on both sides.
	names := map[string]bool{}
	for name := range oldFiles {
		names[name] = true
	}
	for name := range newFiles {
		names[name] = true
	}
	for name := range names {
		op, inOld := oldP[name]
		np, inNew := newP[name]
		if inOld && inNew && op.Header == np.Header {
			continue
		}
		if !inOld && !inNew {
			continue
		}
		if !strings.HasSuffix(name, "_test.go") {
			if (inOld && op.OK) || (inNew && np.OK) {
				if inOld && inNew {
					ch.Production = true
					ch.ProductionWhy = append(ch.ProductionWhy, name+" header")
				}
			}
			continue
		}
		if inOld && inNew {
			ch.TestFilesChanged = true
			for _, t := range np.Tests {
				ch.Tests[t] = "its file's build constraint or package clause changed"
			}
		}
	}

	// Reachability over the whole package on the new side.
	if len(changedHelpers) > 0 {
		graph := map[string]map[string]bool{}
		var tests []string
		for _, f := range newAll {
			if !fileMatches(cfg, dir, f) {
				continue
			}
			pf := parseGo(f)
			for _, d := range pf.Decls {
				if graph[d.Name] == nil {
					graph[d.Name] = map[string]bool{}
				}
				for r := range d.Refs {
					graph[d.Name][r] = true
				}
				if d.Test {
					tests = append(tests, d.Name)
				}
			}
		}
		helpers := make([]string, 0, len(changedHelpers))
		for h := range changedHelpers {
			helpers = append(helpers, h)
		}
		sort.Strings(helpers)
		for _, t := range tests {
			if _, already := ch.Tests[t]; already {
				continue
			}
			if via := reaches(graph, t, changedHelpers); via != "" {
				ch.Tests[t] = "reaches changed " + via
			}
		}
	}

	// Tests that exist only on the old side are removed, not selected.
	sort.Strings(ch.Removed)
	return ch
}

// reaches reports which target name `from` reaches through graph, or "".
func reaches(graph map[string]map[string]bool, from string, targets map[string]bool) string {
	seen := map[string]bool{from: true}
	queue := []string{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		var next []string
		for r := range graph[cur] {
			next = append(next, r)
		}
		sort.Strings(next)
		for _, r := range next {
			if targets[r] {
				return r
			}
			if !seen[r] && graph[r] != nil {
				seen[r] = true
				queue = append(queue, r)
			}
		}
	}
	return ""
}

// testsDeclared lists the TestXxx funcs declared in files that compile in
// cfg, and whether each is declared in a file that compiles ONLY under the
// integration tag.
func testsDeclared(dir string, files []srcFile) (unit map[string]bool, integration map[string]bool) {
	unit, integration = map[string]bool{}, map[string]bool{}
	for _, f := range files {
		if !strings.HasSuffix(f.Name, "_test.go") {
			continue
		}
		inUnit := fileMatches(cfgUnit, dir, f)
		inInteg := fileMatches(cfgIntegration, dir, f)
		if !inUnit && !inInteg {
			continue
		}
		pf := parseGo(f)
		for _, t := range pf.Tests {
			if inUnit {
				unit[t] = true
			} else {
				integration[t] = true
			}
		}
	}
	return unit, integration
}

// testsReferencing lists the tests of a package (any configuration) that
// reach a declaration whose string literals contain needle. It is how a
// changed tool, example or fixture finds the tests that use it: by search,
// never by a hand-kept map.
func testsReferencing(dir string, files []srcFile, needle func(string) bool) map[string]string {
	out := map[string]string{}
	for _, cfg := range configs {
		graph := map[string]map[string]bool{}
		hits := map[string]bool{}
		var tests []string
		for _, f := range files {
			if !strings.HasSuffix(f.Name, ".go") || !fileMatches(cfg, dir, f) {
				continue
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, f.Name, f.Content, parser.SkipObjectResolution)
			if err != nil {
				continue
			}
			for _, d := range file.Decls {
				var names []string
				var node ast.Node = d
				isTest := false
				switch d := d.(type) {
				case *ast.FuncDecl:
					names = []string{d.Name.Name}
					isTest = d.Recv == nil && testName(d.Name.Name) && strings.HasSuffix(f.Name, "_test.go")
				case *ast.GenDecl:
					if d.Tok == token.IMPORT {
						continue
					}
					for _, s := range d.Specs {
						switch s := s.(type) {
						case *ast.TypeSpec:
							names = append(names, s.Name.Name)
						case *ast.ValueSpec:
							for _, n := range s.Names {
								names = append(names, n.Name)
							}
						}
					}
				}
				r := refs(node)
				hit := false
				ast.Inspect(node, func(n ast.Node) bool {
					if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING {
						if s, err := strconv.Unquote(bl.Value); err == nil && needle(s) {
							hit = true
						}
					}
					return !hit
				})
				for _, name := range names {
					if graph[name] == nil {
						graph[name] = map[string]bool{}
					}
					for x := range r {
						graph[name][x] = true
					}
					if hit {
						hits[name] = true
					}
					if isTest {
						tests = append(tests, name)
					}
				}
			}
		}
		for _, t := range tests {
			if hits[t] {
				out[t] = "references it"
				continue
			}
			if via := reaches(graph, t, hits); via != "" {
				out[t] = "reaches " + via + ", which references it"
			}
		}
	}
	return out
}
