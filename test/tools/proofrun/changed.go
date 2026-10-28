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
// the package's top-level declarations (reach.go). A func, var, const or
// type is matched by identifier name, so that can only over-select. A
// method is matched by its receiver type and its name, never by its name
// alone. A changed TestMain or init selects the whole package. A changed
// build constraint or package clause selects every test the file declares,
// on either side.

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
	Key    string          // "func Name", "method T.Name", "var X", "const X", "type T", "init#3"
	Name   string          // the identifier other declarations reference it by
	Node   string          // its node in the reference graph: Name, "T.Name" for a method, the key of an init
	Recv   string          // a method's receiver type
	IsType bool            // a type declaration
	Alias  bool            // a type alias (type A = T)
	Canon  string          // canonical source, comments removed
	Refs   map[string]bool // identifiers it mentions, selector names included
	Sels   map[string]bool // names its selector expressions select (x.Sel)
	Lits   []string        // its string literals, unquoted
	File   string
	InTest bool // declared in a _test.go file
	Test   bool // a top-level func TestXxx
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
			key, node, recv := "func "+name, name, ""
			if d.Recv != nil && len(d.Recv.List) > 0 {
				recv = recvName(d.Recv.List[0].Type)
				key = "method " + recv + "." + name
				node = recv + "." + name
			} else if name == "init" {
				inits++
				key = "init#" + strconv.Itoa(inits)
				node = key
			}
			// The declared name is not a mention: a method named like a
			// function would otherwise refer to it.
			r, sels := refs(d.Recv, d.Type, d.Body)
			dd := decl{Key: key, Name: name, Node: node, Recv: recv, Canon: canon(d), Refs: r, Sels: sels, Lits: lits(d), File: f.Name}
			if d.Recv == nil && testName(name) && pf.TestGo {
				dd.Test = true
				pf.Tests = append(pf.Tests, name)
			}
			pf.Decls = append(pf.Decls, dd)
		case *ast.GenDecl:
			if d.Tok == token.IMPORT {
				continue
			}
			groupLits := lits(d) // a group's literals belong to every name it declares
			for _, s := range d.Specs {
				switch s := s.(type) {
				case *ast.TypeSpec:
					r, sels := refs(s)
					pf.Decls = append(pf.Decls, decl{Key: "type " + s.Name.Name, Name: s.Name.Name, Node: s.Name.Name,
						IsType: true, Alias: s.Assign.IsValid(),
						Canon: canon(&ast.GenDecl{Tok: d.Tok, Specs: []ast.Spec{s}}), Refs: r, Sels: sels, Lits: groupLits, File: f.Name})
				case *ast.ValueSpec:
					c := canon(&ast.GenDecl{Tok: d.Tok, Specs: []ast.Spec{s}})
					r, sels := refs(s)
					for _, n := range s.Names {
						if n.Name == "_" {
							continue
						}
						pf.Decls = append(pf.Decls, decl{Key: d.Tok.String() + " " + n.Name, Name: n.Name, Node: n.Name,
							Canon: c, Refs: r, Sels: sels, Lits: groupLits, File: f.Name})
					}
				}
			}
		}
	}
	for i := range pf.Decls {
		pf.Decls[i].InTest = pf.TestGo
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

// refs collects every identifier the nodes mention, selector names included,
// and separately the names their selector expressions select. A nil node
// (a func without a body or a receiver) is skipped.
func refs(nodes ...ast.Node) (idents, sels map[string]bool) {
	idents, sels = map[string]bool{}, map[string]bool{}
	for _, n := range nodes {
		if isNilNode(n) {
			continue
		}
		ast.Inspect(n, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.Ident:
				idents[n.Name] = true
			case *ast.SelectorExpr:
				sels[n.Sel.Name] = true
			}
			return true
		})
	}
	return idents, sels
}

// isNilNode reports a nil pointer held in an ast.Node interface, which
// ast.Inspect would dereference.
func isNilNode(n ast.Node) bool {
	switch n := n.(type) {
	case nil:
		return true
	case *ast.FieldList:
		return n == nil
	case *ast.BlockStmt:
		return n == nil
	case *ast.FuncType:
		return n == nil
	}
	return false
}

// lits collects the string literals a node holds, unquoted.
func lits(n ast.Node) []string {
	var out []string
	ast.Inspect(n, func(n ast.Node) bool {
		if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING {
			if s, err := strconv.Unquote(bl.Value); err == nil {
				out = append(out, s)
			}
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
	// Reached names the tests in Tests that are there only because they
	// reach a changed helper, type or variable through the package's
	// declarations; their own source did not change.
	Reached map[string]bool
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
	ch := dirChange{Tests: map[string]string{}, Reached: map[string]bool{}}
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
	// A file that does not parse on either side cannot be mapped onto its
	// declarations: it is a change to the package as a whole (and the
	// build gate fails on it anyway).
	for name := range oldFiles {
		op, inOld := oldP[name]
		np, inNew := newP[name]
		if (inOld && !op.OK) || (inNew && !np.OK) {
			ch.Production = true
			ch.ProductionWhy = append(ch.ProductionWhy, name+" does not parse")
			delete(oldP, name)
			delete(newP, name)
		}
	}
	for name := range newFiles {
		if np, inNew := newP[name]; inNew && !np.OK {
			ch.Production = true
			ch.ProductionWhy = append(ch.ProductionWhy, name+" does not parse")
			delete(newP, name)
		}
	}

	// Declarations by key on each side, across the changed files, so code
	// moved between two changed files is not a change.
	type side struct {
		canon  map[string]string
		isTest map[string]bool
		name   map[string]string
		node   map[string]string
		recv   map[string]string
		test   map[string]bool
	}
	index := func(ps map[string]parsedFile) side {
		s := side{canon: map[string]string{}, isTest: map[string]bool{}, name: map[string]string{}, node: map[string]string{},
			recv: map[string]string{}, test: map[string]bool{}}
		for _, pf := range ps {
			for _, d := range pf.Decls {
				k := d.Key
				if pf.TestGo {
					k = "test:" + k
				}
				s.canon[k] += d.Canon
				s.isTest[k] = pf.TestGo
				s.name[k] = d.Name
				s.node[k] = d.Node
				s.recv[k] = d.Recv
				s.test[k] = d.Test
			}
		}
		return s
	}
	o, n := index(oldP), index(newP)
	changedHelpers := map[string]bool{}       // graph nodes
	changedMethods := map[string]methodNode{} // the methods among them
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
			node, recv := n.node[k], n.recv[k]
			if node == "" {
				node, recv = o.node[k], o.recv[k]
			}
			changedHelpers[node] = true
			if recv != "" {
				// A changed helper is declared in a test file, on either side.
				changedMethods[node] = methodNode{node: node, recv: recv, name: name, test: true}
			}
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
		graph := newDeclGraph()
		var tests []string
		for _, f := range newAll {
			if !fileMatches(cfg, dir, f) {
				continue
			}
			pf := parseGo(f)
			for _, d := range pf.Decls {
				graph.add(d)
				if d.Test {
					tests = append(tests, d.Name)
				}
			}
		}
		for _, m := range changedMethods {
			graph.addMethod(m) // a deleted method is still one
		}
		for _, t := range tests {
			if _, already := ch.Tests[t]; already {
				continue
			}
			if via := graph.reaches(t, changedHelpers); via != "" {
				ch.Tests[t] = "reaches changed " + via
				ch.Reached[t] = true
			}
		}
	}

	// Tests that exist only on the old side are removed, not selected.
	sort.Strings(ch.Removed)
	return ch
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
		graph := newDeclGraph()
		hits := map[string]bool{}
		var tests []string
		for _, f := range files {
			if !strings.HasSuffix(f.Name, ".go") || !fileMatches(cfg, dir, f) {
				continue
			}
			for _, d := range parseGo(f).Decls {
				graph.add(d)
				for _, l := range d.Lits {
					if needle(l) {
						hits[d.Node] = true
						break
					}
				}
				if d.Test {
					tests = append(tests, d.Name)
				}
			}
		}
		if len(hits) == 0 {
			continue
		}
		for _, t := range tests {
			if hits[t] {
				out[t] = "references it"
				continue
			}
			if via := graph.reaches(t, hits); via != "" {
				out[t] = "reaches " + via + ", which references it"
			}
		}
	}
	return out
}
