package main

import (
	"bytes"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Build configurations a test file can compile in.
const (
	cfgUnit        = "unit"
	cfgIntegration = "integration"
)

// srcFile is one Go file's name and content.
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

// hasTestPrefix is go test's own rule for a test-like name: the prefix alone,
// or the prefix followed by a rune that is not a lower-case letter.
func hasTestPrefix(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	rest := name[len(prefix):]
	if rest == "" {
		return true
	}
	r, _ := utf8.DecodeRuneInString(rest)
	return !unicode.IsLower(r)
}

// isFuzzTarget reports whether a declaration is a native fuzz target: a
// top-level func named Fuzz... that takes one *testing.F.
func isFuzzTarget(d *ast.FuncDecl) bool {
	if d.Recv != nil || !hasTestPrefix(d.Name.Name, "Fuzz") || d.Type.Params == nil || len(d.Type.Params.List) != 1 {
		return false
	}
	param := d.Type.Params.List[0]
	if len(param.Names) > 1 {
		return false
	}
	star, ok := param.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "F"
}

// testsDeclaredIn lists the tests and fuzz targets a _test.go file declares
// at top level. TestMain is the package's entry point, not a test. A file
// that does not parse declares none.
func testsDeclaredIn(f srcFile) []string {
	if !strings.HasSuffix(f.Name, "_test.go") {
		return nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), f.Name, f.Content, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	var tests []string
	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		name := fn.Name.Name
		if (name != "TestMain" && hasTestPrefix(name, "Test")) || isFuzzTarget(fn) {
			tests = append(tests, name)
		}
	}
	return tests
}
