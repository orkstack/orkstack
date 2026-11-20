package docscheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// searchAttrSource is where the engine names and registers its search
// attributes; searchAttrBoot is where it logs whether each group is usable.
const (
	searchAttrSource = "internal/task/searchattr.go"
	searchAttrBoot   = "cmd/ork/main.go"
	// searchAttrSitePage is the site's how-to that repeats the commands.
	searchAttrSitePage = "site/content/how-to/temporal-search-attributes.md"
)

// registeredSearchAttributes is every custom search attribute the engine
// registers, by name and Temporal CLI type: the keys and values of each
// map[string]enumspb.IndexedValueType literal in searchattr.go, the maps
// ensureSearchAttributes is handed. A key is a constant of that file.
func registeredSearchAttributes(t *testing.T, root string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, searchAttrSource), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	consts := map[string]string{}
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range spec.Names {
			if i < len(spec.Values) {
				if lit, ok := spec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					v, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
					consts[name.Name] = v
				}
			}
		}
		return true
	})
	types := map[string]string{
		"INDEXED_VALUE_TYPE_KEYWORD":      "Keyword",
		"INDEXED_VALUE_TYPE_KEYWORD_LIST": "KeywordList",
		"INDEXED_VALUE_TYPE_TEXT":         "Text",
		"INDEXED_VALUE_TYPE_INT":          "Int",
		"INDEXED_VALUE_TYPE_DOUBLE":       "Double",
		"INDEXED_VALUE_TYPE_BOOL":         "Bool",
		"INDEXED_VALUE_TYPE_DATETIME":     "Datetime",
	}
	out := map[string]string{}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		mt, ok := lit.Type.(*ast.MapType)
		if !ok {
			return true
		}
		if sel, ok := mt.Value.(*ast.SelectorExpr); !ok || sel.Sel.Name != "IndexedValueType" {
			return true
		}
		for _, el := range lit.Elts {
			kv := el.(*ast.KeyValueExpr)
			key, ok := kv.Key.(*ast.Ident)
			if !ok || consts[key.Name] == "" {
				t.Fatalf("%s: %s registers a key that is not one of its string constants", searchAttrSource, fset.Position(kv.Pos()))
			}
			sel, ok := kv.Value.(*ast.SelectorExpr)
			if !ok || types[sel.Sel.Name] == "" {
				t.Fatalf("%s: %s registers %s with a type this test does not know", searchAttrSource, fset.Position(kv.Pos()), key.Name)
			}
			out[consts[key.Name]] = types[sel.Sel.Name]
		}
		return true
	})
	if len(out) == 0 {
		t.Fatalf("%s registers no search attribute this test can find", searchAttrSource)
	}
	// Every map ensureSearchAttributes is handed must be one this test read:
	// a literal written in the call, or a package variable holding one.
	literalVars := map[string]bool{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if i < len(vs.Values) {
					if _, ok := vs.Values[i].(*ast.CompositeLit); ok {
						literalVars[name.Name] = true
					}
				}
			}
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "ensureSearchAttributes" || len(call.Args) == 0 {
			return true
		}
		switch arg := call.Args[len(call.Args)-1].(type) {
		case *ast.CompositeLit:
		case *ast.Ident:
			if !literalVars[arg.Name] {
				t.Fatalf("%s: %s hands ensureSearchAttributes %s, which is not a map literal this test reads", searchAttrSource, fset.Position(call.Pos()), arg.Name)
			}
		default:
			t.Fatalf("%s: %s hands ensureSearchAttributes a map this test cannot read", searchAttrSource, fset.Position(call.Pos()))
		}
		return true
	})
	return out
}

var (
	searchAttrRow       = regexp.MustCompile("^\\| `([A-Za-z]+)` \\| ([A-Za-z]+) \\|.*\\| `([a-z_]+)` \\|$")
	searchAttrSelfHost  = regexp.MustCompile(`temporal operator search-attribute create --namespace ork-main --name ([A-Za-z]+) --type ([A-Za-z]+)$`)
	searchAttrCloudFlag = regexp.MustCompile(`--search-attribute ([A-Za-z]+)=([A-Za-z]+)`)
)

// markedBlock is what follows the marker <!-- search-attributes: name -->:
// the code block it opens, or else the lines up to the next blank line.
func markedBlock(t *testing.T, doc, name string) string {
	t.Helper()
	marker := "<!-- search-attributes: " + name + " -->\n"
	if n := strings.Count(doc, marker); n != 1 {
		t.Fatalf("docs/DEPLOY.md holds the marker %q %d times, want once", strings.TrimSpace(marker), n)
	}
	rest := doc[strings.Index(doc, marker)+len(marker):]
	if strings.HasPrefix(rest, "```") {
		end := strings.Index(rest[3:], "```")
		if end < 0 {
			t.Fatalf("docs/DEPLOY.md: the block after %q does not close", strings.TrimSpace(marker))
		}
		return rest[:end+3]
	}
	block, _, _ := strings.Cut(rest, "\n\n")
	return block
}

// docs/DEPLOY.md's "Temporal search attributes" lists exactly the custom
// search attributes the engine registers, each with the type it registers it
// under and a boot log key the engine logs, and its by-hand commands, for a
// self-hosted cluster and for Temporal Cloud, add exactly those attributes
// with those types, as do the site how-to's. An attribute added to the code and not to the doc, or
// one the doc keeps after the code dropped it, fails here.
func TestDeployDoc_SearchAttributesTableMatchesTheCode(t *testing.T) {
	root := repoRoot(t)
	want := registeredSearchAttributes(t, root)
	doc := readFile(t, filepath.Join(root, "docs", "DEPLOY.md"))
	boot := readFile(t, filepath.Join(root, filepath.FromSlash(searchAttrBoot)))

	table := map[string]string{}
	for _, line := range strings.Split(markedBlock(t, doc, "table"), "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		m := searchAttrRow.FindStringSubmatch(line)
		if m == nil {
			t.Errorf("docs/DEPLOY.md: a search attribute row is not `| `Name` | Type | … | `log_key` |`: %s", line)
			continue
		}
		if _, dup := table[m[1]]; dup {
			t.Errorf("docs/DEPLOY.md lists %s twice", m[1])
		}
		table[m[1]] = m[2]
		if !strings.Contains(boot, strconv.Quote(m[3])) {
			t.Errorf("docs/DEPLOY.md names the boot log key %s for %s, which %s does not log", m[3], m[1], searchAttrBoot)
		}
	}
	if !maps.Equal(table, want) {
		t.Errorf("docs/DEPLOY.md's table lists %v; %s registers %v", sortedPairs(table), searchAttrSource, sortedPairs(want))
	}

	selfHosted := map[string]string{}
	for _, line := range strings.Split(markedBlock(t, doc, "self-hosted"), "\n") {
		if m := searchAttrSelfHost.FindStringSubmatch(line); m != nil {
			selfHosted[m[1]] = m[2]
		}
	}
	if !maps.Equal(selfHosted, want) {
		t.Errorf("docs/DEPLOY.md's self-hosted commands add %v; %s registers %v", sortedPairs(selfHosted), searchAttrSource, sortedPairs(want))
	}

	page := readFile(t, filepath.Join(root, filepath.FromSlash(searchAttrSitePage)))
	site := map[string]string{}
	for _, line := range strings.Split(page, "\n") {
		if m := searchAttrSelfHost.FindStringSubmatch(line); m != nil {
			site[m[1]] = m[2]
		}
	}
	if !maps.Equal(site, want) {
		t.Errorf("%s's commands add %v; %s registers %v", searchAttrSitePage, sortedPairs(site), searchAttrSource, sortedPairs(want))
	}

	cloud := map[string]string{}
	for _, m := range searchAttrCloudFlag.FindAllStringSubmatch(markedBlock(t, doc, "cloud"), -1) {
		cloud[m[1]] = m[2]
	}
	if !maps.Equal(cloud, want) {
		t.Errorf("docs/DEPLOY.md's Temporal Cloud command adds %v; %s registers %v", sortedPairs(cloud), searchAttrSource, sortedPairs(want))
	}
}

func sortedPairs(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	slices.Sort(out)
	return out
}
