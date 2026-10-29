package docscheck

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// yamlFenceOpen opens a fenced YAML block, at the margin or indented under a
// list item: its indent, its marker run and nothing but the info string after.
var yamlFenceOpen = regexp.MustCompile("^( *)(```+|~~~+)[ \t]*ya?ml\\b")

// yamlBlock is one YAML source a reader is shown: a file, or a fence of a
// markdown file whose body starts on line first+1.
type yamlBlock struct {
	first int
	src   string
}

// yamlFences returns the fenced YAML blocks of a markdown document, each
// dedented by its fence's indent.
func yamlFences(doc string) []yamlBlock {
	lines := strings.Split(doc, "\n")
	var out []yamlBlock
	for i := 0; i < len(lines); i++ {
		m := yamlFenceOpen.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		indent, mark := len(m[1]), m[2][:1]
		var body []string
		j := i + 1
		for ; j < len(lines); j++ {
			t := strings.TrimSpace(lines[j])
			if len(t) >= 3 && strings.Trim(t, mark) == "" {
				break
			}
			lead := len(lines[j]) - len(strings.TrimLeft(lines[j], " "))
			body = append(body, lines[j][min(indent, lead):])
		}
		out = append(out, yamlBlock{first: i + 1, src: strings.Join(body, "\n")})
		i = j
	}
	return out
}

// blockStyleProblems returns one line per non-empty flow collection — a
// [a, b] sequence or a {k: v} mapping — in the YAML a reader is shown in the
// file at path: the whole file for a .yaml or .yml file, every yaml fence for
// a markdown one. Each line names the file and the line the collection opens
// on. An empty [] or {} has no block form and passes. A block that does not
// parse is a problem too: an example a reader cannot load teaches nothing.
func blockStyleProblems(path, text string) []string {
	blocks := []yamlBlock{{src: text}}
	if strings.HasSuffix(path, ".md") {
		blocks = yamlFences(text)
	}
	var bad []string
	for _, b := range blocks {
		lines := strings.Split(b.src, "\n")
		var walk func(n *yaml.Node)
		walk = func(n *yaml.Node) {
			if (n.Kind == yaml.SequenceNode || n.Kind == yaml.MappingNode) && n.Style&yaml.FlowStyle != 0 && len(n.Content) > 0 {
				bad = append(bad, fmt.Sprintf("%s:%d: %s", path, b.first+n.Line, strings.TrimSpace(lines[n.Line-1])))
				return
			}
			for _, c := range n.Content {
				walk(c)
			}
		}
		dec := yaml.NewDecoder(strings.NewReader(b.src))
		for {
			var doc yaml.Node
			err := dec.Decode(&doc)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				bad = append(bad, fmt.Sprintf("%s:%d: the YAML does not parse: %v", path, b.first+1, err))
				break
			}
			walk(&doc)
		}
	}
	return bad
}

// TestDocs_YAMLExamplesUseBlockStyle: every YAML example a reader is shown —
// the shipped examples, the Kubernetes manifests, the example configuration,
// the generated defaults the docs site's Configuration page shows, and every
// yaml fence of the documentation — writes its sequences and mappings in block style, one
// `- item` or `key: value` per line, never as a JSON-looking flow collection
// ([a, b], {k: v}). An Orkflow is a YAML document, and its examples teach the
// shape authors copy, so they all write it one way.
func TestDocs_YAMLExamplesUseBlockStyle(t *testing.T) {
	root := repoRoot(t)
	files := markdownFiles(t)
	for _, dir := range []string{"examples", filepath.Join("deploy", "kubernetes")} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && (strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml")) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	files = append(files, filepath.Join(root, "deploy", "orkstack.example.yaml"), filepath.Join(root, "deploy", "orkstack.defaults.yaml"))

	var bad []string
	fences, examples := 0, 0
	for _, file := range files {
		rel, err := filepath.Rel(root, file)
		if err != nil {
			t.Fatal(err)
		}
		rel = filepath.ToSlash(rel)
		text := readFile(t, file)
		if strings.HasSuffix(rel, ".md") {
			fences += len(yamlFences(text))
		} else {
			examples++
		}
		bad = append(bad, blockStyleProblems(rel, text)...)
	}
	if fences == 0 || examples < 2 {
		t.Fatalf("the scan found %d yaml fences and %d YAML files: the fence pattern or the file list has drifted", fences, examples)
	}
	if len(bad) > 0 {
		t.Fatalf("%d YAML example(s) write a flow collection; write it in block style, one `- item` or `key: value` per line:\n%s",
			len(bad), strings.Join(bad, "\n"))
	}
}

// TestDocs_YAMLExamplesUseBlockStyle_BitesOnPlantedText: the check above sees a
// flow collection wherever a reader is shown one, and nothing else. Without
// this, a fence pattern that matched nothing would pass every tree.
func TestDocs_YAMLExamplesUseBlockStyle_BitesOnPlantedText(t *testing.T) {
	tests := []struct {
		name string
		path string
		text string
		want []string // the substrings each reported line must carry, one entry per problem
	}{
		{"block style", "x.md", "```yaml\nneeds:\n  - build\nretry:\n  maximum_attempts: 2\n```\n", nil},
		{"a flow sequence", "x.md", "text\n\n```yaml\ntasks:\n  - id: a\n    needs: [build]\n```\n", []string{"x.md:6: needs: [build]"}},
		{"a flow mapping", "x.md", "```yml\nretry: { maximum_attempts: 2 }\n```\n", []string{"x.md:2: retry: { maximum_attempts: 2 }"}},
		{"nested flow reports the outer one", "x.md", "```yaml\nparts: [{name: a}, {name: b}]\n```\n", []string{"x.md:2: parts:"}},
		{"empty collections have no block form", "x.md", "```yaml\nneeds: []\nwith: {}\n```\n", nil},
		{"a fence under a list item", "x.md", "1. Apply it:\n\n   ```yaml\n   needs: [a]\n   ```\n", []string{"x.md:4: needs: [a]"}},
		{"a tilde fence", "x.md", "~~~yaml\nneeds: [a]\n~~~\n", []string{"x.md:2:"}},
		{"two fences, the second wrong", "x.md", "```yaml\na: 1\n```\n\n```yaml\nb: [1, 2]\n```\n", []string{"x.md:6: b: [1, 2]"}},
		{"json is not yaml", "x.md", "```json\n{\"needs\": [\"a\"]}\n```\n", nil},
		{"prose is not an example", "x.md", "Write `needs: [a]` and see.\n", nil},
		{"a quoted jq program is a string", "x.md", "```yaml\nfilter: '{plan: .value}'\n```\n", nil},
		{"a template is a string", "x.md", "```yaml\nover: '{{ .tasks.list.output.result }}'\n```\n", nil},
		{"a yaml file", "x.yaml", "a: 1\nb: [x]\n", []string{"x.yaml:2: b: [x]"}},
		{"a document separator", "x.yaml", "a: 1\n---\nb: {c: d}\n", []string{"x.yaml:3: b: {c: d}"}},
		{"a fence that does not parse", "x.md", "```yaml\na: [b\n```\n", []string{"x.md:2: the YAML does not parse"}},
	}
	for _, tc := range tests {
		got := blockStyleProblems(tc.path, tc.text)
		if len(got) != len(tc.want) {
			t.Errorf("%s: %d problems %q, want %d", tc.name, len(got), got, len(tc.want))
			continue
		}
		for i, want := range tc.want {
			if !strings.Contains(got[i], want) {
				t.Errorf("%s: problem %q does not carry %q", tc.name, got[i], want)
			}
		}
	}
}
