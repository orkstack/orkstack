package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// backendName is a whole-word, case-sensitive name of an observability
// backend. The kit's own files name them; the product's surfaces never do.
var backendName = regexp.MustCompile(`\b(Grafana|Tempo|Loki|Prometheus)\b`)

// The UI, orkctl, the API and its errors, the proto comments and the
// reference docs people read for them never name or link an observability
// backend: OrkStack exports OpenTelemetry, and which backend reads it is the
// deployment's choice (prod-kit design §2.5). Tests are not surfaces.
func TestSurfaces_NameNoObservabilityBackend(t *testing.T) {
	repo := filepath.Join("..", "..")
	scanned := 0
	scan := func(path string, body string) {
		scanned++
		for i, line := range strings.Split(body, "\n") {
			if m := backendName.FindString(line); m != "" {
				t.Errorf("%s:%d names %s: %s", path, i+1, m, strings.TrimSpace(line))
			}
		}
	}
	isTest := func(name string) bool {
		return strings.HasSuffix(name, "_test.go") || strings.Contains(name, ".test.") || strings.Contains(name, ".spec.")
	}
	walk := func(dir string, keep func(name string) bool) {
		err := filepath.WalkDir(filepath.Join(repo, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "node_modules" || d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if isTest(d.Name()) || !keep(d.Name()) {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(repo, path)
			scan(rel, string(b))
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	source := func(exts ...string) func(string) bool {
		return func(name string) bool {
			for _, e := range exts {
				if strings.HasSuffix(name, e) {
					return true
				}
			}
			return false
		}
	}
	walk("ui/src", source(".ts", ".svelte", ".js", ".css", ".html"))
	walk("cmd/orkctl", source(".go"))
	walk("internal/api", source(".go"))
	walk("proto", source(".proto"))
	for _, doc := range []string{"docs/CLI.md", "docs/API.md", "docs/REASONS.md"} {
		b, err := os.ReadFile(filepath.Join(repo, doc))
		if err != nil {
			t.Fatal(err)
		}
		scan(doc, string(b))
	}

	// The help tree as orkctl renders it, every command's own text included.
	root, _ := newRootCommand()
	var help strings.Builder
	var visit func(*cobra.Command)
	visit = func(c *cobra.Command) {
		help.WriteString(c.CommandPath() + "\n" + c.Short + "\n" + c.Long + "\n" + c.Example + "\n" + c.UsageString() + "\n")
		for _, sub := range c.Commands() {
			visit(sub)
		}
	}
	visit(root)
	scan("orkctl help", help.String())

	if scanned < 50 {
		t.Fatalf("scanned %d sources; the scan has too little to hold", scanned)
	}
}
