package docscheck

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
)

// activityImageMention matches a built-in activity image written with a tag:
// ghcr.io/orkstack/activity-shell:0.1, or the wildcard form
// ghcr.io/orkstack/activity-*:0.1 the docs use for the whole catalog. A
// reference pinned by digest has no tag and is not matched, nor is a
// placeholder tag such as <tag>, which names no real one. A tag ends in a
// letter or digit, so a sentence's full stop is not part of it.
var activityImageMention = regexp.MustCompile(
	`ghcr\.io/orkstack/activity-([A-Za-z0-9*<>_-]+):([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)`)

// catalogTagMismatches returns one line per mention of a built-in activity
// image in text whose tag is not tag, each naming path and line.
func catalogTagMismatches(path, text, tag string) []string {
	var bad []string
	for i, line := range strings.Split(text, "\n") {
		for _, m := range activityImageMention.FindAllStringSubmatch(line, -1) {
			if m[2] != tag {
				bad = append(bad, fmt.Sprintf("%s:%d: %s names :%s, but the built-in activity catalog is at :%s",
					path, i+1, m[0], m[2], tag))
			}
		}
	}
	return bad
}

// TestDocs_NameTheBuiltinCatalogTag: every built-in activity image a document
// or the configuration reference writes with a tag carries the tag the
// engine's catalog resolves a short name to (internal/dsl/activity_catalog.go).
// A release publishes the images as :MAJOR.MINOR, so a reference left at an
// older series would tell a reader to pull an image the release does not
// serve.
func TestDocs_NameTheBuiltinCatalogTag(t *testing.T) {
	tag, err := dist.BuiltinCatalogTag()
	if err != nil {
		t.Fatal(err)
	}
	root := repoRoot(t)
	files := markdownFiles(t)
	for _, extra := range []string{"deploy/orkstack.defaults.yaml", "deploy/orkstack.example.yaml"} {
		files = append(files, filepath.Join(root, extra))
	}
	var bad []string
	mentions := map[string]int{}
	for _, file := range files {
		rel, err := filepath.Rel(root, file)
		if err != nil {
			t.Fatal(err)
		}
		rel = filepath.ToSlash(rel)
		text := readFile(t, file)
		mentions[rel] = len(activityImageMention.FindAllString(text, -1))
		bad = append(bad, catalogTagMismatches(rel, text, tag)...)
	}
	if mentions["deploy/orkstack.defaults.yaml"] == 0 || mentions["docs/OCI_ACTIVITY_CONTRACT.md"] == 0 {
		t.Fatalf("the catalog's tag is documented in deploy/orkstack.defaults.yaml and docs/OCI_ACTIVITY_CONTRACT.md, "+
			"and the scan found no mention in one of them (%v): the pattern or the file list has drifted", mentions)
	}
	if len(bad) > 0 {
		t.Fatalf("%d mention(s) of a built-in activity image disagree with the catalog's tag :%s:\n%s\n\n"+
			"Write a placeholder as <tag>. To move the series, change the catalog and these together.",
			len(bad), tag, strings.Join(bad, "\n"))
	}
}

// TestDocs_NameTheBuiltinCatalogTag_BitesOnPlantedText: the check above sees a
// wrong tag wherever it hides, and nothing else. Without this, a pattern that
// matched nothing would pass every tree.
func TestDocs_NameTheBuiltinCatalogTag_BitesOnPlantedText(t *testing.T) {
	const tag = "0.1"
	tests := []struct {
		name string
		text string
		want []string // the substrings each reported line must carry, one entry per mismatch
	}{
		{"the catalog's own tag", "uses: ghcr.io/orkstack/activity-shell:0.1\n", nil},
		{"a wildcard at the right tag", "ORK_ACTIVITY_CATALOG: ghcr.io/orkstack/activity-*:0.1/x\n", nil},
		{"an older series", "pull ghcr.io/orkstack/activity-shell:0.0\n", []string{"x.md:1: ghcr.io/orkstack/activity-shell:0.0 names :0.0"}},
		{"a newer series", "see `ghcr.io/orkstack/activity-http:0.2` for more\n", []string{"x.md:1: ghcr.io/orkstack/activity-http:0.2"}},
		{"a full version is not the series", "ghcr.io/orkstack/activity-file:0.1.0\n", []string{"names :0.1.0"}},
		{"a floating tag", "ghcr.io/orkstack/activity-json:latest\n", []string{"names :latest"}},
		{"a wildcard at the wrong tag", "\n\nghcr.io/orkstack/activity-*:0.3\n", []string{"x.md:3: ghcr.io/orkstack/activity-*:0.3"}},
		{"two wrong among three", "a ghcr.io/orkstack/activity-git:0.1 b ghcr.io/orkstack/activity-git:0.9 c ghcr.io/orkstack/activity-git:1.0\n",
			[]string{"activity-git:0.9", "activity-git:1.0"}},
		{"a full stop ends a sentence, not a tag", "It is ghcr.io/orkstack/activity-template:0.1.\n", nil},
		{"a digest has no tag", "ghcr.io/orkstack/activity-shell@sha256:9ab2\n", nil},
		{"a placeholder is no tag", "ghcr.io/orkstack/activity-shell:<tag>\n", nil},
		{"another image is not the catalog's", "ghcr.io/orkstack/orkctl:0.7\n", nil},
		{"another registry is not the catalog's", "registry.example/orkstack/activity-shell:0.7\n", nil},
	}
	for _, tc := range tests {
		got := catalogTagMismatches("x.md", tc.text, tag)
		if len(got) != len(tc.want) {
			t.Errorf("%s: %d mismatches %q, want %d", tc.name, len(got), got, len(tc.want))
			continue
		}
		for i, want := range tc.want {
			if !strings.Contains(got[i], want) {
				t.Errorf("%s: mismatch %q does not carry %q", tc.name, got[i], want)
			}
		}
	}
}
