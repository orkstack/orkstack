package dsl

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// docSection is one "### title" section of docs/ACTIVITY_IMAGES.md.
func docSection(t *testing.T, title string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "ACTIVITY_IMAGES.md"))
	must.OK(t, err)
	section := regexp.MustCompile(`(?s)\n### ` + regexp.QuoteMeta(title) + `\n(.*?)(\n## |\n### |\z)`).FindStringSubmatch(string(raw))
	if section == nil {
		t.Fatalf("docs/ACTIVITY_IMAGES.md has no section %q", title)
	}
	return section[1]
}

// fences are the bodies of a section's fenced blocks of one language.
func fences(section, language string) []string {
	var out []string
	for _, m := range regexp.MustCompile("(?s)```"+language+"\n(.*?)```").FindAllStringSubmatch(section, -1) {
		out = append(out, m[1])
	}
	return out
}

// TestActivityImagesDoc_ExamplesPackToTheLabelsShown: every LABEL line a
// section shows is what pack writes for one of the YAML examples it shows,
// so the reference never teaches a label pack would not write.
func TestActivityImagesDoc_ExamplesPackToTheLabelsShown(t *testing.T) {
	for _, tc := range []struct{ section, label string }{
		{"Describing the activity", DocsLabel},
		{"The icon", IconLabel},
	} {
		section := docSection(t, tc.section)
		yamls, dockerfiles := fences(section, "yaml"), fences(section, "dockerfile")
		if len(yamls) == 0 || len(dockerfiles) == 0 {
			t.Fatalf("%q shows no YAML or no LABEL example", tc.section)
		}
		packed := map[string]bool{}
		for _, document := range yamls {
			if !strings.HasPrefix(document, "apiVersion:") {
				document = "apiVersion: orkstack/v1\nkind: ActivityInterface\n" + document
			}
			decl, err := ParseActivityDocument([]byte(document))
			must.OKf(t, err, "%q", tc.section)
			var value string
			if tc.label == IconLabel {
				value, err = PackIconLabel(decl.Icon)
			} else {
				value, err = PackDocsLabel(decl.Docs, decl.Interface)
			}
			must.OKf(t, err, "%q", tc.section)
			packed["LABEL "+tc.label+"='"+value+"'\n"] = true
		}
		for _, line := range dockerfiles {
			if !packed[line] {
				t.Errorf("%q shows\n%s\nwhich no example in it packs to", tc.section, line)
			}
		}
	}
}

// TestActivityImagesDoc_TheDefaultIconIsOneTheEngineReads: the default icon
// clients draw is published as an icon: block, held to the rules every
// image's icon is, so a client copies a drawing the engine itself accepts.
func TestActivityImagesDoc_TheDefaultIconIsOneTheEngineReads(t *testing.T) {
	yamls := fences(docSection(t, "The default icon"), "yaml")
	if len(yamls) != 1 {
		t.Fatalf("the default icon section shows %d YAML blocks, want one", len(yamls))
	}
	decl, err := ParseActivityDocument([]byte("apiVersion: orkstack/v1\nkind: ActivityInterface\n" + yamls[0]))
	must.OK(t, err)
	if decl.Icon == nil {
		t.Fatal("the default icon section declares no icon")
	}
	label, err := PackIconLabel(decl.Icon)
	must.OK(t, err)
	if _, err := ParseIconLabel(label); err != nil {
		t.Fatalf("the engine refuses the default icon: %v", err)
	}
}
