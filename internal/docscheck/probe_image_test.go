package docscheck

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestDocs_NoDocSaysProbeImageIsRequired: orker.profiles.probe_image is
// optional on a release Orker, which defaults to its own release image, and
// required only on one built from source. No sentence, list item or table
// row of the reference docs, the site or the examples may say it is required
// without naming that case.
func TestDocs_NoDocSaysProbeImageIsRequired(t *testing.T) {
	root := repoRoot(t)
	required := regexp.MustCompile(`(?i)\brequired\b|\bmust be set\b|\bmandatory\b`)
	// A unit is one sentence, list item or table row: what one claim can
	// span.
	units := regexp.MustCompile(`\.\s+|\n\s*\n|\n\s*(?:[-*|]|\d+\.)\s`)
	for _, dir := range []string{"docs", "site/content", "examples"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !(strings.HasSuffix(path, ".md") || strings.HasSuffix(path, ".mdx") || strings.HasSuffix(path, ".yaml")) {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, unit := range units.Split(string(raw), -1) {
				if !strings.Contains(unit, "probe_image") || !required.MatchString(unit) || strings.Contains(unit, "from source") {
					continue
				}
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s says probe_image is required without naming an Orker built from source:\n%s", rel, unit)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
}
