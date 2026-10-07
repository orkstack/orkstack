package localcatalog_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
)

// TestNames_AreEveryFirstPartyImage holds the four places that name the
// first-party images as a set to one another: this package's Names (what the
// integration suites push and resolve), the built-in catalog, the
// images/activity-* directories, and images/Makefile's ACTIVITIES (what
// `make -C images push`, the proof scripts and `make up` iterate). A fifth
// image added to one and not the others fails here, not in a suite that
// happens to use it.
func TestNames_AreEveryFirstPartyImage(t *testing.T) {
	names := slices.Clone(localcatalog.Names)
	sort.Strings(names)

	catalog := dsl.BuiltinActivityCatalog().Names()

	root := filepath.Join("..", "..", "..")
	dirs, err := filepath.Glob(filepath.Join(root, "images", "activity-*"))
	if err != nil {
		t.Fatal(err)
	}
	var images []string
	for _, d := range dirs {
		images = append(images, strings.TrimPrefix(filepath.Base(d), "activity-"))
	}
	sort.Strings(images)

	makefile, err := os.ReadFile(filepath.Join(root, "images", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^ACTIVITIES := (.*)$`).FindSubmatch(makefile)
	if m == nil {
		t.Fatal("images/Makefile has no `ACTIVITIES := …` line")
	}
	activities := strings.Fields(string(m[1]))
	sort.Strings(activities)

	for label, got := range map[string][]string{
		"dsl.BuiltinActivityCatalog()": catalog,
		"images/activity-*":            images,
		"images/Makefile ACTIVITIES":   activities,
	} {
		if !slices.Equal(got, names) {
			t.Errorf("%s = %v, localcatalog.Names = %v; every list of the first-party images must agree", label, got, names)
		}
	}
}
