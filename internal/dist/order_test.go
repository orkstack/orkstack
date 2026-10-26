package dist_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
)

// TestReleaseOrder: a release is the latest only when no release tag is
// above it, and a release below a tag of its own series is refused, because it
// would move that series' image tag back to an older build.
func TestReleaseOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		version string
		tags    []string
		latest  bool
		refused string
	}{
		{version: "v0.1.0", tags: nil, latest: true},
		{version: "v0.1.1", tags: []string{"v0.1.0"}, latest: true},
		{version: "v0.2.0", tags: []string{"v0.1.0", "v0.1.9"}, latest: true},
		// A backport to an older series is a release, never the latest one.
		{version: "v0.1.3", tags: []string{"v0.1.2", "v0.2.0"}, latest: false},
		// v0.10 is above v0.9: versions compare as numbers, not strings.
		{version: "v0.10.0", tags: []string{"v0.9.0"}, latest: true},
		{version: "v0.9.1", tags: []string{"v0.10.0", "v0.9.0"}, latest: false},
		{version: "v1.0.0", tags: []string{"v0.99.99"}, latest: true},
		// The version's own tag, from an earlier dispatch, changes nothing.
		{version: "v0.1.1", tags: []string{"v0.1.0", "v0.1.1"}, latest: true},
		// Tags that are not release versions are not releases.
		{version: "v0.1.0", tags: []string{"v9.0.0-rc.1", "nightly", "", "v1.2"}, latest: true},
		{version: "v0.1.2", tags: []string{"v0.1.3"}, refused: "v0.1.3"},
		{version: "v0.1.0", tags: []string{"v0.1.1", "v0.2.0"}, refused: "v0.1.1"},
	} {
		version, err := dist.ParseVersion(tc.version)
		if err != nil {
			t.Fatal(err)
		}
		latest, err := dist.ReleaseOrder(version, tc.tags)
		if tc.refused != "" {
			if err == nil || !strings.Contains(err.Error(), tc.refused) || !strings.Contains(err.Error(), ":"+version.Series()) {
				t.Errorf("%s after %q: %v, want a refusal naming %s and :%s", tc.version, tc.tags, err, tc.refused, version.Series())
			}
			continue
		}
		if err != nil || latest != tc.latest {
			t.Errorf("%s after %q: latest %v, %v; want %v", tc.version, tc.tags, latest, err, tc.latest)
		}
	}
}
