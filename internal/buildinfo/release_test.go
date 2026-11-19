package buildinfo

import "testing"

// TestReleaseImage_OnlyAReleaseBuildHasOne: a stamped commit and a release
// version name the release image by its plain version; anything else has
// none.
func TestReleaseImage_OnlyAReleaseBuildHasOne(t *testing.T) {
	for _, tc := range []struct {
		version, stamped, want string
		ok                     bool
	}{
		{"v0.4.0", "abc", "ghcr.io/orkstack/orker:0.4.0", true},
		{"v10.20.30", "abc", "ghcr.io/orkstack/orker:10.20.30", true},
		{"v0.4.0", "", "", false},
		{"dev", "abc", "", false},
		{"v0.4.0-rc1", "abc", "", false},
		{"0.4.0", "abc", "", false},
		{"v01.4.0", "abc", "", false},
	} {
		got, ok := ReleaseImage("orker", tc.version, tc.stamped)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ReleaseImage(%q, %q) = %q, %v; want %q, %v", tc.version, tc.stamped, got, ok, tc.want, tc.ok)
		}
	}
}
