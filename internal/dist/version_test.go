package dist_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
)

func TestParseVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    dist.Version
		series  string
		wantErr bool
	}{
		{in: "v0.1.0", want: dist.Version{Major: 0, Minor: 1, Patch: 0}, series: "0.1"},
		{in: "v1.2.3", want: dist.Version{Major: 1, Minor: 2, Patch: 3}, series: "1.2"},
		{in: "v10.20.30", want: dist.Version{Major: 10, Minor: 20, Patch: 30}, series: "10.20"},
		{in: "v0.0.0", want: dist.Version{}, series: "0.0"},

		{in: "", wantErr: true},
		{in: "1.2.3", wantErr: true},         // a release tag starts with v
		{in: "V1.2.3", wantErr: true},        // and the v is lower case
		{in: "v1.2", wantErr: true},          // MAJOR.MINOR.PATCH, all three
		{in: "v1.2.3.4", wantErr: true},      // and no more
		{in: "v1.2.3-rc.1", wantErr: true},   // a pre-release is never published
		{in: "v1.2.3+build5", wantErr: true}, // and build metadata is not part of a tag
		{in: " v1.2.3", wantErr: true},
		{in: "v1.2.3 ", wantErr: true},
		{in: "v1.2.3\n", wantErr: true},
		{in: "v1.2.x", wantErr: true},
		{in: "v1..3", wantErr: true},
		{in: "v-1.2.3", wantErr: true},
		{in: "v+1.2.3", wantErr: true},
		{in: "v01.2.3", wantErr: true}, // semver forbids leading zeros
		{in: "v1.02.3", wantErr: true},
		{in: "v1.2.03", wantErr: true},
		{in: "v99999999999999999999.0.0", wantErr: true}, // does not fit an int
		{in: "v1.2.3/../x", wantErr: true},               // the version names files and tags
	}
	for _, tc := range tests {
		got, err := dist.ParseVersion(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseVersion(%q) = %v, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseVersion(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseVersion(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
		if got.String() != tc.in {
			t.Errorf("String() = %q, want %q back", got.String(), tc.in)
		}
		if got.Series() != tc.series {
			t.Errorf("Series() = %q, want %q", got.Series(), tc.series)
		}
	}
}

// TestParseVersion_ErrorSaysTheShape: the refusal names the accepted shape and
// quotes what it was given.
func TestParseVersion_ErrorSaysTheShape(t *testing.T) {
	t.Parallel()
	_, err := dist.ParseVersion("1.2")
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{`"1.2"`, "vMAJOR.MINOR.PATCH"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
