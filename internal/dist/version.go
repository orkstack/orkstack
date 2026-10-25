package dist

import (
	"fmt"
	"regexp"
	"strconv"
)

// Version is a release version, vMAJOR.MINOR.PATCH: the shape of a release tag
// (AGENTS.md §12) and of the release workflow's `version` input. A pre-release
// or build suffix is not a version here, because a release is never marked a
// pre-release: `releases/latest` skips those.
type Version struct {
	Major, Minor, Patch int
}

var versionPattern = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// ParseVersion parses "v1.2.3". It accepts nothing else: no missing part, no
// suffix, no whitespace and no leading zero, so the string it was given is
// also exactly the tag, and exactly what String returns.
func ParseVersion(s string) (Version, error) {
	m := versionPattern.FindStringSubmatch(s)
	if m == nil {
		return Version{}, fmt.Errorf("version %q is not vMAJOR.MINOR.PATCH (for example v0.1.0)", s)
	}
	var v Version
	for i, part := range []*int{&v.Major, &v.Minor, &v.Patch} {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return Version{}, fmt.Errorf("version %q is not vMAJOR.MINOR.PATCH: %w", s, err)
		}
		*part = n
	}
	return v, nil
}

// String is the version as a tag: "v1.2.3".
func (v Version) String() string {
	return fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Series is "MAJOR.MINOR", the tag a release publishes the activity images
// under: every 0.1.z release serves the built-in catalog's ":0.1".
func (v Version) Series() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// Plain is the version without its leading v, "1.2.3": how CHANGELOG.md
// headings spell it.
func (v Version) Plain() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
