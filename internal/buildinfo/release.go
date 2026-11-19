package buildinfo

import (
	"regexp"
	"strings"
)

// ReleaseVersionPattern is a release version, vMAJOR.MINOR.PATCH: the shape
// of a release tag (AGENTS.md §12). internal/dist parses versions with it, so
// what a release stamps and what a binary takes for a release are one rule.
var ReleaseVersionPattern = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// ReleaseRegistry is where every release image is published:
// ghcr.io/orkstack/<name>. internal/dist holds it to the namespace a release
// pushes to (TestReleaseImage_MatchesWhatAReleasePublishes).
const ReleaseRegistry = "ghcr.io/orkstack"

// IsReleaseBuild reports whether a binary was built by a release: its stamped
// version is a release version and its commit was stamped, as
// internal/dist's build stamps both. A development build, `make build` or a
// source archive's build is not one.
func IsReleaseBuild(version, stamped string) bool {
	return stamped != "" && ReleaseVersionPattern.MatchString(version)
}

// ReleaseImage is the image a release publishes for name, tagged with the
// release's plain version ("ghcr.io/orkstack/orker:0.4.0"), and whether the
// build is a release build that has one.
func ReleaseImage(name, version, stamped string) (string, bool) {
	if !IsReleaseBuild(version, stamped) {
		return "", false
	}
	return ReleaseRegistry + "/" + name + ":" + strings.TrimPrefix(version, "v"), true
}
