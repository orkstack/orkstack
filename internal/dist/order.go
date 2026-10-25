package dist

import "fmt"

// less reports whether v is below w.
func (v Version) less(w Version) bool {
	if v.Major != w.Major {
		return v.Major < w.Major
	}
	if v.Minor != w.Minor {
		return v.Minor < w.Minor
	}
	return v.Patch < w.Patch
}

// ReleaseOrder decides where version stands among the repository's existing
// tags. It is the latest release when no release tag is above it, which is
// what the GitHub release marks and what .../releases/latest/download/ serves.
// A backport to an older series is released, but never the latest. A version
// below a tag of its own series is refused: publishing it would move the
// series' image tag (:MAJOR.MINOR) back to an older build. Tags that are not
// vMAJOR.MINOR.PATCH are not releases and are ignored.
func ReleaseOrder(version Version, tags []string) (latest bool, err error) {
	latest = true
	for _, tag := range tags {
		other, parseErr := ParseVersion(tag)
		if parseErr != nil || !version.less(other) {
			continue
		}
		if other.Major == version.Major && other.Minor == version.Minor {
			return false, fmt.Errorf("%s is below %s, already released: publishing it would move :%s back to an older build",
				version, other, version.Series())
		}
		latest = false
	}
	return latest, nil
}
