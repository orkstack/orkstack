package evidence

import (
	"encoding/base64"
	"regexp"
	"strings"
)

// memberNameMaxClean bounds a verbatim name, and is the encoded form's chunk
// width.
const memberNameMaxClean = 200

// encodedMarker starts every encoded name. It is outside the clean alphabet,
// so a clean name can never start with it and the two forms never collide.
const encodedMarker = "~"

var cleanMemberName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

// MemberName renders an artifact's recorded name as one safe path segment.
// A name that is already a clean component — ^[A-Za-z0-9._-]{1,200}$ and
// neither "." nor ".." — is used verbatim, as encodeArchiveSegment
// (pkg/artifacts/archive.go) uses a clean segment verbatim. Anything else
// is "~" followed by RawURLEncoding base64 of the name's bytes, chunked at
// 200 characters with "~" between chunks, on the marker rule the same
// function uses: a clean name cannot start with "~", so the two forms never
// collide. The manifest's Member.Name always carries the exact name; Path is
// derived and lossy, exactly as task.json's `path` is beside its `name`.
func MemberName(name string) string {
	if name != "." && name != ".." && cleanMemberName.MatchString(name) {
		return name
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte(name))
	var out strings.Builder
	out.WriteString(encodedMarker)
	for len(encoded) > memberNameMaxClean {
		out.WriteString(encoded[:memberNameMaxClean])
		out.WriteString(encodedMarker)
		encoded = encoded[memberNameMaxClean:]
	}
	out.WriteString(encoded)
	return out.String()
}
