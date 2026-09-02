package orker

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// safeComponent turns an opaque logical artifact name into a single safe path
// component.
//
// The staging path it produces is pinned by tests (§6.2): a change here
// changes where every task finds its inputs.
//
// It is deliberately LOSSY, and that is why task.json carries `nameB64`
// alongside `path` (§3.2). Logical names may contain separators, dot segments
// or 200 bytes of Unicode — all valid UTF-8, none representable as a path
// segment — so any rewritten name carries a digest suffix to stay unique, and
// a workload that needs the exact name reads nameB64 rather than trying to
// reverse this.
func safeComponent(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.' || r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	cleaned := b.String()
	if len(cleaned) > 64 {
		cleaned = cleaned[:64]
	}
	if cleaned == name && cleaned != "." && cleaned != ".." && cleaned != "" {
		return cleaned
	}
	sum := sha256.Sum256([]byte(name))
	if cleaned == "" || cleaned == "." || cleaned == ".." {
		cleaned = "artifact"
	}
	return cleaned + "-" + hex.EncodeToString(sum[:])[:8]
}

func joinOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}
