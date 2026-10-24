package artifacts

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"strings"
)

// S3 object keys are a human-readable hierarchy, so a key names the run that
// produced it:
//
//	orkflows/{orkflow-name}/{run-id}/{task-id}/{artifact-name}/{sha256}
//
// Every part is exactly one path segment. That gives two things the previous
// content-hash layout could not: a URI a person can read, and an S3 lifecycle
// rule that selects one orkflow (prefix `orkflows/<name>/`), one run, or one
// task within a run.
//
// Orkflow names and task ids are slugs (internal/dsl), run ids are
// `ork-{timestamp}-{sequence}`, and artifact names are usually file names, so
// in practice every segment is used verbatim. A segment that is not already
// safe and short keeps a readable head plus a digest of its exact bytes, which
// bounds the key and keeps distinct values apart. The exact logical name is
// restored from the binary companion manifest, never from the key.

const (
	// s3OrkflowNamespace roots the object hierarchy.
	s3OrkflowNamespace = "orkflows"
	// s3ObjectKeySegments is the fixed segment count of every object key:
	// namespace, orkflow, run, task, artifact, content digest.
	s3ObjectKeySegments = 6
	// s3ManifestNamespace holds the binary companion that restores an object's
	// exact logical name.
	s3ManifestNamespace = "manifests"
	// s3UnattributedComponent stands in for provenance the caller did not
	// supply (a direct library Put outside a run). A slug cannot start with
	// "_", so a real orkflow, task or run never lands under it and a lifecycle
	// rule for an orkflow never selects it.
	s3UnattributedComponent = "_unattributed"
	// s3MaxComponent bounds every generated segment, so a key stays bounded no
	// matter how long a logical name is:
	// len("orkflows") + 4*(1+s3MaxComponent) = 8 + 4*57 = 236 bytes,
	// far below S3's 1024-byte key limit.
	s3MaxComponent = 56
	// s3ComponentDigestLen is how many hex characters of a segment's sha256
	// keep sanitized segments distinct (64 bits).
	s3ComponentDigestLen = 16
	// s3ComponentDigestMarker joins a readable head to that digest. A clean
	// segment cannot contain it, so the two forms can never collide.
	s3ComponentDigestMarker = "~"
)

// cleanComponentPattern matches a segment that is already readable, safe in an
// S3 key and in a URL, and distinguishable from a sanitized segment. A leading
// alphanumeric also rules out "." and ".." and hidden-file segments.
var cleanComponentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// s3ObjectKey renders the physical key for an artifact produced by p.
func s3ObjectKey(p Provenance, name, digest string) string {
	return path.Join(
		s3OrkflowNamespace,
		s3ProvenanceComponent(p.Orkflow),
		s3ProvenanceComponent(p.RunID),
		s3ProvenanceComponent(p.TaskID),
		s3KeyComponent(name),
		digest,
	)
}

// s3ProvenanceComponent renders one provenance segment, falling back to the
// reserved segment when the caller supplied none.
func s3ProvenanceComponent(raw string) string {
	if raw == "" {
		return s3UnattributedComponent
	}
	return s3KeyComponent(raw)
}

// s3KeyComponent renders raw as a single readable, bounded segment. A clean
// value is used verbatim; anything else keeps a readable head and a digest of
// the exact bytes, so two different values never share a segment.
func s3KeyComponent(raw string) string {
	if isCleanS3Component(raw) {
		return raw
	}
	digest := sha256.Sum256([]byte(raw))
	suffix := s3ComponentDigestMarker + hex.EncodeToString(digest[:])[:s3ComponentDigestLen]
	return sanitizeS3Component(raw, s3MaxComponent-len(suffix)) + suffix
}

func isCleanS3Component(raw string) bool {
	return len(raw) <= s3MaxComponent && cleanComponentPattern.MatchString(raw)
}

// sanitizeS3Component keeps the readable head of raw within budget: safe bytes
// survive, every other byte becomes '-', and leading punctuation is dropped so
// the segment still starts with something meaningful.
func sanitizeS3Component(raw string, budget int) string {
	var head strings.Builder
	for i := 0; i < len(raw) && head.Len() < budget; i++ {
		c := raw[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			head.WriteByte(c)
		default:
			head.WriteByte('-')
		}
	}
	return strings.TrimLeft(head.String(), ".-_")
}

// splitS3ObjectKey reports whether key has the six-segment shape this store
// generates and returns namespace, orkflow, run, task, artifact and digest.
func splitS3ObjectKey(key string) (segments []string, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) != s3ObjectKeySegments || parts[0] != s3OrkflowNamespace {
		return nil, false
	}
	for _, part := range parts[1:] {
		if part == "" || part == "." || part == ".." {
			return nil, false
		}
	}
	if !digestPattern.MatchString(parts[len(parts)-1]) {
		return nil, false
	}
	return parts, true
}

// s3NameFromKey recovers the exact logical name from an object key's artifact
// segment. A clean segment IS the name, byte for byte, so the common case needs
// no companion object at all; a sanitized segment (it carries the digest
// marker) only says which manifest to read.
func s3NameFromKey(segments []string) (name string, exact bool) {
	artifact := segments[len(segments)-2]
	if !isCleanS3Component(artifact) {
		return "", false
	}
	return artifact, true
}

// s3ManifestKey addresses the companion manifest of one object key. Manifests
// mirror the object hierarchy, so the same orkflow, run or task prefix that
// selects artifacts for a lifecycle rule selects their manifests too, and
// nothing is left behind when an orkflow is expired.
func s3ManifestKey(objectKey string) string {
	return path.Join(s3ManifestNamespace, objectKey)
}

const (
	s3NameManifestMagic  = "ORKNAME\x02"
	s3ManifestHeaderSize = len(s3NameManifestMagic) + sha256.Size + 8
)

// marshalS3NameManifest stores the exact logical name as versioned binary data
// bound to its object key. Binary framing preserves empty names, NUL and
// invalid UTF-8 byte for byte; the embedded key digest makes a manifest that
// was misfiled or truncated detectable.
func marshalS3NameManifest(objectKey, name string) []byte {
	manifest := make([]byte, s3ManifestHeaderSize+len(name))
	n := copy(manifest, s3NameManifestMagic)
	keyDigest := sha256.Sum256([]byte(objectKey))
	n += copy(manifest[n:], keyDigest[:])
	binary.BigEndian.PutUint64(manifest[n:s3ManifestHeaderSize], uint64(len(name)))
	copy(manifest[s3ManifestHeaderSize:], name)
	return manifest
}

func unmarshalS3NameManifest(manifest []byte, objectKey string) (string, error) {
	if len(manifest) < s3ManifestHeaderSize {
		return "", fmt.Errorf("manifest is shorter than its header")
	}
	if string(manifest[:len(s3NameManifestMagic)]) != s3NameManifestMagic {
		return "", fmt.Errorf("manifest has an unknown format")
	}
	keyDigest := sha256.Sum256([]byte(objectKey))
	if !bytes.Equal(manifest[len(s3NameManifestMagic):len(s3NameManifestMagic)+sha256.Size], keyDigest[:]) {
		return "", fmt.Errorf("manifest belongs to another object key")
	}
	nameBytes := manifest[s3ManifestHeaderSize:]
	declared := binary.BigEndian.Uint64(manifest[len(s3NameManifestMagic)+sha256.Size : s3ManifestHeaderSize])
	if declared != uint64(len(nameBytes)) {
		return "", fmt.Errorf("manifest name length is %d, payload is %d", declared, len(nameBytes))
	}
	return string(nameBytes), nil
}
