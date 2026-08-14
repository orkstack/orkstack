package artifacts

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
)

const (
	s3ManifestNamespace  = "manifests"
	s3NameManifestMagic  = "ORKNAME\x01"
	s3ManifestHeaderSize = len(s3NameManifestMagic) + 8
)

// s3ObjectKey keeps S3 object keys fixed in size regardless of logical-name
// length. The name digest distinguishes identical content stored under
// different names; the content digest keeps the object content-addressed.
func s3ObjectKey(name, contentDigest string) string {
	return s3ObjectKeyFromDigests(s3NameDigest(name), contentDigest)
}

func s3ObjectKeyFromDigests(nameDigest, contentDigest string) string {
	return path.Join(objectNamespace, nameDigest, contentDigest)
}

func splitS3ObjectKey(key string) (nameDigest, contentDigest string, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[0] != objectNamespace {
		return "", "", false
	}
	if !digestPattern.MatchString(parts[1]) || !digestPattern.MatchString(parts[2]) {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func s3NameDigest(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:])
}

func s3ManifestKey(nameDigest, contentDigest string) string {
	return path.Join(s3ManifestNamespace, nameDigest, contentDigest)
}

// marshalS3NameManifest stores the logical name as versioned binary data.
// Binary framing preserves empty names, NUL, and invalid UTF-8 byte-for-byte.
func marshalS3NameManifest(name string) []byte {
	manifest := make([]byte, s3ManifestHeaderSize+len(name))
	copy(manifest, s3NameManifestMagic)
	binary.BigEndian.PutUint64(manifest[len(s3NameManifestMagic):s3ManifestHeaderSize], uint64(len(name)))
	copy(manifest[s3ManifestHeaderSize:], name)
	return manifest
}

func unmarshalS3NameManifest(manifest []byte, wantDigest string) (string, error) {
	if len(manifest) < s3ManifestHeaderSize {
		return "", fmt.Errorf("manifest is shorter than its header")
	}
	if string(manifest[:len(s3NameManifestMagic)]) != s3NameManifestMagic {
		return "", fmt.Errorf("manifest has an unknown format")
	}
	nameBytes := manifest[s3ManifestHeaderSize:]
	declared := binary.BigEndian.Uint64(manifest[len(s3NameManifestMagic):s3ManifestHeaderSize])
	if declared != uint64(len(nameBytes)) {
		return "", fmt.Errorf("manifest name length is %d, payload is %d", declared, len(nameBytes))
	}
	name := string(nameBytes)
	if got := s3NameDigest(name); got != wantDigest {
		return "", fmt.Errorf("manifest name digest is %s, want %s", got, wantDigest)
	}
	return name, nil
}
