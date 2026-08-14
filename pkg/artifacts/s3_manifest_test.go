package artifacts

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestS3NameManifest_PreservesOpaqueNames(t *testing.T) {
	cases := []struct {
		label string
		name  string
	}{
		{label: "empty", name: ""},
		{label: "NUL", name: "nul\x00name"},
		{label: "invalid UTF-8", name: string([]byte{0xff, 0xfe, 'x'})},
		{label: "very long", name: strings.Repeat("long-name/", 410)},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			manifest := marshalS3NameManifest(tc.name)
			got, err := unmarshalS3NameManifest(manifest, s3NameDigest(tc.name))
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got != tc.name {
				t.Errorf("name changed: got %d bytes exact=%t", len(got), got == tc.name)
			}
		})
	}
}

func TestS3ObjectKey_IsBoundedAndReversibleByManifest(t *testing.T) {
	name := strings.Repeat("x", 100_000)
	contentDigest := strings.Repeat("a", 64)
	key := s3ObjectKey(name, contentDigest)
	if len(key) > 256 {
		t.Fatalf("key length = %d, want at most 256", len(key))
	}
	nameDigest, gotContentDigest, ok := splitS3ObjectKey(key)
	if !ok {
		t.Fatalf("splitS3ObjectKey(%q) failed", key)
	}
	manifestKey := s3ManifestKey(nameDigest, contentDigest)
	if len(manifestKey) > 256 {
		t.Fatalf("manifest key length = %d, want at most 256", len(manifestKey))
	}
	if nameDigest != s3NameDigest(name) || gotContentDigest != contentDigest {
		t.Errorf("split = %s/%s, want %s/%s", nameDigest, gotContentDigest, s3NameDigest(name), contentDigest)
	}
	gotName, err := unmarshalS3NameManifest(marshalS3NameManifest(name), nameDigest)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if gotName != name {
		t.Error("manifest did not restore the exact name")
	}
}

func TestS3NameManifest_RejectsCorruption(t *testing.T) {
	name := "opaque\x00name"
	wantDigest := s3NameDigest(name)
	valid := marshalS3NameManifest(name)

	badMagic := append([]byte(nil), valid...)
	badMagic[0] ^= 0xff
	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(badLength[len(s3NameManifestMagic):s3ManifestHeaderSize], uint64(len(name)+1))

	cases := []struct {
		label    string
		manifest []byte
		digest   string
	}{
		{label: "short header", manifest: valid[:s3ManifestHeaderSize-1], digest: wantDigest},
		{label: "bad magic", manifest: badMagic, digest: wantDigest},
		{label: "bad length", manifest: badLength, digest: wantDigest},
		{label: "wrong name digest", manifest: valid, digest: strings.Repeat("0", 64)},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			if _, err := unmarshalS3NameManifest(tc.manifest, tc.digest); err == nil {
				t.Fatal("unmarshal returned nil error")
			}
		})
	}
}
