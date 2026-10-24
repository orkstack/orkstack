package artifacts

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"
)

// runProvenance is a realistic set of run coordinates: orkflow names and task
// ids are slugs (internal/dsl), run ids are ork-{timestamp}-{sequence}.
var runProvenance = Provenance{Orkflow: "hello-ork", TaskID: "fetch", RunID: "ork-1755302400123456789-1"}

func TestS3ObjectKey_NamesTheOrkflowTaskAndRunThatProducedTheArtifact(t *testing.T) {
	cases := []struct {
		label string
		prov  Provenance
		name  string
		want  string
	}{
		{
			label: "example run",
			prov:  runProvenance,
			name:  "body",
			want:  "orkflows/hello-ork/ork-1755302400123456789-1/fetch/body/" + strings.Repeat("0", 64),
		},
		{
			label: "file name with extension",
			prov:  Provenance{Orkflow: "nightly-etl", TaskID: "save", RunID: "ork-42-7"},
			name:  "result.txt",
			want:  "orkflows/nightly-etl/ork-42-7/save/result.txt/" + strings.Repeat("0", 64),
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got := s3ObjectKey(tc.prov, tc.name, strings.Repeat("0", 64))
			if got != tc.want {
				t.Errorf("s3ObjectKey = %q, want %q", got, tc.want)
			}
			if _, ok := splitS3ObjectKey(got); !ok {
				t.Errorf("splitS3ObjectKey(%q) rejected a key this store generated", got)
			}
		})
	}
}

// An S3 lifecycle rule selects objects by key prefix, so every artifact of one
// orkflow must sit under that orkflow's prefix and nothing else may.
func TestS3ObjectKey_PutsOneOrkflowUnderOneLifecyclePrefix(t *testing.T) {
	const deprecated = "orkflows/deprecated-flow/"

	doomed := []string{
		s3ObjectKey(Provenance{Orkflow: "deprecated-flow", TaskID: "fetch", RunID: "ork-1-1"}, "body", strings.Repeat("0", 64)),
		s3ObjectKey(Provenance{Orkflow: "deprecated-flow", TaskID: "save", RunID: "ork-1-1"}, "result.txt", strings.Repeat("0", 64)),
		s3ObjectKey(Provenance{Orkflow: "deprecated-flow", TaskID: "save", RunID: "ork-2-1"}, "result.txt", strings.Repeat("0", 64)),
		s3ObjectKey(Provenance{Orkflow: "deprecated-flow", TaskID: "save", RunID: "ork-3-1"}, strings.Repeat("x", 5000), strings.Repeat("0", 64)),
	}
	kept := []string{
		s3ObjectKey(Provenance{Orkflow: "kept-flow", TaskID: "save", RunID: "ork-1-1"}, "result.txt", strings.Repeat("0", 64)),
		s3ObjectKey(Provenance{Orkflow: "deprecated-flow-2", TaskID: "save", RunID: "ork-1-1"}, "result.txt", strings.Repeat("0", 64)),
		s3ObjectKey(Provenance{}, "result.txt", strings.Repeat("0", 64)),
	}

	for _, key := range doomed {
		if !strings.HasPrefix(key, deprecated) {
			t.Errorf("key %q is outside the lifecycle prefix %q", key, deprecated)
		}
	}
	for _, key := range kept {
		if strings.HasPrefix(key, deprecated) {
			t.Errorf("key %q would be deleted by the lifecycle prefix %q", key, deprecated)
		}
	}
}

// A run prefix and a task prefix must select their own slice of a run too, so
// lifecycle rules can be narrower than a whole orkflow.
func TestS3ObjectKey_NestsRunsInsideTasksInsideOrkflows(t *testing.T) {
	key := s3ObjectKey(runProvenance, "body", strings.Repeat("0", 64))
	for _, prefix := range []string{
		"orkflows/",
		"orkflows/hello-ork/",
		"orkflows/hello-ork/ork-1755302400123456789-1/",
		"orkflows/hello-ork/ork-1755302400123456789-1/fetch/",
	} {
		if !strings.HasPrefix(key, prefix) {
			t.Errorf("key %q does not nest under %q", key, prefix)
		}
	}
	segments, ok := splitS3ObjectKey(key)
	if !ok || len(segments) != s3ObjectKeySegments {
		t.Fatalf("splitS3ObjectKey(%q) = %v, %t, want %d segments", key, segments, ok, s3ObjectKeySegments)
	}
	if last := segments[len(segments)-2]; last != "body" {
		t.Errorf("last segment = %q, want the artifact name %q", last, "body")
	}
}

func TestS3ObjectKey_StaysBoundedAndDistinctForHostileComponents(t *testing.T) {
	// 8 + 4*(1+56) = 236: the longest key this layout can generate.
	const maxKey = len(s3OrkflowNamespace) + 4*(1+s3MaxComponent) + 1 + 64

	names := []string{
		"",
		".",
		"..",
		"a//b",
		"a/./b",
		"a/../b",
		"/abs",
		"nul\x00name",
		string([]byte{0xff, 0xfe, 'x'}),
		strings.Repeat("x", 100_000),
		strings.Repeat("x", 100_001),
		strings.Repeat("long-name/", 410),
		"report/../../etc/passwd",
	}

	seen := map[string]string{}
	for _, name := range names {
		key := s3ObjectKey(runProvenance, name, strings.Repeat("0", 64))
		if len(key) > maxKey {
			t.Errorf("key for a %d-byte name is %d bytes, want at most %d", len(name), len(key), maxKey)
		}
		if _, ok := splitS3ObjectKey(key); !ok {
			t.Errorf("splitS3ObjectKey(%q) rejected a generated key", key)
		}
		if previous, clash := seen[key]; clash {
			t.Errorf("names %q and %q share key %q", previous, name, key)
		}
		seen[key] = name
	}

	// A sanitized segment still reads: it keeps the head of the name.
	hostile := s3ObjectKey(runProvenance, "report/../../etc/passwd", strings.Repeat("0", 64))
	segments, _ := splitS3ObjectKey(hostile)
	artifact := segments[len(segments)-2]
	if !strings.HasPrefix(artifact, "report-") || !strings.Contains(artifact, s3ComponentDigestMarker) {
		t.Errorf("sanitized artifact segment = %q, want a readable head plus a digest", artifact)
	}
	if strings.Contains(hostile, "/../") {
		t.Errorf("key %q escaped its own hierarchy", hostile)
	}
}

func TestS3ObjectKey_FilesPutsWithoutProvenanceUnderTheReservedPrefix(t *testing.T) {
	key := s3ObjectKey(Provenance{}, "loose.txt", strings.Repeat("0", 64))
	want := "orkflows/_unattributed/_unattributed/_unattributed/loose.txt/" + strings.Repeat("0", 64)
	if key != want {
		t.Errorf("s3ObjectKey without provenance = %q, want %q", key, want)
	}
	// The reserved segment starts with "_", which internal/dsl's slug rule
	// (^[a-z][a-z0-9-]*$) forbids, so a real orkflow can never land here and a
	// lifecycle rule for an orkflow can never sweep these up.
	if s3KeyComponent(s3UnattributedComponent) == s3UnattributedComponent {
		t.Errorf("a caller could reach the reserved segment %q verbatim", s3UnattributedComponent)
	}
	// A partially attributed put keeps the coordinates it does have.
	partial := s3ObjectKey(Provenance{Orkflow: "hello-ork", TaskID: "fetch"}, "body", strings.Repeat("0", 64))
	if partial != "orkflows/hello-ork/_unattributed/fetch/body/"+strings.Repeat("0", 64) {
		t.Errorf("partial provenance key = %q, want the known coordinates kept", partial)
	}
}

func TestSplitS3ObjectKey_RejectsKeysThisStoreNeverWrote(t *testing.T) {
	digest := strings.Repeat("a", 64)
	cases := []struct {
		label string
		key   string
	}{
		{label: "content-hash layout this store never writes", key: "objects/" + digest + "/" + digest},
		{label: "manifest namespace", key: "manifests/" + digest},
		{label: "too few segments", key: "orkflows/hello-ork/fetch/body"},
		{label: "too many segments", key: "orkflows/hello-ork/fetch/ork-1-1/dir/body"},
		{label: "empty segment", key: "orkflows/hello-ork//ork-1-1/body"},
		{label: "dot segment", key: "orkflows/hello-ork/./ork-1-1/body"},
		{label: "parent segment", key: "orkflows/hello-ork/../ork-1-1/body"},
		{label: "empty", key: ""},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			if _, ok := splitS3ObjectKey(tc.key); ok {
				t.Errorf("splitS3ObjectKey(%q) accepted a foreign key", tc.key)
			}
		})
	}
}

// A readable key is its own record: the artifact segment of a clean name IS the
// name, so the store reads it back without any companion object.
func TestS3NameFromKey_ReadsCleanNamesStraightOffTheKey(t *testing.T) {
	for _, name := range []string{"body", "result.txt", "a-b_c.9", "TITLE"} {
		segments, ok := splitS3ObjectKey(s3ObjectKey(runProvenance, name, strings.Repeat("0", 64)))
		if !ok {
			t.Fatalf("generated key for %q has a foreign shape", name)
		}
		got, exact := s3NameFromKey(segments)
		if !exact || got != name {
			t.Errorf("s3NameFromKey(%q) = %q, %t, want %q, true", segments, got, exact, name)
		}
	}
	// A name the key cannot hold verbatim must fall back to its manifest.
	for _, name := range []string{"", "a/b", "nul\x00name", strings.Repeat("x", 100)} {
		segments, ok := splitS3ObjectKey(s3ObjectKey(runProvenance, name, strings.Repeat("0", 64)))
		if !ok {
			t.Fatalf("generated key for a %d-byte name has a foreign shape", len(name))
		}
		if got, exact := s3NameFromKey(segments); exact {
			t.Errorf("s3NameFromKey read %q off a sanitized segment for a %d-byte name", got, len(name))
		}
	}
}

func TestS3NameManifest_PreservesOpaqueNames(t *testing.T) {
	cases := []struct {
		label string
		name  string
	}{
		{label: "empty", name: ""},
		{label: "NUL", name: "nul\x00name"},
		{label: "invalid UTF-8", name: string([]byte{0xff, 0xfe, 'x'})},
		{label: "very long", name: strings.Repeat("long-name/", 410)},
		{label: "clean", name: "result.txt"},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			key := s3ObjectKey(runProvenance, tc.name, strings.Repeat("0", 64))
			got, err := unmarshalS3NameManifest(marshalS3NameManifest(key, tc.name), key)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got != tc.name {
				t.Errorf("name changed: got %d bytes exact=%t", len(got), got == tc.name)
			}
			if manifestKey := s3ManifestKey(key); len(manifestKey) > 256 {
				t.Errorf("manifest key length = %d, want at most 256", len(manifestKey))
			}
		})
	}
}

func TestS3NameManifest_RejectsCorruption(t *testing.T) {
	name := "opaque\x00name"
	key := s3ObjectKey(runProvenance, name, strings.Repeat("0", 64))
	valid := marshalS3NameManifest(key, name)

	badMagic := append([]byte(nil), valid...)
	badMagic[0] ^= 0xff
	badBinding := append([]byte(nil), valid...)
	badBinding[len(s3NameManifestMagic)] ^= 0xff
	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(
		badLength[len(s3NameManifestMagic)+sha256.Size:s3ManifestHeaderSize], uint64(len(name)+1))

	cases := []struct {
		label    string
		manifest []byte
		key      string
	}{
		{label: "short header", manifest: valid[:s3ManifestHeaderSize-1], key: key},
		{label: "bad magic", manifest: badMagic, key: key},
		{label: "bad key binding", manifest: badBinding, key: key},
		{label: "bad length", manifest: badLength, key: key},
		{label: "manifest of another object", manifest: valid, key: s3ObjectKey(runProvenance, "other", strings.Repeat("0", 64))},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			if _, err := unmarshalS3NameManifest(tc.manifest, tc.key); err == nil {
				t.Fatal("unmarshal returned nil error")
			}
		})
	}
}

func TestProvenanceMetadata_CarriesCoordinatesAndOutranksCallerMetadata(t *testing.T) {
	meta := runProvenance.Metadata(map[string]string{
		"origin":    "caller",
		MetaOrkflow: "spoofed-flow",
		MetaTaskID:  "spoofed-task",
		MetaRunID:   "spoofed-run",
	})
	if meta["origin"] != "caller" {
		t.Errorf("caller metadata lost: %#v", meta)
	}
	if got := ProvenanceFromMetadata(meta); got != runProvenance {
		t.Errorf("ProvenanceFromMetadata = %+v, want %+v", got, runProvenance)
	}
	if key := s3ObjectKey(ProvenanceFromMetadata(meta), "body", strings.Repeat("0", 64)); key != s3ObjectKey(runProvenance, "body", strings.Repeat("0", 64)) {
		t.Errorf("caller metadata moved the object to %q", key)
	}

	// S3 servers normalize user-metadata header names, so reading back is
	// case-insensitive.
	upper := map[string]string{"Orkflow": "hello-ork", "Task_Id": "fetch", "Run_Id": "ork-1-1"}
	if got := ProvenanceFromMetadata(upper); got != (Provenance{Orkflow: "hello-ork", TaskID: "fetch", RunID: "ork-1-1"}) {
		t.Errorf("ProvenanceFromMetadata(%v) = %+v, want the same coordinates", upper, got)
	}

	// Empty coordinates never write empty keys.
	if _, ok := (Provenance{}).Metadata(map[string]string{MetaRunID: "ork-1-1"})[MetaRunID]; ok {
		t.Error("empty provenance kept a stale run id")
	}
}
