package dsl

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// goldenOrkflow is the fixture the golden vector below pins. It deliberately
// carries one of every field the canonical form enumerates, so a field that
// silently stops being serialized fails the vector.
func goldenOrkflow(t *testing.T) *orkstackv1.Orkflow {
	t.Helper()
	with, err := structpb.NewStruct(map[string]any{
		"op":      "exec",
		"retries": float64(3),
		"ratio":   0.5,
		"nested":  map[string]any{"b": "2", "a": "1"},
		"list":    []any{"x", float64(1)},
	})
	if err != nil {
		t.Fatalf("with: %v", err)
	}
	inputs, err := structpb.NewStruct(map[string]any{"url": map[string]any{"type": "string"}})
	if err != nil {
		t.Fatalf("inputs: %v", err)
	}
	return &orkstackv1.Orkflow{
		Namespace:   "default",
		Name:        "golden",
		Version:     "1.0.0",
		Description: "a fixture",
		Labels:      map[string]string{"team": "platform"},
		Inputs:      inputs,
		Tasks: []*orkstackv1.Task{{
			Id:   "one",
			Uses: "shell",
			Image: &orkstackv1.ImageRef{
				Reference:   "shell",
				Registry:    "ghcr.io",
				Repository:  "orkstack/activity-shell",
				IndexDigest: "sha256:" + strings.Repeat("ab", 32),
				Pinned:      "ghcr.io/orkstack/activity-shell@sha256:" + strings.Repeat("ab", 32),
				MediaType:   MediaTypeOCIIndex,
				Platforms:   []string{"linux/amd64", "linux/arm64/v8"},
			},
			With:                           with,
			Needs:                          []string{},
			Condition:                      "",
			TimeoutSeconds:                 300,
			Retry:                          &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 2},
			Pool:                           "main",
			Command:                        []string{"sh", "-c", "echo hi"},
			Env:                            map[string]string{"B": "2", "A": "1"},
			Secrets:                        []*orkstackv1.SecretRef{{Name: "db", Source: "env:DB"}},
			Resources:                      &orkstackv1.ResourceRequest{Cpu: "2", Memory: "4Gi"},
			Network:                        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
			CancellationGracePeriodSeconds: 30,
			RunAsOrkerUser:                 true,
		}},
	}
}

// GoldenDefinitionDigest is the pinned digest of goldenOrkflow.
//
// A change here is a WIRE-VISIBLE change: definitions already stamped with the
// old digest would no longer verify. If this test fails, the question is not
// "what is the new value" but "was changing the canonical form intended".
const GoldenDefinitionDigest = "sha256:44b9f67f0b023df8d3b3039f30cb7608aa1af923e2862660e193ec3154407fe5"

func TestDefinitionDigest_GoldenVector(t *testing.T) {
	got, err := DefinitionDigest(goldenOrkflow(t))
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if got != GoldenDefinitionDigest {
		t.Fatalf("definition digest changed.\n got: %s\nwant: %s\n\n"+
			"The canonical form of §4.4 is a contract: a definition already stamped "+
			"with the old digest would no longer verify. Update the constant only if "+
			"changing the canonical form was the intent.", got, GoldenDefinitionDigest)
	}
}

func TestCanonicalOrkflow_IsValidCompactSortedJSON(t *testing.T) {
	canonical, err := CanonicalOrkflow(goldenOrkflow(t))
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	if !json.Valid(canonical) {
		t.Fatalf("canonical form is not valid JSON: %s", canonical)
	}
	if strings.ContainsAny(string(canonical), "\n\t") {
		t.Fatalf("canonical form carries insignificant whitespace: %s", canonical)
	}
	// Keys sorted by byte sequence: "description" precedes "inputs" precedes
	// "labels" precedes "name" precedes "namespace" precedes "tasks".
	order := []string{`"description"`, `"inputs"`, `"labels"`, `"name"`, `"namespace"`, `"tasks"`, `"version"`}
	at := 0
	for _, key := range order {
		next := strings.Index(string(canonical[at:]), key)
		if next < 0 {
			t.Fatalf("key %s missing or out of order in %s", key, canonical)
		}
		at += next
	}
}

// §4.4: only enumerated fields are serialized, so adding a proto field cannot
// silently change an existing digest. source_yaml and created_at are the two
// the spec names explicitly.
func TestCanonicalOrkflow_IgnoresSourceAndCreatedAt(t *testing.T) {
	base := goldenOrkflow(t)
	before, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	base.SourceYaml = "name: golden\n# a comment that changes nothing about what runs\n"
	base.CreatedAt = timestamppb.Now()
	after, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if before != after {
		t.Fatalf("source_yaml or created_at changed the definition digest: %s -> %s", before, after)
	}
}

// The per-execution ImageRef fields are facts an Orker records, never part of
// a definition. Hashing them would make a definition's digest depend on a
// value it can never carry.
func TestCanonicalOrkflow_IgnoresPerExecutionImageFields(t *testing.T) {
	base := goldenOrkflow(t)
	before, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	base.Tasks[0].Image.PlatformDigest = "sha256:" + strings.Repeat("cd", 32)
	base.Tasks[0].Image.Platform = "linux/amd64"
	base.Tasks[0].Image.Delivery = "embedded"
	after, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if before != after {
		t.Fatalf("platform_digest/platform/delivery changed the digest: %s -> %s", before, after)
	}
}

// A moved tag is a different definition. This is the property §4.4 exists for.
func TestDefinitionDigest_ChangesWhenTheImageDigestMoves(t *testing.T) {
	base := goldenOrkflow(t)
	before, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	base.Tasks[0].Image.IndexDigest = "sha256:" + strings.Repeat("ef", 32)
	after, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if before == after {
		t.Fatal("a moved index digest produced the same definition digest")
	}
}

// platforms is hashed in the index's own order (§4.3 step 4), so reordering it
// is a different definition rather than the same one spelled differently.
func TestDefinitionDigest_PlatformOrderIsPartOfTheContract(t *testing.T) {
	base := goldenOrkflow(t)
	before, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	base.Tasks[0].Image.Platforms = []string{"linux/arm64/v8", "linux/amd64"}
	after, err := DefinitionDigest(base)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if before == after {
		t.Fatal("reordering platforms did not change the digest, but the order is hashed")
	}
}

func TestCanonicalOrkflow_IsStableAcrossRepeatedMarshals(t *testing.T) {
	// Map iteration order is randomized per range in Go, so a serializer that
	// leaked it would fail here rather than intermittently in production.
	wf := goldenOrkflow(t)
	first, err := CanonicalOrkflow(wf)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	for i := 0; i < 200; i++ {
		again, err := CanonicalOrkflow(wf)
		if err != nil {
			t.Fatalf("canonical: %v", err)
		}
		if string(again) != string(first) {
			t.Fatalf("canonical form is not stable at iteration %d:\n%s\n%s", i, first, again)
		}
	}
}

// §4.4: "integers rendered without exponent or fraction". structpb holds every
// number as a float64, so this is the one place Go's default formatting is
// actively wrong.
func TestCanonicalNumber_RendersWithoutExponentOrFraction(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{8080, "8080"},
		{-5, "-5"},
		{1e21, "1000000000000000000000"},
		{0.5, "0.5"},
		{1.5e-7, "0.00000015"},
	} {
		encoded, err := canonicalJSON(canonicalNumber(tc.in))
		if err != nil {
			t.Fatalf("%v: %v", tc.in, err)
		}
		if string(encoded) != tc.want {
			t.Fatalf("canonicalNumber(%v) = %s, want %s", tc.in, encoded, tc.want)
		}
		if strings.ContainsAny(string(encoded), "eE") {
			t.Fatalf("canonicalNumber(%v) = %s carries an exponent", tc.in, encoded)
		}
	}
}

func TestDerivedVersion_CoversBothSourceAndResolution(t *testing.T) {
	source := []byte("name: golden\ntasks: []\n")
	wf := goldenOrkflow(t)

	base, err := DerivedVersion(source, wf)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(base) != 64 {
		t.Fatalf("derived version %q is not a hex sha256", base)
	}

	t.Run("a comment-only edit still mints a new version", func(t *testing.T) {
		// hash.go settled this: the source bytes are hashed in full, so a
		// cosmetic edit is a version. Two notions of "the same document" in
		// one system is one too many.
		got, err := DerivedVersion([]byte("name: golden\ntasks: []\n# a comment\n"), wf)
		if err != nil {
			t.Fatalf("derive: %v", err)
		}
		if got == base {
			t.Fatal("a comment-only edit derived the same version")
		}
	})

	t.Run("a moved tag mints a new version for identical source", func(t *testing.T) {
		moved := goldenOrkflow(t)
		moved.Tasks[0].Image.IndexDigest = "sha256:" + strings.Repeat("ef", 32)
		got, err := DerivedVersion(source, moved)
		if err != nil {
			t.Fatalf("derive: %v", err)
		}
		if got == base {
			t.Fatal("a moved tag derived the same version for the same source — " +
				"this is the defect §4.4(b) exists to fix")
		}
	})

	t.Run("identical source and identical resolution is idempotent", func(t *testing.T) {
		got, err := DerivedVersion(source, goldenOrkflow(t))
		if err != nil {
			t.Fatalf("derive: %v", err)
		}
		if got != base {
			t.Fatalf("the same document resolved the same way derived %q then %q", base, got)
		}
	})
}
