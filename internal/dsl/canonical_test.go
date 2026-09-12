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

// pinnedOrkflow is goldenOrkflow with a pin declared and both tasks joined to
// it. It is built by hand rather than compiled, because `pins:`/`pin:` have no
// DSL support yet (Phase 2); this file only pins the wire and the digest.
func pinnedOrkflow(t *testing.T) *orkstackv1.Orkflow {
	t.Helper()
	wf := goldenOrkflow(t)
	wf.Name = "golden-pinned"
	wf.Pins = []*orkstackv1.Pin{{Name: "build", TimeoutSeconds: 3600}}
	wf.Tasks[0].Id = "clone"
	wf.Tasks[0].Pin = "build"
	compile := &orkstackv1.Task{
		Id:             "compile",
		Uses:           "shell",
		Image:          wf.Tasks[0].GetImage(),
		Needs:          []string{"clone"},
		TimeoutSeconds: 300,
		Pool:           "main",
		Pin:            "build",
		Command:        []string{"sh", "-c", "true"},
		RunAsOrkerUser: true,
	}
	wf.Tasks = append(wf.Tasks, compile)
	return wf
}

// GoldenPinnedDefinitionDigest is the pinned digest of pinnedOrkflow, frozen
// the day pins landed on the wire. It carries the same contract
// GoldenDefinitionDigest does: a change here is wire-visible.
const GoldenPinnedDefinitionDigest = "sha256:16f3d7f278d4632d31f9a7a682f6045972efb48d6e4457f4e24fc3e6f3f74c7c"

func TestDefinitionDigest_GoldenPinnedVector(t *testing.T) {
	got, err := DefinitionDigest(pinnedOrkflow(t))
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if got != GoldenPinnedDefinitionDigest {
		t.Fatalf("pinned definition digest changed.\n got: %s\nwant: %s\n\n"+
			"Pins are part of the canonical form (design §5.4). Update the "+
			"constant only if changing the canonical form was the intent.",
			got, GoldenPinnedDefinitionDigest)
	}
}

// The canonical form of a pinned orkflow carries `pins` on the document, sorted
// by name, and `pin` on each member task (design §5.4).
func TestCanonicalOrkflow_EmitsPinsAndPinWhenSet(t *testing.T) {
	canonical, err := CanonicalOrkflow(pinnedOrkflow(t))
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	var decoded struct {
		Pins []struct {
			Name           string `json:"name"`
			TimeoutSeconds int64  `json:"timeoutSeconds"`
		} `json:"pins"`
		Tasks []struct {
			ID  string `json:"id"`
			Pin string `json:"pin"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatalf("decode %s: %v", canonical, err)
	}
	if len(decoded.Pins) != 1 {
		t.Fatalf("want one pin, got %d in %s", len(decoded.Pins), canonical)
	}
	if decoded.Pins[0].Name != "build" || decoded.Pins[0].TimeoutSeconds != 3600 {
		t.Fatalf("pin canonicalized as %+v, want {build 3600}", decoded.Pins[0])
	}
	for _, task := range decoded.Tasks {
		if task.Pin != "build" {
			t.Fatalf("task %q canonicalized with pin %q, want \"build\"", task.ID, task.Pin)
		}
	}
}

// pins are sorted by name, so the order they arrive in cannot change a digest.
func TestCanonicalOrkflow_SortsPinsByName(t *testing.T) {
	wf := pinnedOrkflow(t)
	wf.Pins = []*orkstackv1.Pin{
		{Name: "build", TimeoutSeconds: 3600},
		{Name: "acceptance", TimeoutSeconds: 60},
	}
	first, err := CanonicalOrkflow(wf)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	wf.Pins = []*orkstackv1.Pin{
		{Name: "acceptance", TimeoutSeconds: 60},
		{Name: "build", TimeoutSeconds: 3600},
	}
	second, err := CanonicalOrkflow(wf)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("pin order changed the canonical form:\n%s\n%s", first, second)
	}
	if at := strings.Index(string(first), `"acceptance"`); at < 0 || at > strings.Index(string(first), `"build"`) {
		t.Fatalf("pins are not sorted by name: %s", first)
	}
}

// A pin changes what runs and WHERE, so it is in the digest: the same document
// with its pins stripped is a different definition.
func TestDefinitionDigest_ChangesWhenAPinIsAddedOrRemoved(t *testing.T) {
	pinned, err := DefinitionDigest(pinnedOrkflow(t))
	if err != nil {
		t.Fatalf("digest: %v", err)
	}

	stripped := pinnedOrkflow(t)
	stripped.Pins = nil
	for _, task := range stripped.GetTasks() {
		task.Pin = ""
	}
	unpinned, err := DefinitionDigest(stripped)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if pinned == unpinned {
		t.Fatal("stripping pins produced the same definition digest, so the pin " +
			"is not part of the hash")
	}

	t.Run("the pin's own timeout is hashed", func(t *testing.T) {
		moved := pinnedOrkflow(t)
		moved.Pins[0].TimeoutSeconds = 7200
		got, err := DefinitionDigest(moved)
		if err != nil {
			t.Fatalf("digest: %v", err)
		}
		if got == pinned {
			t.Fatal("changing Pin.timeout_seconds did not change the digest")
		}
	})

	t.Run("moving one task out of the pin is a different definition", func(t *testing.T) {
		moved := pinnedOrkflow(t)
		moved.Tasks[1].Pin = ""
		got, err := DefinitionDigest(moved)
		if err != nil {
			t.Fatalf("digest: %v", err)
		}
		if got == pinned {
			t.Fatal("unpinning one task did not change the digest")
		}
	})
}

// THE BACKWARD-COMPATIBILITY CRUX (design §5.4, §10.1). An orkflow that
// declares no pin must canonicalize with NO `pins` key and NO `pin` key —
// not present, not empty, not null — because the digest is stamped into the
// document and a moved digest turns a re-apply of an unchanged document into
// `already_exists` (see canonicalPins). GoldenDefinitionDigest is the other
// half of this proof; this test says WHY it still holds.
func TestCanonicalOrkflow_OmitsPinKeysEntirelyWhenThereAreNoPins(t *testing.T) {
	canonical, err := CanonicalOrkflow(goldenOrkflow(t))
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	// ImageRef.pinned is a legitimate key that starts with "pin", so the two
	// new keys are looked for exactly.
	for _, key := range []string{`"pins"`, `"pin":`} {
		if strings.Contains(string(canonical), key) {
			t.Fatalf("an unpinned orkflow emitted %s: %s", key, canonical)
		}
	}

	var document map[string]any
	if err := json.Unmarshal(canonical, &document); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := document["pins"]; ok {
		t.Fatalf("an unpinned orkflow carries a \"pins\" key: %s", canonical)
	}
	tasks, ok := document["tasks"].([]any)
	if !ok || len(tasks) == 0 {
		t.Fatalf("no tasks in %s", canonical)
	}
	for _, task := range tasks {
		fields, ok := task.(map[string]any)
		if !ok {
			t.Fatalf("task is not an object in %s", canonical)
		}
		if _, ok := fields["pin"]; ok {
			t.Fatalf("an unpinned task carries a \"pin\" key: %s", canonical)
		}
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
