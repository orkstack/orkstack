package api

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func payloadOf(t *testing.T, v map[string]any) *orkstackv1.Payload {
	t.Helper()
	s, err := structpb.NewStruct(v)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	return &orkstackv1.Payload{Data: s}
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// The encoding is fixed (workload-identity.md §3.1 rule 1): a change here
// moves every key's fingerprint, and a claim made before it would refuse its
// own retry. This golden pins it.
func TestSubmissionFingerprint_Golden(t *testing.T) {
	msg := &orkstackv1.SubmitRunRequest{
		OrkflowName:    "deploy",
		OrkflowVersion: "v1",
		SourceRunId:    "ork-src",
		Input:          payloadOf(t, map[string]any{"b": 1.0, "a": "x"}),
	}
	got, err := submissionFingerprint(msg)
	if err != nil {
		t.Fatal(err)
	}
	inputSHA := sha(`{"a":"x","b":1}`)
	want := sha(`{"input_sha256":"` + inputSHA + `","orkflow":"deploy","source_run_id":"ork-src","v":1,"version":"v1"}`)
	if got != want {
		t.Fatalf("fingerprint = %s, want %s", got, want)
	}
}

func TestSubmissionFingerprint_SameAndDifferent(t *testing.T) {
	base := func() *orkstackv1.SubmitRunRequest {
		return &orkstackv1.SubmitRunRequest{
			OrkflowName: "deploy",
			Input:       payloadOf(t, map[string]any{"a": "x", "n": 2.0}),
		}
	}
	fp := func(m *orkstackv1.SubmitRunRequest) string {
		t.Helper()
		f, err := submissionFingerprint(m)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	ref := fp(base())

	same := map[string]func(*orkstackv1.SubmitRunRequest){
		"key order":        func(m *orkstackv1.SubmitRunRequest) { m.Input = payloadOf(t, map[string]any{"n": 2.0, "a": "x"}) },
		"allow_empty_pool": func(m *orkstackv1.SubmitRunRequest) { m.AllowEmptyPool = true },
		"no_cache":         func(m *orkstackv1.SubmitRunRequest) { m.NoCache = true },
		"origin_reference": func(m *orkstackv1.SubmitRunRequest) { m.OriginReference = "ci-42" },
		"namespace":        func(m *orkstackv1.SubmitRunRequest) { m.Namespace = "other" }, // in the claim's id, not here
		"idempotency_key":  func(m *orkstackv1.SubmitRunRequest) { m.IdempotencyKey = "k2" },
	}
	for name, mutate := range same {
		m := base()
		mutate(m)
		if got := fp(m); got != ref {
			t.Errorf("%s: fingerprint changed; it must not", name)
		}
	}

	different := map[string]func(*orkstackv1.SubmitRunRequest){
		"orkflow":       func(m *orkstackv1.SubmitRunRequest) { m.OrkflowName = "deploy2" },
		"version":       func(m *orkstackv1.SubmitRunRequest) { m.OrkflowVersion = "v1" },
		"source run":    func(m *orkstackv1.SubmitRunRequest) { m.SourceRunId = "ork-x" },
		"input value":   func(m *orkstackv1.SubmitRunRequest) { m.Input = payloadOf(t, map[string]any{"a": "y", "n": 2.0}) },
		"no input":      func(m *orkstackv1.SubmitRunRequest) { m.Input = nil },
		"empty input":   func(m *orkstackv1.SubmitRunRequest) { m.Input = payloadOf(t, map[string]any{}) },
		"concatenation": func(m *orkstackv1.SubmitRunRequest) { m.OrkflowName = "deplo"; m.OrkflowVersion = "y" },
	}
	for name, mutate := range different {
		m := base()
		mutate(m)
		if got := fp(m); got == ref {
			t.Errorf("%s: fingerprint unchanged; it must change", name)
		}
	}
}
