package artifacts

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// helloOrkflow is a compiled orkflow shaped like the one internal/dsl produces
// for examples/hello.ork.yaml: a templated `with` block, a declared input, and
// the original document kept for audit.
func helloOrkflow(t *testing.T, version string) *orkstackv1.Orkflow {
	t.Helper()
	with, err := structpb.NewStruct(map[string]any{
		"op":              "get",
		"url":             "{{ .inputs.url }}",
		"capture_body_as": "body",
	})
	if err != nil {
		t.Fatalf("with: %v", err)
	}
	inputs, err := structpb.NewStruct(map[string]any{
		"url": map[string]any{"type": "string", "default": "https://httpbin.org/json"},
	})
	if err != nil {
		t.Fatalf("inputs: %v", err)
	}
	return &orkstackv1.Orkflow{
		Name:        "hello-ork",
		Version:     version,
		Description: "Fetch JSON, extract a field, and persist it as an artifact.",
		Labels:      map[string]string{"owner": "platform"},
		Inputs:      inputs,
		Tasks: []*orkstackv1.Task{{
			Id:             "fetch",
			Plugin:         "http",
			With:           with,
			TimeoutSeconds: 300,
			Retry:          &orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 2},
		}},
		SourceYaml: "name: hello-ork\nversion: \"" + version + "\"\n",
		CreatedAt:  timestamppb.New(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)),
	}
}

func TestOrkflowKey_AddressesOneVersionUnderMetadataOrkflows(t *testing.T) {
	key, err := OrkflowKey("hello-ork", "0.1.0")
	if err != nil {
		t.Fatalf("OrkflowKey: %v", err)
	}
	if want := "metadata/orkflows/hello-ork/0.1.0.json"; key != want {
		t.Fatalf("OrkflowKey = %q, want %q", key, want)
	}

	// A second version is a second object; the first one's key is untouched.
	next, err := OrkflowKey("hello-ork", "0.2.0")
	if err != nil {
		t.Fatalf("OrkflowKey: %v", err)
	}
	if next == key {
		t.Fatal("two versions share one key")
	}
	if !strings.HasPrefix(next, OrkflowPrefix+"/hello-ork/") {
		t.Fatalf("version key %q is not under the orkflow's prefix", next)
	}
}

func TestOrkflowKey_RejectsCoordinatesThatCouldEscapeThePrefix(t *testing.T) {
	cases := []struct{ name, version string }{
		{"", "0.1.0"},
		{"hello-ork", ""},
		{"..", "0.1.0"},
		{"hello-ork", ".."},
		{"../../etc/passwd", "0.1.0"},
		{"hello/ork", "0.1.0"},
		{"hello-ork", "0.1.0/../../secret"},
		{"hello-ork", "0.1.0\n"},
		{".hidden", "0.1.0"},
		{"hello ork", "0.1.0"},
		{strings.Repeat("a", maxOrkflowComponent+1), "0.1.0"},
	}
	for _, tc := range cases {
		key, err := OrkflowKey(tc.name, tc.version)
		if !errors.Is(err, ErrInvalidOrkflow) {
			t.Errorf("OrkflowKey(%q, %q) = %q, %v; want ErrInvalidOrkflow", tc.name, tc.version, key, err)
		}
		if key != "" {
			t.Errorf("OrkflowKey(%q, %q) returned key %q on error", tc.name, tc.version, key)
		}
	}
}

func TestParseOrkflowKey_RoundTripsGeneratedKeysAndSkipsForeignOnes(t *testing.T) {
	for _, want := range []struct{ name, version string }{
		{"hello-ork", "0.1.0"},
		{"a", "1"},
		{"hello-ork-m4", "2026.08.16-rc1"},
	} {
		key, err := OrkflowKey(want.name, want.version)
		if err != nil {
			t.Fatalf("OrkflowKey(%q, %q): %v", want.name, want.version, err)
		}
		name, version, ok := parseOrkflowKey(key)
		if !ok || name != want.name || version != want.version {
			t.Errorf("parseOrkflowKey(%q) = %q, %q, %v; want %q, %q, true",
				key, name, version, ok, want.name, want.version)
		}
	}

	// Nothing else in the bucket is an orkflow definition: run artifacts, their
	// name manifests, and anything nested deeper are ignored, not decoded.
	foreign := []string{
		"orkflows/hello-ork/fetch/ork-1755302400123456789-1/body",
		"manifests/orkflows/hello-ork/fetch/ork-1/body",
		"metadata/orkflows/hello-ork/0.1.0.txt",
		"metadata/orkflows/hello-ork",
		"metadata/orkflows/hello-ork/nested/0.1.0.json",
		"metadata/runs/ork-1/run.json",
		"",
	}
	for _, key := range foreign {
		if name, version, ok := parseOrkflowKey(key); ok {
			t.Errorf("parseOrkflowKey(%q) = %q, %q, true; want false", key, name, version)
		}
	}
}

func TestMarshalOrkflow_IsByteStableAndLossless(t *testing.T) {
	wf := helloOrkflow(t, "0.1.0")

	first, err := marshalOrkflow(wf)
	if err != nil {
		t.Fatalf("marshalOrkflow: %v", err)
	}
	// protojson perturbs its own whitespace on purpose; the stored document
	// must not inherit that, or "this version was not rewritten" could not be
	// checked by comparing bytes.
	for i := range 16 {
		again, err := marshalOrkflow(wf)
		if err != nil {
			t.Fatalf("marshalOrkflow #%d: %v", i, err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("marshalOrkflow is not byte stable:\n%s\n---\n%s", first, again)
		}
	}

	if !json.Valid(first) {
		t.Fatalf("stored document is not JSON:\n%s", first)
	}
	if !bytes.HasSuffix(first, []byte("\n")) {
		t.Error("stored document does not end in a newline")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(first, &envelope); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	for _, field := range []string{"name", "version", "tasks", "sourceYaml", "createdAt"} {
		if _, ok := envelope[field]; !ok {
			t.Errorf("stored document has no %q field:\n%s", field, first)
		}
	}

	decoded, err := unmarshalOrkflow(first)
	if err != nil {
		t.Fatalf("unmarshalOrkflow: %v", err)
	}
	if !proto.Equal(wf, decoded) {
		t.Errorf("document round trip lost data:\nstored: %v\nread:   %v", wf, decoded)
	}
}

func TestUnmarshalOrkflow_ReadsADocumentWrittenByANewerEngine(t *testing.T) {
	document := []byte(`{"name":"hello-ork","version":"0.1.0","futureField":{"a":1}}`)
	wf, err := unmarshalOrkflow(document)
	if err != nil {
		t.Fatalf("unmarshalOrkflow: %v", err)
	}
	if wf.GetName() != "hello-ork" || wf.GetVersion() != "0.1.0" {
		t.Fatalf("decoded %s@%s, want hello-ork@0.1.0", wf.GetName(), wf.GetVersion())
	}
}

func TestUnmarshalOrkflow_RejectsADocumentThatIsNotAnOrkflow(t *testing.T) {
	if _, err := unmarshalOrkflow([]byte("not json")); err == nil {
		t.Fatal("unmarshalOrkflow(garbage) = nil error")
	}
}

func TestLatestOrkflow_IsTheMostRecentlyApplied(t *testing.T) {
	stamp := func(version string, at time.Time) *orkstackv1.Orkflow {
		return &orkstackv1.Orkflow{Name: "hello-ork", Version: version, CreatedAt: timestamppb.New(at)}
	}
	base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	// Applying an older version last makes it the latest: "latest" is the last
	// apply, not the highest version string.
	got := latestOrkflow([]*orkstackv1.Orkflow{
		stamp("0.2.0", base.Add(time.Second)),
		stamp("0.1.0", base.Add(2*time.Second)),
	})
	if got.GetVersion() != "0.1.0" {
		t.Errorf("latest = %q, want 0.1.0 (applied last)", got.GetVersion())
	}

	// Identical stamps still resolve deterministically.
	got = latestOrkflow([]*orkstackv1.Orkflow{stamp("0.1.0", base), stamp("0.2.0", base)})
	if got.GetVersion() != "0.2.0" {
		t.Errorf("latest of tied stamps = %q, want 0.2.0", got.GetVersion())
	}
	if latestOrkflow(nil) != nil {
		t.Error("latestOrkflow(nil) is not nil")
	}
}

func TestLatestByName_ReturnsOneVersionPerNameOrderedByName(t *testing.T) {
	stamp := func(name, version string, seconds int) *orkstackv1.Orkflow {
		return &orkstackv1.Orkflow{
			Name:      name,
			Version:   version,
			CreatedAt: timestamppb.New(time.Date(2026, 8, 16, 12, 0, seconds, 0, time.UTC)),
		}
	}
	got := latestByName([]*orkstackv1.Orkflow{
		stamp("zeta", "1.0.0", 1),
		stamp("alpha", "0.1.0", 2),
		stamp("alpha", "0.2.0", 3),
		stamp("mid", "9.9.9", 0),
	})

	want := []string{"alpha@0.2.0", "mid@9.9.9", "zeta@1.0.0"}
	rendered := make([]string, 0, len(got))
	for _, wf := range got {
		rendered = append(rendered, wf.GetName()+"@"+wf.GetVersion())
	}
	if strings.Join(rendered, ",") != strings.Join(want, ",") {
		t.Errorf("latestByName = %v, want %v", rendered, want)
	}
}
