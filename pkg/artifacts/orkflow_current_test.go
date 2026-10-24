package artifacts

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestOrkflowCurrentKey_SitsBesideTheVersionsAndIsNotOne pins the pointer's
// address and the reason it cannot collide with a version: it is under the
// orkflow's own prefix (so one scan sees both) and the version parser refuses
// it (so no scan ever decodes it as a definition).
func TestOrkflowCurrentKey_SitsBesideTheVersionsAndIsNotOne(t *testing.T) {
	key, err := OrkflowCurrentKey("payments", "deploy")
	if err != nil {
		t.Fatalf("OrkflowCurrentKey: %v", err)
	}
	if want := "metadata/orkflows/payments/deploy/@current.json"; key != want {
		t.Fatalf("OrkflowCurrentKey = %q, want %q", key, want)
	}
	versionKey, err := OrkflowKey("payments", "deploy", "1.0.0")
	if err != nil {
		t.Fatalf("OrkflowKey: %v", err)
	}
	prefix := strings.TrimSuffix(versionKey, "1.0.0.json")
	if !strings.HasPrefix(key, prefix) {
		t.Fatalf("the pointer %q is not under the orkflow's prefix %q", key, prefix)
	}

	// The version parser must refuse it, or a listing would try to decode the
	// pointer as a definition and report a version called "@current".
	if namespace, name, version, ok := parseOrkflowKey(key); ok {
		t.Fatalf("parseOrkflowKey(%q) = %q, %q, %q, true; want false", key, namespace, name, version)
	}
	// And the pointer parser must accept exactly it.
	namespace, name, ok := ParseOrkflowCurrentKey(key)
	if !ok || namespace != "payments" || name != "deploy" {
		t.Fatalf("ParseOrkflowCurrentKey(%q) = %q, %q, %v; want payments, deploy, true",
			key, namespace, name, ok)
	}

	// An empty namespace is the default one here too, so a pointer written by
	// a document that names no namespace is addressed with the same rule as
	// its versions.
	fallback, err := OrkflowCurrentKey("", "deploy")
	if err != nil {
		t.Fatalf("OrkflowCurrentKey(default): %v", err)
	}
	if want := "metadata/orkflows/default/deploy/@current.json"; fallback != want {
		t.Fatalf("OrkflowCurrentKey(\"\") = %q, want %q", fallback, want)
	}
}

// TestParseOrkflowKey_ReservesTheAtBasename holds the reservation itself: every
// basename beginning "@" is metadata beside the versions, not a version.
//
// The version pattern already refuses "@…", so this is belt and braces — and
// it is what keeps the NEXT such document (a per-orkflow note, an owner file)
// from being read as an orkflow the day somebody adds it.
func TestParseOrkflowKey_ReservesTheAtBasename(t *testing.T) {
	reserved := []string{
		"metadata/orkflows/default/deploy/@current.json",
		"metadata/orkflows/payments/deploy/@current.json",
		"metadata/orkflows/payments/deploy/@anything.json",
		"metadata/orkflows/payments/deploy/@.json",
	}
	for _, key := range reserved {
		if namespace, name, version, ok := parseOrkflowKey(key); ok {
			t.Errorf("parseOrkflowKey(%q) = %q, %q, %q, true; want false (reserved)",
				key, namespace, name, version)
		}
	}
	// A pointer key for one orkflow is not a pointer key for anything else.
	foreign := []string{
		"metadata/orkflows/payments/deploy/1.0.0.json",
		"metadata/orkflows/payments/deploy/current.json",
		"metadata/orkflows/payments/@current.json",
		"metadata/orkflows/payments/deploy/nested/@current.json",
		"metadata/namespaces/payments.json",
		"",
	}
	for _, key := range foreign {
		if namespace, name, ok := ParseOrkflowCurrentKey(key); ok {
			t.Errorf("ParseOrkflowCurrentKey(%q) = %q, %q, true; want false", key, namespace, name)
		}
	}
}

// TestCurrentPointer_HoldsOnlyTheVersion is the drift guard: the pointer stores
// a version string and a time, never a copy of the document. A pointer that
// carried the definition would be a second place the definition lives, and the
// two would disagree the first time somebody wrote one and not the other.
func TestCurrentPointer_HoldsOnlyTheVersion(t *testing.T) {
	moved := time.Date(2026, 8, 19, 9, 30, 0, 0, time.UTC)
	document, err := marshalCurrentPointer(currentPointer{Version: "2.0.0", UpdatedAt: moved})
	if err != nil {
		t.Fatalf("marshalCurrentPointer: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(document, &fields); err != nil {
		t.Fatalf("pointer document is not JSON: %v\n%s", err, document)
	}
	if len(fields) != 2 {
		t.Fatalf("pointer document has %d fields, want exactly version and updated_at:\n%s",
			len(fields), document)
	}
	for _, field := range []string{"version", "updated_at"} {
		if _, ok := fields[field]; !ok {
			t.Errorf("pointer document has no %q:\n%s", field, document)
		}
	}

	back, err := unmarshalCurrentPointer(document)
	if err != nil {
		t.Fatalf("unmarshalCurrentPointer: %v", err)
	}
	if back.Version != "2.0.0" || !back.UpdatedAt.Equal(moved) {
		t.Fatalf("pointer round trip = %+v, want 2.0.0 at %s", back, moved)
	}

	// A document written by a newer engine still reads.
	forward, err := unmarshalCurrentPointer([]byte(`{"version":"3.0.0","promoted_by":"someone"}`))
	if err != nil {
		t.Fatalf("unmarshalCurrentPointer(newer): %v", err)
	}
	if forward.Version != "3.0.0" {
		t.Fatalf("newer pointer decoded as %+v, want version 3.0.0", forward)
	}
}

// TestOrkflowVersions_ResolvedPrefersThePointer is the resolution rule in one
// place: the pointer decides, and only an orkflow nobody promoted falls back to
// the most recently applied version.
func TestOrkflowVersions_ResolvedPrefersThePointer(t *testing.T) {
	cases := []struct {
		name     string
		versions OrkflowVersions
		want     string
	}{
		{"nobody promoted anything", OrkflowVersions{Head: "2.0.0"}, "2.0.0"},
		{"promoted to the head", OrkflowVersions{Current: "2.0.0", Head: "2.0.0"}, "2.0.0"},
		{"rolled back to an older version", OrkflowVersions{Current: "1.0.0", Head: "2.0.0"}, "1.0.0"},
		{"nothing stored", OrkflowVersions{}, ""},
	}
	for _, tc := range cases {
		if got := tc.versions.Resolved(); got != tc.want {
			t.Errorf("%s: Resolved() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestCurrentExpectation pins the three claims a promotion can make about the
// pointer it replaces, and that a refusal is recognised both as "the pointer
// moved" and as the older "lost a race".
func TestCurrentExpectation(t *testing.T) {
	cases := []struct {
		name      string
		e         CurrentExpectation
		satisfied map[string]bool
	}{
		{"no expectation", NoCurrentExpectation(), map[string]bool{"": true, "1.0.0": true, "2.0.0": true}},
		{"expect unset", ExpectCurrent(""), map[string]bool{"": true, "1.0.0": false}},
		{"expect 1.0.0", ExpectCurrent("1.0.0"), map[string]bool{"": false, "1.0.0": true, "2.0.0": false}},
	}
	for _, tc := range cases {
		for current, want := range tc.satisfied {
			if got := tc.e.SatisfiedBy(current); got != want {
				t.Errorf("%s: SatisfiedBy(%q) = %v, want %v", tc.name, current, got, want)
			}
		}
	}
	if NoCurrentExpectation().Stated() || !ExpectCurrent("").Stated() {
		t.Fatal("Stated: NoCurrentExpectation must claim nothing and ExpectCurrent(\"\") must claim unset")
	}
	if ExpectCurrent("1.0.0").Version() != "1.0.0" {
		t.Fatal("Version() must return the claimed pointer")
	}
	var err error = &CurrentMovedError{Namespace: "default", Name: "deploy", Expected: "1.0.0", Current: "2.0.0"}
	if !errors.Is(err, ErrCurrentMoved) || !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("CurrentMovedError must be errors.Is ErrCurrentMoved and ErrPreconditionFailed: %v", err)
	}
	if msg := err.Error(); !strings.Contains(msg, `"2.0.0"`) || !strings.Contains(msg, `"1.0.0"`) {
		t.Fatalf("CurrentMovedError must name both pointers: %q", msg)
	}
}
