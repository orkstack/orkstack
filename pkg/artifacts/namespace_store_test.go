package artifacts

import (
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestNamespaceKey_ResolvesTheBuiltInDefault(t *testing.T) {
	for _, name := range []string{"", DefaultNamespace} {
		got, err := NamespaceKey(name)
		if err != nil {
			t.Fatalf("NamespaceKey(%q): %v", name, err)
		}
		if got != NamespacePrefix+"/default.json" {
			t.Errorf("NamespaceKey(%q) = %q, want the default key", name, got)
		}
	}
}

func TestNamespaceKey_RejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"..", "../billing", "billing/team", "billing team", string(rune(10))} {
		_, err := NamespaceKey(name)
		if !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("NamespaceKey(%q) error = %v, want ErrInvalidNamespace", name, err)
		}
	}

}
func TestParseNamespaceKey_RoundTripsAndSkipsForeignKeys(t *testing.T) {
	for _, want := range []string{"default", "payments", "team-42"} {
		key, err := NamespaceKey(want)
		if err != nil {
			t.Fatalf("NamespaceKey(%q): %v", want, err)
		}
		got, ok := ParseNamespaceKey(key)
		if !ok || got != want {
			t.Errorf("ParseNamespaceKey(%q) = %q, %v; want %q, true", key, got, ok, want)
		}
	}
	for _, foreign := range []string{
		"metadata/namespaces/payments.txt",
		"metadata/namespaces/payments/extra.json",
		"metadata/orkflows/payments/deploy/1.0.0.json",
		"metadata/namespaces/../secret.json",
	} {
		if got, ok := ParseNamespaceKey(foreign); ok {
			t.Errorf("ParseNamespaceKey(%q) = %q, true; want false", foreign, got)
		}
	}
}

func TestDefaultNamespaceDocument_IsAlwaysAvailableWithoutAWrite(t *testing.T) {
	ns := DefaultNamespaceDocument()
	if ns.GetName() != DefaultNamespace {
		t.Fatalf("default namespace name = %q, want %q", ns.GetName(), DefaultNamespace)
	}
	if ns.GetDescription() == "" {
		t.Fatal("default namespace has no description")
	}
}

func TestValidNamespacePools_UsesMainWhenDefaultIsUnset(t *testing.T) {
	if err := validNamespacePools(&orkstackv1.Namespace{
		Name:         "payments",
		AllowedPools: []string{"main", "gpu"},
	}); err != nil {
		t.Fatalf("validNamespacePools: %v", err)
	}
	for _, ns := range []*orkstackv1.Namespace{
		{Name: "payments", DefaultPool: "gpu", AllowedPools: []string{"main"}},
		{Name: "payments", DefaultPool: "bad pool"},
		{Name: "payments", AllowedPools: []string{"GPU"}},
	} {
		if err := validNamespacePools(ns); !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("validNamespacePools(%v) = %v, want ErrInvalidNamespace", ns, err)
		}
	}
}

// TestValidNamespaceTelemetry_EnforcesOnlyTheDocumentOnlyRules is the store
// half of the retention/pools split: shape and reserved prefixes are checked
// here, sink existence is not — this package has no ORK_TELEMETRY_SINKS
// table to check it against, only the engine that parsed one does
// (internal/api.validNamespaceTelemetry). A sink name of any shape at all,
// even one no deployment would ever declare, must pass here.
func TestValidNamespaceTelemetry_EnforcesOnlyTheDocumentOnlyRules(t *testing.T) {
	for _, ns := range []*orkstackv1.Namespace{
		{Name: "payments"},
		{Name: "payments", Telemetry: &orkstackv1.TelemetryPolicy{Sink: "no-such-sink-anywhere"}},
		{Name: "payments", Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"team": "money"},
		}},
	} {
		if err := validNamespaceTelemetry(ns); err != nil {
			t.Errorf("validNamespaceTelemetry(%v) = %v, want nil (sink existence is the API's job)", ns, err)
		}
	}
}

// TestValidNamespaceTelemetry_RefusesAReservedAttributePrefix.
func TestValidNamespaceTelemetry_RefusesAReservedAttributePrefix(t *testing.T) {
	for _, prefix := range []string{"ork.", "service.", "host."} {
		ns := &orkstackv1.Namespace{
			Name: "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{
				Attributes: map[string]string{prefix + "x": "1"},
			},
		}
		if err := validNamespaceTelemetry(ns); !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("validNamespaceTelemetry(prefix %q) = %v, want ErrInvalidNamespace", prefix, err)
		}
	}
}

// TestValidNamespaceTelemetry_RefusesAMalformedAttribute delegates key/value
// shape to internal/dsl.ValidateAttributeMap; this pins that the store
// actually calls it rather than silently accepting anything.
func TestValidNamespaceTelemetry_RefusesAMalformedAttribute(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name: "payments",
		Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"Not-Lowercase": "1"},
		},
	}
	if err := validNamespaceTelemetry(ns); !errors.Is(err, ErrInvalidNamespace) {
		t.Errorf("validNamespaceTelemetry(bad key) = %v, want ErrInvalidNamespace", err)
	}
}

// TestValidNamespaceTelemetry_RefusesAControlCharacterInAValue: an attribute
// value is rendered verbatim by `orkctl namespace get` and the UI's
// namespace page, so a control character in it (an embedded newline, an
// escape sequence) is refused at the same document-only boundary as a
// malformed key.
func TestValidNamespaceTelemetry_RefusesAControlCharacterInAValue(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name: "payments",
		Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"team": "payments\ndrop table"},
		},
	}
	if err := validNamespaceTelemetry(ns); !errors.Is(err, ErrInvalidNamespace) {
		t.Errorf("validNamespaceTelemetry(control char value) = %v, want ErrInvalidNamespace", err)
	}
}

func TestValidNamespaceHold_RequiresAReasonAndBoundsIt(t *testing.T) {
	cases := []struct {
		name string
		hold *orkstackv1.NamespaceHold
		ok   bool
	}{
		{"nil hold is accepting", nil, true},
		{"a short reason", &orkstackv1.NamespaceHold{Reason: "partner CI loop, INC-4412"}, true},
		{"exactly 512 bytes", &orkstackv1.NamespaceHold{Reason: strings.Repeat("x", 512)}, true},
		{"empty", &orkstackv1.NamespaceHold{}, false},
		{"whitespace only", &orkstackv1.NamespaceHold{Reason: "   "}, false},
		{"513 bytes", &orkstackv1.NamespaceHold{Reason: strings.Repeat("x", 513)}, false},
		{"newline", &orkstackv1.NamespaceHold{Reason: "loop\nINC-4412"}, false},
		{"escape", &orkstackv1.NamespaceHold{Reason: "loop \x1b[31m"}, false},
		{"NUL", &orkstackv1.NamespaceHold{Reason: "loop\x00"}, false},
		{"DEL", &orkstackv1.NamespaceHold{Reason: "loop\x7f"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validNamespaceHold(&orkstackv1.Namespace{Name: "payments", Hold: tc.hold})
			if tc.ok {
				if err != nil {
					t.Fatalf("validNamespaceHold = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidNamespace) {
				t.Fatalf("validNamespaceHold = %v, want ErrInvalidNamespace", err)
			}
			if !strings.Contains(err.Error(), `"payments"`) {
				t.Errorf("validNamespaceHold = %q, want it to name the namespace", err)
			}
		})
	}
}

func TestValidNamespaceMaxRunning_RefusesANegativeValue(t *testing.T) {
	for _, tc := range []struct {
		max int32
		ok  bool
	}{{-1, false}, {0, true}, {20, true}} {
		err := validNamespaceMaxRunning(&orkstackv1.Namespace{Name: "payments", MaxRunning: tc.max})
		if tc.ok && err != nil {
			t.Errorf("validNamespaceMaxRunning(%d) = %v, want nil", tc.max, err)
		}
		if !tc.ok && !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("validNamespaceMaxRunning(%d) = %v, want ErrInvalidNamespace", tc.max, err)
		}
	}
}

func TestStampNamespaceHold_KeepsSinceAcrossAReasonEditAndClearsOnRelease(t *testing.T) {
	now := time.Date(2026, 9, 22, 2, 14, 7, 0, time.UTC)
	earlier := timestamppb.New(now.Add(-3 * time.Hour))
	forged := timestamppb.New(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	cases := []struct {
		name      string
		next      *orkstackv1.NamespaceHold
		existing  *orkstackv1.Namespace
		wantSince *timestamppb.Timestamp
	}{
		{"new hold is stamped now", &orkstackv1.NamespaceHold{Reason: "r"}, nil, timestamppb.New(now)},
		{"new hold over an accepting document is stamped now",
			&orkstackv1.NamespaceHold{Reason: "r"}, &orkstackv1.Namespace{}, timestamppb.New(now)},
		{"a reason edit keeps since",
			&orkstackv1.NamespaceHold{Reason: "new reason"},
			&orkstackv1.Namespace{Hold: &orkstackv1.NamespaceHold{Reason: "old", Since: earlier}}, earlier},
		{"a client since on a new hold is discarded",
			&orkstackv1.NamespaceHold{Reason: "r", Since: forged}, nil, timestamppb.New(now)},
		{"a client since on a kept hold is discarded",
			&orkstackv1.NamespaceHold{Reason: "r", Since: forged},
			&orkstackv1.Namespace{Hold: &orkstackv1.NamespaceHold{Reason: "r", Since: earlier}}, earlier},
		{"release carries nothing forward", nil,
			&orkstackv1.Namespace{Hold: &orkstackv1.NamespaceHold{Reason: "old", Since: earlier}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next := &orkstackv1.Namespace{Name: "payments", Hold: tc.next}
			StampNamespaceHold(next, tc.existing, now)
			if tc.wantSince == nil {
				if next.GetHold() != nil {
					t.Fatalf("hold = %v, want none", next.GetHold())
				}
				return
			}
			if !proto.Equal(next.GetHold().GetSince(), tc.wantSince) {
				t.Fatalf("since = %v, want %v", next.GetHold().GetSince().AsTime(), tc.wantSince.AsTime())
			}
		})
	}
}

// TestValidNamespaceSecrets_RefusesAMalformedEntryAndNamesIt: the registry
// holds allowed_secrets to dsl.ValidSecretPatterns and nothing more — a
// scheme no Orker resolves is each Orker's business, not the document's.
func TestValidNamespaceSecrets_RefusesAMalformedEntryAndNamesIt(t *testing.T) {
	many := make([]string, 0, 33)
	for i := 0; i < 33; i++ {
		many = append(many, "env:P"+strings.Repeat("X", i)+"_*")
	}
	for _, tc := range []struct {
		name    string
		entries []string
		names   string
	}{
		{"257 bytes", []string{"abcdefghijklmnop:" + strings.Repeat("A", 240)}, "abcdefghijklmnop:"},
		{"no colon", []string{"envX"}, `"envX"`},
		{"empty scheme", []string{":X"}, `":X"`},
		{"empty pattern", []string{"env:"}, `"env:"`},
		{"long scheme", []string{"abcdefghijklmnopq:X"}, `"abcdefghijklmnopq:X"`},
		{"star in scheme", []string{"en*v:X"}, `"en*v:X"`},
		{"upper scheme", []string{"Env:X"}, `"Env:X"`},
		{"comma", []string{"env:A,B"}, `"env:A,B"`},
		{"space", []string{"env:A B"}, `"env:A B"`},
		{"tab", []string{"env:A\tB"}, `"env:A\tB"`},
		{"relative file", []string{"file:relative"}, `"file:relative"`},
		{"dotdot", []string{"file:/a/../b"}, `"file:/a/../b"`},
		{"dot", []string{"file:/a/./b"}, `"file:/a/./b"`},
		{"empty segment", []string{"file:/a//b"}, `"file:/a//b"`},
		{"trailing slash", []string{"file:/a/"}, `"file:/a/"`},
		{"33 entries", many, "33"},
		{"duplicate", []string{"env:A_*", "env:A_*"}, `"env:A_*"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidNamespaceSecrets(&orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: tc.entries})
			if !errors.Is(err, ErrInvalidNamespace) {
				t.Fatalf("ValidNamespaceSecrets = %v, want ErrInvalidNamespace", err)
			}
			if !strings.Contains(err.Error(), `"sandbox"`) || !strings.Contains(err.Error(), tc.names) {
				t.Errorf("ValidNamespaceSecrets = %q, want it to name the namespace and %s", err, tc.names)
			}
		})
	}
	for _, ok := range [][]string{nil, {}, {"vault:x"}, {"env:SANDBOX_*", "file:/etc/orkstack/secrets/sandbox/*"}} {
		if err := ValidNamespaceSecrets(&orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: ok}); err != nil {
			t.Errorf("ValidNamespaceSecrets(%q) = %v, want nil", ok, err)
		}
	}
}

// TestValidateNamespaceSpec_IsWhatPutEnforces: every document Put refuses for
// its own shape is refused by ValidateNamespaceSpec with the same error, and a
// valid one is accepted by both — the function is a move of Put's checks, not
// a second copy of them.
func TestValidateNamespaceSpec_IsWhatPutEnforces(t *testing.T) {
	double := newS3Double(t)
	store, err := NewS3NamespaceStore(t.Context(), double.config())
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	refused := map[string]*orkstackv1.Namespace{
		"pools":             {Name: "payments", DefaultPool: "gpu", AllowedPools: []string{"main"}},
		"retention":         {Name: "payments", RunRetention: durationpb.New(-time.Hour)},
		"telemetry":         {Name: "payments", Telemetry: &orkstackv1.TelemetryPolicy{Attributes: map[string]string{"ork.x": "y"}}},
		"hold":              {Name: "payments", Hold: &orkstackv1.NamespaceHold{}},
		"max_running":       {Name: "payments", MaxRunning: -1},
		"allowed_secrets":   {Name: "payments", AllowedSecrets: []string{"no-scheme"}},
		"require_signed_by": {Name: "payments", RequireSignedBy: "gpg:key"},
	}
	for name, spec := range refused {
		t.Run(name, func(t *testing.T) {
			specErr := ValidateNamespaceSpec(spec)
			if specErr == nil {
				t.Fatal("ValidateNamespaceSpec accepted it")
			}
			_, putErr := store.Put(t.Context(), spec, PutConditions{IfAbsent: true})
			if putErr == nil || putErr.Error() != specErr.Error() {
				t.Fatalf("Put = %v, ValidateNamespaceSpec = %v; want the same refusal", putErr, specErr)
			}
		})
	}
	valid := &orkstackv1.Namespace{Name: "payments", DefaultPool: "main", AllowedPools: []string{"main", "gpu"}, MaxRunning: 3}
	if err := ValidateNamespaceSpec(valid); err != nil {
		t.Fatalf("ValidateNamespaceSpec(valid) = %v", err)
	}
	if _, err := store.Put(t.Context(), valid, PutConditions{IfAbsent: true}); err != nil {
		t.Fatalf("Put(valid) = %v", err)
	}
}
