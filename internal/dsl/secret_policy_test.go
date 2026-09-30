package dsl

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// check runs one vector through CheckSecretSource: a Pattern of "" is an
// empty list.
func checkVector(v SecretPolicyVector) error {
	var patterns []string
	if v.Pattern != "" {
		patterns = []string{v.Pattern}
	}
	return CheckSecretSource(patterns, v.Source)
}

func assertVector(t *testing.T, v SecretPolicyVector) {
	t.Helper()
	err := checkVector(v)
	if v.Permitted {
		if err != nil {
			t.Fatalf("pattern %q, source %q: want permitted, got %v", v.Pattern, v.Source, err)
		}
		return
	}
	if err == nil {
		t.Fatalf("pattern %q, source %q: want refused, got permitted", v.Pattern, v.Source)
	}
	var refusal *SecretSourceRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("pattern %q, source %q: error %T is not a *SecretSourceRefusal", v.Pattern, v.Source, err)
	}
	if refusal.Why != v.Why {
		t.Fatalf("pattern %q, source %q: Why = %q, want %q", v.Pattern, v.Source, refusal.Why, v.Why)
	}
	if refusal.Source != v.Source {
		t.Fatalf("refusal.Source = %q, want %q", refusal.Source, v.Source)
	}
}

func TestSecretPolicy_MatchVectors(t *testing.T) {
	want := []SecretPolicyVector{
		{Pattern: "env:SANDBOX_*", Source: "env:SANDBOX_DB", Permitted: true},
		{Pattern: "env:SANDBOX_*", Source: "env:SANDBOX_", Permitted: true},
		{Pattern: "env:SANDBOX_*", Source: "env:PROD_DB_PASSWORD"},
		{Pattern: "env:SANDBOX_*", Source: "ENV:SANDBOX_DB"},
		{Pattern: "env:*", Source: "file:/etc/x"},
		{Pattern: "env:SANDBOX_*", Source: "SANDBOX_DB"},
		{Pattern: "file:/etc/orkstack/secrets/sandbox/*", Source: "file:/etc/orkstack/secrets/sandbox/db", Permitted: true},
		{Pattern: "file:/etc/orkstack/secrets/sandbox/*", Source: "file:/etc/orkstack/secrets/sandbox/sub/db"},
		{Pattern: "file:/etc/orkstack/secrets/sandbox/*", Source: "file:/etc/orkstack/secrets/sandbox/../payments/db", Why: ".."},
		{Pattern: "file:/etc/orkstack/secrets/sandbox/*", Source: "file:/etc/orkstack/secrets/sandbox//db", Why: "empty"},
		{Pattern: "file:/etc/orkstack/secrets/sandbox/*", Source: "file:/etc/orkstack/secrets/sandbox/./db", Why: "."},
		{Pattern: "file:/etc/orkstack/secrets/sandbox/*", Source: "file:etc/orkstack/secrets/sandbox/db", Why: "relative"},
		{Pattern: "file:/etc/*/secrets/sandbox/*", Source: "file:/etc/orkstack/secrets/sandbox/db", Permitted: true},
		{Pattern: "env:DB?", Source: "env:DBX"},
		{Pattern: "env:DB?", Source: "env:DB?", Permitted: true},
		{Pattern: "env:A..B", Source: "env:A..B", Permitted: true},
		{Pattern: "", Source: "file:/a/../b", Permitted: true},
	}
	for _, v := range want {
		t.Run(v.Pattern+" "+v.Source, func(t *testing.T) { assertVector(t, v) })
	}
	// Every row the spec names is in the exported table the boundaries use.
	exported := map[SecretPolicyVector]bool{}
	for _, v := range SecretPolicyVectors() {
		exported[v] = true
	}
	for _, v := range want {
		if !exported[v] {
			t.Errorf("SecretPolicyVectors() lacks %+v", v)
		}
	}
}

func TestSecretPolicy_OnlyStarIsAWildcard(t *testing.T) {
	for _, v := range []SecretPolicyVector{
		{Pattern: "env:DB?", Source: "env:DBX"},
		{Pattern: "env:DB?", Source: "env:DB?", Permitted: true},
		{Pattern: "env:DB[a-z]", Source: "env:DBa"},
		{Pattern: "env:DB[a-z]", Source: "env:DB[a-z]", Permitted: true},
		{Pattern: `env:DB\*`, Source: "env:DBX"},
		{Pattern: `env:DB\*`, Source: `env:DB\X`, Permitted: true},
		{Pattern: "env:DB{A,B}", Source: "env:DBA"},
		{Pattern: "env:DB**", Source: "env:DBX", Permitted: true},
		{Pattern: "env:^DB$", Source: "env:DB"},
	} {
		t.Run(v.Pattern+" "+v.Source, func(t *testing.T) { assertVector(t, v) })
	}
}

func TestSecretPolicy_StarNeverCrossesASlash(t *testing.T) {
	for _, v := range []SecretPolicyVector{
		{Pattern: "file:/etc/*", Source: "file:/etc/a/b"},
		{Pattern: "file:/etc/*/*", Source: "file:/etc/a/b", Permitted: true},
		{Pattern: "file:/etc/*b", Source: "file:/etc/a/b"},
		{Pattern: "file:/etc/a*", Source: "file:/etc/ab/c"},
		{Pattern: "env:A*", Source: "env:A/B"},
		{Pattern: "file:/etc/*x*", Source: "file:/etc/axb", Permitted: true},
		{Pattern: "file:/etc/*x*y", Source: "file:/etc/axbxcy", Permitted: true},
	} {
		t.Run(v.Pattern+" "+v.Source, func(t *testing.T) { assertVector(t, v) })
	}
}

func TestSecretPolicy_TraversalIsRefusedNotNormalized(t *testing.T) {
	for _, v := range []SecretPolicyVector{
		// Normalized, /etc/secrets/b would match; it is refused anyway.
		{Pattern: "file:/etc/secrets/*", Source: "file:/etc/secrets/a/../b", Why: ".."},
		{Pattern: "file:/etc/secrets/*", Source: "file:/etc/secrets/./b", Why: "."},
		{Pattern: "file:/etc/secrets/*", Source: "file:/etc//secrets/b", Why: "empty"},
		{Pattern: "file:/etc/secrets/*", Source: "file:/etc/secrets/b/", Why: "empty"},
		{Pattern: "file:*", Source: "file:b", Why: "relative"},
		{Pattern: "env:A..B", Source: "env:A..B", Permitted: true},
		{Pattern: "env:*", Source: "env:..", Permitted: true},
	} {
		t.Run(v.Pattern+" "+v.Source, func(t *testing.T) { assertVector(t, v) })
	}
}

func TestSecretSourceRefusal_RendersEachShape(t *testing.T) {
	patterns := []string{"env:SANDBOX_*", "file:/etc/orkstack/secrets/sandbox/*"}
	for source, want := range map[string]string{
		"env:PROD_DB_PASSWORD":                              `"env:PROD_DB_PASSWORD" matches none of env:SANDBOX_*, file:/etc/orkstack/secrets/sandbox/*`,
		"file:/etc/orkstack/secrets/sandbox/../payments/db": `"file:/etc/orkstack/secrets/sandbox/../payments/db" has a ".." segment, which is refused rather than normalized`,
		"file:/etc/orkstack/secrets/sandbox/./db":           `"file:/etc/orkstack/secrets/sandbox/./db" has a "." segment, which is refused rather than normalized`,
		"file:/etc/orkstack/secrets/sandbox//db":            `"file:/etc/orkstack/secrets/sandbox//db" has an empty segment, which is refused rather than normalized`,
		"file:etc/orkstack/secrets/db":                      `"file:etc/orkstack/secrets/db" is not an absolute path`,
	} {
		err := CheckSecretSource(patterns, source)
		if err == nil || err.Error() != want {
			t.Errorf("CheckSecretSource(%q) = %v\nwant %s", source, err, want)
		}
	}
}

func TestValidSecretPattern_RefusesEveryMalformedShape(t *testing.T) {
	long := "abcdefghijklmnop:" + strings.Repeat("A", 240) // 16-byte scheme, 257 bytes total
	if len(long) != 257 {
		t.Fatalf("fixture is %d bytes", len(long))
	}
	for _, tc := range []struct {
		entry string
		want  string
	}{
		{long, "at most 256 bytes"},
		{"envX", `":"`},
		{":X", "scheme"},
		{"env:", "pattern"},
		{"abcdefghijklmnopq:X", "at most 16 bytes"},
		{"en*v:X", "^[a-z][a-z0-9]*$"},
		{"Env:X", "^[a-z][a-z0-9]*$"},
		{"env:A,B", `","`},
		{"env:A B", "whitespace"},
		{"env:A\tB", "control"},
		{"env:A\x7fB", "control"},
		{"env:A\xffB", "UTF-8"},
		{"file:relative", `begin with "/"`},
		{"file:/a/../b", `".."`},
		{"file:/a/./b", `"."`},
		{"file:/a//b", "empty segment"},
		{"file:/a/", `end in "/"`},
	} {
		err := ValidSecretPattern(tc.entry)
		if err == nil {
			t.Errorf("ValidSecretPattern(%q) = nil, want a refusal", tc.entry)
			continue
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", tc.entry)) {
			t.Errorf("ValidSecretPattern(%q) = %q: does not name the entry", tc.entry, err)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ValidSecretPattern(%q) = %q: want it to mention %q", tc.entry, err, tc.want)
		}
	}
	// Rule 1 is the whole entry, checked before either half: a 257-byte
	// entry is refused by its length even though its scheme is legal.
	if err := ValidSecretPattern(long); err == nil || strings.Contains(err.Error(), "scheme") {
		t.Errorf("a 257-byte entry must be refused by the total bound first, got %v", err)
	}
	for _, ok := range []string{
		"env:SANDBOX_*", "env:*", "file:/etc/orkstack/secrets/sandbox/*", "file:/*",
		"env:DB?", "env:A..B", "vault:x", "abcdefghijklmnop:X", `env:DB\*`, "env:DB[a-z]",
		"abcdefghijklmnop:" + strings.Repeat("A", 239),
	} {
		if err := ValidSecretPattern(ok); err != nil {
			t.Errorf("ValidSecretPattern(%q) = %v, want nil", ok, err)
		}
	}
}

func TestValidSecretPatterns_BoundsAndDuplicates(t *testing.T) {
	entries := make([]string, 0, MaxSecretPatterns+1)
	for i := 0; i < MaxSecretPatterns; i++ {
		entries = append(entries, fmt.Sprintf("env:P%d_*", i))
	}
	if err := ValidSecretPatterns(entries); err != nil {
		t.Fatalf("%d entries: %v", len(entries), err)
	}
	entries = append(entries, "env:ONE_TOO_MANY")
	if err := ValidSecretPatterns(entries); err == nil || !strings.Contains(err.Error(), "33") {
		t.Fatalf("33 entries: %v, want a refusal naming the count", err)
	}
	err := ValidSecretPatterns([]string{"env:A", "env:B", "env:A"})
	if err == nil || !strings.Contains(err.Error(), "0") || !strings.Contains(err.Error(), "2") ||
		!strings.Contains(err.Error(), `"env:A"`) {
		t.Fatalf("duplicate: %v, want both indexes and the entry", err)
	}
	err = ValidSecretPatterns([]string{"env:A", "file:/a/../b"})
	if err == nil || !strings.Contains(err.Error(), "1") || !strings.Contains(err.Error(), `"file:/a/../b"`) {
		t.Fatalf("malformed: %v, want the index and the entry", err)
	}
	if err := ValidSecretPatterns(nil); err != nil {
		t.Fatalf("nil: %v", err)
	}
}

func TestSecretPolicy_EmptyListPermitsEverything(t *testing.T) {
	for _, source := range []string{"file:/a/../b", "NO_SCHEME", "", "vault:x", "file:relative"} {
		if err := CheckSecretSource(nil, source); err != nil {
			t.Errorf("nil list, %q: %v", source, err)
		}
		if err := CheckSecretSource([]string{}, source); err != nil {
			t.Errorf("empty list, %q: %v", source, err)
		}
	}
}

func TestSecretPolicyVectors_AreExportedForTheBoundaries(t *testing.T) {
	vectors := SecretPolicyVectors()
	if len(vectors) < 17 {
		t.Fatalf("SecretPolicyVectors() has %d rows, want at least the spec's 17", len(vectors))
	}
	for _, v := range vectors {
		assertVector(t, v)
	}
}
