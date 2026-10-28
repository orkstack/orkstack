package dsl

import (
	"regexp"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// FuzzCheckSecretSource: the hand-written matcher behind a namespace's
// allowed_secrets and an Orker's secret floor is one rule: a source is
// permitted when a pattern of the same scheme matches its locator, "*"
// standing for any run of characters but "/" and every other character for
// itself; a file source with a relative path or an empty, "." or ".." segment
// is refused before any pattern is looked at; and an empty list restricts
// nothing. The oracle is a regular expression built from the pattern.
func FuzzCheckSecretSource(f *testing.F) {
	for _, seed := range [][3]string{
		{"env:SANDBOX_*", "", "env:SANDBOX_DB"}, {"env:SANDBOX_*", "", "env:PROD_DB"}, {"file:/etc/sec/*", "env:*", "file:/etc/sec/a"},
		{"file:/etc/sec/*", "", "file:/etc/sec/a/b"}, {"file:/etc/sec/*", "", "file:/etc/sec/../x"}, {"file:/etc/*/db", "", "file:/etc/x/db"},
		{"env:a*b*c", "", "env:abbc"}, {"env:a*b*c", "", "env:ac"}, {"env:*", "", "env:"}, {"env:?", "", "env:?"}, {"env:[a]", "", "env:a"},
		{"env:a.b", "", "env:aXb"}, {"", "", "env:x"}, {"env:x", "", "nocolon"}, {"env:x", "", ":x"}, {"file:/a/b", "", "file:a/b"},
		{"env:**", "", "env:a/b"}, {"env:a\\*", "", "env:a\\x"}, {"file:/a//b", "", "file:/a//b"}, {"file:/a/./b", "", "file:/a/./b"},
		{"env:é*", "", "env:éa"}, {"env:*é", "", "env:aé"},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, first, second, source string) {
		if !utf8.ValidString(first) || !utf8.ValidString(second) || !utf8.ValidString(source) {
			return // the oracle is a regular expression, which reads text
		}
		var patterns []string
		for _, p := range []string{first, second} {
			if p != "" {
				patterns = append(patterns, p)
			}
		}
		err := CheckSecretSource(patterns, source)
		if len(patterns) == 0 {
			if err != nil {
				t.Fatalf("an empty list refused %q: %v", source, err)
			}
			return
		}
		scheme, locator, found := strings.Cut(source, ":")
		want := false
		if found {
			refusedBeforeMatching := false
			if scheme == "file" {
				refusedBeforeMatching = !strings.HasPrefix(locator, "/")
				for _, segment := range strings.Split(strings.TrimPrefix(locator, "/"), "/") {
					if segment == "" || segment == "." || segment == ".." {
						refusedBeforeMatching = true
					}
				}
			}
			for _, entry := range patterns {
				entryScheme, pattern, ok := strings.Cut(entry, ":")
				if !ok || entryScheme != scheme || refusedBeforeMatching {
					continue
				}
				expression := "(?s)^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, "[^/]*") + "$"
				if regexp.MustCompile(expression).MatchString(locator) {
					want = true
				}
			}
		}
		if (err == nil) != want {
			t.Fatalf("CheckSecretSource(%q, %q) = %v; the pattern rules say permitted=%v", patterns, source, err, want)
		}
		if err != nil {
			if _, ok := err.(*SecretSourceRefusal); !ok || err.Error() == "" {
				t.Fatalf("CheckSecretSource refused as %T %v, not as a SecretSourceRefusal", err, err)
			}
		}
	})
}

// FuzzValidSecretPattern: an entry is accepted exactly when it is at most 256
// bytes, `scheme:pattern` with both halves, a scheme of up to 16 lower-case
// letters and digits that starts with a letter, a UTF-8 pattern with no control
// character, whitespace or comma, and, for a file pattern, an absolute path
// with no empty, "." or ".." segment. A list is at most 32 entries with no
// duplicate, and every refusal says which entry.
func FuzzValidSecretPattern(f *testing.F) {
	for _, seed := range []string{
		"env:SANDBOX_*", "file:/etc/orkstack/secrets/*", "file:/etc/../x", "file:/a/", "file:a", "env:", ":x", "env", "Env:x", "e-nv:x", "1env:x",
		"env:a,b", "env:a b", "env:a\tb", "env:a b", "env:é", "env:\xff", "env:a\x00", strings.Repeat("e", 17) + ":x", strings.Repeat("a", 257),
		"file:/a//b", "file:/./b", "env:a:b", "env:**", "env:[a-z]",
	} {
		f.Add(seed, seed)
	}
	f.Fuzz(func(t *testing.T, entry, other string) {
		scheme, pattern, found := strings.Cut(entry, ":")
		want := len(entry) <= 256 && found && scheme != "" && pattern != "" && len(scheme) <= 16 && utf8.ValidString(pattern)
		for i := 0; want && i < len(scheme); i++ {
			c := scheme[i]
			want = c >= 'a' && c <= 'z' || i > 0 && c >= '0' && c <= '9'
		}
		for _, r := range pattern {
			if want && (r < 0x20 || r == 0x7f || unicode.IsSpace(r) || r == ',') {
				want = false
			}
		}
		if want && scheme == "file" {
			want = strings.HasPrefix(pattern, "/") && !strings.HasSuffix(pattern, "/")
			for _, segment := range strings.Split(strings.TrimPrefix(pattern, "/"), "/") {
				if segment == "" || segment == "." || segment == ".." {
					want = false
				}
			}
		}
		err := ValidSecretPattern(entry)
		if (err == nil) != want {
			t.Fatalf("ValidSecretPattern(%q) = %v; the grammar says accepted=%v", entry, err, want)
		}
		list := []string{entry, other}
		listErr := ValidSecretPatterns(list)
		listWant := want && ValidSecretPattern(other) == nil && entry != other
		if (listErr == nil) != listWant {
			t.Fatalf("ValidSecretPatterns(%q) = %v; accepted=%v", list, listErr, listWant)
		}
		if listErr != nil && !strings.Contains(listErr.Error(), "entry") {
			t.Fatalf("ValidSecretPatterns(%q) refused as %q, which does not say which entry", list, listErr)
		}
	})
}

// FuzzParseBearerChallenge: a registry answers a request with a
// WWW-Authenticate header, which this parser reads to find the token service
// to ask. The header is the registry's, not ours. It reads only a Bearer
// challenge that names a realm, and a challenge written from plain realm,
// service and scope values (a scope with commas inside its quotes, as
// "repository:a:pull,push" is) reads back as those values.
func FuzzParseBearerChallenge(f *testing.F) {
	for _, seed := range []string{
		`Bearer realm="https://auth.example/token",service="registry.example",scope="repository:a/b:pull,push"`,
		`bearer realm=https://auth.example/token`, `Basic realm="x"`, `Bearer service="x"`, `Bearer realm=""`, `Bearer `, `Bearer`, ``,
		`Bearer realm="a",realm="b"`, `Bearer scope="a,b"realm="r"`, `Bearer realm="r\"s"`, `Bearer realm = "r" , service = "s"`,
		"Bearer realm=\"r\"\r\nX: y", `Bearer realm="a=b",service="c=d"`, `BEARER REALM="r"`,
	} {
		f.Add(seed, "https://auth.example/token", "registry.example", "repository:a/b:pull,push")
	}
	f.Fuzz(func(t *testing.T, header, realm, service, scope string) {
		got := parseBearerChallenge(header)
		if got != nil && (got.Realm == "" || !strings.HasPrefix(strings.ToLower(header), "bearer ")) {
			t.Fatalf("parseBearerChallenge(%q) = %+v", header, got)
		}
		for _, value := range []string{realm, service, scope} {
			if strings.ContainsAny(value, "\"\\") || strings.TrimSpace(value) != value {
				return
			}
		}
		if realm == "" {
			return
		}
		written := `Bearer realm="` + realm + `",service="` + service + `",scope="` + scope + `"`
		back := parseBearerChallenge(written)
		if back == nil || back.Realm != realm || back.Service != service || back.Scope != scope {
			t.Fatalf("a challenge written from %q, %q, %q reads back as %+v", realm, service, scope, back)
		}
	})
}

// FuzzParseCatalog: the activity catalog is the operator's table of short
// names to images. An accepted catalog holds only slug names mapped to
// references the distribution grammar accepts, and an empty document is the
// built-in table.
func FuzzParseCatalog(f *testing.F) {
	for _, seed := range []string{
		"", "activities: {}\n", "activities:\n  shell: localhost:5001/orkstack/activity-shell:dev\n", "something: else\n",
		"activities:\n  Shell: ghcr.io/x/y:1\n", "activities:\n  shell: \"\"\n", "activities:\n  shell: notareference\n",
		"activities:\n  shell: --cap-add=ALL.example/x:1\n", "activities:\n  a: ghcr.io/x/y@sha256:" + strings.Repeat("a", 64) + "\n",
		"activities:\n  a: ghcr.io/x/y:1\n  a: ghcr.io/x/z:1\n", "activities: [", "activities:\n  1: ghcr.io/x/y:1\n", "activities:\n  a: {}\n",
		"activities:\n  a: [x]\n", "activities:\n  a: ghcr.io/x/y:1\nextra: 1\n",
	} {
		f.Add([]byte(seed))
	}
	slug := regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	f.Fuzz(func(t *testing.T, document []byte) {
		catalog, err := ParseCatalog(document)
		if err != nil {
			if !strings.Contains(err.Error(), "ORK_ACTIVITY_CATALOG") || catalog != nil {
				t.Fatalf("ParseCatalog(%q) refused as %q (%v)", document, err, catalog)
			}
			return
		}
		if strings.TrimSpace(string(document)) == "" && len(catalog) != len(BuiltinActivityCatalog()) {
			t.Fatalf("an empty document is not the built-in table: %v", catalog)
		}
		for name, reference := range catalog {
			if !slug.MatchString(name) || len(name) > 200 {
				t.Fatalf("ParseCatalog(%q) accepted the activity name %q", document, name)
			}
			if _, err := ParseReference(reference); err != nil {
				t.Fatalf("ParseCatalog(%q) accepted %q for %q, which is not a reference: %v", document, reference, name, err)
			}
			if got, err := catalog.ResolveUses("t", name); err != nil || got != reference {
				t.Fatalf("a catalog name %q resolves to %q, %v; the table says %q", name, got, err, reference)
			}
		}
	})
}
