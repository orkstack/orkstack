package provider_test

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

func TestValidName(t *testing.T) {
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"aws", true},
		{"vault2", true},
		{"a", true},
		{"abcdefghijklmnop", true},
		{"abcdefghijklmnopq", false}, // 17 bytes
		{"", false},
		{"env", false},
		{"file", false},
		{"Aws", false},
		{"2aws", false},
		{"my-vault", false}, // a scheme has no "-"
		{"aws:", false},
	} {
		if err := provider.ValidName(tc.name); (err == nil) != tc.ok {
			t.Errorf("ValidName(%q) = %v; want ok=%v", tc.name, err, tc.ok)
		}
	}
}

// locatorCases are the table of the locator rule, and the fuzz target's seeds.
var locatorCases = []struct {
	locator string
	ok      bool
}{
	{"secret/data/payments/db#url", true},
	{"ssm:/payments/prod/license", true},
	{"secretsmanager:prod/db#password", true},
	{"kms:alias/payments-secrets", true},
	{"sts:arn:aws:iam::123456789012:role/payments-deploy", true},
	{"database/creds/payments-rw", true},
	{"a+b=c@d.e_f-g", true},
	{"", false},
	{"/leading", false},
	{"trailing/", false},
	{"a//b", false},
	{"a/./b", false},
	{"a/../b", false},
	{"..", false},
	{".", false},
	{"a%2fb", false},
	{"a?b", false},
	{"a b", false},
	{"a\tb", false},
	{"a\x00b", false},
	{"a#", false},
	{"a#b#c", false},
	{"a#b/c", false},
	{"é", false},
	{strings.Repeat("a", provider.MaxLocatorBytes), true},
	{strings.Repeat("a", provider.MaxLocatorBytes+1), false},
}

func TestValidLocator(t *testing.T) {
	for _, tc := range locatorCases {
		if err := provider.ValidLocator(tc.locator); (err == nil) != tc.ok {
			t.Errorf("ValidLocator(%q) = %v; want ok=%v", tc.locator, err, tc.ok)
		}
	}
}

func TestPolicyString_DropsTheField(t *testing.T) {
	for _, tc := range []struct{ name, locator, want string }{
		{"vault", "secret/data/payments/db#url", "vault:secret/data/payments/db"},
		{"aws", "ssm:/payments/prod/license", "aws:ssm:/payments/prod/license"},
		{"aws", "secretsmanager:prod/db#password", "aws:secretsmanager:prod/db"},
	} {
		if got := provider.PolicyString(tc.name, tc.locator); got != tc.want {
			t.Errorf("PolicyString(%q, %q) = %q; want %q", tc.name, tc.locator, got, tc.want)
		}
	}
}

// FuzzLocator: a locator the rule accepts is one a pattern cannot be escaped
// by, and its policy string begins with the provider's name and never holds a
// field selector.
func FuzzLocator(f *testing.F) {
	for _, tc := range locatorCases {
		f.Add(tc.locator)
	}
	f.Fuzz(func(t *testing.T, locator string) {
		if provider.ValidLocator(locator) != nil {
			return
		}
		path, _, _ := strings.Cut(locator, "#")
		if strings.ContainsAny(locator, "%? \\") || strings.Count(locator, "#") > 1 {
			t.Fatalf("ValidLocator accepted %q", locator)
		}
		for _, segment := range strings.Split(path, "/") {
			if segment == "" || segment == "." || segment == ".." {
				t.Fatalf("ValidLocator accepted %q with segment %q", locator, segment)
			}
		}
		for _, r := range locator {
			if r < 0x21 || r > 0x7e {
				t.Fatalf("ValidLocator accepted %q holding %q", locator, r)
			}
		}
		policy := provider.PolicyString("p", locator)
		if !strings.HasPrefix(policy, "p:") || strings.Contains(policy, "#") {
			t.Fatalf("PolicyString(%q) = %q", locator, policy)
		}
	})
}

func TestValidFileNameAndBundle(t *testing.T) {
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"process.json", true}, {"config", true}, {"id_ed25519-cert.pub", true},
		{"", false}, {".hidden", false}, {"..", false}, {"a/b", false},
		{"current", false}, {"expiration", false}, {strings.Repeat("a", 129), false},
	} {
		if err := provider.ValidFileName(tc.name); (err == nil) != tc.ok {
			t.Errorf("ValidFileName(%q) = %v; want ok=%v", tc.name, err, tc.ok)
		}
	}
	files := func(n int) map[string][]byte {
		m := map[string][]byte{}
		for i := range n {
			m["f"+strings.Repeat("x", i)] = []byte("v")
		}
		return m
	}
	for _, tc := range []struct {
		name string
		b    provider.Bundle
		ok   bool
	}{
		{"one file", provider.Bundle{Files: files(1)}, true},
		{"the limit", provider.Bundle{Files: files(provider.MaxBundleFiles)}, true},
		{"over the limit", provider.Bundle{Files: files(provider.MaxBundleFiles + 1)}, false},
		{"empty", provider.Bundle{}, false},
		{"plain names no file", provider.Bundle{Files: files(1), Plain: map[string]bool{"other": true}}, false},
		{"negative lifetime", provider.Bundle{Files: files(1), Lifetime: -1}, false},
		{"a lifetime too short to refresh", provider.Bundle{Files: files(1), Lifetime: time.Second}, false},
		{"the shortest lifetime", provider.Bundle{Files: files(1), Lifetime: provider.MinBundleLifetime}, true},
		{"a lifetime past 31 days", provider.Bundle{Files: files(1), Lifetime: 32 * 24 * time.Hour}, false},
		{"a file too large", provider.Bundle{Files: map[string][]byte{"big": make([]byte, provider.MaxValueBytes+1)}}, false},
	} {
		if err := provider.ValidBundle(tc.b); (err == nil) != tc.ok {
			t.Errorf("%s: ValidBundle = %v; want ok=%v", tc.name, err, tc.ok)
		}
	}
}

// paramsCases are the table of the params bounds and the fuzz target's seeds.
var paramsCases = []struct {
	name    string
	params  map[string]string
	allowed []string
	ok      bool
}{
	{"none", nil, nil, true},
	{"allowed keys", map[string]string{"common_name": "payments.internal", "ttl": "12h"}, []string{"common_name", "ttl"}, true},
	{"a key not allowed", map[string]string{"format": "der"}, []string{"common_name"}, false},
	{"a reference that takes none", map[string]string{"ttl": "1h"}, nil, false},
	{"a key of the wrong shape", map[string]string{"Common": "x"}, []string{"Common"}, false},
	{"a control character", map[string]string{"ttl": "1h\n"}, []string{"ttl"}, false},
	{"a C1 control character", map[string]string{"ttl": "1h\u0085"}, []string{"ttl"}, false},
	{"invalid UTF-8", map[string]string{"ttl": "\xff"}, []string{"ttl"}, false},
	{"a value too long", map[string]string{"ttl": strings.Repeat("a", provider.MaxParamValueBytes+1)}, []string{"ttl"}, false},
}

func TestValidParams(t *testing.T) {
	for _, tc := range paramsCases {
		if err := provider.ValidParams(tc.params, tc.allowed); (err == nil) != tc.ok {
			t.Errorf("%s: ValidParams = %v; want ok=%v", tc.name, err, tc.ok)
		}
	}
	many := map[string]string{}
	var keys []string
	for i := range provider.MaxParams + 1 {
		key := "k" + strings.Repeat("a", i)
		many[key] = "v"
		keys = append(keys, key)
	}
	if err := provider.ValidParams(many, keys); err == nil {
		t.Errorf("ValidParams accepted %d keys; at most %d", len(many), provider.MaxParams)
	}
	big := map[string]string{}
	var bigKeys []string
	for i := range 5 {
		key := "k" + strings.Repeat("b", i)
		big[key] = strings.Repeat("v", provider.MaxParamValueBytes)
		bigKeys = append(bigKeys, key)
	}
	if err := provider.ValidParams(big, bigKeys); err == nil {
		t.Error("ValidParams accepted 5 KiB of params; at most 4")
	}
}

// FuzzWriteParams: params the bounds accept hold only allowed keys, of the
// key shape, with values of bounded UTF-8 and no control character, within
// the total bound.
func FuzzWriteParams(f *testing.F) {
	for _, tc := range paramsCases {
		for k, v := range tc.params {
			f.Add(k, v, strings.Join(tc.allowed, ","))
		}
	}
	f.Fuzz(func(t *testing.T, key, value, allowedList string) {
		allowed := strings.Split(allowedList, ",")
		if provider.ValidParams(map[string]string{key: value}, allowed) != nil {
			return
		}
		found := false
		for _, a := range allowed {
			found = found || a == key
		}
		if !found {
			t.Fatalf("accepted key %q outside %q", key, allowed)
		}
		if len(value) > provider.MaxParamValueBytes || len(key)+len(value) > provider.MaxParamsBytes {
			t.Fatalf("accepted %d bytes", len(value))
		}
		if !strings.HasPrefix(key, strings.ToLower(key[:1])) || strings.ToLower(key) != key {
			t.Fatalf("accepted key %q of the wrong shape", key)
		}
		for _, r := range value {
			if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
				t.Fatalf("accepted a control character in %q", value)
			}
		}
	})
}

// FuzzBundle: a bundle the rules accept can be put in place under its
// directory and nowhere else: every name is one plain path component, the
// Orker's own names are not among them, and every Plain entry names a file.
func FuzzBundle(f *testing.F) {
	for _, seed := range []string{"password,username", "process.json", "../x", ".v1", "current", "a/b", "expiration", ""} {
		f.Add(seed, int64(time.Hour))
	}
	f.Fuzz(func(t *testing.T, names string, lifetime int64) {
		b := provider.Bundle{Files: map[string][]byte{}, Plain: map[string]bool{}, Lifetime: time.Duration(lifetime)}
		for i, name := range strings.Split(names, ",") {
			b.Files[name] = []byte("v")
			if i%2 == 1 {
				b.Plain[name] = true
			}
		}
		if provider.ValidBundle(b) != nil {
			return
		}
		if len(b.Files) > provider.MaxBundleFiles || b.Lifetime < 0 {
			t.Fatalf("accepted %d files, lifetime %s", len(b.Files), b.Lifetime)
		}
		for name := range b.Files {
			if name == "" || strings.ContainsAny(name, "/\\\x00") || strings.HasPrefix(name, ".") || name == "current" || name == "expiration" {
				t.Fatalf("accepted the file name %q", name)
			}
		}
	})
}
