package hostpolicy

import (
	"regexp"
	"strings"
	"testing"
)

// FuzzHostPolicy: a request's Host header is input OrkStack does not
// control, and an allowlist entry is one the operator writes. Whatever
// either holds, nothing panics; an entry the policy accepts admits the name
// it spells, and a refused one names the setting; an ASCII Host is judged
// the same in any case; and a Host is admitted only when it is empty, an
// address, or a DNS name the policy names: localhost or a name under it, a
// name with no dot, no trailing one and not a number, the listen name, a
// listed name, or a name under a listed suffix of two labels or more. The
// seeds are every row of the table tests and the inputs the fuzzer found.
func FuzzHostPolicy(f *testing.F) {
	for _, tc := range defaultCases {
		for _, host := range tc.hosts {
			f.Add("", host)
		}
	}
	for host := range allowedCases {
		for _, entry := range allowedEntries {
			f.Add(entry, host)
		}
	}
	for _, entry := range append(refusedEntries, topLevelPatterns...) {
		f.Add(entry, "evil.example")
	}
	for _, seed := range []struct{ entry, host string }{
		{"", "127.0.0.1:8080"}, {"orkstack.example.com", "ORKSTACK.example.com:443"},
		{"*.corp.example", "a.corp.example"}, {"*.corp.example", "corp.example"}, {"*", "evil.example"},
		{"*.", "x"}, {"ork*.example", "ork1.example"}, {"[::1]", "[::1]:8080"}, {"a..b", "a..b"},
		{"named.example", "evil."}, {"named.example", "[fe80::1%25eth0]:1"}, {"", ""}, {"x", "x.:80"},
		{"", "localhost.evil.example"}, {"", "ork_1:8080"}, {"-a.example", "-a.example"},
		{"", "App.LocalHost."}, {"xn--bcher-kva.example", "XN--BCHER-KVA.EXAMPLE"}, {"", "bücher"},
		{"", "evil@localhost"}, {"", "a:b:c.localhost"}, {"", "."}, {"i.example", "ı.example"},
		{"", "evil:.com"}, {"", "ork:80x"}, {"", "[::1]:http"},
	} {
		f.Add(seed.entry, seed.host)
	}
	f.Fuzz(func(t *testing.T, entry, host string) {
		policy, err := New(engine, "ork.internal:8080", []string{entry})
		if err != nil {
			if !strings.Contains(err.Error(), "ORK_ALLOWED_HOSTS") {
				t.Fatalf("New(%q) refused without naming the setting: %v", entry, err)
			}
			return
		}
		listed := strings.TrimSuffix(asciiLower(strings.TrimSpace(entry)), ".")
		if listed != "" && !strings.Contains(listed, "*") && !policy.Allows(listed) {
			t.Fatalf("entry %q was accepted, but Host %q is refused", entry, listed)
		}
		got := policy.Allows(host)
		if isASCII(host) && got != policy.Allows(strings.ToUpper(host)) {
			t.Fatalf("Host %q and its upper case are judged apart", host)
		}
		// Stated apart from the policy's own helpers: a dotless Host a
		// browser reads as an IPv4 number, with or without a port, is
		// refused unless the operator listed exactly that name.
		if m := browserNumber.FindStringSubmatch(strings.ToLower(host)); m != nil && got && !policy.any && m[1] != listed {
			t.Fatalf("Host %q was admitted, but a browser reads %q as an IPv4 address", host, m[1])
		}
		if !got || policy.any {
			return
		}
		name, qualified, ok := hostName(host)
		switch {
		case host == "":
		case !ok:
			t.Fatalf("Host %q was admitted, but its port is not a number", host)
		case isAddress(name):
		case !validName(name):
			t.Fatalf("Host %q was admitted, but %q is not a DNS name", host, name)
		case name == "localhost", strings.HasSuffix(name, ".localhost"), name == "ork.internal", name == listed:
		case !strings.Contains(name, ".") && !qualified && !isNumber(name):
		case strings.HasPrefix(listed, "*.") && strings.Contains(listed[2:], ".") && strings.HasSuffix(name, listed[1:]):
		default:
			t.Fatalf("Host %q (name %q) was admitted under entry %q, which names no such name", host, name, entry)
		}
	})
}

// browserNumber matches a Host whose name is one label a browser's URL
// parser reads as an IPv4 number (WHATWG URL): decimal or octal digits, or
// 0x and hexadecimal digits, with an optional numeric port.
var browserNumber = regexp.MustCompile(`^(0x[0-9a-f]*|[0-9]+)(:[0-9]*)?$`)

func isASCII(s string) bool {
	for i := range len(s) {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
