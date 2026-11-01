package hostpolicy

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// engine is the API listener's settings, as internal/api names them.
var engine = Listener{Name: "engine", AddrVar: "ORK_HTTP_ADDR", AllowedVar: "ORK_ALLOWED_HOSTS", AllowedKey: "engine.allowed_hosts"}

func mustNew(t testing.TB, listen string, allowed ...string) Policy {
	t.Helper()
	policy, err := New(engine, listen, allowed)
	if err != nil {
		t.Fatalf("New(%q, %q): %v", listen, allowed, err)
	}
	return policy
}

// defaultCases are Hosts judged by the policy of a listener on listen with
// nothing allowed. FuzzHostPolicy seeds from them.
var defaultCases = []struct {
	listen string
	hosts  []string
	admit  bool
}{
	{":8080", []string{
		// Addresses, v4 and v6, with and without a port or a zone.
		"127.0.0.1:8080", "127.0.0.1", "10.0.0.5:30080", "[::1]:8080", "[::1]", "::1", "[fe80::1%25eth0]:8080",
		// localhost and the names under it, any case, a trailing dot.
		"localhost:8080", "LOCALHOST", "localhost.", "localhost:5173", "app.localhost", "A.b.LocalHost.:8080",
		// A name with no dot: a compose service, a Service, a short name, in punycode too.
		"ork:8080", "orkstack-engine", "ORK", "ork_1:8080", "xn--bcher-kva:8080", "0xg", "123abc",
		// No Host at all, which no browser sends.
		"",
	}, true},
	{":8080", []string{
		"evil.example", "evil.example:8080", "EVIL.EXAMPLE", "Evil.Example.", "localhost.evil.example",
		"127.0.0.1.nip.io:8080", "2130706433.example", "xn--bcher-kva.example",
		"ork.internal:8080", "ork.orkstack", "ork.orkstack.svc", "ork.orkstack.svc.cluster.local:8080",
		// A dotless name written fully qualified is a top-level name of public DNS.
		"evil.", "evil.:8080",
		// A dotless name a browser reads as an IPv4 address and would send dotted.
		"2130706433", "2130706433:8080", "0x7f000001", "0X7F000001:8080", "017700000001", "0x",
		// Not a name at all.
		".", ":8080", "evil@localhost", "a:b:c.localhost", "bücher", "evil:.com", "ork:80x", "[::1]:http",
	}, false},
	{"ork.internal:8080", []string{"ork.internal:8080", "ork.internal", "ORK.internal.", "127.0.0.1:8080"}, true},
	{"ork.internal:8080", []string{"other.internal:8080", "evil.ork.internal"}, false},
	{"0.0.0.0:8080", []string{"0.0.0.0:8080", "192.168.1.20:8080"}, true},
	{"0.0.0.0:8080", []string{"evil.example"}, false},
	{"", []string{"localhost", "ork:8080", "127.0.0.1"}, true},
	{"", []string{"evil.example"}, false},
}

// With nothing set, a listener answers the names a DNS-rebinding page cannot
// give: an address, localhost and the names under it, a name with no dot,
// and the name it was told to listen on. Every other name is refused,
// whatever its port or case.
func TestPolicy_TheDefaultAdmitsOnlyNamesARebindingPageCannotGive(t *testing.T) {
	for _, tc := range defaultCases {
		policy := mustNew(t, tc.listen)
		for _, host := range tc.hosts {
			if got := policy.Allows(host); got != tc.admit {
				t.Errorf("listening on %q, Host %q: admitted = %t, want %t", tc.listen, host, got, tc.admit)
			}
		}
	}
}

// allowedEntries and allowedCases: the setting's entries, and Hosts judged by
// the policy they build. FuzzHostPolicy seeds from them.
var (
	allowedEntries = []string{"orkstack.example.com", " *.Corp.Example ", "", "192.168.0.9", "[::1]",
		"xn--bcher-kva.example", "*.localhost", "2130706433"}
	allowedCases = map[string]bool{
		"orkstack.example.com":      true,
		"ORKSTACK.EXAMPLE.COM:443":  true,
		"orkstack.example.com.":     true,
		"XN--BCHER-KVA.example:443": true,
		"bücher.example":            false,
		"a.corp.example":            true,
		"b.a.corp.example:8443":     true,
		"corp.example":              false,
		"xcorp.example":             false,
		"orkstack.example.com.evil": false,
		"evil.example":              false,
		"127.0.0.1:8080":            true,
		"localhost":                 true,
		"a.localhost":               true,
		"ork":                       true,
		// A number a browser never sends is admitted once the operator lists it.
		"2130706433": true,
		"0x7f000001": false,
	}
)

// ORK_ALLOWED_HOSTS adds to the defaults: a name, every name under a
// *.suffix (never the suffix itself), or * for any Host.
func TestPolicy_AllowedHostsAddNamesAndPatterns(t *testing.T) {
	policy := mustNew(t, ":8080", allowedEntries...)
	for host, want := range allowedCases {
		if got := policy.Allows(host); got != want {
			t.Errorf("Host %q: admitted = %t, want %t", host, got, want)
		}
	}
	star := mustNew(t, ":8080", "*")
	for _, host := range []string{"evil.example", "anything.at.all:1", "localhost", "2130706433"} {
		if !star.Allows(host) {
			t.Errorf("with *, Host %q was refused", host)
		}
	}
}

// refusedEntries are setting entries refused at boot. FuzzHostPolicy seeds
// from them.
var refusedEntries = []string{
	"http://orkstack.example.com", "orkstack.example.com:8080", "ork*.example.com", "*example.com",
	"a/b", "*.", ".", "..", "a..b", "-a.example", "*.*.example", "ex ample", "é.example",
	// Half-bracketed addresses (FuzzHostPolicy found "::]").
	"::]", "[::1",
}

// topLevelPatterns are *.suffix entries over a single label: every name a
// page's author may register under that top-level name.
var topLevelPatterns = []string{"*.com", "*.COM", "*.internal", "*.example.", "*.xn--p1ai"}

// An entry that is not a name or a pattern, and a pattern over a top-level
// name, are refused at boot, naming the entry and the setting, rather than
// silently admitting or refusing something the operator did not mean.
func TestNew_RefusesAnEntryThatIsNotANameOrAPattern(t *testing.T) {
	for _, entry := range refusedEntries {
		_, err := New(engine, ":8080", []string{entry})
		if err == nil || !strings.Contains(err.Error(), "ORK_ALLOWED_HOSTS") || !strings.Contains(err.Error(), entry) {
			t.Errorf("entry %q: err = %v, want a refusal naming ORK_ALLOWED_HOSTS and the entry", entry, err)
		}
	}
	for _, entry := range topLevelPatterns {
		_, err := New(engine, ":8080", []string{entry})
		if err == nil || !strings.Contains(err.Error(), "ORK_ALLOWED_HOSTS") || !strings.Contains(err.Error(), entry) ||
			!strings.Contains(err.Error(), "top-level name") {
			t.Errorf("entry %q: err = %v, want a refusal naming the entry and the top-level name it covers", entry, err)
		}
	}
	if _, err := New(engine, "ork.internal:8080:9", nil); err == nil || !strings.Contains(err.Error(), "ORK_HTTP_ADDR") {
		t.Errorf("a listen address that is not one: err = %v, want a refusal naming ORK_HTTP_ADDR", err)
	}
	admin := Listener{Name: "admin listener", AddrVar: "ORK_ADMIN_ADDR", AllowedVar: "ORK_ADMIN_ALLOWED_HOSTS"}
	if _, err := New(admin, "admin:9464", []string{"*.com"}); err == nil || !strings.Contains(err.Error(), "ORK_ADMIN_ALLOWED_HOSTS") {
		t.Errorf("an admin entry: err = %v, want a refusal naming the admin listener's setting", err)
	}
}

// Judging a name Host allocates nothing when it is written in lower case,
// admitted or refused, so a flood of requests costs the guard no garbage;
// folding an upper-case name costs one copy.
func TestPolicy_AllowsAllocatesNothingForALowerCaseName(t *testing.T) {
	policy := mustNew(t, "ork.internal:8080", "orkstack.example.com", "*.corp.example")
	for _, tc := range []struct {
		host   string
		allocs float64
	}{
		{"orkstack.example.com:443", 0}, {"a.corp.example", 0}, {"ork.internal:8080", 0}, {"ork:8080", 0},
		{"ork_1", 0}, {"evil.example:8080", 0}, {"2130706433", 0}, {"localhost", 0}, {"app.localhost:5173", 0},
		{"127.0.0.1:8080", 0}, {"ORKSTACK.example.com", 1},
	} {
		if got := testing.AllocsPerRun(100, func() { policy.Allows(tc.host) }); got > tc.allocs {
			t.Errorf("Allows(%q): %.0f allocations, want at most %.0f", tc.host, got, tc.allocs)
		}
	}
}

// A request with a Host the policy refuses is answered 403 before any route
// sees it: as a Connect error naming the Host and the setting on a Connect
// path, as plain text anywhere else, never cached, never sniffed, and under
// the policy that loads nothing.
func TestPolicy_GuardRefusesAForeignHostBeforeAnyHandler(t *testing.T) {
	reached := 0
	var logged strings.Builder
	guard := mustNew(t, ":8080").Guard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached++
		w.WriteHeader(http.StatusNoContent)
	}), slog.New(slog.NewTextHandler(&logged, nil)))

	request := func(method, target, host string) *http.Response {
		req := httptest.NewRequest(method, target, strings.NewReader("{}"))
		req.Host = host
		rec := httptest.NewRecorder()
		guard.ServeHTTP(rec, req)
		return rec.Result()
	}
	for _, host := range []string{"127.0.0.1:8080", "localhost:8080", "ork:8080"} {
		if res := request(http.MethodGet, "/runs", host); res.StatusCode != http.StatusNoContent {
			t.Errorf("Host %q: %d, want the route's 204", host, res.StatusCode)
		}
	}
	admitted := reached

	connectRes := request(http.MethodPost, "/orkstack.v1.RunService/ListRuns", "evil.example:8080")
	var connectErr struct{ Code, Message string }
	if err := json.NewDecoder(connectRes.Body).Decode(&connectErr); err != nil {
		t.Fatalf("decode the Connect error: %v", err)
	}
	if connectRes.StatusCode != http.StatusForbidden || connectRes.Header.Get("Content-Type") != "application/json" ||
		connectErr.Code != "permission_denied" || !strings.Contains(connectErr.Message, `"evil.example:8080"`) ||
		!strings.Contains(connectErr.Message, "ORK_ALLOWED_HOSTS") {
		t.Errorf("a Connect call with a foreign Host: %d %s %+v", connectRes.StatusCode, connectRes.Header.Get("Content-Type"), connectErr)
	}
	for _, target := range []string{"/", "/runs", "/ws/runs/ork-1", "/v1/webhooks"} {
		res := request(http.MethodGet, target, "evil.example")
		text, _ := io.ReadAll(res.Body)
		if res.StatusCode != http.StatusForbidden || !strings.Contains(string(text), "ORK_ALLOWED_HOSTS") {
			t.Errorf("GET %s with a foreign Host: %d %q, want 403 naming ORK_ALLOWED_HOSTS", target, res.StatusCode, text)
		}
		for header, want := range map[string]string{
			"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Content-Security-Policy": RefusalPolicy,
		} {
			if got := res.Header.Get(header); got != want {
				t.Errorf("GET %s with a foreign Host: %s %q, want %q", target, header, got, want)
			}
		}
	}
	if reached != admitted {
		t.Errorf("a refused request reached the route %d times", reached-admitted)
	}
	// Five refusals in a burst are one log line, naming the Host and the
	// setting: a page that loops cannot flood the log.
	if lines := strings.Count(logged.String(), "request refused: Host not allowed"); lines != 1 ||
		!strings.Contains(logged.String(), "evil.example:8080") || !strings.Contains(logged.String(), "ORK_ALLOWED_HOSTS") {
		t.Errorf("five refusals logged %d lines, want one naming the Host and the setting:\n%s", lines, logged.String())
	}
}
