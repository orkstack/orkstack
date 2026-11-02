package hostpolicy

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
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

// A Host header is input the guard does not control, and one is judged on
// every request, so a flood of them (a page that loops, a probe aimed at the
// wrong port) costs only what judging costs: a few microseconds of CPU for a
// name, admitted or refused, and a cost that grows with the length of a
// hostile one no faster than reading it. The bound is the CPU time of the
// calling thread (threadCPU), which a host loaded by other processes cannot
// move, at about five times what judging took where it was measured (10 ms for
// the names, 25 ms for the long Hosts); a judgement that compiles a pattern
// or resolves a name costs far more.
func TestPolicy_AllowsJudgesAFloodOfHostsWithinACPUBound(t *testing.T) {
	// The race detector's instrumentation slows judging several times over
	// (race_enabled_test.go); the bound is scaled by it, never dropped.
	slowdown := time.Duration(1)
	if raceBuild {
		slowdown = raceSlowdown
	}
	policy := mustNew(t, "ork.internal:8080", "orkstack.example.com", "*.corp.example")
	var names []string
	names = append(names, allowedEntries...)
	for host := range allowedCases {
		names = append(names, host)
	}
	for _, tc := range defaultCases {
		names = append(names, tc.hosts...)
	}
	long := strings.Repeat("a.", 4<<10)
	for _, tc := range []struct {
		name   string
		hosts  []string
		rounds int
		bound  time.Duration // for all the judgements
	}{
		{"the names of the tables", names, 400, 60 * time.Millisecond},
		{"a Host as long as a header line", []string{long, strings.ToUpper(long), long + ":8080", "[" + long + "]:80",
			strings.Repeat("a", 8<<10) + ".example"}, 100, 120 * time.Millisecond},
	} {
		func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			startCPU, cpu := threadCPU()
			startWall := time.Now()
			judgements := 0
			for range tc.rounds {
				for _, host := range tc.hosts {
					policy.Allows(host)
					judgements++
				}
			}
			cost, clock := time.Since(startWall), "wall clock"
			if end, ok := threadCPU(); cpu && ok {
				cost, clock = end-startCPU, "thread CPU time"
			}
			t.Logf("%s: %d judgements took %v of %s", tc.name, judgements, cost, clock)
			if bound := tc.bound * slowdown; cost > bound {
				t.Errorf("%s: %d judgements took %v of %s, want at most %v", tc.name, judgements, cost, clock, bound)
			}
		}()
	}
}

// refusalRecords are the log lines of a guard writing JSON, one object each.
func refusalRecords(t *testing.T, logged *strings.Builder) []map[string]any {
	t.Helper()
	var records []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logged.String()), "\n") {
		if line == "" {
			continue
		}
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

// refuse sends guard one request for target with the Host host, and returns
// what the refusal answered.
func refuse(guard http.Handler, target, host string) *http.Response {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Host = host
	rec := httptest.NewRecorder()
	guard.ServeHTTP(rec, req)
	return rec.Result()
}

// fakeClock is a clock a test moves by hand.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// A page or a probe that loops is one log line per refusalLogInterval, and
// the line that ends an interval counts the refusals it did not log, so the
// log says how loud the loop was without being as loud: nothing is logged
// before the interval has passed, the first refusal after it is, the count
// starts over, and every refusal is still answered 403.
func TestPolicy_GuardLogsARefusalOncePerIntervalAndCountsTheRest(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	policy := mustNew(t, ":8080")
	policy.clock = clock.Now
	var logged strings.Builder
	guard := policy.Guard(http.NotFoundHandler(), slog.New(slog.NewJSONHandler(&logged, nil)))

	// step moves the clock and refuses one request, returning how many lines
	// that refusal logged.
	step := func(advance time.Duration, host string) int {
		clock.now = clock.now.Add(advance)
		before := len(refusalRecords(t, &logged))
		if res := refuse(guard, "/", host); res.StatusCode != http.StatusForbidden {
			t.Fatalf("Host %q: status %d, want 403 whether or not it was logged", host, res.StatusCode)
		}
		return len(refusalRecords(t, &logged)) - before
	}
	unlogged := func() any { // refused_unlogged of the last line
		records := refusalRecords(t, &logged)
		return records[len(records)-1]["refused_unlogged"]
	}

	if step(0, "first.example") != 1 || unlogged() != 0.0 {
		t.Fatalf("the first refusal: want one line counting no refusal before it, got %v", unlogged())
	}
	// Three more inside the interval, the last a nanosecond short of it.
	for _, advance := range []time.Duration{time.Second, time.Second, refusalLogInterval - 2*time.Second - time.Nanosecond} {
		if step(advance, "loop.example") != 0 {
			t.Fatalf("a refusal %v after the last line was logged before the interval of %v", advance, refusalLogInterval)
		}
	}
	// At the interval, the next is logged and counts the three before it.
	if step(time.Nanosecond, "later.example") != 1 || unlogged() != 3.0 {
		t.Fatalf("the refusal at the interval: want one line counting 3 refusals, got %v", unlogged())
	}
	records := refusalRecords(t, &logged)
	if got := records[len(records)-1]["host"]; got != "later.example" {
		t.Errorf("the line that ended the interval names Host %v, want later.example", got)
	}
	// The count starts over: the next line counts only what followed the last.
	if step(time.Second, "loop.example") != 0 {
		t.Fatal("a refusal right after a line was logged")
	}
	if step(refusalLogInterval, "again.example") != 1 || unlogged() != 1.0 {
		t.Fatalf("the refusal one interval on: want one line counting 1 refusal, got %v", unlogged())
	}
	// A burst far inside one interval is still one line.
	for range 50 {
		if step(time.Millisecond, "burst.example") != 0 {
			t.Fatal("a refusal a millisecond after a line was logged")
		}
	}
	if step(refusalLogInterval, "end.example") != 1 || unlogged() != 50.0 {
		t.Fatalf("after a burst of 50: want one line counting 50 refusals, got %v", unlogged())
	}
}

// A refused Host is echoed in the answer and the log so the operator can read
// what to allow, but only the first maxLoggedHost bytes of it, marked as cut:
// a hostile Host cannot make the guard write a line as long as a header.
func TestPolicy_GuardEchoesAtMostTheStartOfALongHost(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		cut  bool
	}{
		{"a short Host", 20, false},
		{"a Host at the bound", maxLoggedHost, false},
		{"a Host a byte over it", maxLoggedHost + 1, true},
		{"a Host of 8 KiB", 8 << 10, true},
	} {
		host := "evil." + strings.Repeat("x", tc.size-len("evil."))
		var logged strings.Builder
		guard := mustNew(t, ":8080").Guard(http.NotFoundHandler(), slog.New(slog.NewJSONHandler(&logged, nil)))
		res := refuse(guard, "/", host)
		answer, _ := io.ReadAll(res.Body)
		records := refusalRecords(t, &logged)
		if res.StatusCode != http.StatusForbidden || len(records) != 1 {
			t.Fatalf("%s: status %d with %d log lines, want a 403 and one line", tc.name, res.StatusCode, len(records))
		}
		wantEcho := host
		if tc.cut {
			wantEcho = host[:maxLoggedHost] + "…"
		}
		if !strings.Contains(string(answer), fmt.Sprintf("%q", wantEcho)) {
			t.Errorf("%s: the answer does not name the Host as %.40q… (%d bytes), got %.80q", tc.name, wantEcho, len(wantEcho), answer)
		}
		if got := records[0]["host"]; got != wantEcho {
			t.Errorf("%s: the log names the Host %.40q (%d bytes), want %.40q (%d bytes)", tc.name, got, len(fmt.Sprint(got)), wantEcho, len(wantEcho))
		}
		if tc.cut && (strings.Contains(string(answer), host[:maxLoggedHost+1]) || len(answer) > 1<<10) {
			t.Errorf("%s: the answer to a %d byte Host is %d bytes, which echoes more than the start of it", tc.name, len(host), len(answer))
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
	policy := mustNew(t, ":8080")
	// The burst is one instant, so no slow host can spread it over an interval.
	policy.clock = (&fakeClock{now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}).Now
	guard := policy.Guard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
