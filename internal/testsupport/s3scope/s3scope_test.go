package s3scope

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestPolicy_Allows(t *testing.T) {
	p := OrkerPolicy()
	cases := []struct {
		verb Verb
		key  string
		want bool
	}{
		{Put, "metadata/commits/ns/run/task/commit.json", true},
		{Get, "metadata/commits/ns/run/task/commit.json", true},
		{List, "metadata/commits/", true},
		{List, "metadata/commits/ns/", true},
		{Delete, "metadata/commits/ns/x", false},
		{Put, "metadata/attested/x", false},
		{Get, "metadata/run-context/x", true},
		{Put, "metadata/run-context/x", false},
		{Delete, "metadata/run-context/x", false},
		{List, "metadata/run-context/", false},
		{Get, "metadata/snapshots/x", false},
		{Get, "metadata/logs/x", false},
		{Put, "metadata/identity/access.json", false},
		{List, "metadata/", false},
		{List, "metadata/commit", false},
		{List, "", false},
		{Delete, "metadata/probes/x", true},
		{Put, "metadata/commitsx/y", false},
	}
	for _, tc := range cases {
		if got := p.Allows(tc.verb, tc.key); got != tc.want {
			t.Errorf("Allows(%s, %q) = %v, want %v", tc.verb, tc.key, got, tc.want)
		}
	}
	if p.Without("metadata/commits/", List).Allows(List, "metadata/commits/") {
		t.Error("Without did not take the listing off")
	}
	if !p.With("metadata/identity/", Get).Allows(Get, "metadata/identity/sentinel") {
		t.Error("With did not add the rule")
	}
}

// upstream is a store that knows one object, answers 404 NoSuchKey for any
// other read, and records the Host each request arrived with.
type upstream struct {
	mu     sync.Mutex
	hosts  []string
	server *httptest.Server
}

func newUpstream(t *testing.T) *upstream {
	u := &upstream{}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.hosts = append(u.hosts, r.Host)
		u.mu.Unlock()
		if (r.Method == http.MethodGet || r.Method == http.MethodHead) && r.URL.Query().Get("list-type") == "" &&
			!strings.HasSuffix(r.URL.Path, "/known") {
			writeError(w, http.StatusNotFound, "NoSuchKey", "missing")
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(u.server.Close)
	return u
}

// TestProxy_HoldsRequestsToThePolicy: allowed requests reach the store with
// their Host; every other one is answered AccessDenied, and a missing key
// under a prefix the caller may not list is answered 403, as AWS does.
func TestProxy_HoldsRequestsToThePolicy(t *testing.T) {
	u := newUpstream(t)
	p := New(t, u.server.URL, "b", OrkerPolicy().Without("metadata/attempts/", List).With("metadata/attempts/", Get))
	cases := []struct {
		name       string
		method     string
		path       string
		copyFrom   string
		wantStatus int
		wantCode   string
	}{
		{"an allowed put", http.MethodPut, "/b/metadata/commits/ns/x", "", 200, ""},
		{"a forbidden put", http.MethodPut, "/b/metadata/identity/access.json", "", 403, "AccessDenied"},
		{"a dot-dot key through an allowed prefix", http.MethodPut, "/b/metadata/probes/../identity/x", "", 403, "AccessDenied"},
		{"an empty segment", http.MethodPut, "/b/metadata/commits//x", "", 403, "AccessDenied"},
		{"another bucket", http.MethodPut, "/other/metadata/commits/x", "", 403, "AccessDenied"},
		{"a listing of an allowed prefix", http.MethodGet, "/b?list-type=2&prefix=metadata/commits/", "", 200, ""},
		{"a listing of the whole bucket", http.MethodGet, "/b?list-type=2", "", 403, "AccessDenied"},
		{"a listing of a parent", http.MethodGet, "/b?list-type=2&prefix=metadata/", "", 403, "AccessDenied"},
		{"a batch delete", http.MethodPost, "/b?delete", "", 403, "AccessDenied"},
		{"a missing key under a listable prefix", http.MethodGet, "/b/metadata/commits/ns/missing", "", 404, "NoSuchKey"},
		{"a missing key under a prefix not listed", http.MethodGet, "/b/metadata/attempts/missing", "", 403, "AccessDenied"},
		{"a known key under a prefix not listed", http.MethodGet, "/b/metadata/attempts/known", "", 200, ""},
		{"a copy from a forbidden source", http.MethodPut, "/b/metadata/commits/ns/y", "/b/metadata/identity/access.json", 403, "AccessDenied"},
		{"a copy from another bucket", http.MethodPut, "/b/metadata/commits/ns/y", "/other/metadata/commits/z", 403, "AccessDenied"},
		{"a copy inside the policy", http.MethodPut, "/b/metadata/commits/ns/y", "/b/metadata/commits/ns/z", 200, ""},
		{"an aborted upload of an artifact", http.MethodDelete, "/b/orkflows/ns/x?uploadId=u1", "", 200, ""},
		{"a delete of an artifact", http.MethodDelete, "/b/orkflows/ns/x", "", 403, "AccessDenied"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, p.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.copyFrom != "" {
				req.Header.Set("X-Amz-Copy-Source", tc.copyFrom)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d (%s), want %d", resp.StatusCode, body, tc.wantStatus)
			}
			if tc.wantCode != "" && !strings.Contains(string(body), "<Code>"+tc.wantCode+"</Code>") {
				t.Fatalf("body = %s, want code %s", body, tc.wantCode)
			}
		})
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	proxyHost := strings.TrimPrefix(p.URL, "http://")
	for _, h := range u.hosts {
		if h != proxyHost {
			t.Fatalf("the store saw Host %q, want the signed %q", h, proxyHost)
		}
	}
	if len(u.hosts) == 0 {
		t.Fatal("nothing reached the store")
	}
}

// TestProxy_Script answers before the policy, and records every request.
func TestProxy_Script(t *testing.T) {
	u := newUpstream(t)
	p := New(t, u.server.URL, "b", OrkerPolicy())
	p.Script(func(r Request) *Answer {
		if strings.HasPrefix(r.Key, "metadata/identity/") {
			return &Answer{Status: 412, Code: "PreconditionFailed"}
		}
		return nil
	})
	req, _ := http.NewRequest(http.MethodPut, p.URL+"/b/metadata/identity/x", nil)
	req.Header.Set("If-Match", `"0"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 412 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	got := p.Requests()
	if len(got) != 1 || got[0].Allowed || got[0].IfMatch != `"0"` || got[0].Verb != Put {
		t.Fatalf("requests = %+v", got)
	}
	p.Reset()
	if len(p.Requests()) != 0 {
		t.Fatal("Reset kept requests")
	}
}

// TestProxy_LoseAnswer: a matched write reaches the store and is answered
// 500; the next one is answered by the store.
func TestProxy_LoseAnswer(t *testing.T) {
	u := newUpstream(t)
	p := New(t, u.server.URL, "b", Everything())
	p.LoseAnswer(func(r Request) bool { return r.Verb == Put }, 1)
	for i, want := range []int{500, 200} {
		req, _ := http.NewRequest(http.MethodPut, p.URL+"/b/metadata/schedules/x", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("request %d answered %d, want %d", i, resp.StatusCode, want)
		}
	}
	u.mu.Lock()
	reached := len(u.hosts)
	u.mu.Unlock()
	if reached != 2 || p.LostSeen() != 1 {
		t.Fatalf("the store saw %d requests and %d answers were lost; want 2 and 1", reached, p.LostSeen())
	}
}

// TestOpen: an open proxy refuses nothing, a bucket's own requests included;
// a script still answers first; and After runs once the store has answered
// and before the client hears.
func TestOpen(t *testing.T) {
	u := newUpstream(t)
	p := Open(t, u.server.URL, "b")
	p.Script(func(r Request) *Answer {
		if r.Key == "cut" {
			return &Answer{Status: 503, Code: "SlowDown"}
		}
		return nil
	})
	after := make(chan int, 4)
	var heard bool
	p.After(func(r Request) {
		u.mu.Lock()
		defer u.mu.Unlock()
		if heard {
			t.Error("the client heard first")
		}
		if want := map[string]int{"": 404, "known": 200}[r.Key]; r.Status != want {
			t.Errorf("the store answered %q %d, After saw %d", r.Key, want, r.Status)
		}
		after <- len(u.hosts)
	})
	for path, want := range map[string]int{"/b": 404, "/b/known": 200, "/b/cut": 503} {
		u.mu.Lock()
		heard = false
		u.mu.Unlock()
		resp, err := http.Get(p.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		u.mu.Lock()
		heard = true
		u.mu.Unlock()
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s answered %d, want %d", path, resp.StatusCode, want)
		}
	}
	if one, two := <-after, <-after; one+two != 3 || len(after) != 0 {
		t.Fatalf("After saw the store after %d and %d requests, and %d more", one, two, len(after))
	}
}

func TestPolicy_SeaweedActions(t *testing.T) {
	b := "orkstack-artifacts/"
	want := []string{
		"Read:" + b + "orkflows/*", "Write:" + b + "orkflows/*", "List:" + b + "orkflows/*",
		"Read:" + b + "manifests/*", "Write:" + b + "manifests/*", "List:" + b + "manifests/*",
		"Read:" + b + "metadata/commits/*", "Write:" + b + "metadata/commits/*", "List:" + b + "metadata/commits/*",
		"Read:" + b + "metadata/attested/*", "List:" + b + "metadata/attested/*",
		"Read:" + b + "metadata/memo/*", "Write:" + b + "metadata/memo/*", "List:" + b + "metadata/memo/*",
		"Write:" + b + "metadata/logs/*",
		"Write:" + b + "metadata/attempts/*", "List:" + b + "metadata/attempts/*",
		"Read:" + b + "metadata/probes/*", "Write:" + b + "metadata/probes/*", "List:" + b + "metadata/probes/*",
		"Write:" + b + "metadata/receipts/*",
		"Write:" + b + "metadata/staging/event-sources/*",
		"Read:" + b + "metadata/run-context/*",
	}
	got := OrkerPolicy().SeaweedActions("orkstack-artifacts")
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("SeaweedActions:\n got %v\nwant %v", got, want)
	}
	// What the Orker may only read is never written, and the run context is
	// never listed.
	for _, a := range got {
		if strings.HasPrefix(a, "Write:") && (strings.Contains(a, "/metadata/attested/") || strings.Contains(a, "/metadata/run-context/")) ||
			a == "List:"+b+"metadata/run-context/*" {
			t.Errorf("SeaweedActions grants %s", a)
		}
	}
	if got := (Policy{}).SeaweedActions("x"); len(got) != 0 {
		t.Errorf("an empty policy grants %v", got)
	}
}
