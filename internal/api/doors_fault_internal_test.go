package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/oidctest"
	"github.com/orkstack/orkstack/internal/testsupport/signintest"
	"github.com/orkstack/orkstack/internal/webui"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// faultText is what a fault says of this deployment: a host, a bucket, a
// path.
const faultText = "dial tcp 10.0.0.7:7233: s3://ork-prod/x: /srv/ork/secret"

var errFault = errors.New(faultText)

type downReader struct{ engine.RunReader }

func (downReader) RunStart(context.Context, string) (*engine.RunStartRecord, error) {
	return nil, errFault
}
func (downReader) GetRun(context.Context, string) (*orkstackv1.Run, error) { return nil, errFault }

type downTriggers struct{ triggers.Store }

func (downTriggers) Get(context.Context, string) (triggers.Trigger, bool, error) {
	return triggers.Trigger{}, false, errFault
}

type downFS struct{ fstest.MapFS }

func (downFS) ReadFile(string) ([]byte, error) { return nil, errFault }

// TestDoors_AFaultSaysNothingOfWhy: every route anyone may call without
// authenticating (exemptPatterns) answers a fault of what stands behind it in
// fixed words: nothing of the cluster's, the store's, the disk's or an
// identity provider's. A door with no row here fails the test.
func TestDoors_AFaultSaysNothingOfWhy(t *testing.T) {
	engineURL := "https://ork.example.test"
	env := signintest.New(t, engineURL, oidctest.Options{})
	idp := strings.TrimPrefix(env.IDP.URL, "https://")
	public, err := url.Parse(engineURL)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := accessFor(t, authn.ModeEnforce, nil)
	a.Authenticator = &authn.Authenticator{Issuers: env.Issuers, Keys: env.Keys}
	a.Signins = env.Signins(public, nil)
	env.IDP.Close() // before its discovery was ever read
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		AuditStore: artifacts.NewMemoryAuditStore(), Reader: downReader{}, Triggers: downTriggers{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	s.issuer, s.serveDiscovery = unreadIssuer(t, "ork.acme.dev"), true
	mux := NewMux(s, MuxOptions{Access: a, UI: webui.Handler(downFS{fstest.MapFS{"index.html": {Data: []byte("x")}}})})

	type call struct {
		method, path string
		status       int
	}
	post, get := http.MethodPost, http.MethodGet
	// A fault's answer is the same for a run or a trigger that exists and one
	// that does not: nothing behind the door answered.
	doors := map[string][]call{
		"/":               {{get, "/", 500}, {get, "/runs/ork-1", 500}},
		TriggerInvokePath: {{post, "/v1/triggers/trg-0000000000000000", 500}, {post, "/v1/triggers/trg-0000000000000001", 500}},
		AwaitCompletePath: {{post, "/v1/runs/ork-1/awaits/gate", 503}, {post, "/v1/runs/ork-2/awaits/gate", 503}},
		AwaitRejectPath:   {{post, "/v1/runs/ork-1/awaits/gate/reject", 503}, {post, "/v1/runs/ork-2/awaits/gate/reject", 503}},

		IssuerDiscoveryPath: {{get, IssuerDiscoveryPath, 200}}, /* configuration alone */
		IssuerKeysPath:      {{get, IssuerKeysPath, 503}},
		IssuerBundlePath:    {{get, IssuerBundlePath, 503}},
		SigninLoginPattern:  {{get, "/v1/auth/login/dev", 503}},
		// With no login cookie it is the page's fixed reason; the rest of the
		// callback needs a sign-in this engine sealed.
		SigninCallbackPattern: {{get, "/v1/auth/callback/dev?code=x&state=y", 302}},
		// Reads its configuration and nothing that can fail.
		"/orkstack.v1.AuthService/GetAuthConfig": {{post, "/orkstack.v1.AuthService/GetAuthConfig", 200}},
	}
	for pattern := range exemptPatterns {
		if len(doors[pattern]) == 0 {
			t.Errorf("%s may be called by anyone and has no row here", pattern)
		}
	}
	for pattern, calls := range doors {
		if _, ok := exemptPatterns[pattern]; !ok {
			t.Errorf("%s has a row and is no unauthenticated door", pattern)
		}
		var first string
		for i, c := range calls {
			req := httptest.NewRequest(c.method, engineURL+c.path, strings.NewReader("{}"))
			req.Header.Set("Authorization", "Bearer nope")
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			answer := w.Body.String() + w.Header().Get("Location")
			if w.Code != c.status {
				t.Errorf("%s %s = %d %s, want %d", c.method, c.path, w.Code, answer, c.status)
			}
			for _, word := range []string{"10.0.0.7", "ork-prod", "/srv/ork", "dial ", "connection refused", "x509", idp} {
				// The issuers a person can sign in with are what it is for.
				if word == idp && strings.HasSuffix(pattern, "/GetAuthConfig") {
					continue
				}
				if strings.Contains(answer, word) {
					t.Errorf("%s %s says %q of its fault: %s", c.method, c.path, word, answer)
				}
			}
			if i == 0 {
				first = answer
			} else if answer != first {
				t.Errorf("%s answers two things apart during a fault: %q and %q", pattern, first, answer)
			}
		}
	}
}

// However often anyone asks during a fault, each door logs at most a line a
// second, with the cause, and none for a caller that left.
func TestDoors_AFaultCannotFillTheLog(t *testing.T) {
	var log bytes.Buffer
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Reader: downReader{}, Triggers: downTriggers{}, Logger: slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelWarn}))})
	mux := NewMux(s, MuxOptions{})
	log.Reset() // what building them said
	ask := func(ctx context.Context, path string) {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer hunter2")
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	ask(gone, "/v1/runs/ork-1/awaits/gate")
	if log.Len() != 0 {
		t.Fatalf("a caller that left was logged: %s", &log)
	}
	begin := time.Now()
	for range 200 {
		ask(context.Background(), "/v1/runs/ork-1/awaits/gate")
		ask(context.Background(), "/v1/triggers/trg-0000000000000000")
	}
	most := 2 * (1 + int(time.Since(begin)/time.Second)) // a line a second of each door
	if n := strings.Count(log.String(), "\n"); n < 1 || n > most {
		t.Fatalf("400 faults logged %d lines, want 1 to %d", n, most)
	}
	if !strings.Contains(log.String(), "10.0.0.7") || strings.Contains(log.String(), "hunter2") {
		t.Fatalf("the log has no cause, or the caller's credential: %s", &log)
	}
}

// Nor do the refusals anyone can provoke at will: an upgrade from a foreign
// origin, and a sign-in's callback with no sign-in in progress.
func TestDoors_RefusalsCannotFillTheLog(t *testing.T) {
	var log bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelWarn}))
	engineURL := "https://ork.example.test"
	env := signintest.New(t, engineURL, oidctest.Options{})
	public, err := url.Parse(engineURL)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := accessFor(t, authn.ModeEnforce, nil)
	a.Authenticator = &authn.Authenticator{Issuers: env.Issuers, Keys: env.Keys}
	a.Signins = env.Signins(public, nil)
	a.Signins.Logger = logger
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Reader: downReader{}, Logger: logger})
	signins, sockets := NewMux(s, MuxOptions{Access: a}), NewMux(s, MuxOptions{})
	// An issuer whose keys cannot be read: a well-formed credential of its
	// cannot be checked.
	down := signintest.New(t, engineURL, oidctest.Options{})
	credential, err := down.Provider.IDToken(signintest.CLIClientID, oidctest.User{Subject: "u", Email: "u@example.test"}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	down.IDP.Close()
	b, _ := accessFor(t, authn.ModeEnforce, nil)
	b.Authenticator, b.Logger = &authn.Authenticator{Issuers: down.Issuers, Keys: down.Keys}, logger
	unreachable := NewMux(s, MuxOptions{Access: b})
	for name, c := range map[string]struct {
		mux    http.Handler
		path   string
		origin string
		line   string
	}{
		"callback":        {signins, "/v1/auth/callback/" + strings.Repeat("i", 5000) + "?code=x", "", "sign-in refused"},
		"a credential":    {unreachable, "/orkstack.v1.RunService/" + strings.Repeat("p", 5000), "", "could not be checked now"},
		"run socket":      {sockets, "/ws/runs/ork-1", "https://" + strings.Repeat("e", 5000) + ".test", "websocket upgrade refused"},
		"task log socket": {sockets, "/ws/runs/ork-1/tasks/build/logs", "https://" + strings.Repeat("e", 5000) + ".test", "websocket upgrade refused"},
	} {
		t.Run(name, func(t *testing.T) {
			log.Reset()
			begin := time.Now()
			for range 100 {
				req := httptest.NewRequest(http.MethodGet, engineURL+c.path, nil)
				if name == "a credential" {
					req.Header.Set("Authorization", "Bearer "+credential)
				}
				if c.origin != "" {
					req.Header.Set("Origin", c.origin)
				}
				c.mux.ServeHTTP(httptest.NewRecorder(), req)
			}
			n, most := strings.Count(log.String(), c.line), 1+int(time.Since(begin)/time.Second)
			if n < 1 || n > most {
				t.Fatalf("100 refusals logged %d lines, want 1 to %d: %s", n, most, &log)
			}
			if log.Len() > n*2000 {
				t.Fatalf("%d lines hold %d bytes of a caller's text", n, log.Len())
			}
		})
	}
}
