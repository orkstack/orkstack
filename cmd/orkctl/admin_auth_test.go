package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestAdminAuthLower lowers an observe deployment to asserted through the
// real mux: the command reads the record's etag and sends it back, and the
// record is rewritten.
func TestAdminAuthLower(t *testing.T) {
	t.Setenv(serverEnvVar, "")
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	guard := &api.ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: authn.ModeObserve, ClaimTTL: time.Hour}
	if err := guard.Boot(ctx); err != nil {
		t.Fatal(err)
	}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		AuditStore: artifacts.NewMemoryAuditStore()})
	admins, err := authz.CompileConfiguration(authz.Configuration{Admins: []string{"user:corp:root"}})
	if err != nil {
		t.Fatal(err)
	}
	access := &api.AccessControl{Mode: authn.ModeObserve, ModeRecord: guard, Configured: admins,
		Policies:      api.NewPolicyCache(artifacts.NewAccessStoreOn(cas), api.PolicyCacheOptions{Configured: admins}),
		Authenticator: &authn.Authenticator{}}
	self := engine.EngineDescription{Mode: "enforce", AdminsDigest: engine.ConfigDigest([]string{"user:corp:root"})}
	other := self
	other.AdminsDigest = engine.ConfigDigest(nil)
	access.Fleet = &api.FleetReport{Self: self, Presence: enginesListed{
		{Identity: "1@api@", Description: engine.EngineDescription{Mode: "asserted"}},
		{Identity: "2@other@", Description: other},
		{Identity: "3@self@", Description: self},
	}}
	access.Fleet.Check(ctx)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// orkctl sends no credential before P8; the stand-in gateway below hands
	// every request the configured admin, as authentication would.
	mux := api.NewMux(svc, api.MuxOptions{Access: access})
	root := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:root", Issuer: "corp", Method: authn.MethodJWT}
	server := api.NewServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(authn.WithPrincipal(r.Context(), root)))
	}))
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	base := "http://" + listener.Addr().String()

	run := func(args ...string) (string, string, int) {
		var out, errOut bytes.Buffer
		code := Execute(append(args, "--server", base), &out, &errOut)
		return out.String(), errOut.String(), code
	}
	out, errOut, code := run("admin", "auth", "status")
	if code != 0 || !strings.Contains(out, "recorded mode:  observe") {
		t.Fatalf("status = %d %q %q", code, out, errOut)
	}
	for _, want := range []string{"fleet checked:", "not enforcing:  1@api@ (asserted)", "differs:        2@other@ holds another auth.admins"} {
		if !strings.Contains(out, want) {
			t.Errorf("status lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "3@self@") || strings.Contains(out, "not enforcing:  2@other@") {
		t.Errorf("status lists an engine that enforces with this engine's lists:\n%s", out)
	}
	if _, errOut, code := run("admin", "auth", "lower", "enforce"); code == 0 {
		t.Fatalf("lowering to enforce succeeded: %q", errOut)
	}
	out, errOut, code = run("admin", "auth", "lower", "asserted")
	if code != 0 || !strings.Contains(out, "from observe to asserted") {
		t.Fatalf("lower = %d %q %q", code, out, errOut)
	}
	rec, _, err := artifacts.NewModeStoreOn(cas).Get(ctx)
	if err != nil || rec.Mode != authn.ModeAsserted {
		t.Fatalf("record = %+v, %v", rec, err)
	}
}

// enginesListed is a presence channel that lists these engines.
type enginesListed []engine.EnginePresence

func (e enginesListed) ObserveEngines(context.Context) ([]engine.EnginePresence, error) {
	return e, nil
}
