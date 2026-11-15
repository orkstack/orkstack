package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// accessEngine is a real mux in observe, whose stand-in gateway hands every
// request the configured admin, as authentication would.
func accessEngine(t *testing.T) func(args ...string) (string, string, int) {
	t.Helper()
	return accessEngineAs(t, &authn.Principal{Kind: authn.KindUser, ID: "user:corp:root", Issuer: "corp", Method: authn.MethodJWT})
}

// accessEngineAs is accessEngine whose gateway hands every request caller,
// over a document holding bindings.
func accessEngineAs(t *testing.T, caller *authn.Principal, bindings ...*orkstackv1.Binding) func(args ...string) (string, string, int) {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(principalEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	cas := artifacts.NewMemoryCAS()
	if len(bindings) > 0 {
		if _, err := artifacts.NewAccessStoreOn(cas).Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: bindings}, nil,
			artifacts.AccessWrite{At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		AuditStore: artifacts.NewMemoryAuditStore()})
	admins, err := authz.CompileConfiguration(authz.Configuration{Admins: []string{"user:corp:root"}})
	if err != nil {
		t.Fatal(err)
	}
	access := &api.AccessControl{Mode: authn.ModeObserve, Configured: admins,
		Policies:      api.NewPolicyCache(artifacts.NewAccessStoreOn(cas), api.PolicyCacheOptions{Configured: admins}),
		Authenticator: &authn.Authenticator{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := api.NewMux(svc, api.MuxOptions{Access: access})
	server := api.NewServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(authn.WithPrincipal(r.Context(), caller)))
	}))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	base := "http://" + listener.Addr().String()
	return func(args ...string) (string, string, int) {
		var out, errOut bytes.Buffer
		code := Execute(append(args, "--server", base), &out, &errOut)
		return out.String(), errOut.String(), code
	}
}

func TestAdminAccessCommands(t *testing.T) {
	run := accessEngine(t)
	must := func(want string, args ...string) string {
		t.Helper()
		out, errOut, code := run(args...)
		if code != 0 || !strings.Contains(out, want) {
			t.Fatalf("orkctl %s = %d\nstdout: %s\nstderr: %s\nwant stdout to contain %q", strings.Join(args, " "), code, out, errOut, want)
		}
		return out
	}
	refused := func(want string, args ...string) {
		t.Helper()
		_, errOut, code := run(args...)
		if code == 0 || !strings.Contains(errOut, want) {
			t.Fatalf("orkctl %s = %d %q; want a refusal containing %q", strings.Join(args, " "), code, errOut, want)
		}
	}

	must("created role release-manager", "admin", "role", "create", "release-manager", "--permission", "orkflow.read", "--permission", "orkflow.promote")
	must("release-manager\tcustom\t2 permissions", "admin", "role", "list")
	must("viewer\tbuilt-in", "admin", "role", "list")
	must("permission:  orkflow.promote", "admin", "role", "get", "release-manager")
	refused("invalid_argument", "admin", "role", "create", "bad", "--permission", "orkflow.teleport")

	must("created binding rm: release-manager at deployment", "admin", "binding", "create", "rm", "--role", "release-manager", "--scope", "deployment", "--subject", "user:corp:ada")
	must("rm\trelease-manager\tdeployment\tuser:corp:ada", "admin", "binding", "list")
	must("config:admins\tadmin\tdeployment\tuser:corp:root\tconfiguration", "admin", "binding", "list")
	refused("binding \"rm\"", "admin", "role", "delete", "release-manager")

	must("allowed: user:corp:ada holds orkflow.promote", "admin", "access", "explain", "user:corp:ada", "orkflow.promote", "--namespace", "payments", "--orkflow", "deploy")
	must("denied (NO_BINDING)", "admin", "access", "explain", "user:corp:bob", "orkflow.promote")

	dir := t.TempDir()
	keyFile := filepath.Join(dir, "bot.key")
	out := must("created service account backup-bot", "admin", "service-account", "create", "backup-bot", "--generate-key", keyFile)
	if info, err := os.Stat(keyFile); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("the private key file = %v, %v; want it written with mode 0600", info, err)
	}
	if !strings.Contains(out, "key: ") {
		t.Fatalf("create printed no key id: %s", out)
	}
	refused("exists", "admin", "service-account", "create", "backup-bot2", "--generate-key", keyFile)
	must("backup-bot\t1 keys", "admin", "service-account", "list")

	must("disabled user:corp:mallory", "admin", "principal", "disable", "user:corp:mallory")
	must("enabled user:corp:mallory", "admin", "principal", "enable", "user:corp:mallory")

	history := must("create role release-manager", "admin", "access", "history")
	if lines := strings.Count(history, "\n"); lines != 5 {
		t.Fatalf("history has %d revisions, want 5:\n%s", lines, history)
	}
	must(`+ binding {"name":"rm"`, "admin", "access", "diff", "1", "2")
	// The newest revision's digest is the history's second column, and the
	// head as it is now may be acknowledged.
	head := strings.Split(strings.SplitN(history, "\n", 2)[0], "\t")
	if len(head) < 2 || head[0] != "5" || len(head[1]) != 64 {
		t.Fatalf("history's first line = %q; want revision 5 and its digest", head)
	}
	must("acknowledged revision 5", "admin", "access", "acknowledge", "5", head[1])
	refused("invalid_argument", "admin", "access", "acknowledge", "5", "abc")
	refused("revision", "admin", "access", "acknowledge", "five", head[1])

	must("deleted binding rm", "admin", "binding", "delete", "rm")
	must("deleted role release-manager", "admin", "role", "delete", "release-manager")
	must("deleted service account backup-bot", "admin", "service-account", "delete", "backup-bot")
}

// TestAdminBinding_ADelegateWithoutAccessRead: a namespace admin, who may
// bind in its namespace but not read the document, creates and deletes a
// binding there from the command line.
func TestAdminBinding_ADelegateWithoutAccessRead(t *testing.T) {
	nina := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:nina", Issuer: "corp", Method: authn.MethodJWT}
	run := accessEngineAs(t, nina, &orkstackv1.Binding{Name: "default-admins", Role: authz.RoleNamespaceAdmin,
		Scope: "namespace:default", Subjects: []string{"user:corp:nina"}})
	if out, errOut, code := run("admin", "binding", "create", "devs", "--role", "developer", "--scope", "namespace:default", "--subject", "group:corp:eng"); code != 0 {
		t.Fatalf("a delegate's create = %d %q %q", code, out, errOut)
	}
	if out, errOut, code := run("admin", "binding", "delete", "devs"); code != 0 {
		t.Fatalf("a delegate's delete = %d %q %q", code, out, errOut)
	}
	if _, _, code := run("admin", "binding", "create", "x", "--role", "admin", "--scope", "namespace:default", "--subject", "group:corp:eng"); code == 0 {
		t.Fatal("a delegate bound admin")
	}
}
