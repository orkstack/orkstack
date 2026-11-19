//go:build integration

package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.yaml.in/yaml/v4"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/dextest"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// docExamplePassword is the password docs/AUTHENTICATION.md says its
// example hash is of.
const docExamplePassword = "example-only"

// TestIntegration_SigninWithDex_DocumentedSetup: the Dex set-up
// docs/AUTHENTICATION.md ("Passwords, through Dex") tells an operator to
// copy works as written. The pinned Dex serves the documented configuration
// (its sqlite3 storage on a volume, its static user, its two clients), the
// engine trusts it through the documented auth.issuers entry and makes the
// documented auth.admins its first admins, and the documented user signs in
// through Dex's password form, as the principal and in the group the doc
// says, holding admin, with a sign-in Dex's refresh token renews. Only what a laptop cannot hold is rewritten: Dex's
// issuer URL and the web client's redirect, to the loopback addresses this
// test serves them on; each is replaced exactly once, so a doc that changes
// either fails here rather than going untested.
func TestIntegration_SigninWithDex_DocumentedSetup(t *testing.T) {
	ctx := context.Background()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := dextest.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	ex, err := dextest.DocExamples(root)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ex.Dex), "`"+docExamplePassword+"`") {
		t.Fatalf("Dex's documented configuration no longer says its hash is of `%s`", docExamplePassword)
	}
	var dexDoc struct {
		StaticPasswords []struct {
			Email  string   `yaml:"email"`
			Groups []string `yaml:"groups"`
		} `yaml:"staticPasswords"`
		StaticClients []struct {
			ID        string `yaml:"id"`
			SecretEnv string `yaml:"secretEnv"`
		} `yaml:"staticClients"`
	}
	if err := yaml.Unmarshal(ex.Dex, &dexDoc); err != nil {
		t.Fatalf("Dex's documented configuration: %v", err)
	}
	var orkDoc struct {
		Auth struct {
			PublicURL string    `yaml:"public_url"`
			Admins    []string  `yaml:"admins"`
			Issuers   yaml.Node `yaml:"issuers"`
		} `yaml:"auth"`
	}
	if err := yaml.Unmarshal(ex.OrkStack, &orkDoc); err != nil {
		t.Fatalf("OrkStack's documented configuration: %v", err)
	}
	documentedRaw := marshalIssuers(t, &orkDoc.Auth.Issuers)
	documented, err := authn.ParseIssuers([]byte(documentedRaw))
	if err != nil {
		t.Fatalf("the documented auth.issuers: %v", err)
	}
	if documented.Len() != 1 {
		t.Fatalf("the documented auth.issuers declares %v, want one", documented.Names())
	}
	name := documented.Names()[0]
	docIssuer, _ := documented.ByName(name)
	if docIssuer.Login == nil {
		t.Fatal("the documented issuer has no login: block")
	}
	secretEnv := ""
	for _, c := range dexDoc.StaticClients {
		if c.ID == docIssuer.Login.ClientID {
			secretEnv = c.SecretEnv
		}
	}
	if secretEnv == "" || len(dexDoc.StaticPasswords) == 0 {
		t.Fatalf("Dex's documented configuration has no secretEnv for %q, or no user", docIssuer.Login.ClientID)
	}
	user := dexDoc.StaticPasswords[0]

	engine := httptest.NewUnstartedServer(nil)
	engineURL := "https://" + engine.Listener.Addr().String()
	callback := engineURL + "/v1/auth/callback/" + name
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	issuer := "https://" + addr + "/dex"
	docRedirect := strings.TrimSuffix(orkDoc.Auth.PublicURL, "/") + "/v1/auth/callback/" + name
	config := replaceOnce(t, string(ex.Dex), docIssuer.Issuer, issuer)
	config = replaceOnce(t, config, docRedirect, callback)
	configFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configFile, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	dex := startDexWith(t, dextest.Options{Root: root, Dir: t.TempDir(), Addr: addr, WebRedirect: callback,
		ConfigFile: configFile, SecretEnv: secretEnv, DataDir: t.TempDir()})
	if dex.Issuer != issuer {
		t.Fatalf("Dex serves %s, the configuration names %s", dex.Issuer, issuer)
	}
	idp, err := dex.Client()
	if err != nil {
		t.Fatal(err)
	}
	dexDiscovery(t, idp, dex.Issuer)

	issuers, err := authn.ParseIssuers([]byte(replaceOnce(t, documentedRaw, docIssuer.Issuer, issuer)))
	if err != nil {
		t.Fatal(err)
	}
	people, _ := issuers.ByName(name)
	keys, err := authn.NewKeyCache(people, idp, nil)
	if err != nil {
		t.Fatal(err)
	}
	configured, err := authz.CompileConfiguration(authz.Configuration{Admins: orkDoc.Auth.Admins})
	if err != nil {
		t.Fatalf("the documented auth.admins: %v", err)
	}
	public, _ := url.Parse(engineURL)
	a := &AccessControl{Mode: authn.ModeEnforce, Configured: configured,
		Policies: NewPolicyCache(artifacts.NewAccessStoreOn(artifacts.NewMemoryCAS()), PolicyCacheOptions{Configured: configured})}
	a.Authenticator = &authn.Authenticator{Issuers: issuers, Keys: map[string]jwt.KeySource{name: keys}}
	a.Signins = &authn.Signins{Issuers: issuers, Keys: map[string]jwt.KeySource{name: keys}, Sealer: signinKey(t),
		Records: artifacts.NewMemorySigninStore(nil), PublicURL: public, Leeway: time.Minute, Now: time.Now,
		Secret: func(source string) (string, error) {
			if source != people.Login.ClientSecret {
				return "", errors.New("unknown source")
			}
			return dex.WebSecret, nil
		},
		HTTPClient: func(*authn.Issuer) (*http.Client, error) { return idp, nil }}
	engine.Config.Handler = NewMux(newAccessService(t), MuxOptions{Access: a})
	engine.StartTLS()
	t.Cleanup(engine.Close)

	browser := dexBrowser(t, idp, engine.Certificate())
	origin, token := engineURL, ""
	opts := connect.WithInterceptors(csrfHeader{origin: &origin, token: &token})
	auth := orkstackv1connect.NewAuthServiceClient(browser, engineURL, opts)
	access := orkstackv1connect.NewAccessServiceClient(browser, engineURL, opts)

	dexLoginAs(t, browser, engineURL, "/", user.Email, docExamplePassword)
	who, err := auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		t.Fatalf("GetSignin after signing in as the documented user: %v", err)
	}
	if got, want := who.Msg.GetPrincipalId(), "user:"+name+":"+user.Email; got != want {
		t.Errorf("principal %q, the doc says %q", got, want)
	}
	if !slices.Equal(who.Msg.GetGroups(), user.Groups) {
		t.Errorf("groups %v, Dex's configuration gives %v", who.Msg.GetGroups(), user.Groups)
	}
	token = who.Msg.GetCsrfToken()
	if _, err := access.ListSignins(ctx, connect.NewRequest(&orkstackv1.ListSigninsRequest{})); err != nil {
		t.Fatalf("the documented auth.admins does not make the documented user an admin: %v", err)
	}
	// The documented scopes ask for offline_access, so the sign-in holds a
	// refresh token Dex redeems.
	if _, err := auth.RefreshSignin(ctx, connect.NewRequest(&orkstackv1.RefreshSigninRequest{})); err != nil {
		t.Fatalf("RefreshSignin with the documented scopes: %v", err)
	}
}

// startDexWith is startDex with every option given.
func startDexWith(t *testing.T, o dextest.Options) *dextest.Dex {
	t.Helper()
	o.ProofID, o.Owner = proofID(), dextest.ProcessOwner()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d, err := dextest.Start(ctx, o)
	if errors.Is(err, dextest.ErrNoImage) {
		image, _ := dextest.Image(o.Root)
		dexAbsent(t, "%v; docker pull %s (the proof host's setup pulls it)", err, image)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dextest.Stop(context.Background(), d.ID); err != nil {
			t.Errorf("stop Dex: %v", err)
		}
	})
	return d
}

// replaceOnce replaces old, which must occur exactly once in s.
func replaceOnce(t *testing.T, s, old, repl string) string {
	t.Helper()
	if n := strings.Count(s, old); n != 1 {
		t.Fatalf("the documented configuration holds %q %d times, want once", old, n)
	}
	return strings.Replace(s, old, repl, 1)
}

// marshalIssuers is the auth.issuers setting's document for the node.
func marshalIssuers(t *testing.T, issuers *yaml.Node) string {
	t.Helper()
	raw, err := yaml.Marshal(map[string]*yaml.Node{"issuers": issuers})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
