package vault

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/testsupport/testpki"
)

// childEnv marks the process TestEnvironment_ProxyYesEveryVaultSettingNo runs
// itself as, and holds the CA file it trusts: Go reads the proxy settings
// once a process.
const childEnv = "ORK_TEST_VAULT_ENVIRONMENT_CHILD"

// inspectedEnv says the child's proxy answers the connection itself.
const inspectedEnv = "ORK_TEST_VAULT_INSPECTED"

// tunnel is a proxy that keeps what it was asked for and carries each
// connection to one address.
type tunnel struct {
	*httptest.Server
	mu    sync.Mutex
	asked []string
}

func newTunnel(t *testing.T, to string) *tunnel {
	t.Helper()
	p := &tunnel{}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.asked = append(p.asked, r.Method+" "+r.Host)
		p.mu.Unlock()
		up, err := net.Dial("tcp", to)
		if r.Method != http.MethodConnect || err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = up.Close() }()
		w.WriteHeader(http.StatusOK)
		down, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = down.Close() }()
		go func() { _, _ = io.Copy(up, down) }()
		_, _ = io.Copy(down, up)
	}))
	t.Cleanup(p.Close)
	return p
}

// childLogin is what the child does: a login to an address the proxy alone
// can reach, and its close.
func childLogin(t *testing.T, ca string) {
	p, err := declare(t, "address: https://vault.invalid:8200\naudience: a\nca_file: "+ca+"\n")
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("NO_PROXY") != "" {
		client, err := p.(*vaultProvider).http.get()
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodGet, "https://vault.invalid:8200/v1/x", nil)
		if err != nil {
			t.Fatal(err)
		}
		if u, err := client.Transport.(*http.Transport).Proxy(req); err != nil || u != nil {
			t.Fatalf("an address of NO_PROXY is asked through %v, %v", u, err)
		}
		return
	}
	s, err := p.Login(context.Background(), "app", provider.Attempt{}, identity)
	if os.Getenv(inspectedEnv) != "" {
		if !is(err, "no-answer") {
			t.Fatalf("a login through a proxy that inspects TLS = %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestEnvironment_ProxyYesEveryVaultSettingNo: the Orker's proxy settings are
// followed, as its other outbound calls follow them, and an address NO_PROXY
// names is not proxied. No VAULT_* variable of it is: the login goes to the
// declared address, and carries the attempt's token alone. The proxy sees a
// connection to that address and never what it carries; one that answers it
// itself, with a certificate the declared CA did not sign, gets nothing.
func TestEnvironment_ProxyYesEveryVaultSettingNo(t *testing.T) {
	if ca := os.Getenv(childEnv); ca != "" {
		childLogin(t, ca)
		return
	}
	pki, strange := testpki.NewCA(t, "vault"), testpki.NewCA(t, "a proxy that inspects")
	cert := func(ca *testpki.CA) *tls.Certificate {
		c := ca.Issue(t, testpki.LeafOptions{Name: "vault.invalid", Hosts: []string{"vault.invalid"}}).TLS(t)
		return &c
	}
	vault, inspector, other := newFaultAs(t, cert(pki)), newFaultAs(t, cert(strange)), newFault(t)
	for _, f := range []*fault{vault, inspector} {
		f.on(loginPath, body(loginBody("hvs.a", 600)))
		f.on(revokePath, status(204))
	}
	proxy, inspecting := newTunnel(t, vault.server.Listener.Addr().String()), newTunnel(t, inspector.server.Listener.Addr().String())
	ca := testpki.Write(t, t.TempDir(), "ca.pem", pki.PEM)
	child := func(proxy *tunnel, more ...string) {
		t.Helper()
		c := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")
		c.Env = append([]string{childEnv + "=" + ca}, more...)
		for _, kv := range os.Environ() {
			if name, _, _ := strings.Cut(kv, "="); !strings.HasSuffix(strings.ToUpper(name), "_PROXY") {
				c.Env = append(c.Env, kv)
			}
		}
		c.Env = append(c.Env, "VAULT_ADDR="+other.server.URL, "VAULT_PROXY_ADDR="+other.server.URL, "VAULT_TOKEN=hvs.the-orkers-own",
			"VAULT_NAMESPACE=the-orkers-own", "VAULT_SKIP_VERIFY=true", "VAULT_CACERT=/nonexistent",
			"HTTPS_PROXY="+proxy.URL, "HTTP_PROXY="+proxy.URL)
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("the child: %v\n%s", err, out)
		}
	}
	child(proxy)
	proxy.mu.Lock()
	asked := slices.Compact(slices.Clone(proxy.asked))
	proxy.mu.Unlock()
	if !slices.Equal(asked, []string{"CONNECT vault.invalid:8200"}) {
		t.Errorf("the proxy was asked for %q, want a tunnel to the declared address alone", asked)
	}
	if vault.count(loginPath) != 1 || vault.count(loginPath+" hvs") != 0 || vault.count(revokePath+" hvs.a") != 1 || other.count("") != 0 {
		t.Errorf("vault was asked %v, another %v", vault.asked, other.asked)
	}
	child(proxy, "NO_PROXY=vault.invalid")
	child(inspecting, inspectedEnv+"=1")
	if n := inspector.count(""); n != 0 || len(inspecting.asked) == 0 {
		t.Errorf("a proxy that inspects TLS was asked %d requests over %d tunnels, want none over some", n, len(inspecting.asked))
	}
}
