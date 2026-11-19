package docscheck

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v4"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/testsupport/dextest"
)

// dexExampleConfig is what this test reads of the documented Dex
// configuration. Dex itself judges the whole file: the integration test
// TestIntegration_SigninWithDex_DocumentedSetup serves it with the pinned
// Dex and signs its user in.
type dexExampleConfig struct {
	Issuer           string `yaml:"issuer"`
	EnablePasswordDB bool   `yaml:"enablePasswordDB"`
	Expiry           struct {
		IDTokens string `yaml:"idTokens"`
	} `yaml:"expiry"`
	StaticPasswords []struct {
		Email         string   `yaml:"email"`
		Hash          string   `yaml:"hash"`
		UserID        string   `yaml:"userID"`
		EmailVerified bool     `yaml:"emailVerified"`
		Groups        []string `yaml:"groups"`
	} `yaml:"staticPasswords"`
	StaticClients []struct {
		ID           string   `yaml:"id"`
		Public       bool     `yaml:"public"`
		Secret       string   `yaml:"secret"`
		SecretEnv    string   `yaml:"secretEnv"`
		RedirectURIs []string `yaml:"redirectURIs"`
	} `yaml:"staticClients"`
}

// orkstackExampleConfig is the part of the documented OrkStack
// configuration this test checks; auth.issuers is parsed by the engine's own
// authn.ParseIssuers.
type orkstackExampleConfig struct {
	Auth struct {
		Mode      string    `yaml:"mode"`
		PublicURL string    `yaml:"public_url"`
		Admins    []string  `yaml:"admins"`
		Issuers   yaml.Node `yaml:"issuers"`
	} `yaml:"auth"`
}

var bcryptHash = regexp.MustCompile(`^\$2[aby]\$[0-9]{2}\$[./A-Za-z0-9]{53}$`)

// The Dex set-up docs/AUTHENTICATION.md tells an operator to copy holds
// together: it names the image deploy/dex-image.txt pins, the engine accepts
// its trusted-issuer entry, and the two halves agree on the issuer URL, the
// client ids, the redirect URI and the token lifetime; every user has a
// bcrypt hash, never a password, a verified email and a group, and the
// first admins are a group a user holds.
func TestAuthenticationDoc_DexExampleHoldsTogether(t *testing.T) {
	root := repoRoot(t)
	image, err := dextest.Image(root)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(dextest.DocFile)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), "`"+image+"`") {
		t.Errorf("%s does not name the pinned Dex `%s`", dextest.DocFile, image)
	}
	ex, err := dextest.DocExamples(root)
	if err != nil {
		t.Fatal(err)
	}

	var dex dexExampleConfig
	if err := yaml.Unmarshal(ex.Dex, &dex); err != nil {
		t.Fatalf("Dex's configuration: %v", err)
	}
	var ork orkstackExampleConfig
	if err := yaml.Unmarshal(ex.OrkStack, &ork); err != nil {
		t.Fatalf("OrkStack's configuration: %v", err)
	}
	raw, err := yaml.Marshal(map[string]*yaml.Node{"issuers": &ork.Auth.Issuers})
	if err != nil {
		t.Fatal(err)
	}
	issuers, err := authn.ParseIssuers(raw)
	if err != nil {
		t.Fatalf("the engine refuses the documented auth.issuers: %v", err)
	}
	if issuers.Len() != 1 {
		t.Fatalf("auth.issuers declares %v, want one issuer", issuers.Names())
	}
	people, _ := issuers.ByName(issuers.Names()[0])
	if people.Login == nil {
		t.Fatal("the documented issuer has no login: block")
	}

	if !dex.EnablePasswordDB {
		t.Error("Dex's configuration does not set enablePasswordDB: true")
	}
	if dex.Issuer != people.Issuer {
		t.Errorf("Dex's issuer %q, OrkStack trusts %q", dex.Issuer, people.Issuer)
	}
	idTokens, err := time.ParseDuration(dex.Expiry.IDTokens)
	if err != nil || idTokens > people.MaxLifetime {
		t.Errorf("Dex's expiry.idTokens %q must be at most the issuer's max_lifetime %s", dex.Expiry.IDTokens, people.MaxLifetime)
	}

	clients := map[string]int{}
	for i, c := range dex.StaticClients {
		clients[c.ID] = i
	}
	web, ok := clients[people.Login.ClientID]
	if !ok {
		t.Fatalf("Dex has no client %q (login.client_id)", people.Login.ClientID)
	}
	wc := dex.StaticClients[web]
	if wc.Public || wc.Secret != "" || wc.SecretEnv == "" {
		t.Errorf("Dex's client %q must be confidential, its secret read from secretEnv, never written in the file", wc.ID)
	}
	redirect := strings.TrimSuffix(ork.Auth.PublicURL, "/") + "/v1/auth/callback/" + people.Name
	if !slices.Equal(wc.RedirectURIs, []string{redirect}) {
		t.Errorf("Dex's client %q redirects to %v, want only %s", wc.ID, wc.RedirectURIs, redirect)
	}
	cli, ok := clients[people.Login.CLIClientID]
	if !ok || !dex.StaticClients[cli].Public {
		t.Errorf("Dex has no public client %q (login.cli_client_id)", people.Login.CLIClientID)
	}
	if !slices.Contains(people.Audiences, people.Login.ClientID) || !slices.Contains(people.Audiences, people.Login.CLIClientID) {
		t.Errorf("audiences %v must hold both client ids", people.Audiences)
	}
	if people.UsernameClaim != "email" || !people.RequireVerifiedEmail {
		t.Errorf("the issuer names people by %q (verified %t); the doc says by their verified email", people.UsernameClaim, people.RequireVerifiedEmail)
	}

	if len(dex.StaticPasswords) == 0 {
		t.Fatal("Dex's configuration lists no staticPasswords")
	}
	held := map[string]bool{}
	for _, u := range dex.StaticPasswords {
		if !bcryptHash.MatchString(u.Hash) {
			t.Errorf("user %s: hash %q is not a bcrypt hash", u.Email, u.Hash)
		}
		if !u.EmailVerified || u.UserID == "" || len(u.Groups) == 0 {
			t.Errorf("user %s needs emailVerified: true, a userID and a group", u.Email)
		}
		for _, g := range u.Groups {
			held["group:"+people.Name+":"+g] = true
		}
	}
	if ork.Auth.Mode != "enforce" || len(ork.Auth.Admins) == 0 {
		t.Errorf("auth.mode %q with admins %v; the example enforces, with a first admin", ork.Auth.Mode, ork.Auth.Admins)
	}
	for _, a := range ork.Auth.Admins {
		if !held[a] {
			t.Errorf("auth.admins names %s, a group no documented user holds", a)
		}
	}
}

// sitePage is the site's how-to that shows the documented Dex set-up.
const sitePage = "site/content/how-to/passwords-with-dex.md"

var siteYAMLBlock = regexp.MustCompile("(?s)```yaml\n(.*?)```\n")

// The site's how-to shows the same two configurations the repository doc
// does, and the same pinned image, so an edit to one cannot leave the other
// stale. Comment lines, and the expiry.signingKeys line the site leaves out
// as a default, are not compared.
func TestAuthenticationDoc_SitePageShowsTheDocumentedDex(t *testing.T) {
	root := repoRoot(t)
	ex, err := dextest.DocExamples(root)
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(sitePage)))
	if err != nil {
		t.Fatal(err)
	}
	image, err := dextest.Image(root)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(page), image) {
		t.Errorf("%s does not name the pinned Dex %s", sitePage, image)
	}
	blocks := siteYAMLBlock.FindAllStringSubmatch(string(page), -1)
	if len(blocks) != 2 {
		t.Fatalf("%s holds %d yaml blocks, want 2: Dex's configuration, then OrkStack's", sitePage, len(blocks))
	}
	for i, want := range [][]byte{ex.Dex, ex.OrkStack} {
		if got, w := comparable(blocks[i][1]), comparable(string(want)); got != w {
			t.Errorf("%s's yaml block %d differs from %s's:\n--- site\n%s\n--- doc\n%s", sitePage, i+1, dextest.DocFile, got, w)
		}
	}
}

// comparable is a yaml block without its comment lines and its
// expiry.signingKeys line.
func comparable(block string) string {
	var keep []string
	for _, line := range strings.Split(block, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "signingKeys:") {
			continue
		}
		keep = append(keep, line)
	}
	return strings.Join(keep, "\n")
}
