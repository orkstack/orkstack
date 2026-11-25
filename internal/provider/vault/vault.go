// Package vault is the "vault" provider type (providers.md §8): it logs in to
// a HashiCorp Vault server as the attempt, with the attempt's identity token
// at a JWT auth mount, reads secrets, and issues leased credentials that end
// with the login.
//
// It speaks Vault's HTTP API with net/http. The official client
// (github.com/hashicorp/vault/api) was weighed and not used: it takes its
// address, token, proxy and TLS settings from the process environment unless
// each is overridden, which a package that must never use the Orker's own
// credentials cannot rest on; it follows redirects with the token, and
// retries on its own clock, where the refresh engine owns every retry.
package vault

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// The name of the type and of its login credentials in a policy string.
const (
	typeName    = "vault"
	loginScheme = "auth"
)

// The bounds of a declaration. A login longer than max_login_ttl is refused:
// an Orker that is killed leaves it to live that long.
const (
	defaultAuthMount   = "jwt"
	defaultMaxLoginTTL = time.Hour
	minMaxLoginTTL     = time.Minute
	maxMaxLoginTTL     = 24 * time.Hour
)

// Type is the "vault" provider type.
type Type struct{}

func (Type) Name() string { return typeName }

// New reads a declaration. It does no I/O: the CA file is read at the first
// login, by the Orker, since ork declares the same provider and need not hold
// the file.
func (Type) New(name string, c *provider.Config) (provider.Provider, error) {
	p := &vaultProvider{name: name, authMount: defaultAuthMount, maxLoginTTL: defaultMaxLoginTTL, now: time.Now}
	if address, ok := c.String("address"); !ok {
		c.Errorf("address", "is required")
	} else if err := validAddress(address); err != nil {
		c.Errorf("address", "%v", err)
	} else {
		p.address = strings.TrimSuffix(address, "/")
	}
	if audience, ok := c.String("audience"); !ok || audience == "" {
		c.Errorf("audience", "is required: the role's bound_audiences")
	} else {
		p.audience = audience
	}
	p.caFile, _ = c.File("ca_file")
	if mount, ok := c.String("auth_mount"); ok {
		if err := validMount(mount); err != nil {
			c.Errorf("auth_mount", "%v", err)
		}
		p.authMount = mount
	}
	if ns, ok := c.String("vault_namespace"); ok {
		if err := validMount(ns); err != nil {
			c.Errorf("vault_namespace", "%v", err)
		}
		p.namespace = ns
	}
	if d, ok := c.Duration("max_login_ttl", minMaxLoginTTL, maxMaxLoginTTL); ok {
		p.maxLoginTTL = d
	}
	p.kv2Mounts, _ = c.Strings("kv2_mounts")
	for i, mount := range p.kv2Mounts {
		if err := validMount(mount); err != nil {
			c.Errorf("kv2_mounts", "%v", err)
		}
		for _, other := range p.kv2Mounts[:i] {
			if mount == other || strings.HasPrefix(mount, other+"/") || strings.HasPrefix(other, mount+"/") {
				c.Errorf("kv2_mounts", "%q and %q are one mount or one inside the other", other, mount)
			}
		}
	}
	p.http = &httpClient{caFile: p.caFile}
	return p, nil
}

// validAddress is an https origin and nothing else.
func validAddress(address string) error {
	u, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("is not a URL")
	}
	if u.Scheme != "https" {
		return fmt.Errorf("must be an https URL: a login carries the attempt's token")
	}
	if u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.HasSuffix(address, "#") {
		return fmt.Errorf("is https://<host>[:port], with no path, query, fragment or user")
	}
	return nil
}

// validMount is a mount's path as Vault names it, with no slash at either
// end: the locator rule, without a field.
func validMount(mount string) error {
	if err := provider.ValidLocator(mount); err != nil {
		return err
	}
	if strings.ContainsAny(mount, "#") {
		return fmt.Errorf("%q holds a \"#\"", mount)
	}
	return nil
}

type vaultProvider struct {
	name, address, audience, caFile, authMount, namespace string
	maxLoginTTL                                           time.Duration
	kv2Mounts                                             []string
	http                                                  *httpClient
	// now is the clock lives are counted on; a test steps it.
	now func() time.Time
}

func (p *vaultProvider) Name() string        { return p.name }
func (p *vaultProvider) Type() string        { return typeName }
func (p *vaultProvider) Audience() string    { return p.audience }
func (p *vaultProvider) LoginScheme() string { return loginScheme }

func (p *vaultProvider) Describe() provider.Description {
	fields := map[string]string{"address": p.address, "auth_mount": p.authMount, "max_login_ttl": p.maxLoginTTL.String()}
	if p.namespace != "" {
		fields["vault_namespace"] = p.namespace
	}
	if len(p.kv2Mounts) > 0 {
		fields["kv2_mounts"] = strings.Join(p.kv2Mounts, ", ")
	}
	return provider.Description{Type: typeName, Audience: p.audience, Fields: fields}
}

// detail is a reference as Parse read it: the API path, the field of a
// secret, and whether the path is under a KV version 2 mount, where a
// secret's fields are one level further in.
type detail struct {
	path, field string
	kv2         bool
}

// Parse checks a reference. Which mount is KV version 2 is configuration,
// never guessed from an answer's shape, and a path under one is its data
// path.
func (p *vaultProvider) Parse(ref provider.Ref) (provider.Parsed, error) {
	if len(ref.Ciphertext) > 0 {
		return provider.Parsed{}, fmt.Errorf("provider %s decrypts nothing: it takes no ciphertext", p.name)
	}
	if err := provider.ValidParams(ref.Params, nil); err != nil {
		return provider.Parsed{}, err
	}
	if ref.Kind == provider.KindCredential && ref.Locator == "" {
		return provider.Parsed{Ref: ref, Detail: detail{}}, nil
	}
	if ref.Kind != provider.KindSecret && ref.Kind != provider.KindCredential {
		return provider.Parsed{}, fmt.Errorf("provider %s has no reference of kind %s", p.name, ref.Kind)
	}
	if err := provider.ValidLocator(ref.Locator); err != nil {
		return provider.Parsed{}, err
	}
	if strings.HasPrefix(ref.Locator, loginScheme+":") {
		return provider.Parsed{}, fmt.Errorf("path %q spells a login; a login is a credential with no path", ref.Locator)
	}
	d := detail{}
	d.path, d.field, _ = strings.Cut(ref.Locator, "#")
	mount, kv2 := p.kv2Mount(d.path)
	d.kv2 = kv2
	switch {
	case ref.Kind == provider.KindCredential && d.field != "":
		return provider.Parsed{}, fmt.Errorf("a credential's path %q names a field", ref.Locator)
	case ref.Kind == provider.KindCredential && kv2:
		return provider.Parsed{}, fmt.Errorf("path %q is a KV secret, which has no lease: read it as a secret", d.path)
	case kv2 && !strings.HasPrefix(d.path, mount+"/data/"):
		return provider.Parsed{}, fmt.Errorf("path %q is under the KV version 2 mount %q, whose secrets are at %s/data/<name>", d.path, mount, mount)
	}
	return provider.Parsed{Ref: ref, Detail: d}, nil
}

// kv2Mount is the KV version 2 mount path is under or is.
func (p *vaultProvider) kv2Mount(path string) (string, bool) {
	i := slices.IndexFunc(p.kv2Mounts, func(m string) bool { return path == m || strings.HasPrefix(path, m+"/") })
	if i < 0 {
		return "", false
	}
	return p.kv2Mounts[i], true
}

// apiURL is the URL of an API path. A path is of the locator's characters,
// none of which a URL's path escapes, so it is asked for exactly as it is
// written.
func apiURL(address, path string) string { return address + "/v1/" + path }
