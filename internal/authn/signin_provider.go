package authn

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
)

// providerMetadataAge is how long an issuer's sign-in endpoints are used
// before discovery is read again.
const providerMetadataAge = time.Hour

// ProviderMetadata is what sign-in reads of an issuer's discovery document
// (OIDC Discovery 1.0, RFC 8414): its endpoints, each https, with no user
// information.
type ProviderMetadata struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	EndSessionEndpoint    string `json:"end_session_endpoint,omitzero"`
	RevocationEndpoint    string `json:"revocation_endpoint,omitzero"`
	DeviceEndpoint        string `json:"device_authorization_endpoint,omitzero"`
	// CodeChallengeMethods, when the issuer lists them, must include S256.
	CodeChallengeMethods []string `json:"code_challenge_methods_supported,omitzero"`
	// IssParameter says the authorization response carries iss (RFC 9207):
	// one that does not is then refused.
	IssParameter bool `json:"authorization_response_iss_parameter_supported,omitzero"`
}

// CheckProviderMetadata reads a discovery document for sign-in: it must
// state the configured issuer exactly, give an authorization and a token
// endpoint, support S256 where it lists challenge methods, and every
// endpoint it gives must be an https URL with no user information or
// fragment.
func CheckProviderMetadata(raw []byte, configured string) (ProviderMetadata, error) {
	var m ProviderMetadata
	if err := json.Unmarshal(raw, &m); err != nil {
		return ProviderMetadata{}, fmt.Errorf("discovery document: %w", err)
	}
	if m.Issuer != configured {
		return ProviderMetadata{}, fmt.Errorf("discovery document states issuer %q, not the configured %q", m.Issuer, configured)
	}
	if m.AuthorizationEndpoint == "" || m.TokenEndpoint == "" {
		return ProviderMetadata{}, errors.New("discovery document gives no authorization_endpoint or token_endpoint")
	}
	for name, v := range map[string]string{
		"authorization_endpoint": m.AuthorizationEndpoint, "token_endpoint": m.TokenEndpoint,
		"end_session_endpoint": m.EndSessionEndpoint, "revocation_endpoint": m.RevocationEndpoint,
		"device_authorization_endpoint": m.DeviceEndpoint,
	} {
		if v == "" {
			continue
		}
		u, err := url.Parse(v)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
			return ProviderMetadata{}, fmt.Errorf("discovery's %s %q is not an https URL", name, v)
		}
	}
	if len(m.CodeChallengeMethods) > 0 && !slices.Contains(m.CodeChallengeMethods, "S256") {
		return ProviderMetadata{}, errors.New("the issuer does not support PKCE with S256")
	}
	return m, nil
}

type providerEntry struct {
	meta    ProviderMetadata
	client  *http.Client
	fetched time.Time
}

// providerRetry is how long endpoints already known are served before
// discovery is tried again, once it failed.
const providerRetry = time.Minute

// providers caches each issuer's sign-in metadata and HTTP client. One
// read of an issuer's discovery is in flight at a time.
type providers struct {
	mu       sync.Mutex
	entries  map[string]*providerEntry
	fetching map[string]*sync.Mutex
	// newClient builds an issuer's client; nil is issuerClient.
	newClient func(i *Issuer) (*http.Client, error)
	// failed is told of every discovery that failed while known endpoints
	// were served in its place.
	failed func(i *Issuer, age time.Duration, err error)
}

func (p *providers) get(ctx context.Context, i *Issuer, now time.Time) (ProviderMetadata, *http.Client, error) {
	p.mu.Lock()
	e := p.entries[i.Name]
	if p.fetching == nil {
		p.fetching = map[string]*sync.Mutex{}
	}
	one, ok := p.fetching[i.Name]
	if !ok {
		one = &sync.Mutex{}
		p.fetching[i.Name] = one
	}
	p.mu.Unlock()
	if e != nil && now.Sub(e.fetched) < providerMetadataAge {
		return e.meta, e.client, nil
	}
	one.Lock()
	defer one.Unlock()
	p.mu.Lock()
	e = p.entries[i.Name]
	p.mu.Unlock()
	if e != nil && now.Sub(e.fetched) < providerMetadataAge {
		return e.meta, e.client, nil // another caller read it meanwhile
	}
	var client *http.Client
	if e != nil {
		client = e.client
	} else {
		var err error
		if p.newClient != nil {
			client, err = p.newClient(i)
		} else {
			client, err = issuerClient(i.CAFile)
		}
		if err != nil {
			return ProviderMetadata{}, nil, fmt.Errorf("issuer %q: %w", i.Name, err)
		}
	}
	meta, err := fetchProviderMetadata(ctx, client, i.Issuer)
	if err != nil {
		if e != nil {
			// The endpoints already known are served past their age while
			// discovery fails, and discovery is tried again a minute on,
			// not by every request meanwhile.
			p.mu.Lock()
			age := now.Sub(e.fetched)
			e.fetched = now.Add(providerRetry - providerMetadataAge)
			p.mu.Unlock()
			if p.failed != nil {
				p.failed(i, age, err)
			}
			return e.meta, e.client, nil
		}
		return ProviderMetadata{}, nil, fmt.Errorf("issuer %q: %w", i.Name, err)
	}
	p.mu.Lock()
	if p.entries == nil {
		p.entries = map[string]*providerEntry{}
	}
	p.entries[i.Name] = &providerEntry{meta: meta, client: client, fetched: now}
	p.mu.Unlock()
	return meta, client, nil
}

// DiscoverProvider reads an issuer's discovery document for sign-in with
// client, which should follow no redirect, and checks it
// (CheckProviderMetadata).
func DiscoverProvider(ctx context.Context, client *http.Client, issuer string) (ProviderMetadata, error) {
	return fetchProviderMetadata(ctx, client, issuer)
}

func fetchProviderMetadata(ctx context.Context, client *http.Client, issuer string) (ProviderMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(issuer, "/")+"/.well-known/openid-configuration", nil)
	if err != nil {
		return ProviderMetadata{}, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return ProviderMetadata{}, fmt.Errorf("discovery: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ProviderMetadata{}, fmt.Errorf("discovery: %s", resp.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxDiscoveryBytes+1))
	if err != nil {
		return ProviderMetadata{}, fmt.Errorf("discovery: %w", err)
	}
	if len(raw) > maxDiscoveryBytes {
		return ProviderMetadata{}, fmt.Errorf("discovery: more than %d bytes", maxDiscoveryBytes)
	}
	return CheckProviderMetadata(raw, issuer)
}

// MaxReturnPathBytes bounds a sign-in's return path, as asked for and as
// returned to.
const MaxReturnPathBytes = 2048

// SafeReturnPath is where a sign-in returns to: raw, its path escaped where
// it needs it and its query as written, when it is a path of this origin,
// else "/" (authn-authz.md §8). It is parsed, not
// pattern-matched: once parsed it must have no scheme and no authority,
// begin with exactly one "/", hold no backslash, no control character and
// no escaped slash or backslash, and its path must equal its own cleaned
// form. A browser reads "/\evil.example" as another host; that and every
// variant of it gives "/".
func SafeReturnPath(raw string) string {
	if raw == "" || len(raw) > MaxReturnPathBytes || strings.ContainsRune(raw, '\\') ||
		strings.ContainsFunc(raw, func(r rune) bool { return unicode.IsControl(r) || r == unicode.ReplacementChar }) {
		return "/"
	}
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") || strings.Contains(lower, "%00") {
		return "/"
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "" || u.Host != "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return "/"
	}
	p := u.Path
	if !strings.HasPrefix(p, "/") || strings.HasPrefix(p, "//") {
		return "/"
	}
	clean := path.Clean(p)
	if strings.HasSuffix(p, "/") && clean != "/" {
		clean += "/"
	}
	if clean != p {
		return "/"
	}
	// Escaping can make the path three times as long as raw: the bound is on
	// what is kept and sent, so it is checked again on that.
	if out := u.RequestURI(); len(out) <= MaxReturnPathBytes {
		return out
	}
	return "/"
}
