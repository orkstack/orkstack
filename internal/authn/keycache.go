package authn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

// Bounds of an issuer's key-set cache (authn-authz.md §3.1).
const (
	MinKeySetAge       = 5 * time.Minute
	MaxKeySetAge       = time.Hour
	UnknownKidInterval = time.Minute
	// FailureBackoff is how long a failed refresh is not retried while the
	// last good set may still be served, so an outage does not turn every
	// request into a fetch.
	FailureBackoff    = 15 * time.Second
	maxDiscoveryBytes = 1 << 20
	fetchTimeout      = 10 * time.Second
)

// KeyCache is one trusted issuer's key set, held in memory and refreshed
// on its Cache-Control age (bounded MinKeySetAge to MaxKeySetAge) and, for
// an unknown kid, at most once per UnknownKidInterval. When a refresh
// fails the last good set is served for at most the issuer's MaxStale past
// its age; then every kid is unavailable until a refresh succeeds: stale
// for a stated bound, then closed, never open. It implements
// jwt.KeySource.
type KeyCache struct {
	issuer *Issuer
	client *http.Client
	now    func() time.Time

	refresh chan struct{} // one fetch at a time; a waiter leaves when its ctx ends

	mu          sync.Mutex
	set         jwt.KeySet
	have        bool
	freshUntil  time.Time
	lastUnknown time.Time
	lastFailure time.Time
	lastErr     error
	jwksURI     string // from the entry, or from discovery once it succeeded
}

// NewKeyCache returns the cache for one issuer. client nil builds one that
// trusts the issuer's ca_file (or the system roots) and follows no
// redirect. now nil is time.Now. An issuer with jwks_file reads it here,
// once, and never fetches.
func NewKeyCache(i *Issuer, client *http.Client, now func() time.Time) (*KeyCache, error) {
	if i == nil {
		return nil, errors.New("no issuer")
	}
	if now == nil {
		now = time.Now
	}
	k := &KeyCache{issuer: i, now: now, jwksURI: i.JWKSURI, refresh: make(chan struct{}, 1)}
	if i.JWKSFile != "" {
		raw, err := readBounded(i.JWKSFile, jwt.MaxKeySetBytes)
		if err != nil {
			return nil, fmt.Errorf("issuer %q: jwks_file: %w", i.Name, err)
		}
		set, err := jwt.ParseKeySet(raw)
		if err != nil {
			return nil, fmt.Errorf("issuer %q: jwks_file: %w", i.Name, err)
		}
		k.set, k.have = set, true
		return k, nil
	}
	if client == nil {
		var err error
		if client, err = issuerClient(i.CAFile); err != nil {
			return nil, fmt.Errorf("issuer %q: %w", i.Name, err)
		}
	}
	k.client = client
	return k, nil
}

func issuerClient(caFile string) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := readBounded(caFile, 1<<20)
		if err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("ca_file holds no certificate")
		}
		tlsConfig.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &http.Client{
		Transport:     transport,
		Timeout:       fetchTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

func readBounded(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("%s is larger than %d bytes", path, limit)
	}
	return raw, nil
}

// Key implements jwt.KeySource.
func (k *KeyCache) Key(ctx context.Context, kid string) (jwt.PublicKey, bool, error) {
	if k.client == nil { // jwks_file: fixed
		key, ok := k.set.Lookup(kid)
		return key, ok, nil
	}
	now := k.now()
	k.mu.Lock()
	fresh := k.have && now.Before(k.freshUntil)
	key, found := k.set.Lookup(kid)
	unknownDue := now.Sub(k.lastUnknown) >= UnknownKidInterval
	k.mu.Unlock()

	switch {
	case fresh && found:
		return key, true, nil
	case fresh && !unknownDue:
		return jwt.PublicKey{}, false, nil
	}

	refreshErr := k.refreshOnce(ctx, fresh)
	now = k.now()
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.have || !now.Before(k.freshUntil.Add(k.issuer.MaxStale)) {
		return jwt.PublicKey{}, false, jwt.Unavailable(
			fmt.Sprintf("issuer %q's keys could not be refreshed and none are held within their stale bound", k.issuer.Name), refreshErr)
	}
	key, found = k.set.Lookup(kid)
	return key, found, nil
}

// refreshOnce fetches the key set unless another caller refreshed it while
// this one waited. forUnknown counts the attempt against the unknown-kid
// interval.
func (k *KeyCache) refreshOnce(ctx context.Context, forUnknown bool) error {
	select {
	case k.refresh <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-k.refresh }()
	k.mu.Lock()
	now := k.now()
	if !k.lastFailure.IsZero() && now.Sub(k.lastFailure) < FailureBackoff {
		err := k.lastErr
		k.mu.Unlock()
		return err // a refresh failed moments ago: serve what is held, or refuse
	}
	if forUnknown {
		if now.Sub(k.lastUnknown) < UnknownKidInterval {
			k.mu.Unlock()
			return nil // another caller just refreshed for an unknown kid
		}
		k.lastUnknown = now
	} else if k.have && now.Before(k.freshUntil) {
		k.mu.Unlock()
		return nil // refreshed by another caller while this one waited
	}
	jwksURI := k.jwksURI
	k.mu.Unlock()

	set, header, uri, err := k.fetch(ctx, jwksURI)
	now = k.now()
	k.mu.Lock()
	defer k.mu.Unlock()
	if err != nil {
		k.lastFailure, k.lastErr = now, err
		return err
	}
	k.lastFailure, k.lastErr = time.Time{}, nil
	k.set, k.have = set, true
	k.freshUntil = now.Add(keySetAge(header))
	if k.issuer.JWKSURI == "" {
		k.jwksURI = uri
	}
	return nil
}

// fetch discovers the key set's URI when it is not yet known, then reads
// and parses the set.
func (k *KeyCache) fetch(ctx context.Context, jwksURI string) (jwt.KeySet, http.Header, string, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	if jwksURI == "" {
		uri, err := k.discover(ctx)
		if err != nil {
			return jwt.KeySet{}, nil, "", err
		}
		jwksURI = uri
	}
	raw, header, err := k.get(ctx, jwksURI, jwt.MaxKeySetBytes)
	if err != nil {
		return jwt.KeySet{}, nil, "", err
	}
	set, err := jwt.ParseKeySet(raw)
	if err != nil {
		return jwt.KeySet{}, nil, "", fmt.Errorf("key set: %w", err)
	}
	return set, header, jwksURI, nil
}

// discover reads the issuer's discovery document (authn-authz.md §2,
// rule 6): it must state the configured issuer exactly, and its jwks_uri
// must be https on the issuer's own host.
func (k *KeyCache) discover(ctx context.Context) (string, error) {
	base := strings.TrimSuffix(k.issuer.Issuer, "/")
	raw, _, err := k.get(ctx, base+"/.well-known/openid-configuration", maxDiscoveryBytes)
	if err != nil {
		return "", fmt.Errorf("discovery: %w", err)
	}
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", fmt.Errorf("discovery document: %w", err)
	}
	if doc.Issuer != k.issuer.Issuer {
		return "", fmt.Errorf("discovery document states issuer %q, not the configured %q", doc.Issuer, k.issuer.Issuer)
	}
	jwks, err := url.Parse(doc.JWKSURI)
	issuer, issErr := url.Parse(k.issuer.Issuer)
	if err != nil || issErr != nil || jwks.Scheme != "https" || !strings.EqualFold(jwks.Host, issuer.Host) || jwks.User != nil {
		return "", fmt.Errorf("discovery's jwks_uri %q is not https on the issuer's own host; pin jwks_uri or supply jwks_file", doc.JWKSURI)
	}
	return doc.JWKSURI, nil
}

func (k *KeyCache) get(ctx context.Context, rawURL string, limit int64) ([]byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("GET %s: %s", rawURL, resp.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, nil, fmt.Errorf("GET %s: %w", rawURL, err)
	}
	if int64(len(raw)) > limit {
		return nil, nil, fmt.Errorf("GET %s: more than %d bytes", rawURL, limit)
	}
	return raw, resp.Header, nil
}

// keySetAge is the Cache-Control max-age, bounded to MinKeySetAge and
// MaxKeySetAge; MinKeySetAge when none is given.
func keySetAge(h http.Header) time.Duration {
	age := MinKeySetAge
	for _, directive := range strings.Split(h.Get("Cache-Control"), ",") {
		name, value, _ := strings.Cut(strings.TrimSpace(directive), "=")
		if strings.EqualFold(name, "max-age") {
			if n, err := strconv.ParseInt(strings.Trim(value, `"`), 10, 64); err == nil && n >= 0 {
				age = time.Duration(min(n, int64(MaxKeySetAge/time.Second))) * time.Second
			}
		}
	}
	return max(MinKeySetAge, min(age, MaxKeySetAge))
}
