package identity

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

const (
	maxDiscoveryBytes = 64 << 10
	liveTimeout       = 10 * time.Second
)

// HTTPLiveIssuer reads what the issuer URL serves: its discovery document,
// then the key set at its jwks_uri, which must be https on the issuer's own
// host. Each read is bounded in size and time, as a trusted issuer's are.
type HTTPLiveIssuer struct {
	URL string
	// Client follows no redirect; NewHTTPLiveIssuer builds one.
	Client *http.Client
}

// NewHTTPLiveIssuer reads issuer with a client that follows no redirect.
func NewHTTPLiveIssuer(issuer string) HTTPLiveIssuer {
	return HTTPLiveIssuer{URL: issuer, Client: &http.Client{
		Timeout:       liveTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// PublishedKeyIDs answers the kids the issuer URL serves.
func (l HTTPLiveIssuer) PublishedKeyIDs(ctx context.Context) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, liveTimeout)
	defer cancel()
	raw, err := l.get(ctx, l.URL+"/.well-known/openid-configuration", maxDiscoveryBytes)
	if err != nil {
		return nil, fmt.Errorf("discovery: %w", err)
	}
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("discovery document: %w", err)
	}
	if doc.Issuer != l.URL {
		return nil, fmt.Errorf("discovery document states issuer %q, not %q", doc.Issuer, l.URL)
	}
	jwks, err := url.Parse(doc.JWKSURI)
	issuer, issErr := url.Parse(l.URL)
	if err != nil || issErr != nil || jwks.Scheme != "https" || !strings.EqualFold(jwks.Host, issuer.Host) || jwks.User != nil || jwks.RawQuery != "" || jwks.Fragment != "" {
		return nil, fmt.Errorf("discovery's jwks_uri %q is not https on the issuer's own host with no query", doc.JWKSURI)
	}
	raw, err = l.get(ctx, doc.JWKSURI, jwt.MaxKeySetBytes)
	if err != nil {
		return nil, fmt.Errorf("key set: %w", err)
	}
	set, err := jwt.ParseKeySet(raw)
	if err != nil {
		return nil, fmt.Errorf("key set: %w", err)
	}
	out := map[string]bool{}
	// A kid counts only for the key it is the thumbprint of: a set that
	// serves another key under our id does not serve ours.
	for _, kid := range set.KeyIDs() {
		k, _ := set.Lookup(kid)
		if tp, err := k.Thumbprint(); err == nil && tp == kid {
			out[kid] = true
		}
	}
	return out, nil
}

func (l HTTPLiveIssuer) get(ctx context.Context, rawURL string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := l.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", rawURL, resp.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", rawURL, err)
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("GET %s: more than %d bytes", rawURL, limit)
	}
	return raw, nil
}
