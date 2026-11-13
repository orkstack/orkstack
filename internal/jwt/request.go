package jwt

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ServiceAccountIssuerPrefix begins the iss (and sub) of every service
// account's assertion: serviceaccount:<name> (authn-authz.md §3.2).
const ServiceAccountIssuerPrefix = "serviceaccount:"

// MaxAssertionLifetime is how far ahead a bound assertion's exp may be.
const MaxAssertionLifetime = 60 * time.Second

// maxJTIBytes bounds an assertion's jti; 16 random bytes are 22 in
// base64url.
const maxJTIBytes = 128

// Request is what the engine received, against which a bound assertion is
// checked (authn-authz.md §3.2).
type Request struct {
	// Method is the HTTP method.
	Method string
	// Path is the request's path as the engine received it, escaped as it
	// arrived (URL.EscapedPath), without query or fragment.
	Path string
	// BodySHA256 is the SHA-256 of the bytes the engine read, hashed before
	// any handler ran; of the empty string for a request with no body.
	BodySHA256 [sha256.Size]byte
	// HostAllowed reports whether the listener's host policy accepts the
	// host htu names (internal/hostpolicy). Required.
	HostAllowed func(host string) bool
	// PublicURL, when set (auth.public_url), fixes htu's scheme and host.
	PublicURL *url.URL
}

// AssertionPolicy is what a service account's bound assertion must satisfy.
type AssertionPolicy struct {
	// Account is the service account's name: iss and sub must both be
	// serviceaccount:<Account>.
	Account string
	// Audience is the deployment's public URL; aud must contain it.
	Audience string
	// Leeway is auth.clock_skew.
	Leeway time.Duration
}

// VerifyRequest checks a service account's self-signed assertion and that
// it was signed for this request: typ ork-request+jwt, iss and sub the
// account, exp at most 60 s ahead, and htm, htu, ork_body_sha256 and jti
// (authn-authz.md §3.2). keys holds the account's keys under their
// thumbprints. replay records the jti only once everything else passed; a
// jti it has seen is refused. An assertion that is not bound is refused
// with ReasonBinding, saying why.
func VerifyRequest(ctx context.Context, token string, p AssertionPolicy, keys KeySource, req Request, replay *ReplayCache, now time.Time) (*Claims, error) {
	if p.Account == "" || p.Audience == "" || req.HostAllowed == nil || replay == nil {
		return nil, refuse(ReasonUnavailable, "the assertion verifier is misconfigured")
	}
	iss := ServiceAccountIssuerPrefix + p.Account
	policy := Policy{
		Issuer:      iss,
		Audiences:   []string{p.Audience},
		Algorithms:  Algorithms(),
		MaxLifetime: MaxAssertionLifetime,
		Leeway:      p.Leeway,
	}
	c, rErr := verify(ctx, token, policy, keys, now, options{types: typeRequestOnly})
	if rErr != nil {
		return nil, rErr
	}
	if c.Subject != iss {
		return nil, refuse(ReasonClaims, "sub must equal iss for a service account's assertion")
	}
	if c.Expiry.Sub(now) > MaxAssertionLifetime+p.Leeway {
		return nil, refuse(ReasonLifetime, "exp is more than %s ahead", MaxAssertionLifetime)
	}
	if rErr := checkBinding(c, req); rErr != nil {
		return nil, rErr
	}
	if c.ID == "" || len(c.ID) > maxJTIBytes {
		return nil, refuse(ReasonBinding, "jti is required, at most %d bytes", maxJTIBytes)
	}
	if rErr := replay.use(iss, c.ID, c.Expiry.Add(p.Leeway), now); rErr != nil {
		return nil, rErr
	}
	return c, nil
}

func checkBinding(c *Claims, req Request) *Error {
	htm, ok := c.String("htm")
	if !ok || htm == "" {
		return refuse(ReasonBinding, "htm is missing: the assertion is not bound to a request")
	}
	if htm != req.Method {
		return refuse(ReasonBinding, "htm %q is not this request's method", htm)
	}
	htu, ok := c.String("htu")
	if !ok || htu == "" {
		return refuse(ReasonBinding, "htu is missing: the assertion is not bound to a request")
	}
	u, err := url.Parse(htu)
	if err != nil || !u.IsAbs() || u.Host == "" || u.User != nil {
		return refuse(ReasonBinding, "htu is not an absolute URL")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(htu, "?#") {
		return refuse(ReasonBinding, "htu carries a query or a fragment")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return refuse(ReasonBinding, "htu's scheme is not http or https")
	}
	if u.EscapedPath() != req.Path {
		return refuse(ReasonBinding, "htu's path is not this request's")
	}
	if !req.HostAllowed(u.Host) {
		return refuse(ReasonBinding, "htu's host is not one this listener accepts")
	}
	if req.PublicURL != nil && (!strings.EqualFold(u.Scheme, req.PublicURL.Scheme) || !strings.EqualFold(u.Host, req.PublicURL.Host)) {
		return refuse(ReasonBinding, "htu's scheme and host are not the deployment's public URL")
	}
	sum, ok := c.String("ork_body_sha256")
	if !ok || sum == "" {
		return refuse(ReasonBinding, "ork_body_sha256 is missing: the assertion is not bound to a request")
	}
	want := hex.EncodeToString(req.BodySHA256[:])
	if sum != want {
		return refuse(ReasonBinding, "ork_body_sha256 is not the hash of this request's body")
	}
	return nil
}

// MaxReplayEntries bounds a ReplayCache, and MaxReplayPerIssuer bounds one
// issuer's share of it, so one account sending at its limit cannot fill the
// cache and lock every other account out.
const (
	MaxReplayEntries   = 100_000
	MaxReplayPerIssuer = 10_000
)

// ReplayCache remembers the jti of every assertion this replica accepted,
// for the assertion's life and the leeway (authn-authz.md §3.2). It is one
// replica's memory, never stored and never shared. When it holds
// MaxReplayEntries live entries it refuses new assertions rather than
// forgetting one early: closed, never open. One issuer holds at most
// MaxReplayPerIssuer entries; past that only that issuer is refused.
// Break-glass assertions are judged with a cache of their own, so no
// account can crowd them out.
type ReplayCache struct {
	mu        sync.Mutex
	max       int
	perIssuer int
	seen      map[string]time.Time
	byIssuer  map[string]int
	expires   expiryHeap
}

// NewReplayCache returns an empty cache bounded at MaxReplayEntries.
func NewReplayCache() *ReplayCache { return newReplayCache(MaxReplayEntries, MaxReplayPerIssuer) }

func newReplayCache(max, perIssuer int) *ReplayCache {
	return &ReplayCache{max: max, perIssuer: perIssuer, seen: map[string]time.Time{}, byIssuer: map[string]int{}}
}

// Len is the number of entries held, live or not yet swept.
func (r *ReplayCache) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func (r *ReplayCache) use(issuer, jti string, until, now time.Time) *Error {
	key := issuer + "\x00" + jti
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.expires) > 0 && !r.expires[0].until.After(now) {
		e := heap.Pop(&r.expires).(expiry)
		if r.seen[e.key].Equal(e.until) {
			delete(r.seen, e.key)
			if r.byIssuer[e.issuer]--; r.byIssuer[e.issuer] <= 0 {
				delete(r.byIssuer, e.issuer)
			}
		}
	}
	if _, ok := r.seen[key]; ok {
		return refuse(ReasonReplay, "this assertion was already used at this replica")
	}
	if r.byIssuer[issuer] >= r.perIssuer {
		return refuse(ReasonUnavailable, "this replica holds %d unexpired assertions of this account; try again shortly", r.perIssuer)
	}
	if len(r.seen) >= r.max {
		return refuse(ReasonUnavailable, "this replica holds %d unexpired assertions; try again shortly", r.max)
	}
	r.seen[key] = until
	r.byIssuer[issuer]++
	heap.Push(&r.expires, expiry{key: key, issuer: issuer, until: until})
	return nil
}

type expiry struct {
	key    string
	issuer string
	until  time.Time
}

type expiryHeap []expiry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].until.Before(h[j].until) }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(expiry)) }
func (h *expiryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// BodySHA256 hashes a request body as ork_body_sha256 carries it.
func BodySHA256(body []byte) [sha256.Size]byte { return sha256.Sum256(body) }

var errNoAccount = errors.New("a service account name is required")
