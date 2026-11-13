package jwt

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

// MaxLeeway bounds the clock leeway a Policy may allow.
const MaxLeeway = 5 * time.Minute

// Policy is what a token from one issuer must satisfy (authn-authz.md §2).
type Policy struct {
	// Issuer is compared with iss exactly.
	Issuer string
	// Audiences: aud must contain one of them.
	Audiences []string
	// Algorithms is the issuer's allow-list. It is enforced twice: by this
	// package before the library is called, and by the library's parse.
	Algorithms []Algorithm
	// MaxLifetime refuses a token whose exp - iat exceeds it. Required.
	MaxLifetime time.Duration
	// Leeway is the clock skew allowed on exp, nbf and iat
	// (auth.clock_skew, 60 s by default).
	Leeway time.Duration
}

// Validate refuses a Policy that could accept a token by omission.
func (p Policy) Validate() error {
	var errs []error
	if p.Issuer == "" {
		errs = append(errs, errors.New("issuer is required"))
	}
	if len(p.Audiences) == 0 || slices.Contains(p.Audiences, "") {
		errs = append(errs, errors.New("at least one non-empty audience is required"))
	}
	if len(p.Algorithms) == 0 {
		errs = append(errs, errors.New("at least one algorithm is required"))
	}
	for _, alg := range p.Algorithms {
		if _, err := ParseAlgorithm(string(alg)); err != nil {
			errs = append(errs, err)
		}
	}
	if p.MaxLifetime <= 0 {
		errs = append(errs, errors.New("max lifetime must be positive"))
	}
	if p.Leeway < 0 || p.Leeway > MaxLeeway {
		errs = append(errs, fmt.Errorf("leeway must be between 0 and %s", MaxLeeway))
	}
	return errors.Join(errs...)
}

// ownAlgorithmCheck is off only in a test that proves the library's parse
// refuses an algorithm outside the list by itself.
var ownAlgorithmCheck = true

// options are what differs between the kinds of token one Policy judges.
type options struct {
	types typeRule
	// audience replaces Policy.Audiences's check when set (an ID token's).
	audience func(c *Claims) *Error
}

// Verify checks a bearer token from a trusted issuer: every rule of
// authn-authz.md §2 except the ID token's (VerifyIDToken). keys is asked
// for one key, only once every header rule has passed.
func Verify(ctx context.Context, token string, p Policy, keys KeySource, now time.Time) (*Claims, error) {
	c, err := verify(ctx, token, p, keys, now, options{types: typeIssued})
	if err != nil {
		return nil, err
	}
	return c, nil
}

// VerifyIDToken checks a sign-in's ID token (authn-authz.md §2, rule 7):
// aud is exactly clientID, or contains it with azp equal to it; nonce
// matches; a token of typ at+jwt is never one. p.Audiences is not used.
func VerifyIDToken(ctx context.Context, token string, p Policy, keys KeySource, now time.Time, clientID, nonce string) (*Claims, error) {
	if clientID == "" || nonce == "" {
		return nil, errors.New("an ID token is verified against a client id and a nonce")
	}
	opts := options{types: typeIDToken, audience: func(c *Claims) *Error {
		if len(c.Audience) == 1 && c.Audience[0] == clientID {
			return nil
		}
		if !slices.Contains(c.Audience, clientID) {
			return refuse(ReasonClaims, "aud does not name this client")
		}
		azp, ok := c.String("azp")
		if !ok || azp != clientID {
			return refuse(ReasonClaims, "aud names several parties and azp is not this client")
		}
		return nil
	}}
	if p.Audiences == nil {
		p.Audiences = []string{clientID} // Validate wants one; the check above replaces it
	}
	c, err := verify(ctx, token, p, keys, now, opts)
	if err != nil {
		return nil, err
	}
	got, ok := c.String("nonce")
	if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(nonce)) != 1 {
		return nil, refuse(ReasonClaims, "nonce does not match the sign-in")
	}
	return c, nil
}

func verify(ctx context.Context, token string, p Policy, keys KeySource, now time.Time, opts options) (*Claims, *Error) {
	if err := p.Validate(); err != nil {
		return nil, refuseWrap(ReasonUnavailable, err, "the verifier is misconfigured")
	}
	headerJSON, segments, rErr := split(token)
	if rErr != nil {
		return nil, rErr
	}
	ours := p.Algorithms
	if !ownAlgorithmCheck {
		ours = Algorithms()
	}
	h, rErr := checkHeader(headerJSON, ours, opts.types)
	if rErr != nil {
		return nil, rErr
	}
	// The payload must be JSON before a key is looked up: a token that can
	// never pass is not worth a key-set refresh.
	payloadJSON, err := base64.RawURLEncoding.Strict().DecodeString(segments[1])
	if err != nil {
		return nil, refuseWrap(ReasonMalformed, err, "payload is not base64url")
	}
	if _, err := decodeObject(payloadJSON); err != nil {
		return nil, refuseWrap(ReasonMalformed, err, "payload is not one JSON object")
	}

	key, rErr := findKey(ctx, keys, h)
	if rErr != nil {
		return nil, rErr
	}

	jws, err := jose.ParseSignedCompact(token, joseAlgorithms(p.Algorithms))
	if err != nil {
		var unexpected *jose.ErrUnexpectedSignatureAlgorithm
		if errors.As(err, &unexpected) {
			return nil, refuseWrap(ReasonAlgorithm, err, "alg %q is not one this issuer is configured for", h.alg)
		}
		return nil, refuseWrap(ReasonMalformed, err, "the token does not parse")
	}
	if len(jws.Signatures) != 1 {
		return nil, refuse(ReasonMalformed, "a token has exactly one signature")
	}
	if got := jws.Signatures[0].Protected; got.Algorithm != string(h.alg) || got.KeyID != h.kid {
		return nil, refuse(ReasonMalformed, "the header read twice gave two answers")
	}
	verified, err := jws.Verify(key.key)
	if err != nil {
		return nil, refuseWrap(ReasonSignature, err, "the signature does not verify with the key %q names", h.kid)
	}

	members, err := decodeObject(verified)
	if err != nil {
		return nil, refuseWrap(ReasonMalformed, err, "payload is not one JSON object")
	}
	c, rErr := parseClaims(members)
	if rErr != nil {
		return nil, rErr
	}
	if rErr := checkClaims(c, p, now, opts); rErr != nil {
		return nil, rErr
	}
	return c, nil
}

func findKey(ctx context.Context, keys KeySource, h header) (PublicKey, *Error) {
	if keys == nil {
		return PublicKey{}, refuse(ReasonUnavailable, "no key source")
	}
	key, ok, err := keys.Key(ctx, h.kid)
	if err != nil {
		if ReasonOf(err) == ReasonUnavailable {
			return PublicKey{}, refuseWrap(ReasonUnavailable, err, "the issuer's keys are unavailable")
		}
		return PublicKey{}, refuseWrap(ReasonKey, err, "no key for kid %q", h.kid)
	}
	if !ok {
		return PublicKey{}, refuse(ReasonKey, "no key for kid %q", h.kid)
	}
	if rErr := key.fits(h.alg); rErr != nil {
		return PublicKey{}, rErr
	}
	return key, nil
}

func checkClaims(c *Claims, p Policy, now time.Time, opts options) *Error {
	if c.Issuer != p.Issuer {
		return refuse(ReasonClaims, "iss is not the configured issuer")
	}
	if opts.audience != nil {
		if err := opts.audience(c); err != nil {
			return err
		}
	} else if !slices.ContainsFunc(c.Audience, func(a string) bool { return slices.Contains(p.Audiences, a) }) {
		return refuse(ReasonClaims, "aud names no configured audience")
	}
	if !now.Before(c.Expiry.Add(p.Leeway)) {
		return refuse(ReasonExpired, "the token expired at %s", c.Expiry.Format(time.RFC3339))
	}
	if !c.NotBefore.IsZero() && now.Add(p.Leeway).Before(c.NotBefore) {
		return refuse(ReasonNotYetValid, "the token is not valid before %s", c.NotBefore.Format(time.RFC3339))
	}
	if now.Add(p.Leeway).Before(c.IssuedAt) {
		return refuse(ReasonNotYetValid, "the token was issued in the future (%s)", c.IssuedAt.Format(time.RFC3339))
	}
	if !c.Expiry.After(c.IssuedAt) {
		return refuse(ReasonClaims, "exp is not after iat")
	}
	if life := c.Expiry.Sub(c.IssuedAt); life > p.MaxLifetime {
		return refuse(ReasonLifetime, "the token lives %s; this issuer's tokens live at most %s", life, p.MaxLifetime)
	}
	return nil
}

func joseAlgorithms(algs []Algorithm) []jose.SignatureAlgorithm {
	out := make([]jose.SignatureAlgorithm, 0, len(algs))
	for _, a := range algs {
		out = append(out, jose.SignatureAlgorithm(a))
	}
	return out
}

// PeekIssuer returns a token's iss without verifying anything but its
// shape, so a caller can choose which verifier judges it (a trusted
// issuer's token or a service account's assertion; authn-authz.md §3).
// The value is unverified and must never be recorded or trusted.
func PeekIssuer(token string) (string, error) {
	_, segments, rErr := split(token)
	if rErr != nil {
		return "", rErr
	}
	payloadJSON, err := base64.RawURLEncoding.Strict().DecodeString(segments[1])
	if err != nil {
		return "", refuseWrap(ReasonMalformed, err, "payload is not base64url")
	}
	members, err := decodeObject(payloadJSON)
	if err != nil {
		return "", refuseWrap(ReasonMalformed, err, "payload is not one JSON object")
	}
	iss, rErr := requiredString(members, "iss")
	if rErr != nil {
		return "", rErr
	}
	return iss, nil
}
