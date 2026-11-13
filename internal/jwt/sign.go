package jwt

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/cryptosigner"
)

// Signer signs tokens with one key: RS256 for an RSA key of MinRSABits or
// more, ES256 for a P-256 key, with kid the key's RFC 7638 thumbprint.
type Signer struct {
	alg    Algorithm
	kid    string
	signer crypto.Signer
	public PublicKey
}

// NewSigner checks key and returns a Signer for it.
func NewSigner(key crypto.Signer) (*Signer, error) {
	if key == nil {
		return nil, errors.New("no signing key")
	}
	public, err := NewPublicKey(key.Public())
	if err != nil {
		return nil, err
	}
	kid, err := public.Thumbprint()
	if err != nil {
		return nil, err
	}
	return &Signer{alg: public.Algorithm(), kid: kid, signer: key, public: public}, nil
}

// KeyID is the kid this Signer writes: its key's thumbprint.
func (s *Signer) KeyID() string { return s.kid }

// Algorithm is the algorithm this Signer writes.
func (s *Signer) Algorithm() Algorithm { return s.alg }

// Public is the key that verifies this Signer's tokens.
func (s *Signer) Public() PublicKey { return s.public }

// Sign writes claims as a compact JWS with typ and kid in its protected
// header. claims is any value encoding/json/v2 encodes as an object.
func (s *Signer) Sign(typ string, claims any) (string, error) {
	payload, err := json.Marshal(claims, json.Deterministic(true))
	if err != nil {
		return "", fmt.Errorf("encode claims: %w", err)
	}
	if _, err := decodeObject(payload); err != nil {
		return "", fmt.Errorf("claims must encode as one JSON object: %w", err)
	}
	var key any = s.signer
	switch s.signer.(type) {
	case *rsa.PrivateKey, *ecdsa.PrivateKey:
	default:
		key = cryptosigner.Opaque(s.signer)
	}
	opts := (&jose.SignerOptions{}).WithHeader(jose.HeaderKey("kid"), s.kid)
	if typ != "" {
		opts = opts.WithType(jose.ContentType(typ))
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.SignatureAlgorithm(s.alg), Key: key}, opts)
	if err != nil {
		return "", fmt.Errorf("signer: %w", err)
	}
	obj, err := signer.Sign(payload)
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	token, err := obj.CompactSerialize()
	if err != nil {
		return "", fmt.Errorf("serialize: %w", err)
	}
	if len(token) > MaxTokenBytes {
		return "", fmt.Errorf("signed token is %d bytes; at most %d", len(token), MaxTokenBytes)
	}
	return token, nil
}

// AssertionRequest is the request a client signs an assertion for.
type AssertionRequest struct {
	Account  string // the service account's name
	Audience string // the deployment's public URL
	Method   string
	URL      string // the request's URL; its query and fragment are dropped
	Body     []byte
}

// SignRequest writes a service account's assertion bound to one request
// (authn-authz.md §3.2): typ ork-request+jwt, iss and sub
// serviceaccount:<Account>, a fresh 128-bit jti, exp 60 s ahead, and htm,
// htu and ork_body_sha256. extra claims are added beside them; none may
// replace one of these.
func (s *Signer) SignRequest(r AssertionRequest, now time.Time, extra map[string]any) (string, error) {
	if r.Account == "" {
		return "", errNoAccount
	}
	if r.Audience == "" || r.Method == "" {
		return "", errors.New("an assertion needs an audience and a method")
	}
	u, err := url.Parse(r.URL)
	if err != nil || !u.IsAbs() || u.Host == "" {
		return "", fmt.Errorf("assertion URL %q is not absolute", r.URL)
	}
	u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment, u.User = "", false, "", "", nil
	var jti [16]byte
	if _, err := rand.Read(jti[:]); err != nil {
		return "", fmt.Errorf("jti: %w", err)
	}
	body := BodySHA256(r.Body)
	iss := ServiceAccountIssuerPrefix + r.Account
	claims := map[string]any{
		"iss":             iss,
		"sub":             iss,
		"aud":             r.Audience,
		"iat":             now.Unix(),
		"exp":             now.Add(MaxAssertionLifetime).Unix(),
		"jti":             base64.RawURLEncoding.EncodeToString(jti[:]),
		"htm":             r.Method,
		"htu":             u.String(),
		"ork_body_sha256": hex.EncodeToString(body[:]),
	}
	for name := range extra {
		if _, taken := claims[name]; taken {
			return "", fmt.Errorf("claim %q is set by the assertion itself", name)
		}
	}
	maps.Copy(claims, extra)
	return s.Sign(TypeRequest, claims)
}
