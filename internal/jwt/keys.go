package jwt

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"

	jose "github.com/go-jose/go-jose/v4"
)

// MinRSABits is the smallest RSA modulus accepted (authn-authz.md §2,
// rule 3).
const MinRSABits = 2048

// MaxKeySetKeys and MaxKeySetBytes bound a key set read from an issuer
// (authn-authz.md §3.1).
const (
	MaxKeySetKeys  = 64
	MaxKeySetBytes = 1 << 20
)

// PublicKey is one verification key: an RSA key of at least MinRSABits or
// an ECDSA P-256 key. No other kind is ever a PublicKey.
type PublicKey struct {
	key crypto.PublicKey // *rsa.PublicKey or *ecdsa.PublicKey
	kid string           // the id it was published under; "" when none
}

// NewPublicKey checks key and returns it as a PublicKey.
func NewPublicKey(key crypto.PublicKey) (PublicKey, error) {
	if err := checkPublic(key); err != nil {
		return PublicKey{}, err
	}
	return PublicKey{key: key}, nil
}

// ParsePublicKeyPEM reads one PEM "PUBLIC KEY" block (PKIX), the form a
// service account's keys are given in (authn-authz.md §3.2).
func ParsePublicKeyPEM(data []byte) (PublicKey, error) {
	block, rest := pem.Decode(data)
	if block == nil {
		return PublicKey{}, errors.New("no PEM block found")
	}
	if block.Type != "PUBLIC KEY" {
		return PublicKey{}, fmt.Errorf("PEM block is %q; a PUBLIC KEY is required", block.Type)
	}
	if len(trimSpace(rest)) != 0 {
		return PublicKey{}, errors.New("more than one PEM block; give one key at a time")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return PublicKey{}, fmt.Errorf("parse public key: %w", err)
	}
	return NewPublicKey(key)
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\r' || b[0] == '\t') {
		b = b[1:]
	}
	return b
}

// checkPublic refuses every key that is not RSA of at least MinRSABits or
// ECDSA on P-256.
func checkPublic(key crypto.PublicKey) error {
	switch k := key.(type) {
	case *rsa.PublicKey:
		if k == nil || k.N == nil {
			return errors.New("empty RSA key")
		}
		if k.N.BitLen() < MinRSABits {
			return fmt.Errorf("RSA key is %d bits; at least %d", k.N.BitLen(), MinRSABits)
		}
		return nil
	case *ecdsa.PublicKey:
		if k == nil || k.Curve != elliptic.P256() {
			return errors.New("ECDSA key is not on P-256")
		}
		return nil
	}
	return fmt.Errorf("key type %T is not accepted (accepted: RSA of %d bits or more, ECDSA P-256)", key, MinRSABits)
}

// Algorithm returns the one algorithm this key verifies.
func (k PublicKey) Algorithm() Algorithm {
	if _, ok := k.key.(*ecdsa.PublicKey); ok {
		return ES256
	}
	return RS256
}

// KeyID is the id the key was published under, or "" for a key built from
// PEM or from a crypto key.
func (k PublicKey) KeyID() string { return k.kid }

// IsZero reports whether k holds no key.
func (k PublicKey) IsZero() bool { return k.key == nil }

// Public returns the crypto key.
func (k PublicKey) Public() crypto.PublicKey { return k.key }

// Thumbprint is the key's RFC 7638 SHA-256 thumbprint, base64url without
// padding: the kid OrkStack signs under and a service account's keys are
// named by.
func (k PublicKey) Thumbprint() (string, error) {
	if k.key == nil {
		return "", errors.New("no key")
	}
	jwk := jose.JSONWebKey{Key: k.key}
	sum, err := jwk.Thumbprint(crypto.SHA256)
	if err != nil {
		return "", fmt.Errorf("thumbprint: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(sum), nil
}

// fits checks rule 3: the key must be of the algorithm's type.
func (k PublicKey) fits(alg Algorithm) *Error {
	if k.key == nil {
		return refuse(ReasonKey, "no key")
	}
	if k.Algorithm() != alg {
		return refuse(ReasonKey, "the key for this kid verifies %s, not %s", k.Algorithm(), alg)
	}
	return nil
}

// MarshalJWK returns the key as a public JWK carrying kid, alg and use, the
// form a published key set holds.
func (k PublicKey) MarshalJWK(kid string) ([]byte, error) {
	if k.key == nil {
		return nil, errors.New("no key")
	}
	jwk := jose.JSONWebKey{Key: k.key, KeyID: kid, Algorithm: string(k.Algorithm()), Use: "sig"}
	return jwk.MarshalJSON()
}

// KeySet is the usable keys of one published key set, by kid.
type KeySet struct {
	keys map[string]PublicKey
	// ambiguous holds kids published more than once: none of them is used.
	ambiguous map[string]bool
}

// ParseKeySet reads a JWK set (RFC 7517 §5) as an issuer publishes it. It
// refuses a set over MaxKeySetBytes or MaxKeySetKeys, and one that carries
// private key material. It keeps only signature keys this package accepts
// (RSA of MinRSABits or more, P-256) that have a kid; a key of another kind,
// or one marked for encryption, is skipped rather than refused, since an
// issuer's set serves more than OrkStack. A kid that appears twice is never
// used.
func ParseKeySet(data []byte) (KeySet, error) {
	if len(data) > MaxKeySetBytes {
		return KeySet{}, fmt.Errorf("key set is %d bytes; at most %d", len(data), MaxKeySetBytes)
	}
	members, err := decodeObject(data)
	if err != nil {
		return KeySet{}, fmt.Errorf("key set is not one JSON object: %w", err)
	}
	rawKeys, ok := members["keys"]
	if !ok || rawKeys.Kind() != '[' {
		return KeySet{}, errors.New(`key set has no "keys" array`)
	}
	var entries []jsontext.Value
	if err := json.Unmarshal(rawKeys, &entries); err != nil {
		return KeySet{}, fmt.Errorf("key set keys: %w", err)
	}
	if len(entries) > MaxKeySetKeys {
		return KeySet{}, fmt.Errorf("key set has %d keys; at most %d are read", len(entries), MaxKeySetKeys)
	}
	set := KeySet{keys: map[string]PublicKey{}, ambiguous: map[string]bool{}}
	for i, entry := range entries {
		fields, err := decodeObject(entry)
		if err != nil {
			return KeySet{}, fmt.Errorf("key %d: %w", i, err)
		}
		for _, private := range []string{"d", "p", "q", "dp", "dq", "qi", "oth", "k"} {
			if _, present := fields[private]; present {
				return KeySet{}, fmt.Errorf("key %d carries private or symmetric key material (%q); a published set holds public keys only", i, private)
			}
		}
		kid, _, kErr := stringMember(fields, "kid")
		if kErr != nil || kid == "" || len(kid) > maxKidBytes {
			continue
		}
		if use, _, _ := stringMember(fields, "use"); use != "" && use != "sig" {
			continue
		}
		var jwk jose.JSONWebKey
		if err := jwk.UnmarshalJSON(entry); err != nil {
			continue // a kind the library does not read: not ours to use
		}
		if checkPublic(jwk.Key) != nil {
			continue
		}
		key := PublicKey{key: jwk.Key, kid: kid}
		if alg, _, _ := stringMember(fields, "alg"); alg != "" && alg != string(key.Algorithm()) {
			continue // published for an algorithm we would not use it with
		}
		if _, dup := set.keys[kid]; dup || set.ambiguous[kid] {
			delete(set.keys, kid)
			set.ambiguous[kid] = true
			continue
		}
		set.keys[kid] = key
	}
	return set, nil
}

// Lookup returns the key published under kid.
func (s KeySet) Lookup(kid string) (PublicKey, bool) {
	k, ok := s.keys[kid]
	return k, ok
}

// Len is the number of usable keys.
func (s KeySet) Len() int { return len(s.keys) }

// KeySource finds the key a token names. An error that is a *Error with
// ReasonUnavailable says the source could not answer; any other error, or
// ok false, says there is no such key.
type KeySource interface {
	Key(ctx context.Context, kid string) (key PublicKey, ok bool, err error)
}

// Static is a fixed KeySource: a key set read once, or a service account's
// keys, each under its thumbprint.
type Static map[string]PublicKey

// Key implements KeySource.
func (s Static) Key(_ context.Context, kid string) (PublicKey, bool, error) {
	k, ok := s[kid]
	return k, ok, nil
}

// StaticFromSet is a key set as a Static source.
func StaticFromSet(set KeySet) Static {
	out := make(Static, len(set.keys))
	for kid, k := range set.keys {
		out[kid] = k
	}
	return out
}

// StaticByThumbprint indexes keys under their RFC 7638 thumbprints, the
// kid a service account's assertion carries.
func StaticByThumbprint(keys ...PublicKey) (Static, error) {
	out := make(Static, len(keys))
	for _, k := range keys {
		tp, err := k.Thumbprint()
		if err != nil {
			return nil, err
		}
		out[tp] = k
	}
	return out, nil
}
