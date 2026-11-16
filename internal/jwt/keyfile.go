package jwt

import (
	"bytes"
	"crypto"
	"crypto/x509"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"

	jose "github.com/go-jose/go-jose/v4"
)

// ParsePrivateKeyPEM reads one unencrypted PEM private key, the form an
// engine's signing key file holds (workload-identity.md §5): PKCS #8
// ("PRIVATE KEY"), PKCS #1 ("RSA PRIVATE KEY") or SEC 1 ("EC PRIVATE KEY").
// The key must be one NewSigner accepts. An encrypted key is refused: the
// engine has no passphrase to open it with.
func ParsePrivateKeyPEM(data []byte) (*Signer, error) {
	block, rest := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("more than one PEM block; a key file holds one key")
	}
	var (
		key any
		err error
	)
	switch block.Type {
	case "PRIVATE KEY":
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
	case "ENCRYPTED PRIVATE KEY":
		return nil, errors.New("the key is ENCRYPTED; give the engine an unencrypted key file it alone can read")
	default:
		return nil, fmt.Errorf("PEM block is %q; a PRIVATE KEY, RSA PRIVATE KEY or EC PRIVATE KEY is required", block.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("key type %T cannot sign", key)
	}
	return NewSigner(signer)
}

// MarshalKeySet writes keys as the JWK set (RFC 7517 §5) relying parties
// are served: each a public signature key under its thumbprint.
func MarshalKeySet(keys []PublicKey) ([]byte, error) {
	entries, err := publicJWKs(keys, "sig")
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Keys []jsontext.Value `json:"keys"`
	}{entries}, json.Deterministic(true))
}

// MarshalSPIFFEBundle writes keys as a SPIFFE trust domain's bundle (the
// SPIFFE Trust Domain and Bundle specification §4): a JWK set whose keys
// are marked for JWT-SVIDs, with the bundle's sequence and its refresh
// hint in seconds.
func MarshalSPIFFEBundle(keys []PublicKey, sequence, refreshHintSeconds int64) ([]byte, error) {
	if sequence < 0 || refreshHintSeconds <= 0 {
		return nil, fmt.Errorf("a bundle's sequence %d must not be negative and its refresh hint %d must be positive", sequence, refreshHintSeconds)
	}
	entries, err := publicJWKs(keys, "jwt-svid")
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Sequence    int64            `json:"spiffe_sequence"`
		RefreshHint int64            `json:"spiffe_refresh_hint"`
		Keys        []jsontext.Value `json:"keys"`
	}{sequence, refreshHintSeconds, entries}, json.Deterministic(true))
}

func publicJWKs(keys []PublicKey, use string) ([]jsontext.Value, error) {
	out := make([]jsontext.Value, 0, len(keys))
	for i, k := range keys {
		if k.IsZero() {
			return nil, fmt.Errorf("key %d is empty", i)
		}
		kid, err := k.Thumbprint()
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i, err)
		}
		jwk := jose.JSONWebKey{Key: k.key, KeyID: kid, Algorithm: string(k.Algorithm()), Use: use}
		raw, err := jwk.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i, err)
		}
		out = append(out, raw)
	}
	return out, nil
}

// ParsePublicJWK reads one public signature key as MarshalJWK wrote it,
// the form the published keys document stores. Its kid must be its own
// thumbprint, and no private or symmetric material may be present.
func ParsePublicJWK(raw []byte) (PublicKey, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return PublicKey{}, fmt.Errorf("a key is not one JSON object: %w", err)
	}
	for _, private := range []string{"d", "p", "q", "dp", "dq", "qi", "oth", "k"} {
		if _, present := fields[private]; present {
			return PublicKey{}, fmt.Errorf("the key carries private or symmetric key material (%q)", private)
		}
	}
	var jwk jose.JSONWebKey
	if err := jwk.UnmarshalJSON(raw); err != nil {
		return PublicKey{}, fmt.Errorf("parse key: %w", err)
	}
	if err := checkPublic(jwk.Key); err != nil {
		return PublicKey{}, err
	}
	key := PublicKey{key: jwk.Key}
	kid, err := key.Thumbprint()
	if err != nil {
		return PublicKey{}, err
	}
	if jwk.KeyID != kid {
		return PublicKey{}, fmt.Errorf("the key's kid %q is not its thumbprint %q", jwk.KeyID, kid)
	}
	if jwk.Algorithm != "" && jwk.Algorithm != string(key.Algorithm()) {
		return PublicKey{}, fmt.Errorf("the key is marked %s; it verifies %s", jwk.Algorithm, key.Algorithm())
	}
	key.kid = kid
	return key, nil
}
