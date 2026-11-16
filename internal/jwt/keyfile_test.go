package jwt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"encoding/pem"
	"strings"
	"testing"
)

func pemOf(t *testing.T, typ string, der []byte) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

// TestParsePrivateKeyPEM: the three forms a key file is written in are
// read, and everything else is refused with its reason.
func TestParsePrivateKeyPEM(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	small, _ := rsa.GenerateKey(rand.Reader, 1024)
	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	pkcs8 := func(k any) []byte {
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	sec1, _ := x509.MarshalECPrivateKey(ecKey)
	for _, tc := range []struct {
		name string
		file []byte
		alg  Algorithm
		want string
	}{
		{"pkcs8 rsa", pemOf(t, "PRIVATE KEY", pkcs8(rsaKey)), RS256, ""},
		{"pkcs1 rsa", pemOf(t, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(rsaKey)), RS256, ""},
		{"pkcs8 p256", pemOf(t, "PRIVATE KEY", pkcs8(ecKey)), ES256, ""},
		{"sec1 p256", pemOf(t, "EC PRIVATE KEY", sec1), ES256, ""},
		{"empty", nil, "", "no PEM block"},
		{"public key", pemOf(t, "PUBLIC KEY", []byte{1}), "", `"PUBLIC KEY"`},
		{"encrypted", []byte("-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n"), "", "ENCRYPTED"},
		{"two keys", append(pemOf(t, "PRIVATE KEY", pkcs8(ecKey)), pemOf(t, "PRIVATE KEY", pkcs8(ecKey))...), "", "more than one"},
		{"small rsa", pemOf(t, "PRIVATE KEY", pkcs8(small)), "", "1024"},
		{"p384", pemOf(t, "PRIVATE KEY", pkcs8(p384)), "", "P-256"},
		{"garbage", pemOf(t, "PRIVATE KEY", []byte("nope")), "", "parse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ParsePrivateKeyPEM(tc.file)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("err = %v, want one naming %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.Algorithm() != tc.alg || s.KeyID() == "" {
				t.Fatalf("signer %s %q", s.Algorithm(), s.KeyID())
			}
		})
	}
}

// TestKeySetDocuments: the set relying parties are served and the SPIFFE
// bundle both carry every key under its kid, read back through
// ParseKeySet, and hold no private material; the bundle marks each key
// for jwt-svid and carries its sequence and refresh hint.
func TestKeySetDocuments(t *testing.T) {
	var keys []PublicKey
	for range 2 {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		s, err := NewSigner(k)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, s.Public())
	}
	r, _ := rsa.GenerateKey(rand.Reader, 3072)
	rs, _ := NewSigner(r)
	keys = append(keys, rs.Public())

	set, err := MarshalKeySet(keys)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseKeySet(set)
	if err != nil || parsed.Len() != 3 {
		t.Fatalf("the served set read back as %d keys, %v", parsed.Len(), err)
	}
	for _, k := range keys {
		kid, _ := k.Thumbprint()
		if _, ok := parsed.Lookup(kid); !ok {
			t.Fatalf("kid %s is missing", kid)
		}
	}

	bundle, err := MarshalSPIFFEBundle(keys, 7, 300)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Sequence    int64            `json:"spiffe_sequence"`
		RefreshHint int64            `json:"spiffe_refresh_hint"`
		Keys        []jsontext.Value `json:"keys"`
	}
	if err := json.Unmarshal(bundle, &doc); err != nil || doc.Sequence != 7 || doc.RefreshHint != 300 || len(doc.Keys) != 3 {
		t.Fatalf("bundle = %s, %v", bundle, err)
	}
	for _, raw := range doc.Keys {
		var k struct {
			Use string `json:"use"`
			Kid string `json:"kid"`
		}
		if err := json.Unmarshal(raw, &k); err != nil || k.Use != "jwt-svid" || k.Kid == "" {
			t.Fatalf("bundle key %s", raw)
		}
	}
	for _, private := range []string{`"d"`, `"p"`, `"q"`} {
		if strings.Contains(string(set), private) || strings.Contains(string(bundle), private) {
			t.Fatalf("a published document holds %s", private)
		}
	}
	if _, err := MarshalKeySet([]PublicKey{{}}); err == nil {
		t.Fatal("an empty key was written")
	}
}

// TestParsePublicJWK: a stored key reads back as the key it was written
// from, under its thumbprint; private material and foreign kinds are
// refused.
func TestParsePublicJWK(t *testing.T) {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	s, _ := NewSigner(k)
	raw, err := s.Public().MarshalJWK(s.KeyID())
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParsePublicJWK(raw)
	if err != nil {
		t.Fatal(err)
	}
	if kid, _ := back.Thumbprint(); kid != s.KeyID() || back.KeyID() != s.KeyID() {
		t.Fatalf("read back as %s / %s, want %s", kid, back.KeyID(), s.KeyID())
	}
	private := strings.Replace(string(raw), `{`, `{"d":"AAAA",`, 1)
	for name, bad := range map[string]string{
		"private": private, "not an object": `[]`, "symmetric": `{"kty":"oct","k":"AAAA"}`,
		"another kid": strings.Replace(string(raw), s.KeyID(), "other", 1),
	} {
		if _, err := ParsePublicJWK([]byte(bad)); err == nil {
			t.Errorf("%s was read", name)
		}
	}
}
