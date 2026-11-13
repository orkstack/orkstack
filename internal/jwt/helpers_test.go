package jwt

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Keys are generated once per test binary: RSA generation is slow.
var (
	keysOnce sync.Once
	rsaKey   *rsa.PrivateKey
	ecKey    *ecdsa.PrivateKey
)

func testKeys(t testing.TB) (*rsa.PrivateKey, *ecdsa.PrivateKey) {
	t.Helper()
	keysOnce.Do(func() {
		var err error
		if rsaKey, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if ecKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			panic(err)
		}
	})
	return rsaKey, ecKey
}

// testNow is the instant every test token is judged at.
var testNow = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

const (
	testIssuer   = "https://issuer.example"
	testAudience = "orkstack"
	kidRSA       = "rsa-1"
	kidEC        = "ec-1"
)

func testPolicy() Policy {
	return Policy{
		Issuer:      testIssuer,
		Audiences:   []string{testAudience},
		Algorithms:  []Algorithm{RS256, ES256},
		MaxLifetime: time.Hour,
		Leeway:      60 * time.Second,
	}
}

// spyKeys is a KeySource that counts how often it was asked.
type spyKeys struct {
	keys  Static
	calls atomic.Int32
	err   error
}

func (s *spyKeys) Key(ctx context.Context, kid string) (PublicKey, bool, error) {
	s.calls.Add(1)
	if s.err != nil {
		return PublicKey{}, false, s.err
	}
	return s.keys.Key(ctx, kid)
}

func newSpy(t testing.TB) *spyKeys {
	r, e := testKeys(t)
	rp, err := NewPublicKey(&r.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	ep, err := NewPublicKey(&e.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return &spyKeys{keys: Static{kidRSA: rp, kidEC: ep}}
}

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

// craft signs headerJSON.payloadJSON by hand, with no library, so a test
// can build a header or a payload the library would never write.
func craft(t testing.TB, key crypto.Signer, headerJSON, payloadJSON string) string {
	t.Helper()
	input := b64(headerJSON) + "." + b64(payloadJSON)
	digest := sha256.Sum256([]byte(input))
	var sig []byte
	switch k := key.(type) {
	case *rsa.PrivateKey:
		s, err := rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		sig = s
	case *ecdsa.PrivateKey:
		r, s, err := ecdsa.Sign(rand.Reader, k, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		sig = make([]byte, 64)
		r.FillBytes(sig[:32])
		s.FillBytes(sig[32:])
	default:
		t.Fatalf("craft: key %T", key)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// verifyIndependently checks a compact JWS with the standard library alone:
// the independent validator of a token this package signs.
func verifyIndependently(token string, pub crypto.PublicKey) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	switch k := pub.(type) {
	case *rsa.PublicKey:
		return rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], sig) == nil
	case *ecdsa.PublicKey:
		if len(sig) != 64 {
			return false
		}
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		return ecdsa.Verify(k, digest[:], r, s)
	}
	return false
}

func unix(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

// claimsWith is a valid payload for testPolicy at testNow, with members
// replaced or added (a value of "" drops the member).
func claimsWith(over map[string]string) string {
	base := map[string]string{
		"iss": `"` + testIssuer + `"`,
		"sub": `"user-1"`,
		"aud": `"` + testAudience + `"`,
		"iat": unix(testNow.Add(-time.Minute)),
		"exp": unix(testNow.Add(10 * time.Minute)),
	}
	for k, v := range over {
		base[k] = v
	}
	order := []string{"iss", "sub", "aud", "iat", "exp"}
	for k := range over {
		found := false
		for _, o := range order {
			found = found || o == k
		}
		if !found {
			order = append(order, k)
		}
	}
	var b strings.Builder
	b.WriteString("{")
	first := true
	for _, k := range order {
		v := base[k]
		if v == "" {
			continue
		}
		if !first {
			b.WriteString(",")
		}
		first = false
		b.WriteString(`"` + k + `":` + v)
	}
	b.WriteString("}")
	return b.String()
}

func claimsJSON() string { return claimsWith(nil) }
