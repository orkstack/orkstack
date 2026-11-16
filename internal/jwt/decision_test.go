package jwt

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

type decisionKeys map[string]PublicKey

func (d decisionKeys) DecisionKey(kid string) (PublicKey, bool) { k, ok := d[kid]; return k, ok }

func decisionSigners(t *testing.T) (*Signer, *Signer, decisionKeys) {
	t.Helper()
	r, e := testKeys(t)
	rs, err := NewSigner(r)
	if err != nil {
		t.Fatal(err)
	}
	es, err := NewSigner(e)
	if err != nil {
		t.Fatal(err)
	}
	return rs, es, decisionKeys{rs.KeyID(): rs.Public(), es.KeyID(): es.Public()}
}

func payloadOf(kind DecisionKind, body []byte) string {
	return `{"v":1,"kind":"` + string(kind) + `","body":"` + base64.RawURLEncoding.EncodeToString(body) + `"}`
}

// TestDecision_SignAndVerify: a decision signed by either kind of key
// verifies as its kind, answers the body and the kid, and as any other
// kind is bad.
func TestDecision_SignAndVerify(t *testing.T) {
	rs, es, keys := decisionSigners(t)
	body := []byte{0, 1, 2, 'x', 0xff}
	for _, s := range []*Signer{rs, es} {
		msg, err := s.SignDecision(KindForceCancelRecord, body)
		if err != nil {
			t.Fatal(err)
		}
		got, kid, answer := VerifyDecision(msg, KindForceCancelRecord, keys)
		if answer != DecisionValid || kid != s.KeyID() || !bytes.Equal(got, body) {
			t.Fatalf("%s: %v %q %x", s.Algorithm(), answer, kid, got)
		}
		if _, _, answer := VerifyDecision(msg, KindForceCancelEvent, keys); answer != DecisionBad {
			t.Fatalf("%s: as another kind: %v", s.Algorithm(), answer)
		}
	}
	if _, err := rs.SignDecision("fc.made-up.v1", body); err == nil {
		t.Fatal("a kind outside the closed set was signed")
	}
	if len(DecisionKinds()) != 6 {
		t.Fatalf("kinds = %v", DecisionKinds())
	}
}

// TestDecision_TheVerifiersRules: each rule of authn-authz.md §6.6 holds.
// A failure under a key of the document is bad; one with no such key, or
// bytes that do not decode, is unknown.
func TestDecision_TheVerifiersRules(t *testing.T) {
	rs, es, keys := decisionSigners(t)
	r, e := testKeys(t)
	kind := KindRunEnding
	good := payloadOf(kind, []byte("b"))
	ekid, rkid := es.KeyID(), rs.KeyID()
	header := func(alg, kid string, extra string) string {
		return `{"alg":"` + alg + `","kid":"` + kid + `","typ":"ork-decision+jws"` + extra + `}`
	}
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	for _, tc := range []struct {
		name string
		msg  string
		want DecisionAnswer
	}{
		{"the header's alg is not the key's", craft(t, e, header("RS256", ekid, ""), good), DecisionBad},
		{"alg none", b64(header("none", ekid, "")) + "." + b64(good) + ".", DecisionBad},
		{"an extra header member", craft(t, e, header("ES256", ekid, `,"x5u":"https://e"`), good), DecisionBad},
		{"a jwk in the header", craft(t, e, header("ES256", ekid, `,"jwk":{}`), good), DecisionBad},
		{"b64 false", craft(t, e, header("ES256", ekid, `,"b64":false`), good), DecisionBad},
		{"the token type", craft(t, e, `{"alg":"ES256","kid":"`+ekid+`","typ":"JWT"}`, good), DecisionBad},
		{"no typ", craft(t, e, `{"alg":"ES256","kid":"`+ekid+`"}`, good), DecisionBad},
		{"another signer under the kid", craft(t, other, header("ES256", ekid, ""), good), DecisionBad},
		{"a payload not canonical", craft(t, e, header("ES256", ekid, ""), `{"kind":"`+string(kind)+`","v":1,"body":"Yg"}`), DecisionBad},
		{"a payload of another version", craft(t, e, header("ES256", ekid, ""), strings.Replace(good, `"v":1`, `"v":2`, 1)), DecisionBad},
		{"a body not base64url", craft(t, e, header("ES256", ekid, ""), strings.Replace(good, `"body":"Yg"`, `"body":"Y="`, 1)), DecisionBad},
		{"a token's claims", craft(t, r, header("RS256", rkid, ""), `{"iss":"https://i","sub":"s","aud":"a","exp":9999999999}`), DecisionBad},
		{"an unknown kid", craft(t, e, header("ES256", "nobody", ""), good), DecisionUnknown},
		{"no kid", craft(t, e, `{"alg":"ES256","typ":"ork-decision+jws"}`, good), DecisionUnknown},
		{"two parts", b64(header("ES256", ekid, "")) + "." + b64(good), DecisionUnknown},
		{"not base64", "!!.!!.!!", DecisionUnknown},
		{"too large", craft(t, e, header("ES256", ekid, ""), payloadOf(kind, bytes.Repeat([]byte("x"), MaxDecisionBytes))), DecisionUnknown},
	} {
		if _, _, got := VerifyDecision(tc.msg, kind, keys); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
	// An ES256 signature is exactly 64 bytes.
	msg, _ := es.SignDecision(kind, []byte("b"))
	parts := strings.Split(msg, ".")
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	long := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(append(sig, 0))
	if _, _, got := VerifyDecision(long, kind, keys); got != DecisionBad {
		t.Errorf("a 65-byte ES256 signature: %v", got)
	}
	// An RSA key under 2048 bits verifies nothing: it is never a PublicKey.
	small, _ := rsa.GenerateKey(rand.Reader, 1024)
	if _, err := NewPublicKey(&small.PublicKey); err == nil {
		t.Error("a 1024-bit RSA key was accepted as a public key")
	}
}

// TestDecision_IsNeverAToken: a signed decision is refused by every token
// verifier, and a token by the decision verifier.
func TestDecision_IsNeverAToken(t *testing.T) {
	_, es, keys := decisionSigners(t)
	msg, err := es.SignDecision(KindForceCancelRoot, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	p := Policy{Issuer: "https://i", Audiences: []string{"a"}, Algorithms: []Algorithm{ES256}}
	if _, err := Verify(context.Background(), msg, p, Static(keys), time.Now()); err == nil {
		t.Fatal("a decision verified as a token")
	}
	token, err := es.Sign("JWT", map[string]any{"iss": "https://i", "sub": "s", "aud": "a", "exp": time.Now().Add(time.Hour).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, answer := VerifyDecision(token, KindForceCancelRoot, keys); answer == DecisionValid {
		t.Fatal("a token verified as a decision")
	}
}

// TestDerive: the derived value is HMAC-SHA256 under an HKDF of the key's
// private scalar: stable for one key and label, different across keys and
// labels, and refused for a label outside the closed set.
func TestDerive(t *testing.T) {
	rs, es, _ := decisionSigners(t)
	for _, s := range []*Signer{rs, es} {
		a, err := s.Derive(DeriveForceCancelRequestID, []byte("run-1"))
		if err != nil || len(a) != 32 {
			t.Fatalf("%s: %x, %v", s.Algorithm(), a, err)
		}
		again, _ := s.Derive(DeriveForceCancelRequestID, []byte("run-1"))
		other, _ := s.Derive(DeriveResumeToken, []byte("run-1"))
		input, _ := s.Derive(DeriveForceCancelRequestID, []byte("run-2"))
		if !bytes.Equal(a, again) || bytes.Equal(a, other) || bytes.Equal(a, input) {
			t.Fatalf("%s: derive is not a function of its label and input", s.Algorithm())
		}
		if _, err := s.Derive("made-up.v1", nil); err == nil {
			t.Fatalf("%s: a label outside the closed set", s.Algorithm())
		}
	}
	ra, _ := rs.Derive(DeriveResumeToken, []byte("x"))
	ea, _ := es.Derive(DeriveResumeToken, []byte("x"))
	if bytes.Equal(ra, ea) {
		t.Fatal("two keys derived the same value")
	}
}
