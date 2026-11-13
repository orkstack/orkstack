package jwt

import (
	"context"
	"crypto"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/jsontext"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

func TestSigner_TokensVerifyIndependentlyAndThroughOurRules(t *testing.T) {
	r, e := testKeys(t)
	for _, key := range []crypto.Signer{r, e} {
		s, err := NewSigner(key)
		if err != nil {
			t.Fatal(err)
		}
		claims := map[string]any{
			"iss": testIssuer, "sub": "user-1", "aud": testAudience,
			"iat": testNow.Unix(), "exp": testNow.Add(5 * time.Minute).Unix(),
		}
		token, err := s.Sign("JWT", claims)
		if err != nil {
			t.Fatal(err)
		}
		// An independent validator: the standard library alone.
		if !verifyIndependently(token, key.Public()) {
			t.Fatalf("%s: a token we signed does not verify without the library", s.Algorithm())
		}
		header, _ := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[0])
		h, err := decodeObject(header)
		if err != nil {
			t.Fatal(err)
		}
		if string(h["kid"]) != `"`+s.KeyID()+`"` || string(h["alg"]) != `"`+string(s.Algorithm())+`"` {
			t.Fatalf("header %s", header)
		}
		tp, _ := s.Public().Thumbprint()
		if s.KeyID() != tp {
			t.Fatalf("kid %q is not the thumbprint %q", s.KeyID(), tp)
		}
		keys, err := StaticByThumbprint(s.Public())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Verify(context.Background(), token, testPolicy(), keys, testNow); err != nil {
			t.Fatalf("our own token: %v", err)
		}
	}
}

func TestVerify_ATokenTheLibrarySigns(t *testing.T) {
	_, e := testKeys(t)
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: e},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", kidEC))
	if err != nil {
		t.Fatal(err)
	}
	obj, err := signer.Sign([]byte(claimsJSON()))
	if err != nil {
		t.Fatal(err)
	}
	token, err := obj.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), token, testPolicy(), newSpy(t), testNow); err != nil {
		t.Fatalf("a token the library signs: %v", err)
	}
}

func TestSigner_Refuses(t *testing.T) {
	if _, err := NewSigner(nil); err == nil {
		t.Fatal("nil key accepted")
	}
	_, e := testKeys(t)
	s, _ := NewSigner(e)
	if _, err := s.Sign("JWT", []int{1}); err == nil {
		t.Fatal("non-object claims accepted")
	}
	if _, err := s.Sign("JWT", map[string]string{"pad": strings.Repeat("x", MaxTokenBytes)}); err == nil {
		t.Fatal("a token over the bound was signed")
	}
	if _, err := s.SignRequest(AssertionRequest{Account: "a", Audience: "https://ork", Method: "GET", URL: "https://ork/x"}, testNow, map[string]any{"htm": "POST"}); err == nil {
		t.Fatal("an extra claim replaced a binding claim")
	}
	if _, err := s.SignRequest(AssertionRequest{Account: "a", Audience: "https://ork", Method: "GET", URL: "/x"}, testNow, nil); err == nil {
		t.Fatal("a relative URL was signed")
	}
}

func TestSigner_RequestAssertionShape(t *testing.T) {
	_, e := testKeys(t)
	s, _ := NewSigner(e)
	token, err := s.SignRequest(AssertionRequest{
		Account: "ci", Audience: "https://ork.example", Method: "POST",
		URL: "https://ork.example/orkstack.v1.RunService/SubmitRun?x=1#f", Body: []byte(`{"a":1}`),
	}, testNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	header, _ := base64.RawURLEncoding.DecodeString(parts[0])
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	h, _ := decodeObject(header)
	c, _ := decodeObject(payload)
	if string(h["typ"]) != `"ork-request+jwt"` {
		t.Fatalf("typ %s", h["typ"])
	}
	sum := sha256.Sum256([]byte(`{"a":1}`))
	want := map[string]string{
		"iss": `"serviceaccount:ci"`, "sub": `"serviceaccount:ci"`, "htm": `"POST"`,
		"htu":             `"https://ork.example/orkstack.v1.RunService/SubmitRun"`,
		"ork_body_sha256": `"` + hexOf(sum[:]) + `"`,
	}
	for k, v := range want {
		if string(c[k]) != v {
			t.Errorf("%s = %s, want %s", k, c[k], v)
		}
	}
	var jti string
	_ = jsontextUnquote(c["jti"], &jti)
	if raw, err := base64.RawURLEncoding.DecodeString(jti); err != nil || len(raw) != 16 {
		t.Fatalf("jti %q is not 128 random bits", jti)
	}
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&15])
	}
	return string(out)
}

func jsontextUnquote(v jsontext.Value, out *string) error {
	s, err := jsontext.AppendUnquote(nil, v)
	*out = string(s)
	return err
}
