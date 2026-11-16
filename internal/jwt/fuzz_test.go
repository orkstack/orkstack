package jwt

import (
	"context"
	"encoding/json/jsontext"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestClaims_StringsRule(t *testing.T) {
	_, e := testKeys(t)
	token := craft(t, e, `{"alg":"ES256","kid":"ec-1"}`, claimsWith(map[string]string{
		"owner":   `1234567`,
		"owner_s": `"1234567"`,
		"long":    `1234567890123456`,
		"frac":    `1.5`,
		"exp10":   `1e3`,
		"neg":     `-5`,
		"flag":    `true`,
		"obj":     `{"kubernetes.io":{"pod":{"name":"orker-1"}}}`,
		"groups":  `["a","b"]`,
		"mixed":   `["a",1]`,
		"null":    `null`,
	}))
	c, err := Verify(context.Background(), token, testPolicy(), newSpy(t), testNow)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path []string
		want []string
		ok   bool
	}{
		{[]string{"owner"}, []string{"1234567"}, true},
		{[]string{"owner_s"}, []string{"1234567"}, true},
		{[]string{"long"}, nil, false},
		{[]string{"frac"}, nil, false},
		{[]string{"exp10"}, nil, false},
		{[]string{"neg"}, nil, false},
		{[]string{"flag"}, nil, false},
		{[]string{"obj"}, nil, false},
		{[]string{"obj", "kubernetes.io", "pod", "name"}, []string{"orker-1"}, true},
		{[]string{"obj", "kubernetes.io", "missing"}, nil, false},
		{[]string{"groups"}, []string{"a", "b"}, true},
		{[]string{"mixed"}, nil, false},
		{[]string{"null"}, nil, false},
		{[]string{"absent"}, nil, false},
	} {
		got, ok := c.Strings(tc.path...)
		if ok != tc.ok || !slices.Equal(got, tc.want) {
			t.Errorf("%v: got %v %v, want %v %v", tc.path, got, ok, tc.want, tc.ok)
		}
	}
	if !c.Matches("1234567", "owner") || !c.Matches("1234567", "owner_s") || c.Matches("1234567", "long") {
		t.Fatal("an integer owner id and its string form must be one requirement")
	}
	if v, ok := c.Bool("flag"); !ok || !v {
		t.Fatal("Bool")
	}
	if _, ok := c.Bool("owner"); ok {
		t.Fatal("a number read as a boolean")
	}
}

// FuzzVerify holds Verify's claims for any input: it never panics, a token
// over the bound is refused for its size, and anything accepted passed
// every header rule and names the configured issuer and an audience.
func FuzzVerify(f *testing.F) {
	_, e := testKeys(f)
	valid := craft(f, e, `{"alg":"ES256","kid":"ec-1"}`, claimsJSON())
	f.Add(valid)
	f.Add(rfcA2)
	f.Add(rfcA3)
	f.Add(craft(f, e, `{"alg":"ES256","kid":"ec-1","crit":["b64"]}`, claimsJSON()))
	f.Add(craft(f, e, `{"alg":"none","kid":"ec-1"}`, claimsJSON()))
	f.Add(craft(f, e, `{"alg":"ES256","kid":"ec-1"}`, `{"sub":"a","sub":"b"}`))
	f.Add("a.b.c")
	f.Add("..")
	f.Fuzz(func(t *testing.T, token string) {
		spy := newSpy(t)
		c, err := Verify(context.Background(), token, testPolicy(), spy, testNow)
		if len(token) > MaxTokenBytes && ReasonOf(err) != ReasonTooLarge {
			t.Fatalf("an oversized token was not refused for its size: %v", err)
		}
		if err != nil {
			if ReasonOf(err) == "" {
				t.Fatalf("a refusal without a reason: %v", err)
			}
			return
		}
		if c.Issuer != testIssuer || !slices.Contains(c.Audience, testAudience) || c.Subject == "" {
			t.Fatalf("accepted claims %+v", c)
		}
		headerJSON, _, rErr := split(token)
		if rErr != nil {
			t.Fatalf("accepted a token that does not split: %v", rErr)
		}
		if _, rErr := checkHeader(headerJSON, testPolicy().Algorithms, typeIssued); rErr != nil {
			t.Fatalf("accepted a token whose header fails the rules: %v", rErr)
		}
		if spy.calls.Load() != 1 {
			t.Fatalf("an accepted token asked for %d keys", spy.calls.Load())
		}
	})
}

// FuzzCheckHeader holds the header rules alone: an accepted header names an
// allowed algorithm and a kid and carries none of the refused parameters.
func FuzzCheckHeader(f *testing.F) {
	for _, seed := range []string{
		`{"alg":"ES256","kid":"k"}`,
		`{"alg":"RS256","kid":"k","typ":"JWT"}`,
		`{"alg":"ES256","kid":"k","jku":"x"}`,
		`{"alg":"ES256","alg":"RS256","kid":"k"}`,
		`{"alg":"HS256","kid":"k"}`,
		`{"alg":"ES256","kid":"k","typ":"application/ork-request+jwt"}`,
		`[]`, `null`, `{}`,
	} {
		f.Add([]byte(seed), uint8(0))
	}
	f.Fuzz(func(t *testing.T, raw []byte, rule uint8) {
		r := typeRule(rule % 3)
		h, err := checkHeader(raw, []Algorithm{ES256}, r)
		if err != nil {
			return
		}
		if h.alg != ES256 || h.kid == "" || len(h.kid) > maxKidBytes {
			t.Fatalf("accepted header %+v", h)
		}
		members, dErr := decodeObject(raw)
		if dErr != nil {
			t.Fatalf("accepted a header that is not one object: %v", dErr)
		}
		for name := range members {
			if slices.Contains(forbiddenHeaders, strings.ToLower(name)) {
				t.Fatalf("accepted a header carrying %q", name)
			}
		}
		if r == typeRequestOnly && h.typ != TypeRequest {
			t.Fatalf("a request verifier took typ %q", h.typ)
		}
		if r != typeRequestOnly && h.typ == TypeRequest {
			t.Fatal("another verifier took a request assertion")
		}
		if r == typeIDToken && h.typ == typeAccessToken {
			t.Fatal("an ID token verifier took an access token")
		}
	})
}

// FuzzVerifyRequest: a bound assertion is accepted only for the request it
// names, and at most once by one cache.
func FuzzVerifyRequest(f *testing.F) {
	fx := newAssertionFixture(f)
	f.Add(fx.sign(f, "GET", testPublic+"/v1/x", nil), "GET", "/v1/x", "")
	f.Add(fx.sign(f, "POST", testPublic+testPath, []byte("{}")), "POST", testPath, "{}")
	f.Add("a.b.c", "GET", "/", "")
	f.Fuzz(func(t *testing.T, token, method, path, body string) {
		req := request(method, path, []byte(body))
		replay := NewReplayCache()
		c, err := VerifyRequest(context.Background(), token, fx.policy, fx.keys, req, replay, testNow)
		if err != nil {
			return
		}
		if htm, _ := c.String("htm"); htm != method {
			t.Fatalf("accepted htm %q for method %q", htm, method)
		}
		if htu, _ := c.String("htu"); !strings.HasSuffix(htu, path) {
			t.Fatalf("accepted htu %q for path %q", htu, path)
		}
		if _, err := VerifyRequest(context.Background(), token, fx.policy, fx.keys, req, replay, testNow); ReasonOf(err) != ReasonReplay {
			t.Fatalf("a second use at one replica: %v", err)
		}
	})
}

// FuzzParseKeySet: never panics, keeps at most MaxKeySetKeys keys, and every
// key it keeps is one this package verifies with.
func FuzzParseKeySet(f *testing.F) {
	f.Add([]byte(rfcA2Key))
	f.Add([]byte(rfcA3Key))
	f.Add([]byte(rfc7638Key))
	f.Add([]byte(`{"keys":[{"kty":"oct","k":"AA"}]}`))
	f.Add([]byte(`{"keys":[]}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		set, err := ParseKeySet(raw)
		if err != nil {
			return
		}
		if set.Len() > MaxKeySetKeys {
			t.Fatalf("kept %d keys", set.Len())
		}
		for kid, k := range set.keys {
			if kid == "" || checkPublic(k.key) != nil {
				t.Fatalf("kept unusable key %q", kid)
			}
		}
	})
}

// FuzzMatchStrings holds the claim-matching rule: an integer matches only
// as its plain decimal digits, and nothing but a string, an array of
// strings or such an integer ever matches.
func FuzzMatchStrings(f *testing.F) {
	for _, seed := range []string{`"a"`, `["a","b"]`, `1234567`, `1.5`, `-1`, `1e3`, `true`, `{}`, `null`, `012`, `["a",1]`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		v := jsontext.Value(raw)
		if !v.IsValid() {
			return
		}
		values, ok := matchStrings(v)
		if !ok {
			return
		}
		switch v.Kind() {
		case '0':
			if len(values) != 1 || strings.Trim(values[0], "0123456789") != "" || len(values[0]) > maxIntegerDigits {
				t.Fatalf("number %s matched as %v", raw, values)
			}
		case '"', '[':
		default:
			t.Fatalf("a %c value matched", v.Kind())
		}
	})
}

// FuzzParsePublicKeyPEM: a client-supplied PEM never panics the parser, and
// an accepted key is one checkPublic admits (RSA of at least MinRSABits, or
// P-256) with a thumbprint.
func FuzzParsePublicKeyPEM(f *testing.F) {
	f.Add([]byte("-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE\n-----END PUBLIC KEY-----\n"))
	f.Add([]byte("not pem"))
	f.Add([]byte("-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		key, err := ParsePublicKeyPEM(raw)
		if err != nil {
			return
		}
		if err := checkPublic(key.Public()); err != nil {
			t.Fatalf("an accepted key fails checkPublic: %v", err)
		}
		if _, err := key.Thumbprint(); err != nil {
			t.Fatalf("an accepted key has no thumbprint: %v", err)
		}
	})
}

// FuzzParsePrivateKeyPEM: a key file read is a key NewSigner accepts, with
// its thumbprint as its kid.
func FuzzParsePrivateKeyPEM(f *testing.F) {
	f.Add([]byte("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"))
	f.Add([]byte("-----BEGIN EC PRIVATE KEY-----\nMHcCAQEE\n-----END EC PRIVATE KEY-----\n"))
	f.Add([]byte("-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n"))
	f.Add([]byte("not pem"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		s, err := ParsePrivateKeyPEM(raw)
		if err != nil {
			return
		}
		if err := checkPublic(s.Public().Public()); err != nil {
			t.Fatalf("an accepted key fails checkPublic: %v", err)
		}
		if kid, err := s.Public().Thumbprint(); err != nil || kid != s.KeyID() {
			t.Fatalf("an accepted key's kid %q is not its thumbprint %q (%v)", s.KeyID(), kid, err)
		}
	})
}

// FuzzParsePublicJWK: a stored key read is a public key this package
// accepts, published under its own thumbprint, and written again it reads
// back as the same key.
func FuzzParsePublicJWK(f *testing.F) {
	f.Add([]byte(`{"kty":"EC","crv":"P-256","x":"AAAA","y":"AAAA","kid":"k"}`))
	f.Add([]byte(`{"kty":"RSA","n":"AQAB","e":"AQAB","kid":"k","d":"AAAA"}`))
	f.Add([]byte(`{"kty":"oct","k":"AAAA"}`))
	f.Add([]byte(`[]`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		key, err := ParsePublicJWK(raw)
		if err != nil {
			return
		}
		if err := checkPublic(key.Public()); err != nil {
			t.Fatalf("an accepted key fails checkPublic: %v", err)
		}
		kid, err := key.Thumbprint()
		if err != nil || kid != key.KeyID() {
			t.Fatalf("an accepted key's kid %q is not its thumbprint %q (%v)", key.KeyID(), kid, err)
		}
		again, err := key.MarshalJWK(kid)
		if err != nil {
			t.Fatal(err)
		}
		back, err := ParsePublicJWK(again)
		if err != nil || back.KeyID() != kid {
			t.Fatalf("an accepted key written again reads back as %q, %v", back.KeyID(), err)
		}
	})
}

// FuzzDecision_Verify: whatever the bytes, verifying never panics; no
// string passes both the decision verifier and the token verifier; a valid
// decision's header is alg, kid and typ alone and its kid a key of the
// document; and a valid decision with one bit changed is never valid.
func FuzzDecision_Verify(f *testing.F) {
	r, e := testKeys(f)
	rs, _ := NewSigner(r)
	es, _ := NewSigner(e)
	keys := decisionKeys{rs.KeyID(): rs.Public(), es.KeyID(): es.Public()}
	for _, s := range []*Signer{rs, es} {
		for _, kind := range DecisionKinds() {
			msg, err := s.SignDecision(kind, []byte("seed body"))
			if err != nil {
				f.Fatal(err)
			}
			f.Add(msg, uint16(0), byte(0))
		}
	}
	f.Add(craft(f, e, `{"alg":"ES256","kid":"`+es.KeyID()+`","typ":"ork-decision+jws","jwk":{}}`, payloadOf(KindRunGap, []byte("x"))), uint16(3), byte(1))
	f.Add(craft(f, e, `{"alg":"ES256","kid":"`+es.KeyID()+`","typ":"JWT"}`, `{"iss":"https://i","sub":"s","aud":"a","exp":9999999999}`), uint16(9), byte(4))
	f.Add("a.b.c", uint16(1), byte(2))
	policy := Policy{Issuer: "https://i", Audiences: []string{"a"}, Algorithms: []Algorithm{RS256, ES256}}
	f.Fuzz(func(t *testing.T, msg string, at uint16, flip byte) {
		valid := 0
		for _, kind := range DecisionKinds() {
			if _, _, a := VerifyDecision(msg, kind, keys); a == DecisionValid {
				valid++
			}
		}
		if valid > 1 {
			t.Fatalf("one message verified as %d kinds", valid)
		}
		for _, kind := range DecisionKinds() {
			_, kid, answer := VerifyDecision(msg, kind, keys)
			if answer != DecisionValid {
				continue
			}
			if _, ok := keys[kid]; !ok {
				t.Fatalf("valid under kid %q, which is not a key of the document", kid)
			}
			if _, err := Verify(context.Background(), msg, policy, Static(keys), time.Unix(1, 0)); err == nil {
				t.Fatal("one string passed both the decision verifier and the token verifier")
			}
			headerJSON, _, _ := split(msg)
			members, err := decodeObject(headerJSON)
			if err != nil || len(members) != 3 {
				t.Fatalf("a valid decision's header has %d members", len(members))
			}
			if flip != 0 && len(msg) > 0 {
				b := []byte(msg)
				b[int(at)%len(b)] ^= flip
				if mutated := string(b); mutated != msg {
					if _, _, a := VerifyDecision(mutated, kind, keys); a == DecisionValid {
						t.Fatalf("a change of one byte at %d verified", int(at)%len(b))
					}
				}
			}
		}
	})
}
