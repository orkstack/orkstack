package jwt

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

// RFC 7515 appendix A.2 (RS256) and A.3 (ES256): the token and the public
// key of each, copied from the RFC.
const (
	rfcPayload = "eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ"
	rfcA2      = "eyJhbGciOiJSUzI1NiJ9." + rfcPayload + ".cC4hiUPoj9Eetdgtv3hF80EGrhuB__dzERat0XF9g2VtQgr9PJbu3XOiZj5RZmh7AAuHIm4Bh-0Qc_lF5YKt_O8W2Fp5jujGbds9uJdbF9CUAr7t1dnZcAcQjbKBYNX4BAynRFdiuB--f_nZLgrnbyTyWzO75vRK5h6xBArLIARNPvkSjtQBMHlb1L07Qe7K0GarZRmB_eSN9383LcOLn6_dO--xi12jzDwusC-eOkHWEsqtFZESc6BfI7noOPqvhJ1phCnvWh6IeYI2w9QOYEUipUTI8np6LbgGY9Fs98rqVt5AXLIhWkWywlVmtVrBp0igcN_IoypGlUPQGe77Rw"
	rfcA2Key   = `{"keys":[{"kty":"RSA","kid":"a2","n":"ofgWCuLjybRlzo0tZWJjNiuSfb4p4fAkd_wWJcyQoTbji9k0l8W26mPddxHmfHQp-Vaw-4qPCJrcS2mJPMEzP1Pt0Bm4d4QlL-yRT-SFd2lZS-pCgNMsD1W_YpRPEwOWvG6b32690r2jZ47soMZo9wGzjb_7OMg0LOL-bSf63kpaSHSXndS5z5rexMdbBYUsLA9e-KXBdQOS-UTo7WTBEMa2R2CapHg665xsmtdVMTBQY4uDZlxvb3qCo5ZwKh9kG4LT6_I5IhlJH7aGhyxXFvUK-DWNmoudF8NAco9_h9iaGNj8q2ethFkMLs91kzk2PAcDTW9gb54h4FRWyuXpoQ","e":"AQAB"}]}`
	rfcA3      = "eyJhbGciOiJFUzI1NiJ9." + rfcPayload + ".DtEhU3ljbEg8L38VWAfUAqOyKAM6-Xx-F4GawxaepmXFCgfTjDxw5djxLa8ISlSApmWQxfKTUJqPP3-Kg6NU1Q"
	rfcA3Key   = `{"keys":[{"kty":"EC","kid":"a3","crv":"P-256","x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU","y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0"}]}`
)

func TestJWT_RFC7515VectorsThroughTheLibrary(t *testing.T) {
	for _, tc := range []struct {
		name, token, set, kid string
		alg                   jose.SignatureAlgorithm
	}{
		{"A.2 RS256", rfcA2, rfcA2Key, "a2", jose.RS256},
		{"A.3 ES256", rfcA3, rfcA3Key, "a3", jose.ES256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set, err := ParseKeySet([]byte(tc.set))
			if err != nil {
				t.Fatal(err)
			}
			key, ok := set.Lookup(tc.kid)
			if !ok {
				t.Fatal("RFC key not kept")
			}
			jws, err := jose.ParseSignedCompact(tc.token, []jose.SignatureAlgorithm{tc.alg})
			if err != nil {
				t.Fatal(err)
			}
			payload, err := jws.Verify(key.Public())
			if err != nil {
				t.Fatalf("the pinned library does not verify the RFC's vector: %v", err)
			}
			if !strings.Contains(string(payload), `"iss":"joe"`) {
				t.Fatalf("payload %q", payload)
			}
			// Our rules refuse the vector before any key is asked for: it
			// has no kid.
			spy := &spyKeys{keys: StaticFromSet(set)}
			p := testPolicy()
			p.Issuer = "joe"
			_, err = Verify(context.Background(), tc.token, p, spy, time.Unix(1300819000, 0))
			if ReasonOf(err) != ReasonHeader || spy.calls.Load() != 0 {
				t.Fatalf("want a header refusal before any key; got %v after %d key calls", err, spy.calls.Load())
			}
		})
	}
}

func TestVerify_AcceptsWhatTheRulesAllow(t *testing.T) {
	r, e := testKeys(t)
	for _, tc := range []struct {
		name   string
		header string
		key    crypto.Signer
		claims string
	}{
		{"RS256", `{"alg":"RS256","kid":"rsa-1"}`, r, claimsJSON()},
		{"ES256", `{"alg":"ES256","kid":"ec-1","typ":"JWT"}`, e, claimsJSON()},
		{"at+jwt is a bearer token", `{"alg":"ES256","kid":"ec-1","typ":"at+jwt"}`, e, claimsJSON()},
		{"aud array holding one configured", `{"alg":"ES256","kid":"ec-1"}`, e, claimsWith(map[string]string{"aud": `["other","orkstack"]`})},
		{"expired within leeway", `{"alg":"ES256","kid":"ec-1"}`, e, claimsWith(map[string]string{"exp": unix(testNow.Add(-30 * time.Second))})},
		{"nbf within leeway", `{"alg":"ES256","kid":"ec-1"}`, e, claimsWith(map[string]string{"nbf": unix(testNow.Add(30 * time.Second))})},
		{"fractional dates", `{"alg":"ES256","kid":"ec-1"}`, e, claimsWith(map[string]string{"iat": unix(testNow) + ".5"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := craft(t, tc.key, tc.header, tc.claims)
			c, err := Verify(context.Background(), token, testPolicy(), newSpy(t), testNow)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if c.Subject != "user-1" || c.Issuer != testIssuer {
				t.Fatalf("claims %+v", c)
			}
		})
	}
}

func TestVerify_RefusesBeforeTheLibrary(t *testing.T) {
	r, e := testKeys(t)
	valid := claimsJSON()
	es := func(header string) string { return craft(t, e, header, valid) }
	good := es(`{"alg":"ES256","kid":"ec-1"}`)
	parts := strings.Split(good, ".")
	for _, tc := range []struct {
		name  string
		token string
		want  Reason
	}{
		{"too large", good + strings.Repeat("A", MaxTokenBytes), ReasonTooLarge},
		{"two segments", parts[0] + "." + parts[1], ReasonMalformed},
		{"four segments", good + ".AA", ReasonMalformed},
		{"padding", parts[0] + "=." + parts[1] + "." + parts[2], ReasonMalformed},
		{"standard base64", strings.Replace(good, "e", "+", 1), ReasonMalformed},
		{"empty signature", parts[0] + "." + parts[1] + ".", ReasonMalformed},
		{"header not an object", b64(`["alg"]`) + "." + parts[1] + "." + parts[2], ReasonMalformed},
		{"header not JSON", b64(`{alg`) + "." + parts[1] + "." + parts[2], ReasonMalformed},
		{"duplicate alg", es(`{"alg":"ES256","alg":"RS256","kid":"ec-1"}`), ReasonMalformed},
		{"alg none", es(`{"alg":"none","kid":"ec-1"}`), ReasonAlgorithm},
		{"alg HS256", es(`{"alg":"HS256","kid":"ec-1"}`), ReasonAlgorithm},
		{"alg HS384", es(`{"alg":"HS384","kid":"ec-1"}`), ReasonAlgorithm},
		{"alg HS512", es(`{"alg":"HS512","kid":"ec-1"}`), ReasonAlgorithm},
		{"alg PS256", es(`{"alg":"PS256","kid":"ec-1"}`), ReasonAlgorithm},
		{"alg lower case", es(`{"alg":"es256","kid":"ec-1"}`), ReasonAlgorithm},
		{"alg missing", es(`{"kid":"ec-1"}`), ReasonAlgorithm},
		{"alg a number", es(`{"alg":256,"kid":"ec-1"}`), ReasonAlgorithm},
		{"kid missing", es(`{"alg":"ES256"}`), ReasonHeader},
		{"kid empty", es(`{"alg":"ES256","kid":""}`), ReasonHeader},
		{"kid a number", es(`{"alg":"ES256","kid":1}`), ReasonHeader},
		{"kid too long", es(`{"alg":"ES256","kid":"` + strings.Repeat("k", maxKidBytes+1) + `"}`), ReasonHeader},
		{"crit", es(`{"alg":"ES256","kid":"ec-1","crit":["exp"],"exp":1}`), ReasonHeader},
		{"jku", es(`{"alg":"ES256","kid":"ec-1","jku":"https://evil.example/jwks"}`), ReasonHeader},
		{"x5u", es(`{"alg":"ES256","kid":"ec-1","x5u":"https://evil.example/cert"}`), ReasonHeader},
		{"jwk", es(`{"alg":"ES256","kid":"ec-1","jwk":{"kty":"EC"}}`), ReasonHeader},
		{"x5c", es(`{"alg":"ES256","kid":"ec-1","x5c":["MII"]}`), ReasonHeader},
		{"typ of a bound assertion", es(`{"alg":"ES256","kid":"ec-1","typ":"ork-request+jwt"}`), ReasonHeader},
		{"typ of a bound assertion, long form", es(`{"alg":"ES256","kid":"ec-1","typ":"application/ORK-REQUEST+JWT"}`), ReasonHeader},
		{"CRIT in upper case", es(`{"alg":"ES256","kid":"ec-1","CRIT":["exp"]}`), ReasonHeader},
		{"Jku in mixed case", es(`{"alg":"ES256","kid":"ec-1","Jku":"https://evil.example/jwks"}`), ReasonHeader},
		{"JWK in upper case", es(`{"alg":"ES256","kid":"ec-1","JWK":{"kty":"EC"}}`), ReasonHeader},
		{"X5C in upper case", es(`{"alg":"ES256","kid":"ec-1","X5C":["MII"]}`), ReasonHeader},
		{"TYP of a bound assertion", es(`{"alg":"ES256","kid":"ec-1","TYP":"ork-request+jwt"}`), ReasonHeader},
		{"ALG beside alg", es(`{"alg":"ES256","ALG":"none","kid":"ec-1"}`), ReasonHeader},
		{"typ a number", es(`{"alg":"ES256","kid":"ec-1","typ":1}`), ReasonHeader},
		{"payload not JSON", parts[0] + "." + b64("not json") + "." + parts[2], ReasonMalformed},
		{"payload an array", parts[0] + "." + b64(`[1]`) + "." + parts[2], ReasonMalformed},
		{"duplicate claim", craft(t, e, `{"alg":"ES256","kid":"ec-1"}`, `{"sub":"a","sub":"b"}`), ReasonMalformed},
		{"payload invalid UTF-8", parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte("{\"sub\":\"\xff\"}")) + "." + parts[2], ReasonMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := newSpy(t)
			_ = r
			_, err := Verify(context.Background(), tc.token, testPolicy(), spy, testNow)
			if ReasonOf(err) != tc.want {
				t.Fatalf("got %v (%s), want %s", err, ReasonOf(err), tc.want)
			}
			if n := spy.calls.Load(); n != 0 {
				t.Fatalf("the key source was asked %d times; a refusal before the library asks for none", n)
			}
		})
	}
}

func TestVerify_TheIssuersAlgorithmListIsEnforcedTwice(t *testing.T) {
	_, e := testKeys(t)
	token := craft(t, e, `{"alg":"ES256","kid":"ec-1"}`, claimsJSON())
	p := testPolicy()
	p.Algorithms = []Algorithm{RS256}

	t.Run("by our check, before any key", func(t *testing.T) {
		spy := newSpy(t)
		_, err := Verify(context.Background(), token, p, spy, testNow)
		if ReasonOf(err) != ReasonAlgorithm || spy.calls.Load() != 0 {
			t.Fatalf("got %v after %d key calls", err, spy.calls.Load())
		}
	})
	t.Run("by the library's own parse", func(t *testing.T) {
		ownAlgorithmCheck = false
		t.Cleanup(func() { ownAlgorithmCheck = true })
		_, err := Verify(context.Background(), token, p, newSpy(t), testNow)
		var unexpected *jose.ErrUnexpectedSignatureAlgorithm
		if ReasonOf(err) != ReasonAlgorithm || !errors.As(err, &unexpected) {
			t.Fatalf("with our check bypassed the library must refuse ES256: got %v", err)
		}
	})
}

func TestVerify_KeyTypeMustFitTheAlgorithm(t *testing.T) {
	r, e := testKeys(t)
	spy := newSpy(t)
	// An RS256 header naming the EC key's kid: the key does not fit.
	token := craft(t, r, `{"alg":"RS256","kid":"ec-1"}`, claimsJSON())
	if _, err := Verify(context.Background(), token, testPolicy(), spy, testNow); ReasonOf(err) != ReasonKey {
		t.Fatalf("RS256 with an EC key: got %v", err)
	}
	token = craft(t, e, `{"alg":"ES256","kid":"rsa-1"}`, claimsJSON())
	if _, err := Verify(context.Background(), token, testPolicy(), spy, testNow); ReasonOf(err) != ReasonKey {
		t.Fatalf("ES256 with an RSA key: got %v", err)
	}

	small, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPublicKey(&small.PublicKey); err == nil {
		t.Fatal("a 1024-bit RSA key was accepted")
	}
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPublicKey(&p384.PublicKey); err == nil {
		t.Fatal("a P-384 key was accepted")
	}
}

func TestVerify_SignatureAndKeySource(t *testing.T) {
	_, e := testKeys(t)
	token := craft(t, e, `{"alg":"ES256","kid":"ec-1"}`, claimsJSON())
	parts := strings.Split(token, ".")

	tampered := parts[0] + "." + b64(claimsWith(map[string]string{"sub": `"admin"`})) + "." + parts[2]
	if _, err := Verify(context.Background(), tampered, testPolicy(), newSpy(t), testNow); ReasonOf(err) != ReasonSignature {
		t.Fatalf("tampered payload: %v", err)
	}
	other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	forged := craft(t, other, `{"alg":"ES256","kid":"ec-1"}`, claimsJSON())
	if _, err := Verify(context.Background(), forged, testPolicy(), newSpy(t), testNow); ReasonOf(err) != ReasonSignature {
		t.Fatalf("another key's signature: %v", err)
	}
	unknown := craft(t, e, `{"alg":"ES256","kid":"nobody"}`, claimsJSON())
	if _, err := Verify(context.Background(), unknown, testPolicy(), newSpy(t), testNow); ReasonOf(err) != ReasonKey {
		t.Fatalf("unknown kid: %v", err)
	}
	down := newSpy(t)
	down.err = refuse(ReasonUnavailable, "issuer unreachable")
	if _, err := Verify(context.Background(), token, testPolicy(), down, testNow); ReasonOf(err) != ReasonUnavailable {
		t.Fatalf("keys unavailable: %v", err)
	}
	if _, err := Verify(context.Background(), token, testPolicy(), nil, testNow); ReasonOf(err) != ReasonUnavailable {
		t.Fatalf("no key source: %v", err)
	}
}

func TestVerify_Claims(t *testing.T) {
	_, e := testKeys(t)
	for _, tc := range []struct {
		name string
		over map[string]string
		want Reason
	}{
		{"iss another", map[string]string{"iss": `"https://issuer.example/"`}, ReasonClaims},
		{"iss missing", map[string]string{"iss": ""}, ReasonClaims},
		{"iss a number", map[string]string{"iss": `1`}, ReasonClaims},
		{"sub missing", map[string]string{"sub": ""}, ReasonClaims},
		{"sub empty", map[string]string{"sub": `""`}, ReasonClaims},
		{"sub a number", map[string]string{"sub": `7`}, ReasonClaims},
		{"aud missing", map[string]string{"aud": ""}, ReasonClaims},
		{"aud another", map[string]string{"aud": `"other"`}, ReasonClaims},
		{"aud empty array", map[string]string{"aud": `[]`}, ReasonClaims},
		{"aud array without ours", map[string]string{"aud": `["a","b"]`}, ReasonClaims},
		{"aud array holding a number", map[string]string{"aud": `["orkstack",1]`}, ReasonClaims},
		{"exp missing", map[string]string{"exp": ""}, ReasonClaims},
		{"exp a string", map[string]string{"exp": `"1"`}, ReasonClaims},
		{"exp negative", map[string]string{"exp": `-1`}, ReasonClaims},
		{"exp past year 9999", map[string]string{"exp": `1e300`}, ReasonClaims},
		{"iat missing", map[string]string{"iat": ""}, ReasonClaims},
		{"expired past leeway", map[string]string{"exp": unix(testNow.Add(-61 * time.Second))}, ReasonExpired},
		{"nbf past leeway", map[string]string{"nbf": unix(testNow.Add(2 * time.Minute))}, ReasonNotYetValid},
		{"iat in the future", map[string]string{"iat": unix(testNow.Add(2 * time.Minute)), "exp": unix(testNow.Add(10 * time.Minute))}, ReasonNotYetValid},
		{"exp before iat", map[string]string{"iat": unix(testNow), "exp": unix(testNow.Add(-time.Second))}, ReasonClaims},
		{"exp equal to iat", map[string]string{"iat": unix(testNow.Add(time.Minute)), "exp": unix(testNow.Add(time.Minute))}, ReasonClaims},
		{"lives longer than the issuer allows", map[string]string{"iat": unix(testNow.Add(-time.Minute)), "exp": unix(testNow.Add(time.Hour))}, ReasonLifetime},
		{"jti a number", map[string]string{"jti": `5`}, ReasonClaims},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := craft(t, e, `{"alg":"ES256","kid":"ec-1"}`, claimsWith(tc.over))
			_, err := Verify(context.Background(), token, testPolicy(), newSpy(t), testNow)
			if ReasonOf(err) != tc.want {
				t.Fatalf("got %v (%s), want %s", err, ReasonOf(err), tc.want)
			}
		})
	}
}

func TestVerifyIDToken(t *testing.T) {
	_, e := testKeys(t)
	const client = "orkstack-web"
	header := `{"alg":"ES256","kid":"ec-1"}`
	for _, tc := range []struct {
		name   string
		header string
		over   map[string]string
		nonce  string
		want   Reason
	}{
		{"aud exactly the client", header, map[string]string{"aud": `"orkstack-web"`, "nonce": `"n1"`}, "n1", ""},
		{"aud array of the client alone", header, map[string]string{"aud": `["orkstack-web"]`, "nonce": `"n1"`}, "n1", ""},
		{"several, azp the client", header, map[string]string{"aud": `["orkstack-web","api"]`, "azp": `"orkstack-web"`, "nonce": `"n1"`}, "n1", ""},
		{"several, no azp", header, map[string]string{"aud": `["orkstack-web","api"]`, "nonce": `"n1"`}, "n1", ReasonClaims},
		{"several, azp another", header, map[string]string{"aud": `["orkstack-web","api"]`, "azp": `"api"`, "nonce": `"n1"`}, "n1", ReasonClaims},
		{"aud another client", header, map[string]string{"aud": `"orkstack"`, "nonce": `"n1"`}, "n1", ReasonClaims},
		{"nonce differs", header, map[string]string{"aud": `"orkstack-web"`, "nonce": `"n2"`}, "n1", ReasonClaims},
		{"nonce missing", header, map[string]string{"aud": `"orkstack-web"`}, "n1", ReasonClaims},
		{"an access token", `{"alg":"ES256","kid":"ec-1","typ":"at+jwt"}`, map[string]string{"aud": `"orkstack-web"`, "nonce": `"n1"`}, "n1", ReasonHeader},
		{"an access token, long form", `{"alg":"ES256","kid":"ec-1","typ":"application/at+jwt"}`, map[string]string{"aud": `"orkstack-web"`, "nonce": `"n1"`}, "n1", ReasonHeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := craft(t, e, tc.header, claimsWith(tc.over))
			_, err := VerifyIDToken(context.Background(), token, testPolicy(), newSpy(t), testNow, client, tc.nonce)
			if ReasonOf(err) != tc.want || (tc.want == "" && err != nil) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestPolicy_Validate(t *testing.T) {
	if err := testPolicy().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Policy){
		"no issuer":        func(p *Policy) { p.Issuer = "" },
		"no audience":      func(p *Policy) { p.Audiences = nil },
		"empty audience":   func(p *Policy) { p.Audiences = []string{""} },
		"no algorithm":     func(p *Policy) { p.Algorithms = nil },
		"HS256":            func(p *Policy) { p.Algorithms = []Algorithm{"HS256"} },
		"none":             func(p *Policy) { p.Algorithms = []Algorithm{"none"} },
		"no lifetime":      func(p *Policy) { p.MaxLifetime = 0 },
		"negative leeway":  func(p *Policy) { p.Leeway = -time.Second },
		"leeway too large": func(p *Policy) { p.Leeway = time.Hour },
	} {
		p := testPolicy()
		mutate(&p)
		if p.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
		// A misconfigured policy refuses every token.
		_, e := testKeys(t)
		token := craft(t, e, `{"alg":"ES256","kid":"ec-1"}`, claimsJSON())
		if _, err := Verify(context.Background(), token, p, newSpy(t), testNow); err == nil {
			t.Errorf("%s: a token passed a misconfigured policy", name)
		}
	}
	for _, name := range []string{"none", "HS256", "HS384", "HS512", "RS384", "ES384", "PS256", "EdDSA", ""} {
		if _, err := ParseAlgorithm(name); err == nil {
			t.Errorf("ParseAlgorithm(%q) accepted", name)
		}
	}
}

func TestPeekIssuer(t *testing.T) {
	_, e := testKeys(t)
	token := craft(t, e, `{"alg":"ES256","kid":"ec-1"}`, claimsJSON())
	iss, err := PeekIssuer(token)
	if err != nil || iss != testIssuer {
		t.Fatalf("got %q, %v", iss, err)
	}
	if _, err := PeekIssuer("a.b"); err == nil {
		t.Fatal("a malformed token was peeked")
	}
	if _, err := PeekIssuer(craft(t, e, `{"alg":"ES256"}`, `{"iss":1}`)); err == nil {
		t.Fatal("a numeric iss was peeked")
	}
}
