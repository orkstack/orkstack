package jwt

import (
	"context"
	"encoding/json/jsontext"
	"slices"
	"strings"
	"testing"
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
