package jwt

import (
	"context"
	"net/url"
	"testing"
	"time"
)

const (
	testPublic = "https://ork.example"
	testPath   = "/orkstack.v1.RunService/SubmitRun"
)

type assertionFixture struct {
	signer *Signer
	keys   Static
	policy AssertionPolicy
}

func newAssertionFixture(t testing.TB) assertionFixture {
	t.Helper()
	_, e := testKeys(t)
	s, err := NewSigner(e)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := StaticByThumbprint(s.Public())
	if err != nil {
		t.Fatal(err)
	}
	return assertionFixture{signer: s, keys: keys, policy: AssertionPolicy{Account: "ci", Audience: testPublic, Leeway: 60 * time.Second}}
}

func (f assertionFixture) sign(t testing.TB, method, rawURL string, body []byte) string {
	t.Helper()
	token, err := f.signer.SignRequest(AssertionRequest{Account: "ci", Audience: testPublic, Method: method, URL: rawURL, Body: body}, testNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func request(method, path string, body []byte) Request {
	pub, _ := url.Parse(testPublic)
	return Request{
		Method:      method,
		Path:        path,
		BodySHA256:  BodySHA256(body),
		HostAllowed: func(h string) bool { return h == "ork.example" },
		PublicURL:   pub,
	}
}

func TestVerifyRequest_BoundToOneRequest(t *testing.T) {
	f := newAssertionFixture(t)
	body := []byte(`{"orkflow":"x"}`)
	token := f.sign(t, "POST", testPublic+testPath, body)

	// Refusals first, each against the token's one valid use, which then
	// succeeds: a refusal never spends the jti.
	for name, req := range map[string]Request{
		"another method": request("PUT", testPath, body),
		"another path":   request("POST", "/orkstack.v1.RunService/CancelRun", body),
		"another body":   request("POST", testPath, []byte(`{"orkflow":"y"}`)),
		"no body":        request("POST", testPath, nil),
	} {
		_, err := VerifyRequest(context.Background(), token, f.policy, f.keys, req, NewReplayCache(), testNow)
		if ReasonOf(err) != ReasonBinding {
			t.Errorf("%s: got %v", name, err)
		}
	}
	replay := NewReplayCache()
	c, err := VerifyRequest(context.Background(), token, f.policy, f.keys, request("POST", testPath, body), replay, testNow)
	if err != nil {
		t.Fatalf("first use refused: %v", err)
	}
	if c.Subject != "serviceaccount:ci" {
		t.Fatalf("sub %q", c.Subject)
	}
	_, err = VerifyRequest(context.Background(), token, f.policy, f.keys, request("POST", testPath, body), replay, testNow)
	if ReasonOf(err) != ReasonReplay {
		t.Fatalf("second use at the same replica: %v", err)
	}
	// Another replica's cache has not seen it: the stated residue.
	if _, err := VerifyRequest(context.Background(), token, f.policy, f.keys, request("POST", testPath, body), NewReplayCache(), testNow); err != nil {
		t.Fatalf("first use at another replica: %v", err)
	}
	// Never after exp and the leeway.
	late := testNow.Add(MaxAssertionLifetime + 61*time.Second)
	if _, err := VerifyRequest(context.Background(), token, f.policy, f.keys, request("POST", testPath, body), NewReplayCache(), late); ReasonOf(err) != ReasonExpired {
		t.Fatalf("after exp: %v", err)
	}
}

func TestVerifyRequest_Refuses(t *testing.T) {
	f := newAssertionFixture(t)
	_, e := testKeys(t)
	kid := f.signer.KeyID()
	req := request("GET", "/v1/x", nil)
	iat := unix(testNow)
	exp := unix(testNow.Add(MaxAssertionLifetime))
	emptySum := `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`
	bound := func(over map[string]string) string {
		base := map[string]string{
			"iss": `"serviceaccount:ci"`, "sub": `"serviceaccount:ci"`, "aud": `"` + testPublic + `"`,
			"iat": iat, "exp": exp, "jti": `"j1"`, "htm": `"GET"`, "htu": `"https://ork.example/v1/x"`,
			"ork_body_sha256": emptySum,
		}
		for k, v := range over {
			base[k] = v
		}
		return objectOf(base)
	}
	header := `{"alg":"ES256","kid":"` + kid + `","typ":"ork-request+jwt"}`
	for _, tc := range []struct {
		name, header, claims string
		want                 Reason
	}{
		{"accepted control", header, bound(nil), ""},
		{"revision 11's unbound shape", `{"alg":"ES256","kid":"` + kid + `"}`, bound(map[string]string{"htm": "", "htu": "", "ork_body_sha256": ""}), ReasonBinding},
		{"typ JWT", `{"alg":"ES256","kid":"` + kid + `","typ":"JWT"}`, bound(nil), ReasonBinding},
		{"no htm", header, bound(map[string]string{"htm": ""}), ReasonBinding},
		{"no htu", header, bound(map[string]string{"htu": ""}), ReasonBinding},
		{"no body hash", header, bound(map[string]string{"ork_body_sha256": ""}), ReasonBinding},
		{"no jti", header, bound(map[string]string{"jti": ""}), ReasonBinding},
		{"htu with a query", header, bound(map[string]string{"htu": `"https://ork.example/v1/x?a=1"`}), ReasonBinding},
		{"htu with an empty query", header, bound(map[string]string{"htu": `"https://ork.example/v1/x?"`}), ReasonBinding},
		{"htu with a fragment", header, bound(map[string]string{"htu": `"https://ork.example/v1/x#a"`}), ReasonBinding},
		{"htu relative", header, bound(map[string]string{"htu": `"/v1/x"`}), ReasonBinding},
		{"htu with userinfo", header, bound(map[string]string{"htu": `"https://u@ork.example/v1/x"`}), ReasonBinding},
		{"htu host refused by the policy", header, bound(map[string]string{"htu": `"https://evil.example/v1/x"`}), ReasonBinding},
		{"htu scheme not the public URL's", header, bound(map[string]string{"htu": `"http://ork.example/v1/x"`}), ReasonBinding},
		{"htu path differs in its escaping", header, bound(map[string]string{"htu": `"https://ork.example/v1/%78"`}), ReasonBinding},
		{"htm lower case", header, bound(map[string]string{"htm": `"get"`}), ReasonBinding},
		{"body hash upper case", header, bound(map[string]string{"ork_body_sha256": `"E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"`}), ReasonBinding},
		{"sub another account", header, bound(map[string]string{"sub": `"serviceaccount:other"`}), ReasonClaims},
		{"iss another account", header, bound(map[string]string{"iss": `"serviceaccount:other"`}), ReasonClaims},
		{"aud another deployment", header, bound(map[string]string{"aud": `"https://other.example"`}), ReasonClaims},
		{"exp more than a minute ahead", header, bound(map[string]string{"iat": unix(testNow.Add(time.Minute)), "exp": unix(testNow.Add(2*time.Minute + 30*time.Second))}), ReasonLifetime},
		{"lives more than a minute", header, bound(map[string]string{"iat": unix(testNow.Add(-time.Minute)), "exp": unix(testNow.Add(time.Second))}), ReasonLifetime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := craft(t, e, tc.header, tc.claims)
			_, err := VerifyRequest(context.Background(), token, f.policy, f.keys, req, NewReplayCache(), testNow)
			if ReasonOf(err) != tc.want || (tc.want == "" && err != nil) {
				t.Fatalf("got %v (%s), want %q", err, ReasonOf(err), tc.want)
			}
		})
	}
}

func TestVerifyRequest_MisconfiguredRefusesEverything(t *testing.T) {
	f := newAssertionFixture(t)
	token := f.sign(t, "GET", testPublic+"/v1/x", nil)
	req := request("GET", "/v1/x", nil)
	noHost := req
	noHost.HostAllowed = nil
	for name, call := range map[string]func() error{
		"no account": func() error {
			p := f.policy
			p.Account = ""
			_, err := VerifyRequest(context.Background(), token, p, f.keys, req, NewReplayCache(), testNow)
			return err
		},
		"no audience": func() error {
			p := f.policy
			p.Audience = ""
			_, err := VerifyRequest(context.Background(), token, p, f.keys, req, NewReplayCache(), testNow)
			return err
		},
		"no host policy": func() error {
			_, err := VerifyRequest(context.Background(), token, f.policy, f.keys, noHost, NewReplayCache(), testNow)
			return err
		},
		"no replay cache": func() error {
			_, err := VerifyRequest(context.Background(), token, f.policy, f.keys, req, nil, testNow)
			return err
		},
	} {
		if err := call(); ReasonOf(err) != ReasonUnavailable {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestReplayCache_BoundedAndClosedWhenFull(t *testing.T) {
	r := newReplayCache(2, 2)
	until := testNow.Add(time.Minute)
	if r.use("a", "1", until, testNow) != nil || r.use("a", "2", until, testNow) != nil {
		t.Fatal("first uses refused")
	}
	if err := r.use("b", "1", until, testNow); ReasonOf(err) != ReasonUnavailable {
		t.Fatalf("a full cache must refuse, not forget: %v", err)
	}
	if err := r.use("a", "1", until, testNow); ReasonOf(err) != ReasonReplay {
		t.Fatalf("replay: %v", err)
	}
	// Once the entries expire they are swept and room returns.
	later := until.Add(time.Second)
	if err := r.use("b", "1", later.Add(time.Minute), later); err != nil {
		t.Fatalf("after expiry: %v", err)
	}
	if r.Len() != 1 {
		t.Fatalf("len %d after the sweep", r.Len())
	}
	// The same jti under another issuer is another key.
	if err := r.use("c", "1", later.Add(time.Minute), later); err != nil {
		t.Fatalf("another issuer's jti: %v", err)
	}
}

func objectOf(m map[string]string) string {
	out := "{"
	first := true
	for k, v := range m {
		if v == "" {
			continue
		}
		if !first {
			out += ","
		}
		first = false
		out += `"` + k + `":` + v
	}
	return out + "}"
}

func TestReplayCache_OneAccountCannotCrowdOutAnother(t *testing.T) {
	r := newReplayCache(10, 3)
	until := testNow.Add(time.Minute)
	for i := range 3 {
		if err := r.use("serviceaccount:noisy", string(rune('a'+i)), until, testNow); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.use("serviceaccount:noisy", "d", until, testNow); ReasonOf(err) != ReasonUnavailable {
		t.Fatalf("past its share: %v", err)
	}
	if err := r.use("serviceaccount:quiet", "a", until, testNow); err != nil {
		t.Fatalf("another account was crowded out: %v", err)
	}
	// Its share frees as its entries expire.
	later := until.Add(time.Second)
	if err := r.use("serviceaccount:noisy", "d", later.Add(time.Minute), later); err != nil {
		t.Fatalf("after expiry: %v", err)
	}
}
