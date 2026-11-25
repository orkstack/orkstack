package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/sts/types"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/testsupport/mototest"
)

// The servers of this file are fakes for faults alone: an answer STS, SSM,
// Secrets Manager or KMS gives when something is wrong, which Moto cannot be
// made to give on demand. What they answer when nothing is wrong is tested
// against Moto (TestIntegration_AWSProvider_*).

func wantError(t *testing.T, err error, op string, class provider.Class, code string) {
	t.Helper()
	var pe *provider.Error
	if !errors.As(err, &pe) {
		t.Fatalf("error = %T %v, want a *provider.Error", err, err)
	}
	if pe.Op != op || pe.Class != class || pe.Code != code {
		t.Fatalf("error = %s, want %s: %s (%s)", pe, op, class, code)
	}
}

const testRole = "arn:aws:iam::123456789012:role/secret-role"

var testAttempt = provider.Attempt{Namespace: "payments", RunID: "ork-1", TaskID: "migrate", Attempt: 1}

func tokenSource(token string) provider.TokenSource {
	return func(context.Context) (string, time.Duration, error) { return token, time.Hour, nil }
}

// against is a provider whose STS and other services are h.
func against(t *testing.T, h http.HandlerFunc) *Provider {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return mustNew(t, "region: eu-west-1", "sts_endpoint: "+srv.URL, "endpoint: "+srv.URL)
}

func stsFault(status int, code string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(status)
		fmt.Fprintf(w, `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>%s</Code>`+
			`<Message>User: %s is not authorized, token identity-token</Message></Error><RequestId>r</RequestId></ErrorResponse>`, code, testRole)
	}
}

func TestLogin_ClassifiesEveryFault(t *testing.T) {
	type answer struct {
		status int
		code   string
		class  provider.Class
		want   string
	}
	answers := map[string]answer{
		"an unlisted code":                  {400, "SomethingNew", provider.Unavailable, codeUnlisted},
		"a code that would carry a message": {400, "AccessDenied for " + testRole, provider.Unavailable, codeUnlisted},
		"a 5xx, whatever its code":          {500, "AccessDenied", provider.Unavailable, "AccessDenied"},
		"a 503":                             {503, "ServiceUnavailable", provider.Unavailable, "ServiceUnavailable"},
	}
	for code, class := range loginClasses {
		answers[code] = answer{400, code, class, code}
	}
	for name, tc := range answers {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			fault := stsFault(tc.status, tc.code)
			p := against(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fault(w, r) })
			_, err := p.Login(t.Context(), testRole, testAttempt, tokenSource("identity-token"))
			wantError(t, err, opLogin, tc.class, tc.want)
			if strings.Contains(err.Error(), "role/") || strings.Contains(err.Error(), "identity-token") {
				t.Errorf("the error holds AWS's message: %v", err)
			}
			if errors.Unwrap(errors.Unwrap(err)) != nil {
				t.Errorf("the error keeps %v, which may hold the response", errors.Unwrap(err))
			}
			if n := calls.Load(); n != 1 {
				t.Errorf("STS was asked %d times; the attempt's budget is the one retry policy", n)
			}
		})
	}
}

// An answer that is not the service's own (a proxy's page, another protocol's
// error) is told from no answer at all: both are retried, and whoever reads the
// message looks in a different place.
func TestAnAnswerThatCannotBeReadIsNotNoAnswer(t *testing.T) {
	for name, tc := range map[string]struct {
		status            int
		contentType, body string
	}{
		"Moto's refusal":                  {mototest.DeniedStatus, "text/html; charset=utf-8", mototest.DeniedBody(testRole, "ssm:GetParameter")},
		"Moto's refusal, as XML":          {mototest.DeniedStatus, "application/xml", mototest.DeniedBody(testRole, "ssm:GetParameter")},
		"Moto's refusal, said to be JSON": {mototest.DeniedStatus, "application/x-amz-json-1.1", mototest.DeniedBody(testRole, "ssm:GetParameter")},
		"a proxy's page":                  {403, "text/html", "<html><body>" + testRole + " is blocked</body></html>"},
		"a page that says all is well":    {200, "text/html", "<html><body>Welcome</body></html>"},
	} {
		t.Run(name, func(t *testing.T) {
			p := against(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			parsed, err := p.Parse(secret("ssm:a"))
			if err != nil {
				t.Fatal(err)
			}
			s := &session{p: p, who: testAttempt, now: time.Now, keys: keys{id: "a", secret: "b", token: "c", life: time.Hour, asked: time.Now()}}
			_, err = s.Secret(t.Context(), parsed)
			wantError(t, err, opRead, provider.Unavailable, codeUnreadable)
			if strings.Contains(err.Error(), "role/") {
				t.Errorf("the error holds the answer: %v", err)
			}
		})
	}
}

// Only these answers are final: a typo in the table that made another one so
// would fail a task for an outage.
func TestClasses_OnlyTheListedAnswersAreFinal(t *testing.T) {
	final := func(classes map[string]provider.Class) (n int) {
		for _, c := range classes {
			if c != provider.Unavailable {
				n++
			}
		}
		return n
	}
	if n := final(loginClasses); n != 6 {
		t.Errorf("%d login codes are final, want 6", n)
	}
	if n := final(readClasses); n != 13 {
		t.Errorf("%d read codes are final, want 13", n)
	}
	// A request AWS calls malformed is malformed again on every retry, and a
	// secret marked for deletion is not there to be read; neither is a refusal
	// of this principal (AWS-1).
	for code, want := range map[string]provider.Class{
		"ValidationException":       provider.Invalid,
		"InvalidParameterException": provider.Invalid,
		"InvalidKeyUsageException":  provider.Invalid,
		"InvalidRequestException":   provider.NotFound,
	} {
		if got, listed := readClasses[code]; !listed || got != want {
			t.Errorf("%s is %s (listed %v), want %s", code, got, listed, want)
		}
	}
	if loginClasses["InvalidIdentityToken"] != provider.Unavailable {
		t.Error("InvalidIdentityToken is final; STS answers it while it holds an old key set")
	}
}

// The exchange is anonymous and carries the attempt's token alone, whatever
// the Orker's environment says; the token is in the body, never the URL; and
// no credential of the environment is read.
func TestLogin_UsesNothingButTheAttemptsToken(t *testing.T) {
	var other atomic.Int64
	trap := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { other.Add(1) }))
	t.Cleanup(trap.Close)
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "AKIAORKER", "AWS_SECRET_ACCESS_KEY": "orker-secret", "AWS_SESSION_TOKEN": "orker-session",
		"AWS_PROFILE": "orker", "AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL": trap.URL, "AWS_ENDPOINT_URL_STS": trap.URL,
		"AWS_EC2_METADATA_SERVICE_ENDPOINT": trap.URL, "AWS_CONTAINER_CREDENTIALS_FULL_URI": trap.URL,
		"AWS_WEB_IDENTITY_TOKEN_FILE": "/orker/token", "AWS_ROLE_ARN": "arn:aws:iam::1:role/orker",
	} {
		t.Setenv(k, v)
	}
	var query string
	var form url.Values
	var header http.Header
	p := against(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		form, _ = url.ParseQuery(string(body))
		query, header = r.URL.RawQuery, r.Header.Clone()
		stsFault(400, "AccessDenied")(w, r)
	})
	_, err := p.Login(t.Context(), testRole, testAttempt, tokenSource("identity-token"))
	wantError(t, err, opLogin, provider.Refused, "AccessDenied")
	if got := header.Get("Authorization"); got != "" {
		t.Errorf("the exchange was signed: %q", got)
	}
	if header.Get("X-Amz-Security-Token") != "" || query != "" {
		t.Errorf("the exchange carried %q and the query %q", header.Get("X-Amz-Security-Token"), query)
	}
	for k, want := range map[string]string{"Action": "AssumeRoleWithWebIdentity", "RoleArn": testRole, "WebIdentityToken": "identity-token",
		"DurationSeconds": "3600", "RoleSessionName": "ork-1-1", "Policy": ""} {
		if got := form.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if other.Load() != 0 {
		t.Error("an endpoint of the Orker's environment was asked")
	}
}

func TestLogin_FollowsNoRedirect(t *testing.T) {
	var asked atomic.Int64
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { asked.Add(1) }))
	t.Cleanup(elsewhere.Close)
	for _, status := range []int{301, 302, 303, 307, 308} {
		p := against(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, elsewhere.URL, status) })
		_, err := p.Login(t.Context(), testRole, testAttempt, tokenSource("identity-token"))
		if provider.ClassOf(err) != provider.Unavailable || err == nil {
			t.Errorf("%d: %v, want unavailable", status, err)
		}
	}
	if asked.Load() != 0 {
		t.Error("a redirect was followed: the identity token reached another host")
	}
}

func TestLogin_NoAnswerIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	p := mustNew(t, "region: eu-west-1", "sts_endpoint: "+srv.URL)
	srv.Close()
	_, err := p.Login(t.Context(), testRole, testAttempt, tokenSource("identity-token"))
	wantError(t, err, opLogin, provider.Unavailable, codeNoAnswer)

	release := make(chan struct{})
	p = against(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = p.Login(ctx, testRole, testAttempt, tokenSource("identity-token"))
	wantError(t, err, opLogin, provider.Unavailable, codeNoAnswer)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("%v does not say the context ended", err)
	}
}

func TestLogin_TheTokenSourceFailing(t *testing.T) {
	p := against(t, func(http.ResponseWriter, *http.Request) { t.Error("STS was asked with no token") })
	refusal := &provider.TokenRefused{Reason: "IDENTITY_REVOKED"}
	_, err := p.Login(t.Context(), testRole, testAttempt, func(context.Context) (string, time.Duration, error) { return "", 0, refusal })
	if !errors.Is(err, refusal) {
		t.Errorf("%v, want the refusal itself", err)
	}
	_, err = p.Login(t.Context(), testRole, testAttempt, func(context.Context) (string, time.Duration, error) {
		return "", 0, errors.New("the engine at 10.0.0.1 did not answer")
	})
	wantError(t, err, opLogin, provider.Unavailable, codeToken)
}

func TestLogin_AnAnswerWithNoKeysIsUnavailable(t *testing.T) {
	const ok = `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult>%s</AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`
	for name, body := range map[string]string{
		"no credentials": "",
		"no key":         "<Credentials><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken><Expiration>2036-01-01T00:00:00Z</Expiration></Credentials>",
		"no expiry":      "<Credentials><AccessKeyId>a</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken></Credentials>",
	} {
		t.Run(name, func(t *testing.T) {
			p := against(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintf(w, ok, body) })
			_, err := p.Login(t.Context(), testRole, testAttempt, tokenSource("identity-token"))
			wantError(t, err, opLogin, provider.Unavailable, codeIncomplete)
		})
	}
}

// A session's life is the answer's own: its expiry less its date, bounded by
// what was asked for, whatever the Orker's clock says.
func TestNewKeys_LifeIsCountedFromTheAnswersDate(t *testing.T) {
	asked := time.Now()
	served := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC) // far from the Orker's clock
	for name, tc := range map[string]struct {
		expires time.Time
		want    time.Duration
		code    string
	}{
		"an hour":                   {served.Add(time.Hour), time.Hour, ""},
		"less than asked for":       {served.Add(20 * time.Minute), 20 * time.Minute, ""},
		"more than asked for":       {served.Add(10 * time.Hour), time.Hour, ""},
		"already over":              {served, 0, codeExpired},
		"too short to put in place": {served.Add(provider.MinBundleLifetime - time.Second), 0, codeExpired},
		"as short as may be":        {served.Add(provider.MinBundleLifetime), provider.MinBundleLifetime, ""},
		"over before it was given":  {served.Add(-time.Second), 0, codeExpired},
	} {
		t.Run(name, func(t *testing.T) {
			k, err := newKeysAt(served, tc.expires, time.Hour, asked)
			if tc.code != "" {
				wantError(t, err, opLogin, provider.Unavailable, tc.code)
				return
			}
			if err != nil || k.life != tc.want {
				t.Fatalf("life = %s, %v; want %s", k.life, err, tc.want)
			}
		})
	}
	// With no date, the life asked for is all that is known.
	k, err := newKeysAt(time.Time{}, served.Add(time.Hour), 30*time.Minute, asked)
	if err != nil || k.life != 30*time.Minute {
		t.Fatalf("life with no date = %s, %v", k.life, err)
	}
}

func newKeysAt(served, expires time.Time, duration time.Duration, asked time.Time) (keys, error) {
	out := &sts.AssumeRoleWithWebIdentityOutput{Credentials: &types.Credentials{
		AccessKeyId: awssdk.String("a"), SecretAccessKey: awssdk.String("s"), SessionToken: awssdk.String("t"), Expiration: &expires}}
	return keysOf(out.Credentials, served, duration, asked)
}

// A credential is handed out from the login it was made at while that is
// young, and from a new exchange with a fresh token once a quarter of its life
// is gone, by either clock: a refresh starts from the attempt's identity.
func TestCredential_ExchangesAgainOnceAQuarterIsGone(t *testing.T) {
	var tokens atomic.Int64
	p := against(t, stsFault(400, "AccessDenied"))
	start := time.Now()
	now := start
	s := &session{p: p, principal: testRole, who: testAttempt, now: func() time.Time { return now },
		token: func(context.Context) (string, time.Duration, error) { tokens.Add(1); return "fresh", time.Hour, nil },
		keys:  keys{id: "a", secret: "b", token: "c", expiration: start.Add(time.Hour), life: time.Hour, asked: start}}
	parsed := provider.Parsed{Ref: provider.Ref{Kind: provider.KindCredential, Name: "deploy"}, Detail: reference{}}

	now = start.Add(15*time.Minute - time.Second)
	b, err := s.Credential(t.Context(), parsed)
	if err != nil || b.Lifetime != 45*time.Minute+time.Second || tokens.Load() != 0 {
		t.Fatalf("young: %s, %v, %d tokens; want the login's own keys", b.Lifetime, err, tokens.Load())
	}
	now = start.Add(15 * time.Minute)
	if _, err := s.Credential(t.Context(), parsed); provider.ClassOf(err) != provider.Refused || tokens.Load() != 1 {
		t.Fatalf("a quarter gone: %v, %d tokens; want a new exchange", err, tokens.Load())
	}
	// A suspended host: the wall clock moved, the monotonic one did not.
	now = start.Add(time.Second)
	s.keys.asked = start.Add(-30 * time.Minute).Round(0)
	if _, err := s.Credential(t.Context(), parsed); provider.ClassOf(err) != provider.Refused || tokens.Load() != 2 {
		t.Fatalf("after a suspend: %v, %d tokens; want a new exchange", err, tokens.Load())
	}
}

// A secret is read with keys as young as a credential's: the login lasts a
// day, its keys an hour.
func TestSecret_ExchangesAgainOnceAQuarterIsGone(t *testing.T) {
	var tokens atomic.Int64
	p := against(t, stsFault(400, "AccessDenied"))
	start := time.Now()
	s := &session{p: p, principal: testRole, who: testAttempt, now: func() time.Time { return start.Add(2 * time.Hour) },
		token: func(context.Context) (string, time.Duration, error) { tokens.Add(1); return "fresh", time.Hour, nil },
		keys:  keys{id: "a", secret: "b", token: "c", life: time.Hour, asked: start}}
	parsed, err := p.Parse(secret("ssm:a"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Secret(t.Context(), parsed)
	wantError(t, err, opLogin, provider.Refused, "AccessDenied")
	if tokens.Load() != 1 {
		t.Errorf("%d tokens, want one exchange", tokens.Load())
	}
}

// The engine is told the login does not end, so it never replaces the login
// to refresh what the session refreshes itself.
func TestLifetime_IsNotTheKeys(t *testing.T) {
	s := &session{now: time.Now, keys: keys{life: time.Hour, asked: time.Now()}}
	if got := s.Lifetime(); got != 0 {
		t.Errorf("Lifetime = %s, want 0", got)
	}
}

func TestCredential_RefusesAReferenceThatIsNotTheLogin(t *testing.T) {
	s := &session{p: mustNew(t, "region: eu-west-1"), now: time.Now, keys: keys{life: time.Hour, asked: time.Now()}}
	for name, parsed := range map[string]provider.Parsed{
		"no name":    {Ref: provider.Ref{Kind: provider.KindCredential}, Detail: reference{}},
		"a bad name": {Ref: provider.Ref{Kind: provider.KindCredential, Name: "a b; rm"}, Detail: reference{}},
		"a secret":   {Ref: provider.Ref{Kind: provider.KindSecret, Name: "a"}, Detail: reference{service: serviceSSM, id: "x"}},
		"not parsed": {Ref: provider.Ref{Kind: provider.KindCredential, Name: "a"}},
	} {
		if _, err := s.Credential(t.Context(), parsed); provider.ClassOf(err) != provider.Invalid {
			t.Errorf("%s: %v, want invalid", name, err)
		}
	}
	if _, err := s.Secret(t.Context(), provider.Parsed{}); provider.ClassOf(err) != provider.Invalid {
		t.Errorf("Secret of a reference nothing parsed: %v", err)
	}
}

func TestClose_DropsTheKeys(t *testing.T) {
	s := &session{p: mustNew(t, "region: eu-west-1"), now: time.Now, token: tokenSource("t"),
		keys: keys{id: "a", secret: "b", token: "c", life: time.Hour, asked: time.Now()}}
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.keys != (keys{}) {
		t.Error("the keys are kept after Close")
	}
	parsed := provider.Parsed{Ref: provider.Ref{Kind: provider.KindCredential, Name: "a"}, Detail: reference{}}
	if _, err := s.Credential(t.Context(), parsed); err == nil {
		t.Error("Credential after Close succeeded")
	}
	if _, err := s.Secret(t.Context(), provider.Parsed{Detail: reference{service: serviceSSM, id: "x"}}); err == nil {
		t.Error("Secret after Close succeeded")
	}
	if err := s.login(t.Context()); err == nil {
		t.Error("login after Close succeeded")
	}
}

func TestSecret_ClassifiesEveryFault(t *testing.T) {
	refs := map[service]provider.Ref{
		serviceSSM:     secret("ssm:/payments/db"),
		serviceSecrets: secret("secretsmanager:payments/db"),
		serviceKMS:     kms("kms:alias/payments", []byte("blob")),
	}
	answers := map[string]provider.Class{"SomethingNew": provider.Unavailable}
	for code, class := range readClasses {
		answers[code] = class
	}
	for svc, ref := range refs {
		for code, class := range answers {
			t.Run(string(svc)+"/"+code, func(t *testing.T) {
				var calls atomic.Int64
				var header http.Header
				p := against(t, func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					header = r.Header.Clone()
					w.Header().Set("Content-Type", "application/x-amz-json-1.1")
					w.WriteHeader(400)
					fmt.Fprintf(w, `{"__type":%q,"message":"User: %s on secret-value"}`, code, testRole)
				})
				parsed, err := p.Parse(ref)
				if err != nil {
					t.Fatal(err)
				}
				s := &session{p: p, who: testAttempt, now: time.Now, keys: keys{id: "ASIASESSION", secret: "b", token: "session-token", life: time.Hour, asked: time.Now()}}
				_, err = s.Secret(t.Context(), parsed)
				want := code
				if code == "SomethingNew" {
					want = codeUnlisted
				}
				wantError(t, err, opRead, class, want)
				if strings.Contains(err.Error(), "role/") || strings.Contains(err.Error(), "secret-value") {
					t.Errorf("the error holds AWS's message: %v", err)
				}
				if calls.Load() != 1 {
					t.Errorf("asked %d times", calls.Load())
				}
				if !strings.Contains(header.Get("Authorization"), "ASIASESSION/") || header.Get("X-Amz-Security-Token") != "session-token" {
					t.Error("the call was not signed with the session's keys")
				}
			})
		}
	}
}

// A ciphertext is bound to a namespace, so an attempt that names none
// decrypts nothing; KMS is not asked.
func TestSecret_KMSNeedsANamespace(t *testing.T) {
	p := against(t, func(http.ResponseWriter, *http.Request) { t.Error("KMS was asked") })
	parsed, err := p.Parse(kms("kms:alias/a", []byte("blob")))
	if err != nil {
		t.Fatal(err)
	}
	s := &session{p: p, now: time.Now, keys: keys{id: "a", secret: "b", token: "c", life: time.Hour, asked: time.Now()}}
	_, err = s.Secret(t.Context(), parsed)
	wantError(t, err, opRead, provider.Invalid, codeNoNamespace)
}

// An answer past the bound is never read whole: it is no answer.
func TestSecret_AnEndlessAnswerIsCutOff(t *testing.T) {
	var sent atomic.Int64
	p := against(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		io.WriteString(w, `{"Parameter":{"Value":"`)
		chunk := []byte(strings.Repeat("x", 64<<10))
		for r.Context().Err() == nil && sent.Load() < 256<<20 {
			n, err := w.Write(chunk)
			sent.Add(int64(n))
			if err != nil {
				return
			}
		}
	})
	parsed, err := p.Parse(secret("ssm:a"))
	if err != nil {
		t.Fatal(err)
	}
	s := &session{p: p, who: testAttempt, now: time.Now, keys: keys{id: "a", secret: "b", token: "c", life: time.Hour, asked: time.Now()}}
	_, err = s.Secret(t.Context(), parsed)
	if err == nil || provider.ClassOf(err) != provider.Unavailable {
		t.Fatalf("%v, want unavailable", err)
	}
	if sent.Load() >= 256<<20 {
		t.Error("the whole answer was read")
	}
}

// An answer that holds no value is never an empty secret.
func TestSecret_AnAnswerWithNoValueIsUnavailable(t *testing.T) {
	for _, ref := range []provider.Ref{secret("ssm:a"), secret("secretsmanager:a"), kms("kms:alias/a", []byte("blob"))} {
		p := against(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/x-amz-json-1.1")
			io.WriteString(w, `{}`)
		})
		parsed, err := p.Parse(ref)
		if err != nil {
			t.Fatal(err)
		}
		s := &session{p: p, who: testAttempt, now: time.Now, keys: keys{id: "a", secret: "b", token: "c", life: time.Hour, asked: time.Now()}}
		_, err = s.Secret(t.Context(), parsed)
		wantError(t, err, opRead, provider.Unavailable, codeIncomplete)
	}
}
