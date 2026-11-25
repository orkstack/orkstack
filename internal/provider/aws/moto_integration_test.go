//go:build integration

package aws_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/aws"
	"github.com/orkstack/orkstack/internal/provider/providertest"
	"github.com/orkstack/orkstack/internal/testsupport/dextest"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/mototest"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

const (
	region = "eu-west-1"
	// Moto's account, which a role's ARN names.
	role    = "arn:aws:iam::123456789012:role/payments-deployer"
	refused = "arn:aws:iam::123456789012:role/not-trusted"
)

var attempt = provider.Attempt{Namespace: "payments", RunID: "ork-01jc9", TaskID: "migrate", Attempt: 1}

// startMoto starts a Moto of this test's own.
func startMoto(t *testing.T) *mototest.Moto {
	t.Helper()
	root, err := dextest.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id := os.Getenv(proof.EnvID)
	if id == "" {
		id = "test-" + strconv.Itoa(os.Getpid())
	}
	m, err := mototest.Start(ctx, root, id, dextest.ProcessOwner())
	if errors.Is(err, mototest.ErrAbsent) {
		if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
			t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: %v", err)
		}
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if t.Failed() {
			if out, err := exec.CommandContext(ctx, "docker", "logs", "--tail", "80", m.ID).CombinedOutput(); err == nil {
				t.Logf("Moto's log:\n%s", out)
			}
		}
		if err := m.Stop(ctx); err != nil {
			t.Errorf("stop Moto: %v", err)
		}
	})
	return m
}

// Moto accepts any identity token and any session length (mototest), so no
// test here claims that STS judged either: that a token is AWS's to accept is
// the issuer's own tests' claim.

// front stands between the provider and Moto. It keeps the identity tokens
// STS was presented, and answers the two faults Moto cannot be made to give,
// in STS's own words: Moto accepts any web identity token and never
// throttles. Everything else is Moto's answer.
type front struct {
	url string

	mu       sync.Mutex
	tokens   []string
	names    []string
	throttle bool
	// What Moto answered last, as the provider was given it.
	lastStatus         int
	lastType, lastBody string
}

func newFront(t *testing.T, moto *mototest.Moto) *front {
	t.Helper()
	target, err := url.Parse(moto.URL())
	if err != nil {
		t.Fatal(err)
	}
	forward := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		// The host is signed.
		r.Out.Host = r.In.Host
	}}
	f := &front{}
	forward.ModifyResponse = func(r *http.Response) error {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		f.mu.Lock()
		defer f.mu.Unlock()
		f.lastStatus, f.lastType, f.lastBody = r.StatusCode, r.Header.Get("Content-Type"), string(body)
		return nil
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if form, err := url.ParseQuery(string(body)); err == nil && form.Get("Action") == "AssumeRoleWithWebIdentity" {
			if code := f.saw(form); code != "" {
				w.Header().Set("Content-Type", "text/xml")
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error></ErrorResponse>`,
					code, form.Get("WebIdentityToken"))
				return
			}
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	f.url = srv.URL
	return f
}

// saw keeps what an exchange carried and is the fault to answer it with, or
// "".
func (f *front) saw(form url.Values) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens = append(f.tokens, form.Get("WebIdentityToken"))
	f.names = append(f.names, form.Get("RoleSessionName"))
	switch {
	case f.throttle:
		return "Throttling"
	case form.Get("RoleArn") == refused:
		return "AccessDenied"
	}
	return ""
}

func (f *front) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.tokens...)
}

// declare is the provider as an operator declares it, at endpoint.
func declare(t *testing.T, endpoint string) provider.Provider {
	t.Helper()
	set, err := provider.ParseWith(fmt.Appendf(nil, "providers:\n  aws:\n    type: aws\n    region: %s\n    sts_endpoint: %s\n    endpoint: %s\n", region, endpoint, endpoint),
		map[string]provider.Type{aws.TypeName: aws.Type{}})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := set.ByName("aws")
	return p
}

// seeded is what the test put in Moto, as its operator.
type seeded struct {
	key        string
	ciphertext []byte
	foreign    []byte
}

func seed(t *testing.T, m *mototest.Moto) seeded {
	t.Helper()
	ctx := t.Context()
	cfg := awssdk.Config{Region: region, BaseEndpoint: awssdk.String(m.URL()), Credentials: credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "operator", "")}
	params := ssm.NewFromConfig(cfg)
	for name, value := range map[string]string{"/payments/db-url": "postgres://payments-db-value"} {
		if _, err := params.PutParameter(ctx, &ssm.PutParameterInput{Name: &name, Value: &value, Type: ssmtypes.ParameterTypeSecureString}); err != nil {
			t.Fatalf("put parameter: %v", err)
		}
	}
	secrets := secretsmanager.NewFromConfig(cfg)
	for _, in := range []*secretsmanager.CreateSecretInput{
		{Name: awssdk.String("payments/db"), SecretString: awssdk.String(`{"url":"postgres://from-secrets-manager","port":5432}`)},
		{Name: awssdk.String("payments/blob"), SecretBinary: []byte{0, 1, 2, 255}},
	} {
		if _, err := secrets.CreateSecret(ctx, in); err != nil {
			t.Fatalf("create secret: %v", err)
		}
	}
	keys := kmssdk.NewFromConfig(cfg)
	made, err := keys.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	if _, err := keys.CreateAlias(ctx, &kmssdk.CreateAliasInput{AliasName: awssdk.String("alias/payments"), TargetKeyId: made.KeyMetadata.KeyId}); err != nil {
		t.Fatalf("create alias: %v", err)
	}
	other, err := keys.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	if _, err := keys.CreateAlias(ctx, &kmssdk.CreateAliasInput{AliasName: awssdk.String("alias/other"), TargetKeyId: other.KeyMetadata.KeyId}); err != nil {
		t.Fatalf("create alias: %v", err)
	}
	out := seeded{key: *made.KeyMetadata.KeyId}
	for namespace, into := range map[string]*[]byte{"payments": &out.ciphertext, "billing": &out.foreign} {
		enc, err := keys.Encrypt(ctx, &kmssdk.EncryptInput{KeyId: made.KeyMetadata.KeyId, Plaintext: []byte("decrypted-by-kms"),
			EncryptionContext: map[string]string{"ork:namespace": namespace}})
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		*into = enc.CiphertextBlob
	}
	return out
}

func tokens() provider.TokenSource {
	var mu sync.Mutex
	n := 0
	return func(context.Context) (string, time.Duration, error) {
		mu.Lock()
		defer mu.Unlock()
		n++
		return fmt.Sprintf("header.identity-token-%04d.signature", n), time.Hour, nil
	}
}

func TestIntegration_AWSProviderPassesTheConformanceSuite(t *testing.T) {
	moto := startMoto(t)
	seed(t, moto)
	f := newFront(t, moto)
	proxy := faultproxy.New(t, strings.TrimPrefix(f.url, "http://"))
	value := []byte("postgres://payments-db-value")
	providertest.Conformance(t, providertest.Harness{
		Provider:    declare(t, "http://"+proxy.Addr()),
		Principal:   role,
		Refused:     refused,
		Token:       tokens(),
		Attempt:     attempt,
		Secret:      &provider.Ref{Kind: provider.KindSecret, Locator: "ssm:/payments/db-url"},
		SecretValue: value,
		Missing:     &provider.Ref{Kind: provider.KindSecret, Locator: "ssm:/payments/none"},
		Credential:  &provider.Ref{Kind: provider.KindCredential, Name: "deploy"},
		Expires:     true,
		Outage: func(*testing.T) func() {
			proxy.Cut()
			return proxy.Restore
		},
		Throttle: func(*testing.T) func() {
			set := func(on bool) { f.mu.Lock(); f.throttle = on; f.mu.Unlock() }
			set(true)
			return func() { set(false) }
		},
		Tokens: f.seen,
		Values: func() [][]byte { return [][]byte{value} },
	})
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, name := range f.names {
		if name != "ork-01jc9-1" {
			t.Errorf("a session is named %q, want the run and its attempt", name)
		}
	}
}

// Every source reads what it names, and what AWS does not have or will not
// give is told apart.
func TestIntegration_AWSProviderReadsEverySource(t *testing.T) {
	moto := startMoto(t)
	s := seed(t, moto)
	p := declare(t, moto.URL())
	session, err := p.Login(t.Context(), role, attempt, tokens())
	if err != nil {
		t.Fatal(err)
	}
	// Moto echoes the length asked for; what is tested is that its answer's
	// expiry and date are read as a lifetime.
	login, err := p.Parse(provider.Ref{Kind: provider.KindCredential})
	if err != nil {
		t.Fatal(err)
	}
	login.Ref.Name = "deploy"
	b, err := session.Credential(t.Context(), login)
	if err != nil {
		t.Fatal(err)
	}
	if b.Lifetime <= 50*time.Minute || b.Lifetime > time.Hour {
		t.Errorf("the credential's lifetime is %s, want about an hour", b.Lifetime)
	}
	for name, tc := range map[string]struct {
		ref   provider.Ref
		want  string
		class provider.Class
	}{
		"a parameter":                    {ref: provider.Ref{Locator: "ssm:/payments/db-url"}, want: "postgres://payments-db-value"},
		"a parameter's version":          {ref: provider.Ref{Locator: "ssm:/payments/db-url:1"}, want: "postgres://payments-db-value"},
		"a parameter that is not there":  {ref: provider.Ref{Locator: "ssm:/payments/none"}, class: provider.NotFound},
		"a secret":                       {ref: provider.Ref{Locator: "secretsmanager:payments/db"}, want: `{"url":"postgres://from-secrets-manager","port":5432}`},
		"a secret's key":                 {ref: provider.Ref{Locator: "secretsmanager:payments/db#url"}, want: "postgres://from-secrets-manager"},
		"a secret's number":              {ref: provider.Ref{Locator: "secretsmanager:payments/db#port"}, want: "5432"},
		"a secret's key that is missing": {ref: provider.Ref{Locator: "secretsmanager:payments/db#user"}, class: provider.NotFound},
		"a binary secret":                {ref: provider.Ref{Locator: "secretsmanager:payments/blob"}, want: "\x00\x01\x02\xff"},
		"a key of a binary secret":       {ref: provider.Ref{Locator: "secretsmanager:payments/blob#x"}, class: provider.Invalid},
		"a secret that is not there":     {ref: provider.Ref{Locator: "secretsmanager:payments/none"}, class: provider.NotFound},
		"a ciphertext, by the key":       {ref: provider.Ref{Locator: "kms:" + s.key, Ciphertext: s.ciphertext}, want: "decrypted-by-kms"},
		"a ciphertext, by its alias":     {ref: provider.Ref{Locator: "kms:alias/payments", Ciphertext: s.ciphertext}, want: "decrypted-by-kms"},
		"another namespace's ciphertext": {ref: provider.Ref{Locator: "kms:alias/payments", Ciphertext: s.foreign}, class: provider.Refused},
		"a ciphertext under another key": {ref: provider.Ref{Locator: "kms:alias/other", Ciphertext: s.ciphertext}, class: provider.Refused},
		"what no key made":               {ref: provider.Ref{Locator: "kms:alias/payments", Ciphertext: []byte("not a ciphertext")}, class: provider.Refused},
		"a key that is not there":        {ref: provider.Ref{Locator: "kms:alias/none", Ciphertext: s.ciphertext}, class: provider.NotFound},
	} {
		t.Run(name, func(t *testing.T) {
			tc.ref.Kind = provider.KindSecret
			parsed, err := p.Parse(tc.ref)
			if err != nil {
				t.Fatal(err)
			}
			got, err := session.Secret(t.Context(), parsed)
			if tc.want != "" {
				if err != nil || string(got.Bytes) != tc.want {
					t.Fatalf("Secret = %q, %v; want %q", got.Bytes, err, tc.want)
				}
				return
			}
			if err == nil || provider.ClassOf(err) != tc.class {
				t.Fatalf("Secret = %q, %v; want %s", got.Bytes, err, tc.class)
			}
		})
	}
}

// Moto verifies a call's signature and the role's own policies once told to:
// the provider's calls are signed with the keys of the session it was given,
// and a role that may not read a parameter is not given it. How AWS words that
// refusal is not tested here: Moto answers a denied call of a JSON service with
// another protocol's error, which no SDK reads.
func TestIntegration_AWSProviderSignsWithTheRolesSessionAlone(t *testing.T) {
	moto := startMoto(t)
	seed(t, moto)
	const reader, idle = "arn:aws:iam::123456789012:role/reader", "arn:aws:iam::123456789012:role/idle"
	iam(t, moto, "CreateRole", "RoleName", "reader", "AssumeRolePolicyDocument", `{"Version":"2012-10-17","Statement":[]}`)
	iam(t, moto, "CreateRole", "RoleName", "idle", "AssumeRolePolicyDocument", `{"Version":"2012-10-17","Statement":[]}`)
	iam(t, moto, "PutRolePolicy", "RoleName", "reader", "PolicyName", "read", "PolicyDocument",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ssm:GetParameter","Resource":"*"}]}`)
	f := newFront(t, moto)
	p := declare(t, f.url)
	sessions := map[string]provider.Session{}
	for _, arn := range []string{reader, idle} {
		s, err := p.Login(t.Context(), arn, attempt, tokens())
		if err != nil {
			t.Fatal(err)
		}
		sessions[arn] = s
	}
	// The exchange is unsigned, which Moto's check refuses altogether, so the
	// check starts after the logins.
	resp, err := http.Post(moto.URL()+"/moto-api/reset-auth", "text/plain", strings.NewReader("0"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	parsed, err := p.Parse(provider.Ref{Kind: provider.KindSecret, Locator: "ssm:/payments/db-url"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := sessions[reader].Secret(t.Context(), parsed)
	if err != nil || string(got.Bytes) != "postgres://payments-db-value" {
		t.Errorf("the role that may read it: %q, %v", got.Bytes, err)
	}
	var pe *provider.Error
	if _, err := sessions[idle].Secret(t.Context(), parsed); !errors.As(err, &pe) || pe.Class != provider.Unavailable || pe.Code != "UnreadableAnswer" {
		t.Errorf("the role that may not: %v, want unavailable (UnreadableAnswer): Moto's 403", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	want := mototest.DeniedBody("arn:aws:sts::123456789012:assumed-role/idle/ork-01jc9-1", "ssm:GetParameter")
	t.Logf("Moto's refusal: %d, Content-Type %q", f.lastStatus, f.lastType)
	if f.lastStatus != mototest.DeniedStatus || f.lastBody != want {
		t.Errorf("Moto's refusal is %d %q, want %d %q", f.lastStatus, f.lastBody, mototest.DeniedStatus, want)
	}
}

// iam calls one IAM action of Moto's, as its operator. The IAM client is not
// a dependency of OrkStack's, and Moto checks no signature yet.
func iam(t *testing.T, m *mototest.Moto, action string, params ...string) {
	t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2010-05-08"}}
	for i := 0; i+1 < len(params); i += 2 {
		form.Set(params[i], params[i+1])
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, m.URL()+"/", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/iam/aws4_request, SignedHeaders=host, Signature=unchecked")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("iam %s: %s: %s", action, resp.Status, body)
	}
}
