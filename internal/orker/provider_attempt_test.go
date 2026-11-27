package orker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/types"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/logstream"
)

// The providers of these tests are in-process: they show what an attempt does
// with a login, a value and a bundle. What a real provider answers is each
// type's own conformance suite's, against the real system.

type loginType struct{ p *loginProvider }

func (loginType) Name() string { return "logins" }
func (l loginType) New(name string, c *provider.Config) (provider.Provider, error) {
	audience, _ := c.String("audience")
	l.p.name, l.p.audience = name, audience
	return l.p, nil
}

type loginProvider struct {
	name, audience string

	mu         sync.Mutex
	principals []string
	tokens     []string
	closed     int
	secret     []byte
	secretErr  error
}

func (p *loginProvider) Name() string                 { return p.name }
func (*loginProvider) Type() string                   { return "logins" }
func (p *loginProvider) Audience() string             { return p.audience }
func (*loginProvider) Describe() provider.Description { return provider.Description{} }
func (*loginProvider) LoginScheme() string            { return "auth" }
func (*loginProvider) Parse(ref provider.Ref) (provider.Parsed, error) {
	if strings.HasPrefix(ref.Locator, "nope/") {
		return provider.Parsed{}, errors.New("not a path of this type")
	}
	return provider.Parsed{Ref: ref}, nil
}
func (p *loginProvider) Login(ctx context.Context, principal string, _ provider.Attempt, token provider.TokenSource) (provider.Session, error) {
	tok, _, err := token(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.principals, p.tokens = append(p.principals, principal), append(p.tokens, tok)
	return loginSession{p}, nil
}

type loginSession struct{ p *loginProvider }

func (s loginSession) Secret(context.Context, provider.Parsed) (provider.Value, error) {
	return provider.Value{Bytes: s.p.secret}, s.p.secretErr
}
func (loginSession) Credential(context.Context, provider.Parsed) (provider.Bundle, error) {
	return provider.Bundle{Files: map[string][]byte{"password": []byte("a-leased-password-value"), "config": []byte("region = eu")},
		Plain: map[string]bool{"config": true}, Lifetime: time.Hour}, nil
}
func (loginSession) Renew(context.Context, provider.Bundle) (provider.Bundle, error) {
	return provider.Bundle{}, provider.ErrReissue
}
func (loginSession) Lifetime() time.Duration { return time.Hour }
func (s loginSession) Close(context.Context) error {
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	s.p.closed++
	return nil
}

// loginDispatcher is an Orker that serves task identity and declares provider
// vault; its mint answers one token for what it is asked.
func loginDispatcher(t *testing.T, rt ContainerRuntime, p *loginProvider) (*OCIDispatcher, *[]string) {
	t.Helper()
	d := newAttemptDispatcher(t, rt, artifacts.NewMemoryAttemptStore())
	set, err := provider.ParseWith([]byte("providers:\n  vault:\n    type: logins\n    audience: vault.internal\n"),
		map[string]provider.Type{"logins": loginType{p}})
	if err != nil {
		t.Fatal(err)
	}
	d.SetProviders(set, 2, time.Second)
	cfg := DefaultIdentityConfig()
	cfg.SetupBudget = 10 * time.Second
	d.identityConfig = &cfg
	d.identity.Store(identitySupportOf(true))
	// A run that logs in has its results recorded by the engine.
	commits := artifacts.NewMemoryCommitStore()
	d.Commits = commits
	d.SetResultCommit((&recordingEngine{store: commits, root: "mem://store/"}).commit)
	var mu sync.Mutex
	var asked []string
	d.identityMint = func(_ context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		resp := &orkstackv1.MintAttemptTokensResponse{}
		for _, tok := range req.GetTokens() {
			asked = append(asked, tok.GetName()+"@"+tok.GetAudience())
			resp.Tokens = append(resp.Tokens, &orkstackv1.IssuedToken{Name: tok.GetName(), Audience: tok.GetAudience(),
				Token: "eyJhbGciOiJFUzI1NiJ9.for-" + tok.GetName() + ".sig", ExpiresInSeconds: 3600, IssuedAt: timestamppb.Now()})
		}
		return resp, nil
	}
	return d, &asked
}

func loginInput() *orkstackv1.ExecuteTaskInput {
	in := baseExportInput("deploy")
	key, err := artifacts.CommitKey(in.GetNamespace(), in.GetRunId(), in.GetTaskId())
	if err != nil {
		panic(err) // a fixed coordinate of this file
	}
	in.CommitUri = "mem://store/" + key
	in.Secrets = []*orkstackv1.SecretRef{{Name: "db-url", Source: "vault:secret/db#url", Grant: "db"}}
	in.Identity = &orkstackv1.IdentityContext{DigestVersion: 1, AdmissionSha256: testAdmissionSHA,
		Credentials: []*orkstackv1.CredentialRef{{Name: "lease", Grant: "db", Path: "database/creds/rw"}},
		Grants:      []*orkstackv1.IdentityGrant{{Name: "db", Provider: "vault", Principal: "payments-app", Audience: "vault.internal"}}}
	return in
}

// loginReadingRuntime is the phase runtime whose container, at create, reads
// what its logins gave it.
type loginReadingRuntime struct {
	phaseCapturingRuntime
	files    map[string]string
	taskJSON string
	mounts   []Mount
}

func (r *loginReadingRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mounts, r.files = spec.Mounts, map[string]string{}
	for _, m := range spec.Mounts {
		for _, f := range map[string][]string{
			credentialsMountPath: {"lease/password", "lease/config"},
			ContainerInput:       {"secrets/db-url", "task.json"},
		}[m.Destination] {
			if b, err := os.ReadFile(filepath.Join(m.Source, f)); err == nil {
				r.files[f] = string(b)
			}
		}
		if m.Destination == ContainerSecrets {
			if b, err := os.ReadFile(filepath.Join(m.Source, "db-url")); err == nil {
				r.files["secrets/db-url"] = string(b)
			}
		}
	}
	r.taskJSON = r.files["task.json"]
	return r.phaseCapturingRuntime.Create(ctx, spec)
}

// TestExecute_ATaskThatLogsIn: an attempt logs in once for its grant, with
// the engine's token for it and as the principal its run was admitted with,
// before its container exists; its secret and its credential are where the
// task finds them, read-only and named in task.json, never with a value; and
// its login is closed and its files gone with the attempt.
func TestExecute_ATaskThatLogsIn(t *testing.T) {
	rt := &loginReadingRuntime{}
	p := &loginProvider{secret: []byte("postgres://a-secret-url")}
	d, asked := loginDispatcher(t, rt, p)
	if _, err := d.Execute(context.Background(), loginInput()); err != nil {
		t.Fatalf("Execute = %v", err)
	}
	if !slices.Equal(*asked, []string{"grant:db@vault.internal"}) || !slices.Equal(p.principals, []string{"payments-app"}) ||
		p.tokens[0] != "eyJhbGciOiJFUzI1NiJ9.for-grant:db.sig" {
		t.Fatalf("asked %v, logged in as %v with %v; want one login for the secret and the credential", *asked, p.principals, p.tokens)
	}
	if rt.files["secrets/db-url"] != "postgres://a-secret-url" || rt.files["lease/password"] != "a-leased-password-value" || rt.files["lease/config"] != "region = eu" {
		t.Fatalf("the container read %v", rt.files)
	}
	var bound *Mount
	for i := range rt.mounts {
		if rt.mounts[i].Destination == credentialsMountPath {
			bound = &rt.mounts[i]
		}
	}
	if bound == nil || !bound.ReadOnly {
		t.Fatalf("credentials mount %+v", bound)
	}
	if _, err := os.Lstat(bound.Source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the credentials survived the attempt: %v", err)
	}
	doc := strings.NewReplacer(" ", "", "\n", "").Replace(rt.taskJSON)
	for _, want := range []string{`"credentials":"/orkstack/credentials"`, `"name":"lease"`, `"provider":"vault"`, `"path":"/orkstack/credentials/lease"`, `"password"`} {
		if !strings.Contains(doc, want) {
			t.Errorf("task.json lacks %s:\n%s", want, rt.taskJSON)
		}
	}
	for _, value := range []string{"a-leased-password-value", "a-secret-url", "for-grant:db"} {
		if strings.Contains(rt.taskJSON, value) {
			t.Errorf("task.json holds %s", value)
		}
	}
	if p.closed != 1 {
		t.Fatalf("%d logins were closed; want 1", p.closed)
	}

	plain := &loginReadingRuntime{}
	d2, asked2 := loginDispatcher(t, plain, &loginProvider{})
	if _, err := d2.Execute(context.Background(), baseExportInput("plain")); err != nil {
		t.Fatal(err)
	}
	if len(*asked2) != 0 || strings.Contains(plain.taskJSON, "credentials") {
		t.Fatalf("a task that uses no provider asked %v; task.json %s", *asked2, plain.taskJSON)
	}
}

// TestExecute_AReferenceThisOrkerRefuses: each is refused with its own reason
// before anything is asked, logged in to or created.
func TestExecute_AReferenceThisOrkerRefuses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*OCIDispatcher, *orkstackv1.ExecuteTaskInput)
		reason string
	}{
		{"a grant the run was not admitted with", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) { in.Identity.Grants = nil }, "IDENTITY_NOT_ADMITTED"},
		{"a source of another provider", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) { in.Secrets[0].Source = "aws:ssm:/x" }, "IDENTITY_NOT_ADMITTED"},
		{"a provider not declared here", func(d *OCIDispatcher, _ *orkstackv1.ExecuteTaskInput) { d.SetProviders(provider.Set{}, 1, time.Second) }, provider.ReasonProviderNotConfigured},
		{"a provider declared for another audience", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) { in.Identity.Grants[0].Audience = "other" }, provider.ReasonProviderNotConfigured},
		{"a path its provider refuses", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) { in.Identity.Credentials[0].Path = "nope/x" }, provider.ReasonProviderRequestInvalid},
		{"a path with a field", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) { in.Identity.Credentials[0].Path = "a/b#c" }, provider.ReasonProviderRequestInvalid},
		{"a path that spells a login", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
			in.Identity.Credentials[0].Path = "auth:admin"
		}, provider.ReasonProviderRequestInvalid},
		{"a ciphertext that is not base64", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) { in.Secrets[0].Ciphertext = "a b" }, provider.ReasonProviderRequestInvalid},
		{"outside this Orker's floor", func(d *OCIDispatcher, _ *orkstackv1.ExecuteTaskInput) {
			d.Policy.SecretAllow = []string{"vault:secret/db"}
		}, ReasonSecretNotPermitted},
		{"the Orker's own source with a grant", func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
			in.Secrets[0].Source = "file:/proc/self/environ"
		}, ReasonSecretNotPermitted},
	} {
		rt := &loginReadingRuntime{}
		p := &loginProvider{secret: []byte("value")}
		d, asked := loginDispatcher(t, rt, p)
		in := loginInput()
		tc.edit(d, in)
		_, err := d.Execute(context.Background(), in)
		var f *Failure
		if !errors.As(err, &f) && !strings.Contains(errText(err), tc.reason) {
			t.Errorf("%s: %v; want %s", tc.name, err, tc.reason)
		} else if f != nil && f.Reason != tc.reason {
			t.Errorf("%s: %s; want %s", tc.name, f.Reason, tc.reason)
		}
		if len(*asked) != 0 || len(p.principals) != 0 || rt.mounts != nil {
			t.Errorf("%s: asked %v, logged in as %v, created %v", tc.name, *asked, p.principals, rt.mounts)
		}
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestSetupProviders_LeavesNothingBehind: a setup that fails ends with its
// own reason, starts no container, closes what it opened and leaves no file.
func TestSetupProviders_LeavesNothingBehind(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*OCIDispatcher, *loginProvider)
		reason string
		closed int
	}{
		{"the engine refuses the token", func(d *OCIDispatcher, _ *loginProvider) {
			d.identityMint = func(context.Context, *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
				return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("IDENTITY_REVOKED: the namespace no longer allows it"))
			}
		}, "IDENTITY_REVOKED", 0},
		{"the engine's clock is apart", func(d *OCIDispatcher, _ *loginProvider) {
			d.identityMint = func(_ context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
				return &orkstackv1.MintAttemptTokensResponse{Tokens: []*orkstackv1.IssuedToken{{Name: req.GetTokens()[0].GetName(),
					Audience: "vault.internal", Token: "t", ExpiresInSeconds: 60, IssuedAt: timestamppb.New(time.Now().Add(-time.Hour))}}}, nil
			}
		}, ReasonIdentityClockSkew, 0},
		{"the secret is not there", func(_ *OCIDispatcher, p *loginProvider) {
			p.secretErr = &provider.Error{Class: provider.NotFound, Op: "read", Code: "404"}
		}, provider.ReasonSecretNotFound, 1},
		{"the secret is too large", func(_ *OCIDispatcher, p *loginProvider) { p.secret = make([]byte, provider.MaxValueBytes+1) }, provider.ReasonSecretTooLarge, 1},
	} {
		rt := &loginReadingRuntime{}
		p := &loginProvider{secret: []byte("value")}
		d, _ := loginDispatcher(t, rt, p)
		tc.edit(d, p)
		_, err := d.Execute(context.Background(), loginInput())
		if !strings.Contains(errText(err), tc.reason) && !failedWith(err, tc.reason) {
			t.Errorf("%s: %v; want %s", tc.name, err, tc.reason)
		}
		if p.closed != tc.closed || rt.mounts != nil {
			t.Errorf("%s: closed %d logins, want %d; created %v", tc.name, p.closed, tc.closed, rt.mounts)
		}
	}
}

func failedWith(err error, reason string) bool {
	var f *Failure
	return errors.As(err, &f) && f.Reason == reason
}

// TestProviderAttempt_StopsTheContainerOnce: what the provider engine stops
// an attempt for reaches it once, also when it came before the container.
func TestProviderAttempt_StopsTheContainerOnce(t *testing.T) {
	for _, before := range []bool{true, false} {
		engine, err := provider.NewEngine(provider.EngineConfig{Masker: newProviderMasker(&attemptMask{}, nil), Clock: providerClock{}})
		if err != nil {
			t.Fatal(err)
		}
		pa := &providerAttempt{engine: engine}
		var got []string
		lost := provider.Failure{Outcome: provider.OutcomeRefreshFailed, Detail: "credential lease"}
		if before {
			pa.stop(lost)
		}
		pa.start(context.Background(), func(f *Failure) { got = append(got, f.Reason) })
		if !before {
			pa.stop(lost)
		}
		pa.stop(provider.Failure{Outcome: provider.OutcomeTooLarge})
		if f := pa.halt(); f == nil || f.Reason != provider.ReasonCredentialRefreshFailed || !slices.Equal(got, []string{provider.ReasonCredentialRefreshFailed}) {
			t.Errorf("before=%v: stopped with %v, told %v", before, f, got)
		}
		if err := pa.close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestProviderMasker: a secret its author does not hide is neither masked nor
// looked for; a pool that masks nothing says so once for each, and never that
// a value is too short.
func TestProviderMasker(t *testing.T) {
	var notes []string
	note := func(s string) { notes = append(notes, s) }
	masked := &attemptMask{logs: true, values: true, note: note, finder: logstream.NewSecretFinder}
	m := newProviderMasker(masked, []providerSecret{{name: "open", unmasked: true}})
	if ok, err := m.Add("secret/open", []byte("an-open-value")); !ok || err != nil {
		t.Fatalf("an unmasked secret: %v, %v", ok, err)
	}
	m.Guard("secret/open", []byte("an-open-value"))
	if ok, err := m.Add("secret/db", []byte("a-hidden-value")); !ok || err != nil {
		t.Fatalf("a secret: %v, %v", ok, err)
	}
	m.Guard("secret/db", []byte("a-hidden-value"))
	if ok, err := m.Add("credential/lease/1", []byte("a-user-name")); !ok || err != nil {
		t.Fatalf("a credential's value: %v, %v", ok, err)
	}
	for result, refused := range map[string]bool{`{"v":"a-hidden-value"}`: true, `{"v":"an-open-value"}`: false, `{"v":"a-user-name"}`: false} {
		if f := masked.checkValues([]byte(result)); (f != nil) != refused {
			t.Errorf("a result of %s: %v; refused should be %v", result, f, refused)
		}
	}
	if got := masked.text("printed an-open-value and a-hidden-value"); !strings.Contains(got, "an-open-value") || strings.Contains(got, "a-hidden-value") {
		t.Fatalf("the log reads %q", got)
	}
	off := newProviderMasker(&attemptMask{note: note}, nil)
	for range 2 {
		if ok, err := off.Add("credential/lease/1", []byte("a-leased-value")); !ok || err != nil {
			t.Fatalf("masking off: %v, %v", ok, err)
		}
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "credential lease is not masked in logs") {
		t.Fatalf("notes %q", notes)
	}
	if _, err := newProviderMasker(nil, nil).Add("secret/x", []byte("v")); err == nil {
		t.Fatal("an attempt with no masker took a value")
	}
}

// TestProviderReasons_AreTheProviderEngines: the Orker reports each reason
// with the category and retry the provider engine classified it with.
func TestProviderReasons_AreTheProviderEngines(t *testing.T) {
	outcomes := []provider.Outcome{provider.OutcomeTooLarge, provider.OutcomeMaskCapacity, provider.OutcomeNotConfigured,
		provider.OutcomeContractViolation, provider.OutcomeRefreshFailed}
	for _, class := range []provider.Class{provider.Unavailable, provider.Refused, provider.NotFound, provider.Invalid} {
		for _, where := range []provider.Where{provider.AtSetup, provider.AtRefresh} {
			for _, call := range []provider.Call{provider.CallLogin, provider.CallSecret, provider.CallCredential} {
				outcomes = append(outcomes, provider.Classify(class, where, call))
			}
		}
	}
	for _, o := range outcomes {
		if CategoryFor(o.Reason) != o.Category || RetryableReason(o.Reason) != o.Retryable {
			t.Errorf("%s is %s, retried %v; the provider engine says %s, %v", o.Reason, CategoryFor(o.Reason), RetryableReason(o.Reason), o.Category, o.Retryable)
		}
	}
}

// TestStoppedByIdentity: a token or a credential that could not be kept is
// the attempt's failure when it stopped the container, or kept it from
// starting, with its own reason and never as a lost heartbeat; a container
// that ended by itself, and an attempt stopped for another reason, keep
// theirs.
func TestStoppedByIdentity(t *testing.T) {
	lost := Fail(provider.ReasonCredentialRefreshFailed, "credential lease")
	stoppedCtx := func(cause error) context.Context {
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(nil) })
		if cause != nil {
			cancel(cause)
		}
		return ctx
	}
	byIdentity := stoppedCtx(errIdentityStopped{lost})
	live := context.Background()
	for _, tc := range []struct {
		name         string
		ctx, runCtx  context.Context
		capture      *runCapture
		failure      *Failure
		wantIdentity bool
	}{
		{"while it ran", live, byIdentity, &runCapture{cancelled: true}, nil, true},
		{"before it started", live, byIdentity, &runCapture{}, FailCause(ReasonHeartbeatLost, context.Cause(byIdentity), "stopped before the start"), true},
		{"it ended by itself", live, byIdentity, &runCapture{}, nil, false},
		{"it ended by itself, failing", live, byIdentity, &runCapture{}, Fail(ReasonContainerExitNonzero, "exit 1"), false},
		{"the attempt was cancelled", stoppedCtx(context.Canceled), byIdentity, &runCapture{cancelled: true}, nil, false},
		{"nothing stopped it", live, stoppedCtx(nil), &runCapture{}, nil, false},
	} {
		got := stoppedByIdentity(tc.ctx, tc.runCtx, tc.capture, tc.failure)
		if (got == lost) != tc.wantIdentity || (got != nil) != tc.wantIdentity {
			t.Errorf("%s: %v; want the credential's failure: %v", tc.name, got, tc.wantIdentity)
		}
	}
}

// TestRunContainer_StoppedBeforeItsStart: the stop's cause crosses a container
// that never started, which is how the attempt reports it.
func TestRunContainer_StoppedBeforeItsStart(t *testing.T) {
	rt := &loginReadingRuntime{}
	d, _ := loginDispatcher(t, rt, &loginProvider{})
	root := t.TempDir()
	ws, err := NewWorkspace(root)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancelCause(context.Background())
	lost := Fail(provider.ReasonCredentialRefreshFailed, "credential lease")
	stop(errIdentityStopped{lost})
	capture, failure := d.runContainer(runCtx, loginInput(), ws, "c1", &Heartbeater{}, io.Discard, io.Discard, d.newAttemptMask(nil))
	if got := stoppedByIdentity(context.Background(), runCtx, capture, failure); got != lost {
		t.Fatalf("stopped with %v (the run said %v); want the credential's failure", got, failure)
	}
}

// TestProviderUse_ARefusalNamesNoPrincipal: what this Orker refuses is stored
// in history, so its message holds no principal, and nothing a provider said.
func TestProviderUse_ARefusalNamesNoPrincipal(t *testing.T) {
	for name, edit := range map[string]func(*OCIDispatcher, *orkstackv1.ExecuteTaskInput){
		"outside the floor": func(d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
			d.Policy.SecretAllow = []string{"env:*"}
			in.Identity.Credentials[0].Path = ""
		},
		"a principal with no policy string": func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
			in.Identity.Credentials[0].Path, in.Identity.Grants[0].Principal = "", "payments-app/../admin"
		},
		"a path its provider refuses": func(_ *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) { in.Identity.Credentials[0].Path = "nope/x" },
	} {
		d, _ := loginDispatcher(t, &loginReadingRuntime{}, &loginProvider{})
		in := loginInput()
		edit(d, in)
		_, f := d.providerUseOf(in)
		if f == nil || strings.Contains(f.Message, "payments-app") || strings.Contains(f.Message, "not a path of this type") {
			t.Errorf("%s: %v", name, f)
		}
	}
}

// TestAttemptMask_ALongTasksRefreshesDoNotOutgrowTheCheck: a token replaced
// and past its grace is no longer looked for in values.json, so a task that
// runs for days can still return a result; the one in place is.
func TestAttemptMask_ALongTasksRefreshesDoNotOutgrowTheCheck(t *testing.T) {
	m := &attemptMask{logs: true, values: true, note: func(string) {}, finder: logstream.NewSecretFinder}
	token := func(n int) string {
		return "eyJhbGciOiJFUzI1NiJ9." + strings.Repeat("claims", 100) + strconv.Itoa(n) + ".signature-" + strconv.Itoa(n)
	}
	const refreshes = 1000
	for n := 1; n <= refreshes; n++ {
		if _, err := m.Add("identity:aws:"+strconv.Itoa(n), []byte(token(n))); err != nil {
			t.Fatalf("refresh %d: %v", n, err)
		}
		m.Drop("identity:aws:" + strconv.Itoa(n-1))
	}
	if f := m.checkValues([]byte(`{"rows":3}`)); f != nil {
		t.Fatalf("a result after %d refreshes: %v", refreshes, f)
	}
	if f := m.checkValues([]byte(`{"leak":"` + token(refreshes) + `"}`)); f == nil || f.Reason != ReasonValuesContainSecret {
		t.Fatalf("the token in place, returned: %v", f)
	}
}

// TestGrantTokens: at setup one mint answers every grant, each token is
// handed out once and only while young, and a login tried again gets a fresh
// one; at a refresh a grant asks for its own alone; one grant's refusal is
// not another's; and an answer that is not what was asked is asked again.
func TestGrantTokens(t *testing.T) {
	ctx := context.Background()
	asks := map[string]*orkstackv1.IdentityGrant{}
	for _, name := range []string{"a", "b", "c"} {
		asks[name] = &orkstackv1.IdentityGrant{Name: name, Provider: "vault", Principal: "p-" + name, Audience: "vault.internal"}
	}
	newTokens := func() (*grantTokens, *[]string) {
		d, asked := loginDispatcher(t, &loginReadingRuntime{}, &loginProvider{})
		return newGrantTokens(d, loginInput(), 1, asks), asked
	}
	tokens, asked := newTokens()
	for _, name := range []string{"b", "a", "c"} {
		tok, life, err := tokens.source(name)(ctx)
		if err != nil || tok != "eyJhbGciOiJFUzI1NiJ9.for-grant:"+name+".sig" || life <= 0 || life > time.Hour {
			t.Fatalf("grant %s: %q, %s, %v", name, tok, life, err)
		}
	}
	if want := []string{"grant:b@vault.internal", "grant:a@vault.internal", "grant:c@vault.internal"}; !slices.Equal(*asked, want) {
		t.Fatalf("three logins asked %v; want one mint of %v", *asked, want)
	}
	if _, _, err := tokens.source("a")(ctx); err != nil || len(*asked) != 6 {
		t.Fatalf("a login tried again: %v, asked %v; want a fresh mint", err, *asked)
	}
	// What waited too long is not handed out.
	b := tokens.unused["b"]
	b.at -= grantTokenAge + time.Second
	tokens.unused["b"] = b
	if _, _, err := tokens.source("b")(ctx); err != nil || len(*asked) <= 6 {
		t.Fatalf("an old token: %v, asked %v", err, *asked)
	}

	tokens, asked = newTokens()
	tokens.running.Store(true)
	if _, _, err := tokens.source("a")(ctx); err != nil || !slices.Equal(*asked, []string{"grant:a@vault.internal"}) {
		t.Fatalf("a refresh asked %v, %v; want its own grant alone", *asked, err)
	}

	// A login whose context ends stops waiting for another's mint.
	tokens, _ = newTokens()
	tokens.turn <- struct{}{}
	ended, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := tokens.source("a")(ended); !errors.Is(err, context.Canceled) {
		t.Fatalf("a login that was cancelled while another minted: %v", err)
	}
	<-tokens.turn
	// A token that is too old once it is there is no token.
	if _, _, err := tokens.source("b")(ctx); err != nil {
		t.Fatal(err)
	}
	slow := tokens.d.identityMint
	tokens.d.identityMint = func(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		resp, err := slow(ctx, req)
		for _, tok := range resp.GetTokens() {
			tok.ExpiresInSeconds = 1
		}
		time.Sleep(1100 * time.Millisecond) // past the life it answers
		return resp, err
	}
	tokens.running.Store(true)
	tokens.clear()
	if tok, _, err := tokens.source("a")(ctx); err == nil || tok != "" {
		t.Fatalf("a token that lapsed as it was minted: %q, %v", tok, err)
	}
	if len(tokens.unused) != 0 {
		t.Fatalf("%d tokens are kept past the setup", len(tokens.unused))
	}

	tokens, _ = newTokens()
	mint := tokens.d.identityMint
	tokens.d.identityMint = func(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		for _, tok := range req.GetTokens() {
			if tok.GetName() == "grant:b" {
				return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("IDENTITY_REVOKED: grant \"b\" is gone"))
			}
		}
		return mint(ctx, req)
	}
	if _, _, err := tokens.source("a")(ctx); err != nil {
		t.Fatalf("grant a beside a revoked b: %v", err)
	}
	var refused *provider.TokenRefused
	if _, _, err := tokens.source("b")(ctx); !errors.As(err, &refused) || refused.Reason != "IDENTITY_REVOKED" {
		t.Fatalf("the revoked grant: %v", err)
	}

	for name, edit := range map[string]func(*orkstackv1.MintAttemptTokensResponse){
		"none":             func(r *orkstackv1.MintAttemptTokensResponse) { r.Tokens = nil },
		"one short":        func(r *orkstackv1.MintAttemptTokensResponse) { r.Tokens = r.Tokens[:2] },
		"one twice":        func(r *orkstackv1.MintAttemptTokensResponse) { r.Tokens[1] = r.Tokens[0] },
		"another's":        func(r *orkstackv1.MintAttemptTokensResponse) { r.Tokens[0].Name = "grant:z" },
		"another audience": func(r *orkstackv1.MintAttemptTokensResponse) { r.Tokens[0].Audience = "sts" },
		"no life":          func(r *orkstackv1.MintAttemptTokensResponse) { r.Tokens[0].ExpiresInSeconds = 0 },
	} {
		tokens, _ := newTokens()
		mint := tokens.d.identityMint
		tokens.d.identityMint = func(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
			resp, err := mint(ctx, req)
			edit(resp)
			return resp, err
		}
		if _, _, err := tokens.source("a")(ctx); err == nil || errors.As(err, &refused) {
			t.Errorf("an answer with %s: %v; want it asked again", name, err)
		}
	}
}

// TestDescribeOrker_ReportsItsProviders: what submit compares across a pool's
// Orkers, by name and type, and nothing else a declaration holds.
func TestDescribeOrker_ReportsItsProviders(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	set, err := provider.ParseWith([]byte("providers:\n  zulu:\n    type: vault\n    address: https://vault.internal:8200\n    audience: v\n  aws:\n    type: aws\n    region: eu-west-1\n"), types.All())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		set  provider.Set
		want []string
	}{{provider.Set{}, nil}, {set, []string{"aws/aws", "zulu/vault"}}} {
		a := NewActivitiesWithOptions(store, Options{Runtime: &reapRecorder{}, WorkRoot: t.TempDir()})
		a.Dispatcher().SetProviders(tc.set, 1, time.Second)
		got, err := a.DescribeOrker(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, p := range got.GetProviders() {
			names = append(names, p.GetName()+"/"+p.GetType())
			if p.GetImageDigest() != "" || len(p.GetContracts()) != 0 {
				t.Errorf("%s reports %v; only an image provider has a digest", p.GetName(), p)
			}
		}
		if !slices.Equal(names, tc.want) {
			t.Errorf("providers = %v, want %v", names, tc.want)
		}
	}
}
