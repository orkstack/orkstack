package api

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// mintService is a service whose mint reads a real local record store
// holding one admission, of run ork-1 whose task build holds identity aws
// at pool main. Its describe fails, so a mint that passes step 3 is
// answered unavailable at step 4: what these tests hold is steps 1 and 3.
func mintService(t *testing.T, roles []*orkstackv1.Role, bindings ...*orkstackv1.Binding) (*Service, *orkstackv1.MintAttemptTokensRequest, *artifacts.MemoryAuditStore) {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	adm := artifacts.Admission{RunID: "ork-1", Namespace: "default", Orkflow: "deploy", OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: artifacts.Starter{Kind: artifacts.StarterEngine},
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(),
		EngineBuild: "b7", SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, Issuer: "https://ork.example",
		NamespaceID: "ns_default", OrkflowID: "of_1", TokenTTL: 3600,
		Sets:  []artifacts.IdentitySet{{Identities: []artifacts.AdmittedIdentity{{Name: "aws", Audience: "sts", TTLSeconds: 900, Refresh: "required"}}}},
		Tasks: map[string]artifacts.AdmittedTask{"build": {Set: 1, Pool: "main", SpecSHA256: strings.Repeat("a", 64), IdentitySlots: 1, AttemptDeadline: 3600}}}
	raw, sha, err := artifacts.EncodeAdmission(adm)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := artifacts.AdmissionKey("default", "ork-1", sha)
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, key)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, key), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	audits := artifacts.NewMemoryAuditStore()
	s := NewService(Config{AuditStore: audits, Minter: &identity.Minter{Stores: store, Issuer: "https://ork.example",
		Describe: func(context.Context, string) (identity.ExecutionFacts, error) {
			return identity.ExecutionFacts{}, errors.New("frontend down")
		}}})
	a, _ := accessFor(t, authn.ModeEnforce, roles, bindings...)
	s.access = a
	req := &orkstackv1.MintAttemptTokensRequest{Namespace: "default", RunId: "ork-1", TaskId: "build", Attempt: 1,
		TaskSpecSha256: strings.Repeat("a", 64), AdmissionSha256: sha, Tokens: []*orkstackv1.TokenRequest{{Name: "aws", Audience: "sts"}}}
	return s, req, audits
}

var anOrker = &authn.Principal{Kind: authn.KindService, ID: "serviceaccount:orker-1", Issuer: "serviceaccount", Method: authn.MethodServiceAccount, OrkerID: "orker-1"}

func asOrker(mode string, p *authn.Principal) context.Context {
	return authn.WithPrincipal(authn.WithMode(context.Background(), mode), p)
}

func refusedRecords(t *testing.T, audits *artifacts.MemoryAuditStore) int {
	t.Helper()
	page, err := audits.ListAuditRecords(context.Background(), artifacts.AuditQuery{Limit: 100, Until: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range page.Records {
		if r.GetAction() == "identity.token.refused" {
			n++
		}
	}
	return n
}

// TestMayMint: the mint answers only in enforce mode and to a credential
// that names an Orker; the caller must hold identity.token.mint and
// task.result.commit at the pool the admission gives the task, never at
// another; a denial writes one audit record.
func TestMayMint(t *testing.T) {
	both := []*orkstackv1.Role{{Name: "mints", Permissions: []string{"identity.token.mint", "task.result.commit"}}}
	mintOnly := []*orkstackv1.Role{{Name: "mints", Permissions: []string{"identity.token.mint"}}}
	at := func(scope string) *orkstackv1.Binding {
		return &orkstackv1.Binding{Name: "b", Role: "mints", Scope: scope, Subjects: []string{"serviceaccount:orker-1"}}
	}
	for _, tc := range []struct {
		name    string
		roles   []*orkstackv1.Role
		binding *orkstackv1.Binding
		ctx     context.Context
		code    connect.Code
		reason  string
		audited int
	}{
		{"observe mode", both, at("pool:main"), asOrker(authn.ModeObserve, anOrker), connect.CodeFailedPrecondition, identity.ReasonEngineRefused, 0},
		{"a credential naming no Orker", both, at("pool:main"), asOrker(authn.ModeEnforce, &authn.Principal{Kind: authn.KindService, ID: "serviceaccount:x", Issuer: "serviceaccount", Method: authn.MethodServiceAccount}),
			connect.CodeFailedPrecondition, identity.ReasonEngineRefused, 1},
		{"both at the task's pool", both, at("pool:main"), asOrker(authn.ModeEnforce, anOrker), connect.CodeUnavailable, identity.ReasonUnavailable, 0},
		{"both at the deployment", both, at("deployment"), asOrker(authn.ModeEnforce, anOrker), connect.CodeUnavailable, identity.ReasonUnavailable, 0},
		{"both at another pool", both, at("pool:gpu"), asOrker(authn.ModeEnforce, anOrker), connect.CodePermissionDenied, "", 1},
		{"mint without commit", mintOnly, at("pool:main"), asOrker(authn.ModeEnforce, anOrker), connect.CodePermissionDenied, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, req, audits := mintService(t, tc.roles, tc.binding)
			_, err := s.MintAttemptTokens(tc.ctx, connect.NewRequest(req))
			if connect.CodeOf(err) != tc.code || (tc.reason != "" && !strings.Contains(err.Error(), tc.reason)) {
				t.Fatalf("err = %v, want %s %s", err, tc.code, tc.reason)
			}
			if got := refusedRecords(t, audits); got != tc.audited {
				t.Fatalf("audit records = %d, want %d", got, tc.audited)
			}
		})
	}
	s, req, _ := mintService(t, both, at("pool:main"))
	s.minter = nil
	if _, err := s.MintAttemptTokens(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req)); !strings.Contains(errString(err), identity.ReasonEngineRefused) {
		t.Fatalf("no minter: %v", err)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestMint_TheInterceptorAdmitsTheHandler: the access interceptor asks the
// mint's permissions of nobody, so it must still let an authenticated
// Orker reach the handler, which asks them at the admitted pool; an
// unauthenticated caller is refused before it.
func TestMint_TheInterceptorAdmitsTheHandler(t *testing.T) {
	s, req, _ := mintService(t, nil)
	if _, err := s.access.decide(asOrker(authn.ModeEnforce, anOrker), s, orkstackv1connect.IdentityServiceMintAttemptTokensProcedure, req); err != nil {
		t.Fatalf("an authenticated Orker is refused before the handler: %v", err)
	}
	if _, err := s.access.decide(authn.WithMode(context.Background(), authn.ModeEnforce), s, orkstackv1connect.IdentityServiceMintAttemptTokensProcedure, req); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("an unauthenticated caller: %v", err)
	}
}

// TestMint_ABadRequestIsRefusedBeforeAnythingIsRead: a request whose names
// or ids are not shaped as they must be is invalid_argument, and nothing it
// carries reaches an audit record.
func TestMint_ABadRequestIsRefusedBeforeAnythingIsRead(t *testing.T) {
	both := []*orkstackv1.Role{{Name: "mints", Permissions: []string{"identity.token.mint", "task.result.commit"}}}
	for name, edit := range map[string]func(*orkstackv1.MintAttemptTokensRequest){
		"a task id that is no name": func(r *orkstackv1.MintAttemptTokensRequest) { r.TaskId = "Build\n" },
		"a long run id":             func(r *orkstackv1.MintAttemptTokensRequest) { r.RunId = strings.Repeat("r", 65) },
		"a long audience":           func(r *orkstackv1.MintAttemptTokensRequest) { r.Tokens[0].Audience = strings.Repeat("a", 256) },
	} {
		s, req, audits := mintService(t, both, &orkstackv1.Binding{Name: "b", Role: "mints", Scope: "pool:main", Subjects: []string{"serviceaccount:orker-1"}})
		edit(req)
		if _, err := s.MintAttemptTokens(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
		if n := refusedRecords(t, audits); n != 0 {
			t.Errorf("%s: %d audit records", name, n)
		}
	}
}

type countedMints map[string]int

func (c countedMints) Minted(reason string) { c[reason]++ }

// TestMint_IsCounted: each mint is counted once under its outcome.
func TestMint_IsCounted(t *testing.T) {
	s, req, _ := mintService(t, []*orkstackv1.Role{{Name: "mints", Permissions: []string{"identity.token.mint", "task.result.commit"}}},
		&orkstackv1.Binding{Name: "b", Role: "mints", Scope: "pool:gpu", Subjects: []string{"serviceaccount:orker-1"}})
	counted := countedMints{}
	s.mintObserver = counted
	s.MintAttemptTokens(asOrker(authn.ModeObserve, anOrker), connect.NewRequest(req))
	s.MintAttemptTokens(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req))
	bad := proto.Clone(req).(*orkstackv1.MintAttemptTokensRequest)
	bad.TaskId = "Bad"
	s.MintAttemptTokens(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(bad))
	if counted[identity.ReasonEngineRefused] != 1 || counted[identity.OutcomePermissionDenied] != 1 || counted[identity.OutcomeInvalid] != 1 {
		t.Fatalf("counted = %v", counted)
	}
}

// TestAttemptKey: one attempt's budget is named by value, so two requests
// for the same item share it and two items do not.
func TestAttemptKey(t *testing.T) {
	one, again, other := int32(3), int32(3), int32(4)
	key := func(i *int32) string {
		return attemptKey(mintRequestOf(&orkstackv1.MintAttemptTokensRequest{RunId: "ork-1", TaskId: "build", Attempt: 1, Item: i}))
	}
	if key(&one) != key(&again) || key(&one) == key(&other) || key(nil) != "default/ork-1/build/-/1" {
		t.Fatalf("keys: %q %q %q %q", key(&one), key(&again), key(&other), key(nil))
	}
}
