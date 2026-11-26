package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestGetSignin_AnswersTheCallerItself: any authenticated caller learns who
// it is, how it was known, and what it holds, with only the subject of each
// binding that matched it; nobody is answered nothing.
func TestGetSignin_AnswersTheCallerItself(t *testing.T) {
	s, _ := accessWriter(t)
	h := authHandler{svc: s}
	resp, err := h.GetSignin(asNina(), connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	m := resp.Msg
	if m.GetPrincipalId() != "user:corp:nina" || m.GetMethod() != authn.MethodJWT || m.GetIssuer() != "corp" || m.GetKind() != "user" {
		t.Fatalf("identity = %v", m)
	}
	if len(m.GetBindings()) != 1 || m.GetBindings()[0].GetBinding() != "payments-admins" || m.GetBindings()[0].GetSubject() != "user:corp:nina" ||
		m.GetBindings()[0].GetRole() != authz.RoleNamespaceAdmin {
		t.Fatalf("bindings = %v; want payments-admins alone", m.GetBindings())
	}
	if len(m.GetPermissions()) != 1 || m.GetPermissions()[0].GetScope() != "namespace:payments" || len(m.GetPermissions()[0].GetPermissions()) == 0 {
		t.Fatalf("permissions = %v", m.GetPermissions())
	}
	if m.GetAccessRevision() == 0 || m.GetSignin() != nil || m.GetCsrfToken() != "" {
		t.Fatalf("revision %d, signin %v: want the document's revision and no sign-in", m.GetAccessRevision(), m.GetSignin())
	}
	if _, err := h.GetSignin(authn.WithMode(asNina(), authn.ModeEnforce), connect.NewRequest(&orkstackv1.GetSigninRequest{})); err != nil {
		t.Fatal(err)
	}
	if m.GetOrkerId() != "" {
		t.Fatalf("a person's credential names the Orker %q", m.GetOrkerId())
	}
	orkerCtx := authn.WithPrincipal(t.Context(), &authn.Principal{Kind: authn.KindService, ID: "service:dev:orker-1",
		Method: authn.MethodJWT, Issuer: "dev", OrkerID: "orker-1"})
	resp, err = h.GetSignin(orkerCtx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil || resp.Msg.GetOrkerId() != "orker-1" {
		t.Fatalf("an Orker's credential = %v, %v; want it to name orker-1", resp, err)
	}
	anonymous := authn.WithMode(t.Context(), authn.ModeObserve)
	if _, err := h.GetSignin(anonymous, connect.NewRequest(&orkstackv1.GetSigninRequest{})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("an anonymous caller = %v; want unauthenticated", err)
	}
}

// scriptedImagePolicies answers a deployment's image policy, or that it cannot be read,
// which a real store does not on demand.
type scriptedImagePolicies struct {
	artifacts.ImagePolicyStore
	policy *orkstackv1.ImagePolicy
	err    error
	hung   bool
	reads  int
}

func (s *scriptedImagePolicies) Get(ctx context.Context) (*orkstackv1.ImagePolicy, error) {
	s.reads++
	if s.hung {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.policy == nil && s.err == nil {
		return nil, artifacts.ErrNotFound
	}
	return s.policy, s.err
}

// TestGetSignin_TheDeploymentsImagePolicy: only a credential that names an Orker and
// holds eventsource.host is told what every image is held to, without its signers; it
// is read for each ask, and one that could not be read is told apart from none.
func TestGetSignin_TheDeploymentsImagePolicy(t *testing.T) {
	policies := &scriptedImagePolicies{}
	h := hostsBound(t, policies)
	ask := func(id, orker string) *orkstackv1.ImagePolicySelection {
		t.Helper()
		return policyTold(t, h, t.Context(), authn.ModeEnforce, id, orker)
	}
	if got := ask("user:dev:orker-1", "orker-1"); got == nil || got.GetRequireDigest() || len(got.GetAllowedRegistries()) != 0 {
		t.Errorf("a deployment with none = %v; want one that is set and empty", got)
	}
	policies.policy = &orkstackv1.ImagePolicy{RequireDigest: true, AllowedRegistries: []string{"ghcr.io/acme"}}
	got := ask("user:dev:orker-1", "orker-1")
	if !got.GetRequireDigest() || !slices.Equal(got.GetAllowedRegistries(), []string{"ghcr.io/acme"}) || got.GetRequireSignedBy() != "" || got.GetSigningKeyPem() != "" {
		t.Errorf("after it was set = %v", got)
	}
	for name, who := range map[string][2]string{
		"no Orker is named":      {"user:dev:unnamed", ""},
		"any Orker id":           {"user:dev:shared", "orker-2"},
		"an administrator":       {"user:corp:alice", ""},
		"an Orker bound to less": {"user:dev:viewer", "orker-3"},
		"an Orker bound nowhere": {"user:dev:nobody", "orker-4"},
		"a disabled Orker":       {"user:dev:disabled", "orker-5"},
	} {
		if got := ask(who[0], who[1]); got != nil {
			t.Errorf("%s: told %v", name, got)
		}
	}
	if policies.reads != 2 {
		t.Errorf("the policy was read %d times for the two asks it was answered to", policies.reads)
	}
	// Its credential is verified in observe too.
	if got := policyTold(t, h, t.Context(), authn.ModeObserve, "user:dev:orker-1", "orker-1"); !got.GetRequireDigest() {
		t.Errorf("in observe = %v", got)
	}
	policies.err = errors.New("the bucket is gone")
	if got := ask("user:dev:orker-1", "orker-1"); got != nil {
		t.Errorf("one that could not be read = %v; want it unset", got)
	}
	h.svc.imagePolicies = nil
	if got := ask("user:dev:orker-1", "orker-1"); got != nil {
		t.Errorf("an engine that has no store = %v; want it unset", got)
	}
}

// TestGetSignin_AStoreThatDoesNotAnswer: an Orker boots on this answer, so it comes
// inside the Orker's own wait, with the policy unset; an outage every Orker asks
// through is a line a second.
func TestGetSignin_AStoreThatDoesNotAnswer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := hostsBound(t, &scriptedImagePolicies{hung: true})
		var lines bytes.Buffer
		h.svc.logger = slog.New(slog.NewTextHandler(&lines, nil))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		start := time.Now()
		if got := policyTold(t, h, ctx, authn.ModeEnforce, "user:dev:orker-1", "orker-1"); got != nil || ctx.Err() != nil || time.Since(start) > 2*time.Second {
			t.Errorf("%v after %s, %v", got, time.Since(start), ctx.Err())
		}
		h.svc.imagePolicies = &scriptedImagePolicies{err: errors.New("the bucket is gone")}
		for range 5 {
			policyTold(t, h, ctx, authn.ModeEnforce, "user:dev:orker-1", "orker-1")
		}
		if n := strings.Count(lines.String(), "level=WARN"); n != 1 {
			t.Errorf("%d lines:\n%s", n, &lines)
		}
		time.Sleep(time.Second)
		policyTold(t, h, ctx, authn.ModeEnforce, "user:dev:orker-1", "orker-1")
		if n := strings.Count(lines.String(), "level=WARN"); n != 2 || !strings.Contains(lines.String(), "more_not_logged=5") {
			t.Errorf("a second later, %d lines:\n%s", n, &lines)
		}
	})
}

// hostsBound is the sign-in service over policies, with Orkers and others bound.
func hostsBound(t *testing.T, policies artifacts.ImagePolicyStore) authHandler {
	t.Helper()
	store := artifacts.NewAccessStoreOn(artifacts.NewMemoryCAS())
	_, err := store.Write(t.Context(), &orkstackv1.AccessDocument{SchemaVersion: 1, Disabled: []string{"user:dev:disabled"}, Bindings: []*orkstackv1.Binding{
		{Name: "orkers", Role: authz.RoleOrker, Scope: "pool:main", Subjects: []string{"user:dev:orker-1", "user:dev:unnamed", "user:dev:disabled"}},
		{Name: "shared", Role: authz.RoleOrker, Scope: "pool:main", OrkerIdsAny: true, Subjects: []string{"user:dev:shared"}},
		{Name: "admins", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:alice"}},
		{Name: "viewers", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:dev:viewer"}},
	}}, nil, artifacts.AccessWrite{At: time.Now()})
	must.OK(t, err)
	s, _ := accessWriter(t)
	s.imagePolicies = policies
	s.access = &AccessControl{Mode: authn.ModeEnforce, Policies: NewPolicyCache(store, PolicyCacheOptions{})}
	_, err = s.access.Policies.Policy(t.Context())
	must.OK(t, err)
	return authHandler{svc: s}
}

// policyTold is the image policy GetSignin tells a credential.
func policyTold(t *testing.T, h authHandler, ctx context.Context, mode, id, orker string) *orkstackv1.ImagePolicySelection {
	t.Helper()
	ctx = authn.WithMode(authn.WithPrincipal(ctx, &authn.Principal{Kind: authn.KindUser, ID: id, Method: authn.MethodJWT, Issuer: "dev", OrkerID: orker}), mode)
	resp, err := h.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	must.OK(t, err)
	return resp.Msg.GetImagePolicy()
}
