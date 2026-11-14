package api

import (
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
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
	anonymous := authn.WithMode(t.Context(), authn.ModeObserve)
	if _, err := h.GetSignin(anonymous, connect.NewRequest(&orkstackv1.GetSigninRequest{})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("an anonymous caller = %v; want unauthenticated", err)
	}
}
