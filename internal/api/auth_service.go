package api

import (
	"context"
	"errors"
	"slices"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
)

// authServiceServed are the AuthService procedures this release serves; the
// sign-in ones are P9's and stay unmounted until then.
var authServiceServed = []string{
	orkstackv1connect.AuthServiceGetSigninProcedure,
}

type authHandler struct {
	orkstackv1connect.UnimplementedAuthServiceHandler
	svc *Service
}

func mountAuthService(svc *Service, mux reservedMux, opts []connect.HandlerOption) {
	_, handler := orkstackv1connect.NewAuthServiceHandler(authHandler{svc: svc}, opts...)
	for _, procedure := range authServiceServed {
		mux.Handle(procedure, handler)
	}
}

// GetSignin answers the caller itself: who it is, how it was known, and
// what it holds, against this replica's copy of the access document. It
// needs no permission. Each binding shows only the subject that matched the
// caller: a binding's other subjects need access.read. The sign-in fields
// stay empty until sign-in exists.
func (h authHandler) GetSignin(ctx context.Context, _ *connect.Request[orkstackv1.GetSigninRequest]) (*connect.Response[orkstackv1.GetSigninResponse], error) {
	p := authn.FromContext(ctx)
	if p == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("an authenticated caller is required"))
	}
	out := &orkstackv1.GetSigninResponse{PrincipalId: p.ID, Display: p.Display, Issuer: p.Issuer, Method: p.Method,
		Kind: string(p.Kind), Groups: slices.Clone(p.Groups), OrkerId: p.OrkerID}
	policy, _ := h.svc.access.policy(ctx)
	if policy == nil {
		return connect.NewResponse(out), nil
	}
	out.AccessRevision = policy.Revision
	byScope := map[string]*orkstackv1.ScopedPermissions{}
	for _, held := range policy.Holdings(subjectOf(p)) {
		out.Bindings = append(out.Bindings, explainedProto(held.Binding))
		sp, ok := byScope[held.Binding.Scope]
		if !ok {
			sp = &orkstackv1.ScopedPermissions{Scope: held.Binding.Scope}
			byScope[held.Binding.Scope] = sp
			out.Permissions = append(out.Permissions, sp)
		}
		for _, perm := range held.Permissions {
			if !slices.Contains(sp.Permissions, string(perm)) {
				sp.Permissions = append(sp.Permissions, string(perm))
			}
		}
	}
	for _, sp := range out.Permissions {
		slices.Sort(sp.Permissions)
	}
	slices.SortFunc(out.Permissions, func(a, b *orkstackv1.ScopedPermissions) int {
		switch {
		case a.Scope < b.Scope:
			return -1
		case a.Scope > b.Scope:
			return 1
		}
		return 0
	})
	return connect.NewResponse(out), nil
}
