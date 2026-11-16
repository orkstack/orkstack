package api

import (
	"context"
	"errors"
	"net/http"
	"slices"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// authServiceServed are the AuthService procedures every authenticating
// engine serves; signinServed are those it serves where an issuer offers
// sign-in.
var (
	authServiceServed = []string{
		orkstackv1connect.AuthServiceGetAuthConfigProcedure,
		orkstackv1connect.AuthServiceGetSigninProcedure,
	}
	signinServed = []string{
		orkstackv1connect.AuthServiceRefreshSigninProcedure,
		orkstackv1connect.AuthServiceSignOutProcedure,
	}
)

type authHandler struct {
	orkstackv1connect.UnimplementedAuthServiceHandler
	svc *Service
}

func mountAuthService(svc *Service, mux reservedMux, opts []connect.HandlerOption) {
	_, handler := orkstackv1connect.NewAuthServiceHandler(authHandler{svc: svc}, opts...)
	for _, procedure := range authServiceServed {
		mux.Handle(procedure, handler)
	}
	if svc.access.Signins != nil {
		for _, procedure := range signinServed {
			mux.Handle(procedure, handler)
		}
	}
}

// GetAuthConfig is public: the issuers a person can sign in with, and the
// client a terminal signs in as.
func (h authHandler) GetAuthConfig(context.Context, *connect.Request[orkstackv1.GetAuthConfigRequest]) (*connect.Response[orkstackv1.GetAuthConfigResponse], error) {
	a := h.svc.access
	out := &orkstackv1.GetAuthConfigResponse{Mode: a.Mode}
	if a.Signins == nil {
		return connect.NewResponse(out), nil
	}
	for _, name := range a.Signins.Issuers.Names() {
		i, _ := a.Signins.Issuers.ByName(name)
		if i.Login == nil {
			continue
		}
		out.Issuers = append(out.Issuers, &orkstackv1.SigninIssuer{Name: name, IssuerUrl: i.Issuer,
			LoginPath: "/v1/auth/login/" + name, CliClientId: i.Login.CLIClientID,
			Scopes: slices.Clone(i.Login.Scopes), DeviceFlow: i.Login.DeviceFlow})
	}
	return connect.NewResponse(out), nil
}

// signinError answers a sign-in's failure in the words the page reads: an
// ended sign-in is unauthenticated SIGNIN_ENDED; the store or the issuer
// not answering is unavailable.
func signinError(err error) error {
	switch {
	case errors.Is(err, authn.ErrNoRefreshToken):
		return connect.NewError(connect.CodeUnauthenticated, authn.ErrNoRefreshToken)
	case errors.Is(err, authn.ErrSigninEnded):
		return connect.NewError(connect.CodeUnauthenticated, authn.ErrSigninEnded)
	}
	// The issuer's answer and the store's names stay in the engine's log.
	if errors.Is(err, authn.ErrIssuerUnavailable) {
		return connect.NewError(connect.CodeUnavailable, authn.ErrIssuerUnavailable)
	}
	return connect.NewError(connect.CodeUnavailable, authn.ErrSigninUnavailable)
}

// RefreshSignin revalidates the caller's sign-in with its issuer.
func (h authHandler) RefreshSignin(ctx context.Context, _ *connect.Request[orkstackv1.RefreshSigninRequest]) (*connect.Response[orkstackv1.RefreshSigninResponse], error) {
	view := authn.SigninFrom(ctx)
	if view == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("the caller is not signed in with a cookie"))
	}
	next, err := h.svc.access.Signins.Refresh(ctx, view)
	if err != nil {
		h.svc.logger.Warn("sign-in refresh failed", "signin", view.PublicID, "error", err)
		return nil, signinError(err)
	}
	return connect.NewResponse(&orkstackv1.RefreshSigninResponse{ValidatedUntil: timestamppb.New(next.ValidatedUntil),
		IdleExpiresAt: timestamppb.New(next.IdleExpiresAt), ExpiresAt: timestamppb.New(next.ExpiresAt)}), nil
}

// SignOut ends the caller's sign-in and clears its cookie.
func (h authHandler) SignOut(ctx context.Context, _ *connect.Request[orkstackv1.SignOutRequest]) (*connect.Response[orkstackv1.SignOutResponse], error) {
	view := authn.SigninFrom(ctx)
	if view == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("the caller is not signed in with a cookie"))
	}
	end, err := h.svc.access.Signins.SignOut(ctx, view)
	if err != nil {
		h.svc.logger.Warn("sign-out failed; the sign-in stands", "signin", view.PublicID, "error", err)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the sign-out could not be recorded now; try again"))
	}
	resp := connect.NewResponse(&orkstackv1.SignOutResponse{EndSessionUrl: end})
	resp.Header().Add("Set-Cookie", (&http.Cookie{Name: authn.SigninCookie, Value: "", Path: "/", Secure: true,
		HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: -1}).String())
	return resp, nil
}

// GetSignin answers the caller itself: who it is, how it was known, and
// what it holds, against this replica's copy of the access document. It
// needs no permission. Each binding shows only the subject that matched the
// caller: a binding's other subjects need access.read. For a caller signed
// in with a cookie it also answers the sign-in and its CSRF token.
func (h authHandler) GetSignin(ctx context.Context, _ *connect.Request[orkstackv1.GetSigninRequest]) (*connect.Response[orkstackv1.GetSigninResponse], error) {
	p := authn.FromContext(ctx)
	if p == nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("an authenticated caller is required"))
	}
	out := &orkstackv1.GetSigninResponse{PrincipalId: p.ID, Display: p.Display, Issuer: p.Issuer, Method: p.Method,
		Kind: string(p.Kind), Groups: slices.Clone(p.Groups), OrkerId: p.OrkerID}
	if v := authn.SigninFrom(ctx); v != nil {
		out.Signin = &orkstackv1.Signin{PublicId: v.PublicID, PrincipalId: p.ID, CreatedAt: timestamppb.New(v.CreatedAt),
			ExpiresAt: timestamppb.New(v.ExpiresAt), ValidatedUntil: timestamppb.New(v.ValidatedUntil),
			IdleExpiresAt: timestamppb.New(v.IdleExpiresAt)}
		out.CsrfToken = v.CSRF
	}
	policy, unavailable := h.svc.access.policy(ctx)
	if unavailable {
		h.svc.logger.Warn("whoami: the access document could not be read; the answer may show fewer bindings than the caller holds",
			"principal", p.ID)
	}
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
