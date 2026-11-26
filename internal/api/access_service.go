package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// accessServiceServed are the AccessService procedures this release serves.
// Every other one stays unmounted, answering 404, until its handler lands.
var accessServiceServed = []string{
	orkstackv1connect.AccessServiceGetAccessProcedure,
	orkstackv1connect.AccessServiceListPermissionsProcedure,
	orkstackv1connect.AccessServiceLowerAuthModeProcedure,
	orkstackv1connect.AccessServiceCreateRoleProcedure,
	orkstackv1connect.AccessServiceUpdateRoleProcedure,
	orkstackv1connect.AccessServiceDeleteRoleProcedure,
	orkstackv1connect.AccessServiceCreateBindingProcedure,
	orkstackv1connect.AccessServiceUpdateBindingProcedure,
	orkstackv1connect.AccessServiceDeleteBindingProcedure,
	orkstackv1connect.AccessServiceCreateServiceAccountProcedure,
	orkstackv1connect.AccessServiceUpdateServiceAccountProcedure,
	orkstackv1connect.AccessServiceDeleteServiceAccountProcedure,
	orkstackv1connect.AccessServiceSetPrincipalDisabledProcedure,
	orkstackv1connect.AccessServiceExplainAccessProcedure,
	orkstackv1connect.AccessServiceListAccessRevisionsProcedure,
	orkstackv1connect.AccessServiceGetAccessRevisionProcedure,
	orkstackv1connect.AccessServiceAcknowledgeAccessRevisionProcedure,
	orkstackv1connect.AccessServiceListPrincipalsProcedure,
	orkstackv1connect.AccessServiceListSigninsProcedure,
	orkstackv1connect.AccessServiceRevokeSigninsProcedure,
}

// accessHandler serves them; the rest of AccessService is unimplemented.
type accessHandler struct {
	orkstackv1connect.UnimplementedAccessServiceHandler
	svc *Service
}

func mountAccessService(svc *Service, mux reservedMux, opts []connect.HandlerOption) {
	_, handler := orkstackv1connect.NewAccessServiceHandler(accessHandler{svc: svc}, opts...)
	for _, procedure := range accessServiceServed {
		mux.Handle(procedure, handler)
	}
}

// ListPermissions answers the closed catalogue.
func (h accessHandler) ListPermissions(context.Context, *connect.Request[orkstackv1.ListPermissionsRequest]) (*connect.Response[orkstackv1.ListPermissionsResponse], error) {
	out := &orkstackv1.ListPermissionsResponse{}
	for _, p := range authz.Permissions() {
		perm := &orkstackv1.Permission{Name: string(p), Allows: authz.Allows(p), BuiltInRoles: authz.RolesHolding(p)}
		for _, k := range authz.ScopeKinds(p) {
			perm.ScopeKinds = append(perm.ScopeKinds, scopeKindProto(k))
		}
		out.Permissions = append(out.Permissions, perm)
	}
	return connect.NewResponse(out), nil
}

func scopeKindProto(k authz.ScopeKind) orkstackv1.ScopeKind {
	switch k {
	case authz.ScopeDeployment:
		return orkstackv1.ScopeKind_SCOPE_KIND_DEPLOYMENT
	case authz.ScopeNamespace:
		return orkstackv1.ScopeKind_SCOPE_KIND_NAMESPACE
	case authz.ScopeOrkflow:
		return orkstackv1.ScopeKind_SCOPE_KIND_ORKFLOW
	case authz.ScopePool:
		return orkstackv1.ScopeKind_SCOPE_KIND_POOL
	}
	return orkstackv1.ScopeKind_SCOPE_KIND_UNSPECIFIED
}

// GetAccess answers the stored document with the built-in roles and the
// bindings configuration makes, and the state the Access page's banners
// read: the mode, the record, the document's age, the engines seen.
func (h accessHandler) GetAccess(ctx context.Context, _ *connect.Request[orkstackv1.GetAccessRequest]) (*connect.Response[orkstackv1.GetAccessResponse], error) {
	a := h.svc.access
	doc := &orkstackv1.AccessDocument{SchemaVersion: authz.AccessSchemaVersion}
	if a.Policies != nil {
		readCtx, cancel := context.WithTimeout(ctx, a.Policies.readTime)
		defer cancel()
		head, err := a.Policies.store.Get(readCtx)
		switch {
		case errors.Is(err, artifacts.ErrNotFound):
		case err != nil:
			return nil, connect.NewError(connect.CodeUnavailable, err)
		default:
			doc = head.Document
		}
	}
	unread := 0
	for i, b := range doc.GetBindings() {
		// What each binding is pinned to: an incarnation, by name, or stale.
		// A pin whose ids cannot be read now is shown as pinned, never as
		// stale: the document is what an admin reads to repair access.
		answered, err := h.svc.answerAccessBinding(ctx, b)
		if err != nil {
			answered, unread = answeredBinding(b), unread+1
		}
		doc.Bindings[i] = answered
	}
	if unread > 0 {
		h.svc.logger.Warn("the access document was answered without telling which pins are stale: the ids could not be read",
			"bindings", unread)
	}
	for _, name := range authz.BuiltInRoles() {
		role := &orkstackv1.Role{Name: name, BuiltIn: true}
		for _, p := range authz.BuiltInRolePermissions(name) {
			role.Permissions = append(role.Permissions, string(p))
		}
		doc.BuiltInRoles = append(doc.BuiltInRoles, role)
	}
	for _, b := range a.Configured {
		cb := &orkstackv1.Binding{Name: b.Name, Role: b.Role, Scope: b.Scope.String(), FromConfiguration: true,
			Pin: orkstackv1.BindingPin_BINDING_PIN_NONE, OrkerIdsAny: b.OrkerIDsAny}
		for _, s := range b.Subjects {
			cb.Subjects = append(cb.Subjects, s.String())
		}
		doc.ConfiguredBindings = append(doc.ConfiguredBindings, cb)
	}
	status := &orkstackv1.AccessStatus{EngineMode: a.Mode}
	if a.Policies != nil {
		st := a.Policies.State()
		doc.WrittenAroundApi = st.WrittenAroundAPI
		status.AcknowledgedRevision = st.AcknowledgedRevision
		if !st.ReadAt.IsZero() {
			status.DocumentReadAt = timestamppb.New(st.ReadAt)
			status.DocumentAge = durationpb.New(st.Age)
		}
		status.DocumentStale = st.Stale
	}
	if a.ModeRecord != nil {
		ms := a.ModeRecord.Status()
		status.RecordedMode, status.ModeRecordEtag = ms.Recorded, ms.ETag
		status.ModeRecordMissing = ms.Missing
		if !ms.AssertedSafe.IsZero() {
			status.AssertedSafeFrom = timestamppb.New(ms.AssertedSafe)
		}
	}
	if a.Fleet != nil {
		status.Engines = a.Fleet.Engines()
		if at := a.Fleet.CheckedAt(); !at.IsZero() {
			status.FleetCheckedAt = timestamppb.New(at)
		}
	}
	return connect.NewResponse(&orkstackv1.GetAccessResponse{Document: doc, Status: status}), nil
}

// LowerAuthMode rewrites the mode record downward. The engines are then
// restarted in the lower mode; there is no setting to leave behind.
func (h accessHandler) LowerAuthMode(ctx context.Context, req *connect.Request[orkstackv1.LowerAuthModeRequest]) (*connect.Response[orkstackv1.LowerAuthModeResponse], error) {
	g := h.svc.access.ModeRecord
	if g == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine keeps no mode record"))
	}
	who := authn.Recorded(ctx, req.Header())
	previous, err := g.Lower(ctx, req.Msg.GetMode(), req.Msg.GetRecordEtag(), req.Msg.GetAcceptDuplicateRetries(), who.ID,
		fmt.Sprintf("LowerAuthMode accept_duplicate_retries=%t", req.Msg.GetAcceptDuplicateRetries()))
	var refused *LowerRefusal
	switch {
	case errors.As(err, &refused) && refused.Precondition:
		return nil, connect.NewError(connect.CodeFailedPrecondition, refused)
	case errors.As(err, &refused):
		return nil, connect.NewError(connect.CodeInvalidArgument, refused)
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&orkstackv1.LowerAuthModeResponse{PreviousMode: previous, Mode: req.Msg.GetMode()}), nil
}

// ListSignins answers a principal's sign-ins (every principal's when none
// is named): never a cookie, a refresh token or a CSRF token.
func (h accessHandler) ListSignins(ctx context.Context, req *connect.Request[orkstackv1.ListSigninsRequest]) (*connect.Response[orkstackv1.ListSigninsResponse], error) {
	out := &orkstackv1.ListSigninsResponse{}
	s := h.svc.access.Signins
	if s == nil {
		return connect.NewResponse(out), nil
	}
	listed, err := s.List(ctx, req.Msg.GetPrincipalId())
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	for _, l := range listed {
		out.Signins = append(out.Signins, &orkstackv1.Signin{PublicId: l.PublicID, PrincipalId: l.PrincipalID,
			CreatedAt: timestamppb.New(l.CreatedAt), ExpiresAt: timestamppb.New(l.ExpiresAt),
			ValidatedUntil: timestamppb.New(l.ValidatedUntil), IdleExpiresAt: timestamppb.New(l.IdleExpiresAt)})
	}
	return connect.NewResponse(out), nil
}

// RevokeSignins ends a principal's sign-ins: those named, or every one.
func (h accessHandler) RevokeSignins(ctx context.Context, req *connect.Request[orkstackv1.RevokeSigninsRequest]) (*connect.Response[orkstackv1.RevokeSigninsResponse], error) {
	if req.Msg.GetPrincipalId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("principal_id is required"))
	}
	s := h.svc.access.Signins
	if s == nil {
		return connect.NewResponse(&orkstackv1.RevokeSigninsResponse{}), nil
	}
	n, err := s.Revoke(ctx, req.Msg.GetPrincipalId(), req.Msg.GetPublicIds())
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("%d sign-in(s) ended before: %w", n, err))
	}
	return connect.NewResponse(&orkstackv1.RevokeSigninsResponse{Revoked: int32(n)}), nil
}
