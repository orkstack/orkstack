package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// accessWriteAttempts bounds how often a write whose caller named no etag
// reads the document again after losing the swap (authn-authz.md §6.2).
const accessWriteAttempts = 3

// accessEdit changes doc, a copy of the stored document, deciding against
// current, the policy compiled from exactly the document it replaces. It
// returns the change's one-line summary for the revision, or "" for a
// change that changes nothing, which is answered without a write.
type accessEdit func(doc *orkstackv1.AccessDocument, current *authz.Policy) (string, error)

// writeAccess is every AccessService write: read the head, check the
// caller's etag, change one thing, validate the whole, write the revision,
// swap. A caller's etag the head has moved past is aborted; a caller that
// named none is retried from a fresh read, three times, then aborted.
func (h accessHandler) writeAccess(ctx context.Context, header http.Header, etag string, edit accessEdit) (*artifacts.AccessHead, error) {
	a := h.svc.access
	if a.Policies == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine keeps no access document"))
	}
	store := a.Policies.store
	for attempt := 0; attempt < accessWriteAttempts; attempt++ {
		readCtx, cancel := context.WithTimeout(ctx, a.Policies.readTime)
		head, err := store.Get(readCtx)
		cancel()
		switch {
		case errors.Is(err, artifacts.ErrNotFound):
			head = nil
		case err != nil:
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read the access document: %w", err))
		}
		var stored *orkstackv1.AccessDocument
		current := ""
		if head != nil {
			stored, current = head.Document, head.ETag
		}
		if etag != "" && etag != current {
			return nil, connect.NewError(connect.CodeAborted, errors.New("the access document changed since it was read; read it again and retry"))
		}
		policy, err := authz.Compile(stored, a.Configured, a.Policies.now())
		if err != nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the stored access document does not compile: %w", err))
		}
		doc := &orkstackv1.AccessDocument{SchemaVersion: authz.AccessSchemaVersion}
		if stored != nil {
			doc = proto.Clone(stored).(*orkstackv1.AccessDocument)
		}
		change, err := edit(doc, policy)
		if err != nil {
			return nil, err
		}
		if change == "" {
			return head, nil
		}
		if _, err := authz.Validate(doc, h.validateOptions(ctx)); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		who := authn.Recorded(ctx, header)
		written, err := store.Write(ctx, doc, head, artifacts.AccessWrite{At: a.Policies.now(), By: who.ID, ByVerified: who.Verified, Change: change})
		switch {
		case errors.Is(err, artifacts.ErrPreconditionFailed) && etag == "":
			continue
		case errors.Is(err, artifacts.ErrPreconditionFailed):
			return nil, connect.NewError(connect.CodeAborted, errors.New("the access document changed while it was written; read it again and retry"))
		case err != nil:
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("write the access document: %w", err))
		}
		a.Policies.Wrote(written)
		noteAudit(ctx, fmt.Sprintf("revision=%d", written.Meta.Revision))
		return written, nil
	}
	return nil, connect.NewError(connect.CodeAborted, fmt.Errorf("the access document changed under %d attempts; retry", accessWriteAttempts))
}

// validateOptions are the facts a write is validated against: the trusted
// issuers' kept claims, which namespaces exist, and whether auth.admins
// names anyone.
func (h accessHandler) validateOptions(ctx context.Context) authz.ValidateOptions {
	a := h.svc.access
	opts := authz.ValidateOptions{
		NamespaceExists: func(name string) bool {
			if name == artifacts.DefaultNamespace {
				return true
			}
			_, err := h.svc.namespace(ctx, name)
			return err == nil
		},
	}
	for _, b := range a.Configured {
		if b.Rule == authz.ConfigAdminsRule {
			opts.AdminsConfigured = true
		}
	}
	if a.Authenticator != nil {
		issuers := a.Authenticator.Issuers
		opts.Issuer = func(name string) (authz.IssuerFacts, bool) {
			iss, ok := issuers.ByName(name)
			if !ok {
				return authz.IssuerFacts{}, false
			}
			return authz.IssuerFacts{Claims: iss.Claims}, true
		}
	}
	return opts
}

// mayWrite is access.write at the deployment, decided against the document
// a write replaces.
func mayWrite(ctx context.Context, current *authz.Policy) error {
	_, err := authz.Authorize(authz.Request{Action: authz.Action(authz.PermAccessWrite),
		Subject: subjectOf(authn.FromContext(ctx)), Policy: current})
	return deniedAsConnect(err)
}

// mayBind is access.write at the deployment, or the delegation rule for a
// binding of role at scope (authz.MayDelegate).
func mayBind(ctx context.Context, current *authz.Policy, role, scope string) error {
	if mayWrite(ctx, current) == nil {
		return nil
	}
	sc, err := authz.ParseScope(scope)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return deniedAsConnect(authz.MayDelegate(current, subjectOf(authn.FromContext(ctx)), role, sc))
}

func deniedAsConnect(err error) error {
	var denied *authz.DeniedError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &denied) && denied.Reason == authz.ReasonUnauthenticated:
		return connect.NewError(connect.CodeUnauthenticated, err)
	case errors.As(err, &denied):
		return connect.NewError(connect.CodePermissionDenied, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

func notFound(kind, name string) error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("%s %q does not exist", kind, name))
}

func exists(kind, name string) error {
	return connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("%s %q already exists", kind, name))
}

func etagOfHead(head *artifacts.AccessHead) string {
	if head == nil {
		return ""
	}
	return head.ETag
}

// --- roles ---------------------------------------------------------------

func (h accessHandler) CreateRole(ctx context.Context, req *connect.Request[orkstackv1.CreateRoleRequest]) (*connect.Response[orkstackv1.CreateRoleResponse], error) {
	role := req.Msg.GetRole()
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		if role == nil {
			return "", connect.NewError(connect.CodeInvalidArgument, errors.New("role is required"))
		}
		if slices.ContainsFunc(doc.GetRoles(), func(r *orkstackv1.Role) bool { return r.GetName() == role.GetName() }) {
			return "", exists("role", role.GetName())
		}
		doc.Roles = append(doc.Roles, proto.Clone(role).(*orkstackv1.Role))
		return "create role " + role.GetName(), nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.CreateRoleResponse{Role: role, Etag: etagOfHead(head)}), nil
}

func (h accessHandler) UpdateRole(ctx context.Context, req *connect.Request[orkstackv1.UpdateRoleRequest]) (*connect.Response[orkstackv1.UpdateRoleResponse], error) {
	role := req.Msg.GetRole()
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		i := slices.IndexFunc(doc.GetRoles(), func(r *orkstackv1.Role) bool { return r.GetName() == role.GetName() })
		if i < 0 {
			return "", notFound("role", role.GetName())
		}
		if proto.Equal(doc.Roles[i], role) {
			return "", nil
		}
		doc.Roles[i] = proto.Clone(role).(*orkstackv1.Role)
		return "update role " + role.GetName(), nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.UpdateRoleResponse{Role: role, Etag: etagOfHead(head)}), nil
}

func (h accessHandler) DeleteRole(ctx context.Context, req *connect.Request[orkstackv1.DeleteRoleRequest]) (*connect.Response[orkstackv1.DeleteRoleResponse], error) {
	name := req.Msg.GetName()
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		i := slices.IndexFunc(doc.GetRoles(), func(r *orkstackv1.Role) bool { return r.GetName() == name })
		if i < 0 {
			return "", notFound("role", name)
		}
		if j := slices.IndexFunc(doc.GetBindings(), func(b *orkstackv1.Binding) bool { return b.GetRole() == name }); j >= 0 {
			return "", connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("role %q is bound by binding %q; delete or change that binding first", name, doc.Bindings[j].GetName()))
		}
		doc.Roles = slices.Delete(doc.Roles, i, i+1)
		return "delete role " + name, nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.DeleteRoleResponse{Etag: etagOfHead(head)}), nil
}

// --- bindings ------------------------------------------------------------

// errNoIncarnationIDs answers a repin: namespaces and Orkflows carry no
// immutable id in this release, so every binding matches by name.
var errNoIncarnationIDs = errors.New("namespaces and Orkflows carry no id in this release, so a binding matches by name and has nothing to pin to")

// storedBinding is the binding as a write stores it: output-only fields
// cleared, the pin held as it was.
func storedBinding(b *orkstackv1.Binding) *orkstackv1.Binding {
	out := proto.Clone(b).(*orkstackv1.Binding)
	out.Pin = orkstackv1.BindingPin_BINDING_PIN_UNSPECIFIED
	out.PinnedAt = nil
	out.FromConfiguration = false
	return out
}

// answeredBinding is a stored binding as a response shows it.
func answeredBinding(b *orkstackv1.Binding) *orkstackv1.Binding {
	out := proto.Clone(b).(*orkstackv1.Binding)
	out.Pin = orkstackv1.BindingPin_BINDING_PIN_NONE
	if sc, err := authz.ParseScope(b.GetScope()); err == nil && (sc.Kind == authz.ScopeNamespace || sc.Kind == authz.ScopeOrkflow) {
		out.Pin = orkstackv1.BindingPin_BINDING_PIN_BY_NAME
		if b.GetScopeId() != "" {
			out.Pin = orkstackv1.BindingPin_BINDING_PIN_PINNED
		}
	}
	return out
}

func (h accessHandler) CreateBinding(ctx context.Context, req *connect.Request[orkstackv1.CreateBindingRequest]) (*connect.Response[orkstackv1.CreateBindingResponse], error) {
	in := req.Msg.GetBinding()
	if in == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("binding is required"))
	}
	b := storedBinding(in)
	b.ScopeId = ""
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayBind(ctx, cur, b.GetRole(), b.GetScope()); err != nil {
			return "", err
		}
		if slices.ContainsFunc(doc.GetBindings(), func(x *orkstackv1.Binding) bool { return x.GetName() == b.GetName() }) {
			if mayWrite(ctx, cur) != nil {
				// A delegate is not told which bindings exist outside its
				// reach: names are the deployment's.
				return "", connect.NewError(connect.CodeAlreadyExists, errors.New("that binding name is taken; choose another"))
			}
			return "", exists("binding", b.GetName())
		}
		doc.Bindings = append(doc.Bindings, b)
		return fmt.Sprintf("create binding %s (%s at %s)", b.GetName(), b.GetRole(), b.GetScope()), nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.CreateBindingResponse{Binding: answeredBinding(b), Etag: etagOfHead(head)}), nil
}

func (h accessHandler) UpdateBinding(ctx context.Context, req *connect.Request[orkstackv1.UpdateBindingRequest]) (*connect.Response[orkstackv1.UpdateBindingResponse], error) {
	in := req.Msg.GetBinding()
	if in == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("binding is required"))
	}
	if req.Msg.GetRepin() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errNoIncarnationIDs)
	}
	b := storedBinding(in)
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		i := slices.IndexFunc(doc.GetBindings(), func(x *orkstackv1.Binding) bool { return x.GetName() == b.GetName() })
		if i < 0 {
			return "", notFound("binding", b.GetName())
		}
		// A save that changes nothing else never moves a pin.
		b.ScopeId = doc.Bindings[i].GetScopeId()
		if b.GetScope() != doc.Bindings[i].GetScope() {
			b.ScopeId = ""
		}
		if proto.Equal(doc.Bindings[i], b) {
			return "", nil
		}
		doc.Bindings[i] = b
		return fmt.Sprintf("update binding %s (%s at %s)", b.GetName(), b.GetRole(), b.GetScope()), nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.UpdateBindingResponse{Binding: answeredBinding(b), Etag: etagOfHead(head)}), nil
}

func (h accessHandler) DeleteBinding(ctx context.Context, req *connect.Request[orkstackv1.DeleteBindingRequest]) (*connect.Response[orkstackv1.DeleteBindingResponse], error) {
	name := req.Msg.GetName()
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		i := slices.IndexFunc(doc.GetBindings(), func(x *orkstackv1.Binding) bool { return x.GetName() == name })
		if i < 0 {
			// Said only to a caller who may write the document: a delegate
			// learns nothing of bindings outside what it could make.
			if err := mayWrite(ctx, cur); err != nil {
				return "", err
			}
			return "", notFound("binding", name)
		}
		if err := mayBind(ctx, cur, doc.Bindings[i].GetRole(), doc.Bindings[i].GetScope()); err != nil {
			return "", err
		}
		doc.Bindings = slices.Delete(doc.Bindings, i, i+1)
		return "delete binding " + name, nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.DeleteBindingResponse{Etag: etagOfHead(head)}), nil
}

// --- service accounts ----------------------------------------------------

// keyedAccount is sa with each key named by its RFC 7638 thumbprint and,
// when it is new to the account, the instant it was added; a key the stored
// account already holds keeps its added_at.
func keyedAccount(sa *orkstackv1.ServiceAccount, stored *orkstackv1.ServiceAccount, at *timestamppb.Timestamp) (*orkstackv1.ServiceAccount, error) {
	out := proto.Clone(sa).(*orkstackv1.ServiceAccount)
	added := map[string]*timestamppb.Timestamp{}
	for _, k := range stored.GetKeys() {
		added[k.GetKid()] = k.GetAddedAt()
	}
	for i, k := range out.GetKeys() {
		key, err := jwt.ParsePublicKeyPEM([]byte(k.GetPublicKeyPem()))
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("service account %q key %d: %w", sa.GetName(), i, err))
		}
		thumb, err := key.Thumbprint()
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("service account %q key %d: %w", sa.GetName(), i, err))
		}
		if k.GetKid() != "" && k.GetKid() != thumb {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("service account %q key %d: kid is not the key's RFC 7638 thumbprint", sa.GetName(), i))
		}
		k.Kid = thumb
		if prev, ok := added[thumb]; ok && prev != nil {
			k.AddedAt = prev
		} else {
			k.AddedAt = at
		}
	}
	return out, nil
}

func (h accessHandler) CreateServiceAccount(ctx context.Context, req *connect.Request[orkstackv1.CreateServiceAccountRequest]) (*connect.Response[orkstackv1.CreateServiceAccountResponse], error) {
	var saved *orkstackv1.ServiceAccount
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		sa := req.Msg.GetServiceAccount()
		if sa == nil {
			return "", connect.NewError(connect.CodeInvalidArgument, errors.New("service_account is required"))
		}
		if slices.ContainsFunc(doc.GetServiceAccounts(), func(x *orkstackv1.ServiceAccount) bool { return x.GetName() == sa.GetName() }) {
			return "", exists("service account", sa.GetName())
		}
		keyed, err := keyedAccount(sa, nil, timestamppb.New(h.svc.access.Policies.now()))
		if err != nil {
			return "", err
		}
		doc.ServiceAccounts = append(doc.ServiceAccounts, keyed)
		saved = keyed
		return "create service account " + sa.GetName(), nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.CreateServiceAccountResponse{ServiceAccount: saved, Etag: etagOfHead(head)}), nil
}

func (h accessHandler) UpdateServiceAccount(ctx context.Context, req *connect.Request[orkstackv1.UpdateServiceAccountRequest]) (*connect.Response[orkstackv1.UpdateServiceAccountResponse], error) {
	var saved *orkstackv1.ServiceAccount
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		sa := req.Msg.GetServiceAccount()
		i := slices.IndexFunc(doc.GetServiceAccounts(), func(x *orkstackv1.ServiceAccount) bool { return x.GetName() == sa.GetName() })
		if i < 0 {
			return "", notFound("service account", sa.GetName())
		}
		keyed, err := keyedAccount(sa, doc.ServiceAccounts[i], timestamppb.New(h.svc.access.Policies.now()))
		if err != nil {
			return "", err
		}
		saved = keyed
		if proto.Equal(doc.ServiceAccounts[i], keyed) {
			return "", nil
		}
		doc.ServiceAccounts[i] = keyed
		return "update service account " + sa.GetName(), nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.UpdateServiceAccountResponse{ServiceAccount: saved, Etag: etagOfHead(head)}), nil
}

func (h accessHandler) DeleteServiceAccount(ctx context.Context, req *connect.Request[orkstackv1.DeleteServiceAccountRequest]) (*connect.Response[orkstackv1.DeleteServiceAccountResponse], error) {
	name := req.Msg.GetName()
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		i := slices.IndexFunc(doc.GetServiceAccounts(), func(x *orkstackv1.ServiceAccount) bool { return x.GetName() == name })
		if i < 0 {
			return "", notFound("service account", name)
		}
		doc.ServiceAccounts = slices.Delete(doc.ServiceAccounts, i, i+1)
		return "delete service account " + name, nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.DeleteServiceAccountResponse{Etag: etagOfHead(head)}), nil
}

// --- disabled principals -------------------------------------------------

func (h accessHandler) SetPrincipalDisabled(ctx context.Context, req *connect.Request[orkstackv1.SetPrincipalDisabledRequest]) (*connect.Response[orkstackv1.SetPrincipalDisabledResponse], error) {
	id, disable := req.Msg.GetPrincipalId(), req.Msg.GetDisabled()
	head, err := h.writeAccess(ctx, req.Header(), req.Msg.GetEtag(), func(doc *orkstackv1.AccessDocument, cur *authz.Policy) (string, error) {
		if err := mayWrite(ctx, cur); err != nil {
			return "", err
		}
		if id == "" {
			return "", connect.NewError(connect.CodeInvalidArgument, errors.New("principal_id is required"))
		}
		if disable {
			if err := h.mayDisable(ctx, cur, id); err != nil {
				return "", err
			}
		}
		i := slices.Index(doc.GetDisabled(), id)
		switch {
		case disable && i >= 0, !disable && i < 0:
			return "", nil
		case disable:
			doc.Disabled = append(doc.Disabled, id)
			return "disable " + id, nil
		}
		doc.Disabled = slices.Delete(doc.Disabled, i, i+1)
		return "enable " + id, nil
	})
	if err != nil {
		return nil, err
	}
	// Sign-ins are P9's: no sign-in exists to end yet.
	return connect.NewResponse(&orkstackv1.SetPrincipalDisabledResponse{Etag: etagOfHead(head)}), nil
}

// mayDisable refuses to disable the way back in: break-glass, a principal
// auth.admins makes admin, and the caller itself. A disabled principal holds
// nothing, configuration's grants included, so either would leave the
// deployment to be repaired by editing the bucket (security review M1). An
// Orker auth.orkers binds is disabled like any other: that is the first step
// of the stolen-Orker procedure.
func (h accessHandler) mayDisable(ctx context.Context, cur *authz.Policy, id string) error {
	refuse := func(why string) error {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("principal %q is not disabled through the API: %s", id, why))
	}
	if id == "serviceaccount:"+authz.BreakGlassAccount {
		return refuse("it is break-glass, the way in when everything else is lost")
	}
	if p := authn.FromContext(ctx); p != nil && p.ID == id {
		return refuse("it is the caller; another admin disables it")
	}
	sub, err := authz.SubjectFromID(id)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	for _, b := range cur.Bindings() {
		if !b.FromConfiguration || b.Role != authz.RoleAdmin {
			continue
		}
		for _, sel := range b.Subjects {
			if sel.Matches(sub) {
				return refuse("auth.admins makes it admin; remove it from auth.admins first")
			}
		}
	}
	return nil
}
