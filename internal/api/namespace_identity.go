package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A namespace's identity policy (docs/TASK_IDENTITY.md): audiences, grants,
// identities_current_only and attempts_extra, kept beside the namespace's
// id and never in its document. The API shows it as Namespace.identity, and
// an UpdateNamespace that changes it changes nothing else, so each call
// writes one object.

// NamespacePolicyStore keeps namespaces' identity policies.
type NamespacePolicyStore interface {
	NamespacePolicy(ctx context.Context, namespace string, createdAt time.Time) (artifacts.NamespacePolicy, bool, error)
	SetNamespacePolicy(ctx context.Context, namespace string, createdAt time.Time, p artifacts.NamespacePolicy, ifGeneration int64, by artifacts.PolicyWriter) (artifacts.NamespacePolicy, error)
	ClearNamespacePolicy(ctx context.Context, namespace string, createdAt time.Time) error
}

var _ NamespacePolicyStore = (*artifacts.IncarnationStore)(nil)

// policyOf is the policy part of a namespace's identity field.
func policyOf(id *orkstackv1.NamespaceIdentity) artifacts.NamespacePolicy {
	var p artifacts.NamespacePolicy
	if id == nil {
		return p
	}
	if len(id.GetAudiences()) > 0 {
		p.Audiences = slices.Clone(id.GetAudiences())
	}
	if len(id.GetGrants()) > 0 {
		p.Grants = map[string]artifacts.Grant{}
		for name, g := range id.GetGrants() {
			grant := artifacts.Grant{Provider: g.GetProvider(), Principal: g.GetPrincipal(),
				Orkflows: slices.Clone(g.GetOrkflows()), CurrentOnly: g.GetCurrentOnly()}
			if len(g.GetOrigins()) > 0 {
				grant.Origins = slices.Clone(g.GetOrigins())
			}
			p.Grants[name] = grant
		}
	}
	p.IdentitiesCurrentOnly = id.GetIdentitiesCurrentOnly()
	if id.AttemptsExtra != nil {
		v := id.GetAttemptsExtra()
		p.AttemptsExtra = &v
	}
	return p
}

// samePolicy says whether two policies say the same thing, whoever wrote
// them and whenever.
func samePolicy(a, b artifacts.NamespacePolicy) bool {
	if !slices.Equal(a.Audiences, b.Audiences) || a.IdentitiesCurrentOnly != b.IdentitiesCurrentOnly ||
		(a.AttemptsExtra == nil) != (b.AttemptsExtra == nil) || (a.AttemptsExtra != nil && *a.AttemptsExtra != *b.AttemptsExtra) {
		return false
	}
	return maps.EqualFunc(a.Grants, b.Grants, func(x, y artifacts.Grant) bool {
		return x.Provider == y.Provider && x.Principal == y.Principal && x.CurrentOnly == y.CurrentOnly &&
			slices.Equal(x.Orkflows, y.Orkflows) && slices.Equal(x.Origins, y.Origins)
	})
}

// fillPolicy sets the policy into ns's identity field; the store's
// generation is its revision and its etag.
func fillPolicy(ns *orkstackv1.Namespace, p artifacts.NamespacePolicy) {
	if ns.Identity == nil {
		ns.Identity = &orkstackv1.NamespaceIdentity{}
	}
	id := ns.Identity
	id.Audiences = slices.Clone(p.Audiences)
	id.Grants = nil
	if len(p.Grants) > 0 {
		id.Grants = map[string]*orkstackv1.Grant{}
		for name, g := range p.Grants {
			id.Grants[name] = &orkstackv1.Grant{Provider: g.Provider, Principal: g.Principal, Orkflows: slices.Clone(g.Orkflows),
				CurrentOnly: g.CurrentOnly, Origins: slices.Clone(g.Origins)}
		}
	}
	id.IdentitiesCurrentOnly = p.IdentitiesCurrentOnly
	id.AttemptsExtra = p.AttemptsExtra
	id.Revision = p.Generation
	id.Etag = ""
	if p.Generation > 0 {
		id.Etag = strconv.FormatInt(p.Generation, 10)
	}
}

// withoutIdentity is ns with no identity field: what the namespace
// document holds.
func withoutIdentity(ns *orkstackv1.Namespace) *orkstackv1.Namespace {
	if ns.GetIdentity() == nil {
		return ns
	}
	out := proto.Clone(ns).(*orkstackv1.Namespace)
	out.Identity = nil
	return out
}

// documentPart is what of ns its document holds and a caller writes: no
// identity, and none of what the registry stamps (its etag, created_at and
// updated_at), which an identity-only write neither reads nor moves.
func documentPart(ns *orkstackv1.Namespace) *orkstackv1.Namespace {
	out := proto.Clone(ns).(*orkstackv1.Namespace)
	out.Identity, out.Etag, out.CreatedAt, out.UpdatedAt = nil, "", nil, nil
	return out
}

// namespacePolicy reads ns's policy: none when no store is wired or none
// is stored for its incarnation.
func (s *Service) namespacePolicy(ctx context.Context, ns *orkstackv1.Namespace) (artifacts.NamespacePolicy, error) {
	if s.namespacePolicies == nil {
		return artifacts.NamespacePolicy{}, nil
	}
	p, _, err := s.namespacePolicies.NamespacePolicy(ctx, ns.GetName(), ns.GetCreatedAt().AsTime())
	return p, err
}

// updateNamespacePolicy writes the identity policy next asks for, when it
// differs from the one stored: changed is false when it does not. An update
// that changes the policy changes nothing else, needs namespace.policy.update
// and names the policy's etag it read.
func (s *Service) updateNamespacePolicy(ctx context.Context, req *connect.Request[orkstackv1.UpdateNamespaceRequest], next, stored *orkstackv1.Namespace) (*orkstackv1.Namespace, bool, error) {
	if next.GetIdentity() == nil {
		return nil, false, nil
	}
	current, err := s.namespacePolicy(ctx, stored)
	if err != nil {
		return nil, false, policyReadError(stored.GetName(), err)
	}
	asked := policyOf(next.GetIdentity())
	if samePolicy(asked, current) {
		return nil, false, nil
	}
	if s.namespacePolicies == nil {
		return nil, false, connect.NewError(connect.CodeUnimplemented, errors.New("this engine keeps no identity policy"))
	}
	if err := s.mayUpdateNamespacePolicy(ctx, stored.GetName()); err != nil {
		return nil, false, err
	}
	if !proto.Equal(documentPart(next), documentPart(stored)) {
		return nil, false, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"namespace %q: change its identity policy in an update of its own, with nothing else changed", stored.GetName()))
	}
	if asked.AttemptsExtra != nil && int(*asked.AttemptsExtra) > s.identityAttemptsMaxExtra {
		return nil, false, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"namespace %q: attempts_extra %d is above the deployment's identity.attempts.max_extra (%d)",
			stored.GetName(), *asked.AttemptsExtra, s.identityAttemptsMaxExtra))
	}
	var ifGeneration int64
	if etag := next.GetIdentity().GetEtag(); etag != "" {
		if ifGeneration, err = strconv.ParseInt(etag, 10, 64); err != nil || ifGeneration < 1 {
			return nil, false, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace %q: identity etag %q is none the policy gave", stored.GetName(), etag))
		}
	}
	caller := authn.Recorded(ctx, req.Header())
	by := artifacts.PolicyWriter{Principal: caller.ID, Verified: caller.Verified}
	written, err := s.namespacePolicies.SetNamespacePolicy(ctx, stored.GetName(), stored.GetCreatedAt().AsTime(), asked, ifGeneration, by)
	switch {
	case errors.Is(err, artifacts.ErrPolicyChanged):
		return nil, false, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q's identity policy changed since it was read; read it again and re-apply the change", stored.GetName()))
	case errors.Is(err, artifacts.ErrInvalidPolicy):
		return nil, false, connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, artifacts.ErrPolicyUnreadable), errors.Is(err, artifacts.ErrIncarnationUnreadable):
		return nil, false, policyReadError(stored.GetName(), err)
	case err != nil:
		return nil, false, connect.NewError(connect.CodeUnavailable, fmt.Errorf("write namespace %q's identity policy: %w", stored.GetName(), err))
	}
	out := proto.Clone(stored).(*orkstackv1.Namespace)
	fillPolicy(out, written)
	return out, true, nil
}

// policyReadError is the answer to a policy that could not be read: one
// stored in a form this release cannot read is the operator's to repair
// (FailedPrecondition); a store that did not answer is Unavailable.
func policyReadError(namespace string, err error) error {
	if errors.Is(err, artifacts.ErrPolicyUnreadable) || errors.Is(err, artifacts.ErrIncarnationUnreadable) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q's identity policy cannot be read; repair or remove its record (docs/STORAGE.md): %w", namespace, err))
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("read namespace %q's identity policy: %w", namespace, err))
}

// checkPlannedPolicy refuses, before anything is written, a policy an
// update would refuse: attempts_extra over identity.attempts.max_extra, or
// a policy the store cannot hold.
func (s *Service) checkPlannedPolicy(spec *orkstackv1.Namespace) error {
	p := policyOf(spec.GetIdentity())
	if p.AttemptsExtra != nil && int(*p.AttemptsExtra) > s.identityAttemptsMaxExtra {
		return fmt.Errorf("attempts_extra %d is above the deployment's identity.attempts.max_extra (%d)", *p.AttemptsExtra, s.identityAttemptsMaxExtra)
	}
	return artifacts.CheckNamespacePolicy(p)
}

// mayUpdateNamespacePolicy asks namespace.policy.update at the namespace,
// where callers are authenticated: the identity policy is what the
// namespace's tasks may reach.
func (s *Service) mayUpdateNamespacePolicy(ctx context.Context, namespace string) error {
	if !s.access.enabled() || authn.ModeFrom(ctx) != authn.ModeEnforce {
		return nil
	}
	policy, unavailable := s.access.policy(ctx)
	_, err := s.authorizeWithIDs(ctx, authz.Request{Action: authz.Action(authz.PermNamespacePolicyUpdate),
		Subject: subjectOf(authn.FromContext(ctx)), Resource: authz.Resource{Namespace: namespace}, Policy: policy})
	if connect.CodeOf(err) == connect.CodeUnavailable {
		return err
	}
	if err != nil && unavailable {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%w; only configuration's principals are served", ErrPolicyUnavailable))
	}
	return deniedAsConnect(err)
}

// checkIdentityPolicy refuses a definition whose tokens' subjects do not fit
// (IDENTITY_SUBJECT_TOO_LONG, which no policy changes) or that asks for an
// audience namespace ns's identity policy does not allow
// (IDENTITY_NOT_PERMITTED), anchored to the field. A definition that asks for nothing reads no policy; a policy
// that cannot be read refuses, never allows.
func (s *Service) checkIdentityPolicy(ctx context.Context, ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) error {
	if !identity.AsksForIdentity(wf) {
		return nil
	}
	if err := s.checkSubjects(ns, wf, ns.GetName()); err != nil {
		var long *identity.SubjectTooLongError
		if errors.As(err, &long) {
			return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{Field: long.Field(), Err: err})
		}
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	p, err := s.namespacePolicy(ctx, ns)
	if err != nil {
		return policyReadError(ns.GetName(), err)
	}
	if err := identity.CheckAudiences(ns.GetName(), p.Audiences, wf); err != nil {
		var refused *identity.AudienceNotPermittedError
		if errors.As(err, &refused) {
			return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{Field: refused.Field(), Err: err})
		}
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	// No run is, so no start and no current version is judged; submit does.
	_, err = s.checkGrants(ctx, ns, p, wf, ns.GetName(), "", false)
	var refused *identity.GrantRefusedError
	if errors.As(err, &refused) {
		return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{Field: refused.Field(), Err: err})
	}
	return err
}
