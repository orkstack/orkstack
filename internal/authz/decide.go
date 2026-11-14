package authz

import (
	"errors"
	"fmt"
)

// The reasons a permission is refused.
const (
	// ReasonPermissionDenied: no binding grants the permission at a scope
	// that covers the resource.
	ReasonPermissionDenied = "PERMISSION_DENIED"
	// ReasonPrincipalDisabled: the caller's id is under `disabled`, whatever
	// it presented.
	ReasonPrincipalDisabled = "PRINCIPAL_DISABLED"
	// ReasonUnauthenticated: the request carries no verified caller.
	ReasonUnauthenticated = "UNAUTHENTICATED"
)

// RuleAuthenticated is the rule that admits catalog.read: every
// authenticated principal holds it.
const RuleAuthenticated = "authenticated"

// ErrNoPolicy is a permission asked with no access document: an error,
// never an allow.
var ErrNoPolicy = errors.New("authorization: a permission is decided against an access document, and none was given")

// DeniedError is a refused permission. Its message names the caller, the
// permission and the scope, and never a binding: a caller is not told what
// someone else holds.
type DeniedError struct {
	Subject    string // "" when nobody was authenticated
	Permission Permission
	Scope      string
	Reason     string
}

func (e *DeniedError) Error() string {
	switch e.Reason {
	case ReasonUnauthenticated:
		return fmt.Sprintf("%s: %s at %s needs an authenticated caller", e.Reason, e.Permission, e.Scope)
	case ReasonPrincipalDisabled:
		return fmt.Sprintf("%s: principal %q is disabled", e.Reason, e.Subject)
	}
	return fmt.Sprintf("%s: principal %q does not hold %s at %s", e.Reason, e.Subject, e.Permission, e.Scope)
}

// GrantedBinding is the binding that admitted a decision, with the subject
// selector that matched. A later check by binding (ByBinding) names it.
type GrantedBinding struct {
	Name    string
	Rule    string
	Subject string
	Role    string
	Scope   string
	ScopeID string
}

// rank orders bindings narrowest first: an Orkflow, then a namespace or a
// pool, then the deployment, then configuration.
func rank(b Binding) int {
	if b.FromConfiguration {
		return 4
	}
	switch b.Scope.Kind {
	case ScopeOrkflow:
		return 0
	case ScopeNamespace, ScopePool:
		return 1
	}
	return 2
}

// pinnedElsewhere reports a binding pinned to an incarnation the resource
// is not known to be: another one, or one the request did not read. A
// pinned binding matches its incarnation only, so a resource whose id the
// caller does not supply is closed to it, never matched by name
// (authn-authz.md §6.2).
func pinnedElsewhere(b Binding, r Resource) bool {
	if b.ScopeID == "" {
		return false
	}
	switch b.Scope.Kind {
	case ScopeNamespace:
		return r.NamespaceID != b.ScopeID
	case ScopeOrkflow:
		return r.OrkflowID != b.ScopeID
	}
	return false
}

// grants reports whether b grants perm on r to sub, and the selector that
// matched.
func (p *Policy) grants(b Binding, perm Permission, r Resource, sub *Subject) (Selector, bool) {
	if !b.Scope.covers(r) || pinnedElsewhere(b, r) || !grantableAt(perm, b.Scope.Kind) {
		return Selector{}, false
	}
	perms, ok := p.RolePermissions(b.Role)
	if !ok || !perms[perm] {
		return Selector{}, false
	}
	for _, sel := range b.Subjects {
		if sel.Matches(sub) {
			return sel, true
		}
	}
	return Selector{}, false
}

func authorizePermission(req Request) (Decision, error) {
	perm := Permission(req.Action)
	if req.Policy == nil {
		return Decision{}, ErrNoPolicy
	}
	scope := req.Resource.String()
	decision := Decision{Action: req.Action, Scope: scope, Generation: req.Policy.Revision,
		DocumentReadAt: req.Policy.ReadAt}
	if req.Subject == nil || req.Subject.ID == "" {
		return decision, &DeniedError{Permission: perm, Scope: scope, Reason: ReasonUnauthenticated}
	}
	decision.Subject = req.Subject.ID
	if req.Policy.Disabled(req.Subject.ID) {
		return decision, &DeniedError{Subject: req.Subject.ID, Permission: perm, Scope: scope, Reason: ReasonPrincipalDisabled}
	}
	if perm == PermCatalogRead && !req.Policy.configOnly {
		decision.Rule = RuleAuthenticated
		return decision, nil
	}
	var (
		best     *Binding
		bestSel  Selector
		bestRank int
		bestOwn  bool
	)
	for b := range req.Policy.allBindings {
		sel, ok := req.Policy.grants(b, perm, req.Resource, req.Subject)
		if !ok {
			continue
		}
		// Narrowest first; among equals, the binding that names the
		// principal itself, then the first by name (authn-authz.md §7).
		r, own := rank(b), sel.namesOnly(req.Subject.ID)
		if best == nil || r < bestRank || (r == bestRank && (own && !bestOwn || own == bestOwn && b.Name < best.Name)) {
			best, bestSel, bestRank, bestOwn = &b, sel, r, own
		}
	}
	if best == nil {
		return decision, &DeniedError{Subject: req.Subject.ID, Permission: perm, Scope: scope, Reason: ReasonPermissionDenied}
	}
	decision.Rule = best.Rule
	decision.Granted = &GrantedBinding{Name: best.Name, Rule: best.Rule, Subject: bestSel.String(), Role: best.Role,
		Scope: best.Scope.String(), ScopeID: best.ScopeID}
	return decision, nil
}

// ByBinding decides a permission again on the binding an earlier decision
// recorded, by its name: that binding still exists, still lists the
// selector that matched, its role still grants the permission at a scope
// that covers the resource (as pinned), and the principal is not disabled.
// No group membership and no claim is read again: the selector is held to
// the binding, not matched to the principal again (authn-authz.md §7).
func ByBinding(policy *Policy, principalID string, granted GrantedBinding, perm Permission, r Resource) (Decision, error) {
	if policy == nil {
		return Decision{}, ErrNoPolicy
	}
	if !Known(perm) {
		return Decision{}, &UnknownActionError{Action: Action(perm)}
	}
	scope := r.String()
	decision := Decision{Action: Action(perm), Subject: principalID, Scope: scope, Generation: policy.Revision,
		DocumentReadAt: policy.ReadAt}
	denied := &DeniedError{Subject: principalID, Permission: perm, Scope: scope, Reason: ReasonPermissionDenied}
	if principalID == "" {
		denied.Reason = ReasonUnauthenticated
		return decision, denied
	}
	if policy.Disabled(principalID) {
		denied.Reason = ReasonPrincipalDisabled
		return decision, denied
	}
	for b := range policy.allBindings {
		if b.Name != granted.Name || b.Rule != granted.Rule {
			continue
		}
		if b.ScopeID != granted.ScopeID || !b.Scope.covers(r) || pinnedElsewhere(b, r) || !grantableAt(perm, b.Scope.Kind) {
			return decision, denied
		}
		perms, ok := policy.RolePermissions(b.Role)
		if !ok || !perms[perm] {
			return decision, denied
		}
		for _, sel := range b.Subjects {
			if sel.String() == granted.Subject {
				decision.Rule = b.Rule
				decision.Granted = &GrantedBinding{Name: b.Name, Rule: b.Rule, Subject: sel.String(), Role: b.Role,
					Scope: b.Scope.String(), ScopeID: b.ScopeID}
				return decision, nil
			}
		}
		return decision, denied
	}
	return decision, denied
}

// HoldsWithin reports whether sub holds perm at some scope inside namespace
// (the namespace itself, or one of its Orkflows); namespace "" means at any
// scope at all. A list a caller may read only part of is answered only to a
// caller who holds something there.
func (p *Policy) HoldsWithin(sub *Subject, perm Permission, namespace string) bool {
	if p == nil || sub == nil || sub.ID == "" || p.Disabled(sub.ID) || !Known(perm) {
		return false
	}
	for b := range p.allBindings {
		r := Resource{Namespace: b.Scope.Namespace, Orkflow: b.Scope.Orkflow, Pool: b.Scope.Pool}
		if namespace != "" {
			if b.Scope.Kind != ScopeDeployment && b.Scope.Namespace != namespace {
				continue
			}
			r.Namespace, r.Pool = namespace, ""
		}
		r.NamespaceID, r.OrkflowID = b.ScopeID, b.ScopeID
		if _, ok := p.grants(b, perm, r, sub); ok {
			return true
		}
	}
	return false
}
