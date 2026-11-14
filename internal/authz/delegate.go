package authz

// MayDelegate decides whether sub, holding access.bind but not access.write,
// may create or delete a binding of role at scope (authn-authz.md §5.4): the
// scope is a namespace or one of its Orkflows, sub holds access.bind at that
// namespace, the role is not admin, and sub itself holds there every
// permission the binding would grant. A permission the role holds that no
// binding at that scope can grant (a deployment's own, at a namespace) is
// inert there, so it is not asked of sub.
//
// A refusal is a *DeniedError naming the first permission sub lacks, or
// access.bind for a binding no delegation may make: one that would grant
// nothing at its scope, or of a role nobody declared.
func MayDelegate(p *Policy, sub *Subject, role string, scope Scope) error {
	if p == nil {
		return ErrNoPolicy
	}
	res := Resource{Namespace: scope.Namespace, Orkflow: scope.Orkflow, Pool: scope.Pool}
	refuse := func(perm Permission) error {
		denied := &DeniedError{Permission: perm, Scope: res.String(), Reason: ReasonUnauthenticated}
		if sub != nil && sub.ID != "" {
			denied.Subject, denied.Reason = sub.ID, ReasonPermissionDenied
		}
		return denied
	}
	if scope.Kind != ScopeNamespace && scope.Kind != ScopeOrkflow {
		return refuse(PermAccessBind)
	}
	if role == RoleAdmin {
		return refuse(PermAccessBind)
	}
	perms, ok := p.RolePermissions(role)
	if !ok {
		// A role nobody declared grants nothing anyone holds: it is refused
		// as the binding no delegation makes, and the write's validation
		// names the missing role to a caller who may write the document.
		return refuse(PermAccessBind)
	}
	if _, err := Authorize(Request{Action: Action(PermAccessBind), Subject: sub,
		Resource: Resource{Namespace: scope.Namespace}, Policy: p}); err != nil {
		return err
	}
	granted := 0
	for _, perm := range Permissions() {
		if !perms[perm] || !grantableAt(perm, scope.Kind) {
			continue
		}
		if _, err := Authorize(Request{Action: Action(perm), Subject: sub, Resource: res, Policy: p}); err != nil {
			return err
		}
		granted++
	}
	if granted == 0 {
		// A binding that would grant nothing at its scope (the orker role's,
		// at a namespace) is no delegation: it is refused, not stored.
		return refuse(PermAccessBind)
	}
	return nil
}
