package authz

import (
	"cmp"
	"fmt"
	"slices"
)

// BinderBinding is the binding that granted a schedule's, a trigger's or an
// event source's binder the permission to submit its Orkflow, recorded at
// the save that made them the binder (workload-identity §3.2): its name,
// the subject selector that matched, its role and its scope. Later
// admissions judge the binder by it alone, from the access document, and
// never read a group or a claim again.
type BinderBinding struct {
	Name, Selector, Role, Scope string
}

// GrantingBinding answers the binding of p that grants perm on r to sub:
// the principal's own binding first (one whose selector names exactly its
// id), then the first by name. ok is false when none grants.
func (p *Policy) GrantingBinding(sub *Subject, perm Permission, r Resource) (BinderBinding, bool) {
	if sub == nil || sub.ID == "" || p.Disabled(sub.ID) {
		return BinderBinding{}, false
	}
	type found struct {
		b   Binding
		sel Selector
	}
	var all []found
	p.allBindings(func(b Binding) bool {
		if sel, ok := p.grants(b, perm, r, sub); ok {
			all = append(all, found{b, sel})
		}
		return true
	})
	if len(all) == 0 {
		return BinderBinding{}, false
	}
	slices.SortStableFunc(all, func(a, b found) int {
		if own, other := a.sel.namesOnly(sub.ID), b.sel.namesOnly(sub.ID); own != other {
			if own {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.b.Name, b.b.Name)
	})
	f := all[0]
	return BinderBinding{Name: f.b.Name, Selector: f.sel.String(), Role: f.b.Role, Scope: f.b.Scope.String()}, true
}

// StillGrants reports whether recorded still grants perm on r to the binder
// id: the binding of that name exists, still lists that selector, its role
// still holds perm, its scope still covers r, and the binder is not under
// disabled. No group or claim is read: what matched at the save stands
// while the binding does. why says what no longer holds.
func (p *Policy) StillGrants(recorded BinderBinding, id string, perm Permission, r Resource) (ok bool, why string) {
	if p.Disabled(id) {
		return false, fmt.Sprintf("%s is disabled", id)
	}
	var binding *Binding
	p.allBindings(func(b Binding) bool {
		if b.Name == recorded.Name {
			binding = &b
			return false
		}
		return true
	})
	switch {
	case binding == nil:
		return false, fmt.Sprintf("binding %q no longer exists", recorded.Name)
	case !slices.ContainsFunc(binding.Subjects, func(s Selector) bool { return s.String() == recorded.Selector }):
		return false, fmt.Sprintf("binding %q no longer lists %s", recorded.Name, recorded.Selector)
	case !binding.Scope.covers(r) || pinnedElsewhere(*binding, r) || !grantableAt(perm, binding.Scope.Kind):
		return false, fmt.Sprintf("binding %q no longer covers %s", recorded.Name, r)
	}
	if !p.bindingGrants(*binding, perm) {
		return false, fmt.Sprintf("binding %q's role %s no longer holds %s", recorded.Name, binding.Role, perm)
	}
	return true, ""
}
