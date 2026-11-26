package authz

import (
	"fmt"
	"strings"
)

// Match is why one binding did or did not grant a permission.
type Match int

const (
	MatchGranted Match = iota + 1
	MatchSubjectDiffers
	MatchScopeDiffers
	MatchRoleLacksPermission
	MatchPinnedElsewhere
	MatchSharedOrkerCredential
)

// Explained is one binding's part in a decision.
type Explained struct {
	Binding string
	Match   Match
	// Subject is the selector that matched the principal, when one did.
	Subject        string
	Role           string
	Scope          string
	ClaimsReliedOn []string
}

// Explanation is a decision with the bindings behind it: every binding that
// names the principal or covers the resource, each with why it did or did
// not grant, and the one that granted.
type Explanation struct {
	Decision   Decision
	Err        error
	GrantedBy  *Explained
	Considered []Explained
}

// Explain decides perm for sub on r, as Authorize does, and says why.
func Explain(p *Policy, sub *Subject, perm Permission, r Resource) Explanation {
	d, err := Authorize(Request{Action: Action(perm), Subject: sub, Resource: r, Policy: p})
	ex := Explanation{Decision: d, Err: err}
	if p == nil {
		return ex
	}
	for b := range p.allBindings {
		var matched *Selector
		for i := range b.Subjects {
			if b.Subjects[i].Matches(sub) {
				matched = &b.Subjects[i]
				break
			}
		}
		covers := b.Scope.covers(r)
		if matched == nil && !covers {
			continue
		}
		e := Explained{Binding: b.Name, Role: b.Role, Scope: b.Scope.String()}
		switch {
		case matched == nil:
			e.Match = MatchSubjectDiffers
		case !covers:
			e.Match = MatchScopeDiffers
		case pinnedElsewhere(b, r):
			e.Match = MatchPinnedElsewhere
		case sharedCredentialLacks(b, perm) && p.roleGrants(b, perm):
			e.Match = MatchSharedOrkerCredential
		case !p.bindingGrants(b, perm):
			e.Match = MatchRoleLacksPermission
		default:
			e.Match = MatchGranted
		}
		if matched != nil {
			e.Subject, e.ClaimsReliedOn = matched.String(), matched.ClaimsReliedOn()
		}
		ex.Considered = append(ex.Considered, e)
		if err == nil && d.Granted != nil && b.Name == d.Granted.Name && b.Rule == d.Granted.Rule {
			granted := e
			ex.GrantedBy = &granted
		}
	}
	return ex
}

// SubjectFromID is the subject a principal id names, with no group and no
// claim but `sub`: what an explanation knows of a principal from its id
// alone. A binding that selects by group, or by any other claim, is
// explained as not matching it.
func SubjectFromID(id string) (*Subject, error) {
	kind, rest, ok := strings.Cut(id, ":")
	if !ok || rest == "" || len(id) > MaxPrincipalIDBytes || !printable(id) {
		return nil, fmt.Errorf("%q is not a principal id (user:, service:, serviceaccount: or cert:)", id)
	}
	switch kind {
	case "user", "service":
		issuer, name, ok := strings.Cut(rest, ":")
		if !ok || issuer == "" || name == "" {
			return nil, fmt.Errorf("%q is not a principal id: %s:<issuer>:<name>", id, kind)
		}
		sub := &Subject{ID: id, Kind: kind, Issuer: issuer}
		if kind == "service" {
			sub.Claims = map[string]string{"sub": name}
		}
		return sub, nil
	case "serviceaccount", "cert":
		return &Subject{ID: id, Kind: "service", Issuer: kind}, nil
	}
	return nil, fmt.Errorf("%q is not a principal id (user:, service:, serviceaccount: or cert:)", id)
}

// Holding is one binding that grants sub something, with the permissions it
// grants at its own scope.
type Holding struct {
	Binding     Explained
	Permissions []Permission
}

// Holdings lists what sub holds: every binding, configuration's included,
// one of whose subjects matches it and whose role grants something at its
// scope, in the document's order and then configuration's. A disabled
// principal holds nothing.
func (p *Policy) Holdings(sub *Subject) []Holding {
	if p == nil || sub == nil || sub.ID == "" || p.Disabled(sub.ID) {
		return nil
	}
	var out []Holding
	for b := range p.allBindings {
		var matched *Selector
		for i := range b.Subjects {
			if b.Subjects[i].Matches(sub) {
				matched = &b.Subjects[i]
				break
			}
		}
		if matched == nil {
			continue
		}
		h := Holding{Binding: Explained{Binding: b.Name, Match: MatchGranted, Subject: matched.String(), Role: b.Role,
			Scope: b.Scope.String(), ClaimsReliedOn: matched.ClaimsReliedOn()}}
		for _, perm := range Permissions() {
			if p.bindingGrants(b, perm) {
				h.Permissions = append(h.Permissions, perm)
			}
		}
		if len(h.Permissions) > 0 {
			out = append(out, h)
		}
	}
	return out
}
