package authn

import (
	"errors"
	"fmt"
	"slices"

	"github.com/orkstack/orkstack/internal/jwt"
)

// ErrClaims is a verified token whose claims this issuer's rules refuse.
var ErrClaims = errors.New("the token's claims do not satisfy the issuer's rules")

// PrincipalFromClaims maps a token internal/jwt has verified against this
// issuer's Policy to a principal (authn-authz.md §3.1, §4). It checks
// required_claims, reads the username, a verified email when the issuer
// requires one, groups and the issuer's listed claims, and drops every
// other claim. method is MethodJWT for a bearer token or MethodOIDC for a
// sign-in.
//
// An array of more than MaxGroups groups, or one holding a group over
// MaxGroupBytes, yields no groups at all, as an issuer's overage pointer
// does: group bindings then do not match, which fails closed.
// groupsDropped says so, for the one log line a sign-in gets.
func PrincipalFromClaims(i *Issuer, c *jwt.Claims, method string) (p *Principal, groupsDropped bool, err error) {
	if i == nil || c == nil {
		return nil, false, errors.New("no issuer or no claims")
	}
	if c.Issuer != i.Issuer {
		return nil, false, fmt.Errorf("%w: iss is not issuer %q's", ErrClaims, i.Name)
	}
	for _, name := range sortedKeys(i.RequiredClaims) {
		if !c.Matches(i.RequiredClaims[name], name) {
			return nil, false, fmt.Errorf("%w: required claim %q does not hold the configured value", ErrClaims, name)
		}
	}
	username, ok := single(c, i.UsernameClaim)
	if !ok {
		return nil, false, fmt.Errorf("%w: username claim %q is not one non-empty string", ErrClaims, i.UsernameClaim)
	}
	if i.RequireVerifiedEmail {
		if verified, ok := c.Bool("email_verified"); !ok || !verified {
			return nil, false, fmt.Errorf("%w: the email is not verified", ErrClaims)
		}
	}

	p = &Principal{Kind: i.Kind, Issuer: i.Name, Method: method}
	if i.Kind == KindService {
		p.ID = ServiceID(i.Name, c.Subject)
	} else {
		p.ID = UserID(i.Name, username)
	}
	if err := checkID(p.ID); err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrClaims, err)
	}
	p.Display = username
	for _, name := range []string{"email", "name"} {
		if v, ok := single(c, name); ok && printable(v) && len(v) <= MaxIDBytes {
			p.Display = v
			break
		}
	}

	if i.GroupsClaim != "" {
		groups, ok := c.Strings(i.GroupsClaim)
		switch {
		case !ok:
			groupsDropped = c.Has(i.GroupsClaim) // present but of another type, or an overage pointer
		case len(groups) > MaxGroups || slices.ContainsFunc(groups, func(g string) bool { return g == "" || len(g) > MaxGroupBytes }):
			groupsDropped = true
		default:
			p.Groups = slices.Clone(groups)
		}
	}

	if len(i.Claims) > 0 {
		p.Claims = make(map[string]string, len(i.Claims))
		for _, name := range i.Claims {
			if v, ok := single(c, name); ok {
				p.Claims[name] = v
			}
		}
	}

	if len(i.OrkerIDClaim) > 0 {
		values, ok := c.Strings(i.OrkerIDClaim...)
		if ok && len(values) == 1 && values[0] != "" && len(values[0]) <= MaxIDBytes {
			p.OrkerID = values[0]
		}
	}
	return p, groupsDropped, nil
}

// single returns a claim that holds exactly one non-empty string by the
// matching rule of jwt.Claims.Strings.
func single(c *jwt.Claims, name string) (string, bool) {
	values, ok := c.Strings(name)
	if !ok || len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
