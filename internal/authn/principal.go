// Package authn turns a credential into a Principal: the trusted-issuers
// document (auth.issuers), each issuer's key-set cache, and the mapping
// from a verified token's claims to the principal records carry
// (authn-authz.md §3, §4). The JOSE rules themselves are internal/jwt's.
//
// Nothing here is enforced yet: the middleware that calls it, and the modes
// that decide whether it runs, come with the authentication interceptor.
package authn

import (
	"context"
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"
)

// Kind is what a principal is.
type Kind string

// The two kinds of principal.
const (
	KindUser    Kind = "user"
	KindService Kind = "service"
)

// The methods a principal authenticated by.
const (
	MethodOIDC           = "oidc"
	MethodJWT            = "jwt"
	MethodServiceAccount = "service-account"
	MethodMTLS           = "mtls"
	MethodBreakGlass     = "break-glass"
)

// Bounds on what a principal carries.
const (
	MaxIDBytes    = 256
	MaxGroups     = 200
	MaxGroupBytes = 256
)

// Principal is a verified caller. Its ID is what records carry.
type Principal struct {
	Kind    Kind
	ID      string            // stable, at most MaxIDBytes
	Display string            // a name or email, for the UI
	Issuer  string            // the trusted issuer's name, "serviceaccount", "cert" or "config"
	Method  string            // one of the Method constants
	Groups  []string          // from the issuer's groups_claim, at most MaxGroups
	Claims  map[string]string // only the issuer's listed claims
	Signin  string            // the sign-in's public id, for a cookie
	OrkerID string            // the one Orker this credential names; empty otherwise
}

// UserID is a person's principal id: user:<issuer>:<username>.
func UserID(issuer, username string) string { return "user:" + issuer + ":" + username }

// ServiceID is a trusted issuer's service principal id:
// service:<issuer>:<sub>.
func ServiceID(issuer, sub string) string { return "service:" + issuer + ":" + sub }

// ServiceAccountID is a service account's principal id.
func ServiceAccountID(name string) string { return "serviceaccount:" + name }

// CertID is a client certificate's principal id: cert:<san or cn>.
func CertID(name string) string { return "cert:" + name }

func checkID(id string) error {
	if id == "" || len(id) > MaxIDBytes {
		return fmt.Errorf("a principal id is 1 to %d bytes; this one is %d", MaxIDBytes, len(id))
	}
	if !printable(id) {
		return errors.New("a principal id holds only printable UTF-8; this one holds a control character or invalid UTF-8")
	}
	return nil
}

// printable reports valid UTF-8 with no control character: what a record
// or a log line may carry verbatim.
func printable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

type principalKey struct{}

// WithPrincipal returns ctx carrying p.
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the verified principal ctx carries, or nil when the
// request has none.
func FromContext(ctx context.Context) *Principal {
	p, _ := ctx.Value(principalKey{}).(*Principal)
	return p
}
