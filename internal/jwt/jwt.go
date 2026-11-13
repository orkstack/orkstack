// Package jwt holds the rules OrkStack keeps for a JSON Web Token, on top of
// github.com/go-jose/go-jose/v4. It is the only package that imports the
// library (TestJWT_OnlyThisPackageImportsTheLibrary): parsing, signature
// verification, signing, key sets and thumbprints are the library's, and
// what a token must satisfy is decided here.
//
// A token is judged in three steps, in this order:
//
//  1. Before the library sees it: its size, its three segments and every
//     header rule are read from the raw header (checkHeader). A token
//     refused here never reaches a key source.
//  2. The library: one key, chosen here by kid and checked for its type and
//     size against the algorithm, and the issuer's algorithm allow-list,
//     which the library's own parse enforces a second time.
//  3. After the library: every claim, read here from the verified payload
//     with duplicate names refused. None is left to a library default.
//
// Signing goes through the same package (Signer), so one piece of code
// decides how OrkStack writes a JWS and how it reads one.
package jwt

import (
	"errors"
	"fmt"
)

// Algorithm is a JWS signature algorithm OrkStack accepts. The set is
// closed: RS256 and ES256. none, the HMAC family and every other value are
// never an Algorithm.
type Algorithm string

// The two algorithms OrkStack signs and verifies with.
const (
	RS256 Algorithm = "RS256"
	ES256 Algorithm = "ES256"
)

// Algorithms lists every Algorithm, in a fixed order.
func Algorithms() []Algorithm { return []Algorithm{RS256, ES256} }

// ParseAlgorithm returns the Algorithm a configuration names, refusing
// every other value, none and HS256/384/512 among them.
func ParseAlgorithm(name string) (Algorithm, error) {
	switch Algorithm(name) {
	case RS256, ES256:
		return Algorithm(name), nil
	}
	return "", fmt.Errorf("algorithm %q is not accepted (accepted: RS256, ES256)", name)
}

// MaxTokenBytes bounds a token in its compact form (authn-authz.md §2,
// rule 1).
const MaxTokenBytes = 16 << 10

// Reason is a machine-readable cause of a refusal. The set is closed.
type Reason string

// Every reason a token is refused for.
const (
	// ReasonMalformed: not three base64url segments, or a segment that is
	// not a JSON object.
	ReasonMalformed Reason = "malformed"
	// ReasonTooLarge: over MaxTokenBytes.
	ReasonTooLarge Reason = "too_large"
	// ReasonAlgorithm: an alg the issuer's configuration does not list.
	ReasonAlgorithm Reason = "algorithm"
	// ReasonHeader: a header the rules refuse (no kid, crit, jku, x5u,
	// jwk, x5c, or a typ this verifier does not take).
	ReasonHeader Reason = "header"
	// ReasonKey: no usable key for the kid, or a key of the wrong type or
	// size for the algorithm.
	ReasonKey Reason = "key"
	// ReasonSignature: the signature does not verify.
	ReasonSignature Reason = "signature"
	// ReasonClaims: a claim missing, of the wrong type, or not equal to
	// what is required.
	ReasonClaims Reason = "claims"
	// ReasonExpired: past exp, with leeway.
	ReasonExpired Reason = "expired"
	// ReasonNotYetValid: before nbf or iat, with leeway.
	ReasonNotYetValid Reason = "not_yet_valid"
	// ReasonLifetime: exp - iat over the issuer's maximum lifetime.
	ReasonLifetime Reason = "lifetime"
	// ReasonBinding: a self-signed assertion not bound to this request.
	ReasonBinding Reason = "binding"
	// ReasonReplay: an assertion's jti already seen at this replica.
	ReasonReplay Reason = "replay"
	// ReasonUnavailable: the keys or the replay cache could not answer;
	// never a reason to accept.
	ReasonUnavailable Reason = "unavailable"
)

// Error is a refusal: a Reason and what was wrong, never the token.
type Error struct {
	Reason Reason
	Detail string
	err    error
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return "token refused: " + string(e.Reason)
	}
	return "token refused (" + string(e.Reason) + "): " + e.Detail
}

func (e *Error) Unwrap() error { return e.err }

func refuse(reason Reason, format string, args ...any) *Error {
	return &Error{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

func refuseWrap(reason Reason, err error, format string, args ...any) *Error {
	return &Error{Reason: reason, Detail: fmt.Sprintf(format, args...), err: err}
}

// Unavailable is a refusal for a key source that could not answer. cause
// is kept for logs through Unwrap and never shown in the message, which a
// client may see.
func Unavailable(detail string, cause error) *Error {
	return &Error{Reason: ReasonUnavailable, Detail: detail, err: cause}
}

// ReasonOf returns the Reason of a refusal, or "" for an error that is not
// one.
func ReasonOf(err error) Reason {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}
