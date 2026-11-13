package jwt

import (
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"
	"strings"
)

// maxKidBytes bounds a key id. A thumbprint is 43 bytes; issuers use
// shorter or comparable ids.
const maxKidBytes = 256

// TypeRequest is the typ of a self-signed assertion bound to one request
// (authn-authz.md §3.2). Only VerifyRequest takes a token of this type,
// and VerifyRequest takes no other.
const TypeRequest = "ork-request+jwt"

// typeAccessToken is RFC 9068's access-token type, never taken for an ID
// token (authn-authz.md §2, rule 7).
const typeAccessToken = "at+jwt"

// reservedTypes are the types OrkStack mints for a purpose of its own. A
// verifier of another purpose refuses them, so a token made for one use is
// never taken for another.
var reservedTypes = []string{TypeRequest}

// forbiddenHeaders are refused wherever they appear (authn-authz.md §2,
// rule 2): crit would ask for semantics this package does not implement,
// and the rest would let a token name its own key.
var forbiddenHeaders = []string{"crit", "jku", "x5u", "jwk", "x5c"}

// ruledHeaders are the parameters the rules read; each must be written in
// lower case.
var ruledHeaders = []string{"alg", "kid", "typ"}

// header is what the rules read from a token's protected header.
type header struct {
	alg Algorithm
	kid string
	typ string // normalised: lower case, without "application/"
}

// typeRule says which typ values a verifier takes.
type typeRule int

const (
	// typeIssued: any typ, or none, except a reserved one.
	typeIssued typeRule = iota
	// typeIDToken: as typeIssued, and never at+jwt.
	typeIDToken
	// typeRequestOnly: exactly TypeRequest.
	typeRequestOnly
)

// split checks rule 1 (size, three base64url segments without padding) and
// returns the decoded header and payload segments' raw bytes. The signature
// segment is only checked for its alphabet; the library decodes it.
func split(token string) (headerJSON []byte, segments [3]string, err *Error) {
	if len(token) > MaxTokenBytes {
		return nil, segments, refuse(ReasonTooLarge, "token is %d bytes; at most %d", len(token), MaxTokenBytes)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, segments, refuse(ReasonMalformed, "a token has three segments; this one has %d", len(parts))
	}
	for i, part := range parts {
		if part == "" {
			return nil, segments, refuse(ReasonMalformed, "segment %d is empty", i+1)
		}
		if !isBase64URL(part) {
			return nil, segments, refuse(ReasonMalformed, "segment %d is not base64url without padding", i+1)
		}
		segments[i] = part
	}
	headerJSON, decodeErr := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if decodeErr != nil {
		return nil, segments, refuseWrap(ReasonMalformed, decodeErr, "header is not base64url")
	}
	return headerJSON, segments, nil
}

func isBase64URL(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// decodeObject reads one JSON object, refusing duplicate member names,
// invalid UTF-8 and anything after the object (encoding/json/v2's
// defaults).
func decodeObject(raw []byte) (map[string]jsontext.Value, error) {
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members, jsontext.AllowDuplicateNames(false), jsontext.AllowInvalidUTF8(false)); err != nil {
		return nil, err
	}
	if members == nil {
		return nil, errNotObject
	}
	return members, nil
}

var errNotObject = &Error{Reason: ReasonMalformed, Detail: "not a JSON object"}

// stringMember returns a member that is a JSON string. ok is false when the
// member is absent; err is set when it is present and not a string.
func stringMember(members map[string]jsontext.Value, name string) (value string, ok bool, err error) {
	raw, present := members[name]
	if !present {
		return "", false, nil
	}
	if raw.Kind() != '"' {
		return "", true, refuse(ReasonMalformed, "%s is not a string", name)
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", true, refuseWrap(ReasonMalformed, err, "%s is not a string", name)
	}
	return value, true, nil
}

// checkHeader applies every header rule (authn-authz.md §2, rule 2) to the
// raw header, before any library call or key lookup.
func checkHeader(headerJSON []byte, allowed []Algorithm, rule typeRule) (header, *Error) {
	members, err := decodeObject(headerJSON)
	if err != nil {
		return header{}, refuseWrap(ReasonMalformed, err, "header is not one JSON object")
	}
	for name := range members {
		lower := strings.ToLower(name)
		if slices.Contains(forbiddenHeaders, lower) {
			return header{}, refuse(ReasonHeader, "header parameter %q is refused", lower)
		}
		// A library may read names without regard to case: a name that is
		// one of ours in another case is never ignored, it is refused.
		if lower != name && slices.Contains(ruledHeaders, lower) {
			return header{}, refuse(ReasonHeader, "header parameter %q is written in the wrong case", name)
		}
	}
	alg, present, aErr := stringMember(members, "alg")
	if aErr != nil || !present {
		return header{}, refuse(ReasonAlgorithm, "alg is required and must be a string")
	}
	if !slices.Contains(allowed, Algorithm(alg)) {
		return header{}, refuse(ReasonAlgorithm, "alg %q is not one this issuer is configured for", alg)
	}
	kid, present, kErr := stringMember(members, "kid")
	if kErr != nil || !present || kid == "" {
		return header{}, refuse(ReasonHeader, "kid is required and must be a non-empty string")
	}
	if len(kid) > maxKidBytes {
		return header{}, refuse(ReasonHeader, "kid is %d bytes; at most %d", len(kid), maxKidBytes)
	}
	typ, present, tErr := stringMember(members, "typ")
	if tErr != nil {
		return header{}, refuse(ReasonHeader, "typ must be a string")
	}
	typ = normaliseType(typ)
	switch rule {
	case typeRequestOnly:
		if !present || typ != TypeRequest {
			return header{}, refuse(ReasonBinding, "a self-signed assertion must have typ %q: it is not bound to a request", TypeRequest)
		}
	case typeIDToken:
		if typ == typeAccessToken {
			return header{}, refuse(ReasonHeader, "a token of typ %q is never taken for an ID token", typeAccessToken)
		}
		fallthrough
	case typeIssued:
		if slices.Contains(reservedTypes, typ) {
			return header{}, refuse(ReasonHeader, "a token of typ %q is not taken here", typ)
		}
	}
	return header{alg: Algorithm(alg), kid: kid, typ: typ}, nil
}

// normaliseType compares typ as RFC 7515 §4.1.9 asks: case-insensitively,
// with "application/" optional when no other slash appears.
func normaliseType(typ string) string {
	typ = strings.ToLower(typ)
	if rest, ok := strings.CutPrefix(typ, "application/"); ok && !strings.Contains(rest, "/") {
		return rest
	}
	return typ
}
