package jwt

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// maxNumericDate is 9999-12-31T23:59:59Z: a later instant is not a date a
// token means.
const maxNumericDate = 253402300799

// maxIntegerDigits is the longest JSON integer matched as a string (an
// owner id; authn-authz.md §3.1): a longer one may not survive a float.
const maxIntegerDigits = 15

// Claims are a verified token's claims. The registered ones are read into
// fields by this package's rules; every other member stays as JSON and is
// read only through the accessors below, which say what each type means.
type Claims struct {
	Issuer    string
	Subject   string
	Audience  []string
	Expiry    time.Time
	IssuedAt  time.Time
	NotBefore time.Time // zero when the token has no nbf
	ID        string    // jti; "" when absent

	members map[string]jsontext.Value
}

// parseClaims reads the registered claims from a payload already decoded
// with duplicate names refused (authn-authz.md §2, rules 4 and 5). It
// checks their types and presence; their values are checked by the caller.
func parseClaims(members map[string]jsontext.Value) (*Claims, *Error) {
	c := &Claims{members: members}
	var err *Error
	if c.Issuer, err = requiredString(members, "iss"); err != nil {
		return nil, err
	}
	if c.Subject, err = requiredString(members, "sub"); err != nil {
		return nil, err
	}
	if c.Audience, err = audience(members); err != nil {
		return nil, err
	}
	exp, present, dErr := numericDate(members, "exp")
	if dErr != nil {
		return nil, dErr
	}
	if !present {
		return nil, refuse(ReasonClaims, "exp is required")
	}
	c.Expiry = exp
	iat, present, dErr := numericDate(members, "iat")
	if dErr != nil {
		return nil, dErr
	}
	if !present {
		return nil, refuse(ReasonClaims, "iat is required")
	}
	c.IssuedAt = iat
	nbf, _, dErr := numericDate(members, "nbf")
	if dErr != nil {
		return nil, dErr
	}
	c.NotBefore = nbf
	jti, _, jErr := stringMember(members, "jti")
	if jErr != nil {
		return nil, refuse(ReasonClaims, "jti must be a string")
	}
	c.ID = jti
	return c, nil
}

func requiredString(members map[string]jsontext.Value, name string) (string, *Error) {
	v, present, err := stringMember(members, name)
	if err != nil || !present || v == "" {
		return "", refuse(ReasonClaims, "%s is required and must be a non-empty string", name)
	}
	return v, nil
}

// audience reads aud: a string, or a non-empty array of strings.
func audience(members map[string]jsontext.Value) ([]string, *Error) {
	raw, present := members["aud"]
	if !present {
		return nil, refuse(ReasonClaims, "aud is required")
	}
	switch raw.Kind() {
	case '"':
		var s string
		if json.Unmarshal(raw, &s) != nil || s == "" {
			return nil, refuse(ReasonClaims, "aud must be a non-empty string or an array of them")
		}
		return []string{s}, nil
	case '[':
		var list []string
		if json.Unmarshal(raw, &list) != nil || len(list) == 0 || slices.Contains(list, "") {
			return nil, refuse(ReasonClaims, "aud must be a non-empty string or an array of them")
		}
		return list, nil
	}
	return nil, refuse(ReasonClaims, "aud must be a non-empty string or an array of them")
}

// numericDate reads a NumericDate (RFC 7519 §2): a non-negative JSON number
// of seconds, fractions allowed, no later than year 9999.
func numericDate(members map[string]jsontext.Value, name string) (time.Time, bool, *Error) {
	raw, present := members[name]
	if !present {
		return time.Time{}, false, nil
	}
	if raw.Kind() != '0' {
		return time.Time{}, true, refuse(ReasonClaims, "%s must be a number of seconds", name)
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f > maxNumericDate {
		return time.Time{}, true, refuse(ReasonClaims, "%s is not a date", name)
	}
	sec, frac := math.Modf(f)
	return time.Unix(int64(sec), int64(frac*1e9)).UTC(), true, nil
}

// Has reports whether the token carries the claim.
func (c *Claims) Has(name string) bool {
	_, ok := c.members[name]
	return ok
}

// Names lists every claim the token carries, sorted.
func (c *Claims) Names() []string {
	names := make([]string, 0, len(c.members))
	for name := range c.members {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// String returns a claim that is a JSON string.
func (c *Claims) String(name string) (string, bool) {
	v, present, err := stringMember(c.members, name)
	return v, present && err == nil
}

// Bool returns a claim that is a JSON boolean.
func (c *Claims) Bool(name string) (value, ok bool) {
	raw, present := c.members[name]
	if !present {
		return false, false
	}
	switch raw.Kind() {
	case 't':
		return true, true
	case 'f':
		return false, true
	}
	return false, false
}

// Lookup returns the JSON value at path, each element an object member's
// name, so a name that holds a dot is reached whole
// ("kubernetes.io", "pod", "name").
func (c *Claims) Lookup(path ...string) (jsontext.Value, bool) {
	if len(path) == 0 {
		return nil, false
	}
	value, ok := c.members[path[0]]
	for _, name := range path[1:] {
		if !ok || value.Kind() != '{' {
			return nil, false
		}
		var next map[string]jsontext.Value
		if json.Unmarshal(value, &next, jsontext.AllowDuplicateNames(false)) != nil {
			return nil, false
		}
		value, ok = next[name]
	}
	return value, ok
}

// Strings returns the claim at path as strings, by the one rule a claim is
// matched by (authn-authz.md §3.1): a string is itself; an array is its
// elements, all of which must be strings; an integer of at most 15 digits,
// with no fraction and no exponent, is its decimal digits. Any other value
// (a boolean, another number, an object, an array holding anything but
// strings) gives ok false: an unexpected type fails closed.
func (c *Claims) Strings(path ...string) ([]string, bool) {
	value, ok := c.Lookup(path...)
	if !ok {
		return nil, false
	}
	return matchStrings(value)
}

func matchStrings(value jsontext.Value) ([]string, bool) {
	switch value.Kind() {
	case '"':
		var s string
		if json.Unmarshal(value, &s) != nil {
			return nil, false
		}
		return []string{s}, true
	case '[':
		var list []string
		if json.Unmarshal(value, &list) != nil {
			return nil, false
		}
		return list, true
	case '0':
		digits := string(value)
		if len(digits) == 0 || len(digits) > maxIntegerDigits {
			return nil, false
		}
		if strings.Trim(digits, "0123456789") != "" {
			return nil, false // a sign, a fraction or an exponent
		}
		if len(digits) > 1 && digits[0] == '0' {
			return nil, false
		}
		return []string{digits}, true
	}
	return nil, false
}

// Matches reports whether the claim at path holds want, by the rule of
// Strings.
func (c *Claims) Matches(want string, path ...string) bool {
	values, ok := c.Strings(path...)
	return ok && slices.Contains(values, want)
}
