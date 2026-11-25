package provider

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// MaxProviders bounds the providers document.
const MaxProviders = 16

// MaxNameBytes bounds a provider's name, which is a secret-source scheme
// and so follows that grammar (internal/dsl, MaxSecretSchemeBytes).
const MaxNameBytes = 16

var nameShape = regexp.MustCompile(`^[a-z][a-z0-9]*$`)

// reservedNames are the secret-source schemes the Orker resolves itself.
// A provider may not take one: a source names exactly one resolver.
var reservedNames = map[string]bool{"env": true, "file": true}

// ValidName reports why name cannot be a provider's name, or nil.
func ValidName(name string) error {
	if name == "" {
		return fmt.Errorf("a provider's name is empty")
	}
	if len(name) > MaxNameBytes {
		return fmt.Errorf("provider name %q is %d bytes; a name is at most %d", name, len(name), MaxNameBytes)
	}
	if !nameShape.MatchString(name) {
		return fmt.Errorf("provider name %q must match ^[a-z][a-z0-9]*$: it is a secret-source scheme", name)
	}
	if reservedNames[name] {
		return fmt.Errorf("provider name %q is taken: %q is a source the Orker resolves itself", name, name)
	}
	return nil
}

// credentialNameShape is a credential's name, which is a directory name
// under /orkstack/credentials/.
var credentialNameShape = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// MaxCredentials bounds a task's credentials: list.
const MaxCredentials = 8

// ValidCredentialName reports why name cannot name a task's credential, or
// nil. The same shape names a grant.
func ValidCredentialName(name string) error {
	if !credentialNameShape.MatchString(name) {
		return fmt.Errorf("credential name %q must match ^[a-z][a-z0-9-]{0,62}$", name)
	}
	return nil
}

// ValidGrantName reports why name cannot name a grant, or nil.
func ValidGrantName(name string) error {
	if !credentialNameShape.MatchString(name) {
		return fmt.Errorf("grant name %q must match ^[a-z][a-z0-9-]{0,62}$", name)
	}
	return nil
}

// MinBundleLifetime is the shortest lifetime a bundle that expires may have:
// a shorter one would be refreshed about every second.
const MinBundleLifetime = 30 * time.Second

// MaxBundleFiles bounds the files of one bundle.
const MaxBundleFiles = 16

// MaxBundleSecrets bounds the values of one bundle named secret.
const MaxBundleSecrets = 16

var fileNameShape = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// fileNamesTaken are the names the Orker writes beside a bundle's own files.
var fileNamesTaken = map[string]bool{"current": true, "expiration": true}

// ValidFileName reports why name cannot be a file of a bundle, or nil. A name
// is one path component: no "/", and never "." or "..". "current" and
// "expiration" are the Orker's, and a name beginning "." is the Orker's
// version directories'.
func ValidFileName(name string) error {
	if !fileNameShape.MatchString(name) {
		return fmt.Errorf("bundle file name %q must match ^[A-Za-z0-9._-]{1,128}$", name)
	}
	if strings.HasPrefix(name, ".") {
		return fmt.Errorf("bundle file name %q may not begin with \".\"", name)
	}
	if fileNamesTaken[name] {
		return fmt.Errorf("bundle file name %q is the Orker's own", name)
	}
	return nil
}

// ValidBundle reports why b cannot be put in place, or nil: its file names,
// its count, each file's size, and a Plain entry that names no file. It is
// the check the Orker makes of every answer, a refresh's included, so a
// provider that breaks it breaks it the same way everywhere.
func ValidBundle(b Bundle) error {
	if len(b.Files) == 0 {
		return fmt.Errorf("a bundle holds no file")
	}
	if len(b.Files) > MaxBundleFiles {
		return fmt.Errorf("a bundle holds %d files; at most %d", len(b.Files), MaxBundleFiles)
	}
	if b.Lifetime < 0 || (b.Lifetime > 0 && b.Lifetime < MinBundleLifetime) || b.Lifetime > 31*24*time.Hour {
		return fmt.Errorf("a bundle's lifetime is %s; it is zero (does not expire), or from %s to 31 days", b.Lifetime, MinBundleLifetime)
	}
	if len(b.Secrets) > MaxBundleSecrets {
		return fmt.Errorf("a bundle names %d values secret; at most %d", len(b.Secrets), MaxBundleSecrets)
	}
	for _, v := range b.Secrets {
		if len(v) == 0 || len(v) > MaxValueBytes {
			return fmt.Errorf("a bundle names a value of %d bytes secret", len(v))
		}
	}
	names := make([]string, 0, len(b.Files))
	for name := range b.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if ValidFileName(name) != nil {
			// The name is the provider's, and is not echoed: an answer that
			// breaks the rules may hold anything.
			return fmt.Errorf("a bundle file name (%d bytes) is not a valid name", len(name))
		}
		if len(b.Files[name]) > MaxValueBytes {
			return fmt.Errorf("bundle file %s is %d bytes; a file is at most %d", name, len(b.Files[name]), MaxValueBytes)
		}
	}
	for name := range b.Plain {
		if _, ok := b.Files[name]; !ok {
			return fmt.Errorf("a bundle marks a file plain that it does not hold")
		}
	}
	return nil
}

// MaxLocatorBytes bounds a locator: a secret's source after "<name>:", or a
// credential's path.
const MaxLocatorBytes = 1024

// ValidLocator reports why locator cannot be a provider reference's locator,
// or nil. A locator is validated, never normalized, like every name in
// OrkStack: a pattern in allowed_secrets can then never be escaped by a path
// that says one thing and reads another (providers.md §4.3). The rule:
//
//   - at most MaxLocatorBytes bytes, not empty;
//   - only A-Z a-z 0-9 and ". _ / - : @ + =", and at most one "#", which
//     begins the field and is followed by at least one character;
//   - no "%", no "?", no space, no control character, nothing else;
//   - split at "/", no segment is empty, ".", or "..": so no leading or
//     trailing "/", no "//".
//
// The field after "#" follows the same character rule, without "/".
func ValidLocator(locator string) error {
	if locator == "" {
		return fmt.Errorf("a provider reference names no locator")
	}
	if len(locator) > MaxLocatorBytes {
		return fmt.Errorf("locator is %d bytes; at most %d", len(locator), MaxLocatorBytes)
	}
	path, field, hasField := strings.Cut(locator, "#")
	if hasField {
		if field == "" {
			return fmt.Errorf("locator %q has an empty field after \"#\"", locator)
		}
		for i := 0; i < len(field); i++ {
			c := field[i]
			if c == '/' || c == '#' || !locatorByte(c) {
				return fmt.Errorf("locator %q: the field after \"#\" may hold only letters, digits and \". _ - : @ + =\"", printable(locator))
			}
		}
	}
	for i := 0; i < len(path); i++ {
		if !locatorByte(path[i]) {
			return fmt.Errorf("locator %q may hold only letters, digits, \". _ / - : @ + =\" and one \"#\"", printable(locator))
		}
	}
	for _, segment := range strings.Split(path, "/") {
		switch segment {
		case "":
			return fmt.Errorf("locator %q has an empty segment: no leading, trailing or doubled \"/\"", locator)
		case ".", "..":
			return fmt.Errorf("locator %q has a %q segment; a locator is never normalized", locator, segment)
		}
	}
	return nil
}

func locatorByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	switch c {
	case '.', '_', '/', '-', ':', '@', '+', '=':
		return true
	}
	return false
}

// printable quotes what a refusal may show of a value that failed the
// character rule: the rule already refused it, and a control character in a
// message stored in history is a line someone else can forge.
func printable(s string) string {
	if utf8.ValidString(s) && !strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return s
	}
	return strings.ToValidUTF8(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s), "?")
}

// SplitSource splits a secret source "<name>:<locator>" at its first ":".
// ok is false when there is no ":".
func SplitSource(source string) (name, locator string, ok bool) {
	return strings.Cut(source, ":")
}

// PolicyString is the policy string of a provider reference: the provider's
// name, ":", and the locator without its "#<field>" (providers.md §9). A
// field selects within a value the policy already admitted; it is never
// part of what the policy judges.
func PolicyString(name, locator string) string {
	path, _, _ := strings.Cut(locator, "#")
	return name + ":" + path
}

// loginSchemeShape is what a type's LoginScheme may be.
var loginSchemeShape = regexp.MustCompile(`^[a-z][a-z0-9]{0,15}$`)

// Policy is the policy string of a reference to p made as principal, the
// grant's: what Namespace.allowed_secrets and the Orker's floor judge. It is
// computed here from the reference alone, so no provider can say what it is
// judged by. A credential with no path is the login itself,
// "<name>:<scheme>:<principal>"; every other reference is
// "<name>:<locator>". A locator that spells a login is refused, so the two
// never share a string.
func Policy(p Provider, ref Ref, principal string) (string, error) {
	login := p.LoginScheme() + ":"
	switch {
	case ref.Kind != KindSecret && ref.Kind != KindCredential:
		return "", fmt.Errorf("a reference of kind %s has no policy string", ref.Kind)
	case ref.Kind == KindCredential && ref.Locator == "":
		if principal == "" {
			return "", fmt.Errorf("a login to provider %s names no principal", p.Name())
		}
		locator := login + principal
		if err := ValidLocator(locator); err != nil {
			return "", fmt.Errorf("principal: %w", err)
		}
		if strings.Contains(locator, "#") {
			return "", fmt.Errorf("principal %q holds a \"#\"", principal)
		}
		return PolicyString(p.Name(), locator), nil
	}
	if err := ValidLocator(ref.Locator); err != nil {
		return "", err
	}
	if strings.HasPrefix(ref.Locator, login) {
		return "", fmt.Errorf("locator %q spells a login to provider %s; a login is a credential with no path", ref.Locator, p.Name())
	}
	if ref.Kind == KindCredential && strings.Contains(ref.Locator, "#") {
		return "", fmt.Errorf("a credential's path %q names a field", ref.Locator)
	}
	return PolicyString(p.Name(), ref.Locator), nil
}

// The bounds of a credential's params (providers.md §8.3).
const (
	MaxParams          = 16
	MaxParamValueBytes = 1024
	MaxParamsBytes     = 4 << 10
)

var paramKeyShape = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// ValidParams reports why params cannot be sent, or nil. allowed is the
// closed list of keys this reference may set, which the provider decides
// from its kind (a pki issue, an ssh sign); a key outside it is refused.
// The shape is bounded whatever allowed says: strings only (the type says
// so), at most MaxParams keys, a key of ^[a-z][a-z0-9_]{0,62}$, a value of
// at most MaxParamValueBytes bytes of UTF-8 with no control character, and
// MaxParamsBytes in all, keys and values together. A value is a literal
// from the definition and is never templated.
func ValidParams(params map[string]string, allowed []string) error {
	if err := ValidParamsShape(params); err != nil {
		return err
	}
	permitted := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		permitted[key] = true
	}
	for _, key := range slices.Sorted(maps.Keys(params)) {
		if permitted[key] {
			continue
		}
		if len(allowed) == 0 {
			return fmt.Errorf("params: this reference takes no params, and %q is one", key)
		}
		return fmt.Errorf("params key %q is not one this reference may set (%s)", key, strings.Join(allowed, ", "))
	}
	return nil
}

// ValidParamsShape is the half of ValidParams a definition alone can be held
// to, before its provider is known: the count, each key's and value's shape,
// and the total.
func ValidParamsShape(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}
	if len(params) > MaxParams {
		return fmt.Errorf("params has %d keys; at most %d", len(params), MaxParams)
	}
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	total := 0
	for _, key := range keys {
		value := params[key]
		if !paramKeyShape.MatchString(key) {
			return fmt.Errorf("params key %q must match ^[a-z][a-z0-9_]{0,62}$", printable(key))
		}
		if len(value) > MaxParamValueBytes {
			return fmt.Errorf("params %q is %d bytes; a value is at most %d", key, len(value), MaxParamValueBytes)
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("params %q is not valid UTF-8", key)
		}
		if strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) }) {
			return fmt.Errorf("params %q holds a control character", key)
		}
		total += len(key) + len(value)
	}
	if total > MaxParamsBytes {
		return fmt.Errorf("params are %d bytes in all; at most %d", total, MaxParamsBytes)
	}
	return nil
}

// ReadByGrant reports a secret read from a provider through a grant: one
// that names a grant and whose source is not the Orker's own (env:, file:).
// One that says both is the Orker's own, and refused as such.
func ReadByGrant(source, grant string) bool {
	scheme, _, _ := SplitSource(source)
	return grant != "" && scheme != "env" && scheme != "file"
}
