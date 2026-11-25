package dsl

import (
	"encoding/base64"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/provider"
)

// A task's credentials (docs/DSL.md, "Credentials"): each a bundle of files
// a provider issues for each attempt, as one of the namespace's grants.
// Rules 62 to 64 are held here, and rule 60 in the awaited, sleeping and
// composed tasks' own lists. What a credential's grant and provider allow is
// checked with the namespace and the deployment in hand, at validate, create
// and submit; this file holds what the document alone says.

// credentialNamePattern is a credential's and a grant's name, for the
// schema; provider.ValidCredentialName and ValidGrantName hold the same rule.
var credentialNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// credentialDocument is one entry of a task's credentials.
type credentialDocument struct {
	Name  string `yaml:"name"`
	Grant string `yaml:"grant"`
	// Path is where the provider issues from; absent for a credential that
	// is the login itself.
	Path string `yaml:"path"`
	// Params are literal strings, for a path the provider writes.
	Params map[string]string `yaml:"params"`
	// Refresh is required (absent) or best_effort.
	Refresh string `yaml:"refresh"`
}

// compileCredentials maps a task's credentials to the proto.
func compileCredentials(taskIndex int, td taskDocument) ([]*orkstackv1.CredentialRef, error) {
	var out []*orkstackv1.CredentialRef
	for i, d := range td.Credentials {
		refresh, ok := refreshWords[d.Refresh]
		if !ok {
			return nil, validationError(fmt.Sprintf("tasks[%d].credentials[%d].refresh", taskIndex, i), fmt.Errorf(
				"task %q: credential %q's refresh %q is required or best_effort", td.ID, d.Name, d.Refresh))
		}
		ref := &orkstackv1.CredentialRef{Name: d.Name, Grant: d.Grant, Path: d.Path, Refresh: refresh}
		if len(d.Params) > 0 {
			ref.Params = maps.Clone(d.Params)
		}
		out = append(out, ref)
	}
	return out, nil
}

// validateCredentials holds a container task's credentials to rules 62 to
// 64, then refuses them while this build has no task identity: a provider
// logs in with the attempt's identity token and nothing else.
func validateCredentials(taskIndex int, task *orkstackv1.Task) error {
	creds := task.GetCredentials()
	if len(creds) == 0 {
		return nil
	}
	at := func(i int, field string) string {
		return fmt.Sprintf("tasks[%d].credentials[%d].%s", taskIndex, i, field)
	}
	if len(creds) > provider.MaxCredentials {
		return validationError(fmt.Sprintf("tasks[%d].credentials", taskIndex), fmt.Errorf(
			"task %q declares %d credentials; at most %d", task.GetId(), len(creds), provider.MaxCredentials))
	}
	seen := map[string]bool{}
	for i, c := range creds {
		// Rule 64, first, so the refusal names the fan-out: every item holds
		// the same credentials. A template there would fail rule 62 too.
		if task.GetEach() != nil {
			for _, field := range []struct{ name, value string }{{"name", c.GetName()}, {"grant", c.GetGrant()}, {"path", c.GetPath()}} {
				if strings.Contains(field.value, "{{") {
					return validationError(at(i, field.name), fmt.Errorf(
						"task %q: a fan-out's credentials are the same for every item, so credential %q's %s is not a template", task.GetId(), c.GetName(), field.name))
				}
			}
		}
		// Rule 62: the name, the grant, the path and the params' shape.
		switch {
		case provider.ValidCredentialName(c.GetName()) != nil:
			return validationError(at(i, "name"), fmt.Errorf("task %q: credential name %q must match ^[a-z][a-z0-9-]{0,62}$", task.GetId(), c.GetName()))
		case seen[c.GetName()]:
			return validationError(at(i, "name"), fmt.Errorf("task %q declares credential %q twice", task.GetId(), c.GetName()))
		}
		seen[c.GetName()] = true
		if c.GetGrant() == "" {
			return validationError(at(i, "grant"), fmt.Errorf("task %q: credential %q names no grant", task.GetId(), c.GetName()))
		}
		if err := provider.ValidGrantName(c.GetGrant()); err != nil {
			return validationError(at(i, "grant"), fmt.Errorf("task %q: credential %q: %w", task.GetId(), c.GetName(), err))
		}
		if c.GetPath() != "" {
			if err := provider.ValidLocator(c.GetPath()); err != nil {
				return validationError(at(i, "path"), fmt.Errorf("task %q: credential %q's path: %w", task.GetId(), c.GetName(), err))
			}
			if strings.Contains(c.GetPath(), "#") {
				return validationError(at(i, "path"), fmt.Errorf("task %q: credential %q's path names a field; a path never does", task.GetId(), c.GetName()))
			}
		}
		if len(c.GetParams()) > 0 {
			if c.GetPath() == "" {
				return validationError(at(i, "params"), fmt.Errorf("task %q: credential %q sets params with no path", task.GetId(), c.GetName()))
			}
			if err := provider.ValidParamsShape(c.GetParams()); err != nil {
				return validationError(at(i, "params"), fmt.Errorf("task %q: credential %q: %w", task.GetId(), c.GetName(), err))
			}
			// Rule 63: params are literals, never templates.
			for _, key := range slices.Sorted(maps.Keys(c.GetParams())) {
				if strings.Contains(c.GetParams()[key], "{{") {
					return validationError(at(i, "params"), fmt.Errorf(
						"task %q: credential %q's params %q is a template; params are literal strings", task.GetId(), c.GetName(), key))
				}
			}
		}
	}
	if !taskIdentityAvailable {
		return validationError(fmt.Sprintf("tasks[%d].credentials", taskIndex), fmt.Errorf(
			"task %q declares credentials, and task identity, which a provider logs in with, is not available in this build", task.GetId()))
	}
	return nil
}

// The bounds of a secret's ciphertext (rule 65).
const (
	MaxCiphertextChars     = 8192
	MaxCiphertextTaskBytes = 16 << 10
)

// validateProviderSecrets holds a container task's provider secrets to rule
// 65: a grant names a provider's source, never the Orker's own; the source is
// a provider name and a locator; a ciphertext is base64 within its bounds and
// comes with a grant. Whether the provider is declared, decrypts, and the
// grant admits this Orkflow is checked with the deployment and the namespace
// in hand. Until task identity lands, a provider secret is refused after
// those rules.
func validateProviderSecrets(taskIndex int, task *orkstackv1.Task) error {
	total := 0
	provided := false
	for i, s := range task.GetSecrets() {
		at := func(field string) string { return fmt.Sprintf("tasks[%d].secrets[%d].%s", taskIndex, i, field) }
		scheme, locator, hasScheme := provider.SplitSource(s.GetSource())
		own := scheme == "env" || scheme == "file"
		if s.GetGrant() == "" {
			if s.GetCiphertext() != "" {
				return validationError(at("ciphertext"), fmt.Errorf("task %q: secret %q has a ciphertext and no grant; a ciphertext is a provider's to decrypt", task.GetId(), s.GetName()))
			}
			// Any scheme but the Orker's own is a provider's, and a provider
			// is read only as a grant. Such a source never resolved: the
			// Orker refused it at dispatch, so refusing it here changes only
			// when a definition that could not run is told so.
			if hasScheme && !own {
				return validationError(at("grant"), fmt.Errorf("task %q: secret %q's source %q is a provider's, and a provider's source needs a grant", task.GetId(), s.GetName(), printableSource(scheme)))
			}
			continue
		}
		provided = true
		if own {
			return validationError(at("grant"), fmt.Errorf("task %q: secret %q names a grant, and its source %q is the Orker's own; a grant is for a provider's source", task.GetId(), s.GetName(), scheme+":"))
		}
		if err := provider.ValidGrantName(s.GetGrant()); err != nil {
			return validationError(at("grant"), fmt.Errorf("task %q: secret %q: %w", task.GetId(), s.GetName(), err))
		}
		if err := provider.ValidName(scheme); err != nil {
			return validationError(at("source"), fmt.Errorf("task %q: secret %q: %w", task.GetId(), s.GetName(), err))
		}
		if err := provider.ValidLocator(locator); err != nil {
			return validationError(at("source"), fmt.Errorf("task %q: secret %q's source: %w", task.GetId(), s.GetName(), err))
		}
		if c := s.GetCiphertext(); c != "" {
			if len(c) > MaxCiphertextChars {
				return validationError(at("ciphertext"), fmt.Errorf("task %q: secret %q's ciphertext is %d characters; at most %d", task.GetId(), s.GetName(), len(c), MaxCiphertextChars))
			}
			raw, err := base64.StdEncoding.Strict().DecodeString(c)
			// The decoder skips line breaks; a ciphertext is one canonical
			// spelling of its bytes, so the definition's digest names them once.
			if err != nil || base64.StdEncoding.EncodeToString(raw) != c {
				return validationError(at("ciphertext"), fmt.Errorf("task %q: secret %q's ciphertext is not standard base64", task.GetId(), s.GetName()))
			}
			total += len(raw)
		}
	}
	if total > MaxCiphertextTaskBytes {
		return validationError(fmt.Sprintf("tasks[%d].secrets", taskIndex), fmt.Errorf("task %q's ciphertexts are %d bytes; at most %d a task", task.GetId(), total, MaxCiphertextTaskBytes))
	}
	if provided && !taskIdentityAvailable {
		return validationError(fmt.Sprintf("tasks[%d].secrets", taskIndex), fmt.Errorf(
			"task %q reads a secret from a provider, and task identity, which a provider logs in with, is not available in this build", task.GetId()))
	}
	return nil
}

// printableSource is a scheme as a refusal may quote it: the grammar has not
// judged it yet.
func printableSource(scheme string) string {
	if len(scheme) > 32 || strings.ContainsFunc(scheme, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return "the scheme"
	}
	return scheme + ":"
}

// canonicalCredentials is a task's credentials as its canonical form holds
// them, or nil when it declares none.
func canonicalCredentials(t *orkstackv1.Task) []any {
	if len(t.GetCredentials()) == 0 {
		return nil
	}
	out := make([]any, 0, len(t.GetCredentials()))
	for _, c := range t.GetCredentials() {
		entry := map[string]any{
			"name":    c.GetName(),
			"grant":   c.GetGrant(),
			"path":    c.GetPath(),
			"refresh": int64(c.GetRefresh()),
		}
		if len(c.GetParams()) > 0 {
			params := map[string]any{}
			for k, v := range c.GetParams() {
				params[k] = v
			}
			entry["params"] = params
		}
		out = append(out, entry)
	}
	return out
}
