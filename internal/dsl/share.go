package dsl

import (
	"fmt"
	"regexp"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// MaxSharedWith bounds Orkflow.shared_with (docs/DSL.md rule 48). It is a
// document's own list of who may compose it, read on every submit that
// composes it, so it stays a list a person reviews, not a directory.
const MaxSharedWith = 256

// EveryNamespace is the one wildcard of shared_with: `namespace: "*"`.
const EveryNamespace = "*"

// shareNamespacePattern and shareOrkflowPattern are the editor schema's
// spelling of ParseShare's shapes; ParseShare is the rule, and it also bounds
// each slug at MaxSlug bytes.
var (
	shareNamespacePattern = regexp.MustCompile(`^(\*|[a-z][a-z0-9-]*)$`)
	shareOrkflowPattern   = regexp.MustCompile(`^[a-z][a-z0-9-]*/[a-z][a-z0-9-]*$`)
)

// ShareKind is what one shared_with entry admits.
type ShareKind int

const (
	// ShareEveryNamespace is `namespace: "*"`: every Orkflow of every namespace.
	ShareEveryNamespace ShareKind = iota + 1
	// ShareNamespace is `namespace: <ns>`: every Orkflow of that namespace.
	ShareNamespace
	// ShareOrkflow is `orkflow: <ns>/<name>`: that one Orkflow.
	ShareOrkflow
)

// Share is one parsed shared_with entry.
type Share struct {
	Kind      ShareKind
	Namespace string // empty for ShareEveryNamespace
	Name      string // set only for ShareOrkflow
}

// String renders the entry the way every message and record names it:
// "namespace:*", "namespace:<ns>" or "orkflow:<ns>/<name>".
func (s Share) String() string {
	switch s.Kind {
	case ShareEveryNamespace:
		return "namespace:" + EveryNamespace
	case ShareNamespace:
		return "namespace:" + s.Namespace
	case ShareOrkflow:
		return "orkflow:" + s.Namespace + "/" + s.Name
	}
	return ""
}

// shareFieldError locates a refusal of one entry: the entry itself, or the
// one key of it that is wrong.
type shareFieldError struct {
	key string // "", "namespace" or "orkflow"
	err error
}

func (e *shareFieldError) Error() string { return e.err.Error() }
func (e *shareFieldError) Unwrap() error { return e.err }

// ParseShare parses one shared_with entry. Exactly one of namespace and
// orkflow is set; a namespace is "*" or a slug, and an orkflow is
// "<namespace>/<name>" with both halves slugs — "*" never appears inside one.
func ParseShare(entry *orkstackv1.OrkflowShare) (Share, error) {
	ns, ork := entry.GetNamespace(), entry.GetOrkflow()
	if (ns == "") == (ork == "") {
		return Share{}, &shareFieldError{err: fmt.Errorf(
			"a shared_with entry names exactly one of namespace and orkflow (namespace: \"*\", " +
				"namespace: <namespace> or orkflow: <namespace>/<name>)")}
	}
	if ns != "" {
		if ns == EveryNamespace {
			return Share{Kind: ShareEveryNamespace}, nil
		}
		if err := checkSlug("namespace", ns); err != nil {
			return Share{}, &shareFieldError{key: "namespace", err: fmt.Errorf(
				"shared_with namespace: %w; write a namespace, or \"*\" for every namespace", err)}
		}
		return Share{Kind: ShareNamespace, Namespace: ns}, nil
	}
	owner, name, found := strings.Cut(ork, "/")
	if !found || owner == "" || name == "" {
		return Share{}, &shareFieldError{key: "orkflow", err: fmt.Errorf(
			"shared_with orkflow %q is not <namespace>/<name>: it names one Orkflow of another namespace; "+
				"to share with every Orkflow of a namespace write namespace: <namespace>", ork)}
	}
	if err := checkSlug("namespace", owner); err != nil {
		return Share{}, &shareFieldError{key: "orkflow", err: fmt.Errorf("shared_with orkflow %q: %w", ork, err)}
	}
	if err := checkSlug("orkflow name", name); err != nil {
		return Share{}, &shareFieldError{key: "orkflow", err: fmt.Errorf("shared_with orkflow %q: %w", ork, err)}
	}
	return Share{Kind: ShareOrkflow, Namespace: owner, Name: name}, nil
}

// ValidateSharedWith is rule 48 over a whole list. namespace is the
// Orkflow's own namespace, resolved ("default" for an empty one) when the
// caller knows it, or empty when it does not: an entry naming the Orkflow's
// own namespace is refused, because a call within one namespace never needs
// one, and an entry that does nothing reads as if it did something.
func ValidateSharedWith(namespace string, entries []*orkstackv1.OrkflowShare) error {
	if len(entries) > MaxSharedWith {
		return validationError("shared_with", fmt.Errorf(
			"shared_with lists %d entries; an Orkflow is shared with at most %d (rule 48)",
			len(entries), MaxSharedWith))
	}
	seen := make(map[string]int, len(entries))
	for i, entry := range entries {
		share, err := ParseShare(entry)
		if err != nil {
			field := fmt.Sprintf("shared_with[%d]", i)
			if fe, ok := err.(*shareFieldError); ok && fe.key != "" {
				field += "." + fe.key
			}
			return validationError(field, err)
		}
		if namespace != "" && share.Namespace == namespace {
			key := "namespace"
			if share.Kind == ShareOrkflow {
				key = "orkflow"
			}
			return validationError(fmt.Sprintf("shared_with[%d].%s", i, key), fmt.Errorf(
				"shared_with entry %s names this Orkflow's own namespace %q: a call within one namespace "+
					"needs no entry; shared_with lists other namespaces only", share, namespace))
		}
		if first, dup := seen[share.String()]; dup {
			return validationError(fmt.Sprintf("shared_with[%d]", i), fmt.Errorf(
				"shared_with entry %s appears twice (entries %d and %d)", share, first, i))
		}
		seen[share.String()] = i
	}
	return nil
}

// ValidOrkflowTarget is the rule for the Orkflow a schedule or a trigger
// starts (starter is "schedule" or "trigger"): a slug of the starter's own
// namespace. A "<namespace>/<name>" is refused by name, because nothing but
// composition crosses a namespace; any other value is held to the slug rule.
func ValidOrkflowTarget(starter, value string) error {
	if owner, name, found := strings.Cut(value, "/"); found {
		return fmt.Errorf("orkflow %q names another namespace's Orkflow: a %s runs only its own namespace's "+
			"Orkflows. Another namespace's Orkflow runs only as a child, composed by an Orkflow of this namespace "+
			"with uses: orkflow:%s/%s@<version>, and only when its shared_with admits that Orkflow",
			value, starter, owner, name)
	}
	return ValidSlug("orkflow name", value)
}
