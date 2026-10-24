package dsl

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The catalog maps a short activity name to a registry reference (§4.2).
//
// It is ENGINE CONFIGURATION. It is not a probe, not a marketplace, and not a
// list of what an Orker was compiled with — an activity is an image, and which
// images this deployment offers a short name for is a decision the operator
// makes once, in configuration, not a capability a worker reports.
//
// The catalog does not bound what a task may run. `uses:` accepts any registry
// reference, so a third-party image needs no catalog entry at all; the catalog
// only supplies the short spellings.

// BuiltinActivityCatalog is the default table of §4.2.
//
// `container` is deliberately absent and is not an oversight: under this
// contract every task already IS a container, so a container-starting activity
// would need the docker socket, which §9.1 forbids without exception. `uses:
// container` therefore names nothing (rule 4) and is a compile error; a
// workload names the image it wants directly.
func BuiltinActivityCatalog() Catalog {
	return Catalog{
		"shell":    "ghcr.io/orkstack/activity-shell:0.1",
		"http":     "ghcr.io/orkstack/activity-http:0.1",
		"file":     "ghcr.io/orkstack/activity-file:0.1",
		"json":     "ghcr.io/orkstack/activity-json:0.1",
		"git":      "ghcr.io/orkstack/activity-git:0.1",
		"template": "ghcr.io/orkstack/activity-template:0.1",
	}
}

// Catalog is one deployment's activity table: short name to registry
// reference.
type Catalog map[string]string

// Names returns the catalog's keys in sorted order, which is what an error
// message lists and what ListActivities renders.
func (c Catalog) Names() []string {
	names := make([]string, 0, len(c))
	for name := range c {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// catalogDocument is the YAML shape of ORK_ACTIVITY_CATALOG (§4.2).
type catalogDocument struct {
	Activities map[string]string `yaml:"activities"`
}

// ParseCatalog reads the ORK_ACTIVITY_CATALOG document.
//
// An empty document selects the built-in table rather than an empty catalog:
// an operator who sets no catalog wants the defaults, and an operator who
// wants none can say `activities: {}` and get exactly that. The two are
// distinguishable because the YAML records which one was written.
func ParseCatalog(document []byte) (Catalog, error) {
	if strings.TrimSpace(string(document)) == "" {
		return BuiltinActivityCatalog(), nil
	}
	var doc catalogDocument
	if err := yaml.Unmarshal(document, &doc); err != nil {
		return nil, fmt.Errorf("ORK_ACTIVITY_CATALOG: parse yaml: %w", err)
	}
	if doc.Activities == nil {
		return nil, fmt.Errorf("ORK_ACTIVITY_CATALOG: no `activities:` mapping; " +
			"the document is `activities:` followed by `<name>: <reference>` pairs")
	}
	catalog := make(Catalog, len(doc.Activities))
	for name, reference := range doc.Activities {
		if !slugPattern.MatchString(name) {
			return nil, fmt.Errorf("ORK_ACTIVITY_CATALOG: activity name %q is not a slug (%s)",
				name, slugPattern.String())
		}
		if strings.TrimSpace(reference) == "" {
			return nil, fmt.Errorf("ORK_ACTIVITY_CATALOG: activity %q has no reference", name)
		}
		if !looksLikeReference(reference) {
			return nil, fmt.Errorf("ORK_ACTIVITY_CATALOG: activity %q maps to %q, "+
				"which is not an OCI reference", name, reference)
		}
		catalog[name] = reference
	}
	return catalog, nil
}

// Reason codes for a `uses:` that cannot become an image. They are
// Error.code strings and travel to the author verbatim (§8).
const (
	// ReasonImageNotPinned is a `uses:` carrying a template action. An image
	// chosen at run time cannot be pinned at apply time.
	ReasonImageNotPinned = "IMAGE_NOT_PINNED"
	// ReasonUnknownActivity is a bare name that is neither a catalog key nor
	// a registry reference.
	ReasonUnknownActivity = "UNKNOWN_ACTIVITY"
)

// UsesError is a `uses:` value this deployment cannot turn into an image. It
// carries the reason code so the apply path can report it without re-deriving
// it from the message text.
type UsesError struct {
	TaskID string
	Uses   string
	Code   string
	Detail string
}

func (e *UsesError) Error() string {
	if e.TaskID == "" {
		return fmt.Sprintf("`uses: %s`: %s", e.Uses, e.Detail)
	}
	return fmt.Sprintf("task %q: `uses: %s`: %s", e.TaskID, e.Uses, e.Detail)
}

// referenceRunes are the three characters that distinguish an OCI reference
// from a bare catalog name: a repository separator, a tag separator, and a
// digest separator. §4.2 rule 3 names exactly these.
func looksLikeReference(uses string) bool {
	return strings.ContainsAny(uses, "/:@")
}

// templateAction is a Go template action opening. `uses:` may not contain one.
var templateAction = regexp.MustCompile(`\{\{`)

// ResolveUses applies §4.2's four rules, in order, and returns the registry
// reference `uses` names.
//
// The order matters and is not incidental: rule 2 runs BEFORE rule 3 so that
// `shell` can never accidentally mean Docker Hub's `library/shell`. A catalog
// name always wins over a bare repository name, because the short spelling is
// the one this deployment promised.
//
// This is a pure function of its arguments. It performs no IO — the registry
// GET that turns a reference into a digest is §4.3's job and runs at apply,
// in internal/api, never here and never in Compile.
func (c Catalog) ResolveUses(taskID, uses string) (string, error) {
	// Rule 1: a template action can never be pinned.
	if templateAction.MatchString(uses) {
		return "", &UsesError{
			TaskID: taskID, Uses: uses, Code: ReasonImageNotPinned,
			Detail: "cannot be a template. The engine resolves `uses:` to an immutable " +
				"digest when the orkflow is applied, and a value that depends on run " +
				"input is not knowable then. Put the choice inside the container, or " +
				"apply one orkflow version per image",
		}
	}
	// Rule 2: a slug that is a catalog key is that catalog entry.
	if slugPattern.MatchString(uses) {
		if reference, ok := c[uses]; ok {
			return reference, nil
		}
		// Rule 4: a bare name that names nothing. Rule 3 cannot rescue it —
		// a slug contains none of `/`, `:` or `@`.
		return "", &UsesError{
			TaskID: taskID, Uses: uses, Code: ReasonUnknownActivity,
			Detail: fmt.Sprintf("is not an activity in this deployment's catalog (%s) "+
				"and is not an OCI reference. Name a catalog activity, or write the "+
				"image out in full, e.g. \"ghcr.io/acme/%s:v1\"",
				strings.Join(c.Names(), ", "), uses),
		}
	}
	// Rule 3: anything that parses as an OCI reference is itself.
	if looksLikeReference(uses) {
		return uses, nil
	}
	// Rule 4: neither.
	return "", &UsesError{
		TaskID: taskID, Uses: uses, Code: ReasonUnknownActivity,
		Detail: fmt.Sprintf("is neither a catalog activity (%s) nor an OCI reference",
			strings.Join(c.Names(), ", ")),
	}
}
