// Package automation parses, plans and exports an automation document: one
// YAML file per namespace that declares, by name, that namespace's settings,
// schedules, triggers and webhooks (docs/AUTOMATION.md).
//
// It is a pure package: no store, no clock, no network. The engine's
// AutomationService (internal/api/automation_service.go) reads what a
// namespace holds into a Live and asks this package what an apply would do;
// the CLIENT then sends each actionable step's carried request to the
// existing write RPC it names. Nothing here writes, stores a document, or
// watches for drift — there is no desired state anywhere but in Git.
package automation

import (
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

const (
	// APIVersion is the only apiVersion an automation document may name.
	APIVersion = "orkstack/v1"
	// Kind is what tells `orkctl apply` a document is not an orkflow.
	Kind = "Automation"
)

// Document is a parsed automation document. A nil section is one the
// document does not manage; a non-nil empty one is managed and empty.
type Document struct {
	Namespace string
	Settings  *Settings
	Schedules map[string]ScheduleEntry
	Triggers  map[string]TriggerEntry
	Webhooks  map[string]WebhookEntry
}

// Settings is the overlay: Present names the keys the document wrote, and
// Values holds their parsed values in Namespace's own fields.
type Settings struct {
	Present map[string]bool
	Values  *orkstackv1.Namespace
}

// ScheduleEntry is one `schedules` entry.
type ScheduleEntry struct {
	Orkflow, Version, Description string
	Input                         *orkstackv1.Payload      // nil when absent or '{}'
	Spec                          *orkstackv1.ScheduleSpec // overlap already normalised: absent → SKIP
	// Keys are the document keys the entry wrote, in the order of the
	// docs/AUTOMATION.md `schedules` table. A CREATE lists exactly these.
	Keys []string
}

// TriggerEntry is one `triggers` entry.
type TriggerEntry struct {
	Orkflow, Version, Description string
	// Keys are the document keys the entry wrote, in the order of the
	// docs/AUTOMATION.md `triggers` table.
	Keys []string
}

// WebhookEntry is one `webhooks` entry.
type WebhookEntry struct {
	URL       string
	Events    []webhooks.Event // lifecycle order, deduplicated
	Orkflow   string
	Task      string
	SecretEnv string
	// Keys are the document keys the entry wrote, in the order of the
	// docs/AUTOMATION.md `webhooks` table.
	Keys []string
}

// Live is what the namespace holds, read by the caller for one plan.
type Live struct {
	Namespace *orkstackv1.Namespace  // nil: the namespace does not exist
	Schedules []*orkstackv1.Schedule // ListSchedules rows, unavailable ones included
	Triggers  []*orkstackv1.Trigger  // every trigger of the namespace, named or not
	Webhooks  []*orkstackv1.Webhook  // every subscription whose namespace scope is this one
	// Orkflows answers, for every (name, version) the document references,
	// whether it is stored or pending in this invocation. version "" is
	// "any version", i.e. the current pointer resolves.
	Orkflows map[OrkflowRef]bool
}

// OrkflowRef is one (orkflow, version) reference a document makes.
type OrkflowRef struct{ Name, Version string }

// Options tunes a plan.
type Options struct{ Prune bool }

// References lists every (orkflow, version) the document's schedules and
// triggers name, deduplicated and sorted, so a caller knows what to read
// into Live.Orkflows.
func (d *Document) References() []OrkflowRef {
	seen := map[OrkflowRef]bool{}
	var out []OrkflowRef
	add := func(ref OrkflowRef) {
		if ref.Name == "" || seen[ref] {
			return
		}
		seen[ref] = true
		out = append(out, ref)
	}
	for _, name := range sortedKeys(d.Schedules) {
		entry := d.Schedules[name]
		add(OrkflowRef{Name: entry.Orkflow, Version: entry.Version})
	}
	for _, name := range sortedKeys(d.Triggers) {
		entry := d.Triggers[name]
		add(OrkflowRef{Name: entry.Orkflow, Version: entry.Version})
	}
	return out
}

// Managed lists the sections the document declares, in the order NAMESPACE
// (settings), SCHEDULE, TRIGGER, WEBHOOK.
func (d *Document) Managed() []orkstackv1.AutomationObjectKind {
	var out []orkstackv1.AutomationObjectKind
	if d.Settings != nil {
		out = append(out, orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_NAMESPACE)
	}
	if d.Schedules != nil {
		out = append(out, orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_SCHEDULE)
	}
	if d.Triggers != nil {
		out = append(out, orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER)
	}
	if d.Webhooks != nil {
		out = append(out, orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_WEBHOOK)
	}
	return out
}

// KindSlug is a kind's client spelling: the value's name after
// AUTOMATION_OBJECT_KIND_, lowercased — namespace, schedule, trigger,
// webhook. UNSPECIFIED and an undeclared number have none.
func KindSlug(kind orkstackv1.AutomationObjectKind) string {
	name, ok := orkstackv1.AutomationObjectKind_name[int32(kind)]
	if !ok || kind == orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_UNSPECIFIED {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(name, "AUTOMATION_OBJECT_KIND_"))
}

// SectionKey is the document key of a kind's section: settings, schedules,
// triggers, webhooks.
func SectionKey(kind orkstackv1.AutomationObjectKind) string {
	switch kind {
	case orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_NAMESPACE:
		return "settings"
	case orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_SCHEDULE:
		return "schedules"
	case orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER:
		return "triggers"
	case orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_WEBHOOK:
		return "webhooks"
	}
	return ""
}

// ActionSlug is an action's client spelling: the value's name after
// AUTOMATION_ACTION_, lowercased.
func ActionSlug(action orkstackv1.AutomationAction) string {
	name, ok := orkstackv1.AutomationAction_name[int32(action)]
	if !ok || action == orkstackv1.AutomationAction_AUTOMATION_ACTION_UNSPECIFIED {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(name, "AUTOMATION_ACTION_"))
}

// ActionSymbol is the one-character column a plan prints before a step:
// + create, ~ update, - delete, space unchanged, ? undeclared, ! unmanaged.
func ActionSymbol(action orkstackv1.AutomationAction) string {
	switch action {
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE:
		return "+"
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE:
		return "~"
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_DELETE:
		return "-"
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED:
		return " "
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_UNDECLARED:
		return "?"
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_UNMANAGED:
		return "!"
	}
	return ""
}

// Actionable reports whether applying the step writes: CREATE, UPDATE and
// DELETE carry a request; nothing else does.
func Actionable(action orkstackv1.AutomationAction) bool {
	switch action {
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE,
		orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE,
		orkstackv1.AutomationAction_AUTOMATION_ACTION_DELETE:
		return true
	}
	return false
}

// HasErrors reports whether any issue is an ERROR.
func HasErrors(issues []*orkstackv1.ValidationIssue) bool {
	for _, issue := range issues {
		if issue.GetSeverity() == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
			return true
		}
	}
	return false
}

func errorIssue(field, message string) *orkstackv1.ValidationIssue {
	return &orkstackv1.ValidationIssue{
		Field: field, Message: message, Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR,
	}
}

func warningIssue(field, message string) *orkstackv1.ValidationIssue {
	return &orkstackv1.ValidationIssue{
		Field: field, Message: message, Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING,
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sortStrings(out)
	return out
}
