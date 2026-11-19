package identity

import (
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ReasonNotPermitted refuses what a namespace's identity policy does not
// allow (docs/REASONS.md).
const ReasonNotPermitted = "IDENTITY_NOT_PERMITTED"

// AudienceAllowed reports whether a namespace whose identity policy lists
// patterns allows audience: a pattern equal to it, or one whose trailing `*`
// matches any rest (docs/TASK_IDENTITY.md). No pattern allows nothing.
func AudienceAllowed(patterns []string, audience string) bool {
	for _, p := range patterns {
		if prefix, ok := strings.CutSuffix(p, "*"); ok {
			if strings.HasPrefix(audience, prefix) {
				return true
			}
			continue
		}
		if p == audience {
			return true
		}
	}
	return false
}

// AudienceNotPermittedError is one identity whose audience its namespace
// does not allow.
type AudienceNotPermittedError struct {
	Namespace     string
	Task          string
	TaskIndex     int
	Identity      string
	IdentityIndex int
	Audience      string
	Allowed       []string
}

// Field is the definition's path to the audience.
func (e *AudienceNotPermittedError) Field() string {
	return fmt.Sprintf("tasks[%d].identities[%d].audience", e.TaskIndex, e.IdentityIndex)
}

func (e *AudienceNotPermittedError) Error() string {
	if len(e.Allowed) == 0 {
		return fmt.Sprintf("%s: task %q asks for audience %q, and namespace %q allows no audience: its identity policy lists none",
			ReasonNotPermitted, e.Task, e.Audience, e.Namespace)
	}
	return fmt.Sprintf("%s: task %q asks for audience %q, which namespace %q does not allow (it allows %s)",
		ReasonNotPermitted, e.Task, e.Audience, e.Namespace, strings.Join(e.Allowed, ", "))
}

// CheckAudiences refuses the first identity of wf whose audience the
// namespace's identity policy (allowed) does not allow. It is checked when
// an Orkflow is created, at every submit, and again at the mint.
func CheckAudiences(namespace string, allowed []string, wf *orkstackv1.Orkflow) error {
	for ti, t := range wf.GetTasks() {
		for ii, id := range t.GetIdentities() {
			if AudienceAllowed(allowed, id.GetAudience()) {
				continue
			}
			return &AudienceNotPermittedError{Namespace: namespace, Task: t.GetId(), TaskIndex: ti,
				Identity: id.GetName(), IdentityIndex: ii, Audience: id.GetAudience(), Allowed: append([]string(nil), allowed...)}
		}
	}
	return nil
}

// AsksForIdentity reports whether any task of wf holds an identity, a
// credential or a secret through a grant.
func AsksForIdentity(wf *orkstackv1.Orkflow) bool {
	for _, t := range wf.GetTasks() {
		if TaskHoldsIdentity(t) {
			return true
		}
	}
	return false
}

// TaskHoldsIdentity reports a task that holds an identity, a credential or
// a secret through a grant.
func TaskHoldsIdentity(t *orkstackv1.Task) bool {
	if len(t.GetIdentities()) > 0 || len(t.GetCredentials()) > 0 {
		return true
	}
	for _, s := range t.GetSecrets() {
		if s.GetGrant() != "" {
			return true
		}
	}
	return false
}
