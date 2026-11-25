package identity

import (
	"errors"
	"fmt"
	"slices"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ReasonSubjectTooLong refuses a task whose token's subject would pass
// MaxSubjectBytes (docs/REASONS.md).
const ReasonSubjectTooLong = "IDENTITY_SUBJECT_TOO_LONG"

// SubjectTooLongError is the first task of a definition whose token's subject
// does not fit, in the form the deployment issues or the other.
type SubjectTooLongError struct {
	Task      string
	TaskIndex int
	Grant     string // the login's grant, "" for an identity
	Err       error
}

// Field is the definition's path to the task.
func (e *SubjectTooLongError) Field() string { return fmt.Sprintf("tasks[%d].id", e.TaskIndex) }

func (e *SubjectTooLongError) Error() string {
	what := "its identities'"
	if e.Grant != "" {
		what = fmt.Sprintf("grant %q's", e.Grant)
	}
	return fmt.Sprintf("%s: task %q cannot hold %s token: %v", ReasonSubjectTooLong, e.Task, what, e.Err)
}

func (e *SubjectTooLongError) Unwrap() error { return e.Err }

// CheckSubjects refuses the first task of wf one of whose tokens' subjects
// does not fit MaxSubjectBytes, as it runs in namespace: for its identities,
// and for a login through each grant it uses. owner is the namespace that
// owns wf, which a shared Orkflow's subject names beside the one it runs in.
// With a trust domain both forms are held to it (Fits). It is checked when an
// Orkflow is created, for what it composes too, and at every submit.
func CheckSubjects(format Format, trustDomain, namespace, owner string, wf *orkstackv1.Orkflow) error {
	base := Subject{Namespace: artifacts.ResolveNamespace(namespace), Orkflow: wf.GetName()}
	if o := artifacts.ResolveNamespace(owner); o != base.Namespace {
		base.Owner = o
	}
	for ti, t := range wf.GetTasks() {
		var grants []string
		if len(t.GetIdentities()) > 0 {
			grants = append(grants, "")
		}
		for _, c := range t.GetCredentials() {
			grants = append(grants, c.GetGrant())
		}
		for _, s := range t.GetSecrets() {
			if s.GetGrant() != "" {
				grants = append(grants, s.GetGrant())
			}
		}
		slices.Sort(grants)
		for _, grant := range slices.Compact(grants) {
			subject := base
			subject.Task, subject.Grant = t.GetId(), grant
			err := subject.Fits(format, trustDomain)
			if errors.Is(err, errSubjectTooLong) {
				return &SubjectTooLongError{Task: t.GetId(), TaskIndex: ti, Grant: grant, Err: err}
			}
			if err != nil {
				return fmt.Errorf("task %q: %w", t.GetId(), err)
			}
		}
	}
	return nil
}
