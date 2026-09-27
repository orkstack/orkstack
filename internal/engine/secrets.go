package engine

import (
	"errors"
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// SecretNotAllowedError reports a task naming a secret source its namespace
// does not allow. It carries what makes the refusal actionable: whose list,
// which task and secret, what was asked for, and the list. It is
// PoolNotAllowedError's sibling and takes its Connect code — InvalidArgument:
// the caller's document is the thing to fix.
//
// It carries the SOURCE, a locator the tenant wrote, and never what that
// source resolves to: nothing on the engine's side ever reads a secret value.
type SecretNotAllowedError struct {
	Namespace   string
	Task        string
	TaskIndex   int // position in Orkflow.tasks, for the issue's field path
	Secret      string
	SecretIndex int // position in Task.secrets
	Source      string
	Allowed     []string
	Refusal     *dsl.SecretSourceRefusal
}

// Error renders the one sentence every client prints (the list joined with
// ", " in stored order):
//
//	namespace "sandbox" may not resolve "env:PROD_DB_PASSWORD" (task "grab", secret "db"); allowed: env:SANDBOX_*, …
//
// and, when the dsl refused a file: source before matching, names that rule
// between the coordinates and the list.
func (e *SecretNotAllowedError) Error() string {
	head := fmt.Sprintf("namespace %q may not resolve %q (task %q, secret %q)",
		e.Namespace, e.Source, e.Task, e.Secret)
	if e.Refusal != nil && e.Refusal.Why != "" {
		head += fmt.Sprintf(`: a "file:" locator with %s is refused, never normalized`,
			dsl.SecretRefusalPhrase(e.Refusal.Why))
	}
	return head + "; allowed: " + strings.Join(e.Allowed, ", ")
}

// Unwrap is the dsl's own refusal, so a caller can ask what rule refused it.
func (e *SecretNotAllowedError) Unwrap() error {
	if e.Refusal == nil {
		return nil
	}
	return e.Refusal
}

// Field is the ValidationIssue field path this refusal anchors to:
// "tasks[3].secrets[0].source" — the tasks[%d].needs[%d] shape
// internal/dsl/validate.go already uses.
func (e *SecretNotAllowedError) Field() string {
	return fmt.Sprintf("tasks[%d].secrets[%d].source", e.TaskIndex, e.SecretIndex)
}

// CheckAllowedSecrets holds every secret of every task against the
// namespace's allowed_secrets. Pure: it reads the two documents and nothing
// else, so ApplyOrkflow, ValidateOrkflow and Plan get the same answer from
// the same function. An empty list returns nil without visiting a task, so
// a namespace with no policy pays nothing. Tasks are walked in document
// order and secrets in declaration order, and the FIRST refusal is
// returned — the "one actionable error, then fix and re-run" shape every
// other preparation refusal has. A package function, not a PoolResolver
// method: unlike a pool name, a secret source has no deployment-specific
// spelling to resolve first.
func CheckAllowedSecrets(ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) error {
	allowed := ns.GetAllowedSecrets()
	if len(allowed) == 0 {
		return nil
	}
	for ti, t := range wf.GetTasks() {
		for si, secret := range t.GetSecrets() {
			err := dsl.CheckSecretSource(allowed, secret.GetSource())
			if err == nil {
				continue
			}
			var refusal *dsl.SecretSourceRefusal
			if !errors.As(err, &refusal) {
				return err
			}
			return &SecretNotAllowedError{
				Namespace:   ns.GetName(),
				Task:        t.GetId(),
				TaskIndex:   ti,
				Secret:      secret.GetName(),
				SecretIndex: si,
				Source:      secret.GetSource(),
				Allowed:     append([]string(nil), allowed...),
				Refusal:     refusal,
			}
		}
	}
	return nil
}
