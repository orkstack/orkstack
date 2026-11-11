// Package authz is the one place OrkStack decides whether a caller may run
// an Orkflow of another namespace.
//
// Today the only such caller is a composing Orkflow — `uses:
// orkflow:<namespace>/<name>@<version>` — and the only rule is the callee
// version's own shared_with (docs/DSL.md "Sharing across namespaces"). A
// submit, a schedule and a trigger never cross a namespace, so they never
// reach this package.
//
// It is the seam an authentication and RBAC layer wraps later: that layer
// adds a verified caller to Request and may narrow a Decision, and every
// check site keeps calling Authorize. Nothing here reads the Ork-Principal
// header: a principal is asserted, never verified, so it can never satisfy a
// rule.
//
// Authorize is pure: it reads only its request, so validate, apply and every
// submit that plans a child reach the same answer from the same definitions.
package authz

import (
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// ReasonNotShared refuses a composition whose child version does not share
// itself with the composing Orkflow (NotSharedError).
const ReasonNotShared = "ORKFLOW_NOT_SHARED"

// ReasonSharedNotRoot refuses running again a child run of another
// namespace's Orkflow: nothing starts one as a root run.
const ReasonSharedNotRoot = "SHARED_ORKFLOW_NOT_ROOT"

// Action is what a caller asks to do with a callee. The set is closed: an
// action this build does not know is refused, never ignored.
type Action string

// ActionRun is starting a run of the callee — today, as a child run.
const ActionRun Action = "run"

// RuleSameNamespace is the rule that admits a call within one namespace,
// which needs no shared_with entry.
const RuleSameNamespace = "same-namespace"

// defaultNamespace is what an empty namespace means everywhere: the built-in
// one (AGENTS.md §7).
const defaultNamespace = "default"

// Callee is the Orkflow version a caller asks to run.
type Callee struct {
	Namespace  string // the namespace that owns it; empty = default
	Name       string
	Version    string
	SharedWith []*orkstackv1.OrkflowShare
}

// Caller is who asks: the Orkflow whose `uses:` names the callee, and its
// namespace. Orkflow is required: composition is the only way across.
type Caller struct {
	Namespace string // empty = default
	Orkflow   string
}

// Request is one question.
type Request struct {
	Action Action
	Callee Callee
	Caller Caller
}

// Decision is what admitted a request. It is pinned in the run plan of every
// child it admits across a namespace, so a run records why it was allowed,
// not only that it was.
type Decision struct {
	Action Action `json:"action"`
	// Rule is RuleSameNamespace or the shared_with entry that admitted the
	// call: "namespace:*", "namespace:<ns>" or "orkflow:<ns>/<name>".
	Rule string `json:"rule"`
	// Callee is "<namespace>/<name>@<version>", Caller "<namespace>/<name>".
	Callee string `json:"callee"`
	Caller string `json:"caller"`
}

// CrossNamespace reports whether the decision admitted a call across a
// namespace, which is the only kind a plan records.
func (d Decision) CrossNamespace() bool { return d.Rule != RuleSameNamespace }

// NotSharedError is ReasonNotShared: the callee version's shared_with does not
// admit the caller.
type NotSharedError struct {
	Callee     string   // "<namespace>/<name>@<version>"
	Caller     string   // "<namespace>/<name>"
	Namespace  string   // the caller's namespace
	SharedWith []string // the callee's entries, rendered
}

func (e *NotSharedError) Error() string {
	listed := "it shares with no other namespace (it declares no shared_with)"
	if len(e.SharedWith) > 0 {
		listed = "its shared_with lists " + strings.Join(e.SharedWith, ", ")
	}
	return fmt.Sprintf("%s: orkflow %q is not shared with %q: %s. Its owner shares it by adding "+
		"namespace: %s or orkflow: %s to shared_with in a new version, which the caller then pins",
		ReasonNotShared, e.Callee, e.Caller, listed, e.Namespace, e.Caller)
}

// UnknownActionError refuses an action this build does not know.
type UnknownActionError struct{ Action Action }

func (e *UnknownActionError) Error() string {
	return fmt.Sprintf("authorization: unknown action %q; this build decides only %q", e.Action, ActionRun)
}

// Authorize decides one request. A call within one namespace is admitted
// with RuleSameNamespace. Across namespaces, the most specific entry that
// matches admits it — an orkflow entry, then the caller's namespace, then
// "*" — so the recorded rule is the narrowest that holds. A malformed entry
// in a stored definition is an error, never a non-match: it would otherwise
// hide a corrupt definition behind a refusal that blames the caller.
func Authorize(req Request) (Decision, error) {
	if req.Action != ActionRun {
		return Decision{}, &UnknownActionError{Action: req.Action}
	}
	calleeNS, callerNS := resolve(req.Callee.Namespace), resolve(req.Caller.Namespace)
	decision := Decision{
		Action: req.Action,
		Callee: calleeNS + "/" + req.Callee.Name + "@" + req.Callee.Version,
		Caller: callerNS + "/" + req.Caller.Orkflow,
	}
	if req.Caller.Orkflow == "" {
		return Decision{}, fmt.Errorf("authorization: orkflow %q is asked to run from namespace %q by no Orkflow; "+
			"another namespace's Orkflow runs only through composition (uses:)", decision.Callee, callerNS)
	}
	if calleeNS == callerNS {
		decision.Rule = RuleSameNamespace
		return decision, nil
	}
	const (
		none = iota
		everyNamespace
		oneNamespace
		oneOrkflow
	)
	best, bestRule := none, ""
	listed := make([]string, 0, len(req.Callee.SharedWith))
	for i, entry := range req.Callee.SharedWith {
		share, err := dsl.ParseShare(entry)
		if err != nil {
			return Decision{}, fmt.Errorf("authorization: orkflow %q has a malformed shared_with[%d]: %w",
				decision.Callee, i, err)
		}
		listed = append(listed, share.String())
		rank := none
		switch share.Kind {
		case dsl.ShareEveryNamespace:
			rank = everyNamespace
		case dsl.ShareNamespace:
			if share.Namespace == callerNS {
				rank = oneNamespace
			}
		case dsl.ShareOrkflow:
			if share.Namespace == callerNS && share.Name == req.Caller.Orkflow {
				rank = oneOrkflow
			}
		}
		if rank > best {
			best, bestRule = rank, share.String()
		}
	}
	if best == none {
		return Decision{}, &NotSharedError{Callee: decision.Callee, Caller: decision.Caller, Namespace: callerNS, SharedWith: listed}
	}
	decision.Rule = bestRule
	return decision, nil
}

func resolve(namespace string) string {
	if namespace == "" {
		return defaultNamespace
	}
	return namespace
}
