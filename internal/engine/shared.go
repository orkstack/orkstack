package engine

import (
	"fmt"
	"strings"
	"sync/atomic"

	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ReasonForeignSecretsUnbounded refuses an Orkflow another namespace shared
// that names a secret, composed from a namespace whose allowed_secrets is
// empty (ForeignSecretsUnboundedError).
const ReasonForeignSecretsUnbounded = "FOREIGN_SECRETS_UNBOUNDED"

// ForeignSecretsUnboundedError is ReasonForeignSecretsUnbounded. An empty
// allowed_secrets means unrestricted, which a namespace grants its own
// Orkflows; a shared Orkflow was written by another namespace, so it never
// inherits that. The namespace lists what it may read first, and the
// ordinary secret policy then holds the shared Orkflow to that list.
type ForeignSecretsUnboundedError struct {
	Namespace string // the namespace the run executes in
	Owner     string // the namespace that owns the definition
	Orkflow   string // "name@version"
	Task      string
	Secret    string
	Source    string
}

func (e *ForeignSecretsUnboundedError) Error() string {
	return fmt.Sprintf("%s: orkflow %s/%s, shared from namespace %q, names secret %q (task %q, source %q), "+
		"and namespace %q sets no allowed_secrets, which leaves its own Orkflows unrestricted but never one of "+
		"another namespace. List the sources it may read with `orkctl namespace update %s --allow-secret PATTERN`",
		ReasonForeignSecretsUnbounded, e.Owner, e.Orkflow, e.Owner, e.Secret, e.Task, e.Source, e.Namespace, e.Namespace)
}

// DefinitionOwner is the namespace that owns a definition: a planned copy's
// definition_namespace when another namespace shared it, else its own
// namespace, resolved.
func DefinitionOwner(wf *orkstackv1.Orkflow) string {
	if owner := wf.GetDefinitionNamespace(); owner != "" {
		return artifacts.ResolveNamespace(owner)
	}
	return artifacts.ResolveNamespace(wf.GetNamespace())
}

// CheckForeignSecrets refuses a definition owned by another namespace than ns
// that names any secret, while ns sets no allowed_secrets. A definition of
// ns's own, or a namespace with a list, is never refused here: CheckAllowedSecrets
// holds every task to the list.
func CheckForeignSecrets(ns *orkstackv1.Namespace, owner string, wf *orkstackv1.Orkflow) error {
	if len(ns.GetAllowedSecrets()) > 0 || artifacts.ResolveNamespace(owner) == artifacts.ResolveNamespace(ns.GetName()) {
		return nil
	}
	for _, t := range wf.GetTasks() {
		for _, secret := range t.GetSecrets() {
			return &ForeignSecretsUnboundedError{
				Namespace: artifacts.ResolveNamespace(ns.GetName()), Owner: artifacts.ResolveNamespace(owner),
				Orkflow: wf.GetName() + "@" + wf.GetVersion(), Task: t.GetId(), Secret: secret.GetName(),
				Source: secret.GetSource(),
			}
		}
	}
	return nil
}

// CrossNamespaceChange is the workflow.GetVersion change id of a shared
// child's owner in its run: the snapshot's orkflow_namespace and a cached
// task's CacheContext.orkflow_namespace. Asked only by a run whose planned
// definition names an owner (definition_namespace), which submit sets only
// on a child another namespace shared and which no history recorded before
// the field holds, once, before its first snapshot (or, in a carried
// generation, read from the carry), so every recorded history replays
// without meeting it; a history whose gate answered DefaultVersion names no
// owner.
const CrossNamespaceChange = "cross-namespace"

// crossNamespaceGateDisabledForTest is a TEST-ONLY switch, the precedent of
// retryMaxIntervalGateDisabledForTest: when set, decideSharedOwner takes the
// DefaultVersion branch without asking, so a test can show the gate is what
// names the owner. Never set in production code.
var crossNamespaceGateDisabledForTest atomic.Bool

// decideSharedOwner sets sharedOwner for a run of a shared definition whose
// gate says so.
func (s *runState) decideSharedOwner(ctx workflow.Context) {
	owner := s.wf.GetDefinitionNamespace()
	if owner == "" || crossNamespaceGateDisabledForTest.Load() {
		return
	}
	if s.version(ctx, CrossNamespaceChange, 1) >= 1 {
		s.sharedOwner = owner
		// A composed task of a shared definition that names no namespace
		// composes its owner's Orkflow, not one of the running namespace.
		for id, child := range s.children {
			if row := s.taskRuns[id].GetChild(); row != nil && child != nil && row.GetOrkflowNamespace() == "" &&
				s.tasks[id].GetOrkflow().GetNamespace() == "" {
				row.OrkflowNamespace = owner
			}
		}
	}
}

// foreignRefOwner is the owner a composed task's row names: the namespace the
// ref is qualified with, when it is not the one the run executes in.
func foreignRefOwner(ref *orkstackv1.OrkflowRef, runNamespace string) string {
	ns := ref.GetNamespace()
	if ns == "" || artifacts.ResolveNamespace(ns) == artifacts.ResolveNamespace(runNamespace) {
		return ""
	}
	return ns
}

// carriedSharedOwner is decideSharedOwner for a generation that cannot adopt
// its saved state: the owner, when the carry records the gate's decision for
// it, without asking the gate on this history.
func carriedSharedOwner(wf *orkstackv1.Orkflow, carry *orkstackv1.RunContinuation) string {
	owner := wf.GetDefinitionNamespace()
	if owner == "" || crossNamespaceGateDisabledForTest.Load() {
		return ""
	}
	if v, ok := carry.GetVersionDecisions()[CrossNamespaceChange]; ok && v >= 1 {
		return owner
	}
	return ""
}

// OrkflowKey is how a run's Orkflow is keyed wherever runs are indexed or
// grouped (the archive's run index, stats, failures, flakes, usage, cache
// usage, a run list's orkflow filter): its name, or "<owner>/<name>" for a
// child run of another namespace's shared Orkflow, so its runs never mix with
// the running namespace's own Orkflow of that name. A "/" is never in a slug,
// so the two shapes cannot collide.
func OrkflowKey(run *orkstackv1.Run) string {
	if owner := run.GetOrkflowNamespace(); owner != "" {
		return owner + "/" + run.GetOrkflowName()
	}
	return run.GetOrkflowName()
}

// SplitOrkflowKey reverses OrkflowKey: the owner (empty for the namespace's
// own Orkflow) and the name.
func SplitOrkflowKey(key string) (namespace, name string) {
	if owner, rest, found := strings.Cut(key, "/"); found {
		return owner, rest
	}
	return "", key
}
