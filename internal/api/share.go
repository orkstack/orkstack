package api

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Sharing across namespaces (docs/DSL.md "Sharing across namespaces"): the
// helpers apply (loadComposeTree, resolveComposition) and submit
// (planChildren) share, so the two doors cannot disagree about who may
// compose what.

// refOwner is the namespace that owns the Orkflow ref names: the namespace
// the author wrote into it, or the composing definition's own.
func refOwner(composer string, ref *orkstackv1.OrkflowRef) string {
	if ns := ref.GetNamespace(); ns != "" {
		return artifacts.ResolveNamespace(ns)
	}
	return composer
}

// treeKey is a node's entry in a composition path: "name@version" for an
// Orkflow of the tree's root namespace, as it always was, and
// "owner/name@version" for another namespace's, so a cycle check never
// confuses two namespaces' Orkflows of one name.
func treeKey(root, owner, name, version string) string {
	key := composeKey(name, version)
	if owner != root {
		key = owner + "/" + key
	}
	return key
}

// ownNamespaceRef refuses a ref that writes the composing definition's own
// namespace: the unqualified form says the same thing, and two spellings of
// one reference would give one definition two digests.
func ownNamespaceRef(composer string, ref *orkstackv1.OrkflowRef) error {
	if ref.GetNamespace() == "" || artifacts.ResolveNamespace(ref.GetNamespace()) != composer {
		return nil
	}
	return fmt.Errorf("uses: orkflow:%s/%s@%s names this Orkflow's own namespace %q; write orkflow:%s@%s",
		ref.GetNamespace(), ref.GetName(), ref.GetVersion(), composer, ref.GetName(), ref.GetVersion())
}

// authorizeChild asks internal/authz whether composer's Orkflow may compose
// the stored child, which owner owns. The caller is the Orkflow that writes
// the uses:, at every depth.
func authorizeChild(composer string, composerName, owner string, ref *orkstackv1.OrkflowRef, child *orkstackv1.Orkflow) (authz.Decision, error) {
	return authz.Authorize(authz.Request{
		Action: authz.ActionRun,
		Callee: authz.Callee{Namespace: owner, Name: ref.GetName(), Version: ref.GetVersion(), SharedWith: child.GetSharedWith()},
		Caller: authz.Caller{Namespace: composer, Orkflow: composerName},
	})
}

// plannedInRunNamespace is the copy of a child definition a run plans: when
// another namespace owns it, the copy runs in the run's namespace and names
// its owner in definition_namespace, so every key, index, list and rule of
// the run is the running namespace's while the digest stays the owner's.
func plannedInRunNamespace(child *orkstackv1.Orkflow, runNamespace, owner string) *orkstackv1.Orkflow {
	if owner == artifacts.ResolveNamespace(runNamespace) {
		return child
	}
	planned := proto.Clone(child).(*orkstackv1.Orkflow)
	planned.Namespace = runNamespace
	planned.DefinitionNamespace = owner
	return planned
}

// sharedWithIssue is rule 48's own-namespace half, against the namespace a
// document is applied into: the compiler knows it only when the document
// names one.
func sharedWithIssue(ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) *orkstackv1.ValidationIssue {
	err := dsl.ValidateSharedWith(artifacts.ResolveNamespace(ns.GetName()), wf.GetSharedWith())
	if err == nil {
		return nil
	}
	field := "shared_with"
	if verr, ok := err.(*dsl.ValidationError); ok {
		field = verr.Field
	}
	return composeIssue(field, err.Error(), orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR)
}

// ownNamespaceTarget refuses a schedule or trigger that names another
// namespace's Orkflow: they run only their own namespace's, and another
// namespace's Orkflow runs only as a child.
func ownNamespaceTarget(starter, orkflow string) error {
	if err := dsl.ValidOrkflowTarget(starter, orkflow); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

// sharedRootRefusal is SHARED_ORKFLOW_NOT_ROOT: running again a child run of
// another namespace's Orkflow would start it as a root run, which nothing
// does. owner is the run's orkflow namespace; root, when known, the run to
// run again instead (its root, or its parent when no root was recorded).
func sharedRootRefusal(runID, owner, orkflow, root string) error {
	again := "run its parent run again"
	if root != "" {
		again = fmt.Sprintf("run its root run %s again (orkctl run again %s)", root, root)
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s: run %q ran orkflow %q, which namespace %q shared through composition; it runs only as a child, "+
			"never as a root run, so it cannot be run again on its own: %s",
		authz.ReasonSharedNotRoot, runID, owner+"/"+orkflow, owner, again))
}

// foreignSecretsRefusal is ForeignSecretsUnboundedError as a Connect error:
// InvalidArgument, the namespace's policy being the thing to change.
func foreignSecretsRefusal(ns *orkstackv1.Namespace, owner string, wf *orkstackv1.Orkflow) error {
	if err := engine.CheckForeignSecrets(ns, owner, wf); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

// sharingNamespaces is every namespace other than owner that a stored
// version of owner's Orkflow name was shared with, sorted: where a child run
// of it may have run. "*" means every registered namespace and default. A
// named namespace the registry does not hold has no runs and is left out.
func (s *Service) sharingNamespaces(ctx context.Context, owner, name string) ([]string, error) {
	versions, err := s.orkflows.Versions(ctx, owner, name)
	if err != nil {
		return nil, fmt.Errorf("read the versions of orkflow %q: %w", name, err)
	}
	named, every := map[string]bool{}, false
	for _, wf := range versions.All {
		for i, entry := range wf.GetSharedWith() {
			share, err := dsl.ParseShare(entry)
			if err != nil {
				return nil, fmt.Errorf("orkflow %s@%s has a malformed shared_with[%d]: %w", name, wf.GetVersion(), i, err)
			}
			if share.Kind == dsl.ShareEveryNamespace {
				every = true
			} else {
				named[share.Namespace] = true
			}
		}
	}
	if every {
		names := []string{artifacts.DefaultNamespace}
		if s.namespaces != nil {
			if names, err = s.namespaces.ListNames(ctx); err != nil {
				return nil, fmt.Errorf("list the namespaces: %w", err)
			}
		}
		for _, ns := range names {
			named[ns] = true
		}
	} else {
		for ns := range named {
			if _, err := s.namespace(ctx, ns); connect.CodeOf(err) == connect.CodeNotFound {
				delete(named, ns)
			} else if err != nil {
				return nil, err
			}
		}
	}
	delete(named, artifacts.ResolveNamespace(owner))
	out := make([]string, 0, len(named))
	for ns := range named {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out, nil
}

// sharedRunsBlocking counts, per namespace owner's Orkflow name was shared
// with, the child runs of it there that are active or finished after cutoff:
// runs a deletion must not orphan, read as completeOrkflowRuns reads the
// owner's own (engine.OrkflowKey keys them "<owner>/<name>").
func (s *Service) sharedRunsBlocking(ctx context.Context, owner, name string, cutoff time.Time) (map[string]int, error) {
	namespaces, err := s.sharingNamespaces(ctx, owner, name)
	if err != nil {
		return nil, err
	}
	blocking := map[string]int{}
	for _, ns := range namespaces {
		runs, err := s.completeOrkflowRuns(ctx, ns, artifacts.ResolveNamespace(owner)+"/"+name)
		if err != nil {
			return nil, fmt.Errorf("namespace %q: %w", ns, err)
		}
		for _, run := range runs {
			finished := run.GetFinishedAt()
			if !isTerminalStatus(run.GetStatus()) || finished == nil || finished.AsTime().After(cutoff) {
				blocking[ns]++
			}
		}
	}
	return blocking, nil
}

// sharedBlockingPhrase renders sharedRunsBlocking for a refusal:
// `; and 2 in namespace "search", 1 in namespace "payments", where it is shared and ran as a child`.
func sharedBlockingPhrase(blocking map[string]int) string {
	if len(blocking) == 0 {
		return ""
	}
	names := make([]string, 0, len(blocking))
	for ns := range blocking {
		names = append(names, ns)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, ns := range names {
		parts = append(parts, fmt.Sprintf("%d in namespace %q", blocking[ns], ns))
	}
	return "; and " + strings.Join(parts, ", ") + ", where it is shared and ran as a child"
}
