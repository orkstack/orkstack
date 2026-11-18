package orker

import (
	"fmt"
	"os"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// refuseNamespace is the namespace guard at dispatch (docs/DEPLOY.md, "Trust
// zones: a deployment per zone"). With no list it checks nothing. With one,
// it refuses a task whose namespace the list does not name, and a task whose
// input names a commit of another namespace than its own or under another
// store root than this Orker's: its own commit, an upstream's, or the root of
// a fan-out upstream's items. It reads nothing; the refusal names the
// namespace and the reference, never a key.
func (d *OCIDispatcher) refuseNamespace(in *orkstackv1.ExecuteTaskInput) *Failure {
	if d.Namespaces.Every() {
		return nil
	}
	ns := artifacts.ResolveNamespace(in.GetNamespace())
	if !d.Namespaces.Serves(ns) {
		// The refusal reaches the run's owner, so it names their namespace and
		// pool and never the other namespaces this Orker serves; the log does.
		return Fail(ReasonNamespaceNotServed,
			"orker %s does not serve namespace %q: send the namespace's tasks to a pool whose Orkers serve it "+
				"(its allowed_pools), or add it to ORK_ORKER_NAMESPACES on every Orker of pool %q",
			d.OrkerID, ns, in.GetPool())
	}
	if in.GetCommitUri() != "" {
		if failure := d.ownNamespace(ns, in.GetCommitUri(), "its commit"); failure != nil {
			return failure
		}
	}
	for _, up := range in.GetUpstream() {
		if uri := up.GetCommitUri(); uri != "" {
			if failure := d.ownNamespace(ns, uri, "the commit of upstream task "+up.GetTaskId()); failure != nil {
				return failure
			}
		}
		if each := up.GetEach(); each != nil && strings.TrimSuffix(each.GetCommitRoot(), "/") != d.CommitRoot {
			return Fail(ReasonNamespaceNotServed,
				"the items of fan-out upstream task %s lie under another store than this Orker's", up.GetTaskId())
		}
	}
	return nil
}

// ownNamespace refuses a commit uri that is not a commit of namespace ns
// under this Orker's store root.
func (d *OCIDispatcher) ownNamespace(ns, uri, what string) *Failure {
	got, err := artifacts.NamespaceOfURI(d.CommitRoot, uri)
	switch {
	case err != nil:
		return Fail(ReasonNamespaceNotServed,
			"%s is not a commit key under this Orker's store root; an Orker whose object store differs from "+
				"the engine's (ORK_ARTIFACT_BACKEND, ORK_S3_BUCKET, ORK_ARTIFACT_DIR) refuses every commit", what)
	case got != ns:
		return Fail(ReasonNamespaceNotServed, "%s is of namespace %q, not the task's own %q", what, got, ns)
	}
	return nil
}

// NamespacesVar lists the namespaces an Orker serves (orker.namespaces).
const NamespacesVar = "ORK_ORKER_NAMESPACES"

// NamespacesFromEnv reads NamespacesVar: comma-separated namespace names,
// blanks dropped. Unset or empty serves every namespace; a name that breaks
// the namespace rule, or a value of only separators, is an error, which
// refuses the boot.
func NamespacesFromEnv() (artifacts.Namespaces, error) {
	raw := strings.TrimSpace(os.Getenv(NamespacesVar))
	var names []string
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	if raw != "" && len(names) == 0 {
		// A list that was set but names nothing is a mistake, never a
		// request to serve every namespace.
		return artifacts.Namespaces{}, fmt.Errorf("%s is set but names no namespace: %w", NamespacesVar, artifacts.ErrInvalidNamespace)
	}
	served, err := artifacts.ParseNamespaces(names)
	if err != nil {
		return artifacts.Namespaces{}, fmt.Errorf("%s: %w", NamespacesVar, err)
	}
	return served, nil
}

// scopeCommits and scopeMemos scope an Orker's stores to the namespaces it
// serves; a nil store stays nil, so an Orker without one keeps its meaning.
func scopeCommits(store artifacts.CommitStore, root string, served artifacts.Namespaces) artifacts.CommitStore {
	if store == nil {
		return nil
	}
	return artifacts.ScopeCommits(store, strings.TrimSuffix(root, "/"), served)
}

func scopeMemos(store artifacts.MemoStore, served artifacts.Namespaces) artifacts.MemoStore {
	if store == nil {
		return nil
	}
	return artifacts.ScopeMemos(store, served)
}
