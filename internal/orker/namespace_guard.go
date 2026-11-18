package orker

import (
	"fmt"
	"os"
	"strings"

	"go.temporal.io/sdk/temporal"

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
	if lines := in.GetEach().GetLines(); lines != nil {
		if failure := d.ownLineIndex(ns, in, lines); failure != nil {
			return failure
		}
	}
	return nil
}

// ownLineIndex refuses the line index of an item of a fan-out over an
// artifact's lines unless it is the one key the item's own namespace, run,
// task, source digest and format render under this Orker's store root. A uri
// that is no line index, or that does not parse, is refused as well: nothing
// the guard cannot place is read.
func (d *OCIDispatcher) ownLineIndex(ns string, in *orkstackv1.ExecuteTaskInput, lines *orkstackv1.EachLines) *Failure {
	key, under := strings.CutPrefix(lines.GetIndex().GetUri(), d.CommitRoot+"/")
	ref, parsed := artifacts.ParseEachLinesIndexKey(key)
	format, known := formatOf(lines.GetFormat())
	switch {
	case d.CommitRoot == "" || !under || !parsed || !known:
		return Fail(ReasonNamespaceNotServed,
			"the line index of task %s is not a line index under this Orker's store root", in.GetTaskId())
	case ref.Namespace != ns || ref.RunID != in.GetRunId() || ref.Task != in.GetTaskId() ||
		ref.SourceSHA256 != lines.GetSource().GetSha256() || ref.Format != format.String():
		return Fail(ReasonNamespaceNotServed,
			"the line index of task %s is not its own: it is of namespace %q, run %s, task %s",
			in.GetTaskId(), ref.Namespace, ref.RunID, ref.Task)
	}
	return nil
}

// refuseIndexNamespace is the namespace guard of the line indexer
// (IndexEachLines): with a list, an index of a namespace the list does not
// name fails NAMESPACE_NOT_SERVED, never retried, before the source is read
// or an index is written or read. The source is an artifact, content-addressed
// and not of a namespace, as an upstream's staged artifacts are; what the
// guard keeps to served namespaces is commits and line indexes.
func refuseIndexNamespace(served artifacts.Namespaces, in *orkstackv1.IndexEachLinesInput) error {
	if served.Every() {
		return nil
	}
	ns := artifacts.ResolveNamespace(in.GetNamespace())
	if served.Serves(ns) {
		return nil
	}
	return temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("this orker does not serve namespace %q: send the namespace's tasks to a pool whose Orkers serve it "+
			"(its allowed_pools), or add it to ORK_ORKER_NAMESPACES on every Orker of the pool", ns),
		ReasonNamespaceNotServed, nil, expansionDetail(ReasonNamespaceNotServed))
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
