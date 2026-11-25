package orker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The run context a task reads in task.json (docs/OCI_ACTIVITY_CONTRACT.md
// §3.2): its run's origin, start times, deadline and composed lineage, and
// its own scheduled time and attempts. The run's data is the run tree's
// run-context object, read once per process and kept; the per-run and
// per-task facts ride the input (ExecuteTaskInput.run_context).

// task.json's run.context marker.
const (
	// RunContextAvailable: every key that applies is present.
	RunContextAvailable = "available"
	// RunContextCached: a cached task, whose run-describing keys are left
	// out so a memo hit can never hand it another run's data.
	RunContextCached = "cached"
	// RunContextUnavailable: the input carries no run context (a local run
	// from files, one of a run whose record does not say it, `orkctl activity
	// test`).
	RunContextUnavailable = "unavailable"
)

// runContextCacheDir is the directory, under this identity's part of
// ork-stage, that holds the run-context objects this Orker has read. Like
// eachIndexCacheDir it is no 64-hex digest, so it never collides with a
// staging entry, and the boot reap removes it with the rest.
const runContextCacheDir = "run-context"

// runContextCacheMaxBytes bounds the run-context objects one Orker keeps.
const runContextCacheMaxBytes = 64 << 20

// RunContextReader reads a run tree's run-context object, verified against
// the SHA-256 that names it (artifacts.RunContextStore).
type RunContextReader interface {
	ReadRunContextBytes(ctx context.Context, namespace, rootRunID, sha256Hex string) ([]byte, error)
}

// runContextCache is the process's copy of the run-context objects it read.
// It is the line-index cache (each_index_cache.go): immutable objects named
// by their SHA-256, verified on every use, the least recently used dropped
// past the bound, concurrent loads of one object sharing one read. The
// repository's own cache is reused rather than golang.org/x/sync/singleflight
// with hashicorp/golang-lru, which would give the same shared read and
// eviction but not the per-use digest check this cache already has.
func (d *OCIDispatcher) runContextCache() *eachIndexCache {
	d.runContextCacheOnce.Do(func() {
		root := d.WorkRoot
		if root == "" {
			root = os.TempDir()
		}
		d.runContexts = newEachIndexCache(filepath.Join(d.scratchDir(root, StageDirName), runContextCacheDir), runContextCacheMaxBytes)
	})
	return d.runContexts
}

// runContextObject reads the object in's run context names: nil for an input
// that carries none. An object the store says is missing is
// RUN_CONTEXT_MISSING; a store that does not answer, or refuses the read and
// so does not say which, is RUN_CONTEXT_UNAVAILABLE. Both are retried under
// the task's policy. Bytes that are not the object its name says, or an entry
// the tree does not hold, are RUN_CONTEXT_INVALID, never retried.
func (d *OCIDispatcher) runContextObject(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*artifacts.RunContextObject, *Failure) {
	rc := in.GetRunContext()
	if rc == nil {
		return nil, nil
	}
	if d.RunContexts == nil {
		return nil, Fail(ReasonRunContextUnavailable, "this Orker has no store to read the run's context from")
	}
	namespace := artifacts.ResolveNamespace(in.GetNamespace())
	// A top-level run's object is under its own id; a child's under its
	// root's, which it must name.
	root := in.GetRunId()
	if rc.GetTreeIndex() > 0 {
		if root = rc.GetRootRunId(); root == "" {
			return nil, Fail(ReasonRunContextInvalid, "the run's context of a child run names no root")
		}
	}
	sha := rc.GetObjectSha256()
	key, err := artifacts.RunContextKey(namespace, root, sha)
	if err != nil {
		return nil, FailCause(ReasonRunContextInvalid, err, "the run's context names no object: %v", err)
	}
	raw, err := d.runContextCache().load(ctx, key, sha, func(ctx context.Context) ([]byte, error) {
		return d.RunContexts.ReadRunContextBytes(ctx, namespace, root, sha)
	})
	switch {
	case errors.Is(err, artifacts.ErrRunContextMissing):
		return nil, FailCause(ReasonRunContextMissing, err, "the run's context object is missing: %v", err)
	case errors.Is(err, artifacts.ErrRunContextRefused):
		return nil, FailCause(ReasonRunContextUnavailable, err,
			"the store refused the read of the run's context: the object is missing, or this Orker's credential may not read %s/: %v",
			artifacts.RunContextPrefix, err)
	case errors.Is(err, artifacts.ErrRunContextInvalid), errors.Is(err, errEachIndexDigest):
		return nil, FailCause(ReasonRunContextInvalid, err, "the run's context object is not what its name says: %v", err)
	case err != nil:
		return nil, FailCause(ReasonRunContextUnavailable, err, "read the run's context: %v", err)
	}
	obj, err := artifacts.DecodeRunContextObject(raw)
	if err != nil {
		return nil, FailCause(ReasonRunContextInvalid, err, "the run's context object does not decode: %v", err)
	}
	if i := rc.GetTreeIndex(); i < 0 || int(i) >= len(obj.Entries) {
		return nil, Fail(ReasonRunContextInvalid, "the run's context names entry %d of a tree of %d runs", i, len(obj.Entries))
	}
	return &obj, nil
}

// RunLineage is where a run came from, as task.json's run says it: what
// started it, the run at the top of its composed chain and, for a child, the
// run and the task that started it.
type RunLineage struct {
	Origin    artifacts.RunContextOrigin
	RootRunID string
	// Root is the root run's Orkflow and what started it, as this run may read
	// that (RunContextRoot.OriginShared is not used).
	Root artifacts.RunContextRoot
	// Parent is nil for a top-level run.
	Parent *RunParent
}

// RunParent is the run and the composed task that started a child run.
type RunParent struct {
	RunID, TaskID string
	Orkflow       artifacts.RunContextOrkflow
}

// lineageSource is where a run context's lineage is read from, once its times
// and attempts stand.
type lineageSource func() (RunLineage, *Failure)

// fixedLineage is a lineage that is known.
func fixedLineage(l RunLineage) lineageSource {
	return func() (RunLineage, *Failure) { return l, nil }
}

// objectLineage is the lineage of in's run from its run tree's object. One it
// cannot account for whole fails closed, RUN_CONTEXT_INVALID: no key is
// guessed, and a run inside a shared Orkflow is never shown the root's
// principal or reference.
func objectLineage(in *orkstackv1.ExecuteTaskInput, obj *artifacts.RunContextObject) lineageSource {
	return func() (RunLineage, *Failure) {
		rc := in.GetRunContext()
		invalid := func(format string, args ...any) (RunLineage, *Failure) {
			return RunLineage{}, runContextInvalid(format, args...)
		}
		if obj == nil {
			return invalid("was not read for a task that is not cached")
		}
		index := int(rc.GetTreeIndex())
		if index < 0 || index >= len(obj.Entries) {
			return invalid("names entry %d of a tree of %d runs", index, len(obj.Entries))
		}
		if index > 0 && (rc.GetRootRunId() == "" || rc.GetParentRunId() == "") {
			return invalid("of a child run names no root or no parent")
		}
		// A run inside an Orkflow another namespace shared, at any depth, reads
		// the root's origin without its principal and reference.
		shared := false
		for i := index; i > 0; {
			e := obj.Entries[i]
			if e.Parent == nil || int(*e.Parent) < 0 || int(*e.Parent) >= i {
				return invalid("entry %d has no parent before it", i)
			}
			shared = shared || e.Shared
			i = int(*e.Parent)
		}
		rootOrigin := obj.Root.Origin
		if shared {
			if obj.Root.OriginShared == nil {
				return invalid("puts the run inside a shared Orkflow but holds no origin it may read")
			}
			rootOrigin = *obj.Root.OriginShared
		}
		lineage := RunLineage{RootRunID: in.GetRunId(), Origin: obj.Root.Origin,
			Root: artifacts.RunContextRoot{Orkflow: obj.Root.Orkflow, Origin: rootOrigin}}
		if index == 0 {
			return lineage, nil
		}
		lineage.RootRunID = rc.GetRootRunId()
		entry := obj.Entries[index]
		parentOrkflow := obj.Root.Orkflow
		if p := int(*entry.Parent); p > 0 {
			if obj.Entries[p].Orkflow == nil {
				return invalid("entry %d names no orkflow", p)
			}
			parentOrkflow = *obj.Entries[p].Orkflow
		}
		lineage.Origin = artifacts.RunContextOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN.String(), ID: rc.GetParentRunId()}
		lineage.Parent = &RunParent{RunID: rc.GetParentRunId(), TaskID: entry.ParentTaskID, Orkflow: parentOrkflow}
		return lineage, nil
	}
}

func runContextInvalid(format string, args ...any) *Failure {
	return Fail(ReasonRunContextInvalid, "the run's context "+format, args...)
}

// runContextTaskJSON is what the run context adds to task.json's run and
// task: the marker, and for an available context every key that applies.
// lineage is read for every task but a cached one; attempt is the cluster's
// attempt of the current execution and startedAt when it started. A context
// it cannot account for whole fails closed, RUN_CONTEXT_INVALID.
func runContextTaskJSON(in *orkstackv1.ExecuteTaskInput, lineage lineageSource, cached bool,
	attempt int32, startedAt time.Time) (taskJSONRunContext, taskJSONTaskContext, *Failure) {
	rc := in.GetRunContext()
	if rc == nil {
		return taskJSONRunContext{Context: RunContextUnavailable}, taskJSONTaskContext{}, nil
	}
	invalid := func(format string, args ...any) (taskJSONRunContext, taskJSONTaskContext, *Failure) {
		return taskJSONRunContext{}, taskJSONTaskContext{}, runContextInvalid(format, args...)
	}
	if attempt < 1 || rc.GetOwnAttemptsBefore() < 0 || rc.GetDeclaredMaxAttempts() < 1 {
		return invalid("counts attempt %d after %d own, of %d declared", attempt, rc.GetOwnAttemptsBefore(), rc.GetDeclaredMaxAttempts())
	}
	// The attempt keys describe the logical task, not the run's data, so a
	// cached task keeps them.
	task := taskJSONTaskContext{
		OwnAttempt:  rc.GetOwnAttemptsBefore() + attempt,
		MaxAttempts: rc.GetDeclaredMaxAttempts(),
	}
	if cached {
		return taskJSONRunContext{Context: RunContextCached}, task, nil
	}
	if rc.GetCreatedAt() == nil || rc.GetStartedAt() == nil || rc.GetTaskScheduledAt() == nil {
		return invalid("lacks the run's created or started time, or the task's scheduled time")
	}
	if startedAt.IsZero() {
		return invalid("has no start for this attempt")
	}
	from, failure := lineage()
	if failure != nil {
		return taskJSONRunContext{}, taskJSONTaskContext{}, failure
	}
	task.ScheduledAt = rfc3339(rc.GetTaskScheduledAt())
	task.AttemptStartedAt = startedAt.UTC().Format(time.RFC3339Nano)
	origin := contextOrigin(from.Origin)
	run := taskJSONRunContext{
		Context:   RunContextAvailable,
		CreatedAt: rfc3339(rc.GetCreatedAt()),
		StartedAt: rfc3339(rc.GetStartedAt()),
		Deadline:  rfc3339(rc.GetRunDeadline()),
		Origin:    &origin,
		Root:      &taskJSONRunRoot{RunID: from.RootRunID, Orkflow: contextOrkflow(from.Root.Orkflow), Origin: contextOrigin(from.Root.Origin)},
	}
	if p := from.Parent; p != nil {
		run.Parent = &taskJSONRunParent{RunID: p.RunID, TaskID: p.TaskID, Orkflow: contextOrkflow(p.Orkflow)}
	}
	return run, task, nil
}

func rfc3339(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}

func contextOrkflow(o artifacts.RunContextOrkflow) taskJSONOrkflow {
	return taskJSONOrkflow{Name: o.Name, Version: o.Version, DefinitionDigest: o.DefinitionDigest}
}

func contextOrigin(o artifacts.RunContextOrigin) taskJSONRunOrigin {
	return taskJSONRunOrigin{Kind: originKind(o.Kind), ID: o.ID, Reference: o.Reference, Description: o.Description,
		Principal: o.Principal, PrincipalVerified: o.PrincipalVerified}
}

// originKind is an origin kind as task.json writes it: the enum value's
// name without its RUN_ORIGIN_KIND_ prefix (TRIGGER, SCHEDULE, PARENT_RUN).
func originKind(kind string) string {
	return strings.TrimPrefix(kind, "RUN_ORIGIN_KIND_")
}

// attemptStart is when this attempt started: the cluster's own record of it
// inside an activity, else when the Orker began it (a local run).
func attemptStart(ctx context.Context, startedAt *timestamppb.Timestamp) time.Time {
	if activity.IsActivity(ctx) {
		if t := activity.GetInfo(ctx).StartedTime; !t.IsZero() {
			return t
		}
	}
	if startedAt == nil {
		return time.Time{}
	}
	return startedAt.AsTime()
}
