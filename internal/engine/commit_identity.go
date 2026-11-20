package engine

import (
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// CommitIdentity is what one activity's commit must name to be that
// activity's result (E85 §4.3): the object key it lives at under the run's
// commit root, and the fields the Orker's retry-start check compares
// (internal/orker commitIdentityMismatch). Dispatch, a resume's read-back
// (AdoptRecorded) and ResumeRun's preflight all take it from
// ExpectedCommitIdentity, so the three can never disagree about which commit
// is a task's.
type CommitIdentity struct {
	// Key is the commit's object key, without the commit root.
	Key              string
	Namespace        string
	RunID            string
	TaskID           string
	DefinitionDigest string
	IdempotencyKey   string
	ImageIndexDigest string
}

// ExpectedCommitIdentity is the identity of task t's commit in run runID of
// wf: the task's own when item is negative, else that item's, under resume
// scope (0 for a run before any resume). Pure.
func ExpectedCommitIdentity(wf *orkstackv1.Orkflow, runID string, t *orkstackv1.Task, item int32, scope uint64) (CommitIdentity, error) {
	id := CommitIdentity{
		Namespace:        artifacts.ResolveNamespace(wf.GetNamespace()),
		RunID:            runID,
		TaskID:           t.GetId(),
		DefinitionDigest: wf.GetDefinitionDigest(),
		ImageIndexDigest: t.GetImage().GetIndexDigest(),
	}
	var err error
	if item < 0 {
		id.Key, err = artifacts.ScopedCommitKey(wf.GetNamespace(), runID, t.GetId(), scope)
		id.IdempotencyKey = task.ScopedIdempotencyKeyFor(wf.GetNamespace(), runID, t.GetId(), scope)
	} else {
		id.Key, err = artifacts.ScopedItemCommitKey(wf.GetNamespace(), runID, t.GetId(), item, scope)
		id.IdempotencyKey = task.ScopedItemIdempotencyKeyFor(wf.GetNamespace(), runID, t.GetId(), item, scope)
	}
	if err != nil {
		return CommitIdentity{}, fmt.Errorf("task %q: the commit identity: %w", t.GetId(), err)
	}
	return id, nil
}

// Mismatch names the first field of commit that is not want's, the same
// fields in the same order as the Orker's retry-start check; "" when the
// commit is want's.
func (want CommitIdentity) Mismatch(commit *orkstackv1.ActivityCommit) string {
	checks := []struct{ field, stored, wanted string }{
		{"namespace", commit.GetNamespace(), want.Namespace},
		{"run_id", commit.GetRunId(), want.RunID},
		{"task_id", commit.GetTaskId(), want.TaskID},
		{"orkflow_definition_digest", commit.GetOrkflowDefinitionDigest(), want.DefinitionDigest},
		{"idempotency_key", commit.GetIdempotencyKey(), want.IdempotencyKey},
		{"image.index_digest", commit.GetImage().GetIndexDigest(), want.ImageIndexDigest},
	}
	for _, c := range checks {
		if c.stored != c.wanted {
			return fmt.Sprintf("%s is %q, this activity's is %q", c.field, c.stored, c.wanted)
		}
	}
	return ""
}
