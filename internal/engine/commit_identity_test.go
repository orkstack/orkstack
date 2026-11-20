package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// committedFrom is the commit an Orker writes for in: every identity field
// copied from the input it was sent (internal/orker, step 8).
func committedFrom(in *orkstackv1.ExecuteTaskInput) *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{
		Namespace:               "default",
		RunId:                   in.GetRunId(),
		TaskId:                  in.GetTaskId(),
		OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
		IdempotencyKey:          in.GetIdempotencyKey(),
		Image:                   in.GetImage(),
	}
}

// ExpectedCommitIdentity is what dispatch sends (E85 §4.3): the key under the
// commit root, the idempotency key, and a commit the Orker would write for
// that input matches it. A resume's read-back and preflight take the same
// identity, so they find exactly the commit dispatch had the Orker write.
func TestExpectedCommitIdentity_IsWhatDispatchSends(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	doc := fmt.Sprintf(eachInputDocument, 10, "", "")
	run := h.run(t, "ork-identity-1", doc, map[string]any{"regions": []any{"eu", "us"}})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	wf := compile(t, doc)
	calls := h.itemCalls("convert")
	if len(calls) != 2 {
		t.Fatalf("%d items dispatched, want 2", len(calls))
	}
	for _, in := range calls {
		item := in.GetEach().GetIndex()
		want, err := engine.ExpectedCommitIdentity(wf, "ork-identity-1", wf.GetTasks()[0], item, 0)
		must.OK(t, err)
		if got := strings.TrimPrefix(in.GetCommitUri(), "mem://commits/"); got != want.Key {
			t.Errorf("item %d: commit key %q, want %q", item, got, want.Key)
		}
		if in.GetIdempotencyKey() != want.IdempotencyKey {
			t.Errorf("item %d: idempotency key %q, want %q", item, in.GetIdempotencyKey(), want.IdempotencyKey)
		}
		if m := want.Mismatch(committedFrom(in)); m != "" {
			t.Errorf("item %d: the Orker's commit does not match: %s", item, m)
		}
	}
}

// A commit under another scope, or of another task, is never this
// activity's.
func TestExpectedCommitIdentity_ScopesAndMismatches(t *testing.T) {
	wf := compile(t, fmt.Sprintf(eachInputDocument, 10, "", ""))
	tk := wf.GetTasks()[0]
	tk.Image = &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("a", 64)}
	base, err := engine.ExpectedCommitIdentity(wf, "ork-1", tk, -1, 0)
	must.OK(t, err)
	scoped, err := engine.ExpectedCommitIdentity(wf, "ork-1", tk, -1, 41)
	must.OK(t, err)
	if !strings.HasSuffix(scoped.Key, "/convert/r00000000000000000041/commit.json") || scoped.IdempotencyKey != "default/ork-1/convert@r41" {
		t.Fatalf("scoped identity = %+v", scoped)
	}
	commit := &orkstackv1.ActivityCommit{
		Namespace: "default", RunId: "ork-1", TaskId: "convert",
		OrkflowDefinitionDigest: wf.GetDefinitionDigest(), IdempotencyKey: base.IdempotencyKey, Image: tk.GetImage(),
	}
	if m := base.Mismatch(commit); m != "" {
		t.Fatalf("the base commit does not match its own identity: %s", m)
	}
	if m := scoped.Mismatch(commit); !strings.HasPrefix(m, "idempotency_key") {
		t.Fatalf("a scope-0 commit matched the rerun's identity: %q", m)
	}
	commit.Image = &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("b", 64)}
	if m := base.Mismatch(commit); !strings.HasPrefix(m, "image.index_digest") {
		t.Fatalf("a commit of another image matched: %q", m)
	}
	if _, err := engine.ExpectedCommitIdentity(wf, "", tk, -1, 0); err == nil {
		t.Fatal("an empty run id rendered a commit key")
	}
}
