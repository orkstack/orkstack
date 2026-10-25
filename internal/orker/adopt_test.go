package orker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// readScriptedCommitStore is a memory commit store whose Get answers come from a
// script first: each entry replaces one Get, in order, and once the script is
// spent Get reads the store. It counts every Get.
type readScriptedCommitStore struct {
	*artifacts.MemoryCommitStore
	mu     sync.Mutex
	script []func(ctx context.Context, uri string) (artifacts.CommitRecord, error)
	gets   int
}

func (s *readScriptedCommitStore) Get(ctx context.Context, uri string) (artifacts.CommitRecord, error) {
	s.mu.Lock()
	s.gets++
	var next func(context.Context, string) (artifacts.CommitRecord, error)
	if len(s.script) > 0 {
		next, s.script = s.script[0], s.script[1:]
	}
	s.mu.Unlock()
	if next != nil {
		return next(ctx, uri)
	}
	return s.MemoryCommitStore.Get(ctx, uri)
}

func (s *readScriptedCommitStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func undecodable(context.Context, string) (artifacts.CommitRecord, error) {
	return artifacts.CommitRecord{Bytes: 7}, artifacts.ErrCommitUndecodable
}

func notFound(context.Context, string) (artifacts.CommitRecord, error) {
	return artifacts.CommitRecord{}, artifacts.ErrNotFound
}

func storeDown(context.Context, string) (artifacts.CommitRecord, error) {
	return artifacts.CommitRecord{}, errors.New("connection refused")
}

// adoptHarness seeds one durable commit by running the activity once, then
// hands back a dispatcher whose commit store is scripted and whose runtime
// logs every pull and create, so a test sees whether a retry ran the
// container again.
type adoptHarness struct {
	d      *OCIDispatcher
	store  *readScriptedCommitStore
	log    *eventLog
	in     *orkstackv1.ExecuteTaskInput
	seeded *orkstackv1.ExecuteTaskResult
}

func newAdoptHarness(t *testing.T) *adoptHarness {
	t.Helper()
	h := &adoptHarness{log: &eventLog{}}
	h.store = &readScriptedCommitStore{MemoryCommitStore: artifacts.NewMemoryCommitStore()}
	h.d = newAttemptDispatcher(t, &policyCapturingRuntime{log: h.log}, artifacts.NewMemoryAttemptStore())
	h.d.Commits = h.store
	h.in = baseExportInput("adopt")
	h.in.CommitUri = "mem://commits/" + h.in.GetRunId() + "/adopt/commit.json"
	seeded, err := h.d.Execute(context.Background(), h.in)
	if err != nil || seeded.GetCommitAdopted() {
		t.Fatalf("seeding attempt: %v (adopted=%v)", err, seeded.GetCommitAdopted())
	}
	h.seeded = seeded
	h.log.reset()
	h.store.mu.Lock()
	h.store.gets = 0
	h.store.mu.Unlock()
	return h
}

// replant rewrites the seeded commit through edit and stores its bytes back.
func (h *adoptHarness) replant(t *testing.T, edit func(*orkstackv1.ActivityCommit)) {
	t.Helper()
	record, err := h.store.MemoryCommitStore.Get(context.Background(), h.in.GetCommitUri())
	if err != nil {
		t.Fatalf("read the seeded commit: %v", err)
	}
	edit(record.Commit)
	document, err := artifacts.MarshalCommit(record.Commit)
	if err != nil {
		t.Fatalf("marshal the edited commit: %v", err)
	}
	h.store.PutRaw(h.in.GetCommitUri(), document)
}

func (h *adoptHarness) ranContainer() bool {
	for _, e := range h.log.all() {
		if e == "pull" || e == "create" {
			return true
		}
	}
	return false
}

// §4.1, §5.4 step 0: a retry whose commit cannot be trusted never runs the
// container past it and never overwrites it. Each refusal names its reason
// and keeps the category its retry policy depends on.
func TestExecute_ARetryRefusesACommitItCannotTrust(t *testing.T) {
	for _, tc := range []struct {
		name     string
		arrange  func(t *testing.T, h *adoptHarness)
		reason   string
		category string
		gets     int
	}{
		{
			name: "undecodable on the reread too",
			arrange: func(t *testing.T, h *adoptHarness) {
				h.store.PutRaw(h.in.GetCommitUri(), []byte(`{"schemaVersion":`))
			},
			reason: ReasonCommitUndecodable, category: CategoryContractViolation, gets: 2,
		},
		{
			name: "the store cannot be read",
			arrange: func(t *testing.T, h *adoptHarness) {
				h.store.script = append(h.store.script, storeDown)
			},
			reason: ReasonCommitReadFailed, category: CategoryInfrastructureError, gets: 1,
		},
		{
			name: "undecodable, then the store cannot be read",
			arrange: func(t *testing.T, h *adoptHarness) {
				h.store.script = append(h.store.script, undecodable, storeDown)
			},
			reason: ReasonCommitReadFailed, category: CategoryInfrastructureError, gets: 2,
		},
		{
			name: "a schema version this orker does not understand",
			arrange: func(t *testing.T, h *adoptHarness) {
				h.replant(t, func(c *orkstackv1.ActivityCommit) { c.SchemaVersion = artifacts.CommitSchemaVersion + 1 })
			},
			reason: ReasonCommitSchema, category: CategoryContractViolation, gets: 1,
		},
		{
			name: "another activity's commit",
			arrange: func(t *testing.T, h *adoptHarness) {
				h.replant(t, func(c *orkstackv1.ActivityCommit) { c.IdempotencyKey = "default/another-run/adopt" })
			},
			reason: ReasonCommitIdentity, category: CategoryContractViolation, gets: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newAdoptHarness(t)
			tc.arrange(t, h)
			before, _ := h.store.MemoryCommitStore.Get(context.Background(), h.in.GetCommitUri())

			_, err := h.d.Execute(context.Background(), h.in)
			failure := requireFailure(t, err, tc.reason, tc.category)
			if !strings.Contains(failure.Message, h.in.GetCommitUri()) {
				t.Errorf("message %q does not name the commit", failure.Message)
			}
			if h.ranContainer() {
				t.Fatalf("the retry ran the container (%v); a commit it cannot trust must stop it", h.log.all())
			}
			if got := h.store.getCount(); got != tc.gets {
				t.Errorf("commit reads = %d, want %d", got, tc.gets)
			}
			after, _ := h.store.MemoryCommitStore.Get(context.Background(), h.in.GetCommitUri())
			if after.Bytes != before.Bytes || after.SHA256 != before.SHA256 {
				t.Errorf("the refused commit was overwritten: %d bytes %s, was %d bytes %s",
					after.Bytes, after.SHA256, before.Bytes, before.SHA256)
			}
		})
	}
}

// The single in-attempt reread decides an undecodable read: a commit that
// decodes on the reread is adopted, and one that is gone by then lets the
// attempt run.
func TestExecute_AnUndecodableCommitIsReadOnceMore(t *testing.T) {
	t.Run("it decodes on the reread and is adopted", func(t *testing.T) {
		h := newAdoptHarness(t)
		h.store.script = append(h.store.script, undecodable)

		result, err := h.d.Execute(context.Background(), h.in)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if !result.GetCommitAdopted() || string(result.GetValuesJson()) != string(h.seeded.GetValuesJson()) {
			t.Fatalf("result = %v, want the committed result adopted", result)
		}
		if h.ranContainer() {
			t.Fatalf("the retry ran the container (%v) instead of adopting the reread commit", h.log.all())
		}
		if got := h.store.getCount(); got != 2 {
			t.Errorf("commit reads = %d, want 2 (the read and its one reread)", got)
		}
	})
	t.Run("it is gone on the reread and the attempt runs", func(t *testing.T) {
		h := newAdoptHarness(t)
		h.store.script = append(h.store.script, undecodable, notFound)

		if _, err := h.d.Execute(context.Background(), h.in); err != nil && failureOf(err) == nil {
			t.Fatalf("Execute: %v", err)
		}
		if !h.ranContainer() {
			t.Fatalf("the attempt did not run after the reread found no commit: %v", h.log.all())
		}
	})
}
