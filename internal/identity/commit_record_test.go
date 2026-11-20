package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordWorld is the commit's recorder over the mint's own world: the same
// admission, chain, describe and record store.
type recordWorld struct {
	*mintWorld
	recorder *Recorder
	pinned   string
}

func newRecordWorld(t *testing.T) *recordWorld {
	t.Helper()
	w := &recordWorld{mintWorld: newMintWorld(t)}
	w.recorder = &Recorder{Stores: w.store, Starts: w.starts,
		Describe:    func(context.Context, string) (ExecutionFacts, error) { return w.facts, nil },
		PinnedImage: func(context.Context, artifacts.Admission, string) (string, error) { return w.pinned, nil },
		EngineBuild: "engine-9",
		Now:         func() time.Time { return w.now },
	}
	return w
}

func (w *recordWorld) commitRequest() CommitRequest {
	key, err := artifacts.CommitKey("ns", "ork-1", "build")
	if err != nil {
		w.t.Fatal(err)
	}
	return CommitRequest{Namespace: "ns", RunID: "ork-1", TaskID: "build", Attempt: 1,
		TaskSpecSHA256: strings.Repeat("a", 64), AdmissionSHA256: w.sha, CommitKey: key, Commit: w.commit()}
}

// commit is the manifest the Orker that holds attempt 1 of "build" hands
// the engine.
func (w *recordWorld) commit() *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "ns", RunId: "ork-1", TaskId: "build",
		OrkflowName: "deploy", OrkflowVersion: "1", OrkflowDefinitionDigest: "sha256:" + w.adm.DefinitionDigest,
		IdempotencyKey: task.IdempotencyKeyFor("ns", "ork-1", "build"),
		Image: &orkstackv1.ImageRef{Reference: "ghcr.io/acme/build:v1", IndexDigest: "sha256:" + strings.Repeat("e", 64),
			Pinned: "ghcr.io/acme/build@sha256:" + strings.Repeat("e", 64)},
		Attempt: 1, OrkerId: "orker-1", BuildId: "orker-build-3"}
}

func (w *recordWorld) record(req CommitRequest) (artifacts.RecordedCommit, error) {
	a, err := w.recorder.Admit(context.Background(), req)
	if err != nil {
		return artifacts.RecordedCommit{}, err
	}
	return w.recorder.Record(context.Background(), a, req, Caller{Principal: "orker:orker-1", OrkerID: "orker-1"})
}

// recordedAt reads what is at req's attested key as an Orker's commit
// store would.
func (w *recordWorld) recordedAt(req CommitRequest) (artifacts.CommitRecord, bool) {
	w.t.Helper()
	attested, err := artifacts.AttestedCommitKey(req.CommitKey)
	if err != nil {
		w.t.Fatal(err)
	}
	store, err := artifacts.NewLocalCommitStore(w.dir)
	if err != nil {
		w.t.Fatal(err)
	}
	root, err := artifacts.CommitRoot(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: w.dir})
	if err != nil {
		w.t.Fatal(err)
	}
	rec, err := store.Get(context.Background(), strings.TrimSuffix(root, "/")+"/"+attested)
	if errors.Is(err, artifacts.ErrNotFound) {
		return artifacts.CommitRecord{}, false
	}
	if err != nil {
		w.t.Fatal(err)
	}
	return rec, true
}

// TestRecord_WritesTheResultTheHolderHandsIt: an attempt every step admits
// has its manifest written under the attested prefix with the recording
// set, and a repeat, or another attempt's write after it, is answered with
// that record and creates nothing.
func TestRecord_WritesTheResultTheHolderHandsIt(t *testing.T) {
	w := newRecordWorld(t)
	req := w.commitRequest()
	got, err := w.record(req)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if !got.Created || got.Key != "metadata/attested/ns/ork-1/build/commit.json" {
		t.Fatalf("record = created %v at %q", got.Created, got.Key)
	}
	stored, ok := w.recordedAt(req)
	if !ok || stored.SHA256 != got.Record.SHA256 {
		t.Fatalf("the stored record = %+v, %v", stored, ok)
	}
	r := stored.Commit.GetRecorded()
	if r.GetPool() != "main" || r.GetOrkerId() != "orker-1" || r.GetAdmissionSha256() != w.sha || r.GetEngineBuild() != "engine-9" ||
		!r.GetTime().AsTime().Equal(w.now) {
		t.Fatalf("recording = %+v", r)
	}
	again, err := w.record(req)
	if err != nil || again.Created || again.Record.SHA256 != got.Record.SHA256 {
		t.Fatalf("a repeat = %+v, %v; want the first record", again, err)
	}
}

// TestRecord_AFanOutItemIsRecordedUnderItsIndex: an item's commit key and
// idempotency key are the item's own.
func TestRecord_AFanOutItemIsRecordedUnderItsIndex(t *testing.T) {
	w := newRecordWorld(t)
	adm := w.adm
	row := adm.Tasks["build"]
	row.MaxItems = 4
	adm.Tasks["build"] = row
	w.sha = w.write(adm)
	w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: w.sha})
	w.facts.Pending[0].ActivityID = "ork/build/2"
	req := w.commitRequest()
	req.AdmissionSHA256, req.Item = w.sha, artifacts.Item{Index: 2, Set: true}
	req.CommitKey, _ = artifacts.ItemCommitKey("ns", "ork-1", "build", 2)
	req.Commit.IdempotencyKey = task.ItemIdempotencyKeyFor("ns", "ork-1", "build", 2)
	got, err := w.record(req)
	if err != nil || got.Key != "metadata/attested/ns/ork-1/build/00002/commit.json" {
		t.Fatalf("item record = %+v, %v", got, err)
	}
}

// TestRecord_APreflightAsksAndWritesNothing: a preflight is steps 1 to 6a
// with no document, and writes nothing whatever it answers.
func TestRecord_APreflightAsksAndWritesNothing(t *testing.T) {
	w := newRecordWorld(t)
	req := w.commitRequest()
	req.Preflight, req.Commit = true, nil
	if got, err := w.record(req); err != nil || got.Created || got.Key != "" {
		t.Fatalf("preflight = %+v, %v", got, err)
	}
	if _, ok := w.recordedAt(req); ok {
		t.Fatal("a preflight wrote a record")
	}
	w.facts.Pending[0].LastWorker = "orker-2"
	if _, err := w.record(req); reasonOf(err) != ReasonAttemptNotHeld {
		t.Fatalf("a preflight of an attempt not held = %v", err)
	}
}

// TestRecord_Refuses: each step's refusal, and each step that could not
// ask answering unavailable, records nothing.
func TestRecord_Refuses(t *testing.T) {
	doc := func(edit func(*orkstackv1.ActivityCommit)) func(*recordWorld, *CommitRequest) {
		return func(w *recordWorld, r *CommitRequest) { edit(r.Commit) }
	}
	for _, tc := range []struct {
		name  string
		edit  func(*recordWorld, *CommitRequest)
		want  string
		audit bool
	}{
		{"no admission record", func(w *recordWorld, r *CommitRequest) { r.AdmissionSHA256 = otherSHA }, ReasonNotAdmitted, true},
		{"another namespace", func(w *recordWorld, r *CommitRequest) { r.Namespace = "other" }, ReasonNotAdmitted, true},
		{"a task not admitted", func(w *recordWorld, r *CommitRequest) { r.TaskID = "deploy" }, ReasonNotAdmitted, true},
		{"an item of a task that is no fan-out", func(w *recordWorld, r *CommitRequest) { r.Item = artifacts.Item{Index: 1, Set: true} }, ReasonNotAdmitted, true},
		{"another Orker", func(w *recordWorld, r *CommitRequest) { w.facts.Pending[0].LastWorker = "orker-2" }, ReasonAttemptNotHeld, false},
		{"another attempt", func(w *recordWorld, r *CommitRequest) { r.Attempt, r.Commit.Attempt = 2, 2 }, ReasonAttemptNotHeld, false},
		{"a closed run", func(w *recordWorld, r *CommitRequest) { w.facts.Open = false }, ReasonAttemptNotHeld, false},
		{"a run no longer executing", func(w *recordWorld, r *CommitRequest) {
			w.recorder.Describe = func(context.Context, string) (ExecutionFacts, error) { return ExecutionFacts{}, ErrNoSuchExecution }
		}, ReasonAttemptNotHeld, false},
		{"a describe that fails", func(w *recordWorld, r *CommitRequest) {
			w.recorder.Describe = func(context.Context, string) (ExecutionFacts, error) {
				return ExecutionFacts{}, errors.New("frontend down")
			}
		}, ReasonUnavailable, false},
		{"another start", func(w *recordWorld, r *CommitRequest) {
			w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e0", AdmissionSHA256: w.sha})
		}, ReasonNotAdmitted, true},
		{"another task spec", func(w *recordWorld, r *CommitRequest) { r.TaskSpecSHA256 = otherSHA }, ReasonTaskNotAsAdmitted, true},
		{"follows with no checker", func(w *recordWorld, r *CommitRequest) {
			adm := w.adm
			adm.Follows = map[string][]artifacts.FollowRef{"build": {{TaskID: "approve", Kind: "awaited"}}}
			r.AdmissionSHA256 = w.write(adm)
			w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: r.AdmissionSHA256})
		}, ReasonEngineRefused, false},
		{"no document", func(w *recordWorld, r *CommitRequest) { r.Commit = nil }, ReasonCommitIdentityMismatch, true},
		{"another schema", doc(func(c *orkstackv1.ActivityCommit) { c.SchemaVersion = 2 }), ReasonCommitIdentityMismatch, true},
		{"another namespace in the document", doc(func(c *orkstackv1.ActivityCommit) { c.Namespace = "other" }), ReasonCommitIdentityMismatch, true},
		{"another run in the document", doc(func(c *orkstackv1.ActivityCommit) { c.RunId = "ork-2" }), ReasonCommitIdentityMismatch, true},
		{"another task in the document", doc(func(c *orkstackv1.ActivityCommit) { c.TaskId = "deploy" }), ReasonCommitIdentityMismatch, true},
		{"another definition", doc(func(c *orkstackv1.ActivityCommit) { c.OrkflowDefinitionDigest = "sha256:" + otherSHA }), ReasonCommitIdentityMismatch, true},
		{"another idempotency key", doc(func(c *orkstackv1.ActivityCommit) { c.IdempotencyKey = "ns/ork-1/deploy" }), ReasonCommitIdentityMismatch, true},
		{"another attempt in the document", doc(func(c *orkstackv1.ActivityCommit) { c.Attempt = 2 }), ReasonCommitIdentityMismatch, true},
		{"another Orker in the document", doc(func(c *orkstackv1.ActivityCommit) { c.OrkerId = "orker-2" }), ReasonCommitIdentityMismatch, true},
		{"no image", doc(func(c *orkstackv1.ActivityCommit) { c.Image = nil }), ReasonCommitIdentityMismatch, true},
		{"a recording the Orker wrote", doc(func(c *orkstackv1.ActivityCommit) { c.Recorded = &orkstackv1.CommitRecording{Pool: "main"} }), ReasonCommitIdentityMismatch, true},
		{"an image the definition does not pin", func(w *recordWorld, r *CommitRequest) { w.pinned = "sha256:" + strings.Repeat("f", 64) }, ReasonCommitIdentityMismatch, true},
		{"a definition that is not the admitted one", func(w *recordWorld, r *CommitRequest) {
			w.recorder.PinnedImage = func(context.Context, artifacts.Admission, string) (string, error) {
				return "", fmt.Errorf("deploy@1 is another definition: %w", ErrDefinitionNotAdmitted)
			}
		}, ReasonNotAdmitted, true},
		{"an undecodable record at the key", func(w *recordWorld, r *CommitRequest) {
			w.recorder.Stores = undecodableThere{w.store}
		}, ReasonCommitIdentityMismatch, true},
		{"a pin that cannot be read", func(w *recordWorld, r *CommitRequest) {
			w.recorder.PinnedImage = func(context.Context, artifacts.Admission, string) (string, error) {
				return "", errors.New("store down")
			}
		}, ReasonUnavailable, false},
		{"another task's commit key", func(w *recordWorld, r *CommitRequest) {
			r.CommitKey, _ = artifacts.CommitKey("ns", "ork-1", "deploy")
		}, ReasonCommitIdentityMismatch, true},
		{"an item's commit key for the task", func(w *recordWorld, r *CommitRequest) {
			r.CommitKey, _ = artifacts.ItemCommitKey("ns", "ork-1", "build", 0)
		}, ReasonCommitIdentityMismatch, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordWorld(t)
			req := w.commitRequest()
			tc.edit(w, &req)
			got, err := w.record(req)
			var m *MintError
			if got.Created || !errors.As(err, &m) || m.Reason != tc.want || m.Audit != tc.audit {
				t.Fatalf("record = %+v, err = %v (audit %v); want %s, audit %v", got, err, m != nil && m.Audit, tc.want, tc.audit)
			}
			if _, ok := w.recordedAt(w.commitRequest()); ok {
				t.Fatal("a refused commit left a record")
			}
		})
	}
}

// TestRecord_AStoreThatCannotWriteIsUnavailable: a write the store did not
// answer is unavailable, never a refusal and never a record.
func TestRecord_AStoreThatCannotWriteIsUnavailable(t *testing.T) {
	w := newRecordWorld(t)
	w.recorder.Stores = failingWrites{w.store}
	_, err := w.record(w.commitRequest())
	var m *MintError
	if !errors.As(err, &m) || m.Reason != ReasonUnavailable || !m.Unavailable {
		t.Fatalf("record = %v; want unavailable", err)
	}
}

type undecodableThere struct{ *artifacts.RecordStore }

func (undecodableThere) RecordCommit(context.Context, string, *orkstackv1.ActivityCommit) (artifacts.RecordedCommit, error) {
	return artifacts.RecordedCommit{}, fmt.Errorf("the record there: %w", artifacts.ErrCommitUndecodable)
}

type failingWrites struct{ *artifacts.RecordStore }

func (failingWrites) RecordCommit(context.Context, string, *orkstackv1.ActivityCommit) (artifacts.RecordedCommit, error) {
	return artifacts.RecordedCommit{}, &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("bucket unreachable")}
}

// TestPinnedImageFrom: the pin is the index digest the stored version
// stamped into the task; another definition, a missing task or a read that
// fails is an error, never "pins nothing".
func TestPinnedImageFrom(t *testing.T) {
	adm, _ := admitted(t, "ork-1", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", DefinitionDigest: "sha256:" + adm.DefinitionDigest,
		Tasks: []*orkstackv1.Task{{Id: "build", Image: &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("e", 64)}}, {Id: "tag"}}}
	read := func(_ context.Context, ns, name, version string) (*orkstackv1.Orkflow, error) {
		if ns != "ns" || name != "deploy" || version != "1" {
			return nil, errors.New("not found")
		}
		return wf, nil
	}
	pin := PinnedImageFrom(read)
	ctx := context.Background()
	if got, err := pin(ctx, adm, "build"); err != nil || got != "sha256:"+strings.Repeat("e", 64) {
		t.Fatalf("build = %q, %v", got, err)
	}
	if got, err := pin(ctx, adm, "tag"); err != nil || got != "" {
		t.Fatalf("a task with no stamped digest = %q, %v", got, err)
	}
	if _, err := pin(ctx, adm, "absent"); !errors.Is(err, ErrDefinitionNotAdmitted) {
		t.Fatalf("a task the version lacks: %v", err)
	}
	other := adm
	other.DefinitionDigest = otherSHA
	if _, err := pin(ctx, other, "build"); !errors.Is(err, ErrDefinitionNotAdmitted) {
		t.Fatalf("another definition: %v", err)
	}
	other = adm
	other.OrkflowVersion = "2"
	if _, err := pin(ctx, other, "build"); err == nil || errors.Is(err, ErrDefinitionNotAdmitted) {
		t.Fatalf("a read that failed: %v, want an error that is no refusal", err)
	}
	gone := PinnedImageFrom(func(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
		return nil, fmt.Errorf("deploy@1: %w", artifacts.ErrNotFound)
	})
	if _, err := gone(ctx, adm, "build"); !errors.Is(err, ErrDefinitionNotAdmitted) {
		t.Fatalf("a version that is gone: %v", err)
	}
}

// TestPinnedImageFrom_ReadsADefinitionOnce: the stored version is
// immutable, so a run's commits read it once; a failed read is not kept.
func TestPinnedImageFrom_ReadsADefinitionOnce(t *testing.T) {
	adm, _ := admitted(t, "ork-1", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", DefinitionDigest: "sha256:" + adm.DefinitionDigest,
		Tasks: []*orkstackv1.Task{{Id: "build", Image: &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("e", 64)}}}}
	reads, fail := 0, true
	pin := PinnedImageFrom(func(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
		reads++
		if fail {
			return nil, errors.New("store down")
		}
		return wf, nil
	})
	ctx := context.Background()
	if _, err := pin(ctx, adm, "build"); err == nil {
		t.Fatal("a failed read pinned something")
	}
	fail = false
	for range 3 {
		if got, err := pin(ctx, adm, "build"); err != nil || got != "sha256:"+strings.Repeat("e", 64) {
			t.Fatalf("pin = %q, %v", got, err)
		}
	}
	if reads != 2 {
		t.Fatalf("reads = %d, want 2: the failure, then one read kept", reads)
	}
}
