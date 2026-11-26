package identity

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ReasonCommitIdentityMismatch is a manifest that is not of the attempt
// the request names (workload-identity §3.4 step 7).
const ReasonCommitIdentityMismatch = "COMMIT_IDENTITY_MISMATCH"

// CommitObserver counts each CommitResult's outcome: "granted", or the
// reason it recorded nothing. Counting can fail nothing.
type CommitObserver interface {
	Committed(reason string)
}

// CommitRequest is one CommitResult request, as the API decoded it. A
// preflight carries no Commit.
type CommitRequest struct {
	Namespace, RunID, TaskID string
	Item                     artifacts.Item
	// Scopes are the resume scopes the attempt says it was dispatched under.
	Scopes          Scopes
	Attempt         int32
	TaskSpecSHA256  string
	AdmissionSHA256 string
	// CommitKey is the commit key the attempt's input named, under
	// metadata/commits/.
	CommitKey string
	Commit    *orkstackv1.ActivityCommit
	Preflight bool
}

// CommitStores is what the commit reads and writes in object storage.
type CommitStores interface {
	ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error)
	RecordCommit(ctx context.Context, commitKey string, c *orkstackv1.ActivityCommit) (artifacts.RecordedCommit, error)
}

// Recorder is CommitResult's steps 2 and 4 to 8 (workload-identity §3.4);
// the API holds steps 1 and 3, the replica's guard and the caller's
// task.result.commit at the admitted pool. Steps 2 and 4 to 6a are the
// mint's own (attempt_checks.go).
type Recorder struct {
	Stores CommitStores
	Starts StartReader
	// Describe is a describe of the run, made after the request arrived.
	Describe func(ctx context.Context, runID string) (ExecutionFacts, error)
	Follows  FollowsChecker
	// PinnedImage answers the image index digest the admitted definition
	// pins for a task, "" where it pins none (a tag the Orker resolves).
	PinnedImage func(ctx context.Context, adm artifacts.Admission, taskID string) (string, error)
	// EngineBuild is ORK_BUILD_ID, written into each record.
	EngineBuild string
	Now         func() time.Time
}

// Admit is step 2: the admission the request names, with the task in its
// table.
func (r *Recorder) Admit(ctx context.Context, req CommitRequest) (Admitted, error) {
	adm, row, err := admitTask(ctx, r.Stores, attemptOf(req.Namespace, req.RunID, req.TaskID, req.AdmissionSHA256, req.Item, req.Scopes))
	if err != nil {
		return Admitted{}, err
	}
	return Admitted{Admission: adm, Task: row}, nil
}

// Recorded is what a commit answers; a preflight's is the artifacts of the
// run's start the task names, as the engine's records have them.
type Recorded struct {
	artifacts.RecordedCommit
	Received []*orkstackv1.ArtifactRef
}

// Record is steps 4 to 8 for a request Admit passed and the caller's
// permission allowed. A preflight stops after 6a and writes nothing; a
// commit checks the manifest against the attempt (7) and writes it,
// create-only, with the recording set (8). A record already there is
// answered, Created false: a repeat of this attempt, or another attempt
// that committed first.
func (r *Recorder) Record(ctx context.Context, a Admitted, req CommitRequest, caller Caller) (Recorded, error) {
	c := attemptOf(req.Namespace, req.RunID, req.TaskID, req.AdmissionSHA256, req.Item, req.Scopes)
	held, err := checkAttempt(ctx, attemptChecks{Describe: r.Describe, Starts: r.Starts, Follows: r.Follows}, a.Admission, c,
		req.Attempt, req.TaskSpecSHA256, caller)
	if err != nil {
		return Recorded{}, err
	}
	if req.Preflight {
		return Recorded{Received: held.Received}, nil
	}
	if r.PinnedImage == nil || r.Now == nil {
		return Recorded{}, refuse(ReasonEngineRefused, false, "this engine cannot check a result's image or time it")
	}
	// Step 7: the document.
	if err := r.checkDocument(ctx, a.Admission, req, caller); err != nil {
		return Recorded{}, err
	}
	// Step 8: write it, with the recording the engine alone sets.
	doc := proto.Clone(req.Commit).(*orkstackv1.ActivityCommit)
	doc.Recorded = &orkstackv1.CommitRecording{Pool: a.Task.Pool, OrkerId: caller.OrkerID, AdmissionSha256: req.AdmissionSHA256,
		EngineBuild: r.EngineBuild, Time: timestamppb.New(r.Now().UTC())}
	got, err := r.Stores.RecordCommit(ctx, req.CommitKey, doc)
	switch {
	case errors.Is(err, artifacts.ErrCommitTooLarge), errors.Is(err, artifacts.ErrInvalidRecordKey), errors.Is(err, artifacts.ErrInvalidCommitKey):
		// Nothing a later ask changes: refused, never an outage the Orker
		// would wait out.
		return Recorded{}, mismatch("it cannot be recorded: %v", err)
	case errors.Is(err, artifacts.ErrCommitUndecodable):
		// The engine's own record at the key cannot be read: corruption an
		// operator repairs, which no ask of the Orker's changes.
		return Recorded{}, mismatch("the record already at its key cannot be read, and is not replaced: %v", err)
	case err != nil:
		return Recorded{}, unavailableMint("the result of task %q could not be recorded: %v", req.TaskID, err)
	}
	return Recorded{RecordedCommit: got}, nil
}

// checkDocument is step 7: the manifest decodes at schema 1; its
// namespace, run, task, definition digest, idempotency key, attempt and
// Orker id are the request's, the admission's and the caller's; its image's
// index digest is the one the admitted definition pins, where it pins one;
// it carries no recording of its own; and the commit key is this task's,
// or this item's, and of no other.
func (r *Recorder) checkDocument(ctx context.Context, adm artifacts.Admission, req CommitRequest, caller Caller) error {
	d := req.Commit
	if d == nil {
		return mismatch("the request carries no result")
	}
	wantKey, wantIdem, err := commitCoordinates(req)
	if err != nil {
		return mismatch("the request names no commit key of task %q: %v", req.TaskID, err)
	}
	for _, f := range [...]struct {
		field, got, want string
	}{
		{"namespace", d.GetNamespace(), adm.Namespace},
		{"run", d.GetRunId(), req.RunID},
		{"task", d.GetTaskId(), req.TaskID},
		{"definition digest", d.GetOrkflowDefinitionDigest(), "sha256:" + adm.DefinitionDigest},
		{"idempotency key", d.GetIdempotencyKey(), wantIdem},
		{"orker id", d.GetOrkerId(), caller.OrkerID},
		{"commit key", req.CommitKey, wantKey},
	} {
		if f.got != f.want {
			return mismatch("its %s is %q, and the attempt's is %q", f.field, f.got, f.want)
		}
	}
	switch {
	case d.GetSchemaVersion() != artifacts.CommitSchemaVersion:
		return mismatch("its schema is %d, not %d", d.GetSchemaVersion(), artifacts.CommitSchemaVersion)
	case d.GetAttempt() != req.Attempt:
		return mismatch("it is of attempt %d, and the request of attempt %d", d.GetAttempt(), req.Attempt)
	case d.GetExitCode() != 0:
		return mismatch("it is of an exit %d, and only a success is recorded", d.GetExitCode())
	case d.GetImage().GetIndexDigest() == "":
		return mismatch("it names no image digest")
	case d.GetRecorded() != nil:
		return mismatch("it carries a recording, which only an engine writes")
	}
	pinned, err := r.PinnedImage(ctx, adm, req.TaskID)
	if errors.Is(err, ErrDefinitionNotAdmitted) {
		return refuse(ReasonNotAdmitted, true, "the definition run %q was admitted with cannot be checked: %v", req.RunID, err)
	}
	if err != nil {
		return unavailableMint("the image the definition of run %q pins for task %q could not be read: %v", req.RunID, req.TaskID, err)
	}
	if pinned != "" && d.GetImage().GetIndexDigest() != pinned {
		return mismatch("its image is %q, and the definition pins %q", d.GetImage().GetIndexDigest(), pinned)
	}
	return nil
}

// commitCoordinates is the commit key and the idempotency key of the
// attempt the request names, under its own run's resume scope: a run below a
// root has its own id and its keys have none.
func commitCoordinates(req CommitRequest) (key, idempotency string, err error) {
	if req.Item.Set {
		key, err = artifacts.ScopedItemCommitKey(req.Namespace, req.RunID, req.TaskID, req.Item.Index, req.Scopes.Resume)
		return key, task.ScopedItemIdempotencyKeyFor(req.Namespace, req.RunID, req.TaskID, req.Item.Index, req.Scopes.Resume), err
	}
	key, err = artifacts.ScopedCommitKey(req.Namespace, req.RunID, req.TaskID, req.Scopes.Resume)
	return key, task.ScopedIdempotencyKeyFor(req.Namespace, req.RunID, req.TaskID, req.Scopes.Resume), err
}

func mismatch(format string, args ...any) *MintError {
	return refuse(ReasonCommitIdentityMismatch, true, "the result is not the attempt's: %s", fmt.Sprintf(format, args...))
}

// ErrDefinitionNotAdmitted is a stored version that is not what the run was
// admitted with: gone, another definition, or without the task. Nothing a
// later ask changes, so the commit is refused, never unavailable.
var ErrDefinitionNotAdmitted = errors.New("the stored version is not the admitted definition")

// DefinitionReader reads one stored version of an Orkflow; a version that
// does not exist is artifacts.ErrNotFound.
type DefinitionReader func(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error)

// pinnedDefinitions bounds how many admitted definitions' pins a
// replica keeps: each is a map of task ids to digests.
const pinnedDefinitions = 256

// PinnedImageFrom answers Recorder.PinnedImage from the stored, immutable
// version the run was admitted with: the image index digest the engine
// stamped into the task when the version was created, "" where it stamped
// none. A version that is not the admitted definition, or that has no such
// task, is an error, never "pins nothing". A version is immutable, so what
// it pins is read once and kept, the least recently used dropped first; a
// read that failed is not kept.
func PinnedImageFrom(read DefinitionReader) func(ctx context.Context, adm artifacts.Admission, taskID string) (string, error) {
	c := &pinCache{read: read, order: list.New(), byKey: map[string]*list.Element{}}
	return c.pinned
}

// AdmittedDefinitionFrom reads the definition a run was admitted with: the
// stored, immutable version its admission names.
func AdmittedDefinitionFrom(read DefinitionReader) func(ctx context.Context, adm artifacts.Admission) (*orkstackv1.Orkflow, error) {
	return func(ctx context.Context, adm artifacts.Admission) (*orkstackv1.Orkflow, error) {
		return admittedDefinition(ctx, read, definitionNamespace(adm), adm)
	}
}

func definitionNamespace(adm artifacts.Admission) string {
	if adm.OrkflowNamespace != "" {
		return adm.OrkflowNamespace
	}
	return adm.Namespace
}

func admittedDefinition(ctx context.Context, read DefinitionReader, ns string, adm artifacts.Admission) (*orkstackv1.Orkflow, error) {
	wf, err := read(ctx, ns, adm.Orkflow, adm.OrkflowVersion)
	if errors.Is(err, artifacts.ErrNotFound) {
		return nil, fmt.Errorf("%s/%s@%s is gone: %w", ns, adm.Orkflow, adm.OrkflowVersion, ErrDefinitionNotAdmitted)
	}
	if err != nil {
		return nil, fmt.Errorf("read %s/%s@%s: %w", ns, adm.Orkflow, adm.OrkflowVersion, err)
	}
	if got := wf.GetDefinitionDigest(); got != "sha256:"+adm.DefinitionDigest {
		return nil, fmt.Errorf("%s/%s@%s is definition %q, and the run was admitted with sha256:%s: %w", ns, adm.Orkflow, adm.OrkflowVersion, got, adm.DefinitionDigest, ErrDefinitionNotAdmitted)
	}
	return wf, nil
}

type pinCache struct {
	read  DefinitionReader
	mu    sync.Mutex
	order *list.List
	byKey map[string]*list.Element
}

type pinEntry struct {
	key  string
	pins map[string]string
}

func (c *pinCache) pinned(ctx context.Context, adm artifacts.Admission, taskID string) (string, error) {
	ns := definitionNamespace(adm)
	key := ns + "/" + adm.Orkflow + "@" + adm.OrkflowVersion + "#" + adm.DefinitionDigest
	pins, err := c.pins(ctx, key, ns, adm)
	if err != nil {
		return "", err
	}
	digest, ok := pins[taskID]
	if !ok {
		return "", fmt.Errorf("%s/%s@%s has no task %q: %w", ns, adm.Orkflow, adm.OrkflowVersion, taskID, ErrDefinitionNotAdmitted)
	}
	return digest, nil
}

func (c *pinCache) pins(ctx context.Context, key, ns string, adm artifacts.Admission) (map[string]string, error) {
	c.mu.Lock()
	if e, ok := c.byKey[key]; ok {
		c.order.MoveToFront(e)
		pins := e.Value.(*pinEntry).pins
		c.mu.Unlock()
		return pins, nil
	}
	c.mu.Unlock()
	wf, err := admittedDefinition(ctx, c.read, ns, adm)
	if err != nil {
		return nil, err
	}
	pins := make(map[string]string, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		pins[t.GetId()] = t.GetImage().GetIndexDigest()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.byKey[key]; !ok {
		c.byKey[key] = c.order.PushFront(&pinEntry{key: key, pins: pins})
		for c.order.Len() > pinnedDefinitions {
			oldest := c.order.Back()
			c.order.Remove(oldest)
			delete(c.byKey, oldest.Value.(*pinEntry).key)
		}
	}
	return pins, nil
}
