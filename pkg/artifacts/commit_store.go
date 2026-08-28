package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// CommitStore holds the durable activity completion records — the object whose
// existence, and only whose existence, means an activity finished.
//
//	metadata/commits/{namespace}/{encoded-run-id}/{task-id}/commit.json
//
// It is deliberately NOT artifacts.Store. Store has no conditional write, so a
// commit written through it is silently overwritable, which is the opposite of
// an immutable record; and the local backend addresses objects by content, so
// a retry that does not yet know the content cannot look the commit up by name
// at all — a Stat by logical name fails, and a second Put of the same name with
// different bytes creates a SECOND object rather than being refused. Both are
// measured in the design's §14.2. A new concern gets a new store, which is what
// LogStore, NamespaceStore, OrkflowStore and RunArchive each already are.
//
// Implementations are the arbitration point of the whole commit protocol: a
// crashed Orker's retry, and a second Orker Temporal handed the same activity
// to, both reach Create with the same uri, and exactly one of them may win.
type CommitStore interface {
	// Create writes the commit if and only if no object exists at uri.
	//
	// An existing object returns an error matching ErrPreconditionFailed. That
	// is a fact about the OBJECT, not about the request: the caller does not
	// retry verbatim, it Gets what is there, re-checks identity, and returns
	// the winner's result (§5.4 step 8).
	//
	// The returned record's SHA256 is of the exact bytes stored. It is never
	// recomputed by re-marshalling a decoded commit, because protojson's output
	// is deliberately unstable across builds (§14.10).
	Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (CommitRecord, error)
	// Get reads the commit at uri. A missing object returns an error matching
	// ErrNotFound; an object that exists but does not decode returns an error
	// matching ErrCommitUndecodable, carrying the bytes read so the caller can
	// name the size in its terminal error (§5.4, "the undecodable commit").
	Get(ctx context.Context, uri string) (CommitRecord, error)
}

// CommitRecord is one stored commit plus the facts about its bytes.
type CommitRecord struct {
	// Commit is the decoded record. It is nil only alongside an error.
	Commit *orkstackv1.ActivityCommit
	// ETag is the store's opaque name for these bytes, as the store reported
	// it. Backends without one leave it empty.
	ETag string
	// SHA256 is the lowercase hex digest of the exact stored bytes — the ones
	// written by Create or read by Get, never a re-marshalling of Commit.
	SHA256 string
	// Bytes is the length of those same bytes.
	Bytes int64
}

const (
	// CommitPrefix roots the commit records in the store. It sits under
	// metadata/, beside metadata/logs, metadata/runs, metadata/orkflows and
	// metadata/namespaces — and crucially NOT under the orkflows/ (S3) or
	// objects/ (local) namespaces where workload artifacts land, so a workload
	// that names an artifact "commit.json" cannot collide with a commit key.
	CommitPrefix = "metadata/commits"

	// CommitObjectName is the fixed leaf of every commit key.
	CommitObjectName = "commit.json"

	// CommitProbePrefix is deliberately outside CommitPrefix: objects under
	// metadata/commits are activity completion records, while objects here are
	// bounded deployment-capability probes.
	CommitProbePrefix = "metadata/probes/commit-store"

	// CommitProbeKey is versioned with the candidate schema. A build that
	// changes CommitSchemaVersion must move to a fresh key rather than refusing
	// to boot forever on the durable winner written by an older build.
	CommitProbeKey = CommitProbePrefix + "/schema-v1/conditional-create/" + CommitObjectName

	// CommitContentType is what a commit record is served as.
	CommitContentType = "application/json"

	// CommitSchemaVersion is the only ActivityCommit schema this build writes,
	// and the only one it will adopt. A reader that finds any other value
	// refuses the commit with ContractViolation/COMMIT_SCHEMA_UNSUPPORTED
	// rather than guessing: an Orker that cannot read a commit must neither
	// overwrite it nor re-run past it.
	CommitSchemaVersion = 1

	// CommitMaxBytes bounds one serialized commit record.
	//
	// It is 4 MiB, not the 64 KiB an earlier draft carried. 64 KiB is
	// unreachable for a LEGAL workload: artifact_max_count is 256, and a commit
	// carrying 256 ArtifactRefs with 8-character names serializes to 67,699
	// bytes, crossing 64 KiB at 248 refs (measured, §14.7). A limit a
	// conforming workload cannot respect is a bug in the limit. 4 MiB leaves
	// ~60x headroom over the worst legal case and stays ~1,250x below the
	// single-PutObject ceiling, so the write never needs the multipart manager.
	CommitMaxBytes = 4 << 20
)

// ErrInvalidCommitKey reports a commit coordinate that cannot be addressed.
var ErrInvalidCommitKey = errors.New("invalid commit key")

// ErrCommitUndecodable reports an object that exists at a commit uri but is not
// a valid ActivityCommit document — a torn write from a pre-link(2) writer, or
// corruption. It is distinct from ErrNotFound on purpose: treating it as "no
// commit" would let a second container run past a possibly durable commit.
var ErrCommitUndecodable = errors.New("commit is undecodable")

// CommitKey renders the object key holding one activity's commit record:
// metadata/commits/{namespace}/{encoded-run-id}/{task-id}/commit.json.
//
// It is deterministic and attempt-independent — namespace, run id and task id
// do not change across retries, and a run pins exactly one definition, so
// run+task determines everything the commit describes.
//
// The namespace and the task id are VALIDATED, never sanitized: both are slugs
// (^[a-z][a-z0-9-]*$, <= 200 bytes) by the same rule every other boundary
// applies, so neither can escape the prefix.
//
// The run id is ENCODED, never parsed. AGENTS.md §7 is explicit that nothing
// may derive meaning from a run id's parts, so it becomes exactly one segment
// through encodeArchiveSegment — the same reversible encoder
// metadata/runs/{encoded-run-id}.jsonl already uses. Requiring the key to match
// ork-{timestamp}-{sequence} would reject every run id the tests inject.
//
// The orkflow name is deliberately absent: a run id is already unique, and one
// fewer segment is one fewer name to validate.
//
// This is the single shared renderer, in the spirit of internal/task/pool.go
// centralizing the pool-to-queue mapping. The workflow composes the uri as
// plan.CommitRoot + "/" + CommitKey(...) and sends it; the Orker MUST use the
// value it was sent rather than re-deriving one, so there is exactly one
// convention with exactly one owner.
func CommitKey(namespace, runID, taskID string) (string, error) {
	namespace = ResolveNamespace(namespace)
	if err := validNamespaceName(namespace); err != nil {
		return "", fmt.Errorf("artifacts: commit key: namespace: %w", errors.Join(ErrInvalidCommitKey, err))
	}
	run, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: commit key: run id: %w", errors.Join(ErrInvalidCommitKey, err))
	}
	if err := validCommitTaskID(taskID); err != nil {
		return "", fmt.Errorf("artifacts: commit key: task id: %w", errors.Join(ErrInvalidCommitKey, err))
	}
	return path.Join(CommitPrefix, namespace, run, taskID, CommitObjectName), nil
}

// validCommitTaskID holds a task id to the DSL's slug rule. A task id reaches
// this package already validated by internal/dsl, so this is a second gate
// rather than the first — but a key builder that trusted its caller would be
// the one place a traversal segment could enter the prefix.
func validCommitTaskID(taskID string) error {
	switch {
	case taskID == "":
		return fmt.Errorf("artifacts: task id is empty: %w", ErrInvalidCommitKey)
	case len(taskID) > maxPoolName:
		return fmt.Errorf("artifacts: task id of %d bytes exceeds the %d-byte limit (%.32q…): %w",
			len(taskID), maxPoolName, taskID, ErrInvalidCommitKey)
	case !namespaceNamePattern.MatchString(taskID):
		return fmt.Errorf("artifacts: task id %q must match %s: %w",
			taskID, namespaceNamePattern, ErrInvalidCommitKey)
	}
	return nil
}

// MarshalCommit renders the exact bytes a commit is stored as.
//
// It is exported because §5.4 has two callers that must agree to the byte: step
// 4 projects a candidate commit and asserts its size against CommitMaxBytes
// BEFORE uploading anything, and step 8 writes it. If those two used different
// encoders, measuring the first would prove nothing about the second.
//
// protojson is followed by json.Indent for the reason the namespace and orkflow
// stores already do it: protojson inserts random whitespace on purpose, to deter
// byte-for-byte comparison (§14.10). Re-indenting normalizes that away, so two
// marshals of the same message in the same build produce the same bytes. It
// still does NOT make the encoding stable across builds, which is why a stored
// commit's SHA256 always comes from the bytes that crossed the store boundary
// and never from re-marshalling a decoded one.
func MarshalCommit(c *orkstackv1.ActivityCommit) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("artifacts: encode commit: %w: nil commit", ErrInvalidCommitKey)
	}
	raw, err := protojson.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode commit for run %q task %q: %w",
			c.GetRunId(), c.GetTaskId(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode commit for run %q task %q: %w",
			c.GetRunId(), c.GetTaskId(), err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// UnmarshalCommit decodes a stored commit. Unknown fields are discarded, so an
// engine can read what a newer engine wrote — the schema_version check, not the
// decoder, is what refuses a schema this build does not understand.
func UnmarshalCommit(document []byte) (*orkstackv1.ActivityCommit, error) {
	c := &orkstackv1.ActivityCommit{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, c); err != nil {
		return nil, fmt.Errorf("decode commit document: %w", errors.Join(ErrCommitUndecodable, err))
	}
	return c, nil
}

// MemoryCommitStore is an in-memory CommitStore for tests and for the seam
// Phase 1 needs before Phase 2's S3 and local backends exist.
//
// It is a real implementation of the contract rather than a stub: Create is
// genuinely create-if-absent under a mutex, so a two-writer race against it
// produces one winner and one ErrPreconditionFailed exactly as the durable
// backends must. That is what lets the OCIDispatcher's own tests exercise the
// adoption and 412 paths without a bucket.
//
// It is NOT durable and must never be selected by a deployment.
type MemoryCommitStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

// compile-time contract check.
var _ CommitStore = (*MemoryCommitStore)(nil)

// NewMemoryCommitStore returns an empty in-memory commit store.
func NewMemoryCommitStore() *MemoryCommitStore {
	return &MemoryCommitStore{objects: make(map[string][]byte)}
}

// Create writes the commit if and only if uri is unoccupied.
func (s *MemoryCommitStore) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (CommitRecord, error) {
	if err := ctx.Err(); err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: memory commit create %q: %w", uri, err)
	}
	if uri == "" {
		return CommitRecord{}, fmt.Errorf("artifacts: memory commit create: %w: empty uri", ErrInvalidCommitKey)
	}
	document, err := MarshalCommit(c)
	if err != nil {
		return CommitRecord{}, err
	}
	if len(document) > CommitMaxBytes {
		return CommitRecord{}, fmt.Errorf("artifacts: memory commit create %q: commit is %d bytes, limit is %d: %w",
			uri, len(document), CommitMaxBytes, ErrInvalidCommitKey)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objects[uri]; exists {
		return CommitRecord{}, fmt.Errorf("artifacts: memory commit create %q: %w", uri, ErrPreconditionFailed)
	}
	stored := bytes.Clone(document)
	s.objects[uri] = stored
	return recordFor(stored, c)
}

// Get reads the commit at uri.
func (s *MemoryCommitStore) Get(ctx context.Context, uri string) (CommitRecord, error) {
	if err := ctx.Err(); err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: memory commit get %q: %w", uri, err)
	}
	s.mu.Lock()
	stored, ok := s.objects[uri]
	s.mu.Unlock()
	if !ok {
		return CommitRecord{}, fmt.Errorf("artifacts: memory commit get %q: %w", uri, ErrNotFound)
	}
	document := bytes.Clone(stored)
	c, err := UnmarshalCommit(document)
	if err != nil {
		return CommitRecord{Bytes: int64(len(document))},
			fmt.Errorf("artifacts: memory commit get %q: %w", uri, err)
	}
	return recordFor(document, c)
}

// PutRaw plants arbitrary bytes at uri, unconditionally. It exists so a test
// can reproduce the undecodable-commit branch of §5.4 without a torn write, and
// it is not part of the CommitStore contract.
func (s *MemoryCommitStore) PutRaw(uri string, document []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[uri] = bytes.Clone(document)
}

// recordFor pairs a decoded commit with the facts about the exact bytes it came
// from or went to. Both Create and Get build their record here, so neither can
// drift into recomputing a digest from the message.
func recordFor(document []byte, c *orkstackv1.ActivityCommit) (CommitRecord, error) {
	sum := sha256.Sum256(document)
	digest := hex.EncodeToString(sum[:])
	return CommitRecord{
		Commit: c,
		// The memory store has no server-assigned entity tag, so it names the
		// bytes with the digest it already computed, quoted the way an HTTP
		// ETag is. Callers treat it as opaque.
		ETag:   `"` + digest + `"`,
		SHA256: digest,
		Bytes:  int64(len(document)),
	}, nil
}
