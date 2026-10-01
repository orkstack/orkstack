package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Temporal owns activity retries, so a workflow only ever learns a task's FINAL
// outcome: the error of every attempt before it is discarded when the next one
// is scheduled. The Orker is the only process that observes each attempt, so it
// writes one small immutable record per attempt:
//
//	metadata/attempts/{run-id}/{task-id}/{attempt}.json   one TaskAttempt
//
// It is the failure-side sibling of metadata/commits/…/commit.json. A commit
// says what the winning attempt produced; these say what every attempt did,
// including the ones that lost.
//
// ORDER LIVES IN THE KEY, the same way it does for log chunks: the attempt is
// zero-padded to attemptWidth, so a lexicographic listing IS numeric order and
// a reader never sorts by LastModified. The width is deliberately the same
// constant the log keys use — the two are read side by side, and an attempt
// that spelled its number differently in two prefixes could not be joined.
//
// The record is written with a PLAIN put, last-writer-wins, unlike commit.json's
// conditional create. Temporal never runs the same attempt number of the same
// activity twice, so there is no second writer to arbitrate against; and a
// record is a projection, never the thing that decides whether work reruns.
const (
	// AttemptPrefix roots every task attempt record in the store. It sits under
	// metadata/, beside metadata/logs and metadata/commits — and crucially NOT
	// under the orkflows/ (S3) or objects/ (local) namespaces where workload
	// artifacts land, so a workload artifact can never collide with a record
	// and an operator can express one bucket lifecycle rule over this prefix.
	AttemptPrefix = "metadata/attempts"

	// AttemptContentType is what an attempt record is served as.
	AttemptContentType = "application/json"

	// AttemptSchemaVersion is the only TaskAttempt schema this build writes.
	AttemptSchemaVersion = 1

	// AttemptObjectSuffix is the fixed leaf suffix of every attempt key.
	AttemptObjectSuffix = ".json"

	// attemptWidth is the fixed width of the attempt segment. It is
	// logAttemptWidth's value for logAttemptWidth's reason — int32 tops out at
	// 10 digits, and unpadded "10" sorts before "2" — and it is written as a
	// reference to that constant so the two prefixes can never drift apart.
	attemptWidth = logAttemptWidth
)

// Truncation bounds for one stored record.
//
// A TaskAttempt is diagnosis, not content: the failing attempt's stdout and
// stderr are already durable under metadata/logs, and its uploaded artifacts
// are referenced rather than copied. What is left is an error message and the
// scan notes attached to it, both of which are short sentences in every
// failure the Orker constructs — but Failure.Message interpolates values a
// workload controls (a file name, a registry error), so a bound is the
// difference between a record and an amplifier.
//
// The bounds are their own constants rather than WorkspaceLimits.commit_max_bytes:
// that limit is 4 MiB and exists to fit 256 ArtifactRefs (§5.3, §14.7), which
// is a statement about a commit's legal CONTENT. An attempt record carries at
// most two system-artifact refs, so sizing it by the commit's ceiling would
// bound nothing.
const (
	// AttemptErrorMessageMaxBytes bounds Error.message.
	AttemptErrorMessageMaxBytes = 4 << 10
	// AttemptErrorDetailsMaxBytes bounds Error.details.
	AttemptErrorDetailsMaxBytes = 16 << 10
	// AttemptMaxBytes bounds the whole serialized record.
	AttemptMaxBytes = 64 << 10
	// AttemptTruncationMarker is appended to anything this package shortened,
	// so a reader is never left guessing whether it has the whole string.
	AttemptTruncationMarker = "...[truncated]"
)

// ErrInvalidAttemptKey reports an attempt coordinate that cannot be addressed.
var ErrInvalidAttemptKey = errors.New("invalid task attempt key")

// AttemptRef addresses one attempt of one task within one run.
type AttemptRef struct {
	RunID   string
	TaskID  string
	Attempt int32
}

// AttemptStore reads and writes the per-attempt records.
//
// It is a separate interface from CommitStore because the two have opposite
// arbitration: a commit is created if and only if nothing is there, while an
// attempt record has exactly one possible writer and is simply put. It is a
// separate interface from Store because Store addresses workload bytes by
// content, and this is a platform record at a key the coordinates derive.
//
// Implementations hold no cache and no per-run state: the Orker that writes a
// record and the engine replica that serves it are different processes, and any
// replica must answer for any run (AGENTS.md §7).
type AttemptStore interface {
	// PutAttempt writes one record. The record is bounded (AttemptMaxBytes)
	// before it is stored; an over-long message or details is truncated with
	// AttemptTruncationMarker rather than refused, because a shortened record
	// of a failed attempt is worth more than no record at all.
	PutAttempt(ctx context.Context, attempt *orkstackv1.TaskAttempt) error
	// ListAttempts returns every stored record for one task, ascending by
	// attempt. A task with no records reads EMPTY, never an error: no record is
	// the ordinary state of a task that has not run yet, and of every run that
	// finished before this key family existed.
	ListAttempts(ctx context.Context, runID, taskID string) ([]*orkstackv1.TaskAttempt, error)
}

// RunAttemptReader is the run-wide read usage accounting needs: every record
// key of one run in one listing, and one record by its address. It is a
// separate interface from AttemptStore, and asserted rather than required,
// for the reason RunIndexTallier is separate from RunArchive: the stores
// that implement it are exactly the three durable-or-test backends in this
// package, and no Orker or test double of AttemptStore needs to grow a
// method it will never be asked for.
type RunAttemptReader interface {
	// ListRunAttemptRefs returns the address of every record under
	// RunAttemptPrefix(runID), in key order — task segment, then attempt
	// ascending — by one prefix listing (paged on S3). A key ParseAttemptKey
	// rejects, or one belonging to another run, is skipped. A run with no
	// records reads EMPTY, never an error.
	ListRunAttemptRefs(ctx context.Context, runID string) ([]AttemptRef, error)
	// GetAttempt reads one record. A record that is gone — expired under a
	// lifecycle rule or reclaimed between a listing and this read — is an
	// error matching ErrNotFound.
	GetAttempt(ctx context.Context, ref AttemptRef) (*orkstackv1.TaskAttempt, error)
}

// runAttemptRefs keeps the keys of one run's records, parsed and in key
// order — the one filter every backend's run-wide listing applies.
func runAttemptRefs(runID string, keys []string) []AttemptRef {
	sort.Strings(keys)
	refs := make([]AttemptRef, 0, len(keys))
	for _, key := range keys {
		ref, ok := ParseAttemptKey(key)
		if !ok || ref.RunID != runID {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

// RunAttemptPrefix renders the prefix containing every attempt record of a run:
// metadata/attempts/{run-id}/. Run-scoped cleanup deletes this prefix, which is
// what keeps attempt records on the same lifecycle as the logs they explain.
func RunAttemptPrefix(runID string) (string, error) {
	segment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run attempt prefix: run id: %w", errors.Join(ErrInvalidAttemptKey, err))
	}
	return path.Join(AttemptPrefix, segment) + "/", nil
}

// AttemptTaskPrefix renders the prefix holding every attempt of one task:
// metadata/attempts/{run-id}/{task-id}/.
func AttemptTaskPrefix(runID, taskID string) (string, error) {
	run, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: task attempt key: run id: %w", errors.Join(ErrInvalidAttemptKey, err))
	}
	taskSegment, err := encodeArchiveSegment(taskID)
	if err != nil {
		return "", fmt.Errorf("artifacts: task attempt key: task id: %w", errors.Join(ErrInvalidAttemptKey, err))
	}
	return path.Join(AttemptPrefix, run, taskSegment) + "/", nil
}

// AttemptKey renders the object key for one attempt record:
// metadata/attempts/{run-id}/{task-id}/{attempt}.json.
func AttemptKey(ref AttemptRef) (string, error) {
	prefix, err := AttemptTaskPrefix(ref.RunID, ref.TaskID)
	if err != nil {
		return "", err
	}
	if ref.Attempt < 1 {
		// Attempts are 1-based (activity.GetInfo(ctx).Attempt), so 0 is a bug
		// at the call site rather than a record of attempt zero.
		return "", fmt.Errorf("artifacts: task attempt key: attempt %d is not 1-based: %w",
			ref.Attempt, ErrInvalidAttemptKey)
	}
	return prefix + fmt.Sprintf("%0*d", attemptWidth, ref.Attempt) + AttemptObjectSuffix, nil
}

// ParseAttemptKey reverses AttemptKey. A key this package did not generate
// reports ok=false rather than an error, so a scan skips foreign objects
// instead of failing on them.
func ParseAttemptKey(key string) (ref AttemptRef, ok bool) {
	rest, found := strings.CutPrefix(key, AttemptPrefix+"/")
	if !found {
		return AttemptRef{}, false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		return AttemptRef{}, false
	}
	runID, ok := decodeArchiveSegment(parts[0])
	if !ok {
		return AttemptRef{}, false
	}
	taskID, ok := decodeArchiveSegment(parts[1])
	if !ok {
		return AttemptRef{}, false
	}
	segment, cut := strings.CutSuffix(parts[2], AttemptObjectSuffix)
	if !cut {
		return AttemptRef{}, false
	}
	attempt, ok := parseFixedWidth(segment, attemptWidth)
	if !ok || attempt < 1 || attempt > (1<<31-1) {
		return AttemptRef{}, false
	}
	return AttemptRef{RunID: runID, TaskID: taskID, Attempt: int32(attempt)}, true
}

// MarshalAttempt renders the exact bytes an attempt record is stored as, after
// bounding it.
//
// protojson is followed by json.Indent for the reason MarshalCommit already
// documents: protojson inserts random whitespace on purpose to deter
// byte-for-byte comparison, and re-indenting normalizes that away.
func MarshalAttempt(attempt *orkstackv1.TaskAttempt) ([]byte, error) {
	if attempt == nil {
		return nil, fmt.Errorf("artifacts: encode task attempt: %w: nil record", ErrInvalidAttemptKey)
	}
	bounded, err := BoundAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return marshalAttemptExact(bounded)
}

func marshalAttemptExact(attempt *orkstackv1.TaskAttempt) ([]byte, error) {
	raw, err := protojson.Marshal(attempt)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode task attempt for run %q task %q attempt %d: %w",
			attempt.GetRunId(), attempt.GetTaskId(), attempt.GetAttempt(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode task attempt for run %q task %q attempt %d: %w",
			attempt.GetRunId(), attempt.GetTaskId(), attempt.GetAttempt(), err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// UnmarshalAttempt decodes a stored record. Unknown fields are discarded, so an
// engine can read what a newer engine wrote.
func UnmarshalAttempt(document []byte) (*orkstackv1.TaskAttempt, error) {
	attempt := &orkstackv1.TaskAttempt{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, attempt); err != nil {
		return nil, fmt.Errorf("artifacts: decode task attempt document: %w", err)
	}
	return attempt, nil
}

// BoundAttempt returns a copy of the record cut down to the documented bounds.
// The original is never modified: the caller owns a message it may still be
// logging or observing, and a store must not rewrite it.
//
// The ladder is deterministic, and it gives up the least useful field first.
// The message and the details are cut to their own limits, and only if the
// whole record is still over AttemptMaxBytes are they cut further — down to
// nothing but the marker, which still leaves the outcome, the reason code, the
// phase and the coordinates: the fields that say WHICH attempt failed and why,
// which is what the record exists for.
func BoundAttempt(attempt *orkstackv1.TaskAttempt) (*orkstackv1.TaskAttempt, error) {
	if attempt == nil {
		return nil, fmt.Errorf("artifacts: bound task attempt: %w: nil record", ErrInvalidAttemptKey)
	}
	bounded, _ := proto.Clone(attempt).(*orkstackv1.TaskAttempt)
	if failure := bounded.GetError(); failure != nil {
		failure.Message = truncateUTF8(failure.GetMessage(), AttemptErrorMessageMaxBytes)
		failure.Details = truncateUTF8(failure.GetDetails(), AttemptErrorDetailsMaxBytes)
	}
	document, err := marshalAttemptExact(bounded)
	if err != nil {
		return nil, err
	}
	if len(document) <= AttemptMaxBytes {
		return bounded, nil
	}
	// Details first: the message names the failure, the details are notes on
	// it. Then the message. Each step re-measures rather than computing a
	// budget, because the encoder's escaping decides how many bytes a string
	// costs and no arithmetic here can predict it.
	for _, shrink := range []func(*orkstackv1.Error){
		func(e *orkstackv1.Error) { e.Details = markerOnly(e.GetDetails()) },
		func(e *orkstackv1.Error) { e.Message = markerOnly(e.GetMessage()) },
	} {
		failure := bounded.GetError()
		if failure == nil {
			break
		}
		shrink(failure)
		document, err = marshalAttemptExact(bounded)
		if err != nil {
			return nil, err
		}
		if len(document) <= AttemptMaxBytes {
			return bounded, nil
		}
	}
	// Nothing string-shaped is left to cut. The remaining bulk is references —
	// a pathological image reference, say — and dropping the record entirely
	// would lose the outcome, so it is stored as it stands. The store's own
	// size check (below) is what refuses it, with the size named.
	return bounded, nil
}

// truncateUTF8 cuts s to at most max bytes, on a rune boundary, appending
// AttemptTruncationMarker when anything was removed. A string already within
// the bound is returned unchanged, marker included in the accounting so a
// truncated string never exceeds max.
func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	keep := max - len(AttemptTruncationMarker)
	if keep < 0 {
		keep = 0
	}
	for keep > 0 && !utf8Boundary(s, keep) {
		keep--
	}
	return s[:keep] + AttemptTruncationMarker
}

// markerOnly reduces a string to the truncation marker, or leaves an already
// empty string empty — an empty field says "there was nothing here", and a
// marker in its place would say "something was removed".
func markerOnly(s string) string {
	if s == "" {
		return ""
	}
	return AttemptTruncationMarker
}

// utf8Boundary reports whether index i starts a rune in s. Cutting mid-rune
// would leave a replacement character in a record whose whole job is to be
// read by a human.
func utf8Boundary(s string, i int) bool {
	if i <= 0 || i >= len(s) {
		return true
	}
	return s[i]&0xC0 != 0x80
}

// AttemptStoreFactory builds an AttemptStore from a Config. A backend that can
// hold attempt records registers one, mirroring Factory, ArchiveFactory and
// LogStoreFactory.
type AttemptStoreFactory func(cfg Config) (AttemptStore, error)

var attemptStoreRegistry = map[string]AttemptStoreFactory{}

// RegisterAttemptStore adds an attempt-store factory under name.
func RegisterAttemptStore(name string, f AttemptStoreFactory) error {
	if name == "" {
		return fmt.Errorf("artifacts: register attempt store: empty name")
	}
	if f == nil {
		return fmt.Errorf("artifacts: register attempt store %q: nil factory", name)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := attemptStoreRegistry[name]; exists {
		return fmt.Errorf("artifacts: register attempt store %q: already registered", name)
	}
	attemptStoreRegistry[name] = f
	return nil
}

// AttemptStoreBackends lists the backends that can hold attempt records, sorted.
func AttemptStoreBackends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(attemptStoreRegistry))
	for name := range attemptStoreRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// NewAttemptStore builds the AttemptStore named by cfg.Backend, so one
// ORK_ARTIFACT_BACKEND selects where artifacts, run history, task logs,
// snapshots and attempt records all live. There is no setting of its own.
func NewAttemptStore(cfg Config) (AttemptStore, error) {
	name := cfg.Backend
	if name == "" {
		name = DefaultBackend
		cfg.Backend = name
	}
	registryMu.RLock()
	f, ok := attemptStoreRegistry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: attempt store backend %q: %w (registered: %v)",
			name, ErrUnknownBackend, AttemptStoreBackends())
	}
	store, err := f(cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: attempt store backend %q: %w", name, err)
	}
	return store, nil
}

// NewAttemptStoreFromEnv builds the AttemptStore described by the ORK_*
// environment.
func NewAttemptStoreFromEnv() (AttemptStore, error) { return NewAttemptStore(ConfigFromEnv()) }

func init() {
	mustRegisterAttemptStore(BackendLocal, func(cfg Config) (AttemptStore, error) {
		return NewLocalAttemptStore(localDir(cfg))
	})
	mustRegisterAttemptStore(BackendS3, func(cfg Config) (AttemptStore, error) {
		return NewS3AttemptStore(context.Background(), cfg)
	})
}

func mustRegisterAttemptStore(name string, f AttemptStoreFactory) {
	if err := RegisterAttemptStore(name, f); err != nil {
		panic(err)
	}
}

// MemoryAttemptStore is an in-memory AttemptStore for tests. It bounds and
// encodes records exactly as the durable backends do, so a test that asserts a
// truncation bound against it is asserting the real one. It is NOT durable and
// must never be selected by a deployment.
type MemoryAttemptStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

// compile-time contract check.
var (
	_ AttemptStore     = (*MemoryAttemptStore)(nil)
	_ RunAttemptReader = (*MemoryAttemptStore)(nil)
)

// NewMemoryAttemptStore returns an empty in-memory attempt store.
func NewMemoryAttemptStore() *MemoryAttemptStore {
	return &MemoryAttemptStore{objects: make(map[string][]byte)}
}

// PutAttempt stores one record.
func (s *MemoryAttemptStore) PutAttempt(ctx context.Context, attempt *orkstackv1.TaskAttempt) error {
	key, document, err := encodeAttemptForStore(attempt)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("artifacts: memory task attempt %q: %w", key, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = document
	return nil
}

// ListAttempts returns one task's records, ascending by attempt.
func (s *MemoryAttemptStore) ListAttempts(
	ctx context.Context, runID, taskID string,
) ([]*orkstackv1.TaskAttempt, error) {
	prefix, err := AttemptTaskPrefix(runID, taskID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	found := make([]*orkstackv1.TaskAttempt, 0, len(s.objects))
	for key, document := range s.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, ok := ParseAttemptKey(key); !ok {
			continue
		}
		record, err := UnmarshalAttempt(document)
		if err != nil {
			return nil, fmt.Errorf("artifacts: memory task attempt %q: %w", key, err)
		}
		found = append(found, record)
	}
	sortAttempts(found)
	return found, nil
}

// ListRunAttemptRefs lists every record key of one run, in key order.
func (s *MemoryAttemptStore) ListRunAttemptRefs(ctx context.Context, runID string) ([]AttemptRef, error) {
	prefix, err := RunAttemptPrefix(runID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	s.mu.Unlock()
	return runAttemptRefs(runID, keys), nil
}

// GetAttempt reads one record; a missing one matches ErrNotFound.
func (s *MemoryAttemptStore) GetAttempt(ctx context.Context, ref AttemptRef) (*orkstackv1.TaskAttempt, error) {
	key, err := AttemptKey(ref)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	document, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: memory task attempt %q: %w", key, ErrNotFound)
	}
	record, err := UnmarshalAttempt(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: memory task attempt %q: %w", key, err)
	}
	return record, nil
}

// DeleteRunAttempts removes every record of one run — what the cleanup
// worker's run-scoped delete does to the durable backends. Test-only, like
// the store itself.
func (s *MemoryAttemptStore) DeleteRunAttempts(runID string) {
	prefix, err := RunAttemptPrefix(runID)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			delete(s.objects, key)
		}
	}
}

// encodeAttemptForStore is the one place a record becomes a key and bytes, so
// no backend can drift into a different bound or a different key.
func encodeAttemptForStore(attempt *orkstackv1.TaskAttempt) (string, []byte, error) {
	if attempt == nil {
		return "", nil, fmt.Errorf("artifacts: put task attempt: %w: nil record", ErrInvalidAttemptKey)
	}
	key, err := AttemptKey(AttemptRef{
		RunID:   attempt.GetRunId(),
		TaskID:  attempt.GetTaskId(),
		Attempt: attempt.GetAttempt(),
	})
	if err != nil {
		return "", nil, err
	}
	document, err := MarshalAttempt(attempt)
	if err != nil {
		return "", nil, err
	}
	if len(document) > AttemptMaxBytes {
		return "", nil, fmt.Errorf(
			"artifacts: put task attempt %q: record is %d bytes, limit is %d: %w",
			key, len(document), AttemptMaxBytes, ErrInvalidAttemptKey)
	}
	return key, document, nil
}

// sortAttempts orders records ascending by attempt. The padded key already
// lists in that order on every backend, but the sort is kept so the contract
// holds for any backend whose listing ever reorders a page.
func sortAttempts(records []*orkstackv1.TaskAttempt) {
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].GetAttempt() < records[j].GetAttempt()
	})
}
