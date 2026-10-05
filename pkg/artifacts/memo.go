package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A cached task's memo is ONE small pointer from a memo key to a SUCCEEDED
// commit another run already durably wrote (docs/DSL.md "Caching"):
//
//	metadata/memo/{ns}/orkflow/{orkflow}/{task-id}/{key}.json   orkflow scope
//	metadata/memo/{ns}/namespace/{task-id}/{key}.json           namespace scope
//
// It is advisory. Nothing reads it to decide execution state: an Orker reads
// it only to decide whether a container needs to run, and every doubt about it
// resolves to "run the task". Deleting any memo, or the whole metadata/memo/
// prefix, changes no result.
//
// The family sits under metadata/, beside metadata/commits and
// metadata/attempts, never under orkflows/ (S3) or objects/ (local), so a
// workload artifact can never collide with a memo. The per-run cleanup worker
// never touches it: a memo belongs to no run.
const (
	// MemoPrefix roots every memo in the store.
	MemoPrefix = "metadata/memo"
	// MemoContentType is what a memo is served as.
	MemoContentType = "application/json"
	// MemoSchemaVersion is the only CacheMemo schema this build writes.
	MemoSchemaVersion = 1
	// MemoMaxBytes bounds one serialized memo; a memo carries one source and
	// about a dozen component digests, far below it.
	MemoMaxBytes = 64 << 10

	// memoObjectSuffix is the leaf suffix of every memo key.
	memoObjectSuffix = ".json"
	// memoOrkflowSegment and memoNamespaceSegment are the two literal family
	// segments. Each follows a namespace slug, so neither can collide with an
	// Orkflow name or a task id.
	memoOrkflowSegment   = "orkflow"
	memoNamespaceSegment = "namespace"
)

// ErrInvalidMemoKey reports a memo coordinate that cannot be addressed.
var ErrInvalidMemoKey = errors.New("invalid memo key")

// ErrMemoUndecodable reports an object at a memo key that is not a CacheMemo.
var ErrMemoUndecodable = errors.New("memo is undecodable")

// memoKeyPattern is a memo key: the lowercase hex SHA-256 of a key document.
var memoKeyPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// MemoScopeRef addresses a group of memos. Namespace is required. Exactly one
// of Orkflow (orkflow scope) and NamespaceScoped selects the family; with
// neither, the ref is the whole namespace (listing only). TaskID narrows a
// family to one task.
type MemoScopeRef struct {
	Namespace       string
	Orkflow         string
	NamespaceScoped bool
	TaskID          string
}

// MemoRef addresses one memo: a family with its TaskID set, plus the key.
type MemoRef struct {
	MemoScopeRef
	Key string
}

// Scope is the CacheScope the ref's family names: ORKFLOW, NAMESPACE, or
// UNSPECIFIED for a whole-namespace ref.
func (r MemoScopeRef) Scope() orkstackv1.CacheScope {
	switch {
	case r.NamespaceScoped:
		return orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE
	case r.Orkflow != "":
		return orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW
	default:
		return orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED
	}
}

// validMemoSlug holds an Orkflow name or a task id to the slug rule every
// boundary applies (^[a-z][a-z0-9-]*$, at most 200 bytes). Validated, never
// sanitised: a key builder that trusted its caller would be the one place a
// traversal segment could enter the prefix.
func validMemoSlug(what, value string) error {
	switch {
	case value == "":
		return fmt.Errorf("artifacts: memo key: %s is empty: %w", what, ErrInvalidMemoKey)
	case len(value) > maxPoolName:
		return fmt.Errorf("artifacts: memo key: %s of %d bytes exceeds the %d-byte limit: %w",
			what, len(value), maxPoolName, ErrInvalidMemoKey)
	case !namespaceNamePattern.MatchString(value):
		return fmt.Errorf("artifacts: memo key: %s %q must match %s: %w",
			what, value, namespaceNamePattern, ErrInvalidMemoKey)
	}
	return nil
}

// MemoScopePrefix renders the "…/"-terminated prefix holding every memo a
// scope addresses: metadata/memo/{ns}/, …/orkflow/{orkflow}/[{task}/] or
// …/namespace/[{task}/].
func MemoScopePrefix(scope MemoScopeRef) (string, error) {
	namespace := ResolveNamespace(scope.Namespace)
	if err := validNamespaceName(namespace); err != nil {
		return "", fmt.Errorf("artifacts: memo key: namespace: %w", errors.Join(ErrInvalidMemoKey, err))
	}
	parts := []string{MemoPrefix, namespace}
	switch {
	case scope.Orkflow != "" && scope.NamespaceScoped:
		return "", fmt.Errorf("artifacts: memo key: an orkflow and namespace scope at once: %w", ErrInvalidMemoKey)
	case scope.Orkflow != "":
		if err := validMemoSlug("orkflow", scope.Orkflow); err != nil {
			return "", err
		}
		parts = append(parts, memoOrkflowSegment, scope.Orkflow)
	case scope.NamespaceScoped:
		parts = append(parts, memoNamespaceSegment)
	default:
		if scope.TaskID != "" {
			return "", fmt.Errorf("artifacts: memo key: a task id needs an orkflow or the namespace scope: %w",
				ErrInvalidMemoKey)
		}
		return path.Join(parts...) + "/", nil
	}
	if scope.TaskID != "" {
		if err := validMemoSlug("task id", scope.TaskID); err != nil {
			return "", err
		}
		parts = append(parts, scope.TaskID)
	}
	return path.Join(parts...) + "/", nil
}

// MemoKey renders the object key of one memo.
func MemoKey(ref MemoRef) (string, error) {
	if ref.Orkflow == "" && !ref.NamespaceScoped {
		return "", fmt.Errorf("artifacts: memo key: name an orkflow or the namespace scope: %w", ErrInvalidMemoKey)
	}
	if ref.TaskID == "" {
		return "", fmt.Errorf("artifacts: memo key: task id is empty: %w", ErrInvalidMemoKey)
	}
	if !memoKeyPattern.MatchString(ref.Key) {
		return "", fmt.Errorf("artifacts: memo key: %q is not 64 lowercase hex: %w", ref.Key, ErrInvalidMemoKey)
	}
	prefix, err := MemoScopePrefix(ref.MemoScopeRef)
	if err != nil {
		return "", err
	}
	return prefix + ref.Key + memoObjectSuffix, nil
}

// ParseMemoKey reverses MemoKey; a foreign key is ok=false, never an error, so
// a listing skips an object this package did not write.
func ParseMemoKey(key string) (MemoRef, bool) {
	rest, found := strings.CutPrefix(key, MemoPrefix+"/")
	if !found {
		return MemoRef{}, false
	}
	parts := strings.Split(rest, "/")
	var ref MemoRef
	switch {
	case len(parts) == 5 && parts[1] == memoOrkflowSegment:
		ref = MemoRef{MemoScopeRef: MemoScopeRef{Namespace: parts[0], Orkflow: parts[2], TaskID: parts[3]}}
	case len(parts) == 4 && parts[1] == memoNamespaceSegment:
		ref = MemoRef{MemoScopeRef: MemoScopeRef{Namespace: parts[0], NamespaceScoped: true, TaskID: parts[2]}}
	default:
		return MemoRef{}, false
	}
	leaf, cut := strings.CutSuffix(parts[len(parts)-1], memoObjectSuffix)
	if !cut {
		return MemoRef{}, false
	}
	ref.Key = leaf
	// Round-trip: only a key MemoKey would render for these coordinates is
	// ours. That one comparison refuses every malformed segment.
	rendered, err := MemoKey(ref)
	if err != nil || rendered != key {
		return MemoRef{}, false
	}
	return ref, true
}

// MemoRefFor derives a memo's own ref from its fields (scope, namespace,
// orkflow_name, task_id, key) — the one place a CacheMemo becomes a key.
func MemoRefFor(m *orkstackv1.CacheMemo) (MemoRef, error) {
	if m == nil {
		return MemoRef{}, fmt.Errorf("artifacts: memo key: nil memo: %w", ErrInvalidMemoKey)
	}
	ref := MemoRef{
		MemoScopeRef: MemoScopeRef{Namespace: ResolveNamespace(m.GetNamespace()), TaskID: m.GetTaskId()},
		Key:          m.GetKey(),
	}
	switch m.GetScope() {
	case orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW:
		ref.Orkflow = m.GetOrkflowName()
		if ref.Orkflow == "" {
			return MemoRef{}, fmt.Errorf("artifacts: memo key: an orkflow-scoped memo names no orkflow: %w",
				ErrInvalidMemoKey)
		}
	case orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE:
		ref.NamespaceScoped = true
	default:
		return MemoRef{}, fmt.Errorf("artifacts: memo key: scope %s is not orkflow or namespace: %w",
			m.GetScope(), ErrInvalidMemoKey)
	}
	if _, err := MemoKey(ref); err != nil {
		return MemoRef{}, err
	}
	return ref, nil
}

// MarshalMemo renders the exact bytes a memo is stored as: protojson then
// json.Indent, the commit's encoder (MarshalCommit). A document over
// MemoMaxBytes is refused.
func MarshalMemo(m *orkstackv1.CacheMemo) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("artifacts: encode memo: nil memo: %w", ErrInvalidMemoKey)
	}
	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode memo %q: %w", m.GetKey(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode memo %q: %w", m.GetKey(), err)
	}
	out.WriteByte('\n')
	if out.Len() > MemoMaxBytes {
		return nil, fmt.Errorf("artifacts: encode memo %q: memo is %d bytes, limit is %d: %w",
			m.GetKey(), out.Len(), MemoMaxBytes, ErrInvalidMemoKey)
	}
	return out.Bytes(), nil
}

// UnmarshalMemo decodes a stored memo, discarding unknown fields. Anything
// that does not decode matches ErrMemoUndecodable.
func UnmarshalMemo(document []byte) (*orkstackv1.CacheMemo, error) {
	m := &orkstackv1.CacheMemo{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, m); err != nil {
		return nil, fmt.Errorf("decode memo document: %w", errors.Join(ErrMemoUndecodable, err))
	}
	return m, nil
}

// MemoEntry is one listed memo, from the listing alone.
type MemoEntry struct {
	Ref          MemoRef
	LastModified time.Time
	Size         int64
}

// MemoStore reads and writes memos. It is its own interface because it has
// neither a commit's create-if-absent arbitration nor an attempt record's
// single writer: two runs may compute one key and both write it, and either
// pointer is correct.
//
// Implementations hold no cache and no per-run state.
type MemoStore interface {
	// GetMemo returns the memo at ref: ErrNotFound when absent,
	// ErrMemoUndecodable when present but not a CacheMemo.
	GetMemo(ctx context.Context, ref MemoRef) (*orkstackv1.CacheMemo, error)
	// PutMemo writes memo at MemoRefFor(memo) with a plain put.
	PutMemo(ctx context.Context, memo *orkstackv1.CacheMemo) error
	// ListMemos lists up to limit memos under scope whose object key sorts
	// after `after` (empty = from the start), in key order, and the cursor to
	// continue from ("" when done). Foreign objects are skipped.
	ListMemos(ctx context.Context, scope MemoScopeRef, after string, limit int) ([]MemoEntry, string, error)
	// DeleteMemo is idempotent: an absent memo is not an error.
	DeleteMemo(ctx context.Context, ref MemoRef) error
}

// encodeMemoForStore is the one place a memo becomes a key and bytes, so no
// backend can drift into a different key or bound.
func encodeMemoForStore(m *orkstackv1.CacheMemo) (string, []byte, error) {
	ref, err := MemoRefFor(m)
	if err != nil {
		return "", nil, err
	}
	key, err := MemoKey(ref)
	if err != nil {
		return "", nil, err
	}
	document, err := MarshalMemo(m)
	if err != nil {
		return "", nil, err
	}
	return key, document, nil
}

// decodeStoredMemo decodes bytes read at key; a body over MemoMaxBytes is
// undecodable rather than read further.
func decodeStoredMemo(key string, document []byte) (*orkstackv1.CacheMemo, error) {
	if len(document) > MemoMaxBytes {
		return nil, fmt.Errorf("artifacts: memo %q is %d bytes, limit is %d: %w",
			key, len(document), MemoMaxBytes, ErrMemoUndecodable)
	}
	m, err := UnmarshalMemo(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: memo %q: %w", key, err)
	}
	return m, nil
}

// pageMemoKeys applies ListMemos' contract to a sorted listing of keys under
// prefix: keys after `after`, foreign ones skipped, at most limit, and the
// cursor ("" when the listing is exhausted).
func pageMemoKeys(keys []string, after string, limit int, info func(key string) (time.Time, int64)) ([]MemoEntry, string) {
	if limit <= 0 {
		limit = 1000
	}
	entries := make([]MemoEntry, 0, min(limit, len(keys)))
	for i, key := range keys {
		if after != "" && key <= after {
			continue
		}
		ref, ok := ParseMemoKey(key)
		if !ok {
			continue
		}
		modified, size := info(key)
		entries = append(entries, MemoEntry{Ref: ref, LastModified: modified, Size: size})
		if len(entries) == limit {
			if i == len(keys)-1 {
				return entries, ""
			}
			return entries, key
		}
	}
	return entries, ""
}

// MemoStoreFactory builds a MemoStore from a Config.
type MemoStoreFactory func(cfg Config) (MemoStore, error)

var memoStoreRegistry = map[string]MemoStoreFactory{}

// RegisterMemoStore adds a memo-store factory under name.
func RegisterMemoStore(name string, f MemoStoreFactory) error {
	if name == "" {
		return fmt.Errorf("artifacts: register memo store: empty name")
	}
	if f == nil {
		return fmt.Errorf("artifacts: register memo store %q: nil factory", name)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := memoStoreRegistry[name]; exists {
		return fmt.Errorf("artifacts: register memo store %q: already registered", name)
	}
	memoStoreRegistry[name] = f
	return nil
}

// MemoStoreBackends lists the backends that can hold memos, sorted.
func MemoStoreBackends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(memoStoreRegistry))
	for name := range memoStoreRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// NewMemoStore builds the MemoStore named by cfg.Backend: one
// ORK_ARTIFACT_BACKEND selects where memos live, exactly as it does for
// commits and attempt records. There is no setting of its own.
func NewMemoStore(cfg Config) (MemoStore, error) {
	name := cfg.Backend
	if name == "" {
		name = DefaultBackend
		cfg.Backend = name
	}
	registryMu.RLock()
	f, ok := memoStoreRegistry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: memo store backend %q: %w (registered: %v)",
			name, ErrUnknownBackend, MemoStoreBackends())
	}
	store, err := f(cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: memo store backend %q: %w", name, err)
	}
	return store, nil
}

// NewMemoStoreFromEnv builds the MemoStore described by the ORK_* environment.
func NewMemoStoreFromEnv() (MemoStore, error) { return NewMemoStore(ConfigFromEnv()) }

func init() {
	mustRegisterMemoStore(BackendLocal, func(cfg Config) (MemoStore, error) {
		return NewLocalMemoStore(localDir(cfg))
	})
	mustRegisterMemoStore(BackendS3, func(cfg Config) (MemoStore, error) {
		return NewS3MemoStore(context.Background(), cfg)
	})
}

func mustRegisterMemoStore(name string, f MemoStoreFactory) {
	if err := RegisterMemoStore(name, f); err != nil {
		panic(err)
	}
}

// MemoryMemoStore is an in-memory MemoStore for tests. It keys and encodes
// memos exactly as the durable backends do — real semantics, not a stub. It is
// NOT durable and must never be selected by a deployment.
type MemoryMemoStore struct {
	mu      sync.Mutex
	objects map[string]memoryMemoObject
	// Now stamps LastModified on a put; nil means time.Now.
	Now func() time.Time
}

type memoryMemoObject struct {
	document []byte
	modified time.Time
}

var _ MemoStore = (*MemoryMemoStore)(nil)

// NewMemoryMemoStore returns an empty in-memory memo store.
func NewMemoryMemoStore() *MemoryMemoStore {
	return &MemoryMemoStore{objects: make(map[string]memoryMemoObject)}
}

func (s *MemoryMemoStore) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// GetMemo reads one memo.
func (s *MemoryMemoStore) GetMemo(ctx context.Context, ref MemoRef) (*orkstackv1.CacheMemo, error) {
	key, err := MemoKey(ref)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("artifacts: memory memo %q: %w", key, err)
	}
	s.mu.Lock()
	object, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: memory memo %q: %w", key, ErrNotFound)
	}
	return decodeStoredMemo(key, bytes.Clone(object.document))
}

// PutMemo writes one memo, last writer wins.
func (s *MemoryMemoStore) PutMemo(ctx context.Context, memo *orkstackv1.CacheMemo) error {
	key, document, err := encodeMemoForStore(memo)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("artifacts: memory memo %q: %w", key, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = memoryMemoObject{document: document, modified: s.now()}
	return nil
}

// ListMemos lists memos under scope in key order.
func (s *MemoryMemoStore) ListMemos(ctx context.Context, scope MemoScopeRef, after string, limit int) ([]MemoEntry, string, error) {
	prefix, err := MemoScopePrefix(scope)
	if err != nil {
		return nil, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	info := make(map[string]memoryMemoObject, len(s.objects))
	for key, object := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
			info[key] = object
		}
	}
	s.mu.Unlock()
	sort.Strings(keys)
	entries, cursor := pageMemoKeys(keys, after, limit, func(key string) (time.Time, int64) {
		return info[key].modified, int64(len(info[key].document))
	})
	return entries, cursor, nil
}

// DeleteMemo removes one memo; an absent memo is not an error.
func (s *MemoryMemoStore) DeleteMemo(ctx context.Context, ref MemoRef) error {
	key, err := MemoKey(ref)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

// PutRaw plants arbitrary bytes at ref, unconditionally — how a test
// reproduces a corrupt memo. Not part of the MemoStore contract.
func (s *MemoryMemoStore) PutRaw(ref MemoRef, document []byte) error {
	key, err := MemoKey(ref)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = memoryMemoObject{document: bytes.Clone(document), modified: s.now()}
	return nil
}

// PutForeign plants bytes at an arbitrary key under the store — a foreign
// object a listing must skip. Test-only.
func (s *MemoryMemoStore) PutForeign(key string, document []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = memoryMemoObject{document: bytes.Clone(document), modified: s.now()}
}

// Len is the number of stored objects, foreign ones included. Test-only.
func (s *MemoryMemoStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}
