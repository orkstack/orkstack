package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// OrkflowStore is the engine's system of record for orkflow definitions.
//
// The split it enforces is the point of the design: a run's state lives in
// Temporal, an orkflow's definition lives in durable object storage. The
// engine keeps neither in process memory, so restarting `ork` loses nothing
// and two engines share one catalog.
//
// An orkflow is addressed by three coordinates: a name is unique within a
// namespace, never across the deployment. An empty namespace means the
// built-in default one, resolved by the store where a name becomes a key.
//
// A stored version is immutable, and one pointer per orkflow decides which
// version an empty version resolves to. Everything else about an orkflow is
// append-only: an edit is a new version, never a rewrite of an old one.
//
// The production implementation is artifacts.S3OrkflowStore, which writes one
// immutable JSON object per version at
// s3://{bucket}/metadata/orkflows/{namespace}/{name}/{version}.json and reads
// it back on every call. MemoryOrkflowStore below is the in-memory
// implementation and is for unit tests only: no code path ever falls back to
// it.
//
// A missing orkflow returns an error matching artifacts.ErrNotFound; every
// other error means the store could not be reached and must be reported as
// such, never answered from memory.
type OrkflowStore interface {
	// Put stores one version and reports whether it created it. Re-applying
	// an identical document writes nothing; a different document under a
	// stored version is artifacts.ErrVersionExists.
	Put(ctx context.Context, wf *orkstackv1.Orkflow) (*orkstackv1.Orkflow, bool, error)
	Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error)
	List(ctx context.Context, namespace string) ([]*orkstackv1.Orkflow, error)
	// Versions returns one orkflow's version catalog: every stored version,
	// the current pointer's target and the head.
	Versions(ctx context.Context, namespace, name string) (artifacts.OrkflowVersions, error)
	// SetCurrentVersion moves the current pointer to an existing version and
	// returns the version it replaced ("" when never set).
	SetCurrentVersion(ctx context.Context, namespace, name, version string) (string, error)
}

// MemoryOrkflowStore is the in-memory OrkflowStore used by unit tests that have no
// bucket. It matches the durable store's semantics: namespaces are disjoint,
// versions are immutable, and an empty version resolves through the current
// pointer before falling back to the most recently applied version.
type MemoryOrkflowStore struct {
	mu sync.RWMutex
	// byNamespace is namespace -> name -> version -> definition. The nesting
	// is the durable store's key shape, so a test that passes here cannot pass
	// for a reason the bucket would not reproduce.
	byNamespace map[string]map[string]map[string]*orkstackv1.Orkflow
	latest      map[string]map[string]string
	current     map[string]map[string]string
	nameList    map[string][]string
}

// compile-time contract check.
var _ OrkflowStore = (*MemoryOrkflowStore)(nil)

// NewMemoryOrkflowStore returns an empty in-memory orkflow store.
func NewMemoryOrkflowStore() *MemoryOrkflowStore {
	return &MemoryOrkflowStore{
		byNamespace: map[string]map[string]map[string]*orkstackv1.Orkflow{},
		latest:      map[string]map[string]string{},
		current:     map[string]map[string]string{},
		nameList:    map[string][]string{},
	}
}

// Put stores an orkflow version in the namespace the message names, and
// reports whether this call created it.
//
// A version is written once. Re-applying an identical document is a no-op that
// returns the stored one (created=false, original created_at intact); a
// different document under a stored version is refused with
// artifacts.ErrVersionExists, exactly as the bucket refuses it.
func (s *MemoryOrkflowStore) Put(
	_ context.Context,
	wf *orkstackv1.Orkflow,
) (*orkstackv1.Orkflow, bool, error) {
	if wf.GetName() == "" || wf.GetVersion() == "" {
		return nil, false, fmt.Errorf("engine: put orkflow %q version %q: %w",
			wf.GetName(), wf.GetVersion(), artifacts.ErrInvalidOrkflow)
	}
	namespace := artifacts.ResolveNamespace(wf.GetNamespace())
	stored := proto.Clone(wf).(*orkstackv1.Orkflow)
	stored.Namespace = namespace

	s.mu.Lock()
	defer s.mu.Unlock()
	byName, ok := s.byNamespace[namespace]
	if !ok {
		byName = map[string]map[string]*orkstackv1.Orkflow{}
		s.byNamespace[namespace] = byName
		s.latest[namespace] = map[string]string{}
		s.current[namespace] = map[string]string{}
	}
	versions, ok := byName[wf.GetName()]
	if !ok {
		versions = map[string]*orkstackv1.Orkflow{}
		byName[wf.GetName()] = versions
		s.nameList[namespace] = append(s.nameList[namespace], wf.GetName())
	}
	if existing, ok := versions[wf.GetVersion()]; ok {
		if sameStoredOrkflow(existing, stored) {
			return proto.Clone(existing).(*orkstackv1.Orkflow), false, nil
		}
		return nil, false, fmt.Errorf(
			"engine: orkflow %q version %q in namespace %q is already stored with different "+
				"content; a version is immutable, so publish a new version: %w",
			wf.GetName(), wf.GetVersion(), namespace, artifacts.ErrVersionExists)
	}
	versions[wf.GetVersion()] = stored
	s.latest[namespace][wf.GetName()] = wf.GetVersion()
	return proto.Clone(stored).(*orkstackv1.Orkflow), true, nil
}

// sameStoredOrkflow mirrors the durable store's rule for "this apply would
// store what is already stored": everything but created_at, which is stamped
// per apply and would otherwise make every repeat a rewrite.
func sameStoredOrkflow(stored, incoming *orkstackv1.Orkflow) bool {
	if stored.GetSourceYaml() != incoming.GetSourceYaml() {
		return false
	}
	a := proto.Clone(stored).(*orkstackv1.Orkflow)
	b := proto.Clone(incoming).(*orkstackv1.Orkflow)
	a.CreatedAt, b.CreatedAt = nil, nil
	return proto.Equal(a, b)
}

// Get returns the orkflow for name at version within one namespace; an empty
// version resolves through the current pointer, then to the most recently
// applied version. A missing orkflow returns artifacts.ErrNotFound.
func (s *MemoryOrkflowStore) Get(_ context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	namespace = artifacts.ResolveNamespace(namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.byNamespace[namespace][name]
	if !ok {
		return nil, notStored(namespace, name, version)
	}
	if version == "" {
		version = s.resolvedLocked(namespace, name)
	}
	wf, ok := versions[version]
	if !ok {
		return nil, notStored(namespace, name, version)
	}
	return proto.Clone(wf).(*orkstackv1.Orkflow), nil
}

// resolvedLocked is what an empty version means for one orkflow: the pointer's
// target when set, the most recently applied version otherwise.
func (s *MemoryOrkflowStore) resolvedLocked(namespace, name string) string {
	if current := s.current[namespace][name]; current != "" {
		return current
	}
	return s.latest[namespace][name]
}

// List returns the version every orkflow in one namespace resolves to, ordered
// by name.
func (s *MemoryOrkflowStore) List(_ context.Context, namespace string) ([]*orkstackv1.Orkflow, error) {
	namespace = artifacts.ResolveNamespace(namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := append([]string(nil), s.nameList[namespace]...)
	sort.Strings(names)
	out := make([]*orkstackv1.Orkflow, 0, len(names))
	for _, name := range names {
		if wf, ok := s.byNamespace[namespace][name][s.resolvedLocked(namespace, name)]; ok {
			out = append(out, proto.Clone(wf).(*orkstackv1.Orkflow))
		}
	}
	return out, nil
}

// Versions returns one orkflow's version catalog, newest applied first.
//
// A name nobody stored is an EMPTY catalog, not an error, because that is what
// a prefix scan of the bucket returns for it. Mapping "no versions" onto
// NotFound is the API's job (ListOrkflowVersions), and doing it in two places
// with two different rules is how the memory store and the bucket drift.
func (s *MemoryOrkflowStore) Versions(
	_ context.Context,
	namespace, name string,
) (artifacts.OrkflowVersions, error) {
	namespace = artifacts.ResolveNamespace(namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions := s.byNamespace[namespace][name]
	all := make([]*orkstackv1.Orkflow, 0, len(versions))
	for _, wf := range versions {
		all = append(all, proto.Clone(wf).(*orkstackv1.Orkflow))
	}
	sort.SliceStable(all, func(i, j int) bool {
		a, b := appliedAt(all[i]), appliedAt(all[j])
		if a.Equal(b) {
			return all[i].GetVersion() > all[j].GetVersion()
		}
		return a.After(b)
	})
	return artifacts.OrkflowVersions{
		All:     all,
		Current: s.current[namespace][name],
		Head:    s.latest[namespace][name],
	}, nil
}

func appliedAt(wf *orkstackv1.Orkflow) time.Time { return wf.GetCreatedAt().AsTime() }

// SetCurrentVersion points the orkflow at an existing version and returns the
// version the pointer named before. Promoting to a version that is not stored
// is artifacts.ErrNotFound — no reader may observe a pointer to a version that
// does not exist.
func (s *MemoryOrkflowStore) SetCurrentVersion(
	_ context.Context,
	namespace, name, version string,
) (string, error) {
	namespace = artifacts.ResolveNamespace(namespace)
	if version == "" {
		return "", fmt.Errorf("engine: promote orkflow %q in namespace %q: version is required: %w",
			name, namespace, artifacts.ErrInvalidOrkflow)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byNamespace[namespace][name][version]; !ok {
		return "", notStored(namespace, name, version)
	}
	previous := s.current[namespace][name]
	s.current[namespace][name] = version
	return previous, nil
}

func notStored(namespace, name, version string) error {
	return fmt.Errorf("engine: orkflow %q version %q in namespace %q: %w",
		name, version, namespace, artifacts.ErrNotFound)
}
