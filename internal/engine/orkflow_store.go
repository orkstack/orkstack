package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"

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
// The production implementation is artifacts.S3OrkflowStore, which writes one
// immutable JSON object per version at
// s3://{bucket}/metadata/orkflows/{name}/{version}.json and reads it back on
// every call. MemoryOrkflowStore below is the in-memory implementation and is for
// unit tests only: no code path ever falls back to it.
//
// A missing orkflow returns an error matching artifacts.ErrNotFound; every
// other error means the store could not be reached and must be reported as
// such, never answered from memory.
type OrkflowStore interface {
	Put(ctx context.Context, wf *orkstackv1.Orkflow) error
	Get(ctx context.Context, name, version string) (*orkstackv1.Orkflow, error)
	List(ctx context.Context) ([]*orkstackv1.Orkflow, error)
}

// MemoryOrkflowStore is the in-memory OrkflowStore used by unit tests that have no
// bucket. It matches the durable store's semantics: versions are independent,
// and an empty version means the most recently applied one.
type MemoryOrkflowStore struct {
	mu       sync.RWMutex
	byName   map[string]map[string]*orkstackv1.Orkflow
	latest   map[string]string
	nameList []string
}

// compile-time contract check.
var _ OrkflowStore = (*MemoryOrkflowStore)(nil)

// NewMemoryOrkflowStore returns an empty in-memory orkflow store.
func NewMemoryOrkflowStore() *MemoryOrkflowStore {
	return &MemoryOrkflowStore{
		byName: map[string]map[string]*orkstackv1.Orkflow{},
		latest: map[string]string{},
	}
}

// Put stores an orkflow version. The most recently applied version becomes
// "latest" for that name.
func (s *MemoryOrkflowStore) Put(_ context.Context, wf *orkstackv1.Orkflow) error {
	if wf.GetName() == "" || wf.GetVersion() == "" {
		return fmt.Errorf("engine: put orkflow %q version %q: %w",
			wf.GetName(), wf.GetVersion(), artifacts.ErrInvalidOrkflow)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	versions, ok := s.byName[wf.GetName()]
	if !ok {
		versions = map[string]*orkstackv1.Orkflow{}
		s.byName[wf.GetName()] = versions
		s.nameList = append(s.nameList, wf.GetName())
	}
	versions[wf.GetVersion()] = proto.Clone(wf).(*orkstackv1.Orkflow)
	s.latest[wf.GetName()] = wf.GetVersion()
	return nil
}

// Get returns the orkflow for name at version; an empty version means the most
// recently applied one. A missing orkflow returns artifacts.ErrNotFound.
func (s *MemoryOrkflowStore) Get(_ context.Context, name, version string) (*orkstackv1.Orkflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.byName[name]
	if !ok {
		return nil, notStored(name, version)
	}
	if version == "" {
		version = s.latest[name]
	}
	wf, ok := versions[version]
	if !ok {
		return nil, notStored(name, version)
	}
	return proto.Clone(wf).(*orkstackv1.Orkflow), nil
}

// List returns the latest version of every orkflow, ordered by name.
func (s *MemoryOrkflowStore) List(_ context.Context) ([]*orkstackv1.Orkflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := append([]string(nil), s.nameList...)
	sort.Strings(names)
	out := make([]*orkstackv1.Orkflow, 0, len(names))
	for _, name := range names {
		if wf, ok := s.byName[name][s.latest[name]]; ok {
			out = append(out, proto.Clone(wf).(*orkstackv1.Orkflow))
		}
	}
	return out, nil
}

func notStored(name, version string) error {
	return fmt.Errorf("engine: orkflow %q version %q: %w", name, version, artifacts.ErrNotFound)
}
