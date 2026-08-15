package engine

import (
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// RunStore is the in-process read model for runs. Temporal remains the source
// of truth; this store materializes Run/TaskRun snapshots for the API.
type RunStore struct {
	mu    sync.RWMutex
	runs  map[string]*orkstackv1.Run
	order []string
}

// NewRunStore returns an empty store.
func NewRunStore() *RunStore {
	return &RunStore{runs: map[string]*orkstackv1.Run{}}
}

// Save upserts a run snapshot wholesale.
func (s *RunStore) Save(run *orkstackv1.Run) {
	if run == nil || run.GetId() == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runs[run.GetId()]; !exists {
		s.order = append(s.order, run.GetId())
	}
	s.runs[run.GetId()] = proto.Clone(run).(*orkstackv1.Run)
}

// Apply merges a workflow-produced snapshot into the store. Fields the
// run workflow does not own (CreatedAt, and Input when the snapshot omits it) are
// preserved from the existing record.
func (s *RunStore) Apply(run *orkstackv1.Run) {
	if run == nil || run.GetId() == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := proto.Clone(run).(*orkstackv1.Run)
	if existing, ok := s.runs[run.GetId()]; ok {
		if next.GetCreatedAt() == nil {
			next.CreatedAt = existing.GetCreatedAt()
		}
		if next.GetInput() == nil {
			next.Input = existing.GetInput()
		}
	} else {
		s.order = append(s.order, run.GetId())
	}
	s.runs[run.GetId()] = next
}

// Get returns a clone of the stored run.
func (s *RunStore) Get(runID string) (*orkstackv1.Run, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[runID]
	if !ok {
		return nil, false
	}
	return proto.Clone(run).(*orkstackv1.Run), true
}

// ListFilter narrows List results. Zero values mean "no filter".
type ListFilter struct {
	WorkflowName string
	Status       orkstackv1.Status
}

// List returns clones of the stored runs, newest first.
func (s *RunStore) List(filter ListFilter) []*orkstackv1.Run {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*orkstackv1.Run, 0, len(s.order))
	for i := len(s.order) - 1; i >= 0; i-- {
		run := s.runs[s.order[i]]
		if run == nil {
			continue
		}
		if filter.WorkflowName != "" && run.GetWorkflowName() != filter.WorkflowName {
			continue
		}
		if filter.Status != orkstackv1.Status_STATUS_UNSPECIFIED && run.GetStatus() != filter.Status {
			continue
		}
		out = append(out, proto.Clone(run).(*orkstackv1.Run))
	}
	return out
}

// WorkflowStore keeps compiled orkflows by name and version.
type WorkflowStore struct {
	mu       sync.RWMutex
	byName   map[string]map[string]*orkstackv1.Orkflow
	latest   map[string]string
	nameList []string
}

// NewWorkflowStore returns an empty orkflow store.
func NewWorkflowStore() *WorkflowStore {
	return &WorkflowStore{
		byName: map[string]map[string]*orkstackv1.Orkflow{},
		latest: map[string]string{},
	}
}

// Put stores an orkflow. The most recently applied version becomes "latest"
// for that name.
func (s *WorkflowStore) Put(wf *orkstackv1.Orkflow) {
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
}

// Get returns the orkflow for name at version; an empty version means the
// most recently applied one.
func (s *WorkflowStore) Get(name, version string) (*orkstackv1.Orkflow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.byName[name]
	if !ok {
		return nil, false
	}
	if version == "" {
		version = s.latest[name]
	}
	wf, ok := versions[version]
	if !ok {
		return nil, false
	}
	return proto.Clone(wf).(*orkstackv1.Orkflow), true
}

// List returns the latest version of every orkflow, ordered by name.
func (s *WorkflowStore) List() []*orkstackv1.Orkflow {
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
	return out
}
