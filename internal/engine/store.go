package engine

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RunStore is the in-process read model for runs. Temporal remains the source
// of truth; this store materializes Run/TaskRun snapshots for the API.
type RunStore struct {
	mu        sync.RWMutex
	runs      map[string]*orkstackv1.Run
	order     []string
	retention terminalRetention
}

// NewRunStore returns an empty store that keeps every run it is given.
func NewRunStore() *RunStore {
	return &RunStore{runs: map[string]*orkstackv1.Run{}, retention: newTerminalRetention()}
}

// WithTerminalRetention makes the store forget a run d after it was first
// saved or applied with a terminal status, on a later write (terminalRetention).
// Zero keeps every run, which is right only where the store is the read model:
// a deployment with no RunReader, and unit tests. It returns the same store so
// it can be chained onto NewRunStore.
func (s *RunStore) WithTerminalRetention(d time.Duration) *RunStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retention.ttl = d
	return s
}

// trackLocked marks or unmarks run for retention by its status, then forgets
// every finished run that is due.
func (s *RunStore) trackLocked(run *orkstackv1.Run) {
	if !s.retention.enabled() {
		return
	}
	if isTerminal(run.GetStatus()) {
		s.retention.mark(run.GetId())
	} else {
		s.retention.unmark(run.GetId())
	}
	due := s.retention.due(nil)
	if len(due) == 0 {
		return
	}
	forget := make(map[string]bool, len(due))
	for _, id := range due {
		forget[id] = true
		delete(s.runs, id)
	}
	kept := s.order[:0]
	for _, id := range s.order {
		if !forget[id] {
			kept = append(kept, id)
		}
	}
	clear(s.order[len(kept):])
	s.order = kept
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
	s.trackLocked(run)
}

// Delete removes a run from the read model.
//
// It exists for one case: a run id the API allocated but never started,
// because Temporal reported the id already taken. Nothing outside the process
// has seen that run — SubmitRun has not returned — so leaving it behind would
// publish a run that never existed, stuck in PENDING forever.
func (s *RunStore) Delete(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runs[runID]; !exists {
		return
	}
	delete(s.runs, runID)
	s.retention.unmark(runID)
	for i, id := range s.order {
		if id == runID {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// Apply merges a workflow-produced snapshot into the store. Fields the
// run workflow does not own (CreatedAt, and Input when the snapshot omits it) are
// preserved from the existing record.
//
// So is the namespace, and for a reason the other two do not carry: List
// filters on it, so a snapshot that omits it does not blank a column — it
// moves the run into another tenant's listing. Same fold, same rule, as
// mergeRunSnapshot on the read path.
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
		if next.GetNamespace() == "" {
			next.Namespace = existing.GetNamespace()
		}
	} else {
		s.order = append(s.order, run.GetId())
	}
	s.runs[run.GetId()] = next
	s.trackLocked(next)
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
	Namespace   string
	OrkflowName string
	Status      orkstackv1.Status
	// OriginKind and OriginID keep runs whose origin matches, on the terms
	// RunQuery states; a run with no origin matches no origin filter.
	OriginKind orkstackv1.RunOriginKind
	OriginID   string
	// Keep is the residual filter set, evaluated on RunFilterView(run) as
	// RunQuery.Keep is; nil keeps every run the fields above keep.
	Keep func(*orkstackv1.Run) bool
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
		if filter.Namespace != "" && artifacts.ResolveNamespace(run.GetNamespace()) != artifacts.ResolveNamespace(filter.Namespace) {
			continue
		}
		if filter.OrkflowName != "" && run.GetOrkflowName() != filter.OrkflowName {
			continue
		}
		if filter.Status != orkstackv1.Status_STATUS_UNSPECIFIED && run.GetStatus() != filter.Status {
			continue
		}
		if filter.OriginKind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED && run.GetOrigin().GetKind() != filter.OriginKind {
			continue
		}
		if filter.OriginID != "" && run.GetOrigin().GetId() != filter.OriginID {
			continue
		}
		if filter.Keep != nil && !filter.Keep(RunFilterView(run)) {
			continue
		}
		out = append(out, proto.Clone(run).(*orkstackv1.Run))
	}
	return out
}
