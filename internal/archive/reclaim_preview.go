package archive

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	// PreviewMaxRuns bounds how many runs of the index one preview covers.
	PreviewMaxRuns = 1000
	// PreviewMaxSamples bounds how many objects one preview names.
	PreviewMaxSamples = 100
	// previewBudget bounds one preview's time.
	previewBudget = 30 * time.Second
)

// ErrPreviewBusy is a preview asked of a replica already answering one.
var ErrPreviewBusy = errors.New("artifact gc: a preview is already running on this replica")

// PreviewRequest is one preview: at most Limit runs of the index after
// Cursor, of Namespace when it is set, naming at most Samples objects (only
// with a namespace).
type PreviewRequest struct {
	Namespace string
	Limit     int
	Samples   int
	Cursor    string
}

// PreviewResult is what a pass over the same runs would do, now.
type PreviewResult struct {
	Mode ReclaimMode
	// Held lists the holds of this replica's last pass (or of now, with no
	// pass): a pass held deletes nothing.
	Held     []string
	Objects  map[TallyKey]Tally
	Samples  []Sample
	LowWater LowWater
	// RunsExamined counts the runs of the index the preview covered;
	// WaitingRuns those of them that wait on a claimant or the mark.
	RunsExamined int
	WaitingRuns  int
	NextCursor   string
	ComputedAt   time.Time
	// StoreVersioned says a delete there only adds a delete marker.
	StoreVersioned bool
}

// versionedStore is a store that can say whether it keeps versions.
type versionedStore interface {
	StoreVersioned(ctx context.Context) (bool, error)
}

// Preview answers what a pass would do with at most PreviewMaxRuns runs,
// within previewBudget, and writes nothing: it reads the store's clock from
// this replica's last pass rather than writing the probe, and its low-water
// mark is the last pass's, or one computed now that no pass will use.
func (r *Reclaimer) Preview(ctx context.Context, req PreviewRequest) (PreviewResult, error) {
	if !r.previewing.TryLock() {
		return PreviewResult{}, ErrPreviewBusy
	}
	defer r.previewing.Unlock()
	ctx, cancel := context.WithTimeout(ctx, previewBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()

	limit := req.Limit
	if limit <= 0 || limit > PreviewMaxRuns {
		limit = PreviewMaxRuns
	}
	var samples *sampler
	if req.Namespace != "" && req.Samples > 0 {
		samples = &sampler{limit: min(req.Samples, PreviewMaxSamples)}
	}
	now := r.now()
	result := PreviewResult{Mode: r.config.Mode, Objects: map[TallyKey]Tally{}, ComputedAt: now}
	if r.store.Local() {
		result.Held = []string{HoldLocalBackend}
		return result, nil
	}
	if v, ok := r.store.(versionedStore); ok {
		versioned, err := v.StoreVersioned(ctx)
		if err != nil {
			r.logger.Warn("artifact gc preview could not read the bucket's versioning", "error", err)
		}
		result.StoreVersioned = versioned
	}
	last, hasLast := r.LastReport()
	storeNow := now
	switch {
	case hasLast && !last.StoreNow.IsZero():
		storeNow = last.StoreNow.Add(now.Sub(last.StoreNowAt))
		result.Held = append(result.Held, last.Held...)
		result.LowWater = last.LowWater
	default:
		result.Held = r.holds(ctx, 0)
		result.LowWater = r.lowWater(ctx, false)
		if ctx.Err() != nil && !result.LowWater.Known {
			result.LowWater = LowWater{Cause: LowWaterNotComputed, Computed: now}
		}
		if !result.LowWater.Known && !slices.Contains(result.Held, HoldLowWaterUnknown) {
			result.Held = append(result.Held, HoldLowWaterUnknown)
		}
	}
	retention, err := r.retentionFor(ctx)
	if err != nil {
		return PreviewResult{}, err
	}
	pass := &passState{
		storeNow: storeNow, lw: result.LowWater, retention: retention,
		verified: map[string]retirement{}, lists: map[string]listRead{}, tallies: result.Objects,
		deadline: deadline, samples: samples,
	}
	cursor := req.Cursor
	until := storeNow.Add(-r.config.Grace)
	for {
		entries, err := r.store.ListRuns(ctx, artifacts.RunQuery{Until: until, Limit: limit, KeyOrder: true, StartAfterKey: cursor})
		if err != nil {
			return PreviewResult{}, fmt.Errorf("artifact gc preview: list runs: %w", err)
		}
		for _, entry := range entries {
			if result.RunsExamined == limit || ctx.Err() != nil {
				result.NextCursor = cursor
				result.Samples = samples.taken()
				return result, nil
			}
			key, keyErr := artifacts.RunIndexKeyForEntry(entry)
			if keyErr != nil || key <= cursor {
				return PreviewResult{}, fmt.Errorf("artifact gc preview: the run index cursor did not advance at run %q", entry.RunID)
			}
			cursor = key
			if req.Namespace != "" && artifacts.ResolveNamespace(entry.Namespace) != artifacts.ResolveNamespace(req.Namespace) {
				continue
			}
			result.RunsExamined++
			if _, outcome := r.examineRun(ctx, pass, entry.RunID); outcome == runWaiting {
				result.WaitingRuns++
			}
		}
		if len(entries) < limit {
			result.Samples = samples.taken()
			return result, nil
		}
	}
}

func (s *sampler) taken() []Sample {
	if s == nil {
		return nil
	}
	return s.out
}

// AcceptedRunTracker remembers this replica's newest accepted run, for the
// index_stale canary. Process-local, as a cordon is; the zero value is ready.
type AcceptedRunTracker struct {
	mu sync.Mutex
	id string
	at time.Time
}

// NoteAccepted records a run this replica started.
func (t *AcceptedRunTracker) NoteAccepted(runID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if at.After(t.at) {
		t.id, t.at = runID, at
	}
}

// NewestAccepted is the newest run noted, if any.
func (t *AcceptedRunTracker) NewestAccepted() (string, time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.id, t.at, t.id != ""
}
