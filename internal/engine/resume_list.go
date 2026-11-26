package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Run.resumes is a read's and an archive's: a run's own state holds only the
// resume its execution is of, and each earlier ending's body lists those
// before it and how each ended.

// resumesOf are every resume run had, in order: what its earlier ending lists,
// then the one the ending under scope is of, which ended as run did. A run
// resumed under a plan that could not run has no entry for it. run is left as
// it is.
func resumesOf(earlier []*orkstackv1.RunResume, run *orkstackv1.Run, scope uint64) []*orkstackv1.RunResume {
	out := make([]*orkstackv1.RunResume, 0, len(earlier)+1)
	listed := map[uint64]bool{}
	for _, entry := range earlier {
		// A read's answer is its caller's to change; earlier is shared.
		out, listed[entry.GetScope()] = append(out, proto.Clone(entry).(*orkstackv1.RunResume)), true
	}
	for _, entry := range run.GetResumes() {
		if listed[entry.GetScope()] {
			continue
		}
		listed[entry.GetScope()] = true
		if entry.GetScope() == scope && isTerminal(run.GetStatus()) {
			entry = proto.Clone(entry).(*orkstackv1.RunResume)
			entry.Ended = &orkstackv1.RunResumeEnded{
				Status: run.GetStatus(), Reason: run.GetError().GetCode(), BeforeAnythingReran: !ranAgain(run.GetTaskRuns(), scope),
			}
		}
		out = append(out, entry)
	}
	return out
}

// ranAgain reports whether a row that the resume under scope ran again started.
func ranAgain(rows []*orkstackv1.TaskRun, scope uint64) bool {
	for _, row := range rows {
		if (uint64(row.GetResultEpoch()) == scope && row.GetStartedAt() != nil) || ranAgain(row.GetItems(), scope) {
			return true
		}
	}
	return false
}

// listResumes fills the resumes of run, read under scope. An earlier ending
// that is gone or damaged leaves what run's own state holds: its events are an
// explicit gap of the stream already, and the archive of this ending logs it.
func (r *TemporalRunReader) listResumes(ctx context.Context, run *orkstackv1.Run, scope uint64) error {
	if scope == 0 {
		return nil
	}
	var earlier []*orkstackv1.RunResume
	if r.endings != nil {
		var err error
		if earlier, err = r.endings.EarlierResumes(ctx, run.GetId(), scope); err != nil && !endingLost(err) {
			return fmt.Errorf("%w: run %s: the archive of its earlier ending could not be read: %v", ErrEventSegmentUnavailable, run.GetId(), err)
		}
	}
	run.Resumes = resumesOf(earlier, run, scope)
	return nil
}

// endingLost reports whether an earlier ending's body is gone for good, as
// opposed to a store that did not answer.
func endingLost(err error) bool {
	return errors.Is(err, artifacts.ErrNotFound) || errors.Is(err, errEndingDamaged)
}

// withResumes is run as the body of its ending under scope holds it: listing
// every resume up to it. run is left as it is. A body that is lost is never
// waited for, because the run would never be archived.
func (a *RunArchiver) withResumes(ctx context.Context, run *orkstackv1.Run, scope uint64) (*orkstackv1.Run, error) {
	earlier, err := a.EarlierResumes(ctx, run.GetId(), scope)
	switch {
	case err != nil && !endingLost(err):
		return nil, err
	case err != nil:
		slog.Error("engine: the archive of a resumed run's earlier ending is gone or damaged; the resumes it listed are not archived with this ending",
			"run_id", run.GetId(), "resumed_under", scope, "error", err)
	}
	resumes := resumesOf(earlier, run, scope)
	run = proto.Clone(run).(*orkstackv1.Run)
	run.Resumes = resumes
	return run, nil
}

// resumeLists caches what each earlier ending lists: at most maxResumeLists
// lists of at most MaxResumesPerRun entries. An ending's body is immutable and
// written before the resume that sets it aside, so an entry is never stale. A
// miss reads the store.
type resumeLists struct {
	mu    sync.Mutex
	lists map[resumeListKey][]*orkstackv1.RunResume
}

type resumeListKey struct {
	runID  string
	before uint64
}

const maxResumeLists = 256

func (c *resumeLists) get(key resumeListKey) ([]*orkstackv1.RunResume, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	list, ok := c.lists[key]
	return list, ok
}

func (c *resumeLists) put(key resumeListKey, list []*orkstackv1.RunResume) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lists == nil {
		c.lists = map[resumeListKey][]*orkstackv1.RunResume{}
	}
	for old := range maps.Keys(c.lists) {
		if len(c.lists) < maxResumeLists {
			break
		}
		delete(c.lists, old)
	}
	c.lists[key] = list
}

// EarlierResumes implements EarlierEndings over the archive. Its entries are
// shared: nothing that holds one changes it.
func (a *RunArchiver) EarlierResumes(ctx context.Context, runID string, before uint64) ([]*orkstackv1.RunResume, error) {
	key := resumeListKey{runID: runID, before: before}
	if a != nil {
		if list, ok := a.resumes.get(key); ok {
			return list, nil
		}
	}
	earlier, err := a.earlierBody(ctx, runID, before)
	if err != nil {
		return nil, err
	}
	run, err := earlier.run(runID)
	if err != nil {
		return nil, err
	}
	if !earlier.whole {
		return nil, fmt.Errorf("engine: the ending of run %q set aside under %d: %w", runID, before, artifacts.ErrNotFound)
	}
	a.resumes.put(key, run.GetResumes())
	return run.GetResumes(), nil
}

// RunEndings is what is archived of a resumed run's later endings; none where
// the archive keeps one ending a run.
func (a *RunArchiver) RunEndings(ctx context.Context, runID string) (artifacts.RunEndings, error) {
	if a == nil {
		return artifacts.RunEndings{RunID: runID}, nil
	}
	store, ok := a.archive.(artifacts.RunEndingStore)
	if !ok {
		return artifacts.RunEndings{RunID: runID}, nil
	}
	return store.RunEndings(ctx, runID)
}

// LoadEnding is the archive body of one ending of a run, verbatim.
func (a *RunArchiver) LoadEnding(ctx context.Context, runID string, scope uint64) ([]byte, error) {
	if a == nil {
		return nil, fmt.Errorf("engine: ending %d of run %q: %w", scope, runID, artifacts.ErrNotFound)
	}
	store, ok := a.archive.(artifacts.RunEndingStore)
	if !ok {
		return nil, fmt.Errorf("engine: ending %d of run %q: the archive keeps one ending a run: %w", scope, runID, artifacts.ErrNotFound)
	}
	return store.GetEnding(ctx, runID, scope)
}
