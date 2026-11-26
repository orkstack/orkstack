package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A resumed run's history holds what it published up to the point its reset
// went back to, then what it published since. What it published between the
// two, the ending the resume set aside included, is read from that ending's
// archive body, which ResumeRun makes sure of before it resets. The plan's
// scope is the sequence of the first event since, and the numbers from the
// ending set aside up to it are a jump, not a gap: no event was ever to be
// published under them (docs/STORAGE.md).

// EarlierEndings reads what a run published before it was resumed
// (RunArchiver).
type EarlierEndings interface {
	// EarlierEnding is the stream of the newest archived ending under a scope
	// below before, with its own gaps, and one up to before where an ending
	// after it is gone. None is artifacts.ErrNotFound, and one that does not
	// decode errEndingDamaged.
	EarlierEnding(ctx context.Context, runID string, before uint64) ([]*orkstackv1.Event, []EventGap, error)
	// EarlierResumes are the resumes the newest archived ending under a scope
	// below before lists, with EarlierEnding's errors.
	EarlierResumes(ctx context.Context, runID string, before uint64) ([]*orkstackv1.RunResume, error)
	// EarlierRun is the run as the ending set aside under before left it, with
	// EarlierEnding's errors: an older ending never stands for it.
	EarlierRun(ctx context.Context, runID string, before uint64) (*orkstackv1.Run, error)
	// ResumedRuns are the runs a resume marked: whether its reset ran is the
	// cluster's to say.
	ResumedRuns(ctx context.Context) (map[string]bool, error)
}

// earlierEndingLost is the reason of a gap whose events the earlier ending of
// a resumed run held.
const earlierEndingLost = "the archive of the run's earlier ending, which held them, is gone or damaged"

// errEndingDamaged is an earlier ending's body that does not decode: as lost as
// one that is gone, however often it is read.
var errEndingDamaged = errors.New("the archived ending does not decode")

// WithEndings reads the events a resume set aside from endings.
func (r *TemporalRunReader) WithEndings(endings EarlierEndings) *TemporalRunReader {
	r.endings = endings
	return r
}

// adoptReset folds the reset that resumed a run into the walk: it names the
// ending's scope, and an event read is handed what the reset set aside.
func (r *TemporalRunReader) adoptReset(
	ctx context.Context,
	runID string,
	historyEvent *historypb.HistoryEvent,
	fromSequence int64,
	wantRun bool,
	send func(*orkstackv1.Event) error,
	st *scanState,
) error {
	attrs := historyEvent.GetWorkflowTaskFailedEventAttributes()
	if attrs.GetCause() != enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW {
		return nil
	}
	key, ok := ParseResumePlanKey(attrs.GetFailure().GetMessage())
	if !ok {
		return nil
	}
	return r.adoptScope(ctx, runID, key.Scope, fromSequence, send != nil || !wantRun, send, st)
}

// adoptScope folds the resume under scope into the walk: an event read is
// handed what it set aside, before the first event it published.
func (r *TemporalRunReader) adoptScope(
	ctx context.Context, runID string, scope uint64, fromSequence int64, eventRead bool, send func(*orkstackv1.Event) error, st *scanState,
) error {
	if int64(scope) <= st.lastSequence {
		return nil
	}
	st.scope, st.filledTo, st.began = scope, 0, false
	before := int64(scope) - 1
	// A reader past the ending set aside needs nothing of it.
	if eventRead && fromSequence < before {
		if err := r.fillSetAside(ctx, runID, scope, fromSequence, send, st); err != nil {
			return err
		}
	}
	st.lastSequence = max(st.lastSequence, before)
	return nil
}

// fillSetAside serves the events between the point a reset went back to and
// the ending it set aside.
func (r *TemporalRunReader) fillSetAside(
	ctx context.Context, runID string, scope uint64, fromSequence int64, send func(*orkstackv1.Event) error, st *scanState,
) error {
	var (
		events []*orkstackv1.Event
		gaps   []EventGap
		err    = fmt.Errorf("%w: no run archive is configured", artifacts.ErrNotFound)
	)
	if r.endings != nil {
		events, gaps, err = r.endings.EarlierEnding(ctx, runID, scope)
	}
	switch {
	case endingLost(err):
		return st.setAsideLost(runID, st.lastSequence+1, int64(scope)-1)
	case err != nil:
		return fmt.Errorf("%w: run %s: the archive of its earlier ending could not be read: %v", ErrEventSegmentUnavailable, runID, err)
	}
	start, last := st.lastSequence, int64(scope)-1
	gaps = clipGaps(gaps, start+1, last)
	// lostBefore is an error of a live read that meets events this ending lost
	// below seq and has not passed them.
	lostBefore := func(seq int64) error {
		for _, gap := range gaps {
			if !st.tolerateLostSegments && !gap.Unpublished() && gap.First > st.lastSequence && gap.First < seq && gap.Last > fromSequence {
				return fmt.Errorf("%w: run %s: events %d–%d: %s", ErrEventSegmentLost, runID, gap.First, gap.Last, gap.Reason)
			}
		}
		return nil
	}
	if st.tolerateLostSegments {
		st.gaps = append(st.gaps, gaps...)
	}
	for _, event := range events {
		seq := event.GetSequence()
		if _, duplicate := st.seen[seq]; duplicate || seq <= start || seq >= int64(scope) {
			continue
		}
		st.seen[seq] = struct{}{}
		if err := lostBefore(seq); err != nil {
			return err
		}
		if !st.tolerateLostSegments {
			if err := st.noteSkipped(runID, seq-1, event.GetTimestamp(), fromSequence, false, send); err != nil {
				return err
			}
		} else if hole := (EventGap{First: st.lastSequence + 1, Last: seq - 1}); hole.First <= hole.Last && !gapsCover(gaps, hole) {
			// A body says what it is missing; one that does not has lost it.
			if err := st.setAsideLost(runID, hole.First, hole.Last); err != nil {
				return err
			}
		}
		st.lastSequence, st.lastEventAt = seq, event.GetTimestamp()
		// The ending set aside ends nothing: the run went on.
		switch {
		case seq <= fromSequence:
		case send != nil:
			if err := send(event); err != nil {
				return err
			}
		default:
			st.projection.events = append(st.projection.events, event)
		}
	}
	if err := lostBefore(int64(scope)); err != nil {
		return err
	}
	st.filledTo = st.lastSequence
	for _, gap := range gaps {
		st.filledTo = max(st.filledTo, gap.Last)
	}
	if st.tolerateLostSegments {
		slices.SortStableFunc(st.gaps, func(a, b EventGap) int { return cmp.Compare(a.First, b.First) })
	}
	return nil
}

// adoptSetAside readies the close of a resumed run that published nothing of
// its own: it ended as the ending its resume set aside left it, and its history
// holds no record past the point the reset went back to. Where that ending is
// gone the run still reads, with how it ended and no task, and its error says
// what is missing: the tasks of the reset point would be another state's.
func (r *TemporalRunReader) adoptSetAside(ctx context.Context, runID string, st *scanState) (adopted bool, err error) {
	if st.scope == 0 || st.began {
		return false, nil
	}
	st.projection.snapshotLoaded = true
	earlier, err := (*orkstackv1.Run)(nil), fmt.Errorf("%w: no run archive is configured", artifacts.ErrNotFound)
	if r.endings != nil {
		earlier, err = r.endings.EarlierRun(ctx, runID, st.scope)
	} else if st.forArchive {
		return false, fmt.Errorf("engine: archive run %s: its reader has no archive to read its earlier ending from", runID)
	}
	switch {
	case err == nil:
	case !endingLost(err):
		return false, fmt.Errorf("%w: run %s: the archive of its earlier ending could not be read: %v", ErrEventSegmentUnavailable, runID, err)
	default:
		slog.Warn("engine: the archive of a resumed run's earlier ending is gone or damaged; the run reads without its tasks",
			"run_id", runID, "resumed_under", st.scope, "error", err)
		st.recordLoss = recordSetAsideSentence
		return true, nil
	}
	earlier = proto.Clone(earlier).(*orkstackv1.Run)
	// The close says how it ended now; its rows are the earlier ending's.
	earlier.Status, earlier.Error, earlier.FinishedAt = orkstackv1.Status_STATUS_RUNNING, nil, nil
	earlier.CanceledBy, earlier.CanceledByVerified, earlier.ForceCancelRecord = "", false, ""
	st.projection.run = mergeRunSnapshot(st.projection.run, earlier)
	return true, nil
}

// clipGaps are the parts of gaps between first and last.
func clipGaps(gaps []EventGap, first, last int64) []EventGap {
	var out []EventGap
	for _, gap := range gaps {
		gap.First, gap.Last = max(gap.First, first), min(gap.Last, last)
		if gap.First <= gap.Last {
			out = append(out, gap)
		}
	}
	return out
}

// gapsCover reports whether gaps, in order, hold every number of hole.
func gapsCover(gaps []EventGap, hole EventGap) bool {
	next := hole.First
	for _, gap := range gaps {
		if gap.First <= next && gap.Last >= next {
			next = gap.Last + 1
		}
	}
	return next > hole.Last
}

// checkSetAside holds what was read of an earlier ending to what the resume
// says it set aside: a body of an older ending does not stand for it.
func (st *scanState) checkSetAside(event *orkstackv1.Event) error {
	after := event.GetResumed().GetResumedAfterSequence()
	if st.filledTo == 0 || uint64(event.GetSequence()) != st.scope || after <= st.filledTo {
		return nil
	}
	first := st.filledTo + 1
	st.filledTo = 0
	return st.setAsideLost(event.GetRunId(), first, after)
}

// setAsideLost says events of an earlier ending are lost: a gap of an
// archive, an error of a live read.
func (st *scanState) setAsideLost(runID string, first, last int64) error {
	if last < first {
		return nil
	}
	if !st.tolerateLostSegments {
		return fmt.Errorf("%w: run %s: events %d–%d: %s", ErrEventSegmentLost, runID, first, last, earlierEndingLost)
	}
	st.gaps = append(st.gaps, EventGap{First: first, Last: last, Reason: earlierEndingLost})
	return nil
}

// earlierBody is the archived ending that the resume under before set aside,
// as its mark names it, and whole. Where that ending is gone, or no mark names
// it, it is the newest archived before it, and not whole: no other ending is
// guessed to be the one.
func (a *RunArchiver) earlierBody(ctx context.Context, runID string, before uint64) (e earlierEnding, err error) {
	if a == nil {
		return e, fmt.Errorf("engine: earlier ending of run %q: %w", runID, artifacts.ErrNotFound)
	}
	store, ok := a.archive.(artifacts.RunEndingStore)
	if !ok {
		return e, fmt.Errorf("engine: earlier ending of run %q: the archive keeps one ending a run: %w", runID, artifacts.ErrNotFound)
	}
	setAside, err := store.SetAside(ctx, runID, before)
	named := err == nil
	switch {
	case named:
	case !errors.Is(err, artifacts.ErrNotFound):
		return e, err
	default:
		slog.Warn("engine: no mark says which earlier ending a resumed run set aside; it reads as lost",
			"run_id", runID, "resumed_under", before, "error", err)
	}
	endings, err := store.RunEndings(ctx, runID)
	if err != nil {
		return e, err
	}
	for _, s := range endings.Archived {
		if s < before && (!named || s <= setAside) {
			e.scope = s
		}
	}
	e.whole = named && e.scope == setAside
	e.body, err = store.GetEnding(ctx, runID, e.scope)
	return e, err
}

// earlierEnding is one archived ending of a run.
type earlierEnding struct {
	body  []byte
	scope uint64
	whole bool
}

// run is the run as the ending left it.
func (e earlierEnding) run(runID string) (*orkstackv1.Run, error) {
	run, err := UnmarshalRunLine(e.body)
	if err == nil && run.GetId() != runID {
		err = fmt.Errorf("it is run %q's", run.GetId())
	}
	if err != nil {
		return nil, fmt.Errorf("engine: ending %d of run %q: %w: %v", e.scope, runID, errEndingDamaged, err)
	}
	return run, nil
}

// EarlierEnding implements EarlierEndings over the archive.
func (a *RunArchiver) EarlierEnding(ctx context.Context, runID string, before uint64) ([]*orkstackv1.Event, []EventGap, error) {
	earlier, err := a.earlierBody(ctx, runID, before)
	if err != nil {
		return nil, nil, err
	}
	run, events, err := UnmarshalRunArchive(earlier.body)
	if err == nil && run.GetId() != runID {
		err = fmt.Errorf("it is run %q's", run.GetId())
	}
	if err != nil {
		return nil, nil, fmt.Errorf("engine: ending %d of run %q: %w: %v", earlier.scope, runID, errEndingDamaged, err)
	}
	gaps, err := UnmarshalRunArchiveGaps(earlier.body)
	if err != nil {
		return nil, nil, fmt.Errorf("engine: ending %d of run %q: %w: %v", earlier.scope, runID, errEndingDamaged, err)
	}
	if !earlier.whole {
		last := int64(0)
		for _, event := range events {
			last = max(last, event.GetSequence())
		}
		for _, gap := range gaps {
			last = max(last, gap.Last)
		}
		if last < int64(before)-1 {
			gaps = append(gaps, EventGap{First: last + 1, Last: int64(before) - 1, Reason: earlierEndingLost})
		}
	}
	return events, gaps, nil
}

// EarlierRun implements EarlierEndings over the archive.
func (a *RunArchiver) EarlierRun(ctx context.Context, runID string, before uint64) (*orkstackv1.Run, error) {
	earlier, err := a.earlierBody(ctx, runID, before)
	if err != nil {
		return nil, err
	}
	if !earlier.whole {
		return nil, fmt.Errorf("engine: the ending of run %q set aside under %d: %w", runID, before, artifacts.ErrNotFound)
	}
	return earlier.run(runID)
}

// ResumedRuns implements EarlierEndings over the archive.
func (a *RunArchiver) ResumedRuns(ctx context.Context) (map[string]bool, error) {
	if a == nil {
		return nil, nil
	}
	store, ok := a.archive.(artifacts.RunEndingStore)
	if !ok {
		return nil, errors.New("engine: the archive keeps one ending a run, and cannot say which were resumed")
	}
	out := map[string]bool{}
	err := store.EachResumedRun(ctx, func(e artifacts.RunEndings) error {
		out[e.RunID] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
