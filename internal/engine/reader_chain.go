package engine

import (
	"context"
	"errors"
	"fmt"

	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A run that has continued as new is a chain of Temporal executions under one
// workflow id. The reader answers for the chain as one run: the current
// generation from Temporal, earlier generations' events from the immutable
// event segments the run wrote at each switch, and its state right after a
// switch from the checkpoint (or the switch snapshot) the carry names. Neither
// object is execution state: Temporal decided everything they hold.

// ErrEventSegmentUnavailable reports that the events of an earlier
// generation could not be read. It deliberately does NOT match
// artifacts.ErrNotFound: a missing segment of a live run is "unavailable",
// never "no such run" — the inversion scan rules out for snapshots.
var ErrEventSegmentUnavailable = errors.New("engine: an earlier part of the run's event stream is unavailable")

// ErrEventSegmentLost is ErrEventSegmentUnavailable for a failure retrying
// cannot heal: the segment object is gone, or it failed verification.
// errors.Is(lost, ErrEventSegmentUnavailable) holds.
var ErrEventSegmentLost = fmt.Errorf("%w: it was lost from object storage", ErrEventSegmentUnavailable)

// scanState is what one scan carries across the executions of a chain.
type scanState struct {
	projection   *runProjection
	seen         map[int64]struct{}
	lastSequence int64
	// stopped: a stream delivered the run's terminal event, so the walk ends.
	stopped bool
	// closed: the walk reached an execution's closing event (not a
	// continue-as-new), which loaded the run's snapshot already.
	closed bool
	// carried: the execution being read started from a carry. It decides how
	// its closing event is read (closedExecution).
	carried bool
	// tolerateLostSegments is set on the archive path only: a lost segment is
	// recorded as a gap rather than returned as an error.
	tolerateLostSegments bool
	gaps                 []EventGap
}

func newScanState(tolerateLostSegments bool) *scanState {
	return &scanState{
		projection:           &runProjection{},
		seen:                 map[int64]struct{}{},
		tolerateLostSegments: tolerateLostSegments,
	}
}

// deliver hands one decoded event to the caller — send, or the projection's
// events for an event read — once per sequence, and reports whether the walk
// stops here (a stream at the run's terminal event).
func (st *scanState) deliver(
	event *orkstackv1.Event,
	fromSequence int64,
	wantRun bool,
	send func(*orkstackv1.Event) error,
) (bool, error) {
	if _, duplicate := st.seen[event.GetSequence()]; duplicate {
		return false, nil
	}
	st.seen[event.GetSequence()] = struct{}{}
	if event.GetSequence() > st.lastSequence {
		st.lastSequence = event.GetSequence()
	}
	if event.GetSequence() <= fromSequence {
		if send != nil && isTerminalRunEvent(event) {
			st.stopped = true
			return true, nil
		}
		return false, nil
	}
	if send != nil {
		if err := send(event); err != nil {
			return false, err
		}
		if isTerminalRunEvent(event) {
			st.stopped = true
			return true, nil
		}
		return false, nil
	}
	if !wantRun {
		// A run read never looks at the events, so it never keeps them:
		// GetRun on a 24k-event history allocates none.
		st.projection.events = append(st.projection.events, event)
	}
	return false, nil
}

// decodeCarry decodes a started event's fifth argument: the carry of a
// generation after the first, nil when absent (generation 0, and every start
// made before carries existed).
func (r *TemporalRunReader) decodeCarry(attrs *historypb.WorkflowExecutionStartedEventAttributes) (*orkstackv1.RunContinuation, error) {
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) < 5 {
		return nil, nil
	}
	var carry *orkstackv1.RunContinuation
	if err := r.converter.FromPayload(payloads[4], &carry); err != nil {
		return nil, fmt.Errorf("decode the saved-state reference of its start: %w", err)
	}
	return carry, nil
}

// adoptCarry folds a carried start into the scan: it checks every reference
// the carry names before anything is read, replays the earlier generations'
// events for an event read, and points the snapshot load at the checkpoint
// (falling back to the switch snapshot) for a run read.
func (r *TemporalRunReader) adoptCarry(
	ctx context.Context,
	runID string,
	attrs *historypb.WorkflowExecutionStartedEventAttributes,
	carry *orkstackv1.RunContinuation,
	fromSequence int64,
	wantRun bool,
	send func(*orkstackv1.Event) error,
	st *scanState,
) error {
	eventRead := send != nil || !wantRun
	if err := validateCarry(runID, r.startedNamespace(attrs), carry); err != nil {
		if !eventRead {
			return fmt.Errorf("engine: run %s: its saved state cannot be read: %v", runID, err)
		}
		if !st.tolerateLostSegments {
			return fmt.Errorf("%w: run %s: its saved-state references failed verification: %v",
				ErrEventSegmentLost, runID, err)
		}
		// Archive path: every earlier event is unreadable, recorded as one
		// explicit gap rather than a run that is never archived.
		if last := carry.GetEventSequence(); last > st.lastSequence {
			st.gaps = append(st.gaps, EventGap{First: st.lastSequence + 1, Last: last,
				Reason: "the run's saved-state references failed verification: " + err.Error()})
			st.lastSequence = last
		}
	} else if eventRead {
		if err := r.replaySegments(ctx, runID, carry, fromSequence, send, st); err != nil {
			return err
		}
		if st.stopped {
			return nil
		}
	}
	st.lastSequence = max(st.lastSequence, carry.GetEventSequence())
	projection := st.projection
	projection.checkpoint = carry.GetCheckpoint()
	projection.switchSnapshot = carry.GetSnapshot()
	projection.snapshotURI = carry.GetCheckpoint().GetUri()
	projection.priorSnapshotURI = carry.GetSnapshot().GetUri()
	projection.snapshotLoaded = false
	return nil
}

// startedNamespace is the OrkStack namespace a start's planned definition
// names ("" when it does not decode, which resolves to default).
func (r *TemporalRunReader) startedNamespace(attrs *historypb.WorkflowExecutionStartedEventAttributes) string {
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) < 2 {
		return ""
	}
	var wf *orkstackv1.Orkflow
	if err := r.converter.FromPayload(payloads[1], &wf); err != nil {
		return ""
	}
	return wf.GetNamespace()
}

// validateCarry applies the carry's consistency rules — the ones a
// generation's own start checks (spec 2.8 step 2) that need no plan — before
// anything is read: a contiguous chain of segments from sequence 1 to the
// carried sequence, a checkpoint whose parts tile it, and every object's URI
// exactly this run's own key for its generation, kind and digest. A carry
// naming another run's object is refused here, and nothing is read from it.
func validateCarry(runID, namespace string, carry *orkstackv1.RunContinuation) error {
	generation := carry.GetGeneration()
	segments := carry.GetSegments()
	if generation < 1 {
		return fmt.Errorf("generation %d is not after the first", generation)
	}
	if int64(len(segments)) != generation {
		return fmt.Errorf("generation %d names %d event segments", generation, len(segments))
	}
	next := int64(1)
	for i, seg := range segments {
		if seg.GetGeneration() != int64(i) {
			return fmt.Errorf("event segment %d names generation %d", i, seg.GetGeneration())
		}
		if seg.GetFirstSequence() != next {
			return fmt.Errorf("event segment %d starts at sequence %d, want %d", i, seg.GetFirstSequence(), next)
		}
		if seg.GetLastSequence() < seg.GetFirstSequence()-1 ||
			seg.GetEventCount() < 0 || seg.GetEventCount() > seg.GetLastSequence()-seg.GetFirstSequence()+1 {
			return fmt.Errorf("event segment %d holds %d events in [%d, %d]", i,
				seg.GetEventCount(), seg.GetFirstSequence(), seg.GetLastSequence())
		}
		obj := seg.GetObject()
		if obj == nil || !artifacts.ContinuationURIMatches(obj.GetUri(), namespace, runID, int64(i),
			artifacts.ContinuationKindEvents, obj.GetSha256()) {
			return fmt.Errorf("event segment %d is not this run's own object", i)
		}
		next = seg.GetLastSequence() + 1
	}
	if carry.GetEventSequence() != next-1 {
		return fmt.Errorf("the carried sequence %d is not the last segment's last sequence %d",
			carry.GetEventSequence(), next-1)
	}
	checkpoint := carry.GetCheckpoint()
	if checkpoint == nil || len(checkpoint.GetParts()) == 0 {
		return fmt.Errorf("it names no checkpoint parts")
	}
	var offset int64
	for i, part := range checkpoint.GetParts() {
		if part.GetOffset() != offset || part.GetSizeBytes() <= 0 {
			return fmt.Errorf("checkpoint part %d is not contiguous from offset 0", i)
		}
		offset += part.GetSizeBytes()
	}
	if offset != checkpoint.GetSizeBytes() {
		return fmt.Errorf("checkpoint parts cover %d of %d bytes", offset, checkpoint.GetSizeBytes())
	}
	if !artifacts.ContinuationURIMatches(checkpoint.GetUri(), namespace, runID, generation-1,
		artifacts.ContinuationKindCheckpoint, checkpoint.GetSha256()) {
		return fmt.Errorf("the checkpoint is not this run's own object")
	}
	if snapshot := carry.GetSnapshot(); snapshot != nil &&
		!artifacts.ContinuationURIMatches(snapshot.GetUri(), namespace, runID, generation-1,
			artifacts.ContinuationKindSnapshot, snapshot.GetSha256()) {
		return fmt.Errorf("the switch snapshot is not this run's own object")
	}
	return nil
}

// replaySegments hands an event read every event of the earlier generations
// after fromSequence, oldest first, each segment verified before any of its
// events is used. A failed read or check is an error wrapping
// ErrEventSegmentUnavailable (ErrEventSegmentLost when permanent), with the
// store's error formatted %v, never %w; on the archive path a lost segment is
// an EventGap instead.
func (r *TemporalRunReader) replaySegments(
	ctx context.Context,
	runID string,
	carry *orkstackv1.RunContinuation,
	fromSequence int64,
	send func(*orkstackv1.Event) error,
	st *scanState,
) error {
	for _, seg := range carry.GetSegments() {
		if seg.GetEventCount() == 0 || seg.GetLastSequence() <= max(fromSequence, st.lastSequence) {
			continue
		}
		events, reason, err := r.readSegment(ctx, runID, seg)
		if err != nil {
			if st.tolerateLostSegments && errors.Is(err, ErrEventSegmentLost) {
				st.gaps = append(st.gaps, EventGap{First: seg.GetFirstSequence(), Last: seg.GetLastSequence(), Reason: reason})
				st.lastSequence = max(st.lastSequence, seg.GetLastSequence())
				continue
			}
			return err
		}
		for _, event := range events {
			stop, err := st.deliver(event, fromSequence, false, send)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		st.lastSequence = max(st.lastSequence, seg.GetLastSequence())
	}
	return nil
}

// readSegment reads and verifies one event segment. reason is the short
// sentence a gap records when the segment is lost.
func (r *TemporalRunReader) readSegment(
	ctx context.Context,
	runID string,
	seg *orkstackv1.EventSegmentRef,
) ([]*orkstackv1.Event, string, error) {
	span := fmt.Sprintf("events %d–%d of run %s", seg.GetFirstSequence(), seg.GetLastSequence(), runID)
	if r.snapshots == nil {
		return nil, "", fmt.Errorf("%w: %s: snapshot store is not configured", ErrEventSegmentUnavailable, span)
	}
	data, err := r.snapshots.ReadSnapshot(ctx, seg.GetObject().GetUri())
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return nil, "segment object missing", fmt.Errorf("%w: %s: segment object missing: %v", ErrEventSegmentLost, span, err)
		}
		return nil, "", fmt.Errorf("%w: %s: %v", ErrEventSegmentUnavailable, span, err)
	}
	lost := func(what string) ([]*orkstackv1.Event, string, error) {
		reason := "segment failed verification (" + what + ")"
		return nil, reason, fmt.Errorf("%w: %s: %s", ErrEventSegmentLost, span, reason)
	}
	segment, err := DecodeEventSegment(data, seg.GetObject())
	if err != nil {
		return lost(err.Error())
	}
	if segment.GetRunId() != runID || segment.GetGeneration() != seg.GetGeneration() ||
		segment.GetFirstSequence() != seg.GetFirstSequence() || segment.GetLastSequence() != seg.GetLastSequence() {
		return lost("its run, generation or sequence range is not the one its reference names")
	}
	if int64(len(segment.GetEvents())) != seg.GetEventCount() {
		return lost(fmt.Sprintf("it holds %d events, its reference records %d", len(segment.GetEvents()), seg.GetEventCount()))
	}
	previous := seg.GetFirstSequence() - 1
	for _, event := range segment.GetEvents() {
		sequence := event.GetSequence()
		if sequence <= previous || sequence > seg.GetLastSequence() || event.GetRunId() != runID {
			return lost(fmt.Sprintf("event %d is out of order or outside its range", sequence))
		}
		if isTerminalRunEvent(event) {
			return lost(fmt.Sprintf("event %d ends the run", sequence))
		}
		previous = sequence
	}
	return segment.GetEvents(), "", nil
}
