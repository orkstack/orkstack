package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The local activities a switch writes its objects with and a bootstrap
// reads its saved state with. They run on the engine's run worker
// (LocalActivityWorkerOnly), never in workflow code: workflow code only
// schedules them and reads their recorded results. Every object is
// immutable and content-addressed (artifacts.RunContinuationKey), written
// create-if-absent, so a retried or re-decided write lands identical bytes
// on its own key and nothing written at a switch ever lands on a {seq}.pb
// snapshot key.

// ContinuationLocator names the object a bootstrap read reads. A local
// activity's input is never recorded, so widening it later is replay-safe.
type ContinuationLocator struct {
	URI string `json:"uri"`
}

// ContinuationWriteObservation is what one switch write measured. Outcome is
// set for the checkpoint only: written, write_failed or over_cap.
type ContinuationWriteObservation struct {
	Namespace   string
	Kind        string
	Outcome     string
	StateBytes  int64
	Seconds     float64
	HoldSeconds float64
}

// ContinuationRestoreObservation is what one bootstrap read measured.
// Attempt is the successful attempt's number; Final marks the read that
// completed the saved state (Outcome "restored", Seconds from the bootstrap's
// start). A failed bootstrap is observed once, with Outcome its RUN_STATE_*
// code and no attempt.
type ContinuationRestoreObservation struct {
	Namespace string
	Outcome   string
	Attempt   int64
	Final     bool
	Seconds   float64
}

// The outcomes of a switch's checkpoint write and of a bootstrap.
const (
	ContinuationOutcomeWritten     = "written"
	ContinuationOutcomeWriteFailed = "write_failed"
	ContinuationOutcomeOverCap     = "over_cap"
	ContinuationOutcomeRestored    = "restored"
)

// ContinuationObserver receives what a run's own local activities measured
// around a switch. Nil records nothing. Telemetry (internal/telemetry)
// implements it through cmd/ork's adapter: workflow code imports no
// OpenTelemetry (AGENTS.md §11), and nothing here does either. It must not
// block, and a dropped record never fails the activity.
type ContinuationObserver interface {
	ObserveContinuationWrite(ctx context.Context, o ContinuationWriteObservation)
	ObserveContinuationRestore(ctx context.Context, o ContinuationRestoreObservation)
}

// WithContinuationObserver sets the observer the continuation activities
// report to, and returns a for chaining.
func (a *SnapshotActivities) WithContinuationObserver(o ContinuationObserver) *SnapshotActivities {
	a.observer = o
	return a
}

func (a *SnapshotActivities) observeWrite(ctx context.Context, o ContinuationWriteObservation) {
	if a.observer != nil {
		a.observer.ObserveContinuationWrite(ctx, o)
	}
}

func (a *SnapshotActivities) observeRestore(ctx context.Context, o ContinuationRestoreObservation) {
	if a.observer != nil {
		a.observer.ObserveContinuationRestore(ctx, o)
	}
}

// beforeWrite and beforeRead run the test hooks of this instance, if any.
// A hook is never set in production.
func (a *SnapshotActivities) beforeWrite(ctx context.Context, kind string) error {
	if a.beforeContinuationWrite == nil {
		return nil
	}
	return a.beforeContinuationWrite(ctx, kind)
}

func (a *SnapshotActivities) beforeRead(ctx context.Context, offset int64) error {
	if a.beforeContinuationRead == nil {
		return nil
	}
	return a.beforeContinuationRead(ctx, offset)
}

// gzipBytes compresses data as one gzip member at level. Deterministic: the
// same input and level give the same bytes, which content addressing needs.
func gzipBytes(data []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeObject gzips plain and writes it create-if-absent under the run's
// continuation key of kind, returning its reference.
func (a *SnapshotActivities) writeObject(ctx context.Context, namespace, runID string, generation int64,
	kind string, plain []byte,
) (*orkstackv1.ContinuationObject, error) {
	data, err := gzipBytes(plain, gzip.DefaultCompression)
	if err != nil {
		return nil, fmt.Errorf("engine: compress the run's %s: %w", kind, err)
	}
	uri, err := a.store.WriteContinuation(ctx, namespace, runID, generation, kind, data)
	if err != nil {
		return nil, fmt.Errorf("engine: store the run's %s: %w", kind, err)
	}
	return &orkstackv1.ContinuationObject{
		Uri:       uri,
		Sha256:    artifacts.ContinuationSHA256(data),
		SizeBytes: int64(len(data)),
		RawBytes:  int64(len(plain)),
	}, nil
}

// WriteEventSegment stores one generation's event segment: exactly the
// events it published, in sequence order.
func (a *SnapshotActivities) WriteEventSegment(
	ctx context.Context,
	segment *orkstackv1.RunEventSegment,
	namespace string,
	obs ContinuationObservationInput,
) (*orkstackv1.ContinuationObject, error) {
	started := time.Now()
	if err := a.beforeWrite(ctx, artifacts.ContinuationKindEvents); err != nil {
		return nil, err
	}
	if segment == nil {
		return nil, errors.New("engine: write event segment: segment is required")
	}
	plain, err := proto.MarshalOptions{Deterministic: true}.Marshal(segment)
	if err != nil {
		return nil, fmt.Errorf("engine: encode the run's events: %w", err)
	}
	obj, err := a.writeObject(ctx, namespace, segment.GetRunId(), segment.GetGeneration(),
		artifacts.ContinuationKindEvents, plain)
	if err != nil {
		return nil, err
	}
	a.observeWrite(ctx, ContinuationWriteObservation{Namespace: namespace, Kind: artifacts.ContinuationKindEvents,
		StateBytes: obj.GetSizeBytes(), Seconds: time.Since(started).Seconds()})
	return obj, nil
}

// WriteSwitchSnapshot stores the full snapshot of the run's exact state at a
// switch, items included, gzipped under its content-addressed key.
func (a *SnapshotActivities) WriteSwitchSnapshot(
	ctx context.Context,
	run *orkstackv1.Run,
	namespace string,
	generation int64,
	obs ContinuationObservationInput,
) (*orkstackv1.ContinuationObject, error) {
	started := time.Now()
	if err := a.beforeWrite(ctx, artifacts.ContinuationKindSnapshot); err != nil {
		return nil, err
	}
	if run == nil {
		return nil, errors.New("engine: write switch snapshot: run is required")
	}
	plain, err := proto.MarshalOptions{Deterministic: true}.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("engine: encode the run's state: %w", err)
	}
	obj, err := a.writeObject(ctx, namespace, run.GetId(), generation, artifacts.ContinuationKindSnapshot, plain)
	if err != nil {
		return nil, err
	}
	a.observeWrite(ctx, ContinuationWriteObservation{Namespace: namespace, Kind: artifacts.ContinuationKindSnapshot,
		StateBytes: obj.GetSizeBytes(), Seconds: time.Since(started).Seconds()})
	return obj, nil
}

// checkpointParts splits run into RunCheckpointParts: part 0 holds the
// header (the Run with task_runs cleared), then the task rows with their
// items cleared, in definition order, then the item rows in (definition
// order, index) order. A part closes before a row that would take its raw
// size above checkpointPartRawBytes; a larger row goes alone.
func checkpointParts(run *orkstackv1.Run) []*orkstackv1.RunCheckpointPart {
	header := proto.Clone(run).(*orkstackv1.Run)
	header.TaskRuns = nil
	current := &orkstackv1.RunCheckpointPart{Header: header}
	size := proto.Size(current)
	parts := []*orkstackv1.RunCheckpointPart{current}
	add := func(row *orkstackv1.TaskRun, item bool) {
		rowSize := proto.Size(row) + 6 // tag and length prefix
		if size > 0 && size+rowSize > checkpointPartRawBytes {
			current = &orkstackv1.RunCheckpointPart{}
			parts = append(parts, current)
			size = 0
		}
		if item {
			current.Items = append(current.Items, row)
		} else {
			current.TaskRuns = append(current.TaskRuns, row)
		}
		size += rowSize
	}
	for _, row := range run.GetTaskRuns() {
		clone := proto.Clone(row).(*orkstackv1.TaskRun)
		clone.Items = nil
		add(clone, false)
	}
	for _, row := range run.GetTaskRuns() {
		for _, item := range row.GetItems() {
			add(item, true)
		}
	}
	return parts
}

// WriteCheckpoint stores the run's saved state at a switch: its rows as
// concatenated gzip members (BestCompression), each a RunCheckpointPart,
// with every part's and the whole object's SHA-256. A checkpoint above
// capBytes is not written: its reference comes back with its size and no
// URI, and the switch refuses it.
func (a *SnapshotActivities) WriteCheckpoint(
	ctx context.Context,
	run *orkstackv1.Run,
	generation int64,
	capBytes int64,
	obs ContinuationObservationInput,
) (*orkstackv1.ContinuationObject, error) {
	started := time.Now()
	failed := func(err error) (*orkstackv1.ContinuationObject, error) {
		a.observeWrite(ctx, ContinuationWriteObservation{Namespace: obs.Namespace, Kind: artifacts.ContinuationKindCheckpoint,
			Outcome: ContinuationOutcomeWriteFailed, Seconds: time.Since(started).Seconds()})
		return nil, err
	}
	if err := a.beforeWrite(ctx, artifacts.ContinuationKindCheckpoint); err != nil {
		return failed(err)
	}
	if run == nil {
		return failed(errors.New("engine: write checkpoint: run is required"))
	}
	obj := &orkstackv1.ContinuationObject{}
	var data []byte
	for i, part := range checkpointParts(run) {
		plain, err := proto.MarshalOptions{Deterministic: true}.Marshal(part)
		if err != nil {
			return failed(fmt.Errorf("engine: encode the run's saved state, part %d: %w", i, err))
		}
		member, err := gzipBytes(plain, gzip.BestCompression)
		if err != nil {
			return failed(fmt.Errorf("engine: compress the run's saved state, part %d: %w", i, err))
		}
		obj.Parts = append(obj.Parts, &orkstackv1.ContinuationPart{
			Offset:    int64(len(data)),
			SizeBytes: int64(len(member)),
			RawBytes:  int64(len(plain)),
			Sha256:    artifacts.ContinuationSHA256(member),
		})
		obj.RawBytes += int64(len(plain))
		data = append(data, member...)
	}
	obj.SizeBytes = int64(len(data))
	obj.Sha256 = artifacts.ContinuationSHA256(data)
	if obj.SizeBytes > capBytes {
		a.observeWrite(ctx, ContinuationWriteObservation{Namespace: obs.Namespace, Kind: artifacts.ContinuationKindCheckpoint,
			Outcome: ContinuationOutcomeOverCap, StateBytes: obj.SizeBytes, Seconds: time.Since(started).Seconds(),
			HoldSeconds: obs.HoldSeconds})
		return obj, nil
	}
	uri, err := a.store.WriteContinuation(ctx, run.GetNamespace(), run.GetId(), generation,
		artifacts.ContinuationKindCheckpoint, data)
	if err != nil {
		return failed(fmt.Errorf("engine: store the run's saved state: %w", err))
	}
	obj.Uri = uri
	a.observeWrite(ctx, ContinuationWriteObservation{Namespace: obs.Namespace, Kind: artifacts.ContinuationKindCheckpoint,
		Outcome: ContinuationOutcomeWritten, StateBytes: obj.SizeBytes, Seconds: time.Since(started).Seconds(),
		HoldSeconds: obs.HoldSeconds})
	return obj, nil
}

// ReadContinuationChunk reads length bytes at offset of a run's saved state
// (ReadContinuationRange). A size mismatch is a non-retryable
// RUN_STATE_CORRUPT: the object is not the one the carry names, and no retry
// makes it so. NotFound and every other error are retryable, so an operator
// can restore a lost object within the bootstrap's window. No error names
// the object (AGENTS.md §7).
func (a *SnapshotActivities) ReadContinuationChunk(
	ctx context.Context,
	loc ContinuationLocator,
	size, offset, length int64,
	obs ContinuationObservationInput,
) ([]byte, error) {
	if err := a.beforeRead(ctx, offset); err != nil {
		return nil, err
	}
	data, err := a.store.ReadContinuationRange(ctx, loc.URI, size, offset, length)
	if err != nil {
		err = withoutObjectName(err, loc.URI, subjectSavedState)
		if errors.Is(err, artifacts.ErrContinuationSize) {
			return nil, temporal.NewNonRetryableApplicationError(
				"the run's saved state is not the size it was saved at: "+err.Error(), RunStateCorruptCode, nil)
		}
		return nil, fmt.Errorf("read the run's saved state: %w", err)
	}
	if int64(len(data)) != length {
		return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf(
			"the run's saved state returned %d bytes at offset %d, want %d", len(data), offset, length),
			RunStateCorruptCode, nil)
	}
	attempt := int64(activity.GetInfo(ctx).Attempt)
	o := ContinuationRestoreObservation{Namespace: obs.Namespace, Attempt: attempt}
	if obs.Final {
		o.Final = true
		o.Outcome = ContinuationOutcomeRestored
		if !obs.RestoreStartedAt.IsZero() {
			o.Seconds = time.Since(obs.RestoreStartedAt).Seconds()
		}
	}
	a.observeRestore(ctx, o)
	return data, nil
}
