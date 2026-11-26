package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// SourceDocuments is what a source's workflow reads and writes of its
// document.
type SourceDocuments interface {
	Get(ctx context.Context, namespace, name string) (*orkstackv1.EventSource, error)
	Replace(ctx context.Context, src *orkstackv1.EventSource, etag string) (*orkstackv1.EventSource, error)
}

// SourceRecords is what is kept under a source's id.
type SourceRecords interface {
	Tombstoned(ctx context.Context, sourceID string) (bool, error)
	Tombstone(ctx context.Context, sourceID, namespace, name string, at time.Time) error
	KeepsSpecs(ctx context.Context, sourceID string) (bool, error)
	Purge(ctx context.Context, sourceID string, batch int) (left bool, err error)
}

// SourceWorker answers sources' workflows from the stores of the engine that
// runs their tasks.
type SourceWorker struct {
	Documents SourceDocuments
	Records   SourceRecords
	// Namespaces says whether a source's namespace is held, and its default
	// pool.
	Namespaces NamespaceReader
	// Check asks whether what a source runs is still allowed; nil answers
	// nothing, and nothing is decided from no answer.
	Check func(ctx context.Context, ask SourceCheckAsk) (SourceCheckAnswer, error)
	// DefaultQueue is the channel an unnamed pool resolves to.
	DefaultQueue string
	// Pin is what a workflow is pinned with today.
	Pin eventsource.Pinned
	// MaxEvents and MaxBytes are the history limits the deployment declares.
	MaxEvents, MaxBytes int64
	Now                 func() time.Time
	Logger              *slog.Logger
}

var _ SourceWork = (*SourceWorker)(nil)

// LoadEventSource reads what the document asks for. A document that is absent
// is not a deletion: only the mark a delete wrote in it, or the tombstone
// under the source's id, is.
func (w *SourceWorker) LoadEventSource(ctx context.Context, ask SourceLoadAsk) (eventsource.Load, error) {
	l, err := w.load(ctx, ask)
	if err != nil {
		w.Logger.WarnContext(ctx, "an event source could not be loaded", "namespace", ask.Namespace, "event_source", ask.Name, "event_source_id", ask.ID, "error", err)
		return eventsource.Load{}, fmt.Errorf("load event source %s: %w", ask.ID, err)
	}
	if l.Outcome == eventsource.LoadMissing && eventsource.SayMissing(w.Now(), ask.MissingSaid) {
		w.Logger.ErrorContext(ctx, "an event source runs and its document is not in the store: OrkStack stops nothing for it, and a person ends its workflow (docs/DEPLOY.md)",
			"reason", eventsources.ReasonDocumentMissing, "namespace", ask.Namespace, "event_source", ask.Name, "event_source_id", ask.ID)
	}
	l.MaxEvents, l.MaxBytes = w.MaxEvents, w.MaxBytes
	return l, nil
}

func (w *SourceWorker) load(ctx context.Context, ask SourceLoadAsk) (eventsource.Load, error) {
	src, err := w.Documents.Get(ctx, ask.Namespace, ask.Name)
	switch {
	case errors.Is(err, eventsources.ErrNotFound):
		src = nil
	case err != nil:
		return eventsource.Load{}, err
	case src.GetId() != ask.ID:
		// The name was given away: this id has no document.
		src = nil
	case src.GetDeletingSince() != nil:
		return eventsource.Load{Outcome: eventsource.LoadDeleting}, nil
	}
	// A read that failed is no "no tombstone": a deleted source's copy would
	// be run.
	switch gone, err := w.Records.Tombstoned(ctx, ask.ID); {
	case err != nil:
		return eventsource.Load{}, err
	case gone:
		return eventsource.Load{Outcome: eventsource.LoadDeleted}, nil
	}
	if src == nil {
		switch kept, err := w.Records.KeepsSpecs(ctx, ask.ID); {
		case err != nil:
			return eventsource.Load{}, err
		case !kept:
			return eventsource.Load{}, errors.New("the store holds nothing of it: it is not the store this source was written in")
		}
		return eventsource.Load{Outcome: eventsource.LoadMissing}, nil
	}
	if src.GetServiceDigest() == "" || src.GetServiceRevision() < 1 {
		return eventsource.Load{}, errors.New("its document names no service spec")
	}
	ns, err := w.Namespaces.Get(ctx, src.GetNamespace())
	if err != nil {
		return eventsource.Load{}, fmt.Errorf("its namespace: %w", err)
	}
	e := eventsources.Effective(src, eventsources.Limits{})
	queue, pool, err := sourcePool(e.GetPool(), ns, w.DefaultQueue)
	if err != nil {
		return eventsource.Load{}, fmt.Errorf("its pool: %w", err)
	}
	l := eventsource.Load{Outcome: eventsource.LoadFound, Paused: src.GetPause() != nil, Held: ns.GetHold() != nil, Pool: pool, Instances: e.GetInstances(),
		Target: eventsource.Spec{Revision: src.GetServiceRevision(), Digest: src.GetServiceDigest(), Queue: queue,
			CPU: src.GetResources().GetCpu(), Memory: src.GetResources().GetMemory()},
		ResumeSeq: src.GetResumeSeq(), TrialCode: src.GetTrialCode(), InPlace: src.GetInPlaceRevision() == src.GetServiceRevision()}
	if src.GetRestartRevision() > ask.Revision {
		pin := w.Pin
		l.Repin = &pin
	}
	return l, nil
}

// PurgeEventSource removes what a deleted source kept, once its tombstone is
// sure to be there.
func (w *SourceWorker) PurgeEventSource(ctx context.Context, ref SourceRef, batch int) (bool, error) {
	err := w.Records.Tombstone(ctx, ref.ID, ref.Namespace, ref.Name, w.Now())
	left := false
	if err == nil {
		left, err = w.Records.Purge(ctx, ref.ID, batch)
	}
	if err != nil {
		w.Logger.WarnContext(ctx, "what a deleted event source kept could not be removed; it is tried again", "event_source_id", ref.ID, "error", err)
		return false, fmt.Errorf("purge event source %s: %w", ref.ID, err)
	}
	return left, nil
}

// pauseTries is how often a pause is written over a document that changed
// meanwhile.
const pauseTries = 3

// PauseEventSource pauses the source, where it still is what the pause was
// decided of.
func (w *SourceWorker) PauseEventSource(ctx context.Context, ask SourcePauseAsk) (bool, error) {
	pause := eventsources.EnginePause(ask.Code, ask.Refusal, ask.Detail, "", w.Now())
	if pause == nil {
		return false, fmt.Errorf("%q is no code a source is paused with", ask.Code)
	}
	paused, err := w.pause(ctx, ask, pause)
	switch {
	case err != nil:
		w.Logger.WarnContext(ctx, "an event source could not be paused; it is tried again", "namespace", ask.Namespace, "event_source", ask.Name,
			"event_source_id", ask.ID, "code", ask.Code, "refusal_code", ask.Refusal, "error", err)
		return false, fmt.Errorf("pause event source %s: %w", ask.ID, err)
	case !paused:
		w.Logger.InfoContext(ctx, "an event source is another than its pause was decided of, and is not paused", "event_source_id", ask.ID,
			"code", ask.Code, "refusal_code", ask.Refusal, "service_revision", ask.Revision)
	}
	return paused, nil
}

func (w *SourceWorker) pause(ctx context.Context, ask SourcePauseAsk, pause *orkstackv1.EventSourcePause) (bool, error) {
	for range pauseTries {
		src, err := w.Documents.Get(ctx, ask.Namespace, ask.Name)
		switch {
		case errors.Is(err, eventsources.ErrNotFound):
			return false, nil
		case err != nil:
			return false, err
		case src.GetId() != ask.ID:
			return false, nil
		case src.GetPause() != nil || src.GetDeletingSince() != nil:
			return true, nil
		case src.GetServiceRevision() != ask.Revision, src.GetResumeSeq() != ask.ResumeSeq:
			return false, nil
		}
		next := proto.Clone(src).(*orkstackv1.EventSource)
		next.Pause, next.UpdatedAt, next.UpdatedBy = pause, timestamppb.New(w.Now().UTC()), ""
		switch _, err := w.Documents.Replace(ctx, next, src.GetEtag()); {
		case errors.Is(err, eventsources.ErrChanged):
			continue
		case err != nil:
			return false, err
		}
		w.Logger.WarnContext(ctx, "an event source was paused", "namespace", ask.Namespace, "event_source", ask.Name, "event_source_id", ask.ID,
			"code", ask.Code, "refusal_code", ask.Refusal)
		return true, nil
	}
	return false, fmt.Errorf("its document changed %d times while it was written", pauseTries)
}

// CheckEventSource asks whether what the source runs is still allowed.
func (w *SourceWorker) CheckEventSource(ctx context.Context, ask SourceCheckAsk) (SourceCheckAnswer, error) {
	if w.Check == nil {
		return SourceCheckAnswer{}, nil
	}
	return w.Check(ctx, ask)
}
