package telemetry

import (
	"context"
	"crypto/rand"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/runspan"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RunSpanName is the span of a whole run, made once when it ends.
const RunSpanName = "ork.run"

// Attribute keys only a run's span carries.
const (
	AttrOriginKind   = "ork.origin.kind"
	AttrOriginID     = "ork.origin.id"
	AttrParentRunID  = "ork.parent_run_id"
	AttrParentTaskID = "ork.parent_task_id"
	AttrRootRunID    = "ork.root_run_id"
	AttrRunStatus    = "ork.run.status"
)

// pinnedIDs is the SDK's random id generator, save for a span whose context
// pins its id: a run's span has the id its children's spans name as their
// parent, possibly before it exists and from another process.
type pinnedIDs struct{}

func newPinnedIDs() sdktrace.IDGenerator { return pinnedIDs{} }

type spanIDKey struct{}

// pinnedID is taken by the first Start that meets it, so a span started from
// a context derived from it never repeats the id.
type pinnedID struct {
	id    trace.SpanID
	taken atomic.Bool
}

// withSpanID pins id for the next span started from ctx.
func withSpanID(ctx context.Context, id trace.SpanID) context.Context {
	return context.WithValue(ctx, spanIDKey{}, &pinnedID{id: id})
}

func (pinnedIDs) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	var id trace.TraceID
	for !id.IsValid() {
		_, _ = rand.Read(id[:])
	}
	return id, newSpanID(ctx)
}

func (pinnedIDs) NewSpanID(ctx context.Context, _ trace.TraceID) trace.SpanID { return newSpanID(ctx) }

func newSpanID(ctx context.Context) trace.SpanID {
	if pinned, ok := ctx.Value(spanIDKey{}).(*pinnedID); ok && pinned.taken.CompareAndSwap(false, true) {
		return pinned.id
	}
	var id trace.SpanID
	for !id.IsValid() {
		_, _ = rand.Read(id[:])
	}
	return id
}

// recordRunSpan makes a run's span when it ends: under the span its
// submission was accepted in, or its parent's run span, on the run's own
// sink. A run that was not traced, or not sampled, makes none. It can be made
// twice (a redelivered publish on another replica, a resumed run): the id is
// the same, and the later one stands.
func (o *EngineObserver) recordRunSpan(ctx context.Context, event *orkstackv1.Event, run *orkstackv1.Run) {
	// Telemetry can never fail a run: a panic in an exporter loses this span.
	defer func() {
		if r := recover(); r != nil {
			o.logger.Warn("telemetry: a run's span could not be made", "run_id", run.GetId(), "panic", r)
		}
	}()
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
	default:
		return
	}
	if _, ok := runspan.Parse(run.GetTraceparent()); !ok {
		return
	}
	p, ok := o.sinkFor(run)
	if !ok {
		o.sinks.DropRecords(ctx, SignalTraces, DropReasonSinkUnknown, 1)
		return
	}
	// The run's own context, not the publish's: the span's parent is the one
	// the run was started in.
	parent := propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": run.GetTraceparent()})
	started, finished := run.GetStartedAt(), run.GetFinishedAt()
	if started == nil {
		started = run.GetCreatedAt()
	}
	// A run with no end, or one before its start, is not made to last until
	// this publish, which may be a much later one: it ends as it started.
	if finished == nil || finished.AsTime().Before(started.AsTime()) {
		finished = started
	}
	if started == nil {
		return
	}
	opts := []trace.SpanStartOption{trace.WithAttributes(runSpanAttrs(run)...), trace.WithTimestamp(started.AsTime())}
	_, span := p.Tracer.Tracer(ScopeName).Start(withSpanID(parent, trace.SpanID(runspan.RunSpanID(run.GetId()))), RunSpanName, opts...)
	if run.GetStatus() == orkstackv1.Status_STATUS_FAILED {
		span.SetStatus(codes.Error, run.GetError().GetCode())
	}
	span.End(trace.WithTimestamp(finished.AsTime()))
}

// runSpanAttrs says which run it was, what started it and how it ended: never
// who, nor the reference they gave, which a reader of the run reads there.
func runSpanAttrs(run *orkstackv1.Run) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(AttrRunID, run.GetId()),
		attribute.String(AttrNamespace, run.GetNamespace()),
		attribute.String(AttrOrkflowName, run.GetOrkflowName()),
		attribute.String(AttrOrkflowVersion, run.GetOrkflowVersion()),
		attribute.String(AttrOrkflowDigest, run.GetOrkflowDefinitionDigest()),
		attribute.String(AttrRunStatus, statusLabel(run.GetStatus())),
	}
	add := func(key, value string) {
		if value != "" {
			attrs = append(attrs, attribute.String(key, value))
		}
	}
	origin := run.GetOrigin()
	if slug, ok := artifacts.OriginKindSlug(origin.GetKind()); ok {
		add(AttrOriginKind, slug)
	}
	add(AttrOriginID, origin.GetId())
	if origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN {
		add(AttrParentRunID, origin.GetId())
		add(AttrParentTaskID, origin.GetParentTaskId())
		add(AttrRootRunID, origin.GetRootRunId())
	}
	add(AttrFailureReason, run.GetError().GetCode())
	return append(attrs, selectionAttrs(run.GetTelemetry().GetAttributes())...)
}
