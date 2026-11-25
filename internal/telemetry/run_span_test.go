package telemetry

import (
	"bytes"
	"context"
	"encoding/hex"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/runspan"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// tracingProviders are Providers that trace into an exporter, with the id
// generator every tracing sink has.
func tracingProviders(t *testing.T, opts ...sdktrace.TracerProviderOption) (*Providers, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(append([]sdktrace.TracerProviderOption{
		sdktrace.WithSyncer(exporter), sdktrace.WithIDGenerator(newPinnedIDs())}, opts...)...)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	p := noopProviders(Config{TracesExporter: ExporterOTLP})
	p.Tracer = provider
	return p, exporter
}

// TestIDGenerator_PinnedIDUsedOnce: the one Start that pinned an id takes it;
// a concurrent Start of a derived context, and every later one, gets its own.
func TestIDGenerator_PinnedIDUsedOnce(t *testing.T) {
	p, exporter := tracingProviders(t)
	tracer := p.Tracer.Tracer(ScopeName)
	want := trace.SpanID(runspan.RunSpanID("ork-1"))
	ctx := withSpanID(context.Background(), want)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			_, span := tracer.Start(ctx, "ork.run")
			span.End()
		})
	}
	wg.Wait()
	pinned := 0
	seen := map[trace.SpanID]bool{}
	for _, s := range exporter.GetSpans() {
		id := s.SpanContext.SpanID()
		if seen[id] || !id.IsValid() || !s.SpanContext.TraceID().IsValid() {
			t.Fatalf("span %s is repeated or invalid", id)
		}
		seen[id] = true
		if id == want {
			pinned++
		}
	}
	if pinned != 1 || len(seen) != 32 {
		t.Fatalf("%d spans took the pinned id of %d; want exactly one of 32", pinned, len(seen))
	}
}

func finishedRun(id, traceparent string) *orkstackv1.Run {
	return &orkstackv1.Run{Id: id, Namespace: "payments", OrkflowName: "deploy", OrkflowVersion: "1.2.0",
		OrkflowDefinitionDigest: "sha256:abc", Status: orkstackv1.Status_STATUS_FAILED, Traceparent: traceparent,
		StartedAt:  timestamppb.New(time.Unix(1755302400, 0)),
		FinishedAt: timestamppb.New(time.Unix(1755302460, 0)),
		Error:      &orkstackv1.Error{Code: "PIN_HOST_LOST"},
		Telemetry:  &orkstackv1.TelemetrySelection{Attributes: map[string]string{"team": "core"}},
		Origin:     &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1", Principal: "dana", Reference: "PR-7"}}
}

func observe(t *testing.T, p *Providers, run *orkstackv1.Run, event orkstackv1.EventType) {
	t.Helper()
	o, err := NewEngineObserver(p, nil)
	must.OK(t, err)
	o.ObserveEvent(context.Background(), &orkstackv1.Event{RunId: run.GetId(), Type: event, Status: run.GetStatus()}, run)
}

// TestEngineObserver_RunSpanParentAndID: a run's end is one span, under the
// span its submission was accepted in, with the id a child run names.
func TestEngineObserver_RunSpanParentAndID(t *testing.T) {
	p, exporter := tracingProviders(t)
	run := finishedRun("ork-1", callerSampled)
	observe(t, p, run, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("%d spans, want one", len(spans))
	}
	s := spans[0]
	id := runspan.RunSpanID("ork-1")
	if s.Name != "ork.run" || s.SpanContext.SpanID().String() != hex.EncodeToString(id[:]) ||
		s.SpanContext.TraceID().String() != callerTrace || s.Parent.SpanID().String() != callerSpan || !s.Parent.IsRemote() {
		t.Fatalf("span %s %s in %s under %s", s.Name, s.SpanContext.SpanID(), s.SpanContext.TraceID(), s.Parent.SpanID())
	}
	if !s.StartTime.Equal(run.GetStartedAt().AsTime()) || !s.EndTime.Equal(run.GetFinishedAt().AsTime()) {
		t.Fatalf("span from %s to %s", s.StartTime, s.EndTime)
	}
	got := map[attribute.Key]string{}
	for _, kv := range s.Attributes {
		got[kv.Key] = kv.Value.String()
	}
	for key, want := range map[string]string{AttrRunID: "ork-1", AttrNamespace: "payments", AttrOrkflowName: "deploy",
		AttrOrkflowVersion: "1.2.0", AttrOrkflowDigest: "sha256:abc", "ork.origin.kind": "trigger", "ork.origin.id": "trg-1",
		"ork.run.status": "failed", "ork.failure.reason": "PIN_HOST_LOST", "team": "core"} {
		if got[attribute.Key(key)] != want {
			t.Errorf("%s = %q, want %q", key, got[attribute.Key(key)], want)
		}
	}
	for _, kv := range s.Attributes {
		if v := kv.Value.String(); v == "dana" || v == "PR-7" {
			t.Errorf("%s carries the run's principal or reference", kv.Key)
		}
	}
	for _, key := range []string{"ork.parent_run_id", "ork.parent_task_id", "ork.root_run_id"} {
		if _, has := got[attribute.Key(key)]; has {
			t.Errorf("a top-level run carries %s", key)
		}
	}
}

// TestEngineObserver_ChildRunSpanIsUnderItsParents: a child's run span is in
// its parent's trace, under its parent's run span.
func TestEngineObserver_ChildRunSpanIsUnderItsParents(t *testing.T) {
	p, exporter := tracingProviders(t)
	child := finishedRun("ork-child", runspan.ChildTraceparent(callerSampled, "ork-1"))
	child.Status, child.Error = orkstackv1.Status_STATUS_SUCCEEDED, nil
	child.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-1",
		RootRunId: "ork-root", ParentTaskId: "train"}
	observe(t, p, child, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED)
	spans := exporter.GetSpans()
	parent := trace.SpanID(runspan.RunSpanID("ork-1"))
	if len(spans) != 1 || spans[0].Parent.SpanID() != parent || spans[0].SpanContext.TraceID().String() != callerTrace {
		t.Fatalf("spans = %+v", spans)
	}
	got := map[attribute.Key]string{}
	for _, kv := range spans[0].Attributes {
		got[kv.Key] = kv.Value.String()
	}
	if got["ork.parent_run_id"] != "ork-1" || got["ork.parent_task_id"] != "train" || got["ork.root_run_id"] != "ork-root" ||
		got["ork.origin.kind"] != "parent-run" {
		t.Fatalf("attributes = %v", got)
	}
	if _, has := got["ork.failure.reason"]; has {
		t.Fatal("a run that succeeded carries a failure reason")
	}
}

// TestEngineObserver_NoRunSpan: a run that was not traced, or was not
// sampled, or has not ended, makes none.
func TestEngineObserver_NoRunSpan(t *testing.T) {
	unsampled := "00-" + callerTrace + "-" + callerSpan + "-00"
	for name, tc := range map[string]struct {
		traceparent string
		event       orkstackv1.EventType
	}{
		"not traced":  {"", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
		"malformed":   {"00-zz", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
		"not sampled": {unsampled, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
		"not ended":   {callerSampled, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED},
		"a task's":    {callerSampled, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED},
	} {
		t.Run(name, func(t *testing.T) {
			p, exporter := tracingProviders(t)
			observe(t, p, finishedRun("ork-1", tc.traceparent), tc.event)
			if n := len(exporter.GetSpans()); n != 0 {
				t.Fatalf("%d spans, want none", n)
			}
		})
	}
}

// TestEngineObserver_RunSpanGoesToTheNamespacesSinkOnly: a run whose
// namespace selected a sink is told there and never on the default one.
func TestEngineObserver_RunSpanGoesToTheNamespacesSinkOnly(t *testing.T) {
	def, defSpans := tracingProviders(t)
	named, namedSpans := tracingProviders(t)
	o, err := NewEngineObserverForSinks(newSinksForTest(def, map[string]*Providers{"finance": named}), nil)
	must.OK(t, err)
	tell := func(sink string) {
		run := finishedRun("ork-"+sink, callerSampled)
		run.Telemetry.Sink = sink
		o.ObserveEvent(context.Background(), &orkstackv1.Event{RunId: run.GetId(), Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED}, run)
	}
	tell("finance")
	if len(defSpans.GetSpans()) != 0 || len(namedSpans.GetSpans()) != 1 {
		t.Fatalf("default %d, named %d spans", len(defSpans.GetSpans()), len(namedSpans.GetSpans()))
	}
	tell("gone")
	if len(defSpans.GetSpans()) != 0 || len(namedSpans.GetSpans()) != 1 {
		t.Fatalf("an unknown sink's span was sent somewhere: default %d, named %d", len(defSpans.GetSpans()), len(namedSpans.GetSpans()))
	}
	tell("")
	if len(defSpans.GetSpans()) != 1 {
		t.Fatalf("default %d spans", len(defSpans.GetSpans()))
	}
}

// TestOrkerObserver_AttemptSpanNamesItsOrkflowAndTree: an attempt's span says
// which definition it ran and, in a child run, its parent and root.
func TestOrkerObserver_AttemptSpanNamesItsOrkflowAndTree(t *testing.T) {
	for _, child := range []bool{false, true} {
		p, exporter := tracingProviders(t)
		in := observerTestInput()
		in.OrkflowDefinitionDigest = "sha256:abc"
		in.RunContext = &orkstackv1.RunContext{RootRunId: in.GetRunId()}
		if child {
			in.RunContext = &orkstackv1.RunContext{RootRunId: "ork-root", ParentRunId: "ork-mid"}
		}
		ctx, obs := NewOrkerObserver(p, nil, nil).Begin(context.Background(), in, 1)
		obs.End(orker.AttemptOutcome{})
		_ = ctx
		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("%d spans", len(spans))
		}
		want := map[string]string{AttrOrkflowVersion: in.GetOrkflowVersion(), AttrOrkflowDigest: "sha256:abc"}
		if child {
			want[AttrParentRunID], want[AttrRootRunID] = "ork-mid", "ork-root"
		}
		for _, key := range []string{AttrOrkflowVersion, AttrOrkflowDigest, AttrParentRunID, AttrRootRunID} {
			got, has := attrString(spans[0].Attributes, key)
			if w, wants := want[key]; has != wants || got != w {
				t.Errorf("child %v: %s = %q (%v), want %q", child, key, got, has, w)
			}
		}
	}
}

// TestOrkerObserver_TraceContextIsBoundedAndHasNoBaggage: what task.json and
// a task's log records carry is the attempt's span, a bounded state, and
// nothing a caller put in baggage.
func TestOrkerObserver_TraceContextIsBoundedAndHasNoBaggage(t *testing.T) {
	p, exporter := tracingProviders(t)
	member, err := baggage.NewMember("user", "dana")
	must.OK(t, err)
	bag, err := baggage.New(member)
	must.OK(t, err)
	o := NewOrkerObserver(p, nil, nil)
	ctx, obs := o.Begin(baggage.ContextWithBaggage(context.Background(), bag), observerTestInput(), 1)
	carrier := o.TraceContext(ctx)
	obs.End(orker.AttemptOutcome{})
	span := exporter.GetSpans()[0].SpanContext
	want := "00-" + span.TraceID().String() + "-" + span.SpanID().String() + "-01"
	if len(carrier) != 1 || carrier["traceparent"] != want {
		t.Fatalf("carrier = %v, want only traceparent %s", carrier, want)
	}
	if got := o.TraceContext(context.Background()); len(got) != 0 {
		t.Fatalf("carrier of no span = %v", got)
	}
}

// TestEngineObserver_RunSpanNeverLastsUntilItsPublish: a run with no recorded
// end, or an end before its start, spans no time.
func TestEngineObserver_RunSpanNeverLastsUntilItsPublish(t *testing.T) {
	for name, change := range map[string]func(*orkstackv1.Run){
		"no end":                func(r *orkstackv1.Run) { r.FinishedAt = nil },
		"an end before a start": func(r *orkstackv1.Run) { r.FinishedAt = timestamppb.New(r.GetStartedAt().AsTime().Add(-time.Second)) },
		"never started":         func(r *orkstackv1.Run) { r.CreatedAt, r.StartedAt, r.FinishedAt = r.StartedAt, nil, nil },
	} {
		t.Run(name, func(t *testing.T) {
			p, exporter := tracingProviders(t)
			run := finishedRun("ork-1", callerSampled)
			change(run)
			observe(t, p, run, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)
			spans := exporter.GetSpans()
			if len(spans) != 1 || !spans[0].StartTime.Equal(time.Unix(1755302400, 0)) || !spans[0].EndTime.Equal(spans[0].StartTime) {
				t.Fatalf("spans = %+v", spans)
			}
		})
	}
}

// panics is a tracer provider that fails every span.
type panics struct{ tracenoop.TracerProvider }

func (panics) Tracer(string, ...trace.TracerOption) trace.Tracer { panic("exporter") }

// TestEngineObserver_ARunSpanThatPanicsFailsNothing: the event is observed all
// the same.
func TestEngineObserver_ARunSpanThatPanicsFailsNothing(t *testing.T) {
	p := noopProviders(Config{TracesExporter: ExporterOTLP})
	p.Tracer = panics{}
	var logged bytes.Buffer
	o, err := NewEngineObserver(p, slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	must.OK(t, err)
	run := finishedRun("ork-1", callerSampled)
	o.ObserveEvent(context.Background(), &orkstackv1.Event{RunId: "ork-1", Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED}, run)
	if !strings.Contains(logged.String(), "could not be made") || !strings.Contains(logged.String(), "event_type=run_failed") {
		t.Fatalf("logged %q; want the lost span and the event's own line", logged.String())
	}
}
