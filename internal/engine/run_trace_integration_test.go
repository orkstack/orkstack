//go:build integration

package engine_test

import (
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/runspan"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// spanCollector keeps what OTLP exporters post to it. It is not a stand-in
// for a trace store: the exporters, their providers and the wire are the
// production ones, and what it answers is only what arrived.
type spanCollector struct {
	mu    sync.Mutex
	spans []*tracev1.Span
}

func (c *spanCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req collectortrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			c.spans = append(c.spans, ss.GetSpans()...)
		}
	}
}

// named are the spans of name, by their ork.run_id.
func (c *spanCollector) named(name string) map[string][]*tracev1.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string][]*tracev1.Span{}
	for _, s := range c.spans {
		if s.GetName() == name {
			id := spanAttr(s, telemetry.AttrRunID)
			out[id] = append(out[id], s)
		}
	}
	return out
}

func spanAttr(s *tracev1.Span, key string) string {
	for _, kv := range s.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

// TestIntegration_OneTraceAcrossARunTree: a root, its child and its
// grandchild, on a real cluster with an engine and an Orker that trace as two
// processes do, are one trace: the one the submission was accepted in. Each
// run has one ork.run under its parent's, each attempt's span is in the trace
// and is what its container reads in task.json, and every run says its trace.
func TestIntegration_OneTraceAcrossARunTree(t *testing.T) {
	collector := &spanCollector{}
	server := httptest.NewServer(collector)
	t.Cleanup(server.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_always_on")
	setup := func(service string) *telemetry.Providers {
		p, err := telemetry.Setup(context.Background(), telemetry.Config{ServiceName: service, TracesExporter: telemetry.ExporterOTLP})
		must.OKf(t, err, "telemetry of %s", service)
		return p
	}
	engineTelemetry, orkerTelemetry := setup(telemetry.ServiceEngine), setup(telemetry.ServiceOrker)
	flush := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		must.OK(t, orkerTelemetry.Shutdown(ctx))
		must.OK(t, engineTelemetry.Shutdown(ctx))
	}
	engineClient, err := telemetry.TemporalInterceptors(engineTelemetry)
	must.OK(t, err)
	orkerClient, err := telemetry.TemporalInterceptors(orkerTelemetry)
	must.OK(t, err)
	observer, err := telemetry.NewEngineObserver(engineTelemetry, nil)
	must.OK(t, err)
	e := startTracedEachE2E(t, true, eachE2ETracing{engineClient: engineClient, orkerClient: orkerClient, observer: observer,
		orker: telemetry.NewOrkerObserver(orkerTelemetry, nil, nil)})
	e.startOrker("run-trace")

	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	// Each container returns the trace context it was given.
	report := &orkstackv1.Task{Id: "report", Uses: e.alpine, Image: image, TimeoutSeconds: 120,
		Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE, Command: []string{"/bin/sh", "-c",
			`tp=$(tr -d ' \n' < /orkstack/input/task.json | sed -n 's/.*"traceparent":"\([0-9a-f-]*\)".*/\1/p'); ` +
				`printf '{"traceparent":"%s","baggage":"%s"}' "$tp" "$(grep -c baggage /orkstack/input/task.json)" > /orkstack/output/values.json`}}
	composed := func(name string, child *orkstackv1.Orkflow) *orkstackv1.Orkflow {
		wf := &orkstackv1.Orkflow{Name: name, Version: "0.1.0", Tasks: []*orkstackv1.Task{report}}
		if child != nil {
			wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: "below", Uses: "orkflow:" + child.GetName() + "@" + child.GetVersion(),
				Needs: []string{"report"}, Orkflow: &orkstackv1.OrkflowRef{Name: child.GetName(), Version: child.GetVersion(),
					DefinitionDigest: child.GetDefinitionDigest()}})
		}
		stampDigest(t, wf)
		return wf
	}
	leaf := composed("trace-leaf", nil)
	mid := composed("trace-mid", leaf)
	root := composed("trace-root", mid)

	const traceID = "0af7651916cd43dd8448eb211c80319c"
	// The request the run is submitted in, as the API's handler leaves it.
	requestTrace, err := trace.TraceIDFromHex(traceID)
	must.OK(t, err)
	requestSpan, err := trace.SpanIDFromHex("b7ad6b7169203331")
	must.OK(t, err)
	request := trace.ContextWithRemoteSpanContext(e.ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: requestTrace, SpanID: requestSpan, TraceFlags: trace.FlagsSampled}))
	rootID := fmt.Sprintf("ork-trace-%d", time.Now().UnixNano())
	midID, leafID := rootID+"-mid", rootID+"-leaf"
	leafPlan, err := engine.NewChildRunPlan(leaf, e.plan(), false, false)
	must.OK(t, err)
	leafPlan.RunID = leafID
	midPlan, err := engine.NewChildRunPlan(mid, e.plan(), false, false)
	must.OK(t, err)
	midPlan.RunID = midID
	midPlan.Plan.Children = map[string]*engine.ChildRunPlan{"below": leafPlan}
	plan := e.plan()
	plan.RecordRootRun = true
	plan.Trace = engine.NewTraceContext(telemetry.RunTrace(request))
	plan.Children = map[string]*engine.ChildRunPlan{"below": midPlan}
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	must.OKf(t, starter.StartRun(request, rootID, root, nil, plan), "StartRun")

	runs := map[string]*orkstackv1.Run{}
	for _, id := range []string{rootID, midID, leafID} {
		run := e.wait(id)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %s = %v %v", id, run.GetStatus(), run.GetError())
		}
		runs[id] = run
	}
	flush()

	runSpans, attempts := collector.named(telemetry.RunSpanName), collector.named("ork.task.attempt")
	parentOf := map[string]string{rootID: "b7ad6b7169203331", midID: spanHex(rootID), leafID: spanHex(midID)}
	var lines []string
	for _, id := range []string{rootID, midID, leafID} {
		if runs[id].GetTraceId() != traceID || !strings.HasPrefix(runs[id].GetTraceparent(), "00-"+traceID+"-"+parentOf[id]) {
			t.Errorf("run %s trace_id %q traceparent %q", id, runs[id].GetTraceId(), runs[id].GetTraceparent())
		}
		if len(runSpans[id]) != 1 || len(attempts[id]) != 1 {
			t.Fatalf("run %s has %d run spans and %d attempt spans, want one of each", id, len(runSpans[id]), len(attempts[id]))
		}
		span, attempt := runSpans[id][0], attempts[id][0]
		if hex.EncodeToString(span.GetTraceId()) != traceID || hex.EncodeToString(span.GetSpanId()) != spanHex(id) ||
			hex.EncodeToString(span.GetParentSpanId()) != parentOf[id] {
			t.Errorf("run %s: span %x under %x in %x", id, span.GetSpanId(), span.GetParentSpanId(), span.GetTraceId())
		}
		if id != rootID && spanAttr(span, telemetry.AttrRootRunID) != rootID {
			t.Errorf("run %s: its span's root is %q", id, spanAttr(span, telemetry.AttrRootRunID))
		}
		var values struct{ Traceparent, Baggage string }
		must.OK(t, json.Unmarshal([]byte(readValues(t, e, taskRunNamed(runs[id], "report").GetValuesRef())), &values, json.MatchCaseInsensitiveNames(true)))
		want := "00-" + traceID + "-" + hex.EncodeToString(attempt.GetSpanId()) + "-01"
		if values.Traceparent != want || values.Baggage != "0" {
			t.Errorf("run %s: its container read %+v, its attempt's span is %s", id, values, want)
		}
		lines = append(lines, fmt.Sprintf("%s ork.run=%x parent=%x attempt=%s", id, span.GetSpanId(), span.GetParentSpanId(), values.Traceparent))
	}
	proof.Write(t, "engine", "run-trace-tree.txt", []byte(strings.Join(lines, "\n")+"\n"))
}

// TestIntegration_OneTraceAcrossAFireAndAContinuation: a scheduled run's
// trace is its fire's, and it stays one trace when the run continues as new:
// every execution of it, every attempt and its one ork.run, under the span
// its fire prepared it in.
func TestIntegration_OneTraceAcrossAFireAndAContinuation(t *testing.T) {
	collector := &spanCollector{}
	server := httptest.NewServer(collector)
	t.Cleanup(server.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_always_on")
	providers, err := telemetry.Setup(context.Background(), telemetry.Config{ServiceName: telemetry.ServiceEngine, TracesExporter: telemetry.ExporterOTLP})
	must.OK(t, err)
	interceptors, err := telemetry.TemporalInterceptors(providers)
	must.OK(t, err)
	observer, err := telemetry.NewEngineObserver(providers, nil)
	must.OK(t, err)
	attempts := telemetry.NewOrkerObserver(providers, nil, nil)
	canStackInterceptors, canStackObserver = interceptors, observer
	t.Cleanup(func() { canStackInterceptors, canStackObserver = nil, nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var (
		mu         sync.Mutex
		runID      string
		prepared   string
		orkerQueue string
	)
	budget := engine.DefaultContinuationBudget(50, 1<<20, 5*time.Minute)
	const tasks = 12
	prepare := func(actx context.Context, rec *orkstackv1.ScheduleRecord, at time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		mu.Lock()
		defer mu.Unlock()
		wf, err := dsl.Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: nightly\nversion: \"1.0.0\"\ntasks:\n" + guardChain("n", tasks, "")))
		if err != nil {
			return nil, err
		}
		wf.Namespace = "default"
		runID = fmt.Sprintf("ork-traced-fire-%d", time.Now().UnixNano())
		plan := rawPlan(orkerQueue)
		plan.CommitRoot = "mem://commits"
		plan.History = &engine.HistoryLimit{MaxEvents: 1000, MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
		plan.Continuation = &budget
		plan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SCHEDULE", ID: rec.GetScheduleId(), Reference: at.UTC().Format(time.RFC3339)}
		// As the API's preparation pins it, in the fire's own activity.
		plan.Trace = engine.NewTraceContext(telemetry.RunTrace(actx))
		if plan.Trace != nil {
			prepared = plan.Trace.Traceparent
		}
		encoded, err := stablejson.Marshal(plan)
		if err != nil {
			return nil, err
		}
		return &orkstackv1.ScheduledRunPreparation{Run: &orkstackv1.Run{Id: runID, Namespace: "default", OrkflowName: wf.GetName(),
			OrkflowVersion: wf.GetVersion(), Status: orkstackv1.Status_STATUS_PENDING, CreatedAt: timestamppb.Now()}, Orkflow: wf, Plan: encoded}, nil
	}
	behavior := func(actx context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		_, attempt := attempts.Begin(actx, in, 1)
		defer attempt.End(orker.AttemptOutcome{})
		return transparentBehavior(actx, s, in)
	}
	s := startCANStack(t, ctx, "trace-can", false, behavior, func(w worker.Worker) {
		w.RegisterActivityWithOptions(prepare, activity.RegisterOptions{Name: engine.ActivityPrepareScheduledRun})
	})
	mu.Lock()
	orkerQueue = s.orkerQueue
	mu.Unlock()
	scheduler := engine.NewTemporalScheduler(s.client, s.runQueue)
	name := fmt.Sprintf("traced-%d", time.Now().UnixNano())
	rec := &orkstackv1.ScheduleRecord{ScheduleId: "sch-00000000" + fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff),
		Name: name, Namespace: "default", OrkflowName: "nightly",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL}}
	_, err = scheduler.Create(ctx, rec, true)
	must.OKf(t, err, "create schedule")
	defer func() { _ = scheduler.Delete(context.Background(), "default", name) }()
	must.OKf(t, scheduler.Fire(ctx, "default", name, orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED), "fire")
	// The fire ends when its run's chain does, so its end is the run's.
	fires := scheduleFireIDs(t, ctx, s.client, rec.GetScheduleId(), 1)
	must.OKf(t, s.client.GetWorkflow(ctx, fires[0], "").Get(ctx, nil), "the fire")
	mu.Lock()
	id, parent := runID, prepared
	mu.Unlock()
	described, err := describeExecution(s.client, ctx, id, "")
	must.OK(t, err)
	chain := s.chainOf(id, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) < 2 {
		t.Fatalf("the scheduled run ran %d generations, want it to continue as new", len(chain))
	}
	run, err := s.reader.GetRun(ctx, id)
	must.OK(t, err)
	flush, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()
	must.OK(t, providers.Shutdown(flush))

	want, ok := runspan.Parse(parent)
	if !ok {
		t.Fatalf("the fire's preparation was not traced: %q", parent)
	}
	if run.GetTraceId() != want.TraceID || run.GetTraceparent() != parent || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %v trace_id %q traceparent %q, want %s", run.GetStatus(), run.GetTraceId(), run.GetTraceparent(), parent)
	}
	runSpans := collector.named(telemetry.RunSpanName)[id]
	if len(runSpans) != 1 || hex.EncodeToString(runSpans[0].GetTraceId()) != want.TraceID ||
		hex.EncodeToString(runSpans[0].GetParentSpanId()) != want.SpanID || hex.EncodeToString(runSpans[0].GetSpanId()) != spanHex(id) {
		t.Fatalf("run spans = %v, want one under the fire's preparation", runSpans)
	}
	made := collector.named("ork.task.attempt")[id]
	if len(made) != tasks {
		t.Fatalf("%d attempt spans, want %d", len(made), tasks)
	}
	for _, span := range made {
		if hex.EncodeToString(span.GetTraceId()) != want.TraceID {
			t.Errorf("attempt %s is in trace %x, its run's is %s", spanAttr(span, telemetry.AttrTaskID), span.GetTraceId(), want.TraceID)
		}
	}
	// Each generation's workflow span, as the SDK makes one for every execution.
	executions := map[string]string{}
	collector.mu.Lock()
	for _, span := range collector.spans {
		if span.GetName() == "RunWorkflow:"+engine.RunWorkflowType && spanAttr(span, "temporalWorkflowID") == id {
			executions[spanAttr(span, "temporalRunID")] = hex.EncodeToString(span.GetTraceId())
		}
	}
	collector.mu.Unlock()
	for _, execution := range chain {
		if executions[execution] != want.TraceID {
			t.Errorf("generation %s is in trace %q, the fire's is %s", execution, executions[execution], want.TraceID)
		}
	}
	proof.Write(t, "engine", "run-trace-fire-continuation.txt", []byte(fmt.Sprintf("run=%s generations=%d trace=%s fire_span=%s attempts=%d\n",
		id, len(chain), want.TraceID, want.SpanID, len(made))))
}

func spanHex(runID string) string {
	id := runspan.RunSpanID(runID)
	return hex.EncodeToString(id[:])
}
