package api_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// A run's plan pins the trace its submission was accepted in, and none when
// the request was not traced.
func TestSubmitRun_PinsTheRequestsTrace(t *testing.T) {
	const traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	traceID, err := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	must.OK(t, err)
	spanID, err := trace.SpanIDFromHex("b7ad6b7169203331")
	must.OK(t, err)
	state, err := trace.ParseTraceState("vendor=1")
	must.OK(t, err)
	traced := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: state}))

	h := newOriginHarness(t)
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want *engine.TraceContext
	}{
		{"traced", traced, &engine.TraceContext{Traceparent: traceparent, Tracestate: "vendor=1"}},
		{"not traced", context.Background(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.svc.SubmitRun(tc.ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"}))
			must.OK(t, err)
			plans := h.starter.startedPlans()
			got := plans[len(plans)-1].Trace
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("plan.Trace = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestTraceparent_IsOnlyInAnAnswerToAReader: every answer that carries a run,
// through every door (TestRunAnswers_EveryOneIsClassified holds the list), and
// every webhook body: the context the run was started in is only in one that
// asks for run.read.
func TestTraceparent_IsOnlyInAnAnswerToAReader(t *testing.T) {
	const traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	ctx := context.Background()
	at := time.Unix(1755302400, 0)
	traced := func(id string) *orkstackv1.Run {
		return &orkstackv1.Run{Id: id, Namespace: "default", OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_RUNNING,
			CreatedAt: timestamppb.New(at), TraceId: "0af7651916cd43dd8448eb211c80319c", Traceparent: traceparent}
	}
	// keyed is a key bound to a run another caller's request was traced in.
	keyed := func(t *testing.T) *fixture {
		f := newFixture(t)
		f.applyHello(t)
		_, err := f.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", IdempotencyKey: "order-42"}))
		must.OKf(t, err, "first submit")
		run, ok := f.runs.Get("run-001")
		if !ok {
			t.Fatal("no run-001")
		}
		run.Traceparent = traceparent
		f.runs.Save(run)
		return f
	}
	protoText := func(t *testing.T, m proto.Message, err error) string {
		must.OK(t, err)
		b, err := api.MarshalJSON(m)
		must.OK(t, err)
		return string(b)
	}
	table := []struct {
		name   string
		reader bool
		run    string
		answer func(t *testing.T) string
	}{
		{"RunService/GetRun", true, "run-001", func(t *testing.T) string {
			resp, err := keyed(t).runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "run-001"}))
			must.OK(t, err)
			return protoText(t, resp.Msg, nil)
		}},
		{"RunService/ListRuns", true, "run-001", func(t *testing.T) string {
			resp, err := keyed(t).runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{}))
			must.OK(t, err)
			return protoText(t, resp.Msg, nil)
		}},
		{"RunService/DiffRuns", true, "ork-mon", func(t *testing.T) string {
			from, to := diffRun("ork-mon", nil, diffTask("work")), diffRun("ork-tue", nil, diffTask("work"))
			from.Traceparent = traceparent
			return protoText(t, newDiffFixture(t, []*orkstackv1.Run{from, to}).mustDiff(t, "ork-mon", "ork-tue"), nil)
		}},
		{"RunService/SubmitRun", false, "run-001", func(t *testing.T) string {
			resp, err := keyed(t).runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", IdempotencyKey: "order-42"}))
			must.OK(t, err)
			return protoText(t, resp.Msg, nil)
		}},
		{"a trigger, a key's repeat", false, "run-001", func(t *testing.T) string {
			f := newFixture(t)
			f.applyHello(t)
			trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
			headers := map[string]string{api.IdempotencyKeyHeader: "order-42"}
			f.invoke(t, trigger.GetId(), bearer(secret), "{}", headers)
			run, ok := f.runs.Get("run-001")
			if !ok {
				t.Fatal("the invoke started no run-001")
			}
			run.Traceparent = traceparent
			f.runs.Save(run)
			status, body := f.invoke(t, trigger.GetId(), bearer(secret), "{}", headers)
			if status != http.StatusCreated {
				t.Fatalf("invoke = %d %s", status, body)
			}
			return string(body)
		}},
		{"RunService/CancelRun", false, "run-001", func(t *testing.T) string {
			resp, err := keyed(t).runClient().CancelRun(ctx, connect.NewRequest(&orkstackv1.CancelRunRequest{RunId: "run-001"}))
			must.OK(t, err)
			return protoText(t, resp.Msg, nil)
		}},
		{"StatsService/GetStats", false, "ork-live", func(t *testing.T) string {
			f := newStatsFixture(t, at, "alpha")
			live := traced("ork-live")
			live.Namespace = "alpha"
			f.reader.executions["alpha"] = engine.Executions{Running: []*orkstackv1.Run{live}}
			return protoText(t, f.stats(t, api.NewStatsHandler(f.svc, 0), "alpha", at.Add(-time.Hour), at.Add(time.Hour)), nil)
		}},
		{"SystemService/ListReleases", false, "ork-live", func(t *testing.T) string {
			releases := rolloutReleases()
			releases.runs["v1"] = []*orkstackv1.Run{traced("ork-live")}
			return protoText(t, listReleases(t, releaseService(releases, releaseFleet()), ""), nil)
		}},
		{"a webhook", false, "ork-hook", func(t *testing.T) string {
			f := newNotifierFixture(t)
			_, err := f.registry.Register(webhooks.Registration{URL: f.server.URL, Events: []webhooks.Event{webhooks.EventRunSucceeded}})
			must.OK(t, err)
			f.adapter.NotifyRunEvent(ctx, &orkstackv1.Event{RunId: "ork-hook", Sequence: 1,
				Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, Status: orkstackv1.Status_STATUS_SUCCEEDED}, traced("ork-hook"))
			body, _ := f.awaitDelivery(t)
			return string(body)
		}},
		{"a webhook's redelivery", false, "ork-2026-09-21-8f31", func(t *testing.T) string {
			f := newDeliveryFixture(t, http.StatusBadGateway)
			original, run, _ := f.failedOriginal(t)
			run = proto.Clone(run).(*orkstackv1.Run)
			run.Traceparent = traceparent
			f.runs.Save(run)
			f.receiver.answer(http.StatusOK)
			before := f.receiver.count()
			_, err := f.svc.RedeliverWebhook(ctx, connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
			must.OK(t, err)
			body, _ := f.receiver.request(before)
			return string(body)
		}},
	}
	driven := map[string]bool{}
	for _, tc := range table {
		driven[tc.name] = tc.reader
	}
	for reader, names := range map[bool][]string{true: api.RunReaders, false: api.RunsWithoutContext} {
		for _, name := range names {
			if got, ok := driven[name]; !ok || got != reader {
				t.Errorf("%s (a reader's answer: %v) is not driven below", name, reader)
			}
		}
	}
	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.answer(t)
			if !strings.Contains(got, tc.run) {
				t.Fatalf("the answer carries no run %s, so it proves nothing: %s", tc.run, got)
			}
			if has := strings.Contains(got, traceparent); has != tc.reader {
				t.Fatalf("traceparent in the answer = %v, want %v: %s", has, tc.reader, got)
			}
		})
	}
}
