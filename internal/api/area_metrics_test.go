package api_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func metricProviders(t *testing.T) (*telemetry.Providers, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return &telemetry.Providers{Meter: mp}, reader
}

// counts reads every point of name as "k=v,k=v" -> value: a counter's sum,
// or a histogram's count.
func counts(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	out := map[string]float64{}
	key := func(set []string) string { sort.Strings(set); return strings.Join(set, ",") }
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					var set []string
					for _, kv := range dp.Attributes.ToSlice() {
						set = append(set, string(kv.Key)+"="+kv.Value.AsString())
					}
					out[key(set)] += float64(dp.Value)
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					var set []string
					for _, kv := range dp.Attributes.ToSlice() {
						set = append(set, string(kv.Key)+"="+kv.Value.AsString())
					}
					out[key(set)] += float64(dp.Count)
				}
			default:
				t.Fatalf("%s: unexpected data %T", name, m.Data)
			}
		}
	}
	return out
}

// TestTriggerInvoke_EveryAnswerIsCountedOnceWithItsOutcome: each invocation
// is one ork.trigger.invocations point, under the found trigger's namespace,
// or under none while no trigger was found.
func TestTriggerInvoke_EveryAnswerIsCountedOnceWithItsOutcome(t *testing.T) {
	p, reader := metricProviders(t)
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.TriggerMetrics = telemetry.NewTriggerMetrics(p, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})

	for _, c := range []struct {
		id     string
		token  *string
		body   string
		status int
	}{
		{trigger.GetId(), bearer(secret), `{}`, http.StatusCreated},
		{trigger.GetId(), nil, `{}`, http.StatusUnauthorized},
		{trigger.GetId(), bearer("wrong"), `{}`, http.StatusUnauthorized},
		{"trg-0000000000000000", bearer(secret), `{}`, http.StatusNotFound},
		{trigger.GetId(), bearer(secret), `[1]`, http.StatusBadRequest},
	} {
		if status, body := f.invoke(t, c.id, c.token, c.body, nil); status != c.status {
			t.Fatalf("invoke %s = %d, want %d: %s", c.id, status, c.status, body)
		}
	}
	got := counts(t, reader, telemetry.TriggerInvocationsMetric)
	want := map[string]float64{
		"ork.namespace=default,outcome=started":          1,
		"outcome=unauthenticated":                        1,
		"ork.namespace=default,outcome=unauthenticated":  1,
		"outcome=not_found":                              1,
		"ork.namespace=default,outcome=invalid_argument": 1,
	}
	if len(got) != len(want) {
		t.Fatalf("invocations = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("invocations[%s] = %v, want %v (all: %v)", k, got[k], v, got)
		}
	}
}

// TestSchedule_RequestsAndFiresAreCountedUnderTheirNamespace: a schedule
// API request that succeeded is counted by its action, and each fire's
// preparation by its outcome, a refusal with its reason.
func TestSchedule_RequestsAndFiresAreCountedUnderTheirNamespace(t *testing.T) {
	p, reader := metricProviders(t)
	h := newScheduleHarnessWith(t, func(cfg *api.Config) {
		cfg.ScheduleMetrics = telemetry.NewScheduleMetrics(p, nil, false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	ctx := context.Background()
	h.mustCreate(t, "nightly")
	_, err := h.svc.FireSchedule(ctx, connect.NewRequest(&orkstackv1.FireScheduleRequest{Name: "nightly"}))
	must.OK(t, err)
	if _, err := h.svc.FireSchedule(ctx, connect.NewRequest(&orkstackv1.FireScheduleRequest{Name: "missing"})); err == nil {
		t.Fatal("firing a missing schedule succeeded")
	}

	acts := api.NewScheduleActivities(h.svc, fixedIndexes{})
	_, err = acts.PrepareScheduledRun(ctx, scheduleRec(""), h.now.Add(-time.Minute))
	must.OK(t, err)
	missing := scheduleRec("")
	missing.OrkflowName = "no-such-orkflow"
	if _, err := acts.PrepareScheduledRun(ctx, missing, h.now); err == nil {
		t.Fatal("a fire of a missing orkflow was prepared")
	}

	requests := counts(t, reader, telemetry.ScheduleRequestsMetric)
	if requests["action=create,ork.namespace=default"] != 1 || requests["action=fire,ork.namespace=default"] != 1 || len(requests) != 2 {
		t.Fatalf("requests = %v; want one create and one fire, and no failed request", requests)
	}
	fires := counts(t, reader, telemetry.ScheduleFiresMetric)
	if fires["ork.namespace=default,outcome=started"] != 1 ||
		fires["ork.namespace=default,ork.refusal.reason=not_found,outcome=refused"] != 1 || len(fires) != 2 {
		t.Fatalf("fires = %v; want one started and one refused not_found", fires)
	}
	if lag := counts(t, reader, telemetry.ScheduleFireLagMetric); lag["ork.namespace=default"] != 2 {
		t.Fatalf("lag = %v, want one observation per fire", lag)
	}
}

// TestSchedule_AFireALimitKeepsFromStartingIsCountedWithItsOutcome: a skip
// limit's full key and a full line are counted skipped and not_started,
// never dropped as outcomes outside the contract.
func TestSchedule_AFireALimitKeepsFromStartingIsCountedWithItsOutcome(t *testing.T) {
	p, reader := metricProviders(t)
	gate := engine.GateID("default", "", "db:prod")
	concReader := &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2, Queued: 1, HolderRuns: []string{"ork-a", "ork-b"}}}}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(),
		Starter: &concStarter{fakeStarter: &fakeStarter{}, indexed: true}, Capabilities: &fakeCapabilities{}, ConcurrencyReader: concReader,
		ScheduleMetrics: telemetry.NewScheduleMetrics(p, nil, false, slog.New(slog.NewTextHandler(io.Discard, nil)))})
	acts := api.NewScheduleActivities(svc, nil)
	for _, doc := range []string{limitedYAML(templatedLimit + "\n  on_limit: skip"), strings.Replace(limitedYAML(templatedLimit+"\n  max_queued: 1"), "name: migrate", "name: queued", 1)} {
		must.OK(t, applyDoc(t, svc, doc))
	}
	for _, name := range []string{"migrate", "queued"} {
		rec := &orkstackv1.ScheduleRecord{ScheduleId: "sch-1", Name: "nightly", OrkflowName: name}
		if _, err := acts.PrepareScheduledRun(context.Background(), rec, time.Now()); err == nil {
			t.Fatalf("a fire of %s started", name)
		}
	}
	fires := counts(t, reader, telemetry.ScheduleFiresMetric)
	if fires["ork.namespace=default,outcome=skipped"] != 1 || fires["ork.namespace=default,outcome=not_started"] != 1 {
		t.Fatalf("fires = %v; want one skipped and one not_started", fires)
	}
}

// TestSubmitRun_ALimitsRefusalIsCountedAsAnArrival: a submit a limit refuses
// counts one arrival under its outcome and the declaration's label.
func TestSubmitRun_ALimitsRefusalIsCountedAsAnArrival(t *testing.T) {
	p, reader := metricProviders(t)
	gate := engine.GateID("default", "", "db:prod")
	concReader := &fakeConcReader{lines: map[string]engine.GateLine{gate: {Present: true, Holders: 2, HolderRuns: []string{"ork-a", "ork-b"}}}}
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(),
		Starter: &concStarter{fakeStarter: &fakeStarter{}, indexed: true}, Capabilities: &fakeCapabilities{}, ConcurrencyReader: concReader,
		ConcurrencyMetrics: telemetry.NewConcurrencyMetrics(p, nil, true, slog.New(slog.NewTextHandler(io.Discard, nil)))})
	must.OK(t, applyDoc(t, svc, limitedYAML(templatedLimit+"\n  on_limit: refuse")))
	if err := submitName(svc, "migrate"); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("submit = %v; want the limit's refusal", err)
	}
	got := counts(t, reader, telemetry.ConcurrencyArrivalsMetric)
	key := "ork.concurrency.level=run,ork.concurrency.limit=" + engine.CensusLimitLabel("db:{{ .inputs.env }}") +
		",ork.concurrency.outcome=refused,ork.concurrency.strategy=refuse,ork.namespace=default,ork.orkflow.name=migrate"
	if got[key] != 1 || len(got) != 1 {
		t.Fatalf("arrivals = %v; want one refused", got)
	}
}
