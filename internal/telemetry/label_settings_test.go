package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestConfigFrom_LabelAndBudgetSettings: the Orkflow label is on, the
// cardinality limit 2000 and the census budget 60 unless set; each refuses a
// value it cannot use, naming its variable.
func TestConfigFrom_LabelAndBudgetSettings(t *testing.T) {
	cfg, err := configFrom(ServiceEngine, lookupOf(nil))
	must.OK(t, err)
	if !cfg.OrkflowLabel || cfg.MetricCardinalityLimit != DefaultMetricCardinalityLimit ||
		cfg.CensusRequestsPerMinute != DefaultCensusRequestsPerMinute {
		t.Fatalf("defaults = %v, %d, %d; want true, %d, %d", cfg.OrkflowLabel, cfg.MetricCardinalityLimit,
			cfg.CensusRequestsPerMinute, DefaultMetricCardinalityLimit, DefaultCensusRequestsPerMinute)
	}
	cfg, err = configFrom(ServiceEngine, lookupOf(map[string]string{
		OrkflowLabelVar: "false", MetricCardinalityLimitVar: "500", CensusRequestsPerMinuteVar: "12",
	}))
	must.OK(t, err)
	if cfg.OrkflowLabel || cfg.MetricCardinalityLimit != 500 || cfg.CensusRequestsPerMinute != 12 {
		t.Fatalf("set = %v, %d, %d; want false, 500, 12", cfg.OrkflowLabel, cfg.MetricCardinalityLimit, cfg.CensusRequestsPerMinute)
	}
	for variable, value := range map[string]string{
		OrkflowLabelVar: "maybe", MetricCardinalityLimitVar: "0", CensusRequestsPerMinuteVar: "-1",
	} {
		if _, err := configFrom(ServiceEngine, lookupOf(map[string]string{variable: value})); err == nil {
			t.Errorf("%s=%q was accepted", variable, value)
		}
	}
}

// TestCardinalityLimit_FoldsOverflow: past the limit, new series fold into
// the one the SDK marks otel.metric.overflow=true.
func TestCardinalityLimit_FoldsOverflow(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(append(meterLimitOptions(Config{MetricCardinalityLimit: 3}), sdkmetric.WithReader(reader))...)
	counter, err := provider.Meter(ScopeName).Int64Counter("ork.test.series")
	must.OK(t, err)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("k", name)))
	}
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	points := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64]).DataPoints
	overflow := false
	for _, p := range points {
		if v, ok := p.Attributes.Value("otel.metric.overflow"); ok && v.AsBool() {
			overflow = true
		}
	}
	if len(points) != 3 || !overflow {
		t.Fatalf("points = %d (overflow %v); want 3 with the overflow series", len(points), overflow)
	}
}

// TestOrkflowLabel_ScopeIsTheTable: with the label on, ork.runs, ork.tasks,
// ork.run.duration and ork.runs.refused carry ork.orkflow.name and
// ork.task.duration does not; with it off, none does (low cardinality).
func TestOrkflowLabel_ScopeIsTheTable(t *testing.T) {
	for _, on := range []bool{true, false} {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		observer, err := NewEngineObserver(&Providers{Meter: mp, cfg: Config{OrkflowLabel: on}}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
		must.OK(t, err)
		started := time.Unix(1755302400, 0).UTC()
		run := &orkstackv1.Run{
			Id: "run-1", Namespace: "team", OrkflowName: "hello", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			StartedAt: timestamppb.New(started), FinishedAt: timestamppb.New(started.Add(time.Second)),
			TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main",
				ScheduledAt: timestamppb.New(started), FinishedAt: timestamppb.New(started.Add(time.Second)), Attempt: 1}},
		}
		ctx := context.Background()
		observer.ObserveEvent(ctx, runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, "build"), run)
		observer.ObserveEvent(ctx, runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, ""), run)
		observer.ObserveRefusal(ctx, "team", nil, "hello", "NAMESPACE_HELD")

		carries := map[string]bool{}
		for _, sm := range collect(t, reader).ScopeMetrics {
			for _, m := range sm.Metrics {
				has := false
				switch data := m.Data.(type) {
				case metricdata.Sum[int64]:
					for _, dp := range data.DataPoints {
						_, ok := dp.Attributes.Value(AttrOrkflowName)
						has = has || ok
					}
				case metricdata.Histogram[float64]:
					for _, dp := range data.DataPoints {
						_, ok := dp.Attributes.Value(AttrOrkflowName)
						has = has || ok
					}
				}
				carries[m.Name] = has
			}
		}
		for _, name := range []string{"ork.runs", "ork.tasks", "ork.run.duration", "ork.runs.refused"} {
			if carries[name] != on {
				t.Errorf("label on=%v: %s carries ork.orkflow.name = %v", on, name, carries[name])
			}
		}
		if carries["ork.task.duration"] {
			t.Errorf("label on=%v: ork.task.duration carries ork.orkflow.name", on)
		}
	}
}
