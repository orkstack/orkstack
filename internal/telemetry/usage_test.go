package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
)

// usagePoint is one series: its sum (a counter's value, a histogram's sum),
// a histogram's count, and its attributes.
type usagePoint struct {
	sum   float64
	count uint64
	attrs attribute.Set
}

func usagePoints(t *testing.T, reader sdkmetric.Reader) map[string][]usagePoint {
	t.Helper()
	out := map[string][]usagePoint{}
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], usagePoint{sum: float64(dp.Value), attrs: dp.Attributes})
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], usagePoint{sum: dp.Value, attrs: dp.Attributes})
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], usagePoint{sum: dp.Sum, count: dp.Count, attrs: dp.Attributes})
				}
			case metricdata.Histogram[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], usagePoint{sum: float64(dp.Sum), count: dp.Count, attrs: dp.Attributes})
				}
			}
		}
	}
	return out
}

// recordingObserver is the observer UsageObserver wraps.
type recordingObserver struct{ phases, ends int }

func (r *recordingObserver) Begin(ctx context.Context, _ *orkstackv1.ExecuteTaskInput, _ int32) (context.Context, orker.AttemptObserver) {
	return ctx, r
}
func (r *recordingObserver) Phase(string)             { r.phases++ }
func (r *recordingObserver) End(orker.AttemptOutcome) { r.ends++ }

// TestUsageObserver_RecordsWhatTheContainerUsed: an attempt's measured CPU
// time, its exact peak and the utilizations against what was reserved, the
// reservation × held time, an OOM kill, and an unmeasured attempt by reason;
// labelled with pool and namespace; End passed through every time; a sampled
// peak and an unmeasured attempt record no figure.
func TestUsageObserver_RecordsWhatTheContainerUsed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	inner := &recordingObserver{}
	obs := NewUsageObserver(inner, sinksOf(&Providers{Meter: mp}), nil)
	in := &orkstackv1.ExecuteTaskInput{RunId: "r", TaskId: "t", Namespace: "payments", Pool: "gpu"}
	reserved := &orkstackv1.ReservedResources{Cpu: 2, MemoryBytes: 1 << 30}
	start := time.Unix(1000, 0)

	_, attempt := obs.Begin(context.Background(), in, 1)
	attempt.Phase("run")
	measured := orker.AttemptOutcome{Reserved: reserved, Usage: &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2, CpuSeconds: 10,
		MemoryPeakBytes: 256 << 20, MemoryPeakExact: true, MemoryLimitBytes: 1 << 30, OomKilled: true,
		ContainerStartedAt: timestamppb.New(start), ContainerExitedAt: timestamppb.New(start.Add(10 * time.Second)),
	}}
	attempt.End(measured)
	attempt.End(measured) // recorded once

	_, sampled := obs.Begin(context.Background(), in, 2)
	sampled.End(orker.AttemptOutcome{Reserved: reserved, Usage: &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V1, CpuSeconds: 1, MemoryPeakBytes: 1 << 20, MemoryLimitBytes: 1 << 30,
	}})
	_, off := obs.Begin(context.Background(), in, 3)
	off.End(orker.AttemptOutcome{Usage: &orkstackv1.MeasuredUsage{
		Method:           orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED,
		UnmeasuredReason: orkstackv1.UnmeasuredReason_UNMEASURED_REASON_DISABLED,
	}})
	_, newer := obs.Begin(context.Background(), in, 4)
	newer.End(orker.AttemptOutcome{Usage: &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED, UnmeasuredReason: 99,
	}})

	if inner.phases != 1 || inner.ends != 5 {
		t.Fatalf("inner saw %d phases and %d ends, want 1 and 5 (End passed through every time)", inner.phases, inner.ends)
	}
	points := usagePoints(t, reader)
	one := func(name string) usagePoint {
		t.Helper()
		got := points[name]
		if len(got) != 1 {
			t.Fatalf("%s: %d series, want 1: %+v", name, len(got), got)
		}
		for _, key := range []string{AttrPool, AttrNamespace} {
			if v, _ := got[0].attrs.Value(attribute.Key(key)); v.AsString() == "" {
				t.Fatalf("%s carries no %s", name, key)
			}
		}
		return got[0]
	}
	if p := one("ork.orker.task.cpu.time"); p.count != 2 || p.sum != 11 {
		t.Errorf("cpu.time = %+v, want both measured attempts, 11 s", p)
	}
	if p := one("ork.orker.task.cpu.utilization"); p.count != 1 || p.sum != 0.5 {
		t.Errorf("cpu.utilization = %+v, want one point of 10 s over 2 cpu × 10 s", p)
	}
	if p := one("ork.orker.task.memory.peak"); p.count != 1 || p.sum != 256<<20 {
		t.Errorf("memory.peak = %+v, want only the exact peak", p)
	}
	if p := one("ork.orker.task.memory.utilization"); p.count != 1 || p.sum != 0.25 {
		t.Errorf("memory.utilization = %+v, want 0.25", p)
	}
	if p := one("ork.orker.task.oom_kills"); p.sum != 1 {
		t.Errorf("oom_kills = %+v", p)
	}
	if p := one("ork.orker.task.reserved.cpu_time"); p.sum <= 0 {
		t.Errorf("reserved.cpu_time = %+v, want the reservation × held time", p)
	}
	byReason := map[string]float64{}
	for _, p := range points["ork.orker.task.usage.unmeasured"] {
		v, _ := p.attrs.Value(AttrUnmeasuredReason)
		byReason[v.AsString()] = p.sum
	}
	if byReason["DISABLED"] != 1 || byReason["UNSPECIFIED"] != 1 || len(byReason) != 2 {
		t.Errorf("unmeasured by reason = %v, want DISABLED 1 and an unknown reason under UNSPECIFIED", byReason)
	}
}

// TestUsageObserver_NoUsageRecordsNoFigure: an attempt whose outcome carries
// no usage records no figure; a nil inner observer and nil sinks are fine.
func TestUsageObserver_NoUsageRecordsNoFigure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	obs := NewUsageObserver(nil, sinksOf(&Providers{Meter: mp}), nil)
	_, attempt := obs.Begin(context.Background(), &orkstackv1.ExecuteTaskInput{Pool: "main", Namespace: "default"}, 1)
	attempt.Phase("run")
	attempt.End(orker.AttemptOutcome{})
	points := usagePoints(t, reader)
	for _, name := range []string{"ork.orker.task.cpu.time", "ork.orker.task.memory.peak", "ork.orker.task.usage.unmeasured"} {
		if len(points[name]) != 0 {
			t.Errorf("%s recorded %+v for an attempt with no usage", name, points[name])
		}
	}
	_, a := NewUsageObserver(nil, nil, nil).Begin(context.Background(), nil, 1)
	a.End(orker.AttemptOutcome{}) // no panic
}

// TestUsageInstruments_SamplerFailedCounts: a recovered sampler panic counts;
// a nil set of instruments counts nothing and does not panic.
func TestUsageInstruments_SamplerFailedCounts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	inst, err := NewUsageInstruments(&Providers{Meter: mp})
	if err != nil {
		t.Fatal(err)
	}
	inst.SamplerFailed()
	var nilInst *UsageInstruments
	nilInst.SamplerFailed() // no panic
	got := usagePoints(t, reader)["ork.orker.usage.sampler_failures"]
	if len(got) != 1 || got[0].sum != 1 {
		t.Fatalf("sampler_failures = %+v", got)
	}
}

// TestUsageObserver_TwinsCountEveryNamespaceOnTheDefaultSink: the OOM and
// unmeasured twins are recorded on the default sink for an attempt whose
// namespace exports elsewhere, and for one whose sink is undeclared, with
// only the pool and the reason; the namespaced instruments go to the
// attempt's own sink and nowhere when it is undeclared.
func TestUsageObserver_TwinsCountEveryNamespaceOnTheDefaultSink(t *testing.T) {
	defRead, teamRead := sdkmetric.NewManualReader(), sdkmetric.NewManualReader()
	defMP := sdkmetric.NewMeterProvider(sdkmetric.WithReader(defRead))
	teamMP := sdkmetric.NewMeterProvider(sdkmetric.WithReader(teamRead))
	t.Cleanup(func() { _ = defMP.Shutdown(context.Background()); _ = teamMP.Shutdown(context.Background()) })
	sinks := newSinksForTest(&Providers{Meter: defMP}, map[string]*Providers{"team": {Meter: teamMP}})
	obs := NewUsageObserver(nil, sinks, nil)
	oom := orker.AttemptOutcome{Usage: &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2, CpuSeconds: 1, OomKilled: true}}
	off := orker.AttemptOutcome{Usage: &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED, UnmeasuredReason: orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NO_CGROUP}}
	for _, tc := range []struct {
		sink    string
		outcome orker.AttemptOutcome
	}{{"team", oom}, {"team", off}, {"ghost", oom}} {
		in := &orkstackv1.ExecuteTaskInput{Namespace: "payments", Pool: "gpu", OrkflowName: "secret-flow",
			Telemetry: &orkstackv1.TelemetrySelection{Sink: tc.sink}}
		_, a := obs.Begin(context.Background(), in, 1)
		a.End(tc.outcome)
	}
	def, team := usagePoints(t, defRead), usagePoints(t, teamRead)
	if got := def[DeploymentOOMKillsMetric]; len(got) != 1 || got[0].sum != 2 {
		t.Fatalf("OOM twin on the default sink = %+v, want 2 (the undeclared sink's attempt included)", got)
	}
	if got := def[DeploymentUnmeasuredMetric]; len(got) != 1 || got[0].sum != 1 {
		t.Fatalf("unmeasured twin on the default sink = %+v, want 1", got)
	}
	for _, name := range []string{DeploymentOOMKillsMetric, DeploymentUnmeasuredMetric} {
		for _, kv := range def[name][0].attrs.ToSlice() {
			if kv.Key != AttrPool && kv.Key != AttrUnmeasuredReason {
				t.Errorf("twin %s carries %s=%q", name, kv.Key, kv.Value.String())
			}
		}
	}
	if got := team["ork.orker.task.oom_kills"]; len(got) != 1 || got[0].sum != 1 {
		t.Errorf("the team sink's oom_kills = %+v, want 1 (the undeclared sink's attempt is not sent there)", got)
	}
	if len(def["ork.orker.task.oom_kills"]) != 0 {
		t.Errorf("the default sink carries the namespaced oom_kills of a namespace on another sink")
	}
}

// recordAttrs is a log record's attributes as strings.
func recordAttrs(r sdklog.Record) map[string]string {
	out := map[string]string{}
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		out[string(kv.Key)] = kv.Value.String()
		return true
	})
	return out
}

// TestUsageObserver_LogsUsageRecordsOnTheRunsSink: a running attempt's
// readings and its summary are ork.record=usage records on the run's own
// sink, task logs disabled or not, with ids and numbers and no authored
// name; a sink that exports no logs gets none.
func TestUsageObserver_LogsUsageRecordsOnTheRunsSink(t *testing.T) {
	exporter := &recordingExporter{}
	p := newTestProviders(t, exporter, sdkmetric.NewManualReader())
	obs := NewUsageObserver(nil, sinksOf(p), nil).WithLogInterval(30 * time.Second)
	in := &orkstackv1.ExecuteTaskInput{RunId: "r1", TaskId: "t1", Namespace: "ns1", Pool: "gpu", OrkflowName: "secret-flow",
		Each:      &orkstackv1.EachContext{Index: 3},
		Telemetry: &orkstackv1.TelemetrySelection{TaskLogs: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED}}
	_, observed := obs.Begin(context.Background(), in, 2)
	a := observed.(*usageAttempt)
	if a.log == nil {
		t.Fatal("no usage log for a sink that exports logs")
	}
	cores := 1.5
	a.log.UsageSample(orker.UsageSample{At: time.Unix(100, 0), CPUSeconds: 3, CPUCores: &cores, Memory: 1 << 20, MemoryPeak: 2 << 20, Limit: 1 << 30})
	observed.End(orker.AttemptOutcome{
		Reserved: &orkstackv1.ReservedResources{Cpu: 2, MemoryBytes: 1 << 30},
		Usage: &orkstackv1.MeasuredUsage{Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2,
			CpuSeconds: 4, MemoryPeakBytes: 2 << 20, MemoryPeakExact: true, MemoryLimitBytes: 1 << 30, Samples: 5}})
	if len(exporter.records) != 2 {
		t.Fatalf("%d records, want a sample and a summary", len(exporter.records))
	}
	sample, summary := recordAttrs(exporter.records[0]), recordAttrs(exporter.records[1])
	for _, attrs := range []map[string]string{sample, summary} {
		if attrs[AttrRecord] != RecordUsage || attrs[AttrRunID] != "r1" || attrs[AttrTaskID] != "t1" ||
			attrs[AttrAttempt] != "2" || attrs[AttrItem] != "3" || attrs[AttrNamespace] != "ns1" || attrs[AttrPool] != "gpu" {
			t.Fatalf("record attributes %v", attrs)
		}
		if _, named := attrs[AttrOrkflowName]; named {
			t.Fatalf("a usage record carries an authored name: %v", attrs)
		}
	}
	if sample[AttrUsageKind] != "sample" || sample[AttrUsageCPUCores] != "1.5" || sample[AttrUsageMemory] != "1048576" {
		t.Fatalf("sample = %v", sample)
	}
	if summary[AttrUsageKind] != "summary" || summary[AttrUsageMethod] != "CGROUP_V2" || summary[AttrUsageCPUSeconds] != "4" ||
		summary[AttrUsageReservedCPU] != "2" || summary[AttrUsageSamples] != "5" {
		t.Fatalf("summary = %v", summary)
	}
	if !exporter.records[0].Timestamp().Equal(time.Unix(100, 0)) {
		t.Fatalf("the sample's time is %v, want its reading's", exporter.records[0].Timestamp())
	}

	// Unmeasured: the summary says why, with no figures.
	_, off := obs.Begin(context.Background(), in, 3)
	off.End(orker.AttemptOutcome{Usage: &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED, UnmeasuredReason: orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NO_CGROUP}})
	last := recordAttrs(exporter.records[len(exporter.records)-1])
	if last[AttrUnmeasuredReason] != "NO_CGROUP" || last[AttrUsageCPUSeconds] != "" {
		t.Fatalf("unmeasured summary = %v", last)
	}

	// A sink that exports no logs gets no usage log.
	noLogs := NewUsageObserver(nil, sinksOf(&Providers{Meter: sdkmetric.NewMeterProvider()}), nil)
	_, quiet := noLogs.Begin(context.Background(), in, 1)
	if quiet.(*usageAttempt).log != nil {
		t.Fatal("a usage log for a sink that exports no logs")
	}
}

// TestUsageGauges_ReadTheRunningTotalsAtCollection: the gauges read the
// registry when collected, never a stored copy.
func TestUsageGauges_ReadTheRunningTotalsAtCollection(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	watch, err := NewUsageGauges(&Providers{Meter: mp}, nil)
	if err != nil {
		t.Fatal(err)
	}
	totals := orker.UsageTotals{Memory: 300, CPUCores: 1.25, Measured: 2}
	watch(func() orker.UsageTotals { return totals })
	read := func() map[string]float64 {
		out := map[string]float64{}
		for name, points := range metricPoints(t, reader) {
			if len(points) == 1 {
				out[name] = points[0].value
			}
		}
		return out
	}
	if got := read(); got["ork.orker.containers.memory"] != 300 || got["ork.orker.containers.cpu"] != 1.25 || got["ork.orker.containers.measured"] != 2 {
		t.Fatalf("gauges = %v", got)
	}
	totals = orker.UsageTotals{}
	if got := read(); got["ork.orker.containers.memory"] != 0 || got["ork.orker.containers.measured"] != 0 {
		t.Fatalf("gauges after the containers ended = %v", got)
	}
}

// panickingLogger is a log.Logger whose Emit panics, as a broken processor
// would.
type panickingLogger struct{ otellog.Logger }

func (panickingLogger) Emit(context.Context, otellog.Record) { panic("a broken processor") }

// TestUsageAttempt_APanickingSummaryIsContainedAndSaid: a summary record
// that panics is said on the Orker's log and costs neither the attempt's
// metrics nor its End being passed on.
func TestUsageAttempt_APanickingSummaryIsContainedAndSaid(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&out, nil))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	inst, err := NewUsageInstruments(&Providers{Meter: mp})
	if err != nil {
		t.Fatal(err)
	}
	inner := &recordingObserver{}
	a := &usageAttempt{
		inner: inner, logger: logger, inst: inst, ctx: context.Background(), started: time.Now(),
		attrs: []attribute.KeyValue{attribute.String(AttrPool, "main"), attribute.String(AttrNamespace, "default")},
		log:   &usageLog{ctx: context.Background(), logger: panickingLogger{}},
	}
	a.End(orker.AttemptOutcome{Usage: &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2, CpuSeconds: 2}})
	if inner.ends != 1 {
		t.Fatalf("inner End called %d times, want once", inner.ends)
	}
	if got := usagePoints(t, reader)["ork.orker.task.cpu.time"]; len(got) != 1 || got[0].sum != 2 {
		t.Fatalf("cpu.time = %+v, want the attempt's metrics recorded despite the summary's panic", got)
	}
	if !strings.Contains(out.String(), "usage summary record could not be recorded") || !strings.Contains(out.String(), "a broken processor") {
		t.Fatalf("the Orker's log said %q, want the summary's panic and its cause", out.String())
	}
}

// failingRegistration is a meter provider whose meters refuse callbacks.
type failingRegistration struct{ metric.MeterProvider }

func (f failingRegistration) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	return refusingMeter{f.MeterProvider.Meter(name, opts...)}
}

type refusingMeter struct{ metric.Meter }

func (refusingMeter) RegisterCallback(metric.Callback, ...metric.Observable) (metric.Registration, error) {
	return nil, errors.New("the meter refused the callback")
}

// TestUsageGauges_ARegistrationThatFailsIsSaid: gauges that cannot be
// registered are said on the Orker's own log, naming them and why.
func TestUsageGauges_ARegistrationThatFailsIsSaid(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&out, nil))
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	watch, err := NewUsageGauges(&Providers{Meter: failingRegistration{mp}}, logger)
	if err != nil {
		t.Fatal(err)
	}
	watch(func() orker.UsageTotals { return orker.UsageTotals{} })
	if !strings.Contains(out.String(), "ork.orker.containers.* are not reported") || !strings.Contains(out.String(), "the meter refused the callback") {
		t.Fatalf("the Orker's log said %q, want the gauges' registration failure and its cause", out.String())
	}
}
