package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
)

// Measured usage's instruments (E94, prod-kit design §3.5): what each
// attempt's container used, as its cgroup reported it, and what was
// reserved for it, recorded once per attempt when it ends. Every series
// carries the attempt's pool and namespace (Cardinality.TaskAttrs). Nothing
// here decides anything: per-attempt truth is the attempt record.

// AttrUnmeasuredReason labels ork.orker.task.usage.unmeasured.
const AttrUnmeasuredReason = "ork.unmeasured_reason"

// The operator twins of the usage counters an operator rule reads (prod-kit
// design §2.6.0): recorded for every namespace on the default sink, labelled
// with the pool and the reason only.
const (
	DeploymentOOMKillsMetric   = "ork.deployment.orker.task.oom_kills"
	DeploymentUnmeasuredMetric = "ork.deployment.orker.task.usage.unmeasured"
)

// usageTwins are the default sink's operator twins.
type usageTwins struct {
	oomKills   metric.Int64Counter
	unmeasured metric.Int64Counter
}

func newUsageTwins(p *Providers) (*usageTwins, error) {
	meter := meterFor(p)
	oom, err := meter.Int64Counter(DeploymentOOMKillsMetric,
		metric.WithDescription("Attempts in every namespace whose container was stopped at its memory limit, by pool."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	unmeasured, err := meter.Int64Counter(DeploymentUnmeasuredMetric,
		metric.WithDescription("Attempts in every namespace whose container's usage was not measured, by pool and why."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	return &usageTwins{oomKills: oom, unmeasured: unmeasured}, nil
}

// record records one attempt's twins.
func (t *usageTwins) record(ctx context.Context, pool string, m *orkstackv1.MeasuredUsage) {
	if t == nil || m == nil {
		return
	}
	poolAttr := attribute.String(AttrPool, pool)
	if m.GetOomKilled() {
		t.oomKills.Add(ctx, 1, metric.WithAttributes(poolAttr))
	}
	if m.GetMethod() == orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED ||
		m.GetMethod() == orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNSPECIFIED {
		t.unmeasured.Add(ctx, 1, metric.WithAttributes(poolAttr,
			attribute.String(AttrUnmeasuredReason, unmeasuredLabel(m.GetUnmeasuredReason()))))
	}
}

// UsageInstruments are the measured-usage instruments of one sink.
type UsageInstruments struct {
	cpuTime         metric.Float64Histogram
	memoryPeak      metric.Int64Histogram
	cpuUtilization  metric.Float64Histogram
	memUtilization  metric.Float64Histogram
	reservedCPUTime metric.Float64Counter
	reservedMemTime metric.Float64Counter
	oomKills        metric.Int64Counter
	unmeasured      metric.Int64Counter
	samplerFailures metric.Int64Counter
}

// NewUsageInstruments builds the instruments from p's meter provider; on nil
// or disabled Providers they record nothing.
func NewUsageInstruments(p *Providers) (*UsageInstruments, error) {
	meter := meterFor(p)
	var u UsageInstruments
	var err error
	if u.cpuTime, err = meter.Float64Histogram("ork.orker.task.cpu.time",
		metric.WithDescription("CPU time one attempt's container used, as its cgroup reported it at the last reading."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if u.memoryPeak, err = meter.Int64Histogram("ork.orker.task.memory.peak",
		metric.WithDescription("Peak memory of one attempt's container, where the kernel's own peak was read."),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}
	if u.cpuUtilization, err = meter.Float64Histogram("ork.orker.task.cpu.utilization",
		metric.WithDescription("CPU one attempt's container used over the cpu reserved for it while it ran."),
		metric.WithUnit("1")); err != nil {
		return nil, err
	}
	if u.memUtilization, err = meter.Float64Histogram("ork.orker.task.memory.utilization",
		metric.WithDescription("Peak memory of one attempt's container over its memory limit, where the kernel's own peak was read."),
		metric.WithUnit("1")); err != nil {
		return nil, err
	}
	if u.reservedCPUTime, err = meter.Float64Counter("ork.orker.task.reserved.cpu_time",
		metric.WithDescription("Reserved cpu × the time each attempt held it: cpu-seconds reserved, not used."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if u.reservedMemTime, err = meter.Float64Counter("ork.orker.task.reserved.memory_time",
		metric.WithDescription("Reserved memory × the time each attempt held it: GiB-seconds reserved, not used."),
		metric.WithUnit("GiBy.s")); err != nil {
		return nil, err
	}
	if u.oomKills, err = meter.Int64Counter("ork.orker.task.oom_kills",
		metric.WithDescription("Attempts whose container was stopped at its memory limit."),
		metric.WithUnit("{attempt}")); err != nil {
		return nil, err
	}
	if u.unmeasured, err = meter.Int64Counter("ork.orker.task.usage.unmeasured",
		metric.WithDescription("Attempts whose container's usage was not measured, by why."),
		metric.WithUnit("{attempt}")); err != nil {
		return nil, err
	}
	if u.samplerFailures, err = meter.Int64Counter("ork.orker.usage.sampler_failures",
		metric.WithDescription("Usage samplers that panicked and were recovered; the attempt's usage is READ_FAILED."),
		metric.WithUnit("{sampler}")); err != nil {
		return nil, err
	}
	return &u, nil
}

// SamplerFailed counts one recovered sampler panic: the hook
// OCIDispatcher.UsageSamplerFailed takes. It is a process-level series,
// identified by the Orker's own telemetry resource.
func (u *UsageInstruments) SamplerFailed() {
	if u == nil {
		return
	}
	u.samplerFailures.Add(context.Background(), 1)
}

// record records one ended attempt's usage. held is how long it held its
// reservation.
func (u *UsageInstruments) record(ctx context.Context, attrs []attribute.KeyValue,
	m *orkstackv1.MeasuredUsage, reserved *orkstackv1.ReservedResources, held time.Duration,
) {
	with := metric.WithAttributes(attrs...)
	if reserved != nil && held > 0 {
		seconds := held.Seconds()
		if reserved.GetCpu() > 0 {
			u.reservedCPUTime.Add(ctx, reserved.GetCpu()*seconds, with)
		}
		if reserved.GetMemoryBytes() > 0 {
			u.reservedMemTime.Add(ctx, float64(reserved.GetMemoryBytes())/(1<<30)*seconds, with)
		}
	}
	if m == nil {
		return
	}
	if m.GetOomKilled() {
		u.oomKills.Add(ctx, 1, with)
	}
	switch m.GetMethod() {
	case orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2, orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V1:
	default:
		labelled := append(attrs[:len(attrs):len(attrs)], attribute.String(AttrUnmeasuredReason, unmeasuredLabel(m.GetUnmeasuredReason())))
		u.unmeasured.Add(ctx, 1, metric.WithAttributes(labelled...))
		return
	}
	cpu := m.GetCpuSeconds()
	if math.IsNaN(cpu) || math.IsInf(cpu, 0) || cpu < 0 {
		return
	}
	u.cpuTime.Record(ctx, cpu, with)
	if ran := runTime(m); ran > 0 && reserved.GetCpu() > 0 {
		u.cpuUtilization.Record(ctx, cpu/(reserved.GetCpu()*ran.Seconds()), with)
	}
	if m.GetMemoryPeakExact() && m.GetMemoryPeakBytes() > 0 {
		u.memoryPeak.Record(ctx, m.GetMemoryPeakBytes(), with)
		if limit := m.GetMemoryLimitBytes(); limit > 0 {
			u.memUtilization.Record(ctx, float64(m.GetMemoryPeakBytes())/float64(limit), with)
		}
	}
}

// runTime is how long the container ran, from its record.
func runTime(m *orkstackv1.MeasuredUsage) time.Duration {
	started, exited := m.GetContainerStartedAt(), m.GetContainerExitedAt()
	if started == nil || exited == nil {
		return 0
	}
	return exited.AsTime().Sub(started.AsTime())
}

// unmeasuredLabel is the reason's enum name without its prefix: a bounded
// set, the words the attempt record and orkctl carry. A number this build
// does not name is UNSPECIFIED, so the label set stays bounded.
func unmeasuredLabel(r orkstackv1.UnmeasuredReason) string {
	if _, known := orkstackv1.UnmeasuredReason_name[int32(r)]; !known {
		return "UNSPECIFIED"
	}
	return strings.TrimPrefix(r.String(), "UNMEASURED_REASON_")
}

// UsageObserver records measured usage beside another PhaseObserver: it
// passes every call through and, at each attempt's End, records the
// attempt's usage on the sink the attempt's telemetry selection names.
type UsageObserver struct {
	inner     orker.PhaseObserver
	sinks     *Sinks
	logger    *slog.Logger
	routes    sync.Map // *Providers → *UsageInstruments
	twinsOnce sync.Once
	twins     *usageTwins
	// logEvery is orker.usage_log_interval: how often a running attempt's
	// usage is logged as an ork.record=usage record; 0 logs only each
	// attempt's summary.
	logEvery time.Duration
}

// AttrRecord names what an Orker log record is; usage records carry
// RecordUsage.
const (
	AttrRecord  = "ork.record"
	RecordUsage = "usage"
)

// WithLogInterval sets how often a running attempt's usage is logged
// (orker.usage_log_interval); 0 logs only each attempt's summary.
func (o *UsageObserver) WithLogInterval(every time.Duration) *UsageObserver {
	o.logEvery = every
	return o
}

var _ orker.PhaseObserver = (*UsageObserver)(nil)

// NewUsageObserver wraps inner (nil observes nothing else).
func NewUsageObserver(inner orker.PhaseObserver, s *Sinks, logger *slog.Logger) *UsageObserver {
	return &UsageObserver{inner: inner, sinks: s, logger: logger}
}

func (o *UsageObserver) instrumentsFor(p *Providers) *UsageInstruments {
	if v, ok := o.routes.Load(p); ok {
		return v.(*UsageInstruments)
	}
	inst, err := NewUsageInstruments(p)
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("telemetry: failed to build the usage instruments for a sink; usage metrics disabled for it", "error", err)
		}
		inst = nil
	}
	actual, _ := o.routes.LoadOrStore(p, inst)
	return actual.(*UsageInstruments)
}

// twinsFor builds the default sink's twins once.
func (o *UsageObserver) twinsFor() *usageTwins {
	o.twinsOnce.Do(func() {
		twins, err := newUsageTwins(o.sinks.Default())
		if err != nil {
			if o.logger != nil {
				o.logger.Warn("telemetry: failed to build the usage operator twins; they record nothing", "error", err)
			}
			return
		}
		o.twins = twins
	})
	return o.twins
}

// Begin passes through, and returns an observer that records usage at End.
func (o *UsageObserver) Begin(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32) (context.Context, orker.AttemptObserver) {
	var inner orker.AttemptObserver
	if o.inner != nil {
		ctx, inner = o.inner.Begin(ctx, in, attempt)
	}
	// A sink this process does not declare records no usage: the attempt's
	// own observation already counts its records dropped (ForNamespaced).
	p, ok := o.sinks.ForNamespaced(in.GetTelemetry().GetSink())
	var inst *UsageInstruments
	if ok {
		inst = o.instrumentsFor(p)
	}
	a := &usageAttempt{
		inner: inner, logger: o.logger, inst: inst, twins: o.twinsFor(), pool: in.GetPool(),
		attrs: p.Cardinality().TaskAttrs(in), ctx: ctx, started: time.Now(),
	}
	// Usage records are run-bound: they go to the run's own sink whenever it
	// exports logs, whether or not the run exports its task logs.
	if ok && p != nil && p.Config().LogsEnabled() {
		a.log = newUsageLog(ctx, p, in, attempt)
		ctx = orker.WithUsageLog(ctx, a.log, o.logEvery)
	}
	return ctx, a
}

// usageAttempt is one attempt's usage observation.
type usageAttempt struct {
	inner   orker.AttemptObserver
	logger  *slog.Logger
	log     *usageLog
	inst    *UsageInstruments
	twins   *usageTwins
	pool    string
	attrs   []attribute.KeyValue
	ctx     context.Context
	started time.Time
	once    sync.Once
}

func (a *usageAttempt) Phase(name string) {
	if a.inner != nil {
		a.inner.Phase(name)
	}
}

// End records the attempt's usage once, then passes End through. Recording
// never panics into the attempt: a panic is contained and drops the record.
func (a *usageAttempt) End(outcome orker.AttemptOutcome) {
	a.once.Do(func() {
		// Each record is contained on its own and said, so one that fails
		// never costs another, nor the attempt.
		a.contain("operator twins", func() { a.twins.record(a.ctx, a.pool, outcome.Usage) })
		if a.inst != nil {
			a.contain("instruments", func() {
				a.inst.record(a.ctx, a.attrs, outcome.Usage, outcome.Reserved, time.Since(a.started))
			})
		}
		a.contain("summary record", func() { a.log.summary(outcome.Usage, outcome.Reserved) })
	})
	if a.inner != nil {
		a.inner.End(outcome)
	}
}

// contain runs one of End's records, and says a panic in it rather than
// letting it reach the attempt or another record.
func (a *usageAttempt) contain(what string, record func()) {
	defer func() {
		if v := recover(); v != nil && a.logger != nil {
			a.logger.Warn("telemetry: an attempt's usage "+what+" could not be recorded", "cause", fmt.Sprint(v))
		}
	}()
	record()
}

// usageLog emits one attempt's ork.record=usage records: a sample every log
// interval while it runs (orker.UsageSampleSink), and one summary when it
// ends. Numbers and ids only: no authored name rides a usage record.
type usageLog struct {
	ctx    context.Context
	logger log.Logger
	attrs  []attribute.KeyValue
}

var _ orker.UsageSampleSink = (*usageLog)(nil)

func newUsageLog(ctx context.Context, p *Providers, in *orkstackv1.ExecuteTaskInput, attempt int32) *usageLog {
	attrs := []attribute.KeyValue{
		attribute.String(AttrRecord, RecordUsage),
		attribute.String(AttrRunID, in.GetRunId()),
		attribute.String(AttrTaskID, in.GetTaskId()),
		attribute.Int64(AttrAttempt, int64(attempt)),
		attribute.String(AttrNamespace, in.GetNamespace()),
		attribute.String(AttrPool, in.GetPool()),
	}
	if each := in.GetEach(); each != nil {
		attrs = append(attrs, attribute.Int64(AttrItem, int64(each.GetIndex())))
	}
	return &usageLog{ctx: context.WithoutCancel(ctx), logger: p.Logs.Logger(ScopeName), attrs: attrs}
}

// Usage records' own attributes.
const (
	AttrUsageKind           = "ork.usage.kind" // "sample" or "summary"
	AttrUsageCPUSeconds     = "ork.usage.cpu_seconds"
	AttrUsageCPUCores       = "ork.usage.cpu_cores"
	AttrUsageMemory         = "ork.usage.memory_bytes"
	AttrUsageMemoryPeak     = "ork.usage.memory_peak_bytes"
	AttrUsagePeakExact      = "ork.usage.memory_peak_exact"
	AttrUsageLimit          = "ork.usage.memory_limit_bytes"
	AttrUsageMethod         = "ork.usage.method"
	AttrUsageOOMKilled      = "ork.usage.oom_killed"
	AttrUsageSamples        = "ork.usage.samples"
	AttrUsageReservedCPU    = "ork.reserved.cpu"
	AttrUsageReservedMemory = "ork.reserved.memory_bytes"
)

// UsageSample emits one running reading. It is called from the attempt's
// sampler and never blocks beyond the SDK's own enqueue.
func (l *usageLog) UsageSample(s orker.UsageSample) {
	if l == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String(AttrUsageKind, "sample"),
		attribute.Float64(AttrUsageCPUSeconds, s.CPUSeconds),
		attribute.Int64(AttrUsageMemory, s.Memory),
		attribute.Int64(AttrUsageMemoryPeak, s.MemoryPeak),
		attribute.Bool(AttrUsagePeakExact, s.PeakExact),
		attribute.Int64(AttrUsageLimit, s.Limit),
	}
	if s.CPUCores != nil {
		attrs = append(attrs, attribute.Float64(AttrUsageCPUCores, *s.CPUCores))
	}
	l.emit(s.At, "usage sample", attrs)
}

// summary emits the attempt's measured usage once, as its record carries it.
func (l *usageLog) summary(m *orkstackv1.MeasuredUsage, reserved *orkstackv1.ReservedResources) {
	if l == nil || m == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String(AttrUsageKind, "summary"),
		attribute.String(AttrUsageMethod, strings.TrimPrefix(m.GetMethod().String(), "MEASUREMENT_METHOD_")),
		attribute.Bool(AttrUsageOOMKilled, m.GetOomKilled()),
	}
	if m.GetMethod() == orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED || m.GetMethod() == orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNSPECIFIED {
		attrs = append(attrs, attribute.String(AttrUnmeasuredReason, unmeasuredLabel(m.GetUnmeasuredReason())))
	} else {
		attrs = append(attrs,
			attribute.Float64(AttrUsageCPUSeconds, m.GetCpuSeconds()),
			attribute.Int64(AttrUsageMemoryPeak, m.GetMemoryPeakBytes()),
			attribute.Bool(AttrUsagePeakExact, m.GetMemoryPeakExact()),
			attribute.Int64(AttrUsageLimit, m.GetMemoryLimitBytes()),
			attribute.Int64(AttrUsageSamples, int64(m.GetSamples())),
		)
	}
	if reserved != nil {
		attrs = append(attrs,
			attribute.Float64(AttrUsageReservedCPU, reserved.GetCpu()),
			attribute.Int64(AttrUsageReservedMemory, reserved.GetMemoryBytes()))
	}
	l.emit(time.Now(), "usage summary", attrs)
}

// emit hands one record to the sink's logger. It does not contain a panic
// itself: a sample's is contained by the attempt's sampler, which says so
// and logs no more samples, and the summary's by End.
func (l *usageLog) emit(at time.Time, body string, attrs []attribute.KeyValue) {
	var rec log.Record
	rec.SetTimestamp(at)
	rec.SetObservedTimestamp(time.Now())
	rec.SetSeverity(log.SeverityInfo)
	rec.SetSeverityText("INFO")
	rec.SetBody(attribute.StringValue(body))
	rec.AddAttributes(l.attrs...)
	rec.AddAttributes(attrs...)
	l.logger.Emit(l.ctx, rec)
}

// NewUsageGauges registers ork.orker.containers.memory and .cpu on p, the
// default sink: what every attempt running on this Orker uses now, summed
// over their latest readings, and how many containers that covers. It
// returns the hook orker.Options.UsageGauges takes.
func NewUsageGauges(p *Providers, logger *slog.Logger) (func(read func() orker.UsageTotals), error) {
	meter := meterFor(p)
	memory, err := meter.Int64ObservableGauge("ork.orker.containers.memory",
		metric.WithDescription("Memory every running attempt's container on this Orker uses now, summed over their latest readings."),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	cpu, err := meter.Float64ObservableGauge("ork.orker.containers.cpu",
		metric.WithDescription("CPU every running attempt's container on this Orker uses now, in cores, summed over their latest rates."),
		metric.WithUnit("{cpu}"))
	if err != nil {
		return nil, err
	}
	measured, err := meter.Int64ObservableGauge("ork.orker.containers.measured",
		metric.WithDescription("Running attempts' containers the containers.memory and .cpu sums cover."),
		metric.WithUnit("{container}"))
	if err != nil {
		return nil, err
	}
	return func(read func() orker.UsageTotals) {
		_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			t := read()
			o.ObserveInt64(memory, t.Memory)
			o.ObserveFloat64(cpu, t.CPUCores)
			o.ObserveInt64(measured, int64(t.Measured))
			return nil
		}, memory, cpu, measured)
		if err != nil && logger != nil {
			logger.Warn("telemetry: the usage gauges could not be registered; ork.orker.containers.* are not reported", "error", err)
		}
	}, nil
}
