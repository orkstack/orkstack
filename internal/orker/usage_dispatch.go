package orker

import (
	"context"
	"fmt"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// UsageSampleIntervalVar is orker.usage_sample_interval's variable.
const UsageSampleIntervalVar = "ORK_ORKER_USAGE_SAMPLE_INTERVAL"

// DefaultUsageSampleInterval is how often an attempt's usage sampler reads
// its container's cgroup after the ramp (orker.usage_sample_interval).
const DefaultUsageSampleInterval = 5 * time.Second

// MaxUsageSampleInterval bounds orker.usage_sample_interval: a slower read
// leaves the live figures stale for most of a task.
const MaxUsageSampleInterval = time.Hour

// UsageLogIntervalVar is orker.usage_log_interval's variable.
const UsageLogIntervalVar = "ORK_ORKER_USAGE_LOG_INTERVAL"

// DefaultUsageLogInterval is how often a running attempt's usage is logged
// as an ork.record=usage record (orker.usage_log_interval).
const DefaultUsageLogInterval = 30 * time.Second

// ParseUsageLogInterval reads orker.usage_log_interval: unset is the default,
// 0s logs only each attempt's summary, and anything else must be a duration
// from 1s to 1h, refused at boot naming the bound rather than clamped.
func ParseUsageLogInterval(raw string) (time.Duration, error) {
	if raw == "" {
		return DefaultUsageLogInterval, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d != 0 && (d < time.Second || d > time.Hour) {
		return DefaultUsageLogInterval, fmt.Errorf("%s=%q is not 0s (each attempt's summary only) or a duration from 1s to 1h",
			UsageLogIntervalVar, raw)
	}
	return d, nil
}

// ParseUsageSampleInterval reads orker.usage_sample_interval: unset is the
// default, 0s disables measuring, and anything else must be a duration from
// 1s to 1h, refused at boot naming the bound rather than clamped.
func ParseUsageSampleInterval(raw string) (time.Duration, error) {
	if raw == "" {
		return DefaultUsageSampleInterval, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d != 0 && (d < time.Second || d > MaxUsageSampleInterval) {
		return DefaultUsageSampleInterval, fmt.Errorf("%s=%q is not 0s (no measuring) or a duration from 1s to 1h",
			UsageSampleIntervalVar, raw)
	}
	return d, nil
}

// usageLocating is a runtime that can find a container's cgroup on this host
// (DockerRuntime). A runtime that cannot, a test's stand-in included, leaves
// every attempt's usage CGROUP_NOT_VISIBLE.
type usageLocating interface {
	usageLocator(id ContainerID, host cgroupHost) func(context.Context) (cgroupReader, usageMethod, error)
}

// The production runtime measures; a change that loses that fails to build.
var _ usageLocating = (*DockerRuntime)(nil)

// startUsage starts the usage sampler for the container id just started. It
// runs on a context cancellation never reaches, so a cancelled attempt's
// container is still read after its kill; finish ends it.
func (d *OCIDispatcher) startUsage(ctx context.Context, in *orkstackv1.ExecuteTaskInput, id ContainerID) *usageSampler {
	locate := func(context.Context) (cgroupReader, usageMethod, error) {
		return nil, usageUnmeasured, fmt.Errorf("%w: the runtime %T cannot locate a container's cgroup", errCgroupNotFound, d.Runtime)
	}
	if runtime, ok := d.Runtime.(usageLocating); ok {
		locate = runtime.usageLocator(id, hostCgroups())
	}
	deps := samplerDeps{
		locate:   locate,
		interval: d.UsageSampleInterval,
		failed:   d.UsageSamplerFailed,
		warn: func(msg string, cause any) {
			d.logger().Warn(msg, "run_id", in.GetRunId(), "task_id", in.GetTaskId(),
				"container_id", string(id), "cause", fmt.Sprint(cause))
		},
	}
	if log, ok := ctx.Value(usageLogKey{}).(usageLog); ok {
		deps.sample, deps.logEvery = log.sink.UsageSample, log.every
	}
	registry, _ := ctx.Value(usageRegistryKey{}).(*UsageRegistry)
	var s *usageSampler
	if registry != nil {
		deps.ended = func() { registry.remove(s) }
	}
	s = startUsageSampler(context.WithoutCancel(ctx), deps)
	registry.add(s)
	return s
}

// UsageSampleSink hears an attempt's periodic usage readings
// (orker.usage_log_interval): its usage log records. It is called from the
// attempt's sampler and must not block.
type UsageSampleSink interface {
	UsageSample(UsageSample)
}

type usageLogKey struct{}

type usageLog struct {
	sink  UsageSampleSink
	every time.Duration
}

// WithUsageLog has the attempt that runs under ctx hand sink a reading every
// every; a zero every hands none (the attempt's summary is its outcome's).
// The PhaseObserver sets it on the context its Begin returns, which the
// whole attempt runs under.
func WithUsageLog(ctx context.Context, sink UsageSampleSink, every time.Duration) context.Context {
	if sink == nil || every <= 0 {
		return ctx
	}
	return context.WithValue(ctx, usageLogKey{}, usageLog{sink: sink, every: every})
}

type usageRegistryKey struct{}

// UsageTotals is what every running attempt's container uses now, summed
// over their latest readings: the containers.memory and .cpu gauges.
type UsageTotals struct {
	// Memory is the sum of the measured containers' current memory.
	Memory int64
	// CPUCores is the sum of their CPU rates, over those with a rate.
	CPUCores float64
	// Measured is how many running containers the sums cover.
	Measured int
}

// UsageRegistry is this process's running samplers, read for the gauges:
// process-local, never stored, gone with the process.
type UsageRegistry struct {
	mu   sync.Mutex
	live map[*usageSampler]struct{}
}

// NewUsageRegistry is an empty registry.
func NewUsageRegistry() *UsageRegistry { return &UsageRegistry{live: map[*usageSampler]struct{}{}} }

func (r *UsageRegistry) add(s *usageSampler) {
	if r == nil || s == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.live[s] = struct{}{}
}

func (r *UsageRegistry) remove(s *usageSampler) {
	if r == nil || s == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.live, s)
}

// Totals sums the running samplers' latest readings. A sampler with no
// reading yet, or none it can take, adds nothing.
func (r *UsageRegistry) Totals() UsageTotals {
	var t UsageTotals
	if r == nil {
		return t
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := range r.live {
		st := s.snapshot()
		if st.reason != reasonNone || st.samples == 0 {
			continue
		}
		t.Measured++
		t.Memory += st.reading.memory
		if st.hasRate {
			t.CPUCores += st.cores
		}
	}
	return t
}

// withUsageRegistry has every attempt that runs under ctx count in r.
func withUsageRegistry(ctx context.Context, r *UsageRegistry) context.Context {
	if r == nil {
		return ctx
	}
	return context.WithValue(ctx, usageRegistryKey{}, r)
}

// unmeasuredSampler is a sampler that measured nothing, for reason: every
// beat and the record then say why.
func unmeasuredSampler(reason usageReason) *usageSampler {
	s := &usageSampler{done: make(chan struct{}), cancel: func() {}, deps: samplerDeps{now: time.Now}}
	s.state.Store(&usageState{reason: reason})
	close(s.done)
	return s
}

// measuredOf is the attempt's final usage as its heartbeater holds it: the
// sampler's result once its container ran, NOT_RUN when no container did
// (an adopted commit, a memo hit, a failure before the start). finish is
// idempotent, so this returns what the container's own end recorded.
func measuredOf(beat *Heartbeater) measuredUsage {
	if beat == nil {
		return measuredUsage{method: usageUnmeasured, reason: reasonNotRun}
	}
	s := beat.usage.Load()
	if s == nil {
		return measuredUsage{method: usageUnmeasured, reason: reasonNotRun}
	}
	return s.finish(false)
}
