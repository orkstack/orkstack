package orker

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// sampledAt is a sampler holding st, read on a clock fixed at now.
func sampledAt(st usageState, interval time.Duration, now time.Time) *usageSampler {
	s := &usageSampler{deps: samplerDeps{interval: interval, now: func() time.Time { return now }}}
	s.state.Store(&st)
	return s
}

func decodeBeatUsage(t *testing.T, raw jsontext.Value) task.HeartbeatUsage {
	t.Helper()
	var u task.HeartbeatUsage
	if err := json.Unmarshal(raw, &u); err != nil {
		t.Fatalf("decode the beat's usage %s: %v", raw, err)
	}
	return u
}

// TestHeartbeater_CarriesUsageOnceItsContainerStarted: no beat carries usage
// before the container started; from then each carries the sampler's latest
// reading, its age on the sampler's clock, the sampler's interval and the
// loop's own tick, or why there is no reading.
func TestHeartbeater_CarriesUsageOnceItsContainerStarted(t *testing.T) {
	h := &Heartbeater{interval: 7 * time.Second}
	if raw := h.progress().Usage; raw != nil {
		t.Fatalf("usage before the container started = %s, want none", raw)
	}

	now := time.Unix(1000, 0)
	cases := []struct {
		name string
		st   usageState
		want task.HeartbeatUsage
	}{
		{"no reading yet", usageState{}, task.HeartbeatUsage{Unavailable: "NO_SAMPLE_YET", Interval: 5000, Tick: 7000}},
		{"not visible", usageState{reason: reasonCgroupNotVisible}, task.HeartbeatUsage{Unavailable: "CGROUP_NOT_VISIBLE", Interval: 5000, Tick: 7000}},
		{"disabled", usageState{reason: reasonDisabled}, task.HeartbeatUsage{Unavailable: "DISABLED", Interval: 5000, Tick: 7000}},
		{"start failed", usageState{reason: reasonStartFailed}, task.HeartbeatUsage{Unavailable: "START_FAILED", Interval: 5000, Tick: 7000}},
		{
			// One reading has no rate yet: cpu_cores is absent, never 0.
			"one reading",
			usageState{method: usageCgroupV2, reading: cgroupReading{cpuSeconds: 0.2, memory: 64}, samples: 1, last: now},
			task.HeartbeatUsage{CPUSeconds: 0.2, Memory: 64, Interval: 5000, Tick: 7000},
		},
		{
			"a reading",
			usageState{
				method:  usageCgroupV2,
				reading: cgroupReading{cpuSeconds: 3.5, memory: 100, memoryPeak: 150, peakRead: true, limit: 1 << 20},
				samples: 4, cores: 0.75, hasRate: true, last: now.Add(-1500 * time.Millisecond),
			},
			task.HeartbeatUsage{CPUCores: new(0.75), CPUSeconds: 3.5, Memory: 100, MemoryPeak: 150, PeakExact: true,
				Limit: 1 << 20, AgeMillis: 1500, Interval: 5000, Tick: 7000},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h.setUsage(sampledAt(tc.st, 5*time.Second, now))
			got := decodeBeatUsage(t, h.progress().Usage)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("usage = %+v, want %+v", got, tc.want)
			}
		})
	}

	// An unconfigured loop's tick is the default.
	unset := &Heartbeater{}
	unset.setUsage(sampledAt(usageState{}, 5*time.Second, now))
	if got := decodeBeatUsage(t, unset.progress().Usage).Tick; got != DefaultHeartbeatInterval.Milliseconds() {
		t.Fatalf("tick_ms = %d, want the default %d", got, DefaultHeartbeatInterval.Milliseconds())
	}
}

// TestHeartbeatUsage_StaysWithinItsBound: the widest usage a beat can carry
// encodes within HeartbeatUsageMaxBytes, so it adds a small fixed amount to
// every beat in history. The ceilings are physical, not the types': the
// reader refuses negatives, a cgroup's memory stays below 8 PiB (2^53), its
// CPU time below 10^9 core-seconds (over 30 core-years), a rate below 10^4
// cores, and the interval and the reading's age below a day.
func TestHeartbeatUsage_StaysWithinItsBound(t *testing.T) {
	h := &Heartbeater{interval: MaxHeartbeatInterval}
	now := time.Unix(1_000_000, 0)
	const memory = int64(1)<<53 - 1
	widest := []usageState{
		{
			method:  usageCgroupV1,
			reading: cgroupReading{cpuSeconds: 999_999_999.987_654, memory: memory, memoryPeak: memory, peakRead: true, limit: memory},
			samples: math.MaxInt32, cores: 9_999.987_654, hasRate: true, last: now.Add(-23 * time.Hour),
		},
		{reason: reasonEndedBeforeFirstSample},
	}
	for _, st := range widest {
		h.setUsage(sampledAt(st, 24*time.Hour-time.Millisecond, now))
		raw := h.progress().Usage
		if raw == nil {
			t.Fatalf("usage left off the beat for %+v", st)
		}
		if len(raw) > task.HeartbeatUsageMaxBytes {
			t.Fatalf("usage %s is %d bytes, over the %d bound", raw, len(raw), task.HeartbeatUsageMaxBytes)
		}
		t.Logf("widest usage: %d bytes: %s", len(raw), raw)
	}
}

// TestSampler_CoresAreTheRateBetweenItsLastTwoReadings: the live CPU figure
// is cores used between the last two readings, not the run's average.
func TestSampler_CoresAreTheRateBetweenItsLastTwoReadings(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{
		{cpuSeconds: 1.0, memory: 10, limit: 100},
		{cpuSeconds: 1.2, memory: 10, limit: 100},
		{cpuSeconds: 2.2, memory: 10, limit: 100},
	}}
	var mu sync.Mutex
	clock := time.Unix(100, 0)
	deps := samplerFor(timers, cg, nil)
	deps.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }
	s := startUsageSampler(context.Background(), deps)
	defer s.finish(false)

	timers.await(t, 1)
	timers.fire(0) // 1.0 at t=100
	timers.await(t, 2)
	if got := s.snapshot().cores; got != 0 {
		t.Fatalf("cores after one reading = %v, want 0", got)
	}
	advance(time.Second)
	timers.fire(1) // 1.2 at t=101
	timers.await(t, 3)
	if got := s.snapshot().cores; math.Abs(got-0.2) > 1e-9 {
		t.Fatalf("cores = %v, want 0.2 (0.2 s over 1 s)", got)
	}
	advance(500 * time.Millisecond)
	timers.fire(2) // 2.2 at t=101.5
	timers.await(t, 4)
	if got := s.snapshot().cores; math.Abs(got-2.0) > 1e-9 {
		t.Fatalf("cores = %v, want 2.0 (1 s over 0.5 s), the last interval's rate", got)
	}
}

// locatingRuntime is a stand-in runtime that can find a container's cgroup.
type locatingRuntime struct {
	*phaseCapturingRuntime
	read cgroupReader
	// located, when set, is what a locate waits for.
	located chan struct{}
}

func (r *locatingRuntime) usageLocator(ContainerID, cgroupHost) func(context.Context) (cgroupReader, usageMethod, error) {
	return func(context.Context) (cgroupReader, usageMethod, error) {
		if r.located != nil {
			<-r.located
		}
		return r.read, usageCgroupV2, nil
	}
}

// TestDispatcher_MeasuresTheContainerItStarted runs a real Execute: the
// attempt's usage is what its container's cgroup read at its end, through a
// runtime that can locate it; CGROUP_NOT_VISIBLE through one that cannot;
// DISABLED at a zero interval; and NOT_RUN where no container started.
func TestDispatcher_MeasuresTheContainerItStarted(t *testing.T) {
	reading := cgroupReading{cpuSeconds: 0.25, memory: 4096, memoryPeak: 8192, peakRead: true, limit: 1 << 30}
	cases := []struct {
		name     string
		runtime  ContainerRuntime
		interval time.Duration
		want     measuredUsage
	}{
		{
			"located",
			&locatingRuntime{phaseCapturingRuntime: &phaseCapturingRuntime{}, read: func(bool) (cgroupReading, error) { return reading, nil }},
			DefaultUsageSampleInterval,
			measuredUsage{method: usageCgroupV2, cpuSeconds: 0.25, memoryPeak: 8192, peakExact: true, limit: 1 << 30},
		},
		{"not visible", &phaseCapturingRuntime{}, DefaultUsageSampleInterval, measuredUsage{method: usageUnmeasured, reason: reasonCgroupNotVisible}},
		{"disabled", &phaseCapturingRuntime{}, 0, measuredUsage{method: usageUnmeasured, reason: reasonDisabled}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			dispatcher := newPhaseDispatcher(t, &phaseCapturingRuntime{}, nil)
			dispatcher.Runtime = tc.runtime
			dispatcher.UsageSampleInterval = tc.interval
			var beat *Heartbeater
			dispatcher.heartbeaters = func(ctx context.Context) *Heartbeater {
				beat = sdkRouteHeartbeater(ctx)
				return beat
			}
			in := baseExportInput("usage-" + strings.ReplaceAll(tc.name, " ", "-"))
			activityFn := func(ctx context.Context) error {
				_, err := dispatcher.Execute(ctx, in)
				return err
			}
			env.RegisterActivity(activityFn)
			if _, err := env.ExecuteActivity(activityFn); err != nil {
				t.Fatalf("ExecuteActivity: %v", err)
			}
			got := measuredOf(beat)
			got.samples, got.firstSample, got.lastSample, got.started, got.exited = 0, time.Time{}, time.Time{}, time.Time{}, time.Time{}
			if got != tc.want {
				t.Fatalf("measured = %+v, want %+v", got, tc.want)
			}
			if tc.want.method != usageUnmeasured {
				final := measuredOf(beat)
				if final.samples < 1 || final.started.IsZero() || final.exited.Before(final.started) {
					t.Fatalf("measured = %+v, want at least the final read and the container's start and end", final)
				}
			}
		})
	}

	if got := measuredOf(nil); got.reason != reasonNotRun {
		t.Fatalf("no heartbeater: measured = %+v, want NOT_RUN", got)
	}
	if got := measuredOf(&Heartbeater{}); got.reason != reasonNotRun {
		t.Fatalf("no container started: measured = %+v, want NOT_RUN", got)
	}
}

// TestRecordMeasured_PutsTheUsageOnTheAttemptRecord: the record carries the
// sampler's final usage field for field, its instants included, and an
// unmeasured attempt carries only its method and reason, never zeros that read
// as a measurement.
func TestRecordMeasured_PutsTheUsageOnTheAttemptRecord(t *testing.T) {
	at := func(s int64) time.Time { return time.Unix(s, 0).UTC() }
	measured := measuredUsage{
		method: usageCgroupV2, cpuSeconds: 1.5, memoryPeak: 2048, peakExact: true, limit: 4096,
		samples: 3, firstSample: at(101), lastSample: at(105), oomKilled: true, started: at(100), exited: at(106),
	}
	record := &orkstackv1.TaskAttempt{}
	recordMeasured(record, measured)
	want := &orkstackv1.MeasuredUsage{
		Method: orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2, CpuSeconds: 1.5,
		MemoryPeakBytes: 2048, MemoryPeakExact: true, MemoryLimitBytes: 4096, Samples: 3,
		FirstSampledAt: timestamppb.New(at(101)), LastSampledAt: timestamppb.New(at(105)),
		ContainerStartedAt: timestamppb.New(at(100)), ContainerExitedAt: timestamppb.New(at(106)),
		OomKilled: true,
	}
	if !proto.Equal(record.GetMeasured(), want) {
		t.Fatalf("measured = %v, want %v", record.GetMeasured(), want)
	}

	reasons := map[usageReason]orkstackv1.UnmeasuredReason{
		reasonCgroupNotVisible:       orkstackv1.UnmeasuredReason_UNMEASURED_REASON_CGROUP_NOT_VISIBLE,
		reasonNoCgroup:               orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NO_CGROUP,
		reasonEndedBeforeFirstSample: orkstackv1.UnmeasuredReason_UNMEASURED_REASON_ENDED_BEFORE_FIRST_SAMPLE,
		reasonReadFailed:             orkstackv1.UnmeasuredReason_UNMEASURED_REASON_READ_FAILED,
		reasonNotRun:                 orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NOT_RUN,
		reasonDisabled:               orkstackv1.UnmeasuredReason_UNMEASURED_REASON_DISABLED,
		reasonStartFailed:            orkstackv1.UnmeasuredReason_UNMEASURED_REASON_START_FAILED,
		reasonCgroupUnverified:       orkstackv1.UnmeasuredReason_UNMEASURED_REASON_CGROUP_UNVERIFIED,
	}
	for reason, wire := range reasons {
		record := &orkstackv1.TaskAttempt{}
		recordMeasured(record, measuredUsage{method: usageUnmeasured, reason: reason, started: at(100), exited: at(101)})
		want := &orkstackv1.MeasuredUsage{
			Method:             orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED,
			UnmeasuredReason:   wire,
			ContainerStartedAt: timestamppb.New(at(100)), ContainerExitedAt: timestamppb.New(at(101)),
		}
		if !proto.Equal(record.GetMeasured(), want) {
			t.Fatalf("reason %v: measured = %v, want %v", reason, record.GetMeasured(), want)
		}
	}
	// NOT_RUN has no container, so no instants.
	notRun := &orkstackv1.TaskAttempt{}
	recordMeasured(notRun, measuredOf(nil))
	if m := notRun.GetMeasured(); m.GetUnmeasuredReason() != orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NOT_RUN ||
		m.GetContainerStartedAt() != nil || m.GetContainerExitedAt() != nil {
		t.Fatalf("not run: measured = %v, want NOT_RUN without instants", m)
	}
	if len(reasons) != int(reasonCgroupUnverified) {
		t.Fatalf("the table names %d reasons; usageReason has %d", len(reasons), reasonCgroupUnverified)
	}
}

// failingStartRuntime is a stand-in whose start fails, as a daemon that cut
// the start off would.
type failingStartRuntime struct{ *phaseCapturingRuntime }

func (failingStartRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	return errors.New("the daemon cut the start off")
}

// TestDispatcher_AFailedStartIsStartFailedNotNotRun: a start that failed may
// still have started the container, so its usage says START_FAILED, never
// that no container ran.
func TestDispatcher_AFailedStartIsStartFailedNotNotRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	dispatcher := newPhaseDispatcher(t, &phaseCapturingRuntime{}, nil)
	dispatcher.Runtime = failingStartRuntime{&phaseCapturingRuntime{}}
	dispatcher.UsageSampleInterval = DefaultUsageSampleInterval
	var beat *Heartbeater
	dispatcher.heartbeaters = func(ctx context.Context) *Heartbeater {
		beat = sdkRouteHeartbeater(ctx)
		return beat
	}
	in := baseExportInput("usage-start-failed")
	activityFn := func(ctx context.Context) error {
		_, err := dispatcher.Execute(ctx, in)
		return err
	}
	env.RegisterActivity(activityFn)
	_, _ = env.ExecuteActivity(activityFn) // the attempt fails; its usage is the point
	if got := measuredOf(beat); got.method != usageUnmeasured || got.reason != reasonStartFailed {
		t.Fatalf("measured = %+v, want START_FAILED", got)
	}
	record := &orkstackv1.TaskAttempt{}
	recordMeasured(record, measuredOf(beat))
	if r := record.GetMeasured().GetUnmeasuredReason(); r != orkstackv1.UnmeasuredReason_UNMEASURED_REASON_START_FAILED {
		t.Fatalf("record reason = %v, want START_FAILED", r)
	}
}

// TestSampler_LocateIsAskedAgainWhenTheEngineDidNotAnswer: an engine that
// does not answer the pid inspect (errCgroupRead) is asked again on the
// ramp, and the attempt is measured once it answers; one that never answers
// is READ_FAILED, said through warn, never CGROUP_NOT_VISIBLE.
func TestSampler_LocateIsAskedAgainWhenTheEngineDidNotAnswer(t *testing.T) {
	t.Run("answers on the second ask", func(t *testing.T) {
		timers := newFakeTimers()
		cg := &scriptedCgroup{readings: []cgroupReading{{cpuSeconds: 1, memory: 1, limit: 10}}}
		deps := samplerFor(timers, cg, nil)
		var asks atomic.Int32
		deps.locate = func(context.Context) (cgroupReader, usageMethod, error) {
			if asks.Add(1) == 1 {
				return nil, usageUnmeasured, fmt.Errorf("%w: inspect timed out", errCgroupRead)
			}
			return cg.read, usageCgroupV2, nil
		}
		s := startUsageSampler(context.Background(), deps)
		timers.await(t, 1)
		timers.fire(0) // the retry's wait
		timers.await(t, 2)
		timers.fire(1) // the first sample
		timers.await(t, 3)
		got := s.finish(false)
		if asks.Load() != 2 || got.method != usageCgroupV2 || got.samples < 1 {
			t.Fatalf("asks %d, finish %+v; want a second ask, then a measurement", asks.Load(), got)
		}
	})
	t.Run("never answers", func(t *testing.T) {
		timers := newFakeTimers()
		deps := samplerFor(timers, &scriptedCgroup{}, fmt.Errorf("%w: inspect timed out", errCgroupRead))
		var said []string
		var mu sync.Mutex
		deps.warn = func(msg string, cause any) {
			mu.Lock()
			said = append(said, fmt.Sprintf("%s: %v", msg, cause))
			mu.Unlock()
		}
		s := startUsageSampler(context.Background(), deps)
		for i := range usageRamp {
			timers.await(t, i+1)
			timers.fire(i)
		}
		<-s.done
		got := s.finish(false)
		if got.reason != reasonReadFailed {
			t.Fatalf("finish = %+v, want READ_FAILED after the ramp's asks", got)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(said) != 1 || !strings.Contains(said[0], "inspect timed out") {
			t.Fatalf("warn heard %q, want the locate's own error once", said)
		}
	})
}

// TestSampler_APanicIsSaidWithItsValue: a recovered panic reaches warn with
// its value, so the log says what broke, and the attempt is READ_FAILED.
func TestSampler_APanicIsSaidWithItsValue(t *testing.T) {
	timers := newFakeTimers()
	deps := samplerFor(timers, &scriptedCgroup{panics: true}, nil)
	heard := make(chan string, 1)
	deps.warn = func(msg string, cause any) { heard <- fmt.Sprint(cause) }
	s := startUsageSampler(context.Background(), deps)
	timers.await(t, 1)
	timers.fire(0)
	<-s.done
	if got := <-heard; got != "a broken reader" {
		t.Fatalf("warn heard %q, want the panic's value", got)
	}
	if got := s.finish(false); got.reason != reasonReadFailed {
		t.Fatalf("finish = %+v, want READ_FAILED", got)
	}
}

// TestHeartbeater_AnUnencodableUsageSaysSo: a reading that cannot be encoded
// within its bound is sent as UNENCODABLE, never left off the beat, which
// would read as an Orker too old to measure.
func TestHeartbeater_AnUnencodableUsageSaysSo(t *testing.T) {
	h := &Heartbeater{}
	h.setUsage(sampledAt(usageState{method: usageCgroupV2, samples: 2, hasRate: true, cores: math.NaN(), last: time.Unix(1, 0)},
		time.Second, time.Unix(2, 0)))
	got := decodeBeatUsage(t, h.progress().Usage)
	if got.Unavailable != task.HeartbeatUsageUnencodable {
		t.Fatalf("usage = %+v, want unavailable %s", got, task.HeartbeatUsageUnencodable)
	}
}

// TestParseUsageSampleInterval: unset is the default, 0s turns measuring
// off, and anything outside 1s to 1h is refused naming the setting, never
// clamped.
func TestParseUsageSampleInterval(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		refused bool
	}{
		{"", DefaultUsageSampleInterval, false},
		{"0s", 0, false},
		{"0", 0, false},
		{"1s", time.Second, false},
		{"30s", 30 * time.Second, false},
		{"1h", time.Hour, false},
		{"999ms", 0, true},
		{"61m", 0, true},
		{"-5s", 0, true},
		{"5", 0, true},
		{"soon", 0, true},
	} {
		got, err := ParseUsageSampleInterval(tc.raw)
		if tc.refused {
			if err == nil || !strings.Contains(err.Error(), UsageSampleIntervalVar) {
				t.Errorf("%q: err = %v, want a refusal naming %s", tc.raw, err, UsageSampleIntervalVar)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q = %v, %v; want %v", tc.raw, got, err, tc.want)
		}
	}
}

// TestDispatcher_TheObserverHearsTheAttemptsUsage: End carries the
// attempt's measured usage, as its record does, and its reservation, so
// telemetry records them beside the attempt.
func TestDispatcher_TheObserverHearsTheAttemptsUsage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	obs := &fakePhaseObserver{}
	dispatcher := newPhaseDispatcher(t, &phaseCapturingRuntime{}, obs)
	dispatcher.Runtime = &locatingRuntime{phaseCapturingRuntime: &phaseCapturingRuntime{},
		read: func(bool) (cgroupReading, error) {
			return cgroupReading{cpuSeconds: 2, memoryPeak: 64, peakRead: true, limit: 128}, nil
		}}
	dispatcher.UsageSampleInterval = DefaultUsageSampleInterval
	dispatcher.heartbeaters = sdkRouteHeartbeater
	in := baseExportInput("usage-observed")
	activityFn := func(ctx context.Context) error {
		_, err := dispatcher.Execute(ctx, in)
		return err
	}
	env.RegisterActivity(activityFn)
	if _, err := env.ExecuteActivity(activityFn); err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}
	_, _, ends, outcome := obs.recorded()
	if ends != 1 || outcome.Usage.GetMethod() != orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2 ||
		outcome.Usage.GetCpuSeconds() != 2 || outcome.Reserved.GetCpu() <= 0 {
		t.Fatalf("End %d times with usage %v, reserved %v; want once, the measured usage and the reservation", ends, outcome.Usage, outcome.Reserved)
	}
}

// TestSampler_LogsAReadingEveryLogInterval: the usage log hears a reading
// at most every log interval, the first one at once, and never more than
// usageLogMax; a sink that panics is contained and hears nothing more,
// while sampling goes on.
func TestSampler_LogsAReadingEveryLogInterval(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{{cpuSeconds: 1, memory: 10}}}
	var mu sync.Mutex
	clock := time.Unix(100, 0)
	var got []UsageSample
	deps := samplerFor(timers, cg, nil)
	deps.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	deps.logEvery = 30 * time.Second
	deps.sample = func(s UsageSample) { got = append(got, s) }
	s := startUsageSampler(context.Background(), deps)
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }
	// Reads at 100, 105, ... 130 s: each tick fired only once the read
	// before it has asked for the next one, the clock advanced in between.
	for i := range 7 {
		timers.await(t, i+1)
		if i > 0 {
			advance(5 * time.Second)
		}
		timers.fire(i)
	}
	timers.await(t, 8)
	s.finish(false)
	if len(got) != 2 || !got[0].At.Equal(time.Unix(100, 0)) || !got[1].At.Equal(time.Unix(130, 0)) {
		at := make([]int64, len(got))
		for i, g := range got {
			at[i] = g.At.Unix()
		}
		t.Fatalf("logged at %v, want 100 and 130 (every 30 s, the first at once)", at)
	}

	// The cap, and a panicking sink.
	logged := &usageSampler{deps: samplerDeps{logEvery: time.Nanosecond, sample: func(UsageSample) {}}}
	logged.state.Store(&usageState{samples: 1})
	for i := range usageLogMax + 10 {
		logged.logSample(time.Unix(int64(i), 0))
	}
	if logged.logged != usageLogMax {
		t.Fatalf("logged %d readings, want the cap %d", logged.logged, usageLogMax)
	}
	calls := 0
	broken := &usageSampler{deps: samplerDeps{logEvery: time.Nanosecond, sample: func(UsageSample) { calls++; panic("a broken sink") }}}
	broken.state.Store(&usageState{samples: 1})
	broken.logSample(time.Unix(1, 0))
	broken.logSample(time.Unix(2, 0))
	if calls != 1 {
		t.Fatalf("a panicking sink was called %d times, want once", calls)
	}
}

// TestUsageRegistry_SumsTheRunningContainers: the gauges' totals are the
// running samplers' latest readings, a sampler counts from its start to its
// finish, and one with no reading adds nothing.
func TestUsageRegistry_SumsTheRunningContainers(t *testing.T) {
	reg := NewUsageRegistry()
	ctx := withUsageRegistry(context.Background(), reg)
	// A sampler's state has one writer, its own goroutine: each waits to locate
	// its cgroup while the test writes the states it sums.
	located := make(chan struct{})
	d := &OCIDispatcher{Runtime: &locatingRuntime{phaseCapturingRuntime: &phaseCapturingRuntime{}, located: located,
		read: func(bool) (cgroupReading, error) { return cgroupReading{cpuSeconds: 1, memory: 100}, nil }}}
	d.UsageSampleInterval = time.Hour
	in := &orkstackv1.ExecuteTaskInput{RunId: "r", TaskId: "t"}
	a, b := d.startUsage(ctx, in, "a"), d.startUsage(ctx, in, "b")
	a.update(func(st *usageState) {
		st.method, st.samples, st.reading, st.cores, st.hasRate = usageCgroupV2, 2, cgroupReading{memory: 100}, 0.5, true
	})
	b.update(func(st *usageState) { st.method, st.samples, st.reading = usageCgroupV2, 1, cgroupReading{memory: 50} })
	if got := reg.Totals(); got != (UsageTotals{Memory: 150, CPUCores: 0.5, Measured: 2}) {
		t.Fatalf("totals = %+v, want both containers' memory and the one rate", got)
	}
	close(located)
	a.finish(false)
	b.finish(false)
	if got := reg.Totals(); got != (UsageTotals{}) {
		t.Fatalf("totals after both finished = %+v, want none", got)
	}
	if reg := (*UsageRegistry)(nil); reg.Totals() != (UsageTotals{}) {
		t.Fatal("a nil registry has totals")
	}
	if WithUsageLog(context.Background(), nil, time.Second).Value(usageLogKey{}) != nil {
		t.Fatal("a nil sink was attached")
	}
}

// TestParseUsageLogInterval: unset is the default, 0s logs only each
// attempt's summary, and anything outside 1s to 1h is refused naming the
// setting, never clamped.
func TestParseUsageLogInterval(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		refused bool
	}{
		{"", DefaultUsageLogInterval, false},
		{"0s", 0, false},
		{"1s", time.Second, false},
		{"1h", time.Hour, false},
		{"500ms", 0, true},
		{"2h", 0, true},
		{"-1s", 0, true},
		{"often", 0, true},
	} {
		got, err := ParseUsageLogInterval(tc.raw)
		if tc.refused {
			if err == nil || !strings.Contains(err.Error(), UsageLogIntervalVar) {
				t.Errorf("%q: err = %v, want a refusal naming %s", tc.raw, err, UsageLogIntervalVar)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q = %v, %v; want %v", tc.raw, got, err, tc.want)
		}
	}
}

// sinkRecorder is a UsageSampleSink that keeps what it hears.
type sinkRecorder struct {
	mu      sync.Mutex
	samples []UsageSample
}

func (r *sinkRecorder) UsageSample(s UsageSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, s)
}

// TestStartUsage_TheAttemptsContextCarriesItsUsageLog: the sink a
// PhaseObserver attaches to the context its Begin returns is the one the
// attempt's sampler hands its readings to; without one it is measured all the same.
func TestStartUsage_TheAttemptsContextCarriesItsUsageLog(t *testing.T) {
	d := &OCIDispatcher{Runtime: &locatingRuntime{phaseCapturingRuntime: &phaseCapturingRuntime{},
		read: func(bool) (cgroupReading, error) { return cgroupReading{cpuSeconds: 1, memory: 100}, nil }}}
	d.UsageSampleInterval = time.Hour // only the final read
	in := &orkstackv1.ExecuteTaskInput{RunId: "r", TaskId: "t"}
	sink := &sinkRecorder{}
	ctx := WithUsageLog(context.Background(), sink, 30*time.Second)
	d.startUsage(ctx, in, "a").finish(false)
	sink.mu.Lock()
	heard := len(sink.samples)
	sink.mu.Unlock()
	if heard != 1 {
		t.Fatalf("the sink heard %d readings, want the final one", heard)
	}
	// No sink on the context: the attempt is measured all the same.
	if got := d.startUsage(context.Background(), in, "b").finish(false); got.method != usageCgroupV2 {
		t.Fatalf("without a usage log: %+v, want it measured", got)
	}
}
