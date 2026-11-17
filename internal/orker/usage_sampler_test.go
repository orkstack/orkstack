package orker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTimers is an injected time.After: each call is recorded with its
// duration, and fires only when the test says so.
type fakeTimers struct {
	mu    sync.Mutex
	asked []time.Duration
	chans []chan time.Time
	added chan struct{}
}

func newFakeTimers() *fakeTimers { return &fakeTimers{added: make(chan struct{}, 64)} }

func (f *fakeTimers) after(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := make(chan time.Time, 1)
	f.asked = append(f.asked, d)
	f.chans = append(f.chans, c)
	select {
	case f.added <- struct{}{}:
	default:
	}
	return c
}

// await waits until n timers were asked for, failing at a deadline.
func (f *fakeTimers) await(t *testing.T, n int) []time.Duration {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		f.mu.Lock()
		got := len(f.asked)
		f.mu.Unlock()
		if got >= n {
			f.mu.Lock()
			defer f.mu.Unlock()
			return append([]time.Duration(nil), f.asked...)
		}
		select {
		case <-f.added:
		case <-deadline:
			t.Fatalf("only %d timers were asked for, want %d", got, n)
		}
	}
}

// fire fires the i-th timer.
func (f *fakeTimers) fire(i int) {
	f.mu.Lock()
	c := f.chans[i]
	f.mu.Unlock()
	c <- time.Unix(0, 0)
}

// scriptedCgroup is a fake reader for the sampler's fault cases a real
// cgroup cannot produce on demand: a read that fails once and then
// succeeds, a malformed value, a panicking reader, an exact sequence of
// readings against a fake clock.
type scriptedCgroup struct {
	mu       sync.Mutex
	readings []cgroupReading
	errs     []error
	calls    int
	block    chan struct{} // when set, a read blocks until it closes
	panics   bool
}

func (s *scriptedCgroup) read(bool) (cgroupReading, error) {
	s.mu.Lock()
	block := s.block
	s.mu.Unlock()
	if block != nil {
		<-block
	}
	if s.panics {
		panic("a broken reader")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.calls
	s.calls++
	if i < len(s.errs) && s.errs[i] != nil {
		return cgroupReading{}, s.errs[i]
	}
	if i >= len(s.readings) {
		i = len(s.readings) - 1
	}
	return s.readings[i], nil
}

func samplerFor(timers *fakeTimers, cg *scriptedCgroup, locateErr error) samplerDeps {
	return samplerDeps{
		locate: func(context.Context) (cgroupReader, usageMethod, error) {
			if locateErr != nil {
				return nil, usageUnmeasured, locateErr
			}
			return cg.read, usageCgroupV2, nil
		},
		interval: 5 * time.Second,
		after:    timers.after,
		now:      func() time.Time { return time.Unix(100, 0) },
	}
}

func TestSampler_RampsThenSamplesAtTheInterval(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{
		{cpuSeconds: 0.1, memory: 10, memoryPeak: 10, peakRead: true, limit: 100},
		{cpuSeconds: 0.5, memory: 30, memoryPeak: 40, peakRead: true, limit: 100},
	}}
	s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
	for i := range 4 {
		timers.await(t, i+1)
		timers.fire(i)
	}
	asked := timers.await(t, 5)
	want := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, time.Second, 5 * time.Second, 5 * time.Second}
	for i, d := range want {
		if asked[i] != d {
			t.Fatalf("timer %d = %v, want %v (ramp then interval): %v", i, asked[i], d, asked)
		}
	}
	snap := s.snapshot()
	if snap.samples != 4 || snap.reading.cpuSeconds != 0.5 || snap.reading.memoryPeak != 40 {
		t.Errorf("snapshot = %+v, want 4 samples and the latest reading", snap)
	}
	got := s.finish(false)
	if got.method != usageCgroupV2 || got.samples != 5 || !got.peakExact || got.memoryPeak != 40 || got.cpuSeconds != 0.5 {
		t.Errorf("finish = %+v, want the final read (5 samples) with an exact peak", got)
	}
}

// TestSampler_FinalJoinIsBounded: a final read that hangs costs the attempt
// at most the join bound, and the result keeps the last sample, not exact.
func TestSampler_FinalJoinIsBounded(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{{cpuSeconds: 1, memory: 5, memoryPeak: 7, peakRead: true}}}
	s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
	timers.await(t, 1)
	timers.fire(0) // one good sample
	timers.await(t, 2)
	block := make(chan struct{}) // the next read hangs
	cg.mu.Lock()
	cg.block = block
	cg.mu.Unlock()
	defer close(block)
	done := make(chan measuredUsage, 1)
	go func() { done <- s.finish(false) }()
	asked := timers.await(t, 3)
	if asked[2] != usageFinalJoin {
		t.Fatalf("the final join waited on %v, want %v", asked[2], usageFinalJoin)
	}
	timers.fire(2)
	select {
	case got := <-done:
		if got.samples != 1 || got.peakExact || got.memoryPeak != 7 {
			t.Errorf("finish after a hung final read = %+v, want the last sample, peak not exact", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finish did not return when its join bound passed")
	}
}

func TestSampler_HungOrFailingChangesNoOutcome(t *testing.T) {
	t.Run("a panicking reader is READ_FAILED and counted", func(t *testing.T) {
		timers := newFakeTimers()
		var failures atomic.Int32
		deps := samplerFor(timers, &scriptedCgroup{panics: true}, nil)
		deps.failed = func() { failures.Add(1) }
		s := startUsageSampler(context.Background(), deps)
		timers.await(t, 1)
		timers.fire(0)
		got := s.finish(false)
		if got.method != usageUnmeasured || got.reason != reasonReadFailed || failures.Load() != 1 {
			t.Errorf("finish = %+v, failures %d; want READ_FAILED, counted once", got, failures.Load())
		}
	})
	t.Run("a malformed value is READ_FAILED, never a zero", func(t *testing.T) {
		timers := newFakeTimers()
		cg := &scriptedCgroup{readings: []cgroupReading{{}}, errs: []error{errCgroupRead}}
		s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
		timers.await(t, 1)
		timers.fire(0)
		if got := s.finish(false); got.method != usageUnmeasured || got.reason != reasonReadFailed {
			t.Errorf("finish = %+v, want READ_FAILED", got)
		}
	})
	t.Run("a hung locate still finishes within the bound", func(t *testing.T) {
		timers := newFakeTimers()
		block := make(chan struct{})
		defer close(block)
		deps := samplerFor(timers, nil, nil)
		deps.locate = func(ctx context.Context) (cgroupReader, usageMethod, error) {
			<-block
			return nil, usageUnmeasured, errCgroupNotFound
		}
		s := startUsageSampler(context.Background(), deps)
		done := make(chan measuredUsage, 1)
		go func() { done <- s.finish(false) }()
		timers.await(t, 1)
		timers.fire(0)
		select {
		case got := <-done:
			if got.method != usageUnmeasured || got.reason != reasonEndedBeforeFirstSample {
				t.Errorf("finish = %+v, want ENDED_BEFORE_FIRST_SAMPLE", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("finish waited on a hung locate")
		}
	})
}

func TestSampler_UnmeasuredReasons(t *testing.T) {
	for _, tc := range []struct {
		name   string
		locate error
		want   usageReason
	}{
		{"not visible", errCgroupNotFound, reasonCgroupNotVisible},
		{"refused", errCgroupRefused, reasonCgroupNotVisible},
		{"no cgroup", errNoCgroup, reasonNoCgroup},
		{"gone before the first sample", errCgroupGone, reasonEndedBeforeFirstSample},
		{"identity unverifiable", errCgroupUnverified, reasonCgroupUnverified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timers := newFakeTimers()
			s := startUsageSampler(context.Background(), samplerFor(timers, nil, tc.locate))
			if got := s.finish(false); got.method != usageUnmeasured || got.reason != tc.want {
				t.Errorf("finish = %+v, want %v", got, tc.want)
			}
		})
	}
	t.Run("disabled", func(t *testing.T) {
		deps := samplerFor(newFakeTimers(), nil, nil)
		deps.interval = 0
		if got := startUsageSampler(context.Background(), deps).finish(false); got.reason != reasonDisabled {
			t.Errorf("finish = %+v, want DISABLED", got)
		}
	})
	t.Run("the container ended before a sample", func(t *testing.T) {
		timers := newFakeTimers()
		cg := &scriptedCgroup{readings: []cgroupReading{{}}, errs: []error{errCgroupEmpty, errCgroupGone}}
		s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
		timers.await(t, 1)
		timers.fire(0)
		if got := s.finish(false); got.reason != reasonEndedBeforeFirstSample {
			t.Errorf("finish = %+v, want ENDED_BEFORE_FIRST_SAMPLE", got)
		}
	})
}

// TestSampler_GoneAfterASampleKeepsTheLast: the cgroup removed once the
// container ended leaves the last sample standing, not exact.
func TestSampler_GoneAfterASampleKeepsTheLast(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{{cpuSeconds: 2, memory: 9, memoryPeak: 9, peakRead: true}}, errs: []error{nil, errCgroupGone}}
	s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
	timers.await(t, 1)
	timers.fire(0)
	timers.await(t, 2)
	got := s.finish(false)
	if got.method != usageCgroupV2 || got.samples != 1 || got.peakExact || got.cpuSeconds != 2 {
		t.Errorf("finish = %+v, want the last sample kept, not exact", got)
	}
}

func TestSampler_OOM(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reading cgroupReading
		runtime bool
		peak    int64
		exact   bool
		oom     bool
	}{
		{"oom_kill counted, peak read", cgroupReading{memoryPeak: 90, peakRead: true, limit: 100, oomKills: 1}, false, 90, true, true},
		{"oom_kill counted, no peak file", cgroupReading{memoryPeak: 60, limit: 100, oomKills: 1}, false, 100, false, true},
		{"the runtime saw it", cgroupReading{memoryPeak: 60, limit: 100}, true, 100, false, true},
		{"no oom", cgroupReading{memoryPeak: 60, peakRead: true, limit: 100}, false, 60, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timers := newFakeTimers()
			s := startUsageSampler(context.Background(), samplerFor(timers, &scriptedCgroup{readings: []cgroupReading{tc.reading}}, nil))
			timers.await(t, 1)
			timers.fire(0)
			timers.await(t, 2)
			got := s.finish(tc.runtime)
			if got.oomKilled != tc.oom || got.memoryPeak != tc.peak || got.peakExact != tc.exact {
				t.Errorf("finish = %+v, want oom %v, peak %d, exact %v", got, tc.oom, tc.peak, tc.exact)
			}
		})
	}
}

func TestSampler_FinishTwiceIsTheSameResult(t *testing.T) {
	timers := newFakeTimers()
	s := startUsageSampler(context.Background(), samplerFor(timers, &scriptedCgroup{readings: []cgroupReading{{cpuSeconds: 1}}}, nil))
	timers.await(t, 1)
	first := s.finish(false)
	if second := s.finish(false); second != first {
		t.Errorf("finish twice = %+v then %+v", first, second)
	}
}

// TestSampler_ACancelledAttemptIsNeverAnExactPeak: an attempt cancelled
// before finish ends with no final read, so its last sample is a lower
// bound, never claimed exact.
func TestSampler_ACancelledAttemptIsNeverAnExactPeak(t *testing.T) {
	timers := newFakeTimers()
	ctx, cancel := context.WithCancel(context.Background())
	s := startUsageSampler(ctx, samplerFor(timers, &scriptedCgroup{readings: []cgroupReading{{cpuSeconds: 1, memoryPeak: 9, peakRead: true}}}, nil))
	timers.await(t, 1)
	timers.fire(0)
	timers.await(t, 2)
	cancel()
	<-s.done
	if got := s.finish(false); got.samples != 1 || got.peakExact {
		t.Errorf("finish after a cancel = %+v, want the last sample, not exact", got)
	}
}

// TestSampler_AReadErrorAfterGoodSamplesKeepsTheLast: one failed read late in
// an attempt keeps what was measured instead of unmeasuring it, and sampling
// goes on: the next good read is taken, and the final read's kernel peak,
// the container's lifetime high-water mark, is exact whatever came before.
func TestSampler_AReadErrorAfterGoodSamplesKeepsTheLast(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{
		{cpuSeconds: 3, memoryPeak: 9, peakRead: true},
		{cpuSeconds: 3, memoryPeak: 9, peakRead: true},
		{cpuSeconds: 4, memoryPeak: 9, peakRead: true},
	}, errs: []error{nil, errCgroupRead}}
	s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
	timers.await(t, 1)
	timers.fire(0)
	timers.await(t, 2)
	timers.fire(1) // the failed read
	timers.await(t, 3)
	if snap := s.snapshot(); snap.samples != 1 || !snap.failed || snap.reason != reasonNone {
		t.Fatalf("after the failed read: %+v, want the last sample kept, marked failed, still sampling", snap)
	}
	timers.fire(2)
	timers.await(t, 4)
	got := s.finish(false)
	if got.method != usageCgroupV2 || got.samples < 2 || got.cpuSeconds != 4 || !got.peakExact {
		t.Errorf("finish = %+v, want the final read's totals, its kernel peak exact", got)
	}
}

// TestSampler_WithoutAPeakFileThePeakIsTheMostSeen: memory that rose and fell
// reports the highest reading taken, not the last.
func TestSampler_WithoutAPeakFileThePeakIsTheMostSeen(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{{memory: 400, memoryPeak: 400}, {memory: 50, memoryPeak: 50}}}
	s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
	timers.await(t, 1)
	timers.fire(0)
	timers.await(t, 2)
	timers.fire(1)
	timers.await(t, 3)
	if got := s.finish(false); got.memoryPeak != 400 || got.peakExact {
		t.Errorf("finish = %+v, want peak 400 (the most seen), not exact", got)
	}
}

// TestSampler_ATinyIntervalShortensTheRamp: the ramp never waits longer than
// the configured interval.
func TestSampler_ATinyIntervalShortensTheRamp(t *testing.T) {
	timers := newFakeTimers()
	deps := samplerFor(timers, &scriptedCgroup{readings: []cgroupReading{{}}}, nil)
	deps.interval = 200 * time.Millisecond
	s := startUsageSampler(context.Background(), deps)
	for i := range 2 {
		timers.await(t, i+1)
		timers.fire(i)
	}
	asked := timers.await(t, 3)
	s.finish(false)
	if asked[0] != 100*time.Millisecond || asked[1] != 200*time.Millisecond || asked[2] != 200*time.Millisecond {
		t.Errorf("waits = %v, want 100ms then the 200ms interval", asked)
	}
}

func TestSampler_AFailureReportThatPanicsIsContained(t *testing.T) {
	timers := newFakeTimers()
	deps := samplerFor(timers, &scriptedCgroup{panics: true}, nil)
	deps.failed = func() { panic("a broken metric") }
	s := startUsageSampler(context.Background(), deps)
	timers.await(t, 1)
	timers.fire(0)
	if got := s.finish(false); got.reason != reasonReadFailed {
		t.Errorf("finish = %+v, want READ_FAILED", got)
	}
}

// TestSampler_AShortTaskIsMeasuredByItsFinalRead: a container that ended
// before its first periodic read stops the periodic reads, and the final
// read still measures it, exactly: a short task's figures are the kernel's.
func TestSampler_AShortTaskIsMeasuredByItsFinalRead(t *testing.T) {
	timers := newFakeTimers()
	totals := cgroupReading{cpuSeconds: 0.25, memory: 4096, memoryPeak: 65536, peakRead: true, limit: 1 << 20}
	var finals atomic.Int32
	deps := samplerFor(timers, &scriptedCgroup{}, nil)
	deps.locate = func(context.Context) (cgroupReader, usageMethod, error) {
		return func(final bool) (cgroupReading, error) {
			if !final {
				return cgroupReading{}, errCgroupEmpty
			}
			finals.Add(1)
			return totals, nil
		}, usageCgroupV2, nil
	}
	s := startUsageSampler(context.Background(), deps)
	timers.await(t, 1)
	timers.fire(0) // the first read finds the container ended
	got := s.finish(false)
	if finals.Load() != 1 || got.method != usageCgroupV2 || got.cpuSeconds != 0.25 || got.memoryPeak != 65536 || !got.peakExact {
		t.Fatalf("finish = %+v (final reads %d); want the final read's totals, exact", got, finals.Load())
	}
	// The first read's tick, then only finish's own bound: no periodic read
	// was scheduled after the container ended.
	for i, d := range timers.await(t, 1) {
		if i > 0 && d != usageFinalJoin {
			t.Fatalf("timer %d of %v after the container ended; want only the final join's bound", i, d)
		}
	}
}

// TestSampler_AnIdentityLostAfterReadingsStopsTheReads: a cgroup whose
// identity can no longer be verified is never read again; what was read
// while it was verified stands, not exact, and nothing is read after.
func TestSampler_AnIdentityLostAfterReadingsStopsTheReads(t *testing.T) {
	timers := newFakeTimers()
	cg := &scriptedCgroup{readings: []cgroupReading{{cpuSeconds: 2, memoryPeak: 9, peakRead: true}}, errs: []error{nil, errCgroupUnverified}}
	s := startUsageSampler(context.Background(), samplerFor(timers, cg, nil))
	timers.await(t, 1)
	timers.fire(0)
	timers.await(t, 2)
	timers.fire(1) // the identity is lost
	<-s.done
	got := s.finish(false)
	if got.method != usageCgroupV2 || got.samples != 1 || got.cpuSeconds != 2 || got.peakExact {
		t.Fatalf("finish = %+v, want the verified reading, not exact, and no read after", got)
	}
	cg.mu.Lock()
	calls := cg.calls
	cg.mu.Unlock()
	if calls != 2 {
		t.Fatalf("%d reads, want 2: none after the identity was lost, the final one included", calls)
	}
}
