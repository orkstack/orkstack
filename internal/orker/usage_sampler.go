package orker

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orkstack/orkstack/internal/task"
)

// The per-attempt usage sampler (E94): one goroutine per attempt finds the
// container's cgroup, reads it on a ramp (100 ms, 500 ms, 1 s) and then every
// orker.usage_sample_interval, and publishes each reading as an atomic
// snapshot the heartbeat reads without a lock. Nothing it does decides the
// attempt: a hung or failing read, a panic, a cgroup it cannot see, all end
// as an unmeasured reason, never as the task's outcome or a longer attempt.

// usageMethod is how an attempt's usage was measured (MeasurementMethod).
type usageMethod int

const (
	usageUnmeasured usageMethod = iota
	usageCgroupV2
	usageCgroupV1
)

// usageReason is why an attempt's usage was not measured (UnmeasuredReason).
type usageReason int

const (
	reasonNone usageReason = iota
	reasonCgroupNotVisible
	reasonNoCgroup
	reasonEndedBeforeFirstSample
	reasonReadFailed
	reasonNotRun
	reasonDisabled
	reasonStartFailed
	reasonCgroupUnverified
)

// errNoCgroup: this setup gives the container no cgroup the Orker can read
// (rootless on cgroup v1).
var errNoCgroup = errors.New("the container has no readable cgroup")

// usageFinalJoin bounds the wait for the final read after the container
// exits: past it the attempt goes on with the last sample, peak not exact.
const usageFinalJoin = 2 * time.Second

// usageRamp is the first reads' spacing, before the interval takes over, so
// a short task is still measured.
var usageRamp = []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, time.Second}

// measuredUsage is an attempt's final usage (MeasuredUsage).
type measuredUsage struct {
	method      usageMethod
	reason      usageReason
	cpuSeconds  float64 // cumulative at the last read: a lower bound
	memoryPeak  int64
	peakExact   bool
	limit       int64 // 0 is no limit
	samples     int32
	firstSample time.Time
	lastSample  time.Time
	oomKilled   bool
	// started and exited are when the container started and when the
	// sampler was asked for its final read, on the sampler's clock.
	started time.Time
	exited  time.Time
}

// usageState is what the sampler has seen so far; a snapshot is immutable.
type usageState struct {
	method  usageMethod
	reason  usageReason
	reading cgroupReading
	samples int32
	first   time.Time
	last    time.Time
	failed  bool // a read after the first sample failed; the last one stands
	// finalRead: the read asked for at finish completed with a reading, so
	// what it read is the container's state at its end.
	finalRead bool
	// cores is the CPU rate between the last two readings, in cores, when
	// hasRate: there were two, and the counter did not go back between them.
	cores   float64
	hasRate bool
}

// samplerDeps is what a sampler reads through: locate finds the container's
// cgroup and returns its reader (the runtime's inspect, bounded by its own
// timeout); after and now are the clock.
type samplerDeps struct {
	locate   func(ctx context.Context) (read cgroupReader, method usageMethod, err error)
	interval time.Duration // 0 disables sampling
	after    func(time.Duration) <-chan time.Time
	now      func() time.Time
	failed   func() // a recovered panic (ork.orker.usage.sampler_failures)
	// sample, when set, hears a reading every logEvery (orker.usage_log_interval),
	// at most usageLogMax times: the attempt's usage log records.
	sample   func(UsageSample)
	logEvery time.Duration
	// ended hears the sampler finish, once.
	ended func()
	// warn hears why the sampler could not measure: a locate that failed,
	// a recovered panic's value. Nil says nothing.
	warn func(msg string, cause any)
}

type usageSampler struct {
	deps   samplerDeps
	state  atomic.Pointer[usageState]
	final  chan chan struct{}
	done   chan struct{}
	cancel context.CancelFunc

	once    sync.Once
	result  measuredUsage
	started time.Time

	// logged and lastLog belong to the sampler's goroutine alone.
	logged  int
	lastLog time.Time
}

// usageLogMax bounds an attempt's usage log records: two hours at the
// default 30 s interval (prod-kit design §3.5).
const usageLogMax = 240

// UsageSample is one reading an attempt's usage log carries.
type UsageSample struct {
	At         time.Time
	CPUSeconds float64
	// CPUCores is the rate between the last two readings; nil until there are two.
	CPUCores   *float64
	Memory     int64
	MemoryPeak int64
	PeakExact  bool
	Limit      int64
}

// startUsageSampler starts sampling; finish ends it.
func startUsageSampler(ctx context.Context, deps samplerDeps) *usageSampler {
	if deps.after == nil {
		deps.after = time.After
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	s := &usageSampler{deps: deps, final: make(chan chan struct{}), done: make(chan struct{}), started: deps.now()}
	if deps.interval <= 0 {
		s.state.Store(&usageState{reason: reasonDisabled})
		s.cancel = func() {}
		close(s.done)
		return s
	}
	s.state.Store(&usageState{})
	ctx, s.cancel = context.WithCancel(ctx)
	go s.run(ctx)
	return s
}

// snapshot is the latest published state, read without a lock.
func (s *usageSampler) snapshot() usageState { return *s.state.Load() }

func (s *usageSampler) update(change func(*usageState)) {
	next := *s.state.Load()
	change(&next)
	s.state.Store(&next)
}

func (s *usageSampler) run(ctx context.Context) {
	defer close(s.done)
	defer func() {
		if v := recover(); v != nil {
			s.update(func(st *usageState) { st.method, st.reason = usageUnmeasured, reasonReadFailed })
			s.say("the usage sampler panicked; this attempt's usage is READ_FAILED", v)
			s.countFailure()
		}
	}()
	read, method, i, ok := s.locate(ctx)
	if !ok {
		return
	}
	s.update(func(st *usageState) { st.method = method })
	// ended: a read found the cgroup empty or gone, so the container's
	// processes have ended. No periodic read follows, but the final read
	// still comes: an empty cgroup keeps the container's totals until it is
	// removed.
	ended := false
	for ; ; i++ {
		var tick <-chan time.Time
		if !ended {
			wait := s.deps.interval
			if i < len(usageRamp) {
				wait = min(usageRamp[i], s.deps.interval)
			}
			tick = s.deps.after(wait)
		}
		select {
		case <-ctx.Done():
			return
		case reply := <-s.final:
			if s.sample(read, true) == sampleRead {
				s.update(func(st *usageState) { st.finalRead = true })
			}
			close(reply)
			return
		case <-tick:
			switch s.sample(read, false) {
			case sampleEnded:
				ended = true
			case sampleStop:
				return
			}
		}
	}
}

// sampleOutcome is what one read means for the reads after it.
type sampleOutcome int

const (
	sampleRead  sampleOutcome = iota // a reading; sampling goes on
	sampleEnded                      // the cgroup is empty or gone: only the final read is left
	sampleStop                       // nothing can be measured: READ_FAILED before any reading
)

// locate finds the container's cgroup. A failure the engine may get past (a
// slow or failing inspect, errCgroupRead) is asked again on the ramp's
// schedule, the ramp's steps it spends then skipped; any other ends with its
// reason. i is the ramp step sampling starts from; false ends the sampler,
// its reason recorded.
func (s *usageSampler) locate(ctx context.Context) (read cgroupReader, method usageMethod, i int, ok bool) {
	for ; ; i++ {
		read, method, err := s.deps.locate(ctx)
		if err == nil {
			return read, method, i, true
		}
		if !errors.Is(err, errCgroupRead) || i >= len(usageRamp) {
			reason := locateReason(err)
			s.update(func(st *usageState) { st.method, st.reason = usageUnmeasured, reason })
			s.say("this attempt's usage is not measured: its container's cgroup was not found", err)
			return nil, usageUnmeasured, i, false
		}
		select {
		case <-ctx.Done():
			return nil, usageUnmeasured, i, false
		case reply := <-s.final:
			// The container ended while the engine could not say where it
			// was: nothing was read.
			s.update(func(st *usageState) { st.method, st.reason = usageUnmeasured, reasonReadFailed })
			s.say("this attempt's usage is not measured: its container ended before its cgroup was found", err)
			close(reply)
			return nil, usageUnmeasured, i, false
		case <-s.deps.after(min(usageRamp[i], s.deps.interval)):
		}
	}
}

// say tells warn, never taking the Orker down.
func (s *usageSampler) say(msg string, cause any) {
	if s.deps.warn == nil {
		return
	}
	defer func() { _ = recover() }()
	s.deps.warn(msg, cause)
}

// sample takes one reading; final is the read after the container ended.
func (s *usageSampler) sample(read cgroupReader, final bool) sampleOutcome {
	r, err := read(final)
	switch {
	case err == nil:
		at := s.deps.now()
		s.update(func(st *usageState) {
			// Without the kernel's own peak, the peak is the most this
			// sampler saw, not the latest reading.
			if !r.peakRead && st.samples > 0 && st.reading.memoryPeak > r.memoryPeak {
				r.memoryPeak = st.reading.memoryPeak
			}
			// The live rate is the last interval's, not the run's average.
			if st.samples > 0 {
				dt := at.Sub(st.last).Seconds()
				st.hasRate = dt > 0 && r.cpuSeconds >= st.reading.cpuSeconds
				st.cores = 0
				if st.hasRate {
					st.cores = (r.cpuSeconds - st.reading.cpuSeconds) / dt
				}
			}
			st.reading = r
			if st.samples == 0 {
				st.first = at
			}
			st.samples++
			st.last = at
		})
		s.logSample(at)
		return sampleRead
	case errors.Is(err, errCgroupUnverified):
		// A cgroup whose identity can no longer be verified is never read
		// again: what was read while it was verified stands, not exact, and
		// with nothing read the attempt is CGROUP_UNVERIFIED.
		s.update(func(st *usageState) {
			if st.samples > 0 {
				st.failed = true
				return
			}
			st.method, st.reason = usageUnmeasured, reasonCgroupUnverified
		})
		s.say("this attempt's usage stops: its cgroup's identity could not be verified", err)
		return sampleStop
	case errors.Is(err, errCgroupGone), errors.Is(err, errCgroupEmpty):
		// With no reading at all, usageOf says ENDED_BEFORE_FIRST_SAMPLE
		// unless the final read still finds the container's totals.
		return sampleEnded
	default:
		// A read that fails after good ones leaves the last standing, not
		// exact, and sampling goes on: the next read may succeed. One that
		// fails before any is READ_FAILED, never a zero.
		failedBefore := false
		s.update(func(st *usageState) {
			if st.samples > 0 {
				st.failed = true
				return
			}
			failedBefore = true
			st.method, st.reason = usageUnmeasured, reasonReadFailed
		})
		if !failedBefore {
			return sampleRead
		}
	}
	return sampleStop
}

// logSample hands the latest reading to the attempt's usage log every
// logEvery, at most usageLogMax times. The sink never reaches the sampler: a
// panic in it is contained and ends the attempt's log records, not its
// readings.
func (s *usageSampler) logSample(at time.Time) {
	if s.deps.sample == nil || s.deps.logEvery <= 0 || s.logged >= usageLogMax {
		return
	}
	if !s.lastLog.IsZero() && at.Sub(s.lastLog) < s.deps.logEvery {
		return
	}
	st := s.snapshot()
	sample := UsageSample{At: at, CPUSeconds: st.reading.cpuSeconds, Memory: st.reading.memory,
		MemoryPeak: st.reading.memoryPeak, PeakExact: st.reading.peakRead, Limit: st.reading.limit}
	if st.hasRate {
		cores := st.cores
		sample.CPUCores = &cores
	}
	s.lastLog = at
	s.logged++
	defer func() {
		if v := recover(); v != nil {
			s.deps.sample = nil
			s.say("the usage log sink panicked; this attempt logs no more usage samples", v)
		}
	}()
	s.deps.sample(sample)
}

// countFailure reports a recovered panic; the report itself never takes the
// Orker down.
func (s *usageSampler) countFailure() {
	if s.deps.failed == nil {
		return
	}
	defer func() { _ = recover() }()
	s.deps.failed()
}

func locateReason(err error) usageReason {
	switch {
	case errors.Is(err, errNoCgroup):
		return reasonNoCgroup
	case errors.Is(err, errCgroupUnverified):
		return reasonCgroupUnverified
	case errors.Is(err, errCgroupGone), errors.Is(err, errCgroupEmpty):
		return reasonEndedBeforeFirstSample
	case errors.Is(err, errCgroupNotFound), errors.Is(err, errCgroupRefused):
		return reasonCgroupNotVisible
	}
	return reasonReadFailed
}

// finish asks for a final read and waits for it at most usageFinalJoin, then
// stops the sampler and returns the attempt's usage. oomByRuntime is the
// runtime's own OOMKilled. A second call returns the first's result.
func (s *usageSampler) finish(oomByRuntime bool) measuredUsage {
	s.once.Do(func() {
		exited := s.deps.now()
		select {
		case <-s.done:
		default:
			bound := s.deps.after(usageFinalJoin)
			reply := make(chan struct{})
			select {
			case s.final <- reply:
				select {
				case <-reply:
				case <-s.done:
				case <-bound:
				}
			case <-s.done:
			case <-bound:
			}
		}
		s.cancel()
		s.result = usageOf(s.snapshot(), oomByRuntime)
		s.result.started, s.result.exited = s.started, exited
		if s.deps.ended != nil {
			s.deps.ended()
		}
	})
	return s.result
}

// usageOf is the final usage from what was seen: unmeasured with its reason,
// or the last reading, its peak exact only when the kernel's own peak was
// read by the final read itself.
func usageOf(st usageState, oomByRuntime bool) measuredUsage {
	if st.reason == reasonNone && st.samples == 0 {
		st.reason = reasonEndedBeforeFirstSample
	}
	if st.reason != reasonNone {
		return measuredUsage{method: usageUnmeasured, reason: st.reason, oomKilled: oomByRuntime}
	}
	r := st.reading
	u := measuredUsage{
		method:     st.method,
		cpuSeconds: r.cpuSeconds,
		memoryPeak: r.memoryPeak,
		// The final read's own peak is the kernel's high-water mark for the whole
		// container, whatever an earlier read did: it read the cgroup after its
		// processes ended, before its removal.
		peakExact:   r.peakRead && st.finalRead,
		limit:       r.limit,
		samples:     st.samples,
		firstSample: st.first,
		lastSample:  st.last,
		oomKilled:   r.oomKills > 0 || oomByRuntime,
	}
	// Killed at its limit with no peak of its own: the peak was the limit,
	// but it was not read.
	if u.oomKilled && !r.peakRead && r.limit > 0 {
		u.memoryPeak, u.peakExact = r.limit, false
	}
	return u
}

// name is r's UnmeasuredReason name without its prefix, as a beat's
// usage carries it.
func (r usageReason) name() string {
	switch r {
	case reasonCgroupNotVisible:
		return "CGROUP_NOT_VISIBLE"
	case reasonNoCgroup:
		return "NO_CGROUP"
	case reasonEndedBeforeFirstSample:
		return "ENDED_BEFORE_FIRST_SAMPLE"
	case reasonReadFailed:
		return "READ_FAILED"
	case reasonNotRun:
		return "NOT_RUN"
	case reasonDisabled:
		return "DISABLED"
	case reasonStartFailed:
		return "START_FAILED"
	case reasonCgroupUnverified:
		return "CGROUP_UNVERIFIED"
	}
	return ""
}

// noSampleYet is a beat's reason while the sampler has read nothing and
// knows no reason it cannot.
const noSampleYet = "NO_SAMPLE_YET"

// beatUsage is the live usage a beat carries from this snapshot at now: the
// last reading and how old it is, or why there is none. Tick is the beat's
// own; the caller fills it.
func (st usageState) beatUsage(now time.Time, interval time.Duration) task.HeartbeatUsage {
	u := task.HeartbeatUsage{Interval: interval.Milliseconds()}
	switch {
	case st.reason != reasonNone:
		u.Unavailable = st.reason.name()
	case st.samples == 0:
		u.Unavailable = noSampleYet
	default:
		r := st.reading
		// To the millisecond: finer is noise, and it keeps the beat small.
		if st.hasRate {
			cores := roundMilli(st.cores)
			u.CPUCores = &cores
		}
		u.CPUSeconds = roundMilli(r.cpuSeconds)
		u.Memory, u.MemoryPeak, u.PeakExact, u.Limit = r.memory, r.memoryPeak, r.peakRead, r.limit
		u.AgeMillis = max(now.Sub(st.last).Milliseconds(), 0)
	}
	return u
}

// roundMilli is x to three decimals.
func roundMilli(x float64) float64 { return math.Round(x*1000) / 1000 }
