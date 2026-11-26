package sourcehost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The event-source profile's status words (06 §5).
var Vocabulary = servicehost.Vocabulary{
	Reasons:  map[string]bool{"OBJECT_MISSING": true, CheckObjectInvalid: true, "NO_DEAD_LETTER": true, CheckMessageLimit: true},
	Warnings: map[string]bool{"DELIVERY_LIMIT_LOW": true, "PULL_LIMIT_LOW": true, "MESSAGE_LIMIT_ABOVE_BOUND": true},
	Block:    "broker",
}

// instanceTick is how often the instance's own loop runs: the beat, the
// status, the wait and the watchdog.
const instanceTick = 50 * time.Millisecond

// doorCallBound bounds an engine call the instance makes outside a lane.
const doorCallBound = 10 * time.Second

// localInARow ends the attempt: three local receive failures in a row on a
// lane (06 §4.3).
const localInARow = 3

// InstanceConfig is one instance's settings from its service spec and the
// activity that hosts it.
type InstanceConfig struct {
	Limits     eventsources.SpecLimits
	Park       time.Duration
	Trial      string
	AtMostOnce bool
}

// Ending is how an instance ended: the attempt's reason ("" when it was
// stopped), what the engine stopped it for, and the breaker it asked to pause the
// source with, if any.
type Ending struct {
	Reason  string
	Detail  string
	Stop    string
	Paused  string
	Retried bool
	// exited: its container is gone.
	exited bool
}

// Instance runs one event-source container's lanes (06, 01 §5): the beat,
// the status, the wait and the breakers. Run owns it.
type Instance struct {
	Config  InstanceConfig
	Drivers []*LaneDriver
	Judge   *Judge
	Door    Door
	Steps   Steps
	Asks    Asks
	Now     func() time.Time
	Started time.Time
	// Hold is what the Orker holds the instance back for, "" for nothing: it
	// writes no poll while it does.
	Hold func() string

	mu       sync.Mutex
	waiter   *Waiter
	breakers *Breakers
	limit    DeliveryLimit
	// staleSince is when the status went stale, zero while it is fresh.
	staleSince time.Time
	status     *servicehost.StatusWatch
	healthy    bool
	held       map[int]*heldMessage
	tryLane    int
	locals     map[int]int
	// fails counts each lane's failed receives in a row, of any kind.
	fails    map[int]int
	nextPoll time.Time
	end      chan Ending
	once     sync.Once
	// stopping is closed when the instance is asked to stop; polled is the
	// wait for a container's answer, which then ends.
	stopping chan struct{}
	polled   context.Context
	// beat and lastBeat are the loop's, and then the stop's.
	beat     servicehost.Beat
	lastBeat time.Time

	shown      shown
	waitCause  string
	readySince time.Time
	acceptedAt time.Time
	pollOkAt   time.Time
	block      brokerStatus
	checkedAt  time.Time
	looked     time.Time
	finished   chan struct{}
}

type heldMessage struct {
	msg   HandedMessage
	body  []byte
	since time.Time
	cmd   chan laneCommand
}

type laneCommand int

const (
	cmdRedecide laneCommand = iota + 1
	cmdRelease
)

// The stop's own bounds (06 §7): what is in flight at the engine for a message
// in hand, and how long an at-most-once source is asked for what it buffered.
const (
	stopInFlight  = 5 * time.Second
	stopBuffered  = 10 * time.Second
	stopReceiptAt = 500 * time.Millisecond
	// lanesEnd is how long a lane that was cancelled is given to end. One of
	// the Orker's own that does not holds no ending.
	lanesEnd = 10 * time.Second
)

// Run runs the instance until ctx ends (a stop: Ending{} with no reason),
// or the attempt ends for a reason of its own. A stop tells the container so and
// finishes the Orker's side of each open exchange; stopping the container is the
// caller's.
func (in *Instance) Run(ctx context.Context) Ending {
	work, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	polled, stopPolls := context.WithCancel(work)
	defer stopPolls()
	in.mu.Lock()
	in.waiter = NewWaiter(in.Config.Park, len(in.Drivers))
	in.breakers = NewBreakers(in.Judge.RejectBreaker(), in.Config.Trial)
	in.status = servicehost.NewStatusWatch(in.Started, in.Config.Limits.StartupSeconds, in.Config.Limits.StatusSeconds)
	in.held, in.locals, in.fails, in.end = map[int]*heldMessage{}, map[int]int{}, map[int]int{}, in.endings()
	in.stopping, in.polled = make(chan struct{}), polled
	in.mu.Unlock()
	var wg sync.WaitGroup
	for i, d := range in.Drivers {
		d.Limit, d.Hold = in.DeliveryLimit, in.heldBack
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.lane(work, i)
		}()
	}
	done, returned := make(chan struct{}), make(chan struct{})
	in.Finished()
	go func() { wg.Wait(); close(done); <-returned; close(in.finished) }()
	defer close(returned)
	ending := in.loop(ctx, work)
	if ending.stops() {
		in.windDown(stopPolls, cancel, done)
	}
	cancel()
	in.lanesEnded(done)
	if ending.stops() {
		// What a lane found while it finished is not a clean stop.
		select {
		case e := <-in.end:
			if e != (Ending{}) {
				return e
			}
		default:
		}
	}
	return ending
}

// stops reports an ending whose container is still there to be told of it and
// has kept the contract: a stop that was asked for, or a pause.
func (e Ending) stops() bool { return e.Reason == "" && !e.exited }

// lanesEnded waits for the lanes, which were cancelled, to end.
func (in *Instance) lanesEnded(done <-chan struct{}) {
	bound := time.NewTimer(lanesEnd)
	defer bound.Stop()
	select {
	case <-done:
	case <-bound.C:
		if len(in.Drivers) > 0 {
			in.Drivers[0].failed("stalled", errors.New("a lane of this Orker's own did not end"))
		}
	}
}

// windDown tells the container that it is stopping and finishes the Orker's side
// of each open exchange.
func (in *Instance) windDown(stopPolls, cancel context.CancelFunc, done <-chan struct{}) {
	in.mu.Lock()
	close(in.stopping)
	in.mu.Unlock()
	if err := in.Asks.WriteJSON("stop.json", map[string]any{"schemaVersion": 1}); err != nil && len(in.Drivers) > 0 {
		// It is stopped by its signals then.
		in.Drivers[0].failed("stop", err)
	}
	within := stopInFlight
	if in.Config.AtMostOnce {
		within = stopBuffered
	} else {
		stopPolls()
	}
	bound := time.NewTimer(within)
	defer bound.Stop()
	beats := time.NewTicker(instanceTick)
	defer beats.Stop()
	for {
		select {
		case <-done:
			return
		case <-bound.C:
			cancel()
			return
		case <-beats.C:
			// A beat that cannot be written now ends nothing: the container
			// is being stopped.
			_ = in.beatDue(in.Now())
		}
	}
}

// beatDue writes the beat when it is due.
func (in *Instance) beatDue(now time.Time) error {
	if now.Sub(in.lastBeat) < time.Duration(in.Config.Limits.AliveSeconds)*time.Second {
		return nil
	}
	if err := in.Asks.WriteJSON("alive.json", in.beat.Next()); err != nil {
		return err
	}
	in.lastBeat = now
	return nil
}

// isStopping reports that the instance was asked to stop.
func (in *Instance) isStopping() bool {
	select {
	case <-in.stopping:
		return true
	default:
		return false
	}
}

// Exited ends the instance because its container is gone.
func (in *Instance) Exited(e Ending) {
	e.exited = true
	in.finish(e)
}

// Finished is closed once Run has returned and nothing of it uses the container's
// files any more, which a lane that hangs may do after it.
func (in *Instance) Finished() <-chan struct{} {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.finished == nil {
		in.finished = make(chan struct{})
	}
	return in.finished
}

// DeliveryLimit is the broker's delivery limit as the container last reported
// it, and when.
func (in *Instance) DeliveryLimit() (int, time.Time) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.limit.Limit()
}

func (in *Instance) finish(e Ending) {
	in.once.Do(func() {
		in.mu.Lock()
		end := in.endings()
		in.mu.Unlock()
		end <- e
	})
}

// endings is where the instance's one ending goes, which may come before it
// runs. Locked.
func (in *Instance) endings() chan Ending {
	if in.end == nil {
		in.end = make(chan Ending, 1)
	}
	return in.end
}

// loop is the instance's own loop: the beat every aliveSeconds, the
// status, the wait's steps and the watchdog.
func (in *Instance) loop(ctx, work context.Context) Ending {
	var lastStatus servicehost.StepID
	ticker := time.NewTicker(instanceTick)
	defer ticker.Stop()
	for {
		now := in.Now()
		if err := in.beatDue(now); err != nil {
			return Ending{Reason: eventsources.ReasonServiceHostStalled, Retried: true, Detail: "the beat could not be written: " + err.Error()}
		}
		if e, done := in.readStatus(now, &lastStatus); done {
			return e
		}
		hold := in.heldBack()
		in.mu.Lock()
		in.show(now, hold)
		step := in.waiter.Due(now)
		in.mu.Unlock()
		switch step {
		case StepRetryOldest:
			in.command(cmdRedecide, true)
		case StepReleaseAll:
			in.command(cmdRelease, false)
			in.mu.Lock()
			in.waiter.Released(now)
			in.mu.Unlock()
		case StepProbe:
			// Bounded: the loop that asks is the one that writes the beat,
			// which a hung engine must not stop (14 §6.1).
			pctx, cancel := context.WithTimeout(work, doorCallBound)
			err := in.Door.Probe(pctx)
			cancel()
			in.mu.Lock()
			ok := err == nil && in.healthy
			if in.waiter.Probed(ok, now) == StepTryOne {
				in.tryLane = 0
			}
			in.mu.Unlock()
		}
		if in.unresponsive(now) {
			return Ending{Reason: eventsources.ReasonServiceUnresponsive, Retried: true, Detail: "more than a quarter of the lanes stopped answering"}
		}
		if k, stalled := in.stalled(now); stalled {
			return Ending{Reason: eventsources.ReasonServiceHostStalled, Retried: true, Detail: fmt.Sprintf("lane %d of this Orker's own made no step past the longest hold", k)}
		}
		in.mu.Lock()
		in.looked = now
		in.mu.Unlock()
		select {
		case <-ctx.Done():
			return Ending{}
		case e := <-in.end:
			return e
		case <-ticker.C:
		}
	}
}

// readStatus reads status.json and judges the service's health.
func (in *Instance) readStatus(now time.Time, last *servicehost.StepID) (Ending, bool) {
	if reader, ok := in.Steps.(interface {
		ReadStepID(string, int64) ([]byte, servicehost.StepID, error)
	}); ok {
		raw, id, err := reader.ReadStepID(servicehost.StatusName, servicehost.MaxStatusBytes)
		switch {
		case errors.Is(err, servicehost.ErrNotYet):
		case err != nil:
			return violationEnding(err), true
		default:
			s, err := servicehost.ReadStatus(raw, Vocabulary)
			if err != nil {
				return violationEnding(err), true
			}
			s, limit := JudgeStatus(s, in.Config.AtMostOnce)
			in.mu.Lock()
			if limit > 0 && id != *last {
				// Once per status written, not per look: the smaller of the
				// last two reports holds only if a report is one sample.
				in.limit.Observe(limit, now)
			}
			in.status.Observe(s, now, id != *last)
			if id != *last {
				in.block, in.checkedAt = readBroker(s.Block), now
			}
			in.mu.Unlock()
			*last = id
		}
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	health := in.status.Health(now)
	if health != servicehost.HealthReady {
		in.readySince = time.Time{}
	} else if in.readySince.IsZero() {
		in.readySince = now
	}
	switch health {
	case servicehost.HealthNeverReported:
		return Ending{Reason: eventsources.ReasonServiceNotReady, Retried: true, Detail: "no status within startupSeconds"}, true
	case servicehost.HealthFatal:
		s, _ := in.status.Last()
		return Ending{Reason: eventsources.ReasonServiceConfigRejected, Detail: fmt.Sprintf("the image cannot work with this configuration: %s %s",
			SafeWord(s.Reason, 64), SafeWord(s.Field, 64))}, true
	case servicehost.HealthReady:
		in.healthy, in.staleSince = true, time.Time{}
		return Ending{}, false
	case servicehost.HealthNoVerdict:
		// A status older than three statusSeconds is a late step on every
		// lane (14 §5, rule 3); three in a row end every lane, and so the
		// instance.
		in.healthy = false
		if in.staleSince.IsZero() {
			in.staleSince = now
		}
		if now.Sub(in.staleSince) >= 3*time.Duration(max(in.Config.Limits.StatusSeconds, 1))*time.Second {
			return Ending{Reason: eventsources.ReasonServiceUnresponsive, Retried: true,
				Detail: "its status was not refreshed: three late steps on every lane"}, true
		}
		return Ending{}, false
	}
	in.healthy, in.staleSince = false, time.Time{}
	return Ending{}, false
}

func violationEnding(err error) Ending {
	var v *servicehost.ViolationError
	if errors.As(err, &v) {
		return Ending{Reason: eventsources.ReasonServiceContractViolation, Detail: v.Error()}
	}
	return Ending{Reason: eventsources.ReasonServiceHostStalled, Retried: true, Detail: err.Error()}
}

// unresponsive is the watchdog over every lane.
func (in *Instance) unresponsive(now time.Time) bool {
	lanes := make([]*servicehost.Lane, len(in.Drivers))
	for i, d := range in.Drivers {
		lanes[i] = &d.Lane
	}
	hold := time.Duration(in.Config.Limits.HoldSeconds) * time.Second
	return servicehost.Unresponsive(lanes, now, hold)
}

// stalled is the Orker's watch over itself (14 §4.4): a lane whose exchange is
// open and that made no step for longer than the longest hold.
func (in *Instance) stalled(now time.Time) (int, bool) {
	hold := time.Duration(in.Config.Limits.HoldSeconds) * time.Second
	for i, d := range in.Drivers {
		if open, _ := d.Lane.Open(now); open && now.Sub(d.lastMoved()) > hold {
			return i, true
		}
	}
	return 0, false
}

// command sends cmd to the held lanes: the oldest one alone, or all.
func (in *Instance) command(cmd laneCommand, oldestOnly bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	var oldest *heldMessage
	for _, h := range in.held {
		if oldestOnly {
			if oldest == nil || h.since.Before(oldest.since) {
				oldest = h
			}
			continue
		}
		select {
		case h.cmd <- cmd:
		default:
		}
	}
	if oldest != nil {
		select {
		case oldest.cmd <- cmd:
		default:
		}
	}
}

// mayPoll reports whether lane i may write a poll now: the service is
// ready and healthy, the wait lets this lane poll, and the rate allows one.
func (in *Instance) mayPoll(i int) bool {
	hold, rate := in.heldBack(), in.Judge.Rate()
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.isStopping() {
		// Only an at-most-once source is asked for what it buffered.
		return in.Config.AtMostOnce && in.healthy
	}
	if !in.healthy || i >= in.waiter.Lanes() || hold != "" {
		return false
	}
	now := in.Now()
	if now.Before(in.nextPoll) {
		return false
	}
	in.nextPoll = now.Add(time.Second / time.Duration(rate))
	return true
}

// lane runs one lane until ctx ends or the attempt ends.
func (in *Instance) lane(ctx context.Context, i int) {
	d := in.Drivers[i]
	look := time.NewTicker(lookEvery)
	defer look.Stop()
	for ctx.Err() == nil {
		if in.isStopping() && !in.Config.AtMostOnce {
			return
		}
		if !in.mayPoll(i) {
			select {
			case <-ctx.Done():
				return
			case <-look.C:
			}
			continue
		}
		wait, last := in.Config.Limits.PollWaitSeconds, in.isStopping()
		if last {
			if !in.Config.AtMostOnce {
				return
			}
			wait = 0
		}
		ex, err := d.pollUntil(ctx, in.polled, wait)
		if errors.Is(err, ErrReceiptRequired) && ex.Kind == ExchangeHeld {
			// The verdict waits for its receipt (01 §5): the message is
			// held and judged again after a park.
			err = nil
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				in.finish(endingFor(err))
			}
			return
		}
		if ex.Kind != ExchangeReceiveFailed {
			in.mu.Lock()
			in.pollOkAt = in.Now()
			in.mu.Unlock()
		}
		if last && ex.Kind == ExchangeQuiet {
			// Asked after the notice, it has nothing left.
			return
		}
		if !in.took(ctx, i, d, ex) {
			return
		}
	}
}

func endingFor(err error) Ending {
	if errors.Is(err, ErrUnresponsive) {
		return Ending{Reason: eventsources.ReasonServiceUnresponsive, Retried: true, Detail: err.Error()}
	}
	return violationEnding(err)
}

// took handles one exchange's end; false ends the lane.
func (in *Instance) took(ctx context.Context, i int, d *LaneDriver, ex Exchange) bool {
	switch ex.Kind {
	case ExchangeQuiet:
		in.mu.Lock()
		in.locals[i], in.fails[i] = 0, 0
		in.waiter.Tried(nil, in.Now())
		in.mu.Unlock()
	case ExchangeReceiveFailed:
		in.mu.Lock()
		if ex.Error.Kind == KindLocal {
			in.locals[i]++
			if in.locals[i] >= localInARow {
				in.mu.Unlock()
				in.finish(Ending{Reason: eventsources.ReasonServiceUnresponsive, Retried: true, Detail: "three local receive failures in a row"})
				return false
			}
		}
		in.fails[i]++
		wait := receiveBackoff(in.fails[i])
		in.mu.Unlock()
		// A broker that refuses or fails every receive is asked again on a
		// back-off, never at the speed of a poll.
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
		return true
	case ExchangeSettled, ExchangeHoldExpired:
		in.resetReceive(i)
		in.mu.Lock()
		in.breakers.Certain()
		in.mu.Unlock()
		return in.settled(ctx, ex)
	case ExchangeHeld:
		in.resetReceive(i)
		return in.hold(ctx, i, d, ex)
	}
	return true
}

// resetReceive ends a lane's run of failed receives: a message was handed
// over, so the next failure is the first in a row again.
func (in *Instance) resetReceive(i int) {
	in.mu.Lock()
	in.locals[i], in.fails[i] = 0, 0
	in.mu.Unlock()
}

// receiveBackoff is the wait after the n-th failed receive in a row on a
// lane: 1 s doubling to at most 30 s.
func receiveBackoff(n int) time.Duration {
	d := time.Second
	for i := 1; i < n && d < 30*time.Second; i++ {
		d *= 2
	}
	return min(d, 30*time.Second)
}

// settled counts an exchange that ended with its message decided toward the
// breakers and the wait. One its container closed at the hold bound says
// nothing of how the broker settles, so it counts no settlement.
func (in *Instance) settled(ctx context.Context, ex Exchange) bool {
	rejectAt := in.Judge.RejectBreaker()
	in.mu.Lock()
	refused := !ex.Settled.OK && ex.Settled.Error != nil && ex.Settled.Error.Kind == KindRefused
	trip := ""
	if ex.Kind == ExchangeSettled {
		trip = in.breakers.Settled(refused)
	}
	switch ex.Decision.Receipt.Outcome {
	case artifacts.ReceiptAccepted, artifacts.ReceiptDuplicate:
		in.breakers.Accepted()
		in.waiter.Accepted()
		in.acceptedAt = in.Now()
	case artifacts.ReceiptRejected, artifacts.ReceiptDropped:
		if ex.Decision.Disposition == Reject {
			in.breakers.RejectAt(rejectAt)
			if t := in.breakers.Rejected(ex.Message.ID); t != "" {
				trip = t
			}
		}
	}
	if in.waiter.Waiting() {
		d := ex.Decision
		in.waiter.Tried(&d, in.Now())
	}
	in.mu.Unlock()
	if ex.Decision.Disposition == Pause {
		in.finish(Ending{Stop: ex.Decision.Stop, Detail: "the engine stopped it"})
		return false
	}
	if trip != "" {
		in.pause(ctx, trip)
		return false
	}
	return true
}

// pause asks the engine to pause the source with code and ends the
// instance; a request that gets no answer still ends it, and the source's
// workflow and the engine's next answer pause it.
func (in *Instance) pause(ctx context.Context, code string) {
	pctx, cancel := context.WithTimeout(ctx, doorCallBound)
	err := in.Door.RequestPause(pctx, code)
	cancel()
	detail := "asked to pause the source"
	if err != nil {
		detail = "asked to pause the source; the request got no answer: " + err.Error()
	}
	in.finish(Ending{Paused: code, Stop: eventsources.ReasonPaused, Detail: detail})
}

// hold keeps a message whose disposition is a wait until the instance's
// wait tells the lane what to do with it.
func (in *Instance) hold(ctx context.Context, i int, d *LaneDriver, ex Exchange) bool {
	h := &heldMessage{msg: ex.Message, body: ex.Body, since: in.Now(), cmd: make(chan laneCommand, 1)}
	in.mu.Lock()
	in.held[i], in.waitCause = h, ex.Decision.Cause
	if !ex.Decision.Uncertain {
		// A certain decision ends a run of uncertain ones: the breaker
		// counts five in a row.
		in.breakers.Certain()
	}
	if ex.Decision.Uncertain {
		if t := in.breakers.Uncertain(in.Now()); t != "" {
			in.mu.Unlock()
			in.release(ctx, d, h, ex.Decision)
			in.pause(ctx, t)
			return false
		}
	}
	if in.waiter.Waiting() {
		_, again := in.waiter.Tried(&ex.Decision, in.Now())
		if again {
			in.mu.Unlock()
			return in.releaseOne(ctx, i, d, h, ex.Decision)
		}
	}
	in.waiter.Begin(in.Now())
	in.mu.Unlock()
	look := time.NewTicker(lookEvery)
	defer look.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-in.stopping:
			return in.releaseOne(ctx, i, d, h, ex.Decision)
		case cmd := <-h.cmd:
			if cmd == cmdRelease {
				return in.releaseOne(ctx, i, d, h, ex.Decision)
			}
			next, err := d.decide(ctx, &h.msg, &h.body)
			if err != nil {
				if ctx.Err() == nil {
					in.finish(endingFor(err))
				}
				return false
			}
			in.mu.Lock()
			in.waiter.Retried(next.Disposition != Wait, in.Now())
			in.mu.Unlock()
			if next.Disposition != Wait {
				res, err := d.Verdict(ctx, h.msg, next)
				if res.Kind == ExchangeHeld && (err == nil || errors.Is(err, ErrReceiptRequired)) {
					// Not given yet (its receipt refused, or its
					// confirmation aged): still held, judged again after
					// the next park.
					in.mu.Lock()
					in.waiter.Begin(in.Now())
					in.mu.Unlock()
					continue
				}
				in.unhold(i)
				if err != nil {
					in.finish(endingFor(err))
					return false
				}
				return in.settled(ctx, res)
			}
		case <-look.C:
			expired, err := d.HoldExpired(ctx, h.msg)
			if err != nil {
				in.finish(endingFor(err))
				return false
			}
			if expired {
				in.unhold(i)
				return true
			}
		}
	}
}

func (in *Instance) unhold(i int) {
	in.mu.Lock()
	delete(in.held, i)
	in.mu.Unlock()
}

// releaseOne gives a held message back, with its dead-letter receipt when
// it is at the broker's limit.
func (in *Instance) releaseOne(ctx context.Context, i int, d *LaneDriver, h *heldMessage, why Decision) bool {
	in.unhold(i)
	return in.release(ctx, d, h, why)
}

func (in *Instance) release(ctx context.Context, d *LaneDriver, h *heldMessage, why Decision) bool {
	in.mu.Lock()
	delay := int(in.waiter.CycleDelay() / time.Second)
	limit, _ := in.limit.Limit()
	in.mu.Unlock()
	release := Decision{Disposition: Wait, Action: ActionRelease, Delay: max(delay, releaseDelaySeconds),
		Receipt: Receipt{Outcome: artifacts.ReceiptReleased, Reason: eventsources.ReasonMessageReleasedAcceptFailing, Cause: why.Cause}}
	if in.Config.AtMostOnce {
		release.Receipt.Outcome, release.Receipt.Required = artifacts.ReceiptDropped, true
	}
	if r, ok := DeadLettered(why, h.msg.DeliveryCount, limit, in.Config.AtMostOnce); ok {
		release.Receipt = r
	}
	for {
		_, err := d.Verdict(ctx, h.msg, release)
		if err == nil {
			return true
		}
		if !errors.Is(err, ErrReceiptRequired) {
			if ctx.Err() == nil {
				in.finish(endingFor(err))
			}
			return false
		}
		// Its receipt is required and the store refused it: the release
		// waits a park and is tried again, until the container's own hold
		// bound gives the message back.
		select {
		case <-ctx.Done():
			return false
		case <-in.stopping:
			select {
			case <-ctx.Done():
				return false
			case <-time.After(stopReceiptAt):
			}
		case <-time.After(max(in.Config.Park, lookEvery)):
		}
		if expired, xerr := d.HoldExpired(ctx, h.msg); xerr != nil || expired {
			if xerr != nil && ctx.Err() == nil {
				in.finish(endingFor(xerr))
				return false
			}
			return xerr == nil
		}
	}
}
