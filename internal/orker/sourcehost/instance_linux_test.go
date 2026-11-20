//go:build linux

package sourcehost

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// laneSource is a container of n lanes that keeps the contract: a status,
// a message per poll from a shared queue, and a settlement per verdict. It
// plays the container's side of OrkStack's own file contract in-process,
// for unit tests of the Orker's side; the real fixtures (test/fixtures,
// against real brokers) prove the contract end to end.
type laneSource struct {
	ask, answer string
	lanes       int
	status      string
	mu          sync.Mutex
	queue       []string
	verdicts    map[string]int
	settled     atomic.Int64
	statuses    atomic.Int64
	// statusOnce writes the status once and never refreshes it, as a
	// container that hangs after it became ready.
	statusOnce atomic.Bool
}

func (f *laneSource) rename(name string, body []byte) {
	dir := filepath.Join(f.answer, "tmp")
	tmp := filepath.Join(dir, filepath.Base(name)+"."+itoa(time.Now().UnixNano())+".tmp")
	_ = os.WriteFile(tmp, body, 0o644)
	_ = os.MkdirAll(filepath.Dir(filepath.Join(f.answer, name)), 0o755)
	_ = os.Rename(tmp, filepath.Join(f.answer, name))
}

func (f *laneSource) run(ctx context.Context) {
	polls, verdicts := make([]int64, f.lanes), make([]int64, f.lanes)
	lastStatus := time.Time{}
	for ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
		if f.status != "" && time.Since(lastStatus) > 200*time.Millisecond && !(f.statusOnce.Load() && f.statuses.Load() > 0) {
			f.rename(servicehost.StatusName, []byte(f.status))
			f.statuses.Add(1)
			lastStatus = time.Now()
		}
		for k := 0; k < f.lanes; k++ {
			if raw, err := os.ReadFile(filepath.Join(f.ask, LanePath(k, StepPoll))); err == nil {
				var p Poll
				if json.Unmarshal(raw, &p) == nil && p.Seq > polls[k] {
					polls[k] = p.Seq
					f.mu.Lock()
					if len(f.queue) == 0 {
						f.mu.Unlock()
						f.rename(LanePath(k, StepEmpty), []byte(`{"schemaVersion":1,"seq":`+itoa(p.Seq)+`}`))
						continue
					}
					body := f.queue[0]
					f.queue = f.queue[1:]
					f.mu.Unlock()
					f.rename(LanePath(k, StepBody), []byte(body))
					f.rename(LanePath(k, StepMessage), []byte(`{"schemaVersion":1,"seq":`+itoa(p.Seq)+`,"id":"m-`+itoa(int64(k))+`-`+itoa(p.Seq)+
						`","bodyBytes":`+itoa(int64(len(body)))+`,"deliveryCount":1}`))
				}
			}
			if raw, err := os.ReadFile(filepath.Join(f.ask, LanePath(k, StepVerdict))); err == nil {
				var v Verdict
				if json.Unmarshal(raw, &v) == nil && v.Seq > verdicts[k] {
					verdicts[k] = v.Seq
					f.mu.Lock()
					f.verdicts[v.Action]++
					f.mu.Unlock()
					f.settled.Add(1)
					f.rename(LanePath(k, StepSettled), []byte(`{"schemaVersion":1,"seq":`+itoa(v.Seq)+`,"ok":true}`))
				}
			}
		}
	}
}

const readyStatus = `{"schemaVersion":1,"ready":true,"check":{"ok":true},"broker":{"object":"dir:q","maxDeliveries":5,"messageLimitBytes":1048576}}`

func newInstanceFixture(t *testing.T, door Door, lanes int, status string, queue ...string) (*Instance, *laneSource, *memRecorder) {
	t.Helper()
	root := t.TempDir()
	ask, answer := filepath.Join(root, "ask"), filepath.Join(root, "answer")
	for _, d := range []string{ask, filepath.Join(answer, "tmp")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for k := 0; k < lanes; k++ {
		_ = os.MkdirAll(filepath.Join(answer, "lanes", itoa(int64(k))), 0o755)
	}
	a, err := servicehost.OpenAskDir(ask)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	steps, err := servicehost.OpenAnswerDir(answer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = steps.Close() })
	src := &laneSource{ask: ask, answer: answer, lanes: lanes, status: status, queue: queue, verdicts: map[string]int{}}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan struct{})
	// The fake container stops writing before the test's directories are
	// removed: cleanups run last-registered first.
	t.Cleanup(func() { cancel(); <-ran })
	go func() { defer close(ran); src.run(ctx) }()
	rec := &memRecorder{}
	j := NewJudge(judgingSource("PAID"), 1, door, &fakeStager{}, time.Now)
	limits := eventsources.SpecLimits{StatusSeconds: 30, StartupSeconds: 1, StopSeconds: 20, AliveSeconds: 1, OrphanSeconds: 60,
		MaxBodyBytes: 1 << 20, MaxAttributesBytes: 64 << 10, PollWaitSeconds: 0, AnswerSeconds: 10, SettleSeconds: 30, HoldSeconds: 960}
	in := &Instance{Config: InstanceConfig{Limits: limits, Park: 300 * time.Millisecond, RejectBreaker: 20},
		Judge: j, Door: door, Steps: steps, Asks: a, Now: time.Now, Started: time.Now()}
	for k := 0; k < lanes; k++ {
		in.Drivers = append(in.Drivers, &LaneDriver{Index: k, Steps: steps, Asks: a, Judge: j, Recorder: rec, Now: time.Now,
			Limits: LaneLimits{PollWait: 0, Answer: 10 * time.Second, Settle: 30 * time.Second, Hold: 16 * time.Minute, LeaseSeconds: 60, MaxBodyBytes: 1 << 20}})
	}
	return in, src, rec
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func acceptingDoor() *fakeDoor {
	return &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)}}
}

func TestInstance_AcceptsAcrossLanesAndBeats(t *testing.T) {
	in, src, rec := newInstanceFixture(t, acceptingDoor(), 2, readyStatus,
		`{"status":"PAID","id":"1"}`, `{"status":"PAID","id":"2"}`, `{"status":"PAID","id":"3"}`, `{"status":"PAID","id":"4"}`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Ending, 1)
	go func() { done <- in.Run(ctx) }()
	waitFor(t, "four settled messages", func() bool { return src.settled.Load() == 4 })
	if _, err := os.Stat(filepath.Join(src.ask, "alive.json")); err != nil {
		t.Fatalf("no beat: %v", err)
	}
	cancel()
	if e := <-done; e.Reason != "" {
		t.Fatalf("a stop ended %+v", e)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.receipts) != 4 || src.verdicts[ActionAck] != 4 {
		t.Fatalf("receipts %d, verdicts %v", len(rec.receipts), src.verdicts)
	}
}

func TestInstance_NoStatusIsNotReady(t *testing.T) {
	in, _, _ := newInstanceFixture(t, acceptingDoor(), 1, "")
	if e := in.Run(context.Background()); e.Reason != eventsources.ReasonServiceNotReady {
		t.Fatalf("ending %+v", e)
	}
}

func TestInstance_FatalIsConfigRejected(t *testing.T) {
	in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, `{"schemaVersion":1,"fatal":true,"check":{"ok":false,"reason":"CONFIG_INVALID","field":"stream"}}`,
		`{"status":"PAID","id":"1"}`)
	if e := in.Run(context.Background()); e.Reason != eventsources.ReasonServiceConfigRejected || src.settled.Load() != 0 {
		t.Fatalf("ending %+v, settled %d", e, src.settled.Load())
	}
}

func TestInstance_UnhealthyPollsNothing(t *testing.T) {
	in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, `{"schemaVersion":1,"ready":true,"check":{"ok":false,"reason":"UNREACHABLE"}}`,
		`{"status":"PAID","id":"1"}`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Ending, 1)
	go func() { done <- in.Run(ctx) }()
	// Positive control: the instance runs (it beats) and has read several
	// statuses, each of which its lanes saw before they could poll.
	waitFor(t, "a beat and three statuses", func() bool {
		_, err := os.Stat(filepath.Join(src.ask, "alive.json"))
		return err == nil && src.statuses.Load() >= 3
	})
	if _, err := os.Stat(filepath.Join(src.ask, LanePath(0, StepPoll))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an unhealthy service was polled (%v)", err)
	}
	cancel()
	<-done
}

// A delivery limit the Orker does not accept is OBJECT_INVALID (06 §5): an
// image cannot switch the dead-letter receipt off by reporting a limit
// nobody reaches, so its lanes are never polled; the limit it does accept
// is the one releases are judged against.
func TestInstance_DeliveryLimitFromTheStatus(t *testing.T) {
	t.Run("a billion is refused", func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1,
			`{"schemaVersion":1,"ready":true,"check":{"ok":true},"broker":{"maxDeliveries":1000000000,"messageLimitBytes":1048576}}`,
			`{"status":"PAID","id":"1"}`)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan Ending, 1)
		go func() { done <- in.Run(ctx) }()
		// Positive control: the instance read several statuses.
		waitFor(t, "three statuses", func() bool { return src.statuses.Load() >= 3 })
		if _, err := os.Stat(filepath.Join(src.ask, LanePath(0, StepPoll))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a service reporting a billion deliveries was polled (%v)", err)
		}
		in.mu.Lock()
		st, _ := in.status.Last()
		limit, _ := in.limit.Limit()
		in.mu.Unlock()
		if st.OK || st.Reason != "OBJECT_INVALID" || st.Field != "broker.maxDeliveries" || limit != 0 {
			t.Fatalf("status ok %v reason %q field %q, limit %d", st.OK, st.Reason, st.Field, limit)
		}
		cancel()
		<-done
	})
	t.Run("an accepted limit is kept", func(t *testing.T) {
		in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus, `{"status":"PAID","id":"1"}`)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan Ending, 1)
		go func() { done <- in.Run(ctx) }()
		waitFor(t, "a settled message", func() bool { return src.settled.Load() == 1 })
		in.mu.Lock()
		limit, _ := in.limit.Limit()
		in.mu.Unlock()
		if limit != 5 {
			t.Fatalf("limit = %d; want the status's 5", limit)
		}
		cancel()
		<-done
	})
}

// TestInstance_AStaleStatusEndsTheInstance: a status older than three
// statusSeconds is a late step on every lane (14 §5, rule 3), and three in a
// row end the instance SERVICE_UNRESPONSIVE, retried, never a container
// left running that polls nothing.
func TestInstance_AStaleStatusEndsTheInstance(t *testing.T) {
	in, src, _ := newInstanceFixture(t, acceptingDoor(), 1, readyStatus)
	src.statusOnce.Store(true)
	in.Config.Limits.StatusSeconds = 1
	done := make(chan Ending, 1)
	go func() { done <- in.Run(context.Background()) }()
	select {
	case e := <-done:
		if e.Reason != eventsources.ReasonServiceUnresponsive || !e.Retried {
			t.Fatalf("a stale status ended %+v", e)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("an instance whose status went stale is still running")
	}
	if src.statuses.Load() != 1 {
		t.Fatalf("the fixture refreshed its status (%d)", src.statuses.Load())
	}
}

// TestInstance_ARequiredReceiptTheStoreRefusesHoldsTheVerdict: a verdict
// whose receipt is required waits, the message held, while the store
// refuses it, and is given once it is written (01 §5); the attempt does not
// end, so no message in hand spends a delivery for an outage.
func TestInstance_ARequiredReceiptTheStoreRefusesHoldsTheVerdict(t *testing.T) {
	door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerReject)}}
	in, src, rec := newInstanceFixture(t, door, 1, readyStatus, `{"status":"PAID","id":"1"}`)
	rec.mu.Lock()
	rec.failTimes = 1
	rec.mu.Unlock()
	// A park long enough for the retry within it (wait.go's first back-off).
	in.Config.Park = 10 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Ending, 1)
	go func() { done <- in.Run(ctx) }()
	waitFor(t, "the verdict, once the receipt is written", func() bool { return src.settled.Load() == 1 })
	select {
	case e := <-done:
		t.Fatalf("the attempt ended while the store refused a receipt: %+v", e)
	default:
	}
	cancel()
	<-done
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.receipts) != 1 || rec.receipts[0].Outcome != artifacts.ReceiptRejected || rec.failTimes != 0 {
		t.Fatalf("receipts %+v, refusals left %d", rec.receipts, rec.failTimes)
	}
}

func TestInstance_RejectBreakerAsksForAPause(t *testing.T) {
	door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerReject)}}
	in, _, _ := newInstanceFixture(t, door, 1, readyStatus, `{"status":"PAID","id":"1"}`, `{"status":"PAID","id":"2"}`)
	in.Config.RejectBreaker = 2
	e := in.Run(context.Background())
	door.mu.Lock()
	defer door.mu.Unlock()
	if e.Paused != BreakerAllRejected || len(door.paused) != 1 || door.paused[0] != BreakerAllRejected {
		t.Fatalf("ending %+v, pauses asked %v", e, door.paused)
	}
}

func TestInstance_AShortWaitSpendsNothing(t *testing.T) {
	var offers atomic.Int64
	door := &fakeDoor{
		offers: []func(Offer) (Answer, error){func(Offer) (Answer, error) {
			if offers.Add(1) <= 2 {
				return Answer{}, errors.New("engine unreachable")
			}
			return Answer{Kind: AnswerProceed}, nil
		}},
		starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)},
	}
	in, src, _ := newInstanceFixture(t, door, 1, readyStatus, `{"status":"PAID","id":"1"}`)
	in.Config.Park = 10 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Ending, 1)
	go func() { done <- in.Run(ctx) }()
	waitFor(t, "the message settled after the engine came back", func() bool { return src.settled.Load() == 1 })
	cancel()
	<-done
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.verdicts[ActionAck] != 1 || src.verdicts[ActionRelease] != 0 {
		t.Fatalf("verdicts %v: a wait within the park releases nothing", src.verdicts)
	}
}
