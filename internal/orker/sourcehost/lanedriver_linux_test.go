//go:build linux

package sourcehost

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// fakeSource is a container that keeps the contract over real files: it
// answers each poll from its queue of messages (or quietly), applies each
// verdict and writes settled.json.
type fakeSource struct {
	t           *testing.T
	ask, answer string
	queue       []string
	mu          sync.Mutex
	verdicts    []Verdict
	// settleFirst settles each message before handing it over, as an image
	// that acknowledges without waiting for a verdict does.
	settleFirst bool
	// settleFirstAs, when set, is what settleFirst and the settle below say
	// after the header: an error, or a verdict's id.
	settleFirstAs string
	// closeOnVerdict answers each verdict with settleFirstAs, naming no
	// verdict: an image whose own close landed as the verdict was written.
	closeOnVerdict bool
	// wrongVerdict answers each verdict with a settle naming another one,
	// as an image whose settle answers no verdict of this lane does.
	wrongVerdict bool
	// mute answers no poll: a test that plants its own step needs the fake
	// not to overwrite it.
	mute atomic.Bool
}

// placeStep writes body at path the way a container that keeps the contract
// does. A step that could not be written is told: a lane that waits for it would
// otherwise read as one that stalled.
func placeStep(t *testing.T, tmp, path string, body []byte) {
	if err := errors.Join(os.WriteFile(tmp, body, 0o644), os.MkdirAll(filepath.Dir(path), 0o755), os.Rename(tmp, path)); err != nil {
		t.Errorf("the fake container could not write a step: %v", err)
	}
}

func (f *fakeSource) rename(name string, body []byte) {
	placeStep(f.t, filepath.Join(f.answer, "tmp", filepath.Base(name)+".tmp"), filepath.Join(f.answer, name), body)
}

func (f *fakeSource) run(ctx context.Context) {
	var seenPoll, seenVerdict int64
	for ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
		if f.mute.Load() {
			continue
		}
		if raw, err := os.ReadFile(filepath.Join(f.ask, LanePath(0, StepPoll))); err == nil {
			var p Poll
			if json.Unmarshal(raw, &p) == nil && p.Seq > seenPoll {
				seenPoll = p.Seq
				f.mu.Lock()
				if len(f.queue) == 0 {
					f.mu.Unlock()
					f.rename(LanePath(0, StepEmpty), []byte(`{"schemaVersion":1,"seq":`+itoa(p.Seq)+`}`))
					continue
				}
				body := f.queue[0]
				f.queue = f.queue[1:]
				f.mu.Unlock()
				f.rename(LanePath(0, StepBody), []byte(body))
				if f.settleFirst {
					f.rename(LanePath(0, StepSettled), []byte(`{"schemaVersion":1,"seq":`+itoa(p.Seq)+`,`+f.settleBody()+`}`))
				}
				f.rename(LanePath(0, StepMessage), []byte(`{"schemaVersion":1,"seq":`+itoa(p.Seq)+`,"id":"m-`+itoa(p.Seq)+`","bodyBytes":`+itoa(int64(len(body)))+`,"deliveryCount":1}`))
			}
		}
		if raw, err := os.ReadFile(filepath.Join(f.ask, LanePath(0, StepVerdict))); err == nil {
			var v Verdict
			if json.Unmarshal(raw, &v) == nil && v.Seq > seenVerdict {
				seenVerdict = v.Seq
				f.mu.Lock()
				f.verdicts = append(f.verdicts, v)
				f.mu.Unlock()
				if f.closeOnVerdict {
					f.rename(LanePath(0, StepSettled), []byte(`{"schemaVersion":1,"seq":`+itoa(v.Seq)+`,`+f.settleBody()+`}`))
					continue
				}
				id := v.VerdictID
				if f.wrongVerdict {
					id = "not-" + id
				}
				f.rename(LanePath(0, StepSettled), []byte(`{"schemaVersion":1,"seq":`+itoa(v.Seq)+`,"verdictId":"`+id+`","ok":true}`))
			}
		}
	}
}

func (f *fakeSource) settleBody() string {
	if f.settleFirstAs != "" {
		return f.settleFirstAs
	}
	return `"ok":true`
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

type memRecorder struct {
	mu       sync.Mutex
	receipts []Receipt
	fail     bool
	// failTimes fails that many records before it writes them.
	failTimes int
}

func (r *memRecorder) Record(_ context.Context, _ HandedMessage, _ int64, rc Receipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("store down")
	}
	if r.failTimes > 0 {
		r.failTimes--
		return errors.New("store down")
	}
	r.receipts = append(r.receipts, rc)
	return nil
}

// newDriverFixture is a lane over real files, to be used in a synctest bubble:
// its waits are the bubble's clock, which a loaded host cannot run past.
func newDriverFixture(t *testing.T, door Door, queue ...string) (*LaneDriver, *fakeSource, *memRecorder) {
	t.Helper()
	root := t.TempDir()
	ask, answer := filepath.Join(root, "ask"), filepath.Join(root, "answer")
	for _, d := range []string{ask, filepath.Join(answer, "tmp"), filepath.Join(answer, "lanes", "0")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
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
	rec := &memRecorder{}
	src := &fakeSource{t: t, ask: ask, answer: answer, queue: queue}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan struct{})
	// The fake container stops writing before the test's directories are
	// removed: cleanups run last-registered first.
	t.Cleanup(func() { cancel(); <-ran })
	go func() { defer close(ran); src.run(ctx) }()
	j := NewJudge(judgingSource("PAID"), 1, door, &fakeStager{}, time.Now)
	d := &LaneDriver{Index: 0, Steps: steps, Asks: a, Judge: j, Recorder: rec, Now: time.Now,
		Limits: LaneLimits{PollWait: 20 * time.Second, Answer: 30 * time.Second, Settle: 30 * time.Second, Hold: 16 * time.Minute,
			LeaseSeconds: 60, MaxBodyBytes: 1 << 20}}
	return d, src, rec
}

func TestLane_ReceiptBeforeVerdict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)}}
		d, src, rec := newDriverFixture(t, door, `{"status":"PAID","id":"o-1"}`)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		began := time.Now()
		ex, err := d.Poll(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		if ex.Kind != ExchangeSettled || !ex.Settled.OK || len(rec.receipts) != 1 || rec.receipts[0].Outcome != artifacts.ReceiptAccepted {
			t.Fatalf("exchange %+v, receipts %+v", ex, rec.receipts)
		}
		// Each of the container's two steps is taken at the first look after it.
		if took := time.Since(began); took > 2*lookEvery {
			t.Fatalf("the exchange took %v of the lane's own waits; want two looks", took)
		}
		src.mu.Lock()
		v := src.verdicts
		src.mu.Unlock()
		if len(v) != 1 || v[0].Action != ActionAck || v[0].Seq != 1 {
			t.Fatalf("verdicts %+v", v)
		}
		quiet, err := d.Poll(ctx, 0)
		if err != nil || quiet.Kind != ExchangeQuiet || d.Lane.Seq() != 2 {
			t.Fatalf("second exchange %+v, %v", quiet, err)
		}
	})
}

func TestLane_RequiredReceiptHoldsTheVerdict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerReject)}}
		d, src, rec := newDriverFixture(t, door, `{"status":"PAID","id":"o-1"}`)
		rec.fail = true
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ex, err := d.Poll(ctx, 0)
		if !errors.Is(err, ErrReceiptRequired) || ex.Kind != ExchangeHeld {
			t.Fatalf("exchange %+v, %v", ex, err)
		}
		// The driver writes a verdict synchronously, so none on disk once Poll
		// returned means none was given.
		if _, err := os.Stat(filepath.Join(src.ask, LanePath(0, StepVerdict))); !os.IsNotExist(err) {
			t.Fatalf("a verdict was given before its required receipt was written (%v)", err)
		}
	})
}

func TestLane_AWaitIsHeldWithNoVerdict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){unreachable[Offer]()}}
		d, _, rec := newDriverFixture(t, door, `{"status":"PAID","id":"o-1"}`)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ex, err := d.Poll(ctx, 0)
		if err != nil || ex.Kind != ExchangeHeld || ex.Decision.Disposition != Wait || len(rec.receipts) != 0 || d.Lane.CanBegin() {
			t.Fatalf("exchange %+v, %v: a wait holds the message and its lane", ex, err)
		}
		release := Decision{Disposition: Wait, Action: ActionRelease, Delay: 30, Receipt: Receipt{Outcome: artifacts.ReceiptReleased}}
		settled, err := d.Verdict(ctx, ex.Message, release)
		if err != nil || settled.Kind != ExchangeSettled || !d.Lane.CanBegin() {
			t.Fatalf("release %+v, %v", settled, err)
		}
	})
}

// TestLane_SettledBeforeAVerdictIsAViolation: an image that settles a
// message before the Orker gave its verdict acknowledged on its own; that
// breaks the contract, and the Orker writes neither a receipt nor a verdict
// for it.
func TestLane_SettledBeforeAVerdictIsAViolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)}}
		d, src, rec := newDriverFixture(t, door, `{"status":"PAID","id":"o-1"}`)
		src.settleFirst = true
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var v *servicehost.ViolationError
		if _, err := d.Poll(ctx, 0); !errors.As(err, &v) || !strings.Contains(v.Error(), "without a verdict") {
			t.Fatalf("a settle before any verdict: %v", err)
		}
		if len(rec.receipts) != 0 {
			t.Fatalf("receipts were written for it: %+v", rec.receipts)
		}
		if _, err := os.Stat(filepath.Join(src.ask, LanePath(0, StepVerdict))); !os.IsNotExist(err) {
			t.Fatalf("a verdict was written for it (%v)", err)
		}
	})
}

// TestLane_ASettleMustNameItsVerdict: a settle answers the verdict it names
// (verdictId), so one that names another, or none, is recognised whenever it
// lands, and ends the attempt rather than being recorded as the verdict's.
func TestLane_ASettleMustNameItsVerdict(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)}}
		d, src, _ := newDriverFixture(t, door, `{"status":"PAID","id":"o-1"}`)
		src.wrongVerdict = true
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var v *servicehost.ViolationError
		if _, err := d.Poll(ctx, 0); !errors.As(err, &v) || !strings.Contains(v.Error(), "verdict") {
			t.Fatalf("a settle naming another verdict: %v", err)
		}
		src.mu.Lock()
		defer src.mu.Unlock()
		if len(src.verdicts) != 1 || src.verdicts[0].VerdictID == "" {
			t.Fatalf("the verdict carried no id: %+v", src.verdicts)
		}
	})
}

const holdExpired = `"ok":false,"error":{"kind":"hold_expired"}`

// laterClock is a clock the test moves ahead of the real one.
type laterClock struct{ by atomic.Int64 }

func (c *laterClock) now() time.Time { return time.Now().Add(time.Duration(c.by.Load())) }

// TestLane_ACloseBeforeTheHoldBoundIsAViolation: a close at the hold bound
// names no verdict, so it is taken only once the bound has passed. Before
// that it is a settle nobody asked for, whether it lands before the verdict
// or after it.
func TestLane_ACloseBeforeTheHoldBoundIsAViolation(t *testing.T) {
	tests := []struct {
		name  string
		set   func(*fakeSource)
		later time.Duration
		want  string
		// verdict: a verdict (and its receipt) was written before it landed.
		verdict bool
	}{
		{name: "before the verdict", set: func(f *fakeSource) { f.settleFirst = true }, want: "before its hold bound"},
		{name: "as the verdict is written", set: func(f *fakeSource) { f.closeOnVerdict = true }, want: "before its hold bound", verdict: true},
		{name: "at the bound, but naming a verdict", set: func(f *fakeSource) {
			f.settleFirst, f.settleFirstAs = true, `"verdictId":"1-aa",`+holdExpired
		}, later: time.Hour, want: "names a verdict"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var clock laterClock
				door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){
					func(Start) (Answer, error) {
						clock.by.Store(int64(tt.later))
						return startAnswer(AnswerAccepted)(Start{})
					}}}
				d, src, rec := newDriverFixture(t, door)
				d.Now = clock.now
				src.mu.Lock()
				src.settleFirstAs = holdExpired
				tt.set(src)
				src.queue = []string{`{"status":"PAID","id":"o-1"}`}
				src.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var v *servicehost.ViolationError
				if ex, err := d.Poll(ctx, 0); !errors.As(err, &v) || !strings.Contains(v.Error(), tt.want) {
					t.Fatalf("exchange %+v, %v; want a violation holding %q", ex, err, tt.want)
				}
				_, err := os.Stat(filepath.Join(src.ask, LanePath(0, StepVerdict)))
				if (err == nil) != tt.verdict || (len(rec.receipts) == 1) != tt.verdict {
					t.Fatalf("verdict %v, receipts %+v; want both %v", err, rec.receipts, tt.verdict)
				}
			})
		})
	}
}

// TestLane_ACloseAtTheHoldBoundIsNoVerdictsOutcome: a container's own close
// at the hold bound ends the exchange as just that. Met before the verdict,
// no verdict is written, and the receipt says the message was given back
// unless a run was already started for it; met as the verdict is written,
// the verdict is ignored (docs/EVENT_SOURCE_IMAGES.md, "Leases and the hold
// bound").
func TestLane_ACloseAtTheHoldBoundIsNoVerdictsOutcome(t *testing.T) {
	back := Receipt{Outcome: artifacts.ReceiptReleased, Reason: eventsources.ReasonMessageReleasedHoldExpired}
	gone := Receipt{Outcome: artifacts.ReceiptDropped, Reason: eventsources.ReasonMessageReleasedHoldExpired}
	rejected := Receipt{Outcome: artifacts.ReceiptRejected, Reason: "MESSAGE_REJECTED"}
	accepted := Receipt{Outcome: artifacts.ReceiptAccepted, RunID: "ork-1"}
	dropped := Receipt{Outcome: artifacts.ReceiptDropped, Reason: "MESSAGE_REJECTED"}
	tests := []struct {
		name       string
		decided    Receipt
		atMostOnce bool
		afterward  bool // it lands as the verdict is written
		want       []Receipt
	}{
		{name: "a reject", decided: rejected, want: []Receipt{back}},
		{name: "a reject, crossing", decided: rejected, afterward: true, want: []Receipt{rejected, back}},
		{name: "an accept", decided: accepted, want: []Receipt{accepted}},
		{name: "an accept, crossing", decided: accepted, afterward: true, want: []Receipt{accepted}},
		{name: "at most once", decided: dropped, atMostOnce: true, want: []Receipt{gone}},
		{name: "at most once, crossing", decided: dropped, atMostOnce: true, afterward: true, want: []Receipt{dropped}},
		{name: "no receipt decided", want: []Receipt{back}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var clock laterClock
				d, src, rec := newDriverFixture(t, &fakeDoor{offers: []func(Offer) (Answer, error){unreachable[Offer]()}})
				d.Now, d.Judge.AtMostOnce = clock.now, tt.atMostOnce
				src.mu.Lock()
				src.settleFirstAs = holdExpired
				src.closeOnVerdict = tt.afterward
				src.queue = []string{`{"status":"PAID","id":"o-1"}`}
				src.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				held, err := d.Poll(ctx, 0)
				if err != nil || held.Kind != ExchangeHeld {
					t.Fatalf("%+v, %v; want it held", held, err)
				}
				clock.by.Store(int64(d.Limits.Hold))
				if !tt.afterward {
					src.rename(LanePath(0, StepSettled), []byte(`{"schemaVersion":1,"seq":1,`+holdExpired+`}`))
				}
				ex, err := d.Verdict(ctx, held.Message, Decision{Disposition: Reject, Action: ActionReject, Receipt: tt.decided})
				if err != nil || ex.Kind != ExchangeHoldExpired || !d.Lane.CanBegin() {
					t.Fatalf("%+v, %v; want the exchange closed at its hold bound", ex, err)
				}
				if !slices.Equal(rec.receipts, tt.want) {
					t.Fatalf("receipts %+v; want %+v", rec.receipts, tt.want)
				}
				if _, err := os.Stat(filepath.Join(src.ask, LanePath(0, StepVerdict))); (err == nil) != tt.afterward {
					t.Fatalf("verdict written: %v; want %v", err == nil, tt.afterward)
				}
			})
		})
	}
}

// TestLane_HoldExpired: a held message's close is taken at the bound and
// refused before it.
func TestLane_HoldExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var clock laterClock
		d, src, rec := newDriverFixture(t, &fakeDoor{offers: []func(Offer) (Answer, error){unreachable[Offer]()}}, `{"status":"PAID","id":"o-1"}`)
		d.Now = clock.now
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		held, err := d.Poll(ctx, 0)
		if err != nil || held.Kind != ExchangeHeld {
			t.Fatalf("%+v, %v; want it held", held, err)
		}
		src.rename(LanePath(0, StepSettled), []byte(`{"schemaVersion":1,"seq":1,`+holdExpired+`}`))
		var v *servicehost.ViolationError
		if _, err := d.HoldExpired(ctx, held.Message); !errors.As(err, &v) || !strings.Contains(v.Error(), "before its hold bound") {
			t.Fatalf("an early close: %v", err)
		}
		// At the bound (which clock says so: TestLane_AgeIsTheLongerClocks).
		clock.by.Store(int64(d.Limits.Hold))
		if expired, err := d.HoldExpired(ctx, held.Message); err != nil || !expired || len(rec.receipts) != 1 || !d.Lane.CanBegin() {
			t.Fatalf("a close at the bound: %v, %v, receipts %+v", expired, err, rec.receipts)
		}
	})
}

// apart is at as a host reads it once its wall clock has moved by wall and its
// monotonic one by mono, which no constructor of time gives: it moves the
// monotonic reading of a time.Time itself.
func apart(t *testing.T, at time.Time, wall, mono time.Duration) time.Time {
	t.Helper()
	later := at.Add(wall)
	(*struct {
		wall uint64
		ext  int64
		loc  *time.Location
	})(unsafe.Pointer(&later)).ext += int64(mono - wall)
	if later.Sub(at) != mono || later.Round(0).Sub(at.Round(0)) != wall {
		t.Fatalf("time.Time is not laid out as this test reads it: %v and %v", later.Sub(at), later.Round(0).Sub(at.Round(0)))
	}
	return later
}

// TestLane_AgeIsTheLongerClocks: a suspended host moves the wall clock alone,
// and one whose clock was set back the monotonic alone; an exchange is as old as
// the longer says.
func TestLane_AgeIsTheLongerClocks(t *testing.T) {
	polled := time.Now()
	for name, tc := range map[string]struct{ wall, mono time.Duration }{
		"both": {time.Hour, time.Hour}, "suspended": {time.Hour, time.Second}, "set back": {time.Second, time.Hour},
	} {
		d := &LaneDriver{polled: polled, Now: func() time.Time { return apart(t, polled, tc.wall, tc.mono) }}
		if got := d.age(); got != time.Hour {
			t.Errorf("%s: age %v, want an hour", name, got)
		}
	}
}

func TestLane_AStepNobodyAskedForEndsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{}
		d, src, _ := newDriverFixture(t, door)
		src.mute.Store(true)
		src.rename(LanePath(0, StepEmpty), []byte(`{"schemaVersion":1,"seq":5}`))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var v *servicehost.ViolationError
		if _, err := d.Poll(ctx, 0); !errors.As(err, &v) {
			t.Fatalf("an answer ahead of the lane's seq: %v", err)
		}
	})
}

// TestLane_AStaleFilteredVerdictIsHeld: a FILTERED decision whose
// confirmation aged while it waited is not written.
func TestLane_AStaleFilteredVerdictIsHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{confirm: []func() (Confirmation, error){func() (Confirmation, error) { return Confirmation{}, errors.New("timeout") }}}
		d, src, _ := newDriverFixture(t, door)
		d.Lane.Begin()
		stale := Decision{Disposition: Settle, Action: ActionAck, ConfirmedAt: time.Now().Add(-time.Minute),
			Receipt: Receipt{Outcome: artifacts.ReceiptFiltered}}
		ex, err := d.Verdict(context.Background(), HandedMessage{ID: "m"}, stale)
		if err != nil || ex.Kind != ExchangeHeld {
			t.Fatalf("%+v, %v", ex, err)
		}
		if _, err := os.Stat(filepath.Join(src.ask, LanePath(0, StepVerdict))); !os.IsNotExist(err) {
			t.Fatal("a stale FILTERED verdict was written")
		}
	})
}

func TestLane_AReceiptFailureIsReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)}}
		d, _, rec := newDriverFixture(t, door, `{"status":"PAID","id":"o-1"}`)
		rec.fail = true
		var reported []string
		d.Failed = func(kind string, _ error) { reported = append(reported, kind) }
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if ex, err := d.Poll(ctx, 0); err != nil || ex.Kind != ExchangeSettled || len(reported) != 1 || reported[0] != "receipt" {
			t.Fatalf("%+v, %v, reported %v: a best-effort receipt that failed is told, and the message settles", ex, err, reported)
		}
	})
}

func TestLane_WedgedPastTheHoldEndsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, _, _ := newDriverFixture(t, &fakeDoor{})
		now := time.Now()
		d.Now = func() time.Time { return now }
		d.Limits.Hold = time.Second
		d.Lane.Begin()
		d.Lane.Await(now, time.Millisecond)
		now = now.Add(10 * time.Millisecond)
		if err := d.tick(context.Background()); err != nil {
			t.Fatalf("wedged, within the hold: %v", err)
		}
		now = now.Add(2 * time.Second)
		if err := d.tick(context.Background()); !errors.Is(err, ErrUnresponsive) {
			t.Fatalf("wedged past the hold: %v", err)
		}
	})
}

// TestNewVerdictID: a verdict id is the lane's exchange and 128 random bits
// from crypto/rand, so no two verdicts share one and none is guessable.
func TestNewVerdictID(t *testing.T) {
	seen := map[string]bool{}
	for i := range 10000 {
		id := newVerdictID(int64(i % 3))
		seq, random, ok := strings.Cut(id, "-")
		if !ok || seq != itoa(int64(i%3)) || len(random) != 32 {
			t.Fatalf("verdict id %q: want the seq, a dash and 32 hex digits", id)
		}
		if seen[id] {
			t.Fatalf("two verdicts share the id %q", id)
		}
		seen[id] = true
	}
}
