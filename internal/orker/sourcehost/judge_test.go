package sourcehost

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// fakeDoor answers from scripts and records what it was asked. It stands
// for the engine's host service, which is OrkStack's own and lands with the
// source workflow; tests against the real engine replace it there. Here it
// produces the answers a real engine gives only under faults (a lost
// answer, REFRESH, UPLOAD_AGAIN for ever).
type fakeDoor struct {
	mu       sync.Mutex
	probe    func() error
	paused   []string
	confirm  []func() (Confirmation, error)
	offers   []func(Offer) (Answer, error)
	starts   []func(Start) (Answer, error)
	asked    []string
	lastOffr Offer
	lastStrt Start
}

func pop[T any](q *[]T) T {
	v := (*q)[0]
	if len(*q) > 1 {
		*q = (*q)[1:]
	}
	return v
}

func (d *fakeDoor) Confirm(context.Context, int64) (Confirmation, error) {
	d.mu.Lock()
	d.asked = append(d.asked, "confirm")
	f := pop(&d.confirm)
	d.mu.Unlock()
	return f()
}

func (d *fakeDoor) Offer(_ context.Context, o Offer) (Answer, error) {
	d.mu.Lock()
	d.asked, d.lastOffr = append(d.asked, "offer"), o
	f := pop(&d.offers)
	d.mu.Unlock()
	return f(o)
}

func (d *fakeDoor) Start(_ context.Context, s Start) (Answer, error) {
	d.mu.Lock()
	d.asked, d.lastStrt = append(d.asked, "start"), s
	f := pop(&d.starts)
	d.mu.Unlock()
	return f(s)
}

func (d *fakeDoor) Probe(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asked = append(d.asked, "probe")
	if d.probe != nil {
		return d.probe()
	}
	return nil
}

func (d *fakeDoor) RequestPause(_ context.Context, code string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.paused = append(d.paused, code)
	return nil
}

type fakeStager struct {
	err    error
	staged []Part
	bytes  [][]byte
}

func (s *fakeStager) Stage(_ context.Context, _ HandedMessage, p Part, b []byte) (StagedPart, error) {
	if s.err != nil {
		return StagedPart{}, s.err
	}
	s.staged, s.bytes = append(s.staged, p), append(s.bytes, b)
	return StagedPart{Input: p.Input, Size: int64(len(b))}, nil
}

func judgingSource(status string) *orkstackv1.EventSource {
	return &orkstackv1.EventSource{
		Id: "evs-0123456789abcdef", Namespace: "payments", Name: "orders", OrkflowName: "handle-order",
		Format:          orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON,
		Match:           []*orkstackv1.MatchRule{{Pointer: "/status", Equals: structpb.NewStringValue(status)}},
		Map:             map[string]*orkstackv1.MapRule{"order_id": {Pointer: "/id"}},
		JudgingRevision: 1,
	}
}

func answer(k AnswerKind) func(Offer) (Answer, error) {
	return func(Offer) (Answer, error) { return Answer{Kind: k, RunID: "ork-1", Reason: "MESSAGE_FIELD_TYPE"}, nil }
}
func startAnswer(k AnswerKind) func(Start) (Answer, error) {
	return func(Start) (Answer, error) { return Answer{Kind: k, RunID: "ork-1"}, nil }
}
func unreachable[T any]() func(T) (Answer, error) {
	return func(T) (Answer, error) { return Answer{}, errors.New("connection refused") }
}

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newTestJudge(door *fakeDoor, stager *fakeStager, c *clock) *Judge {
	j := NewJudge(judgingSource("PAID"), 1, door, stager, c.Now)
	j.Ref = SourceRef{ID: "evs-0123456789abcdef", Name: "orders", Image: "img@sha256:aa"}
	return j
}

func handed(body string, count int) (HandedMessage, []byte) {
	return HandedMessage{Seq: 1, ID: "m-1", BodyBytes: int64(len(body)), DeliveryCount: count}, []byte(body)
}

func TestJudge_AcceptedThroughStaging(t *testing.T) {
	door := &fakeDoor{
		offers: []func(Offer) (Answer, error){func(Offer) (Answer, error) {
			return Answer{Kind: AnswerProceed, Parts: []Part{{Input: "payload", Kind: "body"}, {Input: "meta", Kind: "metadata"}}}, nil
		}},
		starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)},
	}
	stager := &fakeStager{}
	j := newTestJudge(door, stager, &clock{now: t0})
	msg, body := handed(`{"status":"PAID","id":"o-1"}`, 1)
	d, err := j.Decide(context.Background(), msg, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.Disposition != Settle || d.Action != ActionAck || d.Receipt.Outcome != artifacts.ReceiptAccepted || d.Receipt.RunID != "ork-1" {
		t.Fatalf("decision %+v", d)
	}
	if string(door.lastOffr.Values["order_id"]) != `"o-1"` || len(door.lastStrt.Staged) != 2 || string(stager.bytes[0]) != string(body) {
		t.Fatalf("offer %+v, start %+v", door.lastOffr, door.lastStrt)
	}
}

func TestFiltered_NeedsAConfirmationFiveSecondsFresh(t *testing.T) {
	c := &clock{now: t0}
	door := &fakeDoor{confirm: []func() (Confirmation, error){func() (Confirmation, error) { return Confirmation{Revision: 1}, nil }}}
	j := newTestJudge(door, &fakeStager{}, c)
	msg, body := handed(`{"status":"OPEN"}`, 1)
	d, _ := j.Decide(context.Background(), msg, body)
	if d.Disposition != Settle || d.Receipt.Outcome != artifacts.ReceiptFiltered || len(door.asked) != 1 || door.asked[0] != "confirm" {
		t.Fatalf("decision %+v, asked %v", d, door.asked)
	}
	c.now = t0.Add(4 * time.Second)
	j.Decide(context.Background(), msg, body)
	if len(door.asked) != 1 {
		t.Fatal("a confirmation four seconds old was not enough")
	}
	c.now = t0.Add(10 * time.Second)
	j.Decide(context.Background(), msg, body)
	if len(door.asked) != 2 {
		t.Fatal("a confirmation ten seconds old was trusted")
	}
}

func TestFiltered_AnEngineThatDoesNotAnswerLeavesItParked(t *testing.T) {
	door := &fakeDoor{confirm: []func() (Confirmation, error){func() (Confirmation, error) { return Confirmation{}, errors.New("timeout") }}}
	j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
	msg, body := handed(`{"status":"OPEN"}`, 1)
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || d.Action != "" {
		t.Fatalf("decision %+v: a filter that cannot be confirmed acknowledges nothing", d)
	}
}

func TestJudging_SwappedWithoutARestart(t *testing.T) {
	door := &fakeDoor{
		confirm: []func() (Confirmation, error){func() (Confirmation, error) {
			return Confirmation{Revision: 2, Source: judgingSource("OPEN")}, nil
		}},
		offers: []func(Offer) (Answer, error){answer(AnswerProceed)},
		starts: []func(Start) (Answer, error){startAnswer(AnswerAccepted)},
	}
	j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
	msg, body := handed(`{"status":"OPEN","id":"o-1"}`, 1)
	d, _ := j.Decide(context.Background(), msg, body)
	if j.Revision() != 2 || d.Receipt.Outcome != artifacts.ReceiptAccepted {
		t.Fatalf("revision %d, decision %+v: the new match: selected the message", j.Revision(), d)
	}
}

func TestWait_EngineUnreachableIsAWait(t *testing.T) {
	for name, door := range map[string]*fakeDoor{
		"offer": {offers: []func(Offer) (Answer, error){unreachable[Offer]()}},
		"start": {offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){unreachable[Start]()}},
	} {
		j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
		msg, body := handed(`{"status":"PAID","id":"o-1"}`, 9)
		d, err := j.Decide(context.Background(), msg, body)
		if err != nil || d.Disposition != Wait || d.Action != "" || d.Receipt.Outcome != "" {
			t.Fatalf("%s: %+v, %v: an unanswered call never rejects, filters or settles", name, d, err)
		}
	}
}

func TestJudge_UploadFailureAndUploadAgain(t *testing.T) {
	j := newTestJudge(&fakeDoor{offers: []func(Offer) (Answer, error){func(Offer) (Answer, error) {
		return Answer{Kind: AnswerProceed, Parts: []Part{{Input: "p", Kind: "body"}}}, nil
	}}}, &fakeStager{err: errors.New("store down")}, &clock{now: t0})
	msg, body := handed(`{"status":"PAID","id":"o-1"}`, 1)
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || d.Cause != "upload" {
		t.Fatalf("a failed upload: %+v", d)
	}
	door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerProceed)}, starts: []func(Start) (Answer, error){startAnswer(AnswerUploadAgain)}}
	j = newTestJudge(door, &fakeStager{}, &clock{now: t0})
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || d.Cause != "upload_again" {
		t.Fatalf("UPLOAD_AGAIN for ever: %+v", d)
	}
	starts := 0
	for _, a := range door.asked {
		if a == "start" {
			starts++
		}
	}
	if starts != maxUploadAgain+1 || !door.lastStrt.Retry {
		t.Fatalf("%d starts, last retry %v", starts, door.lastStrt.Retry)
	}
}

func TestJudge_RejectPauseRefresh(t *testing.T) {
	msg, body := handed(`{"status":"PAID","id":"o-1"}`, 3)
	j := newTestJudge(&fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerReject)}}, &fakeStager{}, &clock{now: t0})
	d, _ := j.Decide(context.Background(), msg, body)
	if d.Disposition != Reject || d.Action != ActionReject || d.Delay != 4 || !d.Receipt.Required || d.Receipt.Reason != "MESSAGE_FIELD_TYPE" {
		t.Fatalf("reject %+v", d)
	}
	j = newTestJudge(&fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerStop)}}, &fakeStager{}, &clock{now: t0})
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Pause || d.Action != ActionRelease || d.Delay != 30 {
		t.Fatalf("stop %+v", d)
	}
	refresh := func(Offer) (Answer, error) {
		return Answer{Kind: AnswerRefresh, Source: judgingSource("PAID")}, nil
	}
	door := &fakeDoor{offers: []func(Offer) (Answer, error){refresh}}
	j = newTestJudge(door, &fakeStager{}, &clock{now: t0})
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || len(door.asked) != maxRefresh+1 {
		t.Fatalf("REFRESH for ever: %+v after %d offers", d, len(door.asked))
	}
	j = newTestJudge(&fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerUncertain)}}, &fakeStager{}, &clock{now: t0})
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || !d.Uncertain {
		t.Fatalf("uncertain %+v", d)
	}
}

func TestJudge_PreRejectionWaitsForTheClaim(t *testing.T) {
	door := &fakeDoor{offers: []func(Offer) (Answer, error){answer(AnswerDuplicate)}}
	j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
	msg, body := handed(`{"status":`, 1)
	d, _ := j.Decide(context.Background(), msg, body)
	if door.lastOffr.PreRejection != eventsources.ReasonMessageNotJSON || d.Receipt.Outcome != artifacts.ReceiptDuplicate {
		t.Fatalf("a pre-rejected message of a run that exists: %+v, offered %+v", d, door.lastOffr)
	}
	over := HandedMessage{Seq: 1, ID: "m-2", Oversize: true, OversizeBytes: 5 << 20}
	j.Decide(context.Background(), over, nil)
	if door.lastOffr.PreRejection != eventsources.ReasonMessageTooLarge || door.lastOffr.BodySize != 5<<20 {
		t.Fatalf("an oversize message: %+v", door.lastOffr)
	}
}

func TestJudge_BodyMismatchIsAViolation(t *testing.T) {
	j := newTestJudge(&fakeDoor{}, &fakeStager{}, &clock{now: t0})
	msg, _ := handed(`{"a":1}`, 1)
	if _, err := j.Decide(context.Background(), msg, []byte("short")); !errors.Is(err, ErrBodyMismatch) {
		t.Fatalf("err = %v", err)
	}
}

func TestLastDelivery_AlwaysLeavesADeadLetteredReceipt(t *testing.T) {
	rej := Decision{Disposition: Reject, Receipt: Receipt{Outcome: artifacts.ReceiptRejected, Reason: "MESSAGE_FIELD_TYPE"}}
	if r, ok := DeadLettered(rej, 5, 5, false); !ok || r.Outcome != artifacts.ReceiptDeadLettered || !r.Required ||
		r.Reason != eventsources.ReasonMessageDeliveriesExhausted || r.Cause != "MESSAGE_FIELD_TYPE" {
		t.Fatalf("dead letter %+v, %v", r, ok)
	}
	wait := Decision{Disposition: Wait, Cause: "engine_unreachable"}
	if r, ok := DeadLettered(wait, 10, 5, true); !ok || r.Outcome != artifacts.ReceiptDropped || r.Cause != "engine_unreachable" {
		t.Fatalf("at-most-once %+v", r)
	}
	if _, ok := DeadLettered(rej, 4, 5, false); ok {
		t.Fatal("below the limit")
	}
	if _, ok := DeadLettered(Decision{Disposition: Settle}, 9, 5, false); ok {
		t.Fatal("a settled message is not dead-lettered")
	}
	if _, ok := DeadLettered(rej, 9, 0, false); ok {
		t.Fatal("no known limit")
	}
}

func TestRejectDelay(t *testing.T) {
	for count, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 4, 5: 16, 6: 30, 50: 30} {
		if got := RejectDelay(count); got != want {
			t.Errorf("RejectDelay(%d) = %d; want %d", count, got, want)
		}
	}
}

var t0 = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// TestJudging_RefreshThenFilteredSettles: the revision a REFRESH brings is
// the one later confirmations ask about, so a FILTERED message after it
// settles instead of waiting for ever.
func TestJudging_RefreshThenFilteredSettles(t *testing.T) {
	fresh := judgingSource("PAID")
	fresh.JudgingRevision = 2
	door := &fakeDoor{
		offers:  []func(Offer) (Answer, error){func(Offer) (Answer, error) { return Answer{Kind: AnswerRefresh, Source: fresh}, nil }},
		confirm: []func() (Confirmation, error){func() (Confirmation, error) { return Confirmation{Revision: 2}, nil }},
	}
	c := &clock{now: t0}
	j := newTestJudge(door, &fakeStager{}, c)
	msg, body := handed(`{"status":"OPEN"}`, 1)
	j.source = judgingSource("OPEN")
	d, _ := j.Decide(context.Background(), msg, body)
	if j.Revision() != 2 {
		t.Fatalf("revision %d after a refresh to 2", j.Revision())
	}
	c.now = t0.Add(time.Minute)
	d, _ = j.Decide(context.Background(), msg, body)
	if d.Disposition != Settle || d.Receipt.Outcome != artifacts.ReceiptFiltered {
		t.Fatalf("after a refresh, a filtered message: %+v", d)
	}
}

func TestJudging_ARefreshWithoutASpecConfirmsNothing(t *testing.T) {
	door := &fakeDoor{offers: []func(Offer) (Answer, error){func(Offer) (Answer, error) { return Answer{Kind: AnswerRefresh}, nil }}}
	j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
	msg, body := handed(`{"status":"PAID","id":"o-1"}`, 1)
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait {
		t.Fatalf("%+v", d)
	}
	if _, _, at := j.held(); !at.IsZero() {
		t.Fatal("a refresh without a spec set the confirmation")
	}
}

func TestJudge_AnUnknownPartKindWaits(t *testing.T) {
	door := &fakeDoor{offers: []func(Offer) (Answer, error){func(Offer) (Answer, error) {
		return Answer{Kind: AnswerProceed, Parts: []Part{{Input: "p", Kind: "headers"}}}, nil
	}}}
	j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
	msg, body := handed(`{"status":"PAID","id":"o-1"}`, 1)
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || d.Cause != "unknown_part" {
		t.Fatalf("%+v", d)
	}
}

// TestJudge_SharedByLanes: lanes decide and swap concurrently (run with
// -race).
func TestJudge_SharedByLanes(t *testing.T) {
	door := &fakeDoor{confirm: []func() (Confirmation, error){func() (Confirmation, error) { return Confirmation{Revision: 1}, nil }}}
	j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
	msg, body := handed(`{"status":"OPEN"}`, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for k := 0; k < 50; k++ {
			_, _ = j.Decide(context.Background(), msg, body)
		}
	}()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 50; k++ {
				j.Swap(judgingSource("PAID"), 1, t0)
				_ = j.Revision()
			}
		}()
	}
	wg.Wait()
}

// TestJudging_NeverGoesBackToAnOlderRevision: a lagging replica's older
// spec is not taken, by a confirmation or a refresh.
func TestJudging_NeverGoesBackToAnOlderRevision(t *testing.T) {
	old := judgingSource("OPEN")
	old.JudgingRevision = 1
	door := &fakeDoor{confirm: []func() (Confirmation, error){func() (Confirmation, error) { return Confirmation{Revision: 1, Source: old}, nil }}}
	j := newTestJudge(door, &fakeStager{}, &clock{now: t0})
	j.Swap(judgingSource("PAID"), 2, time.Time{})
	msg, body := handed(`{"status":"OPEN"}`, 1)
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || d.Cause != "rollout" || j.Revision() != 2 {
		t.Fatalf("an older confirmation: %+v, revision %d", d, j.Revision())
	}
	door = &fakeDoor{offers: []func(Offer) (Answer, error){func(Offer) (Answer, error) { return Answer{Kind: AnswerRefresh, Source: old}, nil }}}
	j = newTestJudge(door, &fakeStager{}, &clock{now: t0})
	j.Swap(judgingSource("PAID"), 2, time.Time{})
	msg, body = handed(`{"status":"PAID","id":"o-1"}`, 1)
	if d, _ := j.Decide(context.Background(), msg, body); d.Disposition != Wait || j.Revision() != 2 {
		t.Fatalf("an older refresh: %+v, revision %d", d, j.Revision())
	}
}
