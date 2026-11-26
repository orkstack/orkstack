package sourcehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// failingStore is a real record store whose next writes fail as a store that is
// down does, which a real one does not on demand, and that keeps the deadline a
// write was given.
type failingStore struct {
	*artifacts.EventSourceRecordStore
	fail     int
	deadline time.Duration
}

func (f *failingStore) down(ctx context.Context) error {
	if at, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(at)
	}
	if f.fail > 0 {
		f.fail--
		return errors.New("store down")
	}
	return nil
}

func (f *failingStore) PutReceipt(ctx context.Context, key string, body []byte) error {
	if err := f.down(ctx); err != nil {
		return err
	}
	return f.EventSourceRecordStore.PutReceipt(ctx, key, body)
}

func (f *failingStore) PutStaged(ctx context.Context, key string, body []byte) error {
	if err := f.down(ctx); err != nil {
		return err
	}
	return f.EventSourceRecordStore.PutStaged(ctx, key, body)
}

var taken = time.Date(2026, 11, 25, 14, 2, 3, 456, time.UTC)

func newRecords(t *testing.T) (*Records, *failingStore, string) {
	t.Helper()
	dir := t.TempDir()
	local, err := artifacts.NewLocalEventSourceRecordStore(dir)
	must.OK(t, err)
	store := &failingStore{EventSourceRecordStore: local}
	return &Records{Store: store, OrkerID: "orker-1", Now: func() time.Time { return taken.Add(time.Second) },
		Instance: &orkstackv1.InstanceRef{Namespace: "payments", Name: "orders", EventSourceId: doorSourceID, Index: 2, Epoch: 7, Attempt: 1},
		Provider: func() string { return "jetstream" },
		Limit:    func() (int, time.Time) { return 10, taken.Add(-time.Minute) }}, store, dir
}

// written is every object under dir, by key.
func written(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	must.OK(t, filepath.WalkDir(dir, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		rel, _ := filepath.Rel(dir, path)
		out[rel] = raw
		return err
	}))
	return out
}

func delivered(id string) HandedMessage {
	return HandedMessage{Seq: 41, ID: id, BodyBytes: 12, BodySHA256: strings.Repeat("a", 64), DeliveryCount: 3, Taken: taken,
		Broker: map[string]string{"stream": "ORDERS"}, Subject: "never", Attributes: map[string][]string{"k": {"never"}}}
}

// TestRecords_AReceipt: a receipt names its delivery, its instance and its
// Orker, holds nothing of the message but its identity, size and digest, and its
// time is when the message was taken, so a write made again is the same object.
func TestRecords_AReceipt(t *testing.T) {
	r, store, dir := newRecords(t)
	lane := r.Lane(4)
	must.OK(t, lane.Record(t.Context(), delivered("m-1"), 41, Receipt{Outcome: artifacts.ReceiptAccepted, RunID: "ork-1"}))
	if store.deadline <= 0 || store.deadline > 10*time.Second {
		t.Errorf("a receipt was given %v; want at most 10 s", store.deadline)
	}
	r.Now = func() time.Time { return taken.Add(time.Hour) }
	must.OK(t, lane.Record(t.Context(), delivered("m-1"), 41, Receipt{Outcome: artifacts.ReceiptAccepted, RunID: "ork-1"}))
	got := written(t, dir)
	key := "metadata/receipts/payments/" + doorSourceID + "/2026/11/25/20261125T140203.000000456Z-0003-7-4-41-accepted-" + hash("m-1")[:16] + ".json"
	if len(got) != 1 || got[key] == nil {
		t.Fatalf("wrote %d objects; want the one %s", len(got), key)
	}
	rec, err := artifacts.UnmarshalReceiptRecord(got[key])
	must.OK(t, err)
	want := &orkstackv1.Receipt{Id: "rcp-20261125T140203.000000456Z", Namespace: "payments", EventSourceId: doorSourceID, EventSourceName: "orders",
		Provider: "jetstream", MessageId: "m-1", DeliveryCount: 3, MaxDeliveries: 10, BodySha256: strings.Repeat("a", 64), BodyBytes: 12,
		Outcome: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_ACCEPTED, RunId: "ork-1", ReceivedAt: timestamppb.New(taken),
		DecidedAt: timestamppb.New(taken.Add(time.Second)), Broker: map[string]string{"stream": "ORDERS"},
		LimitCheckedAt: timestamppb.New(taken.Add(-time.Minute)), Instance: 2, OrkerId: "orker-1"}
	if !proto.Equal(rec, want) {
		t.Fatalf("got  %v\nwant %v", rec, want)
	}
}

// TestRecords_EachOutcome: each outcome has its own word, and a reason its
// sentence; an outcome this release does not know is never written.
func TestRecords_EachOutcome(t *testing.T) {
	for outcome, want := range map[artifacts.ReceiptOutcome]orkstackv1.ReceiptOutcome{
		artifacts.ReceiptAccepted: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_ACCEPTED, artifacts.ReceiptDuplicate: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_DUPLICATE,
		artifacts.ReceiptFiltered: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_FILTERED, artifacts.ReceiptRejected: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_REJECTED,
		artifacts.ReceiptReleased: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_RELEASED, artifacts.ReceiptDropped: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_DROPPED,
		artifacts.ReceiptDeadLettered: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_DEAD_LETTERED,
	} {
		r, _, dir := newRecords(t)
		must.OK(t, r.Lane(0).Record(t.Context(), delivered("m"), 1, Receipt{Outcome: outcome, Reason: eventsources.ReasonMessageFieldType, Cause: "POOL_NOT_ALLOWED"}))
		for _, raw := range written(t, dir) {
			rec, err := artifacts.UnmarshalReceiptRecord(raw)
			must.OK(t, err)
			if rec.GetOutcome() != want || rec.GetReason() != "MESSAGE_FIELD_TYPE" || rec.GetCause() != "POOL_NOT_ALLOWED" ||
				rec.GetMessage() != eventsources.MessageSentence("MESSAGE_FIELD_TYPE") {
				t.Errorf("%s: %v", outcome, rec)
			}
		}
	}
	r, _, dir := newRecords(t)
	if err := r.Lane(0).Record(t.Context(), delivered("m"), 1, Receipt{Outcome: "later"}); err == nil || len(written(t, dir)) != 0 {
		t.Errorf("an outcome nobody knows: %v", err)
	}
}

// TestRecords_WhatAReceiptCannotHold: a message too large to have been read has
// its size and no digest; an identity over its bound is held to it, and a broker
// whose facts do not fit loses them, because the receipt is what says the
// message was refused.
func TestRecords_WhatAReceiptCannotHold(t *testing.T) {
	facts := map[string]string{}
	for i := range 16 {
		facts[strings.Repeat("k", 60)+string(rune('a'+i))] = strings.Repeat("\x01", 400)
	}
	for name, tc := range map[string]struct {
		msg   HandedMessage
		check func(*orkstackv1.Receipt) bool
	}{
		"too large": {HandedMessage{ID: "m", Oversize: true, OversizeBytes: 99, BodySHA256: "e3b0", Taken: taken},
			func(r *orkstackv1.Receipt) bool { return r.GetBodyBytes() == 99 && r.GetBodySha256() == "" }},
		"an identity over its bound": {HandedMessage{ID: strings.Repeat("\u00e9", eventsources.MaxIdentityBytes), Taken: taken},
			func(r *orkstackv1.Receipt) bool {
				return r.GetMessageId() == strings.Repeat("\u00e9", eventsources.MaxIdentityBytes/2)
			}},
		"an identity cut within a letter": {HandedMessage{ID: "a" + strings.Repeat("\u00e9", eventsources.MaxIdentityBytes), Taken: taken},
			func(r *orkstackv1.Receipt) bool {
				return r.GetMessageId() == "a"+strings.Repeat("\u00e9", eventsources.MaxIdentityBytes/2-1)
			}},
		"an identity that is not text": {HandedMessage{ID: "a\xffb", Taken: taken}, func(r *orkstackv1.Receipt) bool { return r.GetMessageId() == "a\ufffdb" }},
		"facts that do not fit":        {HandedMessage{ID: "m", Broker: facts, Taken: taken}, func(r *orkstackv1.Receipt) bool { return len(r.GetBroker()) == 0 && r.GetReason() != "" }},
		"a count no broker gives":      {HandedMessage{ID: "m", DeliveryCount: 1 << 40, Taken: taken}, func(r *orkstackv1.Receipt) bool { return r.GetDeliveryCount() == 1<<31-1 }},
	} {
		r, _, dir := newRecords(t)
		must.OKf(t, r.Lane(0).Record(t.Context(), tc.msg, 1, Receipt{Outcome: artifacts.ReceiptRejected, Reason: "MESSAGE_IDENTITY_INVALID", Required: true}), "%s", name)
		for _, raw := range written(t, dir) {
			rec, err := artifacts.UnmarshalReceiptRecord(raw)
			must.OK(t, err)
			if !tc.check(rec) {
				t.Errorf("%s: %v", name, rec)
			}
		}
		if n := len(written(t, dir)); n != 1 {
			t.Errorf("%s: %d receipts", name, n)
		}
	}
}

// TestRecords_WhatIsWrittenOnce: filtered messages are sampled, one in ten
// minutes whatever the lane, and a message's rejection is recorded the first time
// in a run of rejections; a write that failed is neither.
func TestRecords_WhatIsWrittenOnce(t *testing.T) {
	r, store, dir := newRecords(t)
	now := taken
	r.Now = func() time.Time { return now }
	n := int64(0)
	put := func(lane int, id string, outcome artifacts.ReceiptOutcome) error {
		n++
		return r.Lane(lane).Record(t.Context(), delivered(id), n, Receipt{Outcome: outcome, Sampled: outcome == artifacts.ReceiptFiltered})
	}
	expect := func(what string, want int) {
		t.Helper()
		if got := len(written(t, dir)); got != want {
			t.Errorf("%s: %d receipts, want %d", what, got, want)
		}
	}
	store.fail = 1
	if err := put(0, "f-1", artifacts.ReceiptFiltered); err == nil {
		t.Error("a store that is down said nothing")
	}
	must.OK(t, put(0, "f-1", artifacts.ReceiptFiltered))
	must.OK(t, put(1, "f-2", artifacts.ReceiptFiltered))
	expect("two filtered", 1)
	now = now.Add(10*time.Minute - time.Nanosecond)
	must.OK(t, put(0, "f-3", artifacts.ReceiptFiltered))
	expect("filtered within ten minutes", 1)
	now = now.Add(time.Nanosecond)
	must.OK(t, put(0, "f-3", artifacts.ReceiptFiltered))
	expect("filtered after ten minutes", 2)

	store.fail = 1
	if err := put(0, "r-1", artifacts.ReceiptRejected); err == nil {
		t.Error("a store that is down said nothing")
	}
	must.OK(t, put(0, "r-1", artifacts.ReceiptRejected))
	must.OK(t, put(1, "r-1", artifacts.ReceiptRejected))
	must.OK(t, put(1, "r-2", artifacts.ReceiptRejected))
	expect("two messages rejected, one of them twice", 4)
	must.OK(t, put(0, "r-1", artifacts.ReceiptDeadLettered))
	expect("a dead letter is never held back", 5)
	must.OK(t, put(0, "a", artifacts.ReceiptDuplicate))
	must.OK(t, put(0, "r-1", artifacts.ReceiptRejected))
	expect("a message accepted between two rejections", 7)
	for range 2 {
		must.OK(t, put(0, "d", artifacts.ReceiptDropped))
	}
	expect("each drop", 9)
	// An image names messages as it likes: what is remembered of one is not
	// its name.
	must.OK(t, put(0, strings.Repeat("l", 1<<20), artifacts.ReceiptRejected))
	if size := unsafe.Sizeof(r.rejectedKey("")); size > sha256.Size {
		t.Errorf("a rejected message costs %d bytes", size)
	}
}

// TestRecords_Stage: a part is stored under its message and its own digest, the
// same bytes of another delivery are the same object, and the input is the
// offer's.
func TestRecords_Stage(t *testing.T) {
	r, store, dir := newRecords(t)
	body := []byte(`{"a":1}`)
	sum := sha256.Sum256(body)
	want := StagedPart{Input: "order", Digest: hex.EncodeToString(sum[:]), Size: 7}
	for range 2 {
		got, err := r.Stage(t.Context(), delivered("m-1"), Part{Input: "order", Kind: PartBody}, body)
		if err != nil || got != want {
			t.Fatalf("%+v, %v", got, err)
		}
	}
	key := "metadata/staging/event-sources/payments/" + doorSourceID + "/" + hash("m-1") + "/" + want.Digest
	if got := written(t, dir); len(got) != 1 || string(got[key]) != string(body) {
		t.Fatalf("wrote %v; want %s", got, key)
	}
	store.fail = 1
	if _, err := r.Stage(t.Context(), delivered("m-2"), Part{Input: "order"}, body); err == nil {
		t.Error("a store that is down staged a part")
	}
	if store.deadline <= 0 || store.deadline > 60*time.Second {
		t.Errorf("a part was given %v; want at most 60 s", store.deadline)
	}
}

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
