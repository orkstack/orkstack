package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const recID = "evs-0123456789abcdef"

func receiptParts() ReceiptKeyParts {
	return ReceiptKeyParts{Namespace: "payments", SourceID: recID, Taken: time.Date(2026, 11, 11, 9, 30, 0, 123456789, time.UTC),
		Delivery: 1, Epoch: 12, Lane: 3, Seq: 42, Outcome: ReceiptAccepted, Identity: "jetstream:ORDERS:48213:1762853400123456789"}
}

func TestReceiptKey_SourceFirst(t *testing.T) {
	key, err := ReceiptKey(receiptParts())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(receiptParts().Identity))
	want := "metadata/receipts/payments/" + recID + "/2026/11/11/20261111T093000.123456789Z-0001-12-3-42-accepted-" + hex.EncodeToString(sum[:])[:16] + ".json"
	if key != want {
		t.Fatalf("key\n%s\nwant\n%s", key, want)
	}
	prefix, err := ReceiptSourcePrefix("payments", recID)
	if err != nil || !strings.HasPrefix(key, prefix) {
		t.Fatalf("the receipt %s is not under its source's prefix %s (%v)", key, prefix, err)
	}
	local := receiptParts()
	local.Taken = local.Taken.In(time.FixedZone("x", 5*3600))
	if k, _ := ReceiptKey(local); k != key {
		t.Fatalf("the same instant in another zone named another key: %s", k)
	}
}

func TestReceiptKey_RetryIsOneObjectAndDeliveriesAreTwo(t *testing.T) {
	a, _ := ReceiptKey(receiptParts())
	b, _ := ReceiptKey(receiptParts())
	next := receiptParts()
	next.Delivery = 2
	c, _ := ReceiptKey(next)
	if a != b || a == c {
		t.Fatalf("retry %q vs %q; next delivery %q", a, b, c)
	}
}

func TestReceiptKey_Refusals(t *testing.T) {
	for name, edit := range map[string]func(*ReceiptKeyParts){
		"namespace":   func(p *ReceiptKeyParts) { p.Namespace = "Pay/ments" },
		"id":          func(p *ReceiptKeyParts) { p.SourceID = "evs-../../x" },
		"no instant":  func(p *ReceiptKeyParts) { p.Taken = time.Time{} },
		"negative":    func(p *ReceiptKeyParts) { p.Seq = -1 },
		"outcome":     func(p *ReceiptKeyParts) { p.Outcome = "accepted/../x" },
		"no identity": func(p *ReceiptKeyParts) { p.Identity = "" },
		"long":        func(p *ReceiptKeyParts) { p.Identity = strings.Repeat("i", 257) },
	} {
		p := receiptParts()
		edit(&p)
		if key, err := ReceiptKey(p); err == nil {
			t.Errorf("%s: key %q", name, key)
		}
	}
}

func TestStagingKey_NamesNoRun(t *testing.T) {
	digest := strings.Repeat("a", 64)
	key, err := StagingKey("payments", recID, "msg-1", digest)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("msg-1"))
	if key != EventSourceStagingPrefix+"/payments/"+recID+"/"+hex.EncodeToString(sum[:])+"/"+digest || strings.Contains(key, "ork-") {
		t.Fatalf("key %s", key)
	}
	for _, bad := range [][4]string{{"Bad", recID, "m", digest}, {"p", "x", "m", digest}, {"p", recID, "", digest}, {"p", recID, "m", "../x"}} {
		if k, err := StagingKey(bad[0], bad[1], bad[2], bad[3]); err == nil {
			t.Errorf("StagingKey(%v) = %q", bad, k)
		}
	}
}

func TestSourceLogAndAttemptKeys_HoldTheEpoch(t *testing.T) {
	a := InstanceAttempt{SourceID: recID, Index: 1, Epoch: 7, Attempt: 2}
	logKey, err := SourceLogKey(a, 3)
	if err != nil || logKey != "metadata/logs/"+recID+"/_source/00001/000000000007-0002/3" {
		t.Fatalf("log key %q, %v", logKey, err)
	}
	attemptKey, err := SourceAttemptKey(a)
	if err != nil || attemptKey != "metadata/attempts/"+recID+"/_source/00001/000000000007-0002.json" {
		t.Fatalf("attempt key %q, %v", attemptKey, err)
	}
	for _, bad := range []InstanceAttempt{{SourceID: "x"}, {SourceID: recID, Index: -1}, {SourceID: recID, Epoch: 1e12}, {SourceID: recID, Attempt: 10000}} {
		if _, err := SourceAttemptKey(bad); err == nil {
			t.Errorf("SourceAttemptKey(%+v) accepted", bad)
		}
	}
	if _, err := SourceLogKey(a, -1); err == nil {
		t.Error("a negative seq accepted")
	}
}

func TestPutReceipt_CreateOnlyAlreadyExistsIsSuccess(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalEventSourceRecordStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	key, _ := ReceiptKey(receiptParts())
	ctx := context.Background()
	if err := s.PutReceipt(ctx, key, []byte(`{"first":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutReceipt(ctx, key, []byte(`{"second":true}`)); err != nil {
		t.Fatalf("a retried receipt write failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(key)))
	if err != nil || string(got) != `{"first":true}` {
		t.Fatalf("stored %q, %v: a receipt is never replaced", got, err)
	}
	for name, tc := range map[string]struct {
		key  string
		body []byte
	}{
		"outside the prefix": {"metadata/event-sources/x.json", []byte("{}")},
		"dot-dot":            {"metadata/receipts/../event-sources/x.json", []byte("{}")},
		"empty segment":      {"metadata/receipts//x.json", []byte("{}")},
		"dot":                {"metadata/receipts/./x.json", []byte("{}")},
		"empty":              {key, nil},
		"over 16 KiB":        {key, make([]byte, MaxReceiptBytes+1)},
	} {
		if err := s.PutReceipt(ctx, tc.key, tc.body); err == nil {
			t.Errorf("%s: written", name)
		}
	}
}

func TestPutStaged_DigestIsTheKey(t *testing.T) {
	s, err := NewLocalEventSourceRecordStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	body := []byte("a message body")
	sum := sha256.Sum256(body)
	key, _ := StagingKey("payments", recID, "msg-1", hex.EncodeToString(sum[:]))
	ctx := context.Background()
	if err := s.PutStaged(ctx, key, body); err != nil {
		t.Fatal(err)
	}
	if err := s.PutStaged(ctx, key, body); err != nil {
		t.Fatalf("a part already staged: %v", err)
	}
	if err := s.PutStaged(ctx, key, []byte("other bytes")); err == nil {
		t.Fatal("bytes that do not match the key's digest were staged")
	}
	if err := s.PutStaged(ctx, ReceiptPrefix+"/x/"+hex.EncodeToString(sum[:]), body); err == nil {
		t.Fatal("a part was staged outside the staging prefix")
	}
}

func testReceipt() *orkstackv1.Receipt {
	return &orkstackv1.Receipt{Id: "rcp-1", Namespace: "payments", EventSourceId: recID, EventSourceName: "orders", MessageId: "m-1",
		DeliveryCount: 2, MaxDeliveries: 10, Outcome: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_ACCEPTED, RunId: "ork-1",
		Broker: map[string]string{"stream": "ORDERS"}, Instance: 1, OrkerId: "orker-a"}
}

func TestMarshalReceipt_Bounds(t *testing.T) {
	raw, err := MarshalReceipt(testReceipt())
	if err != nil {
		t.Fatal(err)
	}
	back, err := UnmarshalReceiptRecord(raw)
	if err != nil || !proto.Equal(back, testReceipt()) {
		t.Fatalf("%v, %v", back, err)
	}
	for name, edit := range map[string]func(*orkstackv1.Receipt){
		"no outcome": func(r *orkstackv1.Receipt) { r.Outcome = 0 },
		"bad id":     func(r *orkstackv1.Receipt) { r.EventSourceId = "x" },
		"no message": func(r *orkstackv1.Receipt) { r.MessageId = "" },
		"many facts": func(r *orkstackv1.Receipt) {
			for i := 0; i < 17; i++ {
				r.Broker[string(rune('a'+i))] = "x"
			}
		},
		"too large": func(r *orkstackv1.Receipt) { r.Cause = strings.Repeat("c", MaxReceiptBytes) },
		"negative":  func(r *orkstackv1.Receipt) { r.DeliveryCount = -1 },
	} {
		r := testReceipt()
		edit(r)
		if _, err := MarshalReceipt(r); err == nil {
			t.Errorf("%s: marshalled", name)
		}
	}
	if _, err := UnmarshalReceiptRecord([]byte(`{"outcome":"RECEIPT_OUTCOME_ACCEPTED","later":1}`)); err == nil {
		t.Error("an unknown field read")
	}
}

func FuzzReceiptRecord(f *testing.F) {
	raw, _ := MarshalReceipt(testReceipt())
	f.Add(raw)
	f.Add([]byte(`{"outcome":"RECEIPT_OUTCOME_DROPPED","namespace":"p","eventSourceId":"evs-0123456789abcdef","messageId":"m"}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		r, err := UnmarshalReceiptRecord(raw)
		if err != nil {
			return
		}
		again, err := MarshalReceipt(r)
		if err != nil {
			t.Fatalf("a receipt that reads does not write: %v", err)
		}
		back, err := UnmarshalReceiptRecord(again)
		if err != nil || !proto.Equal(back, r) {
			t.Fatalf("a receipt does not round-trip: %v", err)
		}
	})
}
