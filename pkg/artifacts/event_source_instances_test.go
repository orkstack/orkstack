package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

var confirmAttempt = InstanceAttempt{SourceID: recID, Index: 3, Epoch: 12, Attempt: 2}

func instanceStores(t *testing.T) map[string]*EventSourceInstanceStore {
	t.Helper()
	local, err := NewLocalEventSourceInstanceStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := NewEventSourceInstanceStore(context.Background(), newS3Double(t).config())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*EventSourceInstanceStore{"local": local, "s3": remote}
}

func confirmRecord(orker string) ConfirmRecord {
	return ConfirmRecord{OrkerID: orker, ConfirmedAt: claimAt, ServiceDigest: strings.Repeat("a", 64), AdmissionSave: "save-1"}
}

// The attempt's first confirmation is what every later one is held to: of
// many that confirm at once, one record holds and each is answered with it.
func TestConfirmRecord_FirstOneHolds(t *testing.T) {
	for name, s := range instanceStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, found, err := s.Confirmed(ctx, confirmAttempt); err != nil || found {
				t.Fatalf("before any: found %v, %v", found, err)
			}
			const n = 10
			got := make([]ConfirmRecord, n)
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := range n {
				wg.Go(func() {
					got[i], errs[i] = s.Confirm(ctx, confirmAttempt, confirmRecord("orker-"+string(rune('a'+i))))
				})
			}
			wg.Wait()
			for i := range n {
				if errs[i] != nil || got[i] != got[0] {
					t.Fatalf("confirmation %d: %+v, %v; want the one that holds, %+v", i, got[i], errs[i], got[0])
				}
			}
			read, found, err := s.Confirmed(ctx, confirmAttempt)
			if err != nil || !found || read != got[0] {
				t.Fatalf("read %+v, %v, %v", read, found, err)
			}
			next := confirmAttempt
			next.Attempt++
			if _, found, err := s.Confirmed(ctx, next); err != nil || found {
				t.Fatalf("another attempt reads this one's record: %v, %v", found, err)
			}
		})
	}
}

func TestConfirmRecord_Refusals(t *testing.T) {
	for name, s := range instanceStores(t) {
		ctx := context.Background()
		for what, edit := range map[string]func(*ConfirmRecord){
			"no Orker":   func(r *ConfirmRecord) { r.OrkerID = "" },
			"no instant": func(r *ConfirmRecord) { r.ConfirmedAt = time.Time{} },
			"no digest":  func(r *ConfirmRecord) { r.ServiceDigest = "" },
			"a bad save": func(r *ConfirmRecord) { r.AdmissionSave = strings.Repeat("s", 300) },
		} {
			r := confirmRecord("orker-a")
			edit(&r)
			if _, err := s.Confirm(ctx, confirmAttempt, r); err == nil {
				t.Errorf("%s: %s was written", name, what)
			}
		}
		bad := confirmAttempt
		bad.SourceID = "evs-../x"
		if _, err := s.Confirm(ctx, bad, confirmRecord("orker-a")); err == nil {
			t.Errorf("%s: a record of no source was written", name)
		}
	}
}

// Only "not found" is absence: a record that cannot be read is no answer.
func TestConfirmRecord_UnreadableIsNoAnswer(t *testing.T) {
	for _, body := range []string{"", "{", "{}", `{"schemaVersion":9}`, `{"schemaVersion":1,"orkerId":"o","more":1}`} {
		for name, s := range instanceStores(t) {
			key, err := confirmKey(confirmAttempt)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.b.create(context.Background(), key, "application/json", []byte(body)); err != nil {
				t.Fatal(err)
			}
			if _, found, err := s.Confirmed(context.Background(), confirmAttempt); err == nil || found {
				t.Errorf("%s %q: found %v, %v", name, body, found, err)
			}
			if _, err := s.Confirm(context.Background(), confirmAttempt, confirmRecord("orker-a")); err == nil {
				t.Errorf("%s %q: confirmed over it", name, body)
			}
		}
	}
}

// The tombstone's key being there is what is read, whatever it holds, and
// a tombstone written again is the one that is there.
func TestTombstone_TheKeyIsWhatIsRead(t *testing.T) {
	for name, s := range instanceStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if found, err := s.Tombstoned(ctx, recID); err != nil || found {
				t.Fatalf("before a delete: %v, %v", found, err)
			}
			for range 2 {
				if err := s.Tombstone(ctx, recID, "payments", "orders", claimAt); err != nil {
					t.Fatal(err)
				}
			}
			if found, err := s.Tombstoned(ctx, recID); err != nil || !found {
				t.Fatalf("after a delete: %v, %v", found, err)
			}
			const other = "evs-fedcba9876543210"
			if found, err := s.Tombstoned(ctx, other); err != nil || found {
				t.Fatalf("another source's: %v, %v", found, err)
			}
			key, err := tombstoneKey(other)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.b.create(ctx, key, "application/json", []byte("not a document")); err != nil {
				t.Fatal(err)
			}
			if found, err := s.Tombstoned(ctx, other); err != nil || !found {
				t.Fatalf("one that does not decode: %v, %v", found, err)
			}
			for _, bad := range []string{"", "evs-1", "evs-../../x"} {
				if _, err := s.Tombstoned(ctx, bad); err == nil {
					t.Errorf("%q was read", bad)
				}
				if err := s.Tombstone(ctx, bad, "payments", "orders", claimAt); err == nil {
					t.Errorf("%q was written", bad)
				}
			}
		})
	}
}

// A read that fails is not an absence.
func TestTombstone_AFailedReadIsNotNone(t *testing.T) {
	cfg := newS3Double(t).config()
	cfg.S3Bucket = "nowhere"
	s, err := NewEventSourceInstanceStore(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := s.Tombstoned(context.Background(), recID); err == nil || found {
		t.Fatalf("no bucket: %v, %v", found, err)
	}
	if _, found, err := s.Confirmed(context.Background(), confirmAttempt); err == nil || found || errors.Is(err, errBlobNotFound) {
		t.Fatalf("no bucket: %v, %v", found, err)
	}
}

func FuzzConfirmRecord(f *testing.F) {
	f.Add([]byte(`{"schemaVersion":1,"orkerId":"o","confirmedAt":"2026-11-25T09:30:00Z","serviceDigest":"` + strings.Repeat("a", 64) + `"}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		r, err := decodeConfirm(raw)
		if err != nil {
			return
		}
		again, err := json.Marshal(confirmDocument{SchemaVersion: confirmSchema, ConfirmRecord: r})
		if err != nil {
			t.Fatalf("a record that reads does not write: %v", err)
		}
		if back, err := decodeConfirm(again); err != nil || back != r {
			t.Fatalf("a record does not round-trip: %+v, %v", back, err)
		}
	})
}
