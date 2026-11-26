package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/eventsources"
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

// TestInstanceAttempt_OutlivesACrashLoop: an attempt is numbered by the
// cluster, which numbers as long as it retries: a key holds every number it
// can give, in order, so a long crash loop's next attempt can still confirm.
func TestInstanceAttempt_OutlivesACrashLoop(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalEventSourceInstanceStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, n := range []int{1, 9999, 10000, 100000, math.MaxInt32} {
		a := InstanceAttempt{SourceID: "evs-0123456789abcdef", Index: 1, Epoch: 2, Attempt: n}
		want := ConfirmRecord{OrkerID: "o", ConfirmedAt: time.Unix(1700000000, 0).UTC(), ServiceDigest: strings.Repeat("a", 64)}
		if got, err := s.Confirm(ctx, a, want); err != nil || got != want {
			t.Fatalf("attempt %d: %+v, %v", n, got, err)
		}
		if got, found, err := s.Confirmed(ctx, a); err != nil || !found || got != want {
			t.Fatalf("attempt %d read back: %+v, %v, %v", n, got, found, err)
		}
		key, err := confirmKey(a)
		if err != nil {
			t.Fatal(err)
		}
		record, err := SourceAttemptKey(a)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key+" "+record)
	}
	if !slices.IsSorted(keys) {
		t.Fatalf("the keys are not in order: %q", keys)
	}
	for _, n := range []int{-1, math.MaxInt32 + 1} {
		if (InstanceAttempt{SourceID: "evs-0123456789abcdef", Attempt: n}).Valid() == nil {
			t.Errorf("attempt %d is one", n)
		}
	}
}

// TestPurge: what was kept under a source's id is removed, its specs last,
// a batch at a time, and nothing that is another's: not another source's, not
// a run's, not the tombstone, not a receipt.
func TestPurge(t *testing.T) {
	for name, s := range instanceStores(t) {
		t.Run(name, func(t *testing.T) { purges(t, s) })
	}
}

func purges(t *testing.T, s *EventSourceInstanceStore) {
	const id, other = "evs-0123456789abcdef", "evs-0123456789abcde0"
	ctx, digest := t.Context(), strings.Repeat("a", 64)
	var kept []string
	put := func(key string, err error) string {
		t.Helper()
		if err == nil {
			err = s.b.create(ctx, key, "x", []byte("x"))
		}
		if err != nil {
			t.Fatal(err)
		}
		kept = append(kept, key)
		return key
	}
	of := func(id string) {
		for attempt := 1; attempt <= 2; attempt++ {
			a := InstanceAttempt{SourceID: id, Index: 1, Epoch: 1_800_000_000, Attempt: attempt}
			put(SourceLogKey(a, 1))
			put(SourceAttemptKey(a))
			put(confirmKey(a))
		}
		put(eventSourceSpecKey(id, digest))
	}
	of(id)
	mine := len(kept)
	if found, err := s.KeepsSpecs(ctx, id); !found || err != nil {
		t.Fatalf("a source with a spec: %v, %v", found, err)
	}
	kept = nil
	of(other)
	put(tombstoneKey(id))
	put(LogPrefix+"/"+id+"-2/_source/00001/1", nil)
	put(LogPrefix+"/ork-0/"+id+"/1", nil)
	put(ReceiptKey(ReceiptKeyParts{Namespace: "team", SourceID: id, Taken: time.Unix(1, 0), Outcome: ReceiptAccepted, Identity: "x"}))

	for n := 0; n < mine; n += 3 {
		if found, err := s.KeepsSpecs(ctx, id); !found || err != nil {
			t.Fatalf("after %d of %d its specs are gone: %v, %v", n, mine, found, err)
		}
		left, err := s.Purge(ctx, id, 3)
		if err != nil || left != (n+3 < mine) {
			t.Fatalf("after %d of %d: left %v, %v", n, mine, left, err)
		}
	}
	if found, err := s.KeepsSpecs(ctx, id); found || err != nil {
		t.Fatalf("after the purge: %v, %v", found, err)
	}
	if left, err := s.Purge(ctx, id, 3); left || err != nil {
		t.Fatalf("a purge of nothing: %v, %v", left, err)
	}
	for _, key := range kept {
		if _, err := s.b.head(ctx, key); err != nil {
			t.Errorf("%s was removed: %v", key, err)
		}
	}
	for _, bad := range []string{"", "evs-", "../evs-0123456789abcdef", id + "/x"} {
		if _, err := s.Purge(ctx, bad, 3); err == nil {
			t.Errorf("%q was purged", bad)
		}
		if _, err := s.KeepsSpecs(ctx, bad); err == nil {
			t.Errorf("%q was asked for", bad)
		}
	}
	if _, err := s.Purge(ctx, id, 0); err == nil {
		t.Error("a purge of no object at a time never ends")
	}
}

// TestTombstoneOf: a tombstone names its source, and one that cannot be read
// is a tombstone all the same.
func TestTombstoneOf(t *testing.T) {
	const id = "evs-0123456789abcdef"
	for backend, s := range instanceStores(t) {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			if ns, name, found, err := s.TombstoneOf(ctx, id); found || err != nil || ns != "" || name != "" {
				t.Fatalf("%q %q %v %v", ns, name, found, err)
			}
			if err := s.Tombstone(ctx, id, strings.Repeat("n", 200), strings.Repeat("o", 200), claimAt); err != nil {
				t.Fatal(err)
			}
			if ns, name, found, err := s.TombstoneOf(ctx, id); !found || err != nil || ns != strings.Repeat("n", 200) || name != strings.Repeat("o", 200) {
				t.Fatalf("%q %q %v %v", ns, name, found, err)
			}
			key, _ := tombstoneKey(id)
			for _, body := range []string{"", "x", `{"schemaVersion":2}`, `{"schemaVersion":1,"id":"evs-0123456789abcde0","namespace":"a","name":"b"}`,
				`{"schemaVersion":1,"id":"` + id + `","namespace":"A B","name":"b"}`, `{"schemaVersion":1,"id":"` + id + `","namespace":"a","name":"b","later":1}`} {
				if err := s.b.put(ctx, key, "x", []byte(body)); err != nil {
					t.Fatal(err)
				}
				if ns, name, found, err := s.TombstoneOf(ctx, id); !found || err != nil || ns != "" || name != "" {
					t.Errorf("%s: %q %q %v %v", body, ns, name, found, err)
				}
			}
		})
	}
}

// FuzzDecodeTombstone: whatever is under the key, what it names is a source's
// name, or nothing.
func FuzzDecodeTombstone(f *testing.F) {
	const id = "evs-0123456789abcdef"
	for _, seed := range []string{"", "x", `{"schemaVersion":1,"id":"` + id + `","namespace":"team","name":"orders","deletedAt":"2026-11-25T00:00:00Z"}`,
		`{"schemaVersion":1,"id":"` + id + `","namespace":"Team","name":"orders"}`, `{"schemaVersion":1,"id":"evs-0"}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		ns, name := decodeTombstone(raw, id)
		if (ns == "") != (name == "") || (ns != "" && (!eventsources.ValidName(ns) || !eventsources.ValidName(name))) {
			t.Fatalf("%q names %q %q", raw, ns, name)
		}
	})
}
