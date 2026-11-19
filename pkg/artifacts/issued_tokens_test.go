package artifacts

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

func issuedAt(task string, item Item, attempt int32, at time.Time) IssuedRecord {
	return IssuedRecord{Namespace: "ns", RunID: "ork-1", TaskID: task, Item: item, Attempt: attempt,
		Principal: "orker:o-1", OrkerID: "o-1", IssuedAt: at,
		Tokens: []IssuedEntry{{Name: "deploy", Audience: "https://vault", Subject: "spiffe://d/ns/ns/orkflow/f/task/" + task,
			JTI: "j-" + at.Format("150405.000"), KID: "k1", IssuedAtUnix: at.Unix(), ExpiresAtUnix: at.Unix() + 3600}}}
}

// TestIssuedRecordKey_Shape: a record of what was issued lies under the
// run's identity prefix by task, item ("-" for none) and attempt in five
// digits, named by its time and an id, so key order is time order.
func TestIssuedRecordKey_Shape(t *testing.T) {
	at := time.Date(2026, 11, 16, 12, 0, 1, 2_000_000, time.UTC)
	got, err := IssuedRecordKey("ns", "ork-1", "build", NoItem, 3, at, "0123456789abcdef")
	if err != nil || !strings.HasSuffix(got, "/ns/ork-1/identity/issued/build/-/00003/20261116T120001.002000000Z-0123456789abcdef.json") {
		t.Fatalf("key = %q, %v", got, err)
	}
	later, _ := IssuedRecordKey("ns", "ork-1", "build", NoItem, 3, at.Add(time.Nanosecond), "0000000000000000")
	if later <= got {
		t.Fatalf("a later record sorts first: %q <= %q", later, got)
	}
	for name, k := range map[string]func() (string, error){
		"attempt 0": func() (string, error) {
			return IssuedRecordKey("ns", "ork-1", "build", NoItem, 0, at, "0123456789abcdef")
		},
		"attempt 100000": func() (string, error) {
			return IssuedRecordKey("ns", "ork-1", "build", NoItem, 100000, at, "0123456789abcdef")
		},
		"an id": func() (string, error) { return IssuedRecordKey("ns", "ork-1", "build", NoItem, 1, at, "../x") },
		"no time": func() (string, error) {
			return IssuedRecordKey("ns", "ork-1", "build", NoItem, 1, time.Time{}, "0123456789abcdef")
		},
		"a task": func() (string, error) { return IssuedRecordKey("ns", "ork-1", "B", NoItem, 1, at, "0123456789abcdef") },
		"an item": func() (string, error) {
			return IssuedRecordKey("ns", "ork-1", "b", ItemAt(-1), 1, at, "0123456789abcdef")
		},
	} {
		if key, err := k(); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s = %q, %v; want ErrInvalidRecordKey", name, key, err)
		}
	}
}

// TestIssuedTokens_WriteThenListInKeyOrder: records are listed in key
// order, a page at a time, narrowed by task and by item, and each decodes
// to what was written.
func TestIssuedTokens_WriteThenListInKeyOrder(t *testing.T) {
	s, _ := newTestRecordStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	byKey := map[string]IssuedRecord{}
	for i, r := range []IssuedRecord{
		issuedAt("build", NoItem, 1, base),
		issuedAt("build", NoItem, 1, base.Add(time.Minute)),
		issuedAt("build", NoItem, 2, base.Add(2*time.Minute)),
		issuedAt("build-2", NoItem, 1, base.Add(3*time.Minute)),
		issuedAt("convert", ItemAt(0), 1, base.Add(4*time.Minute)),
		issuedAt("convert", ItemAt(1), 1, base.Add(5*time.Minute)),
	} {
		key, err := s.WriteIssued(ctx, r)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		byKey[key] = r
	}
	// Key order, which is not the order written: "build-2/" sorts before
	// "build/".
	keys := slices.Sorted(maps.Keys(byKey))
	want := make([]IssuedRecord, len(keys))
	for i, k := range keys {
		want[i] = byKey[k]
	}
	if want[0].TaskID != "build-2" {
		t.Fatalf("key order starts at %q", want[0].TaskID)
	}
	var got []IssuedRecord
	after := ""
	for pages := 0; ; pages++ {
		page, next, err := s.ListIssued(ctx, IssuedQuery{Namespace: "ns", RunID: "ork-1", After: after, Limit: 4})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page {
			if r.Answer != RecordFound {
				t.Fatalf("listed %+v", r)
			}
			got = append(got, r.Value)
		}
		if next == "" {
			break
		}
		if pages > 3 {
			t.Fatal("the listing does not end")
		}
		after = next
	}
	if len(got) != len(want) {
		t.Fatalf("listed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !sameIssued(got[i], want[i]) {
			t.Fatalf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	only := func(q IssuedQuery) int {
		t.Helper()
		q.Namespace, q.RunID = "ns", "ork-1"
		page, _, err := s.ListIssued(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		return len(page)
	}
	if n := only(IssuedQuery{TaskID: "build"}); n != 3 {
		t.Fatalf("task build lists %d, want 3 (not build-2's)", n)
	}
	if n := only(IssuedQuery{TaskID: "convert", Item: ItemAt(1)}); n != 1 {
		t.Fatalf("convert[1] lists %d", n)
	}
	if n := only(IssuedQuery{TaskID: "convert"}); n != 2 {
		t.Fatalf("convert lists %d", n)
	}
}

// TestIssuedTokens_RefusedQueriesAndRecords: a record that cannot be
// stored is refused before the store is asked; a query's cursor must lie
// under what it lists; an item needs its task.
func TestIssuedTokens_RefusedQueriesAndRecords(t *testing.T) {
	s, b := newTestRecordStore(t)
	b.down = true
	ctx := context.Background()
	at := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	for name, mutate := range map[string]func(*IssuedRecord){
		"no principal":  func(r *IssuedRecord) { r.Principal = "" },
		"no orker":      func(r *IssuedRecord) { r.OrkerID = "" },
		"no token":      func(r *IssuedRecord) { r.Tokens = nil },
		"17 tokens":     func(r *IssuedRecord) { r.Tokens = make([]IssuedEntry, 17) },
		"a token blank": func(r *IssuedRecord) { r.Tokens[0].JTI = "" },
		"exp not after": func(r *IssuedRecord) { r.Tokens[0].ExpiresAtUnix = r.Tokens[0].IssuedAtUnix },
		"attempt 0":     func(r *IssuedRecord) { r.Attempt = 0 },
	} {
		r := issuedAt("build", NoItem, 1, at)
		mutate(&r)
		if _, err := s.WriteIssued(ctx, r); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s = %v; want ErrInvalidRecordKey", name, err)
		}
	}
	for name, q := range map[string]IssuedQuery{
		"an item without its task": {Item: ItemAt(0)},
		"a cursor of another run":  {After: "metadata/snapshots/ns/ork-2/identity/issued/build/-/00001/x.json"},
		"a cursor of another task": {TaskID: "build", After: "metadata/snapshots/ns/ork-1/identity/issued/test/-/00001/x.json"},
		"a limit too high":         {Limit: MaxIssuedPage + 1},
	} {
		q.Namespace, q.RunID = "ns", "ork-1"
		if _, _, err := s.ListIssued(ctx, q); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s = %v; want ErrInvalidRecordKey", name, err)
		}
	}
}

// TestIssuedTokens_UntrustedAndUnanswered: an object that is not a record
// of its key is listed invalid, naming it, never dropped; a store that does
// not answer a write or a listing is RECORD_STORE_UNANSWERED.
func TestIssuedTokens_UntrustedAndUnanswered(t *testing.T) {
	s, b := newTestRecordStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	key, err := s.WriteIssued(ctx, issuedAt("build", NoItem, 1, at))
	if err != nil {
		t.Fatal(err)
	}
	// Another attempt's record, placed under attempt 2's key.
	other := strings.Replace(key, "/00001/", "/00002/", 1)
	raw, _ := encodeIssued(issuedAt("build", NoItem, 1, at))
	if err := b.create(ctx, other, "application/json", raw); err != nil {
		t.Fatal(err)
	}
	page, _, err := s.ListIssued(ctx, IssuedQuery{Namespace: "ns", RunID: "ork-1"})
	if err != nil || len(page) != 2 || page[0].Answer != RecordFound || page[1].Answer != RecordInvalid || page[1].Object != other {
		t.Fatalf("listed %+v, %v", page, err)
	}
	b.down = true
	var un *RecordStoreUnanswered
	if _, err := s.WriteIssued(ctx, issuedAt("build", NoItem, 1, at)); !errors.As(err, &un) {
		t.Fatalf("a write the store does not answer = %v", err)
	}
	if _, _, err := s.ListIssued(ctx, IssuedQuery{Namespace: "ns", RunID: "ork-1"}); !errors.As(err, &un) {
		t.Fatalf("a listing the store does not answer = %v", err)
	}
}

func sameIssued(a, b IssuedRecord) bool {
	ra, _ := encodeIssued(a)
	rb, _ := encodeIssued(b)
	return bytes.Equal(ra, rb)
}

// FuzzIssuedTokenRecord: a record read back either decodes, and then
// re-encodes to exactly its bytes, or is refused; it never panics.
func FuzzIssuedTokenRecord(f *testing.F) {
	seed, _ := encodeIssued(issuedAt("build", NoItem, 1, time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)))
	f.Add(seed)
	item, _ := encodeIssued(issuedAt("convert", ItemAt(7), 4, time.Date(2026, 11, 16, 12, 0, 0, 5, time.UTC)))
	f.Add(item)
	f.Add([]byte(`{"v":1}`))
	f.Add([]byte(`{"v":2}`))
	f.Add(bytes.Replace(seed, []byte(`"k1"`), []byte(`null`), 1))
	f.Fuzz(func(t *testing.T, raw []byte) {
		r, answer, _ := decodeIssued(raw)
		if answer != RecordFound {
			return
		}
		again, err := encodeIssued(r)
		if err != nil {
			t.Fatalf("a decoded record does not encode: %v", err)
		}
		if !bytes.Equal(again, raw) {
			t.Fatalf("%q re-encodes as %q", raw, again)
		}
	})
}
