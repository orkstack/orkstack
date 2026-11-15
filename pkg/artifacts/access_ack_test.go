package artifacts_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

func sha(c byte) string { return strings.Repeat(string(c), 64) }

// TestAccessStore_AcknowledgeIsWriteOnce: an acknowledgement is one
// create-only record naming who acknowledged which revision and when; a
// second one of the same revision leaves the first standing.
func TestAccessStore_AcknowledgeIsWriteOnce(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	at := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	created, err := store.Acknowledge(ctx, artifacts.AccessAck{Revision: 7, SHA256: sha('a'), At: at, By: "user:corp:a", ByVerified: true})
	if err != nil || !created {
		t.Fatalf("first acknowledgement = %t, %v; want created", created, err)
	}
	key := artifacts.AccessAckKey(7, sha('a'))
	if key != "metadata/identity/access-acks/000000000007-"+sha('a')+".json" {
		t.Fatalf("key = %q", key)
	}
	first, _ := cas.Raw(key)
	var rec map[string]any
	if err := json.Unmarshal(first, &rec); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]any{"format_version": float64(1), "revision": float64(7), "sha256": sha('a'),
		"acknowledged_by": "user:corp:a", "acknowledged_by_verified": true, "acknowledged_at": "2026-11-14T09:00:00Z"} {
		if rec[field] != want {
			t.Errorf("record %s = %v; want %v", field, rec[field], want)
		}
	}
	created, err = store.Acknowledge(ctx, artifacts.AccessAck{Revision: 7, SHA256: sha('a'), At: at.Add(time.Hour), By: "user:corp:b"})
	if err != nil || created {
		t.Fatalf("second acknowledgement = %t, %v; want not created, no error", created, err)
	}
	if again, _ := cas.Raw(key); string(again) != string(first) {
		t.Fatal("a second acknowledgement replaced the first record")
	}
}

func TestAccessStore_AcknowledgeRefusesAMalformedRevision(t *testing.T) {
	store := artifacts.NewMemoryAccessStore()
	for _, ack := range []artifacts.AccessAck{
		{Revision: 0, SHA256: sha('a')},
		{Revision: -1, SHA256: sha('a')},
		{Revision: 1, SHA256: "ABC"},
		{Revision: 1, SHA256: sha('A')},
		{Revision: 1, SHA256: ""},
	} {
		if _, err := store.Acknowledge(context.Background(), ack); !errors.Is(err, artifacts.ErrAccessInvalid) {
			t.Errorf("Acknowledge(%d, %q) = %v; want ErrAccessInvalid", ack.Revision, ack.SHA256, err)
		}
	}
}

// TestAccessStore_ListAcknowledged: the acknowledgements come back in
// revision order, and a key that is not one is
// skipped, never read as one.
func TestAccessStore_ListAcknowledged(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	for _, r := range []int64{12, 3, 100} {
		if _, err := store.Acknowledge(ctx, artifacts.AccessAck{Revision: r, SHA256: sha('b'), At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	cas.PutRaw(artifacts.AccessAcksPrefix+"junk.json", []byte("{}"))
	cas.PutRaw(artifacts.AccessAcksPrefix+"000000000999-"+sha('B')+".json", []byte("{}"))
	cas.PutRaw(artifacts.AccessRevisionsPrefix+"000000000500-"+sha('c')+".json", []byte("{}"))
	acks, truncated, err := store.ListAcknowledged(ctx)
	if err != nil || truncated {
		t.Fatal(truncated, err)
	}
	if got := fmt.Sprint(revisionsOf(acks)); got != "[3 12 100]" {
		t.Fatalf("acknowledged = %s; want [3 12 100]", got)
	}
	for _, c := range []struct {
		rev  int64
		sha  string
		want bool
	}{{12, sha('b'), true}, {12, sha('c'), false}, {13, sha('b'), false}, {0, sha('b'), false}, {12, "x", false}} {
		if got, err := store.IsAcknowledged(ctx, c.rev, c.sha); err != nil || got != c.want {
			t.Errorf("IsAcknowledged(%d, %.4s) = %t, %v; want %t", c.rev, c.sha, got, err, c.want)
		}
	}
	cas.SetFailure(errors.New("store down"))
	if _, _, err := store.ListAcknowledged(ctx); err == nil {
		t.Fatal("a store that does not answer listed no acknowledgement and no error")
	}
	if _, err := store.IsAcknowledged(ctx, 12, sha('b')); err == nil {
		t.Fatal("a store that does not answer was read as no acknowledgement")
	}
}

func revisionsOf(acks []artifacts.AccessAckRef) []int64 {
	out := make([]int64, 0, len(acks))
	for _, a := range acks {
		out = append(out, a.Revision)
	}
	return out
}

// FuzzParseAccessAckKey: a listed key is an acknowledgement only when it
// is exactly one AccessAckKey would write, and then it names that revision
// and digest.
func FuzzParseAccessAckKey(f *testing.F) {
	for _, seed := range []string{
		artifacts.AccessAckKey(1, sha('a')),
		artifacts.AccessAckKey(999999999999, sha('f')),
		artifacts.AccessAcksPrefix + "junk.json",
		artifacts.AccessAcksPrefix + "000000000000-" + sha('a') + ".json",
		artifacts.AccessAcksPrefix + "-00000000001-" + sha('a') + ".json",
		artifacts.AccessAcksPrefix + "000000000001-" + sha('a') + ".json.bak",
		artifacts.AccessRevisionsPrefix + "000000000001-" + sha('a') + ".json",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		ref, ok := artifacts.ParseAccessAckKey(key)
		if !ok {
			return
		}
		if ref.Revision < 1 || ref.Key != key || artifacts.AccessAckKey(ref.Revision, ref.SHA256) != key {
			t.Fatalf("ParseAccessAckKey(%q) = %+v; it does not round-trip", key, ref)
		}
	})
}
