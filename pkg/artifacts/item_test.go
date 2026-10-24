package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestItemSegment(t *testing.T) {
	for _, tc := range []struct {
		index int32
		want  string
	}{{0, "00000"}, {97, "00097"}, {99999, "99999"}} {
		got, err := ItemSegment(tc.index)
		if err != nil || got != tc.want {
			t.Fatalf("ItemSegment(%d) = %q, %v; want %q", tc.index, got, err, tc.want)
		}
	}
	for _, bad := range []int32{-1, 100000} {
		if _, err := ItemSegment(bad); err == nil {
			t.Fatalf("ItemSegment(%d) accepted an index outside 0..%d", bad, MaxItemIndex)
		}
	}
}

func TestItemOfAndProto(t *testing.T) {
	if ItemOf(nil) != NoItem || NoItem.Proto() != nil {
		t.Fatal("nil must be NoItem and NoItem must render nil")
	}
	zero := int32(0)
	item := ItemOf(&zero)
	if !item.Set || item.Index != 0 {
		t.Fatalf("ItemOf(&0) = %+v; item 0 is a real item", item)
	}
	if p := item.Proto(); p == nil || *p != 0 {
		t.Fatalf("ItemAt(0).Proto() = %v, want &0", p)
	}
}

func TestItemCommitKey(t *testing.T) {
	got, err := ItemCommitKey("payments", "ork-1700000000-1", "convert", 97)
	if err != nil {
		t.Fatalf("ItemCommitKey: %v", err)
	}
	want := "metadata/commits/payments/ork-1700000000-1/convert/00097/commit.json"
	if got != want {
		t.Fatalf("ItemCommitKey = %q, want %q", got, want)
	}
	task, err := CommitKey("payments", "ork-1700000000-1", "convert")
	if err != nil {
		t.Fatalf("CommitKey: %v", err)
	}
	if !strings.HasPrefix(got, strings.TrimSuffix(task, CommitObjectName)) {
		t.Fatalf("an item key %q is not under its task's prefix %q", got, task)
	}
	// Validation parity with CommitKey: every coordinate CommitKey refuses,
	// ItemCommitKey refuses with the same sentinel, and a bad index too.
	for _, tc := range []struct {
		name, ns, run, task string
		index               int32
	}{
		{"bad namespace", "Bad_NS", "ork-1", "convert", 0},
		{"empty run", "payments", "", "convert", 0},
		{"bad task", "payments", "ork-1", "../x", 0},
		{"negative index", "payments", "ork-1", "convert", -1},
		{"index too large", "payments", "ork-1", "convert", MaxItemIndex + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, itemErr := ItemCommitKey(tc.ns, tc.run, tc.task, tc.index)
			if !errors.Is(itemErr, ErrInvalidCommitKey) {
				t.Fatalf("ItemCommitKey error = %v, want ErrInvalidCommitKey", itemErr)
			}
			if tc.index >= 0 && tc.index <= MaxItemIndex {
				if _, taskErr := CommitKey(tc.ns, tc.run, tc.task); taskErr == nil {
					t.Fatal("CommitKey accepted what ItemCommitKey refused")
				}
			}
		})
	}
}

func TestAttemptKey_ItemRoundTrip(t *testing.T) {
	ref := AttemptRef{RunID: "ork-1", TaskID: "convert", Item: ItemAt(97), Attempt: 2}
	key, err := AttemptKey(ref)
	if err != nil {
		t.Fatalf("AttemptKey: %v", err)
	}
	if want := "metadata/attempts/ork-1/convert/00097/0000000002.json"; key != want {
		t.Fatalf("AttemptKey = %q, want %q", key, want)
	}
	back, ok := ParseAttemptKey(key)
	if !ok || back != ref {
		t.Fatalf("ParseAttemptKey(%q) = %+v, %v; want %+v", key, back, ok, ref)
	}
	if _, err := AttemptKey(AttemptRef{RunID: "ork-1", TaskID: "convert", Item: ItemAt(-1), Attempt: 1}); !errors.Is(err, ErrInvalidAttemptKey) {
		t.Fatalf("a negative item = %v, want ErrInvalidAttemptKey", err)
	}
}

func TestParseAttemptKey_AcceptsBothShapes(t *testing.T) {
	task, ok := ParseAttemptKey("metadata/attempts/ork-1/convert/0000000001.json")
	if !ok || task.Item.Set || task.Attempt != 1 {
		t.Fatalf("task shape = %+v, %v", task, ok)
	}
	item, ok := ParseAttemptKey("metadata/attempts/ork-1/convert/00003/0000000001.json")
	if !ok || item.Item != ItemAt(3) || item.Attempt != 1 {
		t.Fatalf("item shape = %+v, %v", item, ok)
	}
}

func TestParseAttemptKey_RejectsAnItemSegmentOfAnotherWidth(t *testing.T) {
	for _, key := range []string{
		"metadata/attempts/ork-1/convert/0003/0000000001.json",
		"metadata/attempts/ork-1/convert/000003/0000000001.json",
		"metadata/attempts/ork-1/convert/0000000003/0000000001.json",
		"metadata/attempts/ork-1/convert/0000a/0000000001.json",
		"metadata/attempts/ork-1/convert/00003/00001/0000000001.json",
	} {
		if ref, ok := ParseAttemptKey(key); ok {
			t.Fatalf("ParseAttemptKey(%q) = %+v, want ok=false", key, ref)
		}
	}
}

func TestLogChunkKey_ItemRoundTrip(t *testing.T) {
	ref := LogRef{RunID: "ork-1", TaskID: "convert", Item: ItemAt(4), Attempt: 1}
	key, err := LogChunkKey(ref, 7, false)
	if err != nil {
		t.Fatalf("LogChunkKey: %v", err)
	}
	if want := "metadata/logs/ork-1/convert/00004/0000000001/00000000000000000007"; key != want {
		t.Fatalf("LogChunkKey = %q, want %q", key, want)
	}
	back, seq, trunc, ok := ParseLogChunkKey(key)
	if !ok || back != ref || seq != 7 || trunc {
		t.Fatalf("ParseLogChunkKey = %+v %d %v %v", back, seq, trunc, ok)
	}
}

func TestParseLogChunkKey_AcceptsBothShapes(t *testing.T) {
	task, _, _, ok := ParseLogChunkKey("metadata/logs/ork-1/convert/0000000001/00000000000000000001")
	if !ok || task.Item.Set {
		t.Fatalf("task shape = %+v, %v", task, ok)
	}
	item, _, _, ok := ParseLogChunkKey("metadata/logs/ork-1/convert/00002/0000000001/00000000000000000001.trunc")
	if !ok || item.Item != ItemAt(2) {
		t.Fatalf("item shape = %+v, %v", item, ok)
	}
	if _, _, _, ok := ParseLogChunkKey("metadata/logs/ork-1/convert/002/0000000001/00000000000000000001"); ok {
		t.Fatal("an item segment of another width parsed")
	}
}

// NoItem on a fan-out task reads nothing of its items; ItemAt(3) reads only
// item 3 — on the memory and the local backend alike.
func TestListAttempts_ItemScopesDoNotSeeEachOther(t *testing.T) {
	local, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalAttemptStore: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	for name, store := range map[string]AttemptStore{"local": local, "memory": NewMemoryAttemptStore()} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, idx := range []int32{2, 3} {
				for attempt := int32(1); attempt <= idx-1; attempt++ {
					record := sampleAttempt("ork-1", "convert", attempt)
					record.Item = ItemAt(idx).Proto()
					if err := store.PutAttempt(ctx, record); err != nil {
						t.Fatalf("PutAttempt: %v", err)
					}
				}
			}
			own, err := store.ListAttempts(ctx, "ork-1", "convert", NoItem)
			if err != nil || len(own) != 0 {
				t.Fatalf("NoItem read %d records (%v), want 0", len(own), err)
			}
			three, err := store.ListAttempts(ctx, "ork-1", "convert", ItemAt(3))
			if err != nil || len(three) != 2 {
				t.Fatalf("ItemAt(3) read %d records (%v), want 2", len(three), err)
			}
			for _, r := range three {
				if r.Item == nil || *r.Item != 3 {
					t.Fatalf("ItemAt(3) returned a record of item %v", r.Item)
				}
			}
			two, err := store.ListAttempts(ctx, "ork-1", "convert", ItemAt(2))
			if err != nil || len(two) != 1 {
				t.Fatalf("ItemAt(2) read %d records (%v), want 1", len(two), err)
			}
		})
	}
}

// namespace-budgets' run-wide reader lists item records too, with Item set.
func TestRunAttemptReader_ListsItemRecords(t *testing.T) {
	local, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalAttemptStore: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	for name, store := range map[string]interface {
		AttemptStore
		RunAttemptReader
	}{"local": local, "memory": NewMemoryAttemptStore()} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if err := store.PutAttempt(ctx, sampleAttempt("ork-1", "list", 1)); err != nil {
				t.Fatal(err)
			}
			item := sampleAttempt("ork-1", "convert", 1)
			item.Item = ItemAt(1).Proto()
			if err := store.PutAttempt(ctx, item); err != nil {
				t.Fatal(err)
			}
			refs, err := store.ListRunAttemptRefs(ctx, "ork-1")
			if err != nil {
				t.Fatalf("ListRunAttemptRefs: %v", err)
			}
			if len(refs) != 2 {
				t.Fatalf("refs = %+v, want two", refs)
			}
			var sawItem bool
			for _, ref := range refs {
				if ref.TaskID == "convert" {
					sawItem = ref.Item == ItemAt(1)
					got, err := store.GetAttempt(ctx, ref)
					if err != nil || got.GetItem() != 1 {
						t.Fatalf("GetAttempt(%+v) = %v, %v", ref, got, err)
					}
				}
			}
			if !sawItem {
				t.Fatalf("the item record was not listed with its item: %+v", refs)
			}
		})
	}
}

func TestLatestLogAttempt_ItemScoped(t *testing.T) {
	store, err := NewLocalLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalLogStore: %v", err)
	}
	ctx := context.Background()
	put := func(item Item, attempt int32) {
		t.Helper()
		if err := store.PutLogChunk(ctx, LogChunk{
			LogRef: LogRef{RunID: "ork-1", TaskID: "convert", Item: item, Attempt: attempt},
			Seq:    1, Data: []byte("x\n"),
		}); err != nil {
			t.Fatalf("PutLogChunk: %v", err)
		}
	}
	put(ItemAt(3), 1)
	put(ItemAt(3), 2)
	put(ItemAt(4), 1)
	if _, err := store.LatestLogAttempt(ctx, "ork-1", "convert", NoItem); !errors.Is(err, ErrNotFound) {
		t.Fatalf("NoItem on a fan-out = %v, want ErrNotFound (it never counts items)", err)
	}
	if got, err := store.LatestLogAttempt(ctx, "ork-1", "convert", ItemAt(3)); err != nil || got != 2 {
		t.Fatalf("ItemAt(3) = %d, %v; want 2", got, err)
	}
	if got, err := store.LatestLogAttempt(ctx, "ork-1", "convert", ItemAt(4)); err != nil || got != 1 {
		t.Fatalf("ItemAt(4) = %d, %v; want 1", got, err)
	}
	chunks, err := store.ReadLogChunks(ctx, LogRef{RunID: "ork-1", TaskID: "convert", Item: ItemAt(3), Attempt: 2}, 0, 0, 0)
	if err != nil || len(chunks) != 1 || chunks[0].Item != ItemAt(3) {
		t.Fatalf("ReadLogChunks(item 3) = %+v, %v", chunks, err)
	}
}
