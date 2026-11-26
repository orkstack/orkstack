package artifacts

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingBlobs counts listings and reads by key.
type countingBlobs struct {
	blobs
	lists, reads atomic.Int64
}

func (c *countingBlobs) list(ctx context.Context, in blobListInput) (blobListPage, error) {
	c.lists.Add(1)
	return c.blobs.list(ctx, in)
}

func (c *countingBlobs) getStored(ctx context.Context, key string, max int64) ([]byte, time.Time, error) {
	c.reads.Add(1)
	return c.blobs.getStored(ctx, key, max)
}

// TestTasksOnRecord_ListsRangesThenEachUnreachedTask: a run's tasks are found on
// record by listing ranges of tasks under the run's attested prefix with
// delimiter "/", each range at most its pages, and each task no range
// reached is asked by a listing of its own entry, so a task on record only
// in a resume scope is found either way; a task with nothing is not.
func TestTasksOnRecord_ListsRangesThenEachUnreachedTask(t *testing.T) {
	b, err := openLocalBlobs("test", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := &countingBlobs{blobs: b}
	ctx := context.Background()
	const ns, run, tasks = "ns", "ork-1", 1200
	keyOf := map[string]string{}
	var ids []string
	for i := range tasks {
		id := fmt.Sprintf("t%04d", i)
		ids = append(ids, id)
		k, _ := RecordKey(RecordComposed, ns, run, id, 0)
		keyOf[id] = k
		if i%3 == 1 && i > 1000 {
			// beyond what the listings reach, on record only in a resume scope
			k, _ = RecordKey(RecordComposed, ns, run, id, 2)
		}
		if i%3 != 0 { // every third task has nothing on record
			// put, not create: 800 synced writes are most of the test's time.
			if err := b.put(ctx, k, "application/json", []byte("{}")); err != nil {
				t.Fatal(err)
			}
		}
	}
	r := &recordReader{b: c, slots: newRecordSlots(64, 16), spacing: fast,
		many: manyLimits{RangeTasks: 500, RangePages: 2, PageKeys: 100, Parallel: 16}}
	got, err := r.tasksOnRecord(ctx, SlotRun, ns, run, idsOf(keyOf))
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		if got[id] != (i%3 != 0) {
			t.Fatalf("task %s on record = %v", id, got[id])
		}
	}
	// 3 ranges of at most 2 pages of 100 entries: the first two reach 300
	// of their 500 tasks and leave 200 each, which are asked by a listing
	// of their own entry; the third reaches all of its 200. Nothing is
	// read by key.
	if lists := c.lists.Load(); lists != 6+400 {
		t.Fatalf("%d listings, want %d", lists, 6+400)
	}
	if reads := c.reads.Load(); reads != 0 {
		t.Fatalf("%d reads by key", reads)
	}
}

// TestTasksOnRecord_HyphenatedNamesSortAsTheListingDoes: a listing orders a
// task's entry as its name followed by "/", and "-" sorts below "/", so
// "a-b/" comes before "a/" and "a/" before "a0/"; a task is never missed for
// that, at a range's end or across a page.
func TestTasksOnRecord_HyphenatedNamesSortAsTheListingDoes(t *testing.T) {
	for _, pageKeys := range []int32{1, 2, 0} {
		b, err := openLocalBlobs("test", t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		keyOf := map[string]string{}
		for _, id := range []string{"a", "a-b", "a0"} {
			k, _ := RecordKey(RecordComposed, "ns", "ork-1", id, 0)
			keyOf[id] = k
			if err := b.create(ctx, k, "application/json", []byte("{}")); err != nil {
				t.Fatal(err)
			}
		}
		r := &recordReader{b: b, slots: newRecordSlots(8, 4), spacing: fast,
			many: manyLimits{RangeTasks: 1, RangePages: 1, PageKeys: pageKeys, Parallel: 4}}
		got, err := r.tasksOnRecord(ctx, SlotRun, "ns", "ork-1", idsOf(keyOf))
		if err != nil || !got["a"] || !got["a-b"] || !got["a0"] {
			t.Fatalf("page %d: on record = %v, %v", pageKeys, got, err)
		}
	}
}

func idsOf(keyOf map[string]string) []string {
	ids := make([]string, 0, len(keyOf))
	for id := range keyOf {
		ids = append(ids, id)
	}
	return ids
}

// TestTasksOnRecord_CountsObjectsAndSkipsPastATaskWithMany: a task is on
// record by an object of it, a page that ends inside a task's objects
// resumes past all of them, so a task with many objects (a fan-out's items)
// costs the range no more than the page it fills, and a task whose only
// record was removed is not on record.
func TestTasksOnRecord_CountsObjectsAndSkipsPastATaskWithMany(t *testing.T) {
	b, err := openLocalBlobs("test", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := &countingBlobs{blobs: b}
	ctx := context.Background()
	const ns, run = "ns", "ork-1"
	prefix, err := AttestedRunPrefix(ns, run)
	if err != nil {
		t.Fatal(err)
	}
	put := func(key string) {
		t.Helper()
		if err := b.put(ctx, key, "application/json", []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 5 {
		put(fmt.Sprintf("%sa/item-%d/commit.json", prefix, i))
	}
	for _, id := range []string{"b", "c", "d"} {
		k, _ := RecordKey(RecordComposed, ns, run, id, 0)
		put(k)
	}
	gone, _ := RecordKey(RecordComposed, ns, run, "c", 0)
	if err := b.remove(ctx, gone); err != nil {
		t.Fatal(err)
	}
	r := &recordReader{b: c, slots: newRecordSlots(64, 16), spacing: fast,
		many: manyLimits{RangeTasks: 500, RangePages: 2, PageKeys: 2, Parallel: 1}}
	got, err := r.tasksOnRecord(ctx, SlotRun, ns, run, []string{"a", "b", "c", "d", "e"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"a": true, "b": true, "c": false, "d": true, "e": false}
	for id, on := range want {
		if got[id] != on {
			t.Fatalf("task %s on record = %v, want %v (all: %v)", id, got[id], on, got)
		}
	}
	// Page 1 holds two of a's five objects; page 2 starts past a and holds
	// b's and d's records, and the listing ends there. Nothing is asked
	// alone.
	if lists := c.lists.Load(); lists != 2 {
		t.Fatalf("%d listings, want 2", lists)
	}
}

// dirKeyBlobs serves listings of a fixed set of keys, as S3 does
// (Prefix, StartAfter, Token, MaxKeys, no delimiter), among them keys ending
// in "/": a directory a store kept (SeaweedFS). It is a hand-written fake
// because the fault it serves cannot be produced on demand: no real store
// lets a test place a directory's key exactly at a page boundary, and the
// local backend never lists one.
type dirKeyBlobs struct {
	blobs
	keys  []string // sorted
	lists atomic.Int64
	// noNext answers a truncated page with no continuation, as a broken
	// store might.
	noNext bool
}

func (d *dirKeyBlobs) list(_ context.Context, in blobListInput) (blobListPage, error) {
	d.lists.Add(1)
	after := in.StartAfter
	if in.Token != "" {
		after = in.Token
	}
	limit := int(in.MaxKeys)
	if limit <= 0 {
		limit = 1000
	}
	var page blobListPage
	for _, k := range d.keys {
		if !strings.HasPrefix(k, in.Prefix) || k <= after {
			continue
		}
		if len(page.Objects) == limit {
			page.Truncated, page.Next = true, page.Objects[limit-1].Key
			if d.noNext {
				page.Next = ""
			}
			break
		}
		page.Objects = append(page.Objects, blobObject{Key: k})
	}
	return page, nil
}

// TestTasksOnRecord_ADirectoryKeyIsNoRecord: a key ending in "/" puts no
// task on record. A page that ends on a task's directory key is listed on,
// never skipped past, and a range whose pages end there asks the task
// alone, so the object after the directory's key is found either way; a
// task with only directories' keys is not on record; and a listing that
// holds only directories' keys past anyUnder's pages is unanswered, never
// "not on record".
func TestTasksOnRecord_ADirectoryKeyIsNoRecord(t *testing.T) {
	const ns, run = "ns", "ork-1"
	prefix, err := AttestedRunPrefix(ns, run)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{prefix + "a/", prefix + "a/resolution.json", prefix + "b/", prefix + "b/item-0/", prefix + "c/composed.json"}
	// One page: every task is asked alone. Two: a's directory key ends the
	// first page and a's object the second, which skips past a. Three and
	// more: the listing goes on with the store's continuation past b's
	// directories' keys and reaches c.
	for _, pages := range []int{1, 2, 3, 6} {
		d := &dirKeyBlobs{keys: keys}
		r := &recordReader{b: d, slots: newRecordSlots(64, 16), spacing: fast,
			many: manyLimits{RangeTasks: 500, RangePages: pages, PageKeys: 1, Parallel: 1}}
		got, err := r.tasksOnRecord(context.Background(), SlotRun, ns, run, []string{"a", "b", "c"})
		if err != nil {
			t.Fatalf("%d pages: %v", pages, err)
		}
		if !got["a"] || got["b"] || !got["c"] {
			t.Fatalf("%d pages: on record = %v, want a and c", pages, got)
		}
	}

	// z's record lies after its directories' keys: within anyUnder's pages
	// it is found; one key past them, the read is unanswered.
	dirs := func(n int) []string {
		keys := []string{prefix + "z/"}
		for i := range n - 1 {
			keys = append(keys, fmt.Sprintf("%sz/d%05d/", prefix, i))
		}
		return append(keys, prefix+"z/e/composed.json")
	}
	ask := func(d *dirKeyBlobs) (map[string]bool, error) {
		r := &recordReader{b: d, slots: newRecordSlots(64, 16), spacing: fast,
			many: manyLimits{RangeTasks: 500, RangePages: 1, PageKeys: 1, Parallel: 1}}
		return r.tasksOnRecord(context.Background(), SlotRun, ns, run, []string{"z"})
	}
	const most = anyUnderPageKeys * anyUnderPages
	if got, err := ask(&dirKeyBlobs{keys: dirs(most - 1)}); err != nil || !got["z"] {
		t.Fatalf("%d directories' keys, then a record: %v, %v; want on record", most-1, got, err)
	}
	var unans *RecordStoreUnanswered
	if got, err := ask(&dirKeyBlobs{keys: dirs(most)}); !errors.As(err, &unans) {
		t.Fatalf("%d directories' keys, then a record: %v, %v; want unanswered", most, got, err)
	}
	// A truncated page that names no continuation is unanswered, in a
	// range's listing and when a task is asked alone.
	for _, pages := range []int{1, 2} {
		r := &recordReader{b: &dirKeyBlobs{keys: keys, noNext: true}, slots: newRecordSlots(64, 16), spacing: fast,
			many: manyLimits{RangeTasks: 500, RangePages: pages, PageKeys: 1, Parallel: 1}}
		if got, err := r.tasksOnRecord(context.Background(), SlotRun, ns, run, []string{"a", "b", "c"}); !errors.As(err, &unans) {
			t.Fatalf("%d pages, no continuation: %v, %v; want unanswered", pages, got, err)
		}
	}
}
