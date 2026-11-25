package artifacts

import (
	"context"
	"fmt"
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
