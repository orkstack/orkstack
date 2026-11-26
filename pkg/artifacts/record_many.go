package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// manyLimits bound a read of many tasks' records.
type manyLimits struct {
	// RangeTasks is how many tasks one listing covers, RangePages how many
	// pages it may take, and PageKeys the entries a page holds (0: the
	// store's own, 1000).
	RangeTasks int
	RangePages int
	PageKeys   int32
	// Parallel is how many requests the read has in flight.
	Parallel int
}

var defaultManyLimits = manyLimits{RangeTasks: 500, RangePages: 2, Parallel: 16}

// TasksOnRecord says which of a run's tasks have anything on record
// (tasksOnRecord).
func (s *RecordStore) TasksOnRecord(ctx context.Context, class SlotClass, namespace, runID string, tasks []string) (map[string]bool, error) {
	return s.reader.tasksOnRecord(ctx, class, namespace, runID, tasks)
}

// tasksOnRecord says which of a run's tasks have anything on record. The
// tasks are sorted and cut into ranges; each range's objects are listed
// under the run's attested prefix, from its first task, at most RangePages
// pages, Parallel requests in flight, and a task is on record when the
// listing shows an object of it. A task of a range its pages did not reach
// is asked the same question by a listing of its own entry: is any object
// under it. What is on record is an object, never a directory: a store that
// keeps a directory once its objects are gone (SeaweedFS lists it as a
// common prefix, and may list it as a key ending in "/") never makes a task
// on record. A store that does not answer makes the whole read unanswered.
func (r *recordReader) tasksOnRecord(ctx context.Context, class SlotClass, namespace, runID string, tasks []string) (map[string]bool, error) {
	prefix, err := AttestedRunPrefix(namespace, runID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(tasks))
	for _, id := range tasks {
		if err := validCommitTaskID(id); err != nil {
			return nil, fmt.Errorf("artifacts: tasks on record: %w", errors.Join(ErrInvalidRecordKey, err))
		}
		ids = append(ids, id)
	}
	// A listing orders a task's entry as its name and "/", and "-" sorts
	// below "/": sort, and compare, in that order.
	sort.Slice(ids, func(i, j int) bool { return ids[i]+"/" < ids[j]+"/" })
	limits := r.many
	if limits.RangeTasks <= 0 {
		limits = defaultManyLimits
	}
	var (
		mu       sync.Mutex
		onRecord = make(map[string]bool, len(ids))
		unread   []string
	)
	var ranges [][]string
	for i := 0; i < len(ids); i += limits.RangeTasks {
		ranges = append(ranges, ids[i:min(i+limits.RangeTasks, len(ids))])
	}
	err = r.parallel(ctx, len(ranges), limits.Parallel, func(ctx context.Context, i int) error {
		listed, reachedAll, last, err := r.listRange(ctx, class, prefix, ranges[i], limits)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, id := range ranges[i] {
			switch {
			case listed[id]:
				onRecord[id] = true
			case reachedAll || id+"/" < last:
				// The listing passed every key of id. The last task it
				// reached may have keys it did not reach: one with no
				// object listed yet is asked alone.
				onRecord[id] = false
			default:
				unread = append(unread, id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = r.parallel(ctx, len(unread), limits.Parallel, func(ctx context.Context, i int) error {
		found, err := r.anyUnder(ctx, class, prefix+unread[i]+"/")
		if err != nil {
			return err
		}
		mu.Lock()
		onRecord[unread[i]] = found
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return onRecord, nil
}

// anyUnder says whether any object is under prefix: a listing of its keys,
// in pages of anyUnderPageKeys, until one is an object rather than a
// directory's key or the listing ends. A listing that holds only
// directories' keys for anyUnderPages pages, or that says it continues and
// names no way to, is unanswered.
func (r *recordReader) anyUnder(ctx context.Context, class SlotClass, prefix string) (bool, error) {
	in := blobListInput{Prefix: prefix, MaxKeys: anyUnderPageKeys}
	for page := 0; ; page++ {
		if page == anyUnderPages {
			return false, unanswered(fmt.Errorf("artifacts: tasks on record: %s: %d pages held only directories' keys", prefix, page))
		}
		release, err := r.slots.acquireWithin(ctx, class, r.spacing.AttemptMargin)
		if err != nil {
			return false, err
		}
		got, err := r.b.list(ctx, in)
		release()
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return false, context.Canceled
			}
			return false, unanswered(err)
		}
		for _, o := range got.Objects {
			if isObjectKey(o.Key) {
				return true, nil
			}
		}
		if !got.Truncated {
			return false, nil
		}
		if got.Next == "" {
			return false, unanswered(fmt.Errorf("artifacts: tasks on record: %s: a truncated listing named no continuation", prefix))
		}
		in.Token = got.Next
	}
}

// anyUnderPageKeys is the keys one page of anyUnder asks for, and
// anyUnderPages the most pages it lists: a task's keys under its entry are
// its records and its fan-out's item commits, which are removed only with
// the run's own objects, so a directory a store kept costs a key, and only
// a run's cleanup leaves many. A page is the most one listing returns.
const (
	anyUnderPageKeys = 1000
	anyUnderPages    = 4
)

// isObjectKey says whether a listed key is an object's: a key ending in "/"
// is a directory's, which a store that keeps directories may list.
func isObjectKey(key string) bool { return key != "" && !strings.HasSuffix(key, "/") }

// listRange lists one range's tasks: those with an object listed, whether
// the listing reached past the range's last task, and the last entry (a
// task's name and "/") it reached. The listing is of objects, with no
// delimiter, and each object counts for the task its key lies under. Every
// comparison is of entries, the listing's order: every key of a task starts
// with its entry, so the keys of one task are listed together, in the
// entries' order.
func (r *recordReader) listRange(ctx context.Context, class SlotClass, prefix string, tasks []string, limits manyLimits) (map[string]bool, bool, string, error) {
	listed := make(map[string]bool)
	first, final := tasks[0]+"/", tasks[len(tasks)-1]+"/"
	// The first task's keys, prefix+first+"/"..., sort just after this.
	in := blobListInput{Prefix: prefix, StartAfter: prefix + tasks[0], MaxKeys: limits.PageKeys}
	last := ""
	for page := 0; page < limits.RangePages; page++ {
		release, err := r.slots.acquireWithin(ctx, class, r.spacing.AttemptMargin)
		if err != nil {
			return nil, false, "", err
		}
		got, err := r.b.list(ctx, in)
		release()
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil, false, "", context.Canceled
			}
			return nil, false, "", unanswered(err)
		}
		for _, o := range got.Objects {
			task, _, under := strings.Cut(strings.TrimPrefix(o.Key, prefix), "/")
			if !under {
				continue // an object of the run itself, of no task
			}
			entry := task + "/"
			if entry < first {
				continue
			}
			if entry > final {
				return listed, true, final, nil
			}
			last = entry
			if isObjectKey(o.Key) {
				listed[task] = true
			}
		}
		if !got.Truncated {
			return listed, true, final, nil
		}
		if got.Next == "" {
			return nil, false, "", unanswered(fmt.Errorf("artifacts: tasks on record: %s: a truncated listing named no continuation", prefix))
		}
		in.Token = got.Next
		if task := strings.TrimSuffix(last, "/"); last != "" && listed[task] {
			// The last task reached is on record: resume past every key of
			// it. Its keys all start with its entry, name and "/", and sort
			// below its name and "0", the byte after "/", so a task with
			// many objects (a fan-out's items) costs at most the pages it
			// fills, never the range's. A last task with no object listed
			// yet (only a directory's key) is listed on, never skipped.
			in.StartAfter, in.Token = prefix+task+"0", ""
		}
	}
	return listed, false, last, nil
}

// parallel runs fn for 0..n-1, at most width at a time, and answers the
// first error, which stops what has not started.
func (r *recordReader) parallel(ctx context.Context, n, width int, fn func(context.Context, int) error) error {
	if n == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
		gate  = make(chan struct{}, max(width, 1))
	)
	for i := 0; i < n; i++ {
		select {
		case gate <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-gate }()
			if err := fn(ctx, i); err != nil {
				once.Do(func() { first = err; cancel() })
			}
		}(i)
	}
	wg.Wait()
	if first != nil {
		return first
	}
	if err := ctx.Err(); err != nil {
		return unansweredIn(ctx, err)
	}
	return nil
}
