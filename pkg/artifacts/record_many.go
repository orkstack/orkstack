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
// tasks are sorted and cut into ranges; each range is listed under the
// run's attested prefix with delimiter "/", from its first task, at most
// RangePages pages, Parallel requests in flight, and a task a listing
// shows is on record by its name alone. A task of a range its pages did
// not reach is asked the same question by a listing of its own entry: is
// anything under it. A store that does not answer makes the whole read
// unanswered.
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
			case reachedAll || id+"/" <= last:
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

// anyUnder says whether anything is under prefix: one listing of one key.
func (r *recordReader) anyUnder(ctx context.Context, class SlotClass, prefix string) (bool, error) {
	release, err := r.slots.acquireWithin(ctx, class, r.spacing.AttemptMargin)
	if err != nil {
		return false, err
	}
	got, err := r.b.list(ctx, blobListInput{Prefix: prefix, MaxKeys: 1})
	release()
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return false, context.Canceled
		}
		return false, unanswered(err)
	}
	return len(got.Objects) > 0, nil
}

// listRange lists one range's tasks: those listed, whether the listing
// reached past the range's last task, and the last entry (a task's name
// and "/") it reached. Every comparison is of entries, the listing's order.
func (r *recordReader) listRange(ctx context.Context, class SlotClass, prefix string, tasks []string, limits manyLimits) (map[string]bool, bool, string, error) {
	listed := make(map[string]bool)
	first, final := tasks[0]+"/", tasks[len(tasks)-1]+"/"
	// The first task's entry, prefix+first+"/", sorts just after this.
	in := blobListInput{Prefix: prefix, Delimiter: "/", StartAfter: prefix + tasks[0], MaxKeys: limits.PageKeys}
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
		for _, p := range got.Prefixes {
			entry := strings.TrimPrefix(p, prefix)
			if entry < first {
				continue
			}
			if entry > final {
				return listed, true, final, nil
			}
			listed[strings.TrimSuffix(entry, "/")] = true
			last = entry
		}
		if !got.Truncated {
			return listed, true, final, nil
		}
		in.Token = got.Next
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
