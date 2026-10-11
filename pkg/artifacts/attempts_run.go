package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// RunAttemptReadConcurrency is the default number of record reads
// ReadRunAttempts keeps in flight for one run: the bound every other
// fan-out read in the tree uses.
const RunAttemptReadConcurrency = 8

// AttemptScope names one scope of attempt records within a run: a task's own
// records (Item is NoItem) or one item of a fan-out task.
type AttemptScope struct {
	TaskID string
	Item   Item
}

// ReadRunAttempts reads every attempt record of one run with ONE listing of
// the run's prefix (RunAttemptReader.ListRunAttemptRefs) and then the records
// themselves, concurrency at a time, grouped by scope and ascending by
// attempt within each scope — the same records, in the same order, that one
// ListAttempts per scope returns, at one listing instead of one per task and
// per fan-out item.
//
// A record listed and then gone before its read (reclaimed, or expired under
// a lifecycle rule) reads as missing, exactly as ListAttempts treats it. Any
// other read error fails the whole call; the first one in key order is
// reported, so the error names a record deterministically. A run with no
// records reads as an EMPTY map, never an error.
func ReadRunAttempts(
	ctx context.Context,
	reader RunAttemptReader,
	runID string,
	concurrency int,
) (map[AttemptScope][]*orkstackv1.TaskAttempt, error) {
	if reader == nil {
		return nil, errors.New("artifacts: read run attempts: no run attempt reader")
	}
	if concurrency < 1 {
		concurrency = RunAttemptReadConcurrency
	}
	refs, err := reader.ListRunAttemptRefs(ctx, runID)
	if err != nil {
		return nil, err
	}
	records := make([]*orkstackv1.TaskAttempt, len(refs))
	errs := make([]error, len(refs))

	readCtx, stop := context.WithCancel(ctx)
	defer stop()
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, concurrency)
	)
	for i, ref := range refs {
		if readCtx.Err() != nil {
			break
		}
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, ref AttemptRef) {
			defer wg.Done()
			defer func() { <-slots }()
			record, err := reader.GetAttempt(readCtx, ref)
			switch {
			case errors.Is(err, ErrNotFound):
				// Reclaimed between the listing and this read.
			case err != nil:
				errs[i] = fmt.Errorf("artifacts: read attempt %d of task %q in run %q: %w",
					ref.Attempt, ref.TaskID, runID, err)
				stop()
			default:
				records[i] = record
			}
		}(i, ref)
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A real failure in preference to the reads it cancelled.
	var casualty error
	for _, err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, context.Canceled):
			if casualty == nil {
				casualty = err
			}
		default:
			return nil, err
		}
	}
	if casualty != nil {
		return nil, casualty
	}

	grouped := make(map[AttemptScope][]*orkstackv1.TaskAttempt)
	for i, ref := range refs {
		if records[i] == nil {
			continue
		}
		scope := AttemptScope{TaskID: ref.TaskID, Item: ref.Item}
		grouped[scope] = append(grouped[scope], records[i])
	}
	for _, scoped := range grouped {
		sortAttempts(scoped)
	}
	return grouped, nil
}
