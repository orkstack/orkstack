package artifacts

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RecordSpacings are the waits of one read of identity records. A run's
// reads take its plan's copy; a door's and a mint's take
// DefaultRecordSpacings.
type RecordSpacings struct {
	// MissConfirm is how long after a "no such key" the record is asked
	// for again before the miss is believed.
	MissConfirm time.Duration
	// ReaskInitial and ReaskMax space the asks, doubling, of a store that
	// did not answer, within one attempt.
	ReaskInitial time.Duration
	ReaskMax     time.Duration
	// AttemptMargin is how long before the attempt's deadline the read
	// stops and answers the last error's class.
	AttemptMargin time.Duration
}

// check refuses spacings that would ask again without pausing.
func (s RecordSpacings) check() error {
	if s.MissConfirm <= 0 || s.ReaskInitial <= 0 || s.ReaskMax < s.ReaskInitial || s.AttemptMargin < 0 {
		return fmt.Errorf("artifacts: record spacings %+v: each wait must be positive, and the longest re-ask at least the first", s)
	}
	return nil
}

// DefaultRecordSpacings are the reader's own spacings.
var DefaultRecordSpacings = RecordSpacings{
	MissConfirm:   2 * time.Second,
	ReaskInitial:  200 * time.Millisecond,
	ReaskMax:      2 * time.Second,
	AttemptMargin: 500 * time.Millisecond,
}

// recordReader reads identity records through the replica's slots.
type recordReader struct {
	b       blobs
	slots   *recordSlots
	spacing RecordSpacings
	many    manyLimits
}

// readObject reads key: the bytes and the store's instant when found;
// found false when "no such key" was said twice, MissConfirm apart. A
// store that does not answer is asked again, spaced, until AttemptMargin
// before ctx's deadline (once, with no deadline), and then the read is
// *RecordStoreUnanswered with the last error's class, never timeout once
// the store refused the credential. A miss followed by no answer is
// unanswered too. A cancellation is returned as it is.
func (r *recordReader) readObject(ctx context.Context, class SlotClass, key string, max int64) ([]byte, time.Time, bool, error) {
	var (
		missed     bool
		sawDenied  bool
		lastErr    error
		wait       = r.spacing.ReaskInitial
		_, bounded = ctx.Deadline()
	)
	if err := r.spacing.check(); err != nil {
		return nil, time.Time{}, false, err
	}
	for {
		body, stored, err := r.ask(ctx, class, key, max)
		if err != nil && errors.Is(ctx.Err(), context.Canceled) {
			return nil, time.Time{}, false, context.Canceled
		}
		switch {
		case err == nil:
			return body, stored, true, nil
		case errors.Is(err, errBlobNotFound) && !missed:
			missed = true
			if werr := r.pause(ctx, r.spacing.MissConfirm); werr != nil {
				return nil, time.Time{}, false, r.stop(ctx, werr, err, sawDenied)
			}
			continue
		case errors.Is(err, errBlobNotFound):
			return nil, time.Time{}, false, nil
		}
		var un *RecordStoreUnanswered
		if errors.As(err, &un) && un.Class == UnansweredBusy {
			return nil, time.Time{}, false, r.classed(err, sawDenied)
		}
		if storeErrorClass(err) == UnansweredDenied {
			sawDenied = true
		}
		lastErr = err
		if missed || !bounded {
			// A miss then no answer, or no attempt to spend: unanswered now.
			return nil, time.Time{}, false, r.classed(lastErr, sawDenied)
		}
		if werr := r.pause(ctx, wait); werr != nil {
			return nil, time.Time{}, false, r.stop(ctx, werr, lastErr, sawDenied)
		}
		wait = min(2*wait, r.spacing.ReaskMax)
	}
}

// ask makes one request in a slot.
func (r *recordReader) ask(ctx context.Context, class SlotClass, key string, max int64) ([]byte, time.Time, error) {
	release, err := r.slots.acquireWithin(ctx, class, r.spacing.AttemptMargin)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer release()
	return r.b.getStored(ctx, key, max)
}

// pause waits d, or answers why it cannot: the attempt would end (less its
// margin) first, or ctx ended.
func (r *recordReader) pause(ctx context.Context, d time.Duration) error {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline)-r.spacing.AttemptMargin < d {
		return context.DeadlineExceeded
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stop answers a read the attempt's end stopped: a cancellation as it is,
// else unanswered with the last error's class.
func (r *recordReader) stop(ctx context.Context, why, last error, sawDenied bool) error {
	if errors.Is(why, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	if errors.Is(last, errBlobNotFound) {
		// A miss the attempt left no time to confirm is not an answer.
		return r.classed(&RecordStoreUnanswered{Class: UnansweredTimeout, Err: errors.New("a miss could not be confirmed within the attempt")}, sawDenied)
	}
	return r.classed(last, sawDenied)
}

func (r *recordReader) classed(last error, sawDenied bool) error {
	err := last
	var already *RecordStoreUnanswered
	if !errors.As(last, &already) {
		err = unanswered(last)
	}
	var un *RecordStoreUnanswered
	if sawDenied && errors.As(err, &un) && un.Class != UnansweredDenied {
		un.Class = UnansweredDenied
	}
	return err
}
