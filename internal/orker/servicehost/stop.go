package servicehost

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/orkstack/orkstack/pkg/eventsources"
)

// stuckAfter is how long after a SIGKILL a container that is not confirmed
// gone makes the stop stuck (14 §6.3).
const stuckAfter = 60 * time.Second

// Container is what the stop sequence needs of one running container. The
// runtime adapter implements it; the package names no runtime.
type Container interface {
	// Signal sends sig to the container's main process.
	Signal(ctx context.Context, sig syscall.Signal) error
	// Wait returns once the container has exited, or ctx ends.
	Wait(ctx context.Context) error
	// Remove removes the container; nil only when it is gone.
	Remove(ctx context.Context) error
}

// ErrStopStuck is a container not confirmed gone a minute after its
// SIGKILL: the attempt is reported ended SERVICE_STOP_STUCK, the Orker
// keeps its reservation, stops its beat, keeps trying to remove it and
// takes no further service.
var ErrStopStuck = errors.New(eventsources.ReasonServiceStopStuck)

// StopTimes are the stop sequence's bounds: the container's own
// stopSeconds after SIGTERM, then a minute after SIGKILL.
type StopTimes struct {
	Grace time.Duration
	Stuck time.Duration
}

// DefaultStopTimes are a service spec's stopSeconds and the contract's
// minute.
func DefaultStopTimes(stopSeconds int) StopTimes {
	return StopTimes{Grace: time.Duration(stopSeconds) * time.Second, Stuck: stuckAfter}
}

// StopResult is how the sequence went: whether the container exited by
// itself within its grace, or needed a SIGKILL.
type StopResult struct {
	Killed bool
	// TermErr is a SIGTERM that failed, which skipped the grace.
	TermErr error
	// CallerGaveUp: the caller's context ended during the grace, which cut
	// it short; the container was still killed and removed.
	CallerGaveUp bool
}

// Stop runs steps 2 and 3 of 14 §6.2 once the caller has written
// stop.json and finished its side of every open exchange: SIGTERM, wait up
// to t.Grace, SIGKILL, then remove, and wait up to t.Stuck for the
// container to be gone. ErrStopStuck when it is not. A ctx that ends
// cuts the grace short but never the kill and removal: a container that
// ignores SIGTERM is never left running beside its next attempt. A SIGTERM
// that fails (the container may have exited by itself) still waits and
// removes.
func Stop(ctx context.Context, c Container, t StopTimes) (StopResult, error) {
	termErr := c.Signal(ctx, syscall.SIGTERM)
	if termErr == nil {
		graceCtx, cancel := context.WithTimeout(ctx, t.Grace)
		err := c.Wait(graceCtx)
		cancel()
		if err == nil {
			return StopResult{}, finish(ctx, c, t.Stuck, nil)
		}
	}
	// Past the grace, or the caller gave up: the kill and the removal run
	// on a context of their own, bounded by t.Stuck.
	killCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.Stuck)
	defer cancel()
	killErr := c.Signal(killCtx, syscall.SIGKILL)
	res := StopResult{Killed: true, TermErr: termErr, CallerGaveUp: ctx.Err() != nil}
	if err := finish(killCtx, c, t.Stuck, killErr); err != nil {
		return res, err
	}
	return res, nil
}

// finish waits for the container to be gone and removes it, within stuck.
func finish(ctx context.Context, c Container, stuck time.Duration, signalErr error) error {
	stuckCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stuck)
	defer cancel()
	if err := c.Wait(stuckCtx); err != nil {
		if signalErr != nil {
			return fmt.Errorf("%w: not exited %s after SIGKILL, which failed: %v", ErrStopStuck, stuck, signalErr)
		}
		return fmt.Errorf("%w: not exited %s after SIGKILL", ErrStopStuck, stuck)
	}
	if err := c.Remove(stuckCtx); err != nil {
		return fmt.Errorf("%w: not removed: %v", ErrStopStuck, err)
	}
	return nil
}

// Beat is the Orker's liveness beat (14 §6.1), rewritten every
// aliveSeconds with a higher number.
type Beat struct {
	n uint64
}

// Next is the next alive.json.
func (b *Beat) Next() map[string]any {
	b.n++
	return map[string]any{"schemaVersion": SchemaVersion, "beat": b.n}
}
