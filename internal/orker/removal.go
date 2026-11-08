package orker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	// removalRetryInterval is how often an Orker tries again to remove the
	// containers it owes a removal: its own attempts' and its previous life's
	// that the daemon would not let go, and those a declared capacity still
	// holds a reservation for.
	removalRetryInterval = 30 * time.Second
	// createSettleWindow is how long a container whose create went unanswered
	// stays owed after a removal finds nothing. The CLI gave up on the daemon,
	// not the other way round: the daemon may yet finish, and "no such
	// container" says only that it has not finished so far. A daemon slower than
	// this leaves a stopped container, which holds no cpu or memory, for the
	// next boot under this identity to reap.
	createSettleWindow = 10 * time.Minute
)

// unremovedContainers is what this process owes a removal it could not finish
// when it asked. It is process-local, as a cordon or a capacity's ledger is:
// never stored, gone with the process, whose next boot reaps by label instead.
//
// It holds only containers this process made or found under its own identity,
// by id or by the name it asked for, and never lists the daemon for others: a
// sweep of the identity's label would have to tell a live attempt's container
// from a dead one's, and a wrong answer kills running work.
//
// The zero value is ready to use.
type unremovedContainers struct {
	mu sync.Mutex
	// owed maps each container to the instant before which a removal that
	// succeeds proves nothing, because the daemon may still be creating it. The
	// zero instant is a container known to exist.
	owed map[ContainerID]time.Time
}

// owe records a removal to try again. settledAt is zero for a container known
// to exist; for an unanswered create, it is when a removal that finds nothing
// starts to mean there is nothing.
func (u *unremovedContainers) owe(id ContainerID, settledAt time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.owed == nil {
		u.owed = map[ContainerID]time.Time{}
	}
	u.owed[id] = settledAt
}

// pending is how many removals stand owed.
func (u *unremovedContainers) pending() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.owed)
}

// retry tries each owed removal once, each on its own budget so one a wedged
// daemon holds does not starve the rest, and returns how many are settled and
// each that failed again.
func (u *unremovedContainers) retry(
	ctx context.Context,
	now func() time.Time,
	remove func(context.Context, ContainerID) error,
) (settled int, failed []error) {
	u.mu.Lock()
	pending := make(map[ContainerID]time.Time, len(u.owed))
	for id, settledAt := range u.owed {
		pending[id] = settledAt
	}
	u.mu.Unlock()
	for id, settledAt := range pending {
		if ctx.Err() != nil {
			return settled, failed
		}
		removeCtx, cancel := context.WithTimeout(ctx, controlTimeout)
		err := remove(removeCtx, id)
		cancel()
		if err != nil {
			failed = append(failed, fmt.Errorf("container %s: %w", id, err))
			continue
		}
		if now().Before(settledAt) {
			continue
		}
		u.mu.Lock()
		delete(u.owed, id)
		u.mu.Unlock()
		settled++
	}
	return settled, failed
}
