package orker

import (
	"context"
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// deniedInARow attempts refused a permission make an Orker unfit to host.
	deniedInARow = 3
	// hostPermissionEvery is how often an Orker so unfit asks what it holds.
	hostPermissionEvery = 2 * time.Minute
)

// hostPermissions are what the engine's host service asks of an Orker at its pool.
var hostPermissions = []string{string(authz.PermEventSourceHost)}

// attemptEnded counts the attempts in a row that ended because the engine refused
// this Orker a permission.
func (s *serviceHostState) attemptEnded(reason, pool string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.denied >= deniedInARow:
		// Its own check ends it, and nothing else.
	case reason != eventsources.ReasonHostNotPermitted:
		s.denied = 0
	default:
		if s.denied++; s.denied == deniedInARow {
			s.lacks = lacksSentence(hostPermissions[0], pool)
		}
	}
}

// notPermitted is why this Orker hosts nothing new for lacking a permission.
func (s *serviceHostState) notPermitted() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lacks, s.denied >= deniedInARow
}

func lacksSentence(perm, pool string) string {
	return fmt.Sprintf("the engine refused it, three attempts in a row: its credential does not hold %s at pool %q", perm, pool)
}

// RunHostPermissionCheck asks the engine, for as long as this Orker is unfit for a
// permission, what its credential holds, and ends that when it holds everything
// hosting asks at its pool, under its own id. No answer ends nothing.
func (a *Activities) RunHostPermissionCheck(ctx context.Context, engine Whoamier) {
	tick := time.NewTicker(hostPermissionEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if _, denied := a.services.notPermitted(); !denied {
			continue
		}
		ask, cancel := context.WithTimeout(ctx, identitySupportAsk)
		who, err := engine.Whoami(ask)
		cancel()
		if err != nil {
			a.dispatcher.logger().Warn("this Orker hosts no event source for lacking a permission, and could not ask what it holds", "error", err)
			continue
		}
		lacks := a.lacksToHost(who)
		a.services.mu.Lock()
		if a.services.lacks = lacks; lacks == "" {
			a.services.denied = 0
		}
		a.services.mu.Unlock()
		if lacks == "" {
			a.dispatcher.logger().Info("this Orker holds what hosting event sources asks again")
		}
	}
}

// lacksToHost is what the engine's answer of who this Orker is lacks for hosting, ""
// where it lacks nothing.
func (a *Activities) lacksToHost(who *orkstackv1.GetSigninResponse) string {
	if id := a.dispatcher.OrkerID; who.GetOrkerId() != id {
		return fmt.Sprintf("its credential names Orker %q, not %q", who.GetOrkerId(), id)
	}
	for _, perm := range hostPermissions {
		if pool := a.hostPool(); !holdsAt(who, perm, pool) {
			return lacksSentence(perm, pool)
		}
	}
	return ""
}

// hostPool is the pool this Orker hosts for.
func (a *Activities) hostPool() string {
	if a.pool == "" {
		return task.DefaultPool
	}
	return a.pool
}
