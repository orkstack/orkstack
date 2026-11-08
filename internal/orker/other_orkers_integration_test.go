//go:build integration

package orker_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_BootWarning_NamesOtherIdentitiesContainersAndTouchesNone: a
// boot says what other identities hold on this daemon, and that is all it does,
// for a dead Orker's running and stopped containers and for a live Orker's alike.
// Other proofs' containers may appear too: the test asks only about its own.
// What proves nothing was touched is the workloads' own ticks past the boot.
func TestIntegration_BootWarning_NamesOtherIdentitiesContainersAndTouchesNone(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const ticking = `while :; do echo tick; sleep 1; done`

	deadID := proof.OrkerID("boot-warning-dead")
	dead := removalInput(t, "warning-dead", "/bin/sh", "-c", ticking)
	deadContainer, killDead := startHeldOrker(ctx, t, deadID, removalWorkRoot(t, "boot-warning-dead"), dead)
	killDead()

	stoppedID := proof.OrkerID("boot-warning-stopped")
	stopped := removalInput(t, "warning-stopped", "/bin/sh", "-c", `echo tick; sleep 1; exit 0`)
	stoppedContainer, killStopped := startHeldOrker(ctx, t, stoppedID, removalWorkRoot(t, "boot-warning-stopped"), stopped)
	killStopped()
	awaitState(ctx, t, stoppedContainer, "exited")

	liveID := proof.OrkerID("boot-warning-live")
	live := removalInput(t, "warning-live", "/bin/sh", "-c", ticking)
	liveContainer, killLive := startHeldOrker(ctx, t, liveID, removalWorkRoot(t, "boot-warning-live"), live)
	defer killLive()

	deadBefore, liveBefore := tickCount(ctx, t, deadContainer), tickCount(ctx, t, liveContainer)
	bootingID := proof.OrkerID("boot-warning-booting")
	_, logged := bootLogged(ctx, t, bootingID, removalWorkRoot(t, "boot-warning-booting"))

	if !strings.Contains(logged.String(), "containers of other orker identities") {
		t.Fatalf("the boot gave no warning:\n%s", logged)
	}
	// Through the real CLI, what the warning is made from. The line itself names
	// at most a bounded few, which other proofs' containers may crowd.
	listed, err := orker.NewDockerRuntime().OtherOrkersContainers(ctx, bootingID)
	if err != nil {
		t.Fatalf("OtherOrkersContainers: %v", err)
	}
	for _, want := range []orker.OtherOrkerContainer{
		{ID: deadContainer, State: "running", Orker: deadID},
		{ID: stoppedContainer, State: "exited", Orker: stoppedID},
		{ID: liveContainer, State: "running", Orker: liveID},
	} {
		if !slices.Contains(listed, want) {
			t.Errorf("the list does not hold %+v: %+v", want, listed)
		}
	}
	for _, other := range listed {
		if other.Orker == bootingID {
			t.Errorf("the list holds this identity's own: %+v", other)
		}
	}

	awaitTicks(ctx, t, deadContainer, deadBefore+3)
	awaitTicks(ctx, t, liveContainer, liveBefore+3)
	for _, c := range []struct{ id, want string }{
		{deadContainer, "running"}, {stoppedContainer, "exited"}, {liveContainer, "running"},
	} {
		if got := containerState(ctx, t, c.id); got != c.want {
			t.Errorf("container %s is %q past the boot, want %q", c.id, got, c.want)
		}
	}
}
