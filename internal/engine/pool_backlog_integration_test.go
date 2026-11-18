//go:build integration

package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The pool gauges' read (docs/DEPLOY.md, "A pool's backlog, as metrics")
// against a real cluster, on a versioned pool: while the pool's Orker polls
// it reads that poller and no backlog; once the Orker is gone, a task of a
// run pinned to its release is counted waiting, with an age, as the
// default-mode read with statistics promises.
func TestIntegration_PoolBacklogCountsAVersionedPoolsWaitingTask(t *testing.T) {
	w := newReleaseWorld(t, 5*time.Minute)
	w.startProjection()
	w.startEngine("v1")
	w.startPoolOrker("v1", "main", "")
	build := w.startPoolOrker("v1", "build", "")
	w.awaitRegistered("v1", w.runQueue, w.orkerQueue, "ork-pool-build")
	w.setCurrent("v1")

	backlog, err := engine.NewTemporalPoolBacklog(w.c, w.namespace, w.orkerQueue, []string{"build"})
	must.OKf(t, err, "NewTemporalPoolBacklog")
	if got := backlog.Pools(); len(got) != 2 || got[1] != "build" {
		t.Fatalf("Pools() = %v; want the default pool and build", got)
	}

	// Positive control: the read sees the pool's polling Orker.
	before := awaitPoolReading(t, w, backlog, "build", "build's Orker polling", func(r engine.PoolBacklogReading) bool {
		return r.Pollers >= 1
	})
	w.step(fmt.Sprintf("with its Orker polling: %+v", before))

	build.worker.Stop()
	w.step("stopped v1's build Orker")
	runID := "ork-pool-backlog-" + fmt.Sprint(time.Now().UnixNano())
	must.OKf(t, w.starter.StartRun(w.ctx, runID, buildTaskOrkflow("pool-backlog"), nil,
		rawPlan(w.orkerQueue, "build")), "start run")
	w.step("submitted " + runID + ": its second task names pool build")

	after := awaitPoolReading(t, w, backlog, "build", "a waiting task with an age", func(r engine.PoolBacklogReading) bool {
		return r.Backlog >= 1 && r.BacklogAge > 0 && r.AddRate > 0
	})
	w.step(fmt.Sprintf("with no Orker: %+v", after))
	w.write("pool-backlog.txt")
}

// awaitPoolReading reads pool once a second until want holds, failing on a
// read error and at a 90 s deadline.
func awaitPoolReading(t *testing.T, w *releaseWorld, b *engine.TemporalPoolBacklog, pool, what string,
	want func(engine.PoolBacklogReading) bool) engine.PoolBacklogReading {
	t.Helper()
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		r, err := b.Read(w.ctx, pool)
		must.OKf(t, err, "read pool %s", pool)
		if want(r) {
			return r
		}
		select {
		case <-deadline.C:
			t.Fatalf("pool %s never showed %s: last %+v", pool, what, r)
		case <-w.ctx.Done():
			t.Fatalf("pool %s: %v before it showed %s: last %+v", pool, w.ctx.Err(), what, r)
		case <-tick.C:
		}
	}
}
