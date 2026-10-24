// Command deaf-poller is a real Temporal Activity poller for one pool's
// channel that registers ZERO activities, ork included.
//
//	ORK_TEMPORAL_ADDRESS=127.0.0.1:7233 ORK_TEMPORAL_NAMESPACE=ork-admin-ui \
//		ORK_DEAF_QUEUE=ork-pool-deaf go run ./test/tools/deaf-poller
//
// WHY THIS EXISTS. internal/engine/pools.go reports a pool's live Orker
// count from Temporal's OWN poller records (TemporalPoolProbe.ObservePool —
// DescribeTaskQueueEnhanced), entirely independent of whether that poller
// can run the DescribeOrker activity OrkerOnPool then asks of it
// (internal/engine/pool_probe.go OrkerOnPool). Production can genuinely
// reach the state "the pool has a live poller, and the probe to it fails" —
// an Orker that is up and answering Temporal's own poll but cannot run
// DescribeOrker for whatever reason — and the admin Playwright suite
// (ui/e2e/admin.spec.ts) needs to produce that state for REAL, not merely
// name it, to prove how the Admin UI draws it
// (ui/src/routes/admin/pools/+page.svelte: a live `pool-orker-count-*` with
// no `pool-unavailable-*` banner). Killing a whole Orker only produces the
// OTHER partial state (unavailable_reason, no live count at all — already covered
// by the "Temporal is gone" test in ui/e2e/admin.spec.ts); it cannot produce
// this one, because a dead Orker is not a live poller.
//
// This tool is that Orker-shaped gap: a real worker.Worker on the pool's own
// queue, DisableWorkflowWorker (an Orker is activity-only by construction,
// internal/engine/pool_probe.go's own ObservePool comment), and NO activity
// registered at all. Temporal's DescribeTaskQueueEnhanced counts it as a live
// activity poller the moment it starts polling — nothing there asks what the
// poller can run — so ObservePool reports a live count exactly as it would
// for a real Orker. When the engine dispatches task.ActivityDescribeOrker to
// this channel, the SDK has no such activity registered and reports the task
// back to Temporal as a failure (activity type not found) on first attempt;
// OrkerOnPool's two-attempt retry exhausts the same way, and
// describeOrkerOn returns an error — which is exactly the `err != nil` branch
// in internal/engine/pools.go this tool exists to reach live.
//
// It runs until SIGTERM/SIGINT, so the caller controls its lifetime the same
// way it controls bin/ork and bin/orker (ui/e2e/harness.ts spawnLogged +
// killStarted SIGTERMs then SIGKILLs every child the spec started, by handle,
// generically — this binary needs no special handling).
package main

import (
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	address := os.Getenv("ORK_TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7233"
	}
	namespace := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if namespace == "" {
		log.Fatal("deaf-poller: ORK_TEMPORAL_NAMESPACE is required")
	}
	queue := os.Getenv("ORK_DEAF_QUEUE")
	if queue == "" {
		log.Fatal("deaf-poller: ORK_DEAF_QUEUE is required (the pool's own task queue, e.g. ork-pool-deaf)")
	}

	c, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		log.Fatalf("deaf-poller: dial temporal at %s (namespace %s): %v", address, namespace, err)
	}
	defer c.Close()

	// Activity-only, exactly as internal/engine/pool_probe.go's ObservePool
	// counts — a workflow poller on this channel would be the engine, not an
	// Orker. No activity is ever registered: that omission is the entire
	// point of this tool.
	w := worker.New(c, queue, worker.Options{DisableWorkflowWorker: true})

	interrupt := worker.InterruptCh()

	fmt.Printf("deaf-poller: polling %s on namespace %s as an activity-only worker with NO activities registered\n", queue, namespace)
	if err := w.Run(interrupt); err != nil {
		log.Fatalf("deaf-poller: worker run: %v", err)
	}
	fmt.Println("deaf-poller: stopped")
}
