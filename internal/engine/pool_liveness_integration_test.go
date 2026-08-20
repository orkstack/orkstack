//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestIntegration_APoolCountsTheOrkersPollingItAndNotTheOnesThatLeft is the
// live half of "live Orker count".
//
// Temporal does not forget a poller when its worker stops: the record stays in
// the task queue description for minutes, with its last-access time frozen at
// the moment the worker last polled. This test measures that directly — it
// stops an Orker, then asks the cluster and shows the departed worker STILL
// LISTED beside the live one — and pins what the engine does with the two
// records: it reports one Orker, because one Orker is what is there.
//
// Counting every record instead reports the pool as twice its size, and does
// so for as long as the survivor keeps the newest record fresh. That is
// precisely the operator who scaled a pool down and is reading the number to
// check it worked.
//
// It costs a minute of wall clock, and it has to: the property IS the window
// expiring, and a shorter one cannot be substituted — a live worker's record
// refreshes on its own polling cycle, so a window tighter than that cycle
// would report a healthy Orker as gone.
func TestIntegration_APoolCountsTheOrkersPollingItAndNotTheOnesThatLeft(t *testing.T) {
	address := temporalAddress()
	if !dialable(address) {
		infraAbsent(t, "temporal is not reachable at %s", address)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	temporalClient := dialVersioningCluster(t, ctx, poolRoutingNamespace())
	defer temporalClient.Close()

	// A pool of this run's own, so the only records on the channel are the two
	// this test creates. Everything else here is production code.
	pool := fmt.Sprintf("scale-down-%d-%d", os.Getpid(), time.Now().UnixNano())
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("QueueForPool(%q): %v", pool, err)
	}
	probe := engine.NewTemporalPoolProbe(temporalClient)

	// Distinct identities, because two Orkers ARE distinct processes: Temporal
	// identifies a worker as pid@host@channel, and two workers sharing one test
	// process would otherwise share an identity and collapse into one for the
	// wrong reason.
	departing := startPollingOrker(t, temporalClient, queue, "orker-that-leaves")
	if elapsed := awaitObservedOrkers(t, ctx, probe, queue, 1); elapsed < 0 {
		t.Fatal("unreachable")
	}
	departing.Stop()
	stoppedAt := time.Now()

	// The window expiring is the property: nothing else changes, and the
	// count has to fall to zero on its own.
	fell := awaitObservedOrkers(t, ctx, probe, queue, 0)
	if fell < engine.DefaultPollerFreshness/2 {
		t.Errorf("the pool emptied %v after its only Orker stopped, which is faster than "+
			"the %v window — the count is not being measured against it",
			fell, engine.DefaultPollerFreshness)
	}

	survivor := startPollingOrker(t, temporalClient, queue, "orker-that-stays")
	defer survivor.Stop()
	awaitObservedOrkers(t, ctx, probe, queue, 1)

	// The evidence: what the cluster still says, beside what the engine counts.
	records := rawPollerRecords(t, ctx, temporalClient, queue)
	if len(records) < 2 {
		t.Fatalf("the cluster lists %d poller record(s) %v; this test needs it to still "+
			"remember the departed worker, which it did for minutes when this was "+
			"written. If Temporal now prunes a stopped poller immediately, the whole "+
			"freshness window is measuring nothing and should be revisited",
			len(records), records)
	}
	live, err := probe.ObservePool(ctx, queue)
	if err != nil {
		t.Fatalf("ObservePool: %v", err)
	}
	if live.Orkers != 1 {
		t.Errorf("the pool reports %d Orkers from %d poller records, want 1: only "+
			"%q is polling, %q stopped %v ago",
			live.Orkers, len(records), "orker-that-stays", "orker-that-leaves",
			time.Since(stoppedAt).Round(time.Second))
	}
	if age := time.Since(live.LastSeen); age > engine.DefaultPollerFreshness {
		t.Errorf("last seen %v ago, which is the departed worker's timestamp, not the "+
			"live one's", age.Round(time.Second))
	}
	if !live.Fresh(time.Now(), engine.DefaultPollerFreshness) {
		t.Error("a pool with one polling Orker does not report as live")
	}

	writeLiveCountMeasurement(t, pool, queue, stoppedAt, records, live)
}

// pollerRecord is one line of what the cluster remembers about a channel.
type pollerRecord struct {
	identity string
	age      time.Duration
}

func (p pollerRecord) String() string {
	return fmt.Sprintf("%s (last access %s ago)", p.identity, p.age.Round(time.Second))
}

// rawPollerRecords asks Temporal the same question the probe asks and returns
// the answer UNREDUCED, so the test can show what the count was made from.
func rawPollerRecords(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	queue string,
) []pollerRecord {
	t.Helper()
	description, err := c.DescribeTaskQueueEnhanced(ctx, client.DescribeTaskQueueEnhancedOptions{
		TaskQueue:      queue,
		TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeActivity},
		ReportPollers:  true,
		Versions:       &client.TaskQueueVersionSelection{AllActive: true, Unversioned: true},
	})
	if err != nil {
		t.Fatalf("describe %s: %v", queue, err)
	}
	var records []pollerRecord
	for _, version := range description.VersionsInfo {
		for _, poller := range version.TypesInfo[client.TaskQueueTypeActivity].Pollers {
			records = append(records, pollerRecord{
				identity: poller.Identity,
				age:      time.Since(poller.LastAccessTime),
			})
		}
	}
	return records
}

// startPollingOrker starts an activity-only worker on one channel, as an Orker
// is (cmd/orker sets DisableWorkflowWorker).
func startPollingOrker(t *testing.T, c client.Client, queue, identity string) worker.Worker {
	t.Helper()
	w := worker.New(c, queue, worker.Options{
		DisableWorkflowWorker:       true,
		DisableRegistrationAliasing: true,
		Identity:                    identity,
	})
	w.RegisterActivityWithOptions(
		func(context.Context, task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
			return task.ExecuteTaskResult{}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	if err := w.Start(); err != nil {
		t.Fatalf("start orker %q: %v", identity, err)
	}
	return w
}

// awaitObservedOrkers waits until the probe reports exactly want Orkers on a
// channel, and returns how long that took. The deadline outlasts the freshness
// window, because waiting for it is the point.
func awaitObservedOrkers(
	t *testing.T,
	ctx context.Context,
	probe engine.PoolProbe,
	queue string,
	want int,
) time.Duration {
	t.Helper()
	started := time.Now()
	deadline := started.Add(engine.DefaultPollerFreshness + 45*time.Second)
	var last engine.PoolLiveness
	for time.Now().Before(deadline) {
		live, err := probe.ObservePool(ctx, queue)
		if err == nil {
			last = live
			if live.Orkers == want {
				return time.Since(started)
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("channel %s reports %d Orkers, want %d, after %v",
		queue, last.Orkers, want, time.Since(started).Round(time.Second))
	return 0
}

func dialable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// writeLiveCountMeasurement records the measurement for proof/M10.2: what the
// cluster reported, and what the engine counted from it.
func writeLiveCountMeasurement(
	t *testing.T,
	pool, queue string,
	stoppedAt time.Time,
	records []pollerRecord,
	live engine.PoolLiveness,
) {
	t.Helper()
	lines := []string{
		"# M10.2 — a live Orker count counts the Orkers that are live",
		"#",
		"# Written by TestIntegration_APoolCountsTheOrkersPollingItAndNotTheOnesThatLeft",
		"# (internal/engine/pool_liveness_integration_test.go) against a real Temporal.",
		"#",
		"# recorded " + time.Now().UTC().Format(time.RFC3339),
		"",
		fmt.Sprintf("pool                       %s", pool),
		fmt.Sprintf("channel                    %s", queue),
		fmt.Sprintf("orker-that-leaves stopped  %s (%s ago)",
			stoppedAt.UTC().Format(time.RFC3339), time.Since(stoppedAt).Round(time.Second)),
		"orker-that-stays           polling",
		"",
		fmt.Sprintf("what temporal still lists  %d poller record(s)", len(records)),
	}
	for _, record := range records {
		verdict := "inside the window, counted"
		if record.age > engine.DefaultPollerFreshness {
			verdict = "outside the window, NOT counted"
		}
		lines = append(lines, fmt.Sprintf("  %-20s last access %-8s %s",
			record.identity, record.age.Round(time.Second), verdict))
	}
	lines = append(lines,
		"",
		fmt.Sprintf("window                     %s", engine.DefaultPollerFreshness),
		fmt.Sprintf("ObservePool reports        %d Orker(s), last seen %s ago",
			live.Orkers, time.Since(live.LastSeen).Round(time.Second)),
		"",
		"Counting every record — the shape this replaced — would report "+
			fmt.Sprintf("%d.", len(records)),
	)

	publishProofArtifact(t, "live-count-measurement.txt", strings.Join(lines, "\n")+"\n")
}
