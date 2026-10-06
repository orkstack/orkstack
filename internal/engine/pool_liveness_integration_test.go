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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
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
		func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{}, nil
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

// writeLiveCountMeasurement records the measurement: what the cluster
// reported, and what the engine counted from it.
func writeLiveCountMeasurement(
	t *testing.T,
	pool, queue string,
	stoppedAt time.Time,
	records []pollerRecord,
	live engine.PoolLiveness,
) {
	t.Helper()
	lines := []string{
		"# a live Orker count counts the Orkers that are live",
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
	)

	proof.Write(t, "engine", "live-count-measurement.txt", []byte(strings.Join(lines, "\n")+"\n"))
}

// TestIntegration_AnIdleOrkerStaysLiveAcrossItsPollingCycle is the other edge
// of the freshness window: a healthy Orker must never fall out of it.
//
// Temporal stamps a poller when its long poll starts, and an idle worker's
// poll is held for about a minute, so its record ages to a little over a
// minute before the next poll renews it. This test keeps one idle Orker
// polling a pool channel and its own control channel for two full polling
// cycles, and reads both through the production probes four times a second.
// Every single read must count it. With a 60-second window, the reads at each
// minute boundary dropped it — the moment a release promotion was refused
// because "no Orker of this release serves" a pool its Orker was serving
// (ui/e2e/admin-releases.spec.ts, "a decision made on a stale read is
// refused").
func TestIntegration_AnIdleOrkerStaysLiveAcrossItsPollingCycle(t *testing.T) {
	address := temporalAddress()
	if !dialable(address) {
		infraAbsent(t, "temporal is not reachable at %s", address)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalClient := dialVersioningCluster(t, ctx, poolRoutingNamespace())
	defer temporalClient.Close()

	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	pool := "idle-" + token
	poolQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("QueueForPool(%q): %v", pool, err)
	}
	orkerID := "orker-idle-" + token
	controlQueue, err := task.QueueForOrker(orkerID)
	if err != nil {
		t.Fatalf("QueueForOrker(%q): %v", orkerID, err)
	}
	probe := engine.NewTemporalPoolProbe(temporalClient)
	fleet := engine.NewTemporalStarter(temporalClient, task.DefaultQueue)

	serving := startPollingOrker(t, temporalClient, poolQueue, orkerID)
	defer serving.Stop()
	control := startPollingOrker(t, temporalClient, controlQueue, orkerID)
	defer control.Stop()
	awaitObservedOrkers(t, ctx, probe, poolQueue, 1)

	// Two polling cycles of a live worker, with room for the drift past each
	// minute that the cluster shows.
	const watch = 2*time.Minute + 15*time.Second
	started := time.Now()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var reads int
	var oldest time.Duration
	for time.Since(started) < watch {
		select {
		case <-ctx.Done():
			t.Fatalf("context ended after %v of the watch: %v", time.Since(started).Round(time.Second), ctx.Err())
		case <-ticker.C:
		}
		at := time.Since(started)
		live, err := probe.ObservePool(ctx, poolQueue)
		if err != nil {
			t.Fatalf("ObservePool at +%v: %v", at.Round(time.Millisecond), err)
		}
		presence, present, err := fleet.ObserveOrker(ctx, orkerID)
		if err != nil {
			t.Fatalf("ObserveOrker at +%v: %v", at.Round(time.Millisecond), err)
		}
		reads++
		for _, record := range rawPollerRecords(t, ctx, temporalClient, poolQueue) {
			if record.identity == orkerID && record.age > oldest {
				oldest = record.age
			}
		}
		if live.Orkers != 1 || !live.Fresh(time.Now(), engine.DefaultPollerFreshness) {
			t.Fatalf("at +%v (read %d) the pool its idle Orker is polling reports %d Orkers, fresh=%v; "+
				"the cluster's records: %v. A live Orker fell out of the %v window",
				at.Round(time.Millisecond), reads, live.Orkers, live.Fresh(time.Now(), engine.DefaultPollerFreshness),
				rawPollerRecords(t, ctx, temporalClient, poolQueue), engine.DefaultPollerFreshness)
		}
		if !present {
			t.Fatalf("at +%v (read %d) the Orker list does not contain idle Orker %s; the cluster's records: %v. "+
				"A live Orker fell out of the %v window",
				at.Round(time.Millisecond), reads, orkerID, rawPollerRecords(t, ctx, temporalClient, controlQueue),
				engine.DefaultPollerFreshness)
		}
		if presence.Identity != orkerID {
			t.Fatalf("ObserveOrker answered identity %q, want %q", presence.Identity, orkerID)
		}
	}
	if oldest <= time.Minute {
		t.Logf("the oldest record of the live Orker was %v, inside one minute: this run did not see the "+
			"minute boundary the window has to outlast", oldest.Round(time.Millisecond))
	}

	proof.Write(t, "engine", "idle-orker-freshness.txt", []byte(strings.Join([]string{
		"# an idle Orker stays live across its polling cycle",
		"# recorded " + time.Now().UTC().Format(time.RFC3339),
		"",
		fmt.Sprintf("pool channel               %s", poolQueue),
		fmt.Sprintf("control channel            %s", controlQueue),
		fmt.Sprintf("watched                    %s, %d reads of each probe", watch, reads),
		fmt.Sprintf("oldest live record seen    %s", oldest.Round(time.Millisecond)),
		fmt.Sprintf("window                     %s", engine.DefaultPollerFreshness),
		"every read counted the Orker in its pool and listed it in the fleet",
	}, "\n")+"\n"))
	t.Logf("EVIDENCE idle Orker live on every one of %d reads over %s; oldest record %s, window %s",
		reads, watch, oldest.Round(time.Millisecond), engine.DefaultPollerFreshness)
}
