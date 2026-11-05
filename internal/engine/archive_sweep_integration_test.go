//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	enums "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const sweepChainHeader = "apiVersion: orkstack/v1\nkind: Orkflow\nname: archive-sweep\nversion: \"1.0.0\"\ntasks:\n"

// sweepOver is a sweep over the runs of this stack. Its clock is two hours
// ahead so a run that just closed counts as closed for an interval.
func sweepOver(
	s *chaosStack,
	reader *engine.TemporalRunReader,
	bodies archivecleanup.RunBodyChecker,
	archiver *engine.RunArchiver,
) *archivecleanup.Sweeper {
	return archivecleanup.NewSweeper(
		archivecleanup.CleanupConfig{TemporalNamespace: s.namespace, Interval: time.Hour, BatchSize: 10},
		reader, bodies, engine.NewHistoryArchiver(reader, archiver), reader, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	).WithClock(func() time.Time { return time.Now().Add(2 * time.Hour) })
}

// sweepUntil passes until the cluster's index lists a finished run and the pass
// reports what the test waits for. The index lags a close, so a pass that
// listed nothing yet is not an answer; the passes before it did nothing, which
// the caller's own assertions then hold.
func sweepUntil(
	t *testing.T,
	ctx context.Context,
	sweeper *archivecleanup.Sweeper,
	done func(archivecleanup.SweepResult) bool,
) archivecleanup.SweepResult {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		result, err := sweeper.RunOnce(ctx)
		if result.Examined > 0 && done(result) {
			return result
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sweep never reached the state the test waits for: %+v (%v)", result, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// startAndTerminate starts a run, holds its second task, and terminates it from
// outside: no workflow code runs at its end, so nothing archives it.
func (s *chaosStack) startAndTerminate(t *testing.T) string {
	t.Helper()
	g := newGates()
	t.Cleanup(func() { g.open("c01") })
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker-1", gated(g, "c01"))
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, sweepChainHeader+guardChain("c", 3, "")), nil, plan)
	g.waitEntered(t, "c01", 2*time.Minute)
	if err := s.control.TerminateWorkflow(s.ctx, runID, "", "archive sweep: terminated from outside"); err != nil {
		t.Fatal(err)
	}
	if status, _ := s.wait(runID, time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		t.Fatalf("the run closed %v, want Terminated", status)
	}
	return runID
}

// TestIntegration_ArchiveSweep_ATerminatedRunIsArchivedRetiredAndStillRead is
// the whole path against a real cluster and object store. A run terminated from
// outside has no archive. The sweep archives it from its real history; cleanup
// retires it at its retention, deleting the execution; and the API, with the
// cluster's copy gone, answers from the archive what it answered from the
// cluster.
func TestIntegration_ArchiveSweep_ATerminatedRunIsArchivedRetiredAndStillRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	cfg, _ := privateCleanupBucket(t, ctx)
	store, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	s := newChaosStackWith(t, ctx, "archive-sweep/terminated", cfg, store)
	runID := s.startAndTerminate(t)

	a := s.serveAPI(api.Config{})
	before, err := a.svc.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run := before.Msg.GetRun(); run.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		run.GetError().GetCode() != "RUN_TERMINATED" || len(run.GetTaskRuns()) == 0 {
		t.Fatalf("the live read = %v, want CANCELED RUN_TERMINATED with its tasks", run)
	}
	streamed := func() []*orkstackv1.Event {
		var events []*orkstackv1.Event
		if err := a.svc.StreamRunEvents(ctx, runID, "", 0, func(e *orkstackv1.Event) error {
			events = append(events, e)
			return nil
		}); err != nil {
			t.Fatalf("StreamRunEvents: %v", err)
		}
		return events
	}
	eventsBefore := streamed()
	if has, err := store.HasRun(ctx, runID); err != nil || has {
		t.Fatalf("HasRun before the sweep = %v (%v), want no archive", has, err)
	}

	policies, err := artifacts.NewS3RetentionPolicyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RetentionPolicyStore: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	controller := archivecleanup.NewRetentionController(policies, s.reader, s.namespace, archivecleanup.DefaultRetentionBuffer, logger)
	if _, err := controller.SetMax(ctx, 999*time.Hour); err != nil {
		t.Fatalf("SetMax: %v", err)
	}
	cleaner := archivecleanup.NewCleaner(
		archivecleanup.CleanupConfig{TemporalNamespace: s.namespace, Interval: time.Hour, BatchSize: 10},
		store, s.reader, staticNamespaceLister{docs: []*orkstackv1.Namespace{{Name: "default"}}}, controller, logger).
		WithClock(func() time.Time { return time.Now().Add(1000 * time.Hour) })
	sweepUntil(t, ctx, sweepOver(s, s.reader, store, s.archive), func(r archivecleanup.SweepResult) bool { return r.Archived == 1 })

	stats, err := cleaner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("cleanup after the sweep: %v", err)
	}
	if stats.RunsCleaned != 1 {
		t.Fatalf("cleanup metrics = %+v, want the archived run retired", stats)
	}
	awaitWorkflowGone(t, ctx, s.reader, s.namespace, runID)

	after, err := a.svc.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil {
		t.Fatalf("GetRun after retirement: %v", err)
	}
	if !proto.Equal(after.Msg.GetRun(), before.Msg.GetRun()) {
		t.Fatalf("after retirement the run reads\n%v\nand before it read\n%v", after.Msg.GetRun(), before.Msg.GetRun())
	}
	eventsAfter := streamed()
	if fmt.Sprint(definitionEventTypes(eventsAfter)) != fmt.Sprint(definitionEventTypes(eventsBefore)) {
		t.Fatalf("after retirement the stream reads %v, before it read %v",
			definitionEventTypes(eventsAfter), definitionEventTypes(eventsBefore))
	}
	s.transcript.step("terminated, archived from its history, retired, read back: %v", definitionEventTypes(eventsAfter))
}

// TestIntegration_ArchiveSweep_AStoreThatIsDownSaysSoAndHeals: a sweep with
// object storage cut writes nothing, says which runs are at risk and when the
// first expires, and archives them on the first pass after it returns. The pass
// reaches the store through a proxy that refuses it, not through a fake.
func TestIntegration_ArchiveSweep_AStoreThatIsDownSaysSoAndHeals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "archive-sweep/store-down")
	runID := s.startAndTerminate(t)

	proxy := s.minioProxy()
	viaProxy := s.cfg
	viaProxy.S3Endpoint = "http://" + proxy.Addr()
	store, err := artifacts.NewS3RunArchive(ctx, viaProxy)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	reader := engine.NewTemporalRunReader(s.control).WithSnapshotStore(s.snapshotStoreVia(proxy))
	sweeper := sweepOver(s, reader, store, engine.NewRunArchiver(store))

	proxy.Cut()
	down := sweepUntil(t, ctx, sweeper, func(r archivecleanup.SweepResult) bool { return r.Unconfirmed > 0 })
	if down.Archived != 0 || down.EarliestExpiry.IsZero() {
		t.Fatalf("with the store cut: %+v, want nothing archived and the risk dated", down)
	}
	if proxy.Stats().Refused == 0 {
		t.Fatal("no request met the cut: the outage was not exercised")
	}

	proxy.Restore()
	if has, err := store.HasRun(ctx, runID); err != nil || has {
		t.Fatalf("HasRun after the cut = %v (%v), want nothing written during it", has, err)
	}
	healed := sweepUntil(t, ctx, sweeper, func(r archivecleanup.SweepResult) bool { return r.Archived == 1 })
	if healed.Unconfirmed != 0 {
		t.Fatalf("after the store returned: %+v, want nothing at risk", healed)
	}
	run, err := engine.NewRunArchiver(store).LoadRun(ctx, runID)
	if err != nil || len(run.GetTaskRuns()) == 0 || run.GetError().GetCode() != "RUN_TERMINATED" {
		t.Fatalf("the archived run = %v (%v), want RUN_TERMINATED with its tasks", run, err)
	}
	s.transcript.step("store cut: %+v; store back: %+v", down, healed)
}

// TestIntegration_ArchiveSweep_AnEndingStorageRefusedIsArchived: a run whose
// every snapshot write was refused publishes nothing, so nothing archived it.
// The sweep does, from the history alone, with its code and with the missing
// record of its tasks stated.
func TestIntegration_ArchiveSweep_AnEndingStorageRefusedIsArchived(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := newChaosStackWith(t, ctx, "archive-sweep/ending-refused", s3Config(), store)
	runID, _, _, _ := s.corruptStart(t, func(int, int64) bool { return true })
	if has, err := store.HasRun(ctx, runID); err != nil || has {
		t.Fatalf("HasRun before the sweep = %v (%v), want no archive", has, err)
	}
	sweepUntil(t, ctx, sweepOver(s, s.reader, store, s.archive), func(r archivecleanup.SweepResult) bool { return r.Archived == 1 })

	run, err := s.archive.LoadRun(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRun: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != engine.RunDefinitionCorruptCode ||
		!strings.Contains(run.GetError().GetDetails(), "was ever written") {
		t.Fatalf("the archived run = %v, want FAILED %s saying no record was written", run, engine.RunDefinitionCorruptCode)
	}
	events, err := s.archive.LoadRunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRunEvents: %v", err)
	}
	if got := fmt.Sprint(definitionEventTypes(events)); got != "[1:RUN_FAILED]" {
		t.Fatalf("the archived stream = %s, want the close at 1", got)
	}
}

// TestIntegration_SkippedNumber_ReadsLiveAndFromTheArchive: object storage
// refuses one mid-run event's snapshot on a real cluster, so the run uses that
// number and publishes nothing under it. A live read through the API names it
// and reaches the run's end; the archive records it; a read of the archive over
// it ends DataLoss and one past it reads on.
func TestIntegration_SkippedNumber_ReadsLiveAndFromTheArchive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "archive-sweep/skipped-number")
	const skipped = int64(3)
	var mu sync.Mutex
	refusals := 0
	store := storeHooks{SnapshotStore: s.store, onWrite: func(sequence int64, _ int32) error {
		if sequence != skipped {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		refusals++
		return errors.New("the store refuses this event's snapshot")
	}}
	s.startReplica("replica-a", replicaOptions{store: store})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	plan, _ := s.tcPlan()
	runID := s.newRunID()
	s.transcript.run(runID)
	s.start(runID, compile(t, sweepChainHeader+guardChain("c", 2, "")), nil, plan)
	if status, _ := s.wait(runID, 3*time.Minute); status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatal("the run did not end")
	}
	mu.Lock()
	refused := refusals
	mu.Unlock()
	if refused == 0 {
		t.Fatal("the store refused nothing: the run never reached the number")
	}

	a := s.serveAPI(api.Config{})
	read := func(from int64) ([]*orkstackv1.Event, error) {
		var events []*orkstackv1.Event
		err := a.svc.StreamRunEvents(ctx, runID, "", from, func(e *orkstackv1.Event) error {
			events = append(events, e)
			return nil
		})
		return events, err
	}
	served, err := read(0)
	if err != nil {
		t.Fatalf("StreamRunEvents from 0: %v", err)
	}
	var gap *orkstackv1.Event
	for _, e := range served {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_STREAM_GAP {
			gap = e
		}
	}
	if gap == nil || gap.GetSequence() != skipped || !strings.Contains(gap.GetMessage(), "events 3–3 of this run were never published") {
		t.Fatalf("the live stream %v does not name 3", definitionEventTypes(served))
	}
	last := served[len(served)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("the live stream ends %v, want the run's end", last.GetType())
	}

	archived, err := a.svc.ArchivedEvents(ctx, runID, 0)
	if connect.CodeOf(err) != connect.CodeDataLoss || !strings.Contains(err.Error(), "events 3–3 of run "+runID+" were never published") {
		t.Fatalf("ArchivedEvents from 0 = %v (%v), want DataLoss naming 3", definitionEventTypes(archived), err)
	}
	pastIt, err := a.svc.ArchivedEvents(ctx, runID, skipped)
	if err != nil || len(pastIt) == 0 || pastIt[len(pastIt)-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("ArchivedEvents past it = %v (%v), want it to read on to the run's end", definitionEventTypes(pastIt), err)
	}
	s.transcript.step("live %v; archive past the gap %v", definitionEventTypes(served), definitionEventTypes(pastIt))
}
