//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const snapshotHistoryMeasurementEnv = "ORK_MEASURE_SNAPSHOT_HISTORY"

type historyMeasurement struct {
	tasks                   int
	historyBytes            int
	historyEvents           int
	maxActivityInputBytes   int
	snapshotCount           int
	binarySnapshotBytes     int
	oldSnapshotPayloadBytes int
	snapshotURIBytes        int
	beforeBytes             int
	firstSnapshotBytes      int
	lastSnapshotBytes       int
}

// TestIntegration_SnapshotExternalizationHistoryGrowth is the real-cluster
// measurement behind proof/M11.10. It is opt-in because 156 activities and
// their lifecycle projections make it substantially slower than a unit test.
//
// The before counterfactual follows the old wire shape exactly: replace every
// URI payload in PublishEvent's input with the same stored Run re-encoded by
// Temporal's data converter. Object storage remains compact binary protobuf;
// the old top-level activity argument was protojson. No assumed snapshot size
// or elapsed-time proxy enters the calculation.
func TestIntegration_SnapshotExternalizationHistoryGrowth(t *testing.T) {
	if os.Getenv(snapshotHistoryMeasurementEnv) == "" {
		t.Skip("set " + snapshotHistoryMeasurementEnv + "=1 to run the live history-size measurement")
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)

	cfg := artifacts.ConfigFromEnv()
	if cfg.Backend != artifacts.BackendS3 {
		infraAbsent(t, "history measurement requires ORK_ARTIFACT_BACKEND=s3, got %q", cfg.Backend)
	}
	requireMinIO(t, cfg)
	objectStore, err := artifacts.New(cfg)
	if err != nil {
		infraAbsent(t, "open snapshot object store: %v", err)
	}
	snapshots := engine.NewSnapshotStore(objectStore)

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-m11-10-runs-" + suffix
	taskQueue := "ork-m11-10-tasks-" + suffix

	runWorker := worker.New(c, runQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	taskWorker := worker.New(c, taskQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	taskWorker.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.GetTaskId(),
				ValuesJson: []byte(fmt.Sprintf(`{"task":%q}`, in.GetTaskId())),
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	eventWorker := worker.New(c, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).
			WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for name, w := range map[string]worker.Worker{
		"run": runWorker, "task": taskWorker, "event": eventWorker,
	} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	measurements := make([]historyMeasurement, 0, 3)
	for _, n := range []int{6, 50, 100} {
		runID := fmt.Sprintf("ork-m11-10-%03d-%s", n, suffix)
		starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, taskQueue)
		if err := starter.StartRun(ctx, runID, measuredOrkflow(n), nil, rawPlan(taskQueue)); err != nil {
			t.Fatalf("start %d-task run: %v", n, err)
		}
		if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
			t.Fatalf("wait for %d-task run: %v", n, err)
		}
		m := measureRunHistory(t, ctx, c, objectStore, snapshots, runID, n)
		measurements = append(measurements, m)
		t.Logf("EVIDENCE tasks=%d after_bytes=%d events=%d max_activity_input_bytes=%d before_bytes=%d ratio=%.4f bytes_per_event=%.2f bytes_per_task=%.2f snapshots=%d binary_snapshot_bytes=%d old_snapshot_payload_bytes=%d uri_payload_bytes=%d first_snapshot_bytes=%d last_snapshot_bytes=%d snapshot_growth_bytes_per_task=%.2f",
			m.tasks, m.historyBytes, m.historyEvents, m.maxActivityInputBytes,
			m.beforeBytes, float64(m.beforeBytes)/float64(m.historyBytes),
			float64(m.historyBytes)/float64(m.historyEvents),
			float64(m.historyBytes)/float64(m.tasks), m.snapshotCount,
			m.binarySnapshotBytes, m.oldSnapshotPayloadBytes, m.snapshotURIBytes,
			m.firstSnapshotBytes, m.lastSnapshotBytes,
			float64(m.lastSnapshotBytes-m.firstSnapshotBytes)/float64(m.tasks-1))
		meanSnapshotPayload := float64(m.oldSnapshotPayloadBytes) / float64(m.snapshotCount)
		simulated := m.historyBytes - m.snapshotURIBytes + 40960*m.snapshotCount
		t.Logf("MODEL tasks=%d assumed_snapshot_bytes_per_emit=40960 measured_mean_old_payload_bytes_per_emit=%.2f simulated_before_bytes=%d rederived_before_bytes=%d",
			m.tasks, meanSnapshotPayload, simulated, m.beforeBytes)
	}

	for _, m := range measurements {
		if m.maxActivityInputBytes >= 256*1024 {
			t.Errorf("%d-task max activity input = %d bytes, want < %d", m.tasks, m.maxActivityInputBytes, 256*1024)
		}
		if m.historyBytes >= 50*1024*1024 {
			t.Errorf("%d-task history = %d bytes, want < 50 MiB", m.tasks, m.historyBytes)
		}
	}
	firstSlope := float64(measurements[1].historyBytes-measurements[0].historyBytes) / 44
	secondSlope := float64(measurements[2].historyBytes-measurements[1].historyBytes) / 50
	ratio := secondSlope / firstSlope
	t.Logf("EVIDENCE linear_after_growth bytes_per_added_task_6_to_50=%.2f bytes_per_added_task_50_to_100=%.2f slope_ratio=%.4f", firstSlope, secondSlope, ratio)
	if ratio < 0.8 || ratio > 1.2 {
		t.Errorf("after-history growth is not linear enough: slope ratio %.4f, want [0.8, 1.2]", ratio)
	}
	firstBeforeSlope := float64(measurements[1].beforeBytes-measurements[0].beforeBytes) / 44
	secondBeforeSlope := float64(measurements[2].beforeBytes-measurements[1].beforeBytes) / 50
	beforeSlopeRatio := secondBeforeSlope / firstBeforeSlope
	firstEventSlope := float64(measurements[1].historyEvents-measurements[0].historyEvents) / 44
	secondEventSlope := float64(measurements[2].historyEvents-measurements[1].historyEvents) / 50
	t.Logf("EVIDENCE growth_contrast before_bytes_per_added_task_6_to_50=%.2f before_bytes_per_added_task_50_to_100=%.2f before_slope_ratio=%.4f events_per_added_task_6_to_50=%.2f events_per_added_task_50_to_100=%.2f",
		firstBeforeSlope, secondBeforeSlope, beforeSlopeRatio, firstEventSlope, secondEventSlope)
	if beforeSlopeRatio < 1.5 {
		t.Errorf("counterfactual history does not show quadratic growth: slope ratio %.4f, want >= 1.5", beforeSlopeRatio)
	}
}

func measureRunHistory(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	objectStore artifacts.Store,
	snapshots engine.SnapshotStore,
	runID string,
	tasks int,
) historyMeasurement {
	t.Helper()
	m := historyMeasurement{tasks: tasks}
	history := &historypb.History{}
	iter := c.GetWorkflowHistory(ctx, runID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	dataConverter := converter.GetDefaultDataConverter()
	seenURI := map[string]struct{}{}
	var uris []string
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history of %s: %v", runID, err)
		}
		history.Events = append(history.Events, event)
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil {
			continue
		}
		inputBytes := proto.Size(attrs.GetInput())
		if inputBytes > m.maxActivityInputBytes {
			m.maxActivityInputBytes = inputBytes
		}
		if attrs.GetActivityType().GetName() != engine.ActivityPublishEvent {
			continue
		}
		payloads := attrs.GetInput().GetPayloads()
		if len(payloads) < 2 {
			t.Fatalf("PublishEvent at event %d has %d payloads", event.GetEventId(), len(payloads))
		}
		var uri string
		if err := dataConverter.FromPayload(payloads[1], &uri); err != nil {
			t.Fatalf("decode snapshot URI at event %d: %v", event.GetEventId(), err)
		}
		m.snapshotURIBytes += proto.Size(payloads[1])
		if _, duplicate := seenURI[uri]; duplicate {
			t.Fatalf("duplicate snapshot URI %q", uri)
		}
		seenURI[uri] = struct{}{}
		uris = append(uris, uri)
	}
	m.historyEvents = len(history.Events)
	m.historyBytes = proto.Size(history)
	m.snapshotCount = len(uris)
	for i, uri := range uris {
		data, err := snapshots.ReadSnapshot(ctx, uri)
		if err != nil {
			t.Fatalf("read snapshot %q: %v", uri, err)
		}
		if i == 0 {
			m.firstSnapshotBytes = len(data)
		}
		m.lastSnapshotBytes = len(data)
		m.binarySnapshotBytes += len(data)

		// Object storage deliberately keeps compact binary protobuf. The old
		// top-level Run argument was encoded by Temporal's data converter as a
		// protojson Payload, so re-encode the real stored Run through that same
		// converter to obtain the exact payload it replaced.
		run := &orkstackv1.Run{}
		if err := proto.Unmarshal(data, run); err != nil {
			t.Fatalf("decode stored snapshot %q: %v", uri, err)
		}
		oldPayload, err := dataConverter.ToPayload(run)
		if err != nil {
			t.Fatalf("encode old snapshot payload %q: %v", uri, err)
		}
		m.oldSnapshotPayloadBytes += proto.Size(oldPayload)
		if err := objectStore.Delete(ctx, uri); err != nil {
			t.Fatalf("delete measured snapshot %q: %v", uri, err)
		}
	}
	m.beforeBytes = m.historyBytes - m.snapshotURIBytes + m.oldSnapshotPayloadBytes
	return m
}

func measuredOrkflow(tasks int) *orkstackv1.Orkflow {
	wf := &orkstackv1.Orkflow{Name: "snapshot-history", Version: "1.0.0"}
	for i := 0; i < tasks; i++ {
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{
			Id:   fmt.Sprintf("task-%03d", i+1),
			Uses: "fixture",
		})
	}
	return wf
}
