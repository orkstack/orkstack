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
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const snapshotHistoryMeasurementEnv = "ORK_MEASURE_SNAPSHOT_HISTORY"

type historyMeasurement struct {
	Tasks                 int `json:"tasks"`
	HistoryBytes          int `json:"history_bytes"`
	HistoryEvents         int `json:"history_events"`
	MaxActivityInputBytes int `json:"max_activity_input_bytes"`
	SnapshotCount         int `json:"snapshots"`
	BinarySnapshotBytes   int `json:"binary_snapshot_bytes"`
	SnapshotURIBytes      int `json:"uri_payload_bytes"`
	FirstSnapshotBytes    int `json:"first_snapshot_bytes"`
	LastSnapshotBytes     int `json:"last_snapshot_bytes"`
}

// TestIntegration_SnapshotExternalizationHistoryGrowth is the real-cluster
// measurement of what snapshot externalization buys: a run's history carries
// one snapshot URI per lifecycle event, never the Run itself, so history grows
// linearly in the task count and no activity input approaches Temporal's
// payload limit. It is opt-in because 156 activities and their lifecycle
// projections make it substantially slower than a unit test.
//
// Every number is measured — history bytes and events from the cluster,
// snapshot sizes from object storage — and no assumed snapshot size or
// elapsed-time proxy enters the calculation.
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
	requireObjectStore(t, cfg)
	snapshots, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		infraAbsent(t, "open snapshot store: %v", err)
	}

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-snapshot-history-runs-" + suffix
	taskQueue := "ork-snapshot-history-tasks-" + suffix

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
		runID := fmt.Sprintf("ork-snapshot-history-%03d-%s", n, suffix)
		starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, taskQueue)
		if err := starter.StartRun(ctx, runID, measuredOrkflow(n), nil, rawPlan(taskQueue)); err != nil {
			t.Fatalf("start %d-task run: %v", n, err)
		}
		if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
			t.Fatalf("wait for %d-task run: %v", n, err)
		}
		m := measureRunHistory(t, ctx, c, snapshots, runID, n)
		measurements = append(measurements, m)
		t.Logf("EVIDENCE tasks=%d history_bytes=%d events=%d max_activity_input_bytes=%d bytes_per_event=%.2f bytes_per_task=%.2f snapshots=%d binary_snapshot_bytes=%d uri_payload_bytes=%d first_snapshot_bytes=%d last_snapshot_bytes=%d snapshot_growth_bytes_per_task=%.2f",
			m.Tasks, m.HistoryBytes, m.HistoryEvents, m.MaxActivityInputBytes,
			float64(m.HistoryBytes)/float64(m.HistoryEvents),
			float64(m.HistoryBytes)/float64(m.Tasks), m.SnapshotCount,
			m.BinarySnapshotBytes, m.SnapshotURIBytes,
			m.FirstSnapshotBytes, m.LastSnapshotBytes,
			float64(m.LastSnapshotBytes-m.FirstSnapshotBytes)/float64(m.Tasks-1))
	}
	proof.WriteJSON(t, "engine", "snapshot-history-growth.json", measurements)

	for _, m := range measurements {
		if m.MaxActivityInputBytes >= 256*1024 {
			t.Errorf("%d-task max activity input = %d bytes, want < %d", m.Tasks, m.MaxActivityInputBytes, 256*1024)
		}
		if m.HistoryBytes >= 50*1024*1024 {
			t.Errorf("%d-task history = %d bytes, want < 50 MiB", m.Tasks, m.HistoryBytes)
		}
	}
	firstSlope := float64(measurements[1].HistoryBytes-measurements[0].HistoryBytes) / 44
	secondSlope := float64(measurements[2].HistoryBytes-measurements[1].HistoryBytes) / 50
	ratio := secondSlope / firstSlope
	t.Logf("EVIDENCE linear_growth bytes_per_added_task_6_to_50=%.2f bytes_per_added_task_50_to_100=%.2f slope_ratio=%.4f", firstSlope, secondSlope, ratio)
	if ratio < 0.8 || ratio > 1.2 {
		t.Errorf("history growth is not linear enough: slope ratio %.4f, want [0.8, 1.2]", ratio)
	}
}

func measureRunHistory(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	snapshots *artifacts.S3RunSnapshotStore,
	runID string,
	tasks int,
) historyMeasurement {
	t.Helper()
	m := historyMeasurement{Tasks: tasks}
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
		if inputBytes > m.MaxActivityInputBytes {
			m.MaxActivityInputBytes = inputBytes
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
		m.SnapshotURIBytes += proto.Size(payloads[1])
		if _, duplicate := seenURI[uri]; duplicate {
			t.Fatalf("duplicate snapshot URI %q", uri)
		}
		seenURI[uri] = struct{}{}
		uris = append(uris, uri)
	}
	m.HistoryEvents = len(history.Events)
	m.HistoryBytes = proto.Size(history)
	m.SnapshotCount = len(uris)
	for i, uri := range uris {
		data, err := snapshots.ReadSnapshot(ctx, uri)
		if err != nil {
			t.Fatalf("read snapshot %q: %v", uri, err)
		}
		if i == 0 {
			m.FirstSnapshotBytes = len(data)
		}
		m.LastSnapshotBytes = len(data)
		m.BinarySnapshotBytes += len(data)
		if err := snapshots.DeleteSnapshot(ctx, uri); err != nil {
			t.Fatalf("delete measured snapshot %q: %v", uri, err)
		}
	}
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
