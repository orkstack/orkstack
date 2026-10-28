//go:build integration

package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The task soak (spec T7/E7): one Orkflow at the task ceiling, in each of
// the measured shapes, submitted through the API with the task-ceiling
// defaults — the ceiling raised to the soak's size, continuation on, the definition (over
// 256 KiB at this size) passed by reference and snapshot overlays on — on
// real Temporal and real MinIO, run to completion across its generations,
// measured against E7's thresholds and against the closed form (7.4).
//
// As in the fan-out soak (each_soak_integration_test.go), most tasks run on
// in-process Orkers that answer as a real Orker would (a commit, two logs, a
// values reference and inline values) after the shape's run time; in the
// two flat shapes a real Orker — the production dispatcher, Docker and the
// first-party shell image — runs a sample beside them. The staged and deep
// shapes run on in-process Orkers only: their tasks read their ancestors'
// values, which the in-process Orkers never write to the store. What E7
// gates — history per generation, pending activities, saved state and
// snapshot bytes, read latency, replay, the closed form's per-task costs —
// is engine-side and does not depend on what ran a task.

// soakTasksVar is ORK_SOAK_TASKS: the Orkflow's size. Unset is the
// deployment default engine.DefaultOrkflowMaxTasks — the gated size, so a
// full proof re-proves the ceiling the commit set. The merge gate (spec 7.5)
// runs 4,000, then 3,000 and 2,000 only when a larger size missed a
// threshold.
const soakTasksVar = "ORK_SOAK_TASKS"

func soakTaskCount(t *testing.T) int {
	t.Helper()
	raw := os.Getenv(soakTasksVar)
	if raw == "" {
		return int(engine.DefaultOrkflowMaxTasks)
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 2 || n > 20000 {
		t.Fatalf("%s=%q: want a size between 2 and 20000", soakTasksVar, raw)
	}
	return n
}

// E7's thresholds beyond E4's (each_soak_integration_test.go).
const (
	taskSoakSnapshotBytesMax = 1 << 30 // snapshot bytes written under 1 GB
	taskSoakRunWithin        = 100 * time.Minute
	taskSoakShapeTimeout     = 110 * time.Minute
	// taskSoakLongRun marks the flat-long shape: its in-process tasks run
	// until no task has arrived for taskSoakQuiet — until the generation's
	// first wave is out, however long dispatching it takes under load — and
	// its real Orker's tasks sleep this long.
	taskSoakLongRun = 4 * time.Minute
	taskSoakQuiet   = 15 * time.Second
	// taskSoakHeartbeat is how often a long in-process task heartbeats.
	taskSoakHeartbeat = 5 * time.Second
	// taskSoakShortRun is about the shortest real container's run, so
	// completions land while the run is still dispatching.
	taskSoakShortRun = time.Second
	// deepLaneLength is the deep shape's longest lane (7.4): a real
	// reference per required ancestor stays under the input bound.
	deepLaneLength = 380
)

// taskSoakShape is one of 7.4's shapes.
type taskSoakShape struct {
	name string
	// lanes is how many tasks start together at n tasks: stage i of a lane
	// needs stage i−1 of the same lane.
	lanes func(n int) int
	run   time.Duration
	// real is whether a real Orker samples the shape.
	real bool
}

var taskSoakShapes = []taskSoakShape{
	{"flat-long", func(n int) int { return n }, taskSoakLongRun, true},
	{"flat-short", func(n int) int { return n }, taskSoakShortRun, true},
	{"staged", func(n int) int { return min(250, n) }, taskSoakShortRun, false},
	{"deep", func(n int) int { return (n + deepLaneLength - 1) / deepLaneLength }, taskSoakShortRun, false},
}

// taskSoakDocument is shape's Orkflow at n tasks, t0 … t(n−1), task i in
// lane i mod lanes, each a shell exec a real Orker can run.
func taskSoakDocument(name string, n, lanes int, run time.Duration) string {
	command := `[echo, ok]`
	if run > taskSoakShortRun {
		command = fmt.Sprintf(`[sleep, "%d"]`, int(run.Seconds()))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for i := range n {
		fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n    network: none\n    timeout_seconds: 600\n", i)
		b.WriteString("    retry: {maximum_attempts: 3}\n")
		if i >= lanes {
			fmt.Fprintf(&b, "    needs: [t%d]\n", i-lanes)
		}
		fmt.Fprintf(&b, "    with: {op: exec, command: %s}\n", command)
	}
	return b.String()
}

// soakFastTask answers a plain task after run with a result shaped as a real
// Orker's; a long run (flat-long) answers once no task has arrived for
// taskSoakQuiet instead, so the generation's whole first wave is dispatched
// before any of it completes.
func soakFastTask(run time.Duration) chaosBehavior {
	var mu sync.Mutex
	lastArrival := time.Now()
	return func(ctx context.Context, _ *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		lastArrival = time.Now()
		mu.Unlock()
		wait := run
		if run > taskSoakShortRun {
			wait = taskSoakHeartbeat
		}
		for {
			// A long task heartbeats as a real Orker does while its
			// container runs: the engine schedules every task with a
			// heartbeat timeout.
			if activity.IsActivity(ctx) && run > taskSoakShortRun {
				activity.RecordHeartbeat(ctx)
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			if run <= taskSoakShortRun {
				break
			}
			mu.Lock()
			quiet := time.Since(lastArrival)
			mu.Unlock()
			if quiet >= taskSoakQuiet {
				break
			}
			wait = min(taskSoakHeartbeat, taskSoakQuiet-quiet)
		}
		digest := func(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
		dir := strings.TrimSuffix(in.GetCommitUri(), "commit.json")
		values := []byte(`{"exitCode":0}`)
		stdout := "ok\n"
		now := timestamppb.Now()
		attempt := int32(1)
		if activity.IsActivity(ctx) {
			attempt = activity.GetInfo(ctx).Attempt
		}
		return &orkstackv1.ExecuteTaskResult{
			TaskId: in.GetTaskId(),
			Commit: &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: digest(in.GetCommitUri()),
				Etag: `"` + digest(in.GetCommitUri())[:32] + `"`, CreatedAt: now},
			SystemArtifacts: []*orkstackv1.ArtifactRef{
				{Name: "stdout.log", Uri: dir + "stdout.log", Sha256: digest(stdout), SizeBytes: int64(len(stdout))},
				{Name: "stderr.log", Uri: dir + "stderr.log", Sha256: digest(""), SizeBytes: 0},
			},
			ValuesRef: &orkstackv1.ArtifactRef{Name: "values.json", Uri: dir + "values.json", Sha256: digest(string(values)),
				SizeBytes: int64(len(values))},
			ValuesJson: values, ValuesInlined: true,
			Image: in.GetImage(), StartedAt: now, FinishedAt: now, OrkerId: orker, Attempt: attempt,
		}, nil
	}
}

// latencyHandler is the SDK's own OpenTelemetry metrics handler over a
// manual reader, so the soak reads the workflow-task execution latency the
// SDK records (temporal_workflow_task_execution_latency, in seconds) as a
// histogram with buckets fine enough for its p99.
type latencyHandler struct {
	reader  *sdkmetric.ManualReader
	handler client.MetricsHandler
}

// latencyBuckets are the histogram's bounds, in seconds: 5 ms steps to
// 2 s, then coarser.
func latencyBuckets() []float64 {
	var out []float64
	for ms := 5; ms <= 2000; ms += 5 {
		out = append(out, float64(ms)/1000)
	}
	return append(out, 3, 5, 10, 30, 60)
}

func newLatencyHandler() *latencyHandler {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(sdkmetric.NewView(
		sdkmetric.Instrument{Name: "temporal_workflow_task_execution_latency"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: latencyBuckets()}},
	)))
	return &latencyHandler{reader: reader, handler: opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{
		Meter: provider.Meter("task-soak")})}
}

// maxAndP99 is the latency's largest sample and 99th percentile (the upper
// bound of the bucket it falls in), in ms, and the sample count.
func (h *latencyHandler) maxAndP99(ctx context.Context) (float64, float64, int) {
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(ctx, &rm); err != nil {
		return 0, 0, 0
	}
	ms := func(seconds float64) float64 { return math.Round(seconds*100000) / 100 }
	var count uint64
	var maxSeen float64
	buckets := map[int]uint64{}
	var bounds []float64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "temporal_workflow_task_execution_latency" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, point := range hist.DataPoints {
				count += point.Count
				if v, ok := point.Max.Value(); ok {
					maxSeen = max(maxSeen, v)
				}
				bounds = point.Bounds
				for i, c := range point.BucketCounts {
					buckets[i] += c
				}
			}
		}
	}
	if count == 0 {
		return 0, 0, 0
	}
	target := uint64(math.Ceil(0.99 * float64(count)))
	var cumulative uint64
	p99 := maxSeen
	for i := 0; i <= len(bounds); i++ {
		cumulative += buckets[i]
		if cumulative >= target {
			if i < len(bounds) {
				p99 = min(bounds[i], maxSeen)
			}
			break
		}
	}
	return ms(maxSeen), ms(p99), int(count)
}

// taskSoakGeneration is one generation as task-soak/<size>-<shape>.json
// reports it, with the closed form's inputs measured from its history.
type taskSoakGeneration struct {
	Execution         string   `json:"execution"`
	Events            int      `json:"history_events"`
	Bytes             int64    `json:"history_bytes"`
	PeakPending       int      `json:"peak_pending_activities"`
	PeakPendingTasks  int      `json:"peak_pending_tasks"`
	CheckpointBytes   int64    `json:"checkpoint_bytes,omitempty"`
	ReplaySeconds     float64  `json:"replay_seconds"`
	PendingLimit      int      `json:"pending_activities_limit_exceeded"`
	TaskFailures      int      `json:"workflow_task_failures"`
	TaskFailureCauses []string `json:"workflow_task_failure_causes,omitempty"`
	closedFormView
}

// closedFormView is 7.4's quantities read from one generation's history.
type closedFormView struct {
	// Base is the history before the generation's first dispatch, and
	// PendingAtStart its PENDING tasks then.
	Base           int `json:"base_events"`
	PendingAtStart int `json:"pending_at_start"`
	Dispatches     int `json:"dispatches"`
	Collections    int `json:"collections"`
	// D and C are the events per dispatch and per completion (landing and
	// collection), attributed event by event; MaxLanding is the most events
	// one completion added before its collection, and MaxUncollected the
	// most completions landed and not yet collected at a workflow task.
	D              float64 `json:"d_events_per_dispatch"`
	C              float64 `json:"c_events_per_completion"`
	MaxLanding     int     `json:"max_landing_events"`
	MaxUncollected int     `json:"max_landed_uncollected"`
	// Peak is the largest raw length plus F + p·pending + f·in-flight at a
	// workflow task's start, and Predicted 7.4's base + F + p·P + Δ(W).
	Peak      int64 `json:"peak_history_plus_reserve"`
	Predicted int64 `json:"predicted_peak"`
}

// taskSoakRecord is task-soak/<size>-<shape>.json.
type taskSoakRecord struct {
	Size                 int                  `json:"size"`
	Shape                string               `json:"shape"`
	Width                int                  `json:"width"`
	RunID                string               `json:"run_id"`
	Elapsed              string               `json:"elapsed"`
	ByReference          bool                 `json:"definition_by_reference"`
	DefinitionBytes      int64                `json:"definition_object_bytes"`
	Overlays             bool                 `json:"snapshot_overlays"`
	GenerationCount      int                  `json:"generation_count"`
	Generations          []taskSoakGeneration `json:"generations"`
	InFlightCap          int32                `json:"in_flight_cap"`
	PeakPending          int                  `json:"peak_pending_activities"`
	PeakPendingTasks     int                  `json:"peak_pending_tasks"`
	PeakEvents           int                  `json:"peak_generation_events"`
	PeakBytes            int64                `json:"peak_generation_bytes"`
	EventsLimit          int64                `json:"history_max_events"`
	BytesLimit           int64                `json:"history_max_bytes"`
	LargestCheckpoint    int64                `json:"largest_checkpoint_bytes"`
	ArchiveEqualsStream  bool                 `json:"archive_equals_stream"`
	StreamEvents         int                  `json:"stream_events"`
	PendingLimitExceeded int                  `json:"pending_activities_limit_exceeded"`
	SnapshotBytes        int64                `json:"snapshot_bytes_written"`
	SnapshotObjects      int                  `json:"snapshot_objects"`
	GetRunLive           []float64            `json:"get_run_live_seconds"`
	GetRunLiveP95        float64              `json:"get_run_live_p95_seconds"`
	GetRunLiveBytes      int                  `json:"get_run_live_decoded_bytes"`
	GetRunLiveReads      []getRunRead         `json:"get_run_live_reads"`
	GetRunArchive        []float64            `json:"get_run_archive_seconds"`
	GetRunArchiveP95     float64              `json:"get_run_archive_p95_seconds"`
	GetRunArchiveBytes   int                  `json:"get_run_archive_decoded_bytes"`
	EventsReadSeconds    float64              `json:"events0_seconds"`
	SlowestReplaySeconds float64              `json:"slowest_generation_replay_seconds"`
	DeadlockTrips        int                  `json:"live_deadlock_detector_trips"`
	TaskLatencyMaxMs     float64              `json:"workflow_task_latency_max_ms"`
	TaskLatencyP99Ms     float64              `json:"workflow_task_latency_p99_ms"`
	TaskLatencySamples   int                  `json:"workflow_task_latency_samples"`
	MaxD                 float64              `json:"max_d_events_per_dispatch"`
	MaxC                 float64              `json:"max_c_events_per_completion"`
	MaxLanding           int                  `json:"max_landing_events"`
	MaxUncollected       int                  `json:"max_landed_uncollected"`
	Holds                int                  `json:"holds"`
	TasksOnRealOrkers    int                  `json:"tasks_on_real_orkers"`
	Missed               []string             `json:"missed_thresholds"`
}

// analyzeClosedForm reads 7.4's quantities from one generation's history:
// every event is attributed to a dispatch (its two publishes, their
// workflow tasks and snapshot markers, the ExecuteTask scheduling), a
// completion's landing (ExecuteTask's started and completed events and the
// workflow task it woke), a collection (TASK_SUCCEEDED's publish, its
// workflow task and marker) or neither; and at every workflow task's start
// the raw length plus the failure path's reserve for a run of plain tasks.
func analyzeClosedForm(t *testing.T, h *historypb.History, tasks, dispatchedBefore int, width int64) closedFormView {
	t.Helper()
	r := engine.DefaultHistoryReserve
	const (
		other = iota
		dispatch
		landing
		collect
	)
	type act struct {
		kind   int
		taskID string
	}
	acts := map[int64]*act{}
	wftBucket := map[int64]int{}     // WorkflowTaskScheduled id → bucket
	wftActivity := map[int64]int64{} // WorkflowTaskScheduled id → the ExecuteTask that woke it
	landed := map[int64]int{}        // ExecuteTask scheduled id → events its landing added
	bucket := make([]int, len(h.GetEvents()))
	activityOf := func(e *historypb.HistoryEvent) int64 {
		for _, id := range []int64{
			e.GetActivityTaskStartedEventAttributes().GetScheduledEventId(),
			e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId(),
			e.GetActivityTaskFailedEventAttributes().GetScheduledEventId(),
			e.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId(),
			e.GetActivityTaskCanceledEventAttributes().GetScheduledEventId(),
			e.GetActivityTaskCancelRequestedEventAttributes().GetScheduledEventId(),
		} {
			if id != 0 {
				return id
			}
		}
		return 0
	}
	kindOf := func(a *act) int {
		if a == nil {
			return other
		}
		return a.kind
	}
	events := h.GetEvents()
	for i, e := range events {
		switch {
		case e.GetActivityTaskScheduledEventAttributes() != nil:
			attrs := e.GetActivityTaskScheduledEventAttributes()
			a := &act{kind: other}
			payload := attrs.GetInput().GetPayloads()
			switch attrs.GetActivityType().GetName() {
			case task.ActivityExecuteTask:
				var in *orkstackv1.ExecuteTaskInput
				if len(payload) > 0 && converter.GetDefaultDataConverter().FromPayload(payload[0], &in) == nil {
					a.taskID = in.GetTaskId()
				}
				a.kind = dispatch
			case engine.ActivityPublishEvent:
				var event *orkstackv1.Event
				if len(payload) > 0 && converter.GetDefaultDataConverter().FromPayload(payload[0], &event) == nil {
					switch event.GetType() {
					case orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED:
						a.kind = dispatch
					case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
						a.kind, a.taskID = collect, event.GetTaskId()
					}
				}
			}
			acts[e.GetEventId()] = a
			bucket[i] = a.kind
			// The snapshot a publish names is written by the local
			// activity whose marker is recorded right before it; every
			// other marker (version decisions, a carried generation's
			// reads) is the generation's own.
			if attrs.GetActivityType().GetName() == engine.ActivityPublishEvent && i > 0 &&
				events[i-1].GetMarkerRecordedEventAttributes() != nil {
				bucket[i-1] = a.kind
			}
		case activityOf(e) != 0:
			id := activityOf(e)
			a := acts[id]
			bucket[i] = kindOf(a)
			if a != nil && a.taskID != "" && a.kind == dispatch && e.GetActivityTaskScheduledEventAttributes() == nil {
				// An ExecuteTask's own later events are its completion's
				// landing.
				if e.GetActivityTaskCancelRequestedEventAttributes() == nil {
					bucket[i] = landing
					landed[id]++
				}
			}
		case e.GetWorkflowTaskScheduledEventAttributes() != nil:
			b := other
			if i > 0 {
				if id := activityOf(events[i-1]); id != 0 {
					b = bucket[i-1]
					if b == landing {
						wftActivity[e.GetEventId()] = id
					}
				}
			}
			wftBucket[e.GetEventId()] = b
			bucket[i] = b
			if id := wftActivity[e.GetEventId()]; id != 0 {
				landed[id]++
			}
		default:
			var scheduled int64
			switch {
			case e.GetWorkflowTaskStartedEventAttributes() != nil:
				scheduled = e.GetWorkflowTaskStartedEventAttributes().GetScheduledEventId()
			case e.GetWorkflowTaskCompletedEventAttributes() != nil:
				scheduled = e.GetWorkflowTaskCompletedEventAttributes().GetScheduledEventId()
			case e.GetWorkflowTaskFailedEventAttributes() != nil:
				scheduled = e.GetWorkflowTaskFailedEventAttributes().GetScheduledEventId()
			case e.GetWorkflowTaskTimedOutEventAttributes() != nil:
				scheduled = e.GetWorkflowTaskTimedOutEventAttributes().GetScheduledEventId()
			}
			if scheduled != 0 {
				bucket[i] = wftBucket[scheduled]
				if id := wftActivity[scheduled]; id != 0 {
					landed[id]++
				}
			}
		}
	}
	v := closedFormView{PendingAtStart: tasks - dispatchedBefore}
	counts := map[int]int{}
	base := -1
	for i, b := range bucket {
		counts[b]++
		if b == dispatch && base < 0 {
			base = i
		}
	}
	v.Base = max(base, 0)
	for id, a := range acts {
		switch {
		case a.kind == dispatch && a.taskID != "":
			v.Dispatches++
			v.MaxLanding = max(v.MaxLanding, landed[id])
		case a.kind == collect:
			v.Collections++
		}
	}
	if v.Dispatches > 0 {
		v.D = math.Round(float64(counts[dispatch])/float64(v.Dispatches)*100) / 100
	}
	if v.Collections > 0 {
		v.C = math.Round(float64(counts[landing]+counts[collect])/float64(v.Collections)*100) / 100
	}
	// The peak, at every workflow task's start.
	var dispatched, collected int
	completed := map[string]bool{}
	collectedIDs := map[string]bool{}
	for _, e := range events {
		if attrs := e.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			a := acts[e.GetEventId()]
			if a.kind == collect {
				collected++
				collectedIDs[a.taskID] = true
			}
			if attrs.GetActivityType().GetName() == engine.ActivityPublishEvent && a.kind == dispatch {
				var event *orkstackv1.Event
				if converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &event) == nil &&
					event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
					dispatched++ // out of PENDING from its first publish: counted in flight, the upper bound
				}
			}
		}
		if id := e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId(); id != 0 {
			if a := acts[id]; a != nil && a.kind == dispatch && a.taskID != "" {
				completed[a.taskID] = true
			}
		}
		if e.GetWorkflowTaskStartedEventAttributes() == nil {
			continue
		}
		pending := int64(tasks - dispatchedBefore - dispatched)
		inFlight := int64(dispatched - collected)
		sum := e.GetEventId() + r.Fixed + r.PerPending*pending + r.PerInFlight*inFlight
		v.Peak = max(v.Peak, sum)
		uncollected := 0
		for id := range completed {
			if !collectedIDs[id] {
				uncollected++
			}
		}
		v.MaxUncollected = max(v.MaxUncollected, uncollected)
	}
	limit := engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents, MaxBytes: engine.DefaultHistoryMaxBytes,
		Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, engine.DefaultContinuationBytes,
		engine.DefaultContinuationRestoreTimeout)
	v.Predicted = engine.ShapePeak(limit, budget, width, int64(v.Base), int64(v.PendingAtStart))
	return v
}

// startTaskSoak starts one shape's deployment: an engine replica and a
// projection worker that advertise this build's capabilities (so the
// overlay check passes on its own evidence), in-process Orkers, a real one
// for a flat shape, and the API at the task-ceiling defaults.
func startTaskSoak(t *testing.T, ctx context.Context, shape taskSoakShape, n int) (*eachSoak, *latencyHandler) {
	t.Helper()
	// First, so its cleanup is the last to run: nothing the stack's own
	// teardown says follows the helper's report (proof.TimingBounds).
	timing := proof.TimingBounds(t)
	cfg := s3Config()
	requireObjectStore(t, cfg)
	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("run archive: %v", err)
	}
	s := newChaosStackWith(t, ctx, fmt.Sprintf("soak-tasks-%d-%s", n, shape.name), cfg, runArchive)
	env, err := localcatalog.CatalogEnv()
	if err != nil {
		infraAbsent(t, "%v", err)
	}
	catalog, err := dsl.ParseCatalog([]byte(strings.TrimPrefix(env, "ORK_ACTIVITY_CATALOG=")))
	if err != nil {
		t.Fatalf("parse the local catalog: %v", err)
	}
	e := &eachSoak{chaosStack: s, realIDs: map[string]string{}, inflight: map[string]map[string]bool{}, timing: timing}
	if shape.real {
		e.stores = s.realStores()
	}
	metrics := newLatencyHandler()
	identity := engine.EngineIdentity(fmt.Sprintf("%d@soak-replica-a@", os.Getpid()))
	c, err := client.DialContext(ctx, client.Options{HostPort: s.address, Namespace: s.namespace, Identity: identity,
		Logger: s.logs.logger("replica-a"), MetricsHandler: metrics.handler})
	if err != nil {
		t.Fatalf("dial the replica's client: %v", err)
	}
	t.Cleanup(c.Close)
	s.startReplica("replica-a", replicaOptions{client: c, identity: identity})
	s.startCapableProjection("soak-projection")
	fast := soakFastTask(shape.run) // one behavior: flat-long's quiet is the fleet's
	for i := 1; i <= soakFastOrkers; i++ {
		s.startOrker(fmt.Sprintf("fast-%d", i), fast)
	}
	if shape.real {
		for i := 1; i <= soakRealOrkers; i++ {
			e.startRealItemOrker(fmt.Sprintf("real-%d", i), nil, soakRealOrkerSlots)
		}
	}
	cluster, err := engine.NewTemporalReleaseCluster(s.control, s.reader, s.namespace, task.DefaultDeploymentName,
		s.runQueue, s.orkerQueue)
	if err != nil {
		t.Fatal(err)
	}
	e.api = s.serveAPI(api.Config{
		OrkflowMaxTasks: int32(n), ContinuationEnabled: true,
		DefinitionByReference: true, DefinitionObjects: s.store, SnapshotOverlays: true,
		Capabilities: cluster, Catalog: catalog, Resolver: dsl.NewResolver(""), Reader: e.countedReader(),
	})
	return e, metrics
}

// waitTasksSettled blocks until the stream has carried n TASK_SUCCEEDED
// events.
func (e *eachSoak) waitTasksSettled(t *testing.T, ctx context.Context, stream *streamCollector, n int) {
	t.Helper()
	deadline := time.Now().Add(taskSoakRunWithin)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		stream.mu.Lock()
		settled := 0
		var ended *orkstackv1.Event
		for _, event := range stream.events {
			switch event.GetType() {
			case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
				settled++
			case orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
				ended = event
			}
		}
		stream.mu.Unlock()
		if ended != nil {
			t.Fatalf("the run ended %v with %d tasks SUCCEEDED: %s", ended.GetType(), settled, ended.GetMessage())
		}
		if settled >= n {
			e.transcript.step("%d tasks SUCCEEDED in the stream", settled)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d tasks settled within %s", settled, n, taskSoakRunWithin)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}

// soakTaskRun submits the shape, waits for it, measures it against E7 and
// the closed form, and retires it.
func soakTaskRun(t *testing.T, ctx context.Context, e *eachSoak, metrics *latencyHandler, shape taskSoakShape, n int) taskSoakRecord {
	t.Helper()
	name := "soak-" + shape.name
	lanes := shape.lanes(n)
	e.api.apply(t, ctx, taskSoakDocument(name, n, lanes, shape.run))
	started := time.Now()
	runID := e.api.submit(t, ctx, name).GetRun().GetId()
	e.transcript.run(runID)
	e.transcript.step("submitted %s: %d tasks in %d lanes, run %s", name, n, lanes, runID)
	stream := e.openStream(e.reader, runID, 0)

	width := int64(min(lanes, int(engine.DefaultRunMaxInFlight)))
	rec := taskSoakRecord{Size: n, Shape: shape.name, Width: int(width), RunID: runID,
		InFlightCap: engine.DefaultRunMaxInFlight, EventsLimit: engine.DefaultHistoryMaxEvents,
		BytesLimit: engine.DefaultHistoryMaxBytes}

	e.waitTasksSettled(t, ctx, stream, n/2)
	liveTimes, liveReads, liveRun := e.liveGetRuns(t, ctx, runID, soakGetRunReads)
	rec.GetRunLive, rec.GetRunLiveP95, rec.GetRunLiveBytes = soakSeconds(liveTimes), soakP95(liveTimes), proto.Size(liveRun)
	rec.GetRunLiveReads = liveReads
	e.transcript.step("GetRun live: %v (run %v, %d bytes); read %s", rec.GetRunLive, liveRun.GetStatus(), rec.GetRunLiveBytes, describeReads(liveReads))

	status, err := e.wait(runID, taskSoakRunWithin)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	rec.Elapsed = time.Since(started).Round(time.Second).String()
	rec.TaskLatencyMaxMs, rec.TaskLatencyP99Ms, rec.TaskLatencySamples = metrics.maxAndP99(ctx)

	readStart := time.Now()
	events, err := e.reader.Events(ctx, runID, 0)
	if err != nil {
		t.Fatalf("Events(0): %v", err)
	}
	rec.EventsReadSeconds = time.Since(readStart).Seconds()
	requireContiguous(t, events)
	streamed := stream.result(t, 5*time.Minute)
	archived := awaitArchivedEvents(t, ctx, e.archive, runID)
	rec.StreamEvents = len(events)
	rec.ArchiveEqualsStream = strings.Join(eventKeys(archived), "\n") == strings.Join(eventKeys(streamed), "\n") &&
		strings.Join(eventKeys(streamed), "\n") == strings.Join(eventKeys(events), "\n")

	run, err := e.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	succeeded := 0
	for _, row := range run.GetTaskRuns() {
		if row.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			succeeded++
		}
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || succeeded != n {
		t.Fatalf("run %v with %d of %d tasks SUCCEEDED", run.GetStatus(), succeeded, n)
	}

	chain, histories := e.histories(runID)
	first := histories[0].GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	var plan engine.RunPlan
	if err := converter.GetDefaultDataConverter().FromPayload(first.GetInput().GetPayloads()[3], &plan); err != nil {
		t.Fatal(err)
	}
	rec.ByReference, rec.Overlays = plan.Definition != nil, plan.SnapshotOverlay != nil
	if plan.Definition != nil {
		rec.DefinitionBytes = plan.Definition.SizeBytes
	}
	completions := map[string]int{}
	dispatchedBefore := 0
	for i, h := range histories {
		info, err := e.control.DescribeWorkflowExecution(ctx, runID, chain[i])
		if err != nil {
			t.Fatal(err)
		}
		g := taskSoakGeneration{Execution: chain[i], Events: len(h.GetEvents()),
			Bytes: info.GetWorkflowExecutionInfo().GetHistorySizeBytes()}
		g.PeakPendingTasks, g.PeakPending = pendingPeaks(h)
		var deadlocks int
		g.PendingLimit, g.TaskFailures, deadlocks, g.TaskFailureCauses = soakTaskFailures(h)
		rec.DeadlockTrips += deadlocks
		for key, done := range factsOf(t, h).completedAt {
			completions[key] += len(done)
		}
		if i < len(histories)-1 {
			if open := openActivities(h); len(open) != 0 {
				t.Fatalf("generation %d switched with activities %v open", i, open)
			}
			g.CheckpointBytes = continuedCarryOf(t, h).GetCheckpoint().GetSizeBytes()
		}
		g.closedFormView = analyzeClosedForm(t, h, n, dispatchedBefore, width)
		dispatchedBefore += g.Dispatches
		replayStart := time.Now()
		if err := replayRecorded(h, runID); err != nil {
			t.Fatalf("generation %d does not replay (deadlock detection on): %v", i, err)
		}
		g.ReplaySeconds = math.Round(time.Since(replayStart).Seconds()*1000) / 1000
		rec.Generations = append(rec.Generations, g)
		rec.PeakPending = max(rec.PeakPending, g.PeakPending)
		rec.PeakPendingTasks = max(rec.PeakPendingTasks, g.PeakPendingTasks)
		rec.PeakEvents = max(rec.PeakEvents, g.Events)
		rec.PeakBytes = max(rec.PeakBytes, g.Bytes)
		rec.LargestCheckpoint = max(rec.LargestCheckpoint, g.CheckpointBytes)
		rec.PendingLimitExceeded += g.PendingLimit
		rec.SlowestReplaySeconds = max(rec.SlowestReplaySeconds, g.ReplaySeconds)
		rec.MaxD, rec.MaxC = max(rec.MaxD, g.D), max(rec.MaxC, g.C)
		rec.MaxLanding, rec.MaxUncollected = max(rec.MaxLanding, g.MaxLanding), max(rec.MaxUncollected, g.MaxUncollected)
	}
	rec.GenerationCount = len(chain)
	rec.Holds = len(e.logs.matching(logQuiesce, runID))
	if len(completions) != n || dispatchedBefore != n {
		t.Fatalf("%d tasks completed and %d dispatched across the generations, want %d", len(completions), dispatchedBefore, n)
	}
	for key, done := range completions {
		if done != 1 {
			t.Fatalf("%s completed %d times across the generations", key, done)
		}
	}
	rec.SnapshotBytes, rec.SnapshotObjects = soakSnapshotBytes(t, ctx, runID)
	for _, on := range e.ranOnAll() {
		for _, o := range on {
			if _, real := e.realIDs[o]; real {
				rec.TasksOnRealOrkers++
			}
		}
	}
	if shape.real && rec.TasksOnRealOrkers == 0 {
		t.Errorf("no task ran on a real Orker: the soak exercised no container")
	}

	// Retire: delete every generation, then read the run from the archive.
	for _, execution := range chain {
		if _, err := e.control.WorkflowService().DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace:         e.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: execution},
		}); err != nil {
			t.Fatalf("delete execution %s: %v", execution, err)
		}
	}
	for _, execution := range chain {
		e.waitExecutionGone(t, runID, execution, soakArchiveGetRunWait)
	}
	e.waitRetired(t, ctx, runID)
	archiveTimes, archivedRun := e.getRunTimes(t, ctx, runID, soakGetRunReads)
	if archivedRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("GetRun from the archive = %v", archivedRun.GetStatus())
	}
	rec.GetRunArchive, rec.GetRunArchiveP95 = soakSeconds(archiveTimes), soakP95(archiveTimes)
	rec.GetRunArchiveBytes = proto.Size(archivedRun)

	// E7's thresholds.
	miss := func(format string, args ...any) { rec.Missed = append(rec.Missed, fmt.Sprintf(format, args...)) }
	// Above about 420 tasks a definition is over a start payload's 256 KiB
	// and travels by reference (spec 5.1), and above 256 rows a run writes
	// overlays; every gated size is far above both.
	if (!rec.ByReference && n >= 1000) || (!rec.Overlays && int64(n) > engine.DefaultOverlayMinRows) {
		miss("the run did not start with the task-ceiling defaults: definition by reference %t, overlays %t", rec.ByReference, rec.Overlays)
	}
	if rec.PeakPendingTasks > int(rec.InFlightCap) {
		miss("peak pending tasks %d > the in-flight cap %d", rec.PeakPendingTasks, rec.InFlightCap)
	}
	if float64(rec.PeakEvents) >= soakEventShare*float64(rec.EventsLimit) {
		miss("peak generation events %d >= 80%% of %d", rec.PeakEvents, rec.EventsLimit)
	}
	if float64(rec.PeakBytes) >= soakEventShare*float64(rec.BytesLimit) {
		miss("peak generation bytes %d >= 80%% of %d", rec.PeakBytes, rec.BytesLimit)
	}
	if !rec.ArchiveEqualsStream {
		miss("archive != stream")
	}
	if rec.PendingLimitExceeded != 0 {
		miss("%d PENDING_ACTIVITIES_LIMIT_EXCEEDED", rec.PendingLimitExceeded)
	}
	if rec.SnapshotBytes >= taskSoakSnapshotBytesMax {
		miss("snapshot bytes written %d >= 1 GB", rec.SnapshotBytes)
	}
	for _, structural := range getRunStructuralMisses(liveReads) {
		miss("%s", structural)
	}
	if rec.DeadlockTrips != 0 {
		miss("%d live deadlock-detector trips", rec.DeadlockTrips)
	}
	if rec.MaxD > float64(engine.PlainDispatchEvents) {
		miss("measured d %.2f > %d", rec.MaxD, engine.PlainDispatchEvents)
	}
	if rec.MaxC > float64(engine.PlainCompletionEvents) {
		miss("measured c %.2f > %d", rec.MaxC, engine.PlainCompletionEvents)
	}
	if rec.MaxLanding > int(engine.LandedCompletionEvents) {
		miss("a landing added %d events > u = %d", rec.MaxLanding, engine.LandedCompletionEvents)
	}
	for i, g := range rec.Generations {
		if g.Peak > g.Predicted {
			miss("generation %d's peak of history plus reserve %d > the closed form's %d", i, g.Peak, g.Predicted)
		}
	}
	// The wall-clock bounds, last, through the timing helper (the same four
	// as the fan-out soak's; docs/PROOFS.md §12).
	e.checkReadTimings(shape.name, rec.GetRunLiveP95, rec.GetRunArchiveP95, rec.EventsReadSeconds, rec.SlowestReplaySeconds)
	return rec
}

// reportTaskSoak writes task-soak/<size>-<shape>.json and fails the test
// for every threshold the run missed.
func reportTaskSoak(t *testing.T, rec taskSoakRecord) {
	t.Helper()
	proof.WriteJSON(t, "task-soak", fmt.Sprintf("%d-%s.json", rec.Size, rec.Shape), rec)
	peaks := make([]string, len(rec.Generations))
	for i, g := range rec.Generations {
		peaks[i] = fmt.Sprintf("%d/%d", g.Peak, g.Predicted)
	}
	t.Logf("E7 %d-%s: %d generations in %s; by reference %t (%d B), overlays %t; peak pending %d (tasks %d, cap %d); "+
		"peak generation %d events, %d bytes; largest checkpoint %d; snapshots %d bytes in %d objects; archive == stream %t; "+
		"slowest replay %.3fs, %d deadlock trips; workflow-task latency max %.2f ms p99 %.2f ms (%d); d %.2f c %.2f "+
		"landing <= %d, most landed uncollected %d; peak/predicted per generation %v; GetRun p95 live %.3fs (%d B) "+
		"archive %.3fs (%d B); Events(0) %.3fs; %d tasks on real Orkers",
		rec.Size, rec.Shape, rec.GenerationCount, rec.Elapsed, rec.ByReference, rec.DefinitionBytes, rec.Overlays,
		rec.PeakPending, rec.PeakPendingTasks, rec.InFlightCap, rec.PeakEvents, rec.PeakBytes, rec.LargestCheckpoint,
		rec.SnapshotBytes, rec.SnapshotObjects, rec.ArchiveEqualsStream, rec.SlowestReplaySeconds, rec.DeadlockTrips,
		rec.TaskLatencyMaxMs, rec.TaskLatencyP99Ms, rec.TaskLatencySamples, rec.MaxD, rec.MaxC, rec.MaxLanding,
		rec.MaxUncollected, peaks, rec.GetRunLiveP95, rec.GetRunLiveBytes, rec.GetRunArchiveP95, rec.GetRunArchiveBytes,
		rec.EventsReadSeconds, rec.TasksOnRealOrkers)
	for _, missed := range rec.Missed {
		t.Errorf("E7 threshold missed at %d tasks (%s): %s", rec.Size, rec.Shape, missed)
	}
}

// taskSoakLock is the host-wide lock one task soak holds for its whole run:
// its four shapes, run side by side, are the load each of them is measured
// under, and a second task soak beside them (another proof's on the same
// host) would measure that one too. It is not the fan-out soaks' lock
// (soakAloneLock), so a fan-out soak never waits the half hour this one
// holds its own for: one of them may run beside it.
func taskSoakLock() string { return filepath.Join(os.TempDir(), "orkstack-task-soak.lock") }

// TestIntegration_SoakTasks: an Orkflow of ORK_SOAK_TASKS plain tasks
// (default the deployment's task ceiling) in each of 7.4's shapes — a flat
// stage of long tasks, the same with short ones (completions land while the
// run still dispatches), a staged DAG of 250 lanes and deep lanes of at most
// 380 — completes SUCCEEDED across its generations with the task-ceiling defaults,
// within E7's thresholds and the closed form. The four shapes run side by
// side, each on a deployment and Temporal namespace of its own: one engine
// dispatches a few tasks a second, so in sequence they would take over an
// hour, and side by side each is measured under the others' load.
func TestIntegration_SoakTasks(t *testing.T) {
	n := soakTaskCount(t)
	ctx, cancel := context.WithTimeout(context.Background(), taskSoakShapeTimeout)
	defer cancel()
	holdLock(t, ctx, taskSoakLock())
	t.Run("shapes", func(t *testing.T) {
		for _, shape := range taskSoakShapes {
			t.Run(shape.name, func(t *testing.T) {
				t.Parallel()
				e, metrics := startTaskSoak(t, ctx, shape, n)
				reportTaskSoak(t, soakTaskRun(t, ctx, e, metrics, shape, n))
			})
		}
	})
}

// TestIntegration_ATaskCeilingRunCarriesOnAtAReducedCeiling proves the
// last AGENTS.md §11 bullet's behaviour without the full-size soak: the
// flat-short shape runs at a deployment ceiling of 1,000 tasks, through the
// soak's own helpers, so every E7 threshold and its closed form is checked.
// A small run that never carried on would pass those thresholds without
// exercising generations, so it also asserts at least two generations, a
// definition passed by reference and snapshot overlays. The full ceilings
// are the nightly tier's (scripts/proof-nightly-tests.txt).
func TestIntegration_ATaskCeilingRunCarriesOnAtAReducedCeiling(t *testing.T) {
	const n = 1000
	ctx, cancel := context.WithTimeout(context.Background(), taskSoakShapeTimeout)
	defer cancel()
	// The E7 thresholds are absolute wall-clock bounds, so this run measures
	// under the same host-wide lock the full soak holds.
	holdLock(t, ctx, taskSoakLock())
	var shape taskSoakShape
	for _, candidate := range taskSoakShapes {
		if candidate.name == "flat-short" {
			shape = candidate
		}
	}
	e, metrics := startTaskSoak(t, ctx, shape, n)
	rec := soakTaskRun(t, ctx, e, metrics, shape, n)
	if rec.GenerationCount < 2 {
		rec.Missed = append(rec.Missed, fmt.Sprintf("the run used %d generations; a reduced ceiling must still carry on at least once",
			rec.GenerationCount))
	}
	if !rec.ByReference {
		rec.Missed = append(rec.Missed, "the run did not pass its definition by reference")
	}
	if !rec.Overlays {
		rec.Missed = append(rec.Missed, "the run did not write snapshot overlays")
	}
	t.Logf("reduced task ceiling: %d tasks at the deployment ceiling, %d generations, definition by reference %t, overlays %t",
		n, rec.GenerationCount, rec.ByReference, rec.Overlays)
	reportTaskSoak(t, rec)
}
