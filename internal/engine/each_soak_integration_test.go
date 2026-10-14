//go:build integration

package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
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

// The fan-out soak (spec T4/E4): one fan-out at the item ceiling, submitted
// through the API with engine.each_max_items raised to it and continuation
// on at every default (table TC), on real Temporal and real MinIO, run to
// completion across its generations, and measured against E4's thresholds.
//
// The Orkers are real in kind and in part. Real Orkers — the production
// dispatcher, Docker, the first-party json and shell images (the shell image
// is the one the Bash fixture holds to the ABI), real commits and attempt
// records — compute the fan-out's list (on a pool of their own) and run
// items beside two in-process Orkers that answer an item after a second with
// a result shaped as a real one is (a commit, two logs, a values reference
// and inline values). On the measured host one container takes about a
// second to run end to end, so 10,000 real containers would make one shape
// take hours; the engine-side quantities E4 gates — history per generation,
// pending activities, saved-state and snapshot bytes, read latency, replay —
// do not depend on what ran an item, and the real Orker's sample of items
// crosses the generations (the test fails if it ran none).

// soakItemsVar is ORK_SOAK_ITEMS: the fan-out's size. Unset is the built-in
// maximum, dsl.MaxEachItems — the gated size, so a full proof re-proves the
// ceiling the commit set. The merge gate (spec 4.5) runs 10,000 first, then
// 5,000 and 2,000 only when a larger size missed a threshold.
const soakItemsVar = "ORK_SOAK_ITEMS"

func soakItems(t *testing.T) int {
	t.Helper()
	raw := os.Getenv(soakItemsVar)
	if raw == "" {
		return dsl.MaxEachItems
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 2 || n > dsl.MaxEachItems {
		t.Fatalf("%s=%q: want a size between 2 and the built-in %d", soakItemsVar, raw, dsl.MaxEachItems)
	}
	return n
}

// E4's thresholds.
const (
	soakEventShare       = 0.8 // each generation's peak events and bytes under 80% of the limits
	soakSnapshotBytesMax = 512 << 20
	soakGetRunP95Max     = 2 * time.Second
	soakEventsReadMax    = 30 * time.Second
	soakReplayMax        = 30 * time.Second
	// One real item Orker of two slots per soak (C32: three of one slot):
	// a full proof runs these tests beside each other and beside every
	// other Docker-backed test, on a daemon that starts about one container
	// a second.
	soakRealOrkers        = 1
	soakRealOrkerSlots    = 2
	soakFastOrkers        = 2
	soakGetRunReads       = 10
	soakGetRunSpacing     = 500 * time.Millisecond
	soakListPool          = "soak-list"
	soakRunWithin         = 45 * time.Minute
	soakTestTimeout       = 60 * time.Minute
	soakArchiveGetRunWait = 2 * time.Minute
)

// eachSoak is one deployment for one soak: an engine replica advertising
// this build's capabilities, a projection worker, the Orkers, and the API
// with its fleet check over the stack's cluster.
type eachSoak struct {
	*chaosStack
	items  int
	api    *chaosAPI
	stores *realOrkerStores
	// realIDs is every real item Orker's Orker id, by name.
	realIDs map[string]string
	// inflight is, per real item Orker, the items it is running now.
	inflightMu sync.Mutex
	inflight   map[string]map[string]bool
}

// soakDocument is the soak's Orkflow: a json task computing [0, …, items−1],
// then one fan-out over it (maxParallel 0: none of its own), each item one
// shell exec of its element with three attempts, so an item whose Orker dies
// is retried elsewhere.
func soakDocument(name string, items, maxParallel int) string {
	parallel := ""
	if maxParallel > 0 {
		parallel = fmt.Sprintf(", max_parallel: %d", maxParallel)
	}
	return fmt.Sprintf(`name: %s
version: "1.0.0"
tasks:
  - id: list
    uses: json
    pool: %s
    network: none
    with: {op: query, nullInput: true, filter: "[range(0; %d)]"}
  - id: spread
    uses: shell
    needs: [list]
    network: none
    timeout_seconds: 300
    retry: {maximum_attempts: 3}
    each: {over: "{{ .tasks.list.output.result }}", max_items: %d%s}
    with: {op: exec, command: [echo, "{{ .each.item }}"]}
`, name, soakListPool, items, items, parallel)
}

// soakItemRunTime is how long an in-process Orker's item runs: about what
// the shortest real container takes, so a fan-out with no max_parallel fills
// the in-flight cap and holds near its hold line as a real one does.
const soakItemRunTime = time.Second

// soakFastItem answers an item after soakItemRunTime with a result shaped as
// a real Orker's: a commit, stdout and stderr logs, the values reference and
// the inline values, each with a distinct digest where a real one would
// differ.
func soakFastItem(ctx context.Context, _ *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	timer := time.NewTimer(soakItemRunTime)
	select {
	case <-ctx.Done():
		timer.Stop()
		return nil, ctx.Err()
	case <-timer.C:
	}
	digest := func(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
	dir := strings.TrimSuffix(in.GetCommitUri(), "commit.json")
	values := []byte(`{"exitCode":0}`)
	stdout := fmt.Sprintf("%d\n", in.GetEach().GetIndex())
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

// startEachSoak starts the soak's deployment; the real item Orkers are the
// caller's to start (startRealItemOrker).
func startEachSoak(t *testing.T, ctx context.Context, scenario string, items int) *eachSoak {
	t.Helper()
	cfg := s3Config()
	requireMinIO(t, cfg)
	runArchive, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("run archive: %v", err)
	}
	s := newChaosStackWith(t, ctx, scenario, cfg, runArchive)
	env, err := localcatalog.CatalogEnv()
	if err != nil {
		infraAbsent(t, "%v", err)
	}
	catalog, err := dsl.ParseCatalog([]byte(strings.TrimPrefix(env, "ORK_ACTIVITY_CATALOG=")))
	if err != nil {
		t.Fatalf("parse the local catalog: %v", err)
	}
	stores := s.realStores()
	e := &eachSoak{chaosStack: s, items: items, stores: stores, realIDs: map[string]string{},
		inflight: map[string]map[string]bool{}}

	// The engine: its run worker advertises this build's capabilities, as
	// cmd/ork's client does, so the API's fleet check passes on its own
	// evidence; its CountEachItems reads the list the real Orker committed.
	s.startReplica("replica-a", replicaOptions{
		identity:   engine.EngineIdentity(fmt.Sprintf("%d@soak-replica-a@", os.Getpid())),
		eachValues: stores.store,
	})
	s.startProjection(nil)

	// The list's own real Orker, on a pool of its own.
	listCall, _ := s.realOrkerCall("lister", soakListPool, stores, nil)
	s.startOrkerSized("lister", mustQueueForPool(t, soakListPool), s.control, task.Deployment{}, 1,
		func(ctx context.Context, _ *chaosStack, _ string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return listCall(ctx, in)
		})
	for i := 1; i <= soakFastOrkers; i++ {
		s.startOrker(fmt.Sprintf("fast-%d", i), soakFastItem)
	}

	cluster, err := engine.NewTemporalReleaseCluster(s.control, s.reader, s.namespace, task.DefaultDeploymentName,
		s.runQueue, s.orkerQueue)
	if err != nil {
		t.Fatal(err)
	}
	e.api = s.serveAPI(api.Config{
		EachMaxItems: int32(items), ContinuationEnabled: true, Capabilities: cluster,
		Catalog: catalog, Resolver: dsl.NewResolver(""),
	})
	return e
}

// startRealItemOrker starts one real item Orker, at slots
// containers at once, over c (the control client when nil; C32 hands one
// through a proxy, so cutting it is the host dying).
func (e *eachSoak) startRealItemOrker(name string, c client.Client, slots int) *chaosOrker {
	e.t.Helper()
	call, id := e.realOrkerCall(name, task.DefaultPool, e.stores, nil)
	e.realIDs[name] = id
	if c == nil {
		c = e.control
	}
	e.inflightMu.Lock()
	e.inflight[name] = map[string]bool{}
	e.inflightMu.Unlock()
	return e.startOrkerSized(name, e.orkerQueue, c, task.Deployment{}, slots,
		func(ctx context.Context, _ *chaosStack, _ string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			key := attemptKey(in)
			e.inflightMu.Lock()
			e.inflight[name][key] = true
			e.inflightMu.Unlock()
			defer func() {
				e.inflightMu.Lock()
				delete(e.inflight[name], key)
				e.inflightMu.Unlock()
			}()
			return call(ctx, in)
		})
}

// runningItems is the items the real item Orker name is running now.
func (e *eachSoak) runningItems(name string) []string {
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	var out []string
	for key := range e.inflight[name] {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// soakGeneration is one generation as each-soak/<size>-<shape>.json reports
// it.
type soakGeneration struct {
	Execution          string   `json:"execution"`
	Events             int      `json:"history_events"`
	Bytes              int64    `json:"history_bytes"`
	PeakPending        int      `json:"peak_pending_activities"`
	PeakPendingItems   int      `json:"peak_pending_items"`
	CheckpointBytes    int64    `json:"checkpoint_bytes,omitempty"`
	ItemsCompleted     int      `json:"items_completed"`
	ReplaySeconds      float64  `json:"replay_seconds"`
	PendingLimitFailed int      `json:"pending_activities_limit_exceeded"`
	TaskFailures       int      `json:"workflow_task_failures"`
	TaskFailureCauses  []string `json:"workflow_task_failure_causes,omitempty"`
}

// soakRecord is each-soak/<size>-<shape>.json.
type soakRecord struct {
	Size                 int              `json:"size"`
	Shape                string           `json:"shape"`
	MaxParallel          int              `json:"max_parallel"`
	RunID                string           `json:"run_id"`
	Elapsed              string           `json:"elapsed"`
	GenerationCount      int              `json:"generation_count"`
	Generations          []soakGeneration `json:"generations"`
	InFlightCap          int32            `json:"in_flight_cap"`
	PeakPending          int              `json:"peak_pending_activities"`
	PeakPendingItems     int              `json:"peak_pending_items"`
	PeakEvents           int              `json:"peak_generation_events"`
	PeakBytes            int64            `json:"peak_generation_bytes"`
	EventsLimit          int64            `json:"history_max_events"`
	BytesLimit           int64            `json:"history_max_bytes"`
	LargestCheckpoint    int64            `json:"largest_checkpoint_bytes"`
	CheckpointCap        int64            `json:"checkpoint_cap_bytes"`
	ArchiveEqualsStream  bool             `json:"archive_equals_stream"`
	StreamEvents         int              `json:"stream_events"`
	PendingLimitExceeded int              `json:"pending_activities_limit_exceeded"`
	SnapshotBytes        int64            `json:"snapshot_bytes_written"`
	SnapshotObjects      int              `json:"snapshot_objects"`
	GetRunLive           []float64        `json:"get_run_live_seconds"`
	GetRunLiveP95        float64          `json:"get_run_live_p95_seconds"`
	GetRunArchive        []float64        `json:"get_run_archive_seconds"`
	GetRunArchiveP95     float64          `json:"get_run_archive_p95_seconds"`
	GetRunDecodedBytes   int              `json:"get_run_decoded_bytes"`
	EventsReadSeconds    float64          `json:"events0_seconds"`
	SlowestReplaySeconds float64          `json:"slowest_generation_replay_seconds"`
	DeadlockTrips        int              `json:"live_deadlock_detector_trips"`
	Holds                int              `json:"holds"`
	ItemsOnRealOrkers    int              `json:"items_on_real_orkers"`
	Missed               []string         `json:"missed_thresholds"`
}

// soakP95 is the 95th percentile of samples, in seconds (the slowest of ten).
func soakP95(samples []time.Duration) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[int(math.Ceil(0.95*float64(len(sorted))))-1].Seconds()
}

func soakSeconds(samples []time.Duration) []float64 {
	out := make([]float64, len(samples))
	for i, d := range samples {
		out[i] = math.Round(d.Seconds()*1000) / 1000
	}
	return out
}

// getRunTimes reads the run through the API n times, soakGetRunSpacing
// apart as a page a reader keeps open re-reads it, and times each read.
func (e *eachSoak) getRunTimes(t *testing.T, ctx context.Context, runID string, n int) ([]time.Duration, *orkstackv1.Run) {
	t.Helper()
	var out []time.Duration
	var last *orkstackv1.Run
	for i := 0; i < n; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(soakGetRunSpacing):
			}
		}
		start := time.Now()
		resp, err := e.api.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
		if err != nil {
			t.Fatalf("GetRun %s: %v", runID, err)
		}
		out = append(out, time.Since(start))
		last = resp.Msg.GetRun()
	}
	return out, last
}

// soakTaskFailures counts a history's failed workflow tasks: those the cluster
// refused for too many pending activities, and all of them, with each one's
// cause and message, and how many of those were the deadlock detector's.
func soakTaskFailures(h *historypb.History) (pendingLimit, all, deadlocks int, causes []string) {
	events := h.GetEvents()
	for i, event := range events {
		attrs := event.GetWorkflowTaskFailedEventAttributes()
		if attrs == nil {
			continue
		}
		// What led to it: the events since its workflow task was scheduled,
		// and how long the task ran.
		var before []string
		var ran time.Duration
		for j := i - 1; j >= 0 && len(before) < 6; j-- {
			before = append([]string{events[j].GetEventType().String()}, before...)
			if events[j].GetEventId() == attrs.GetStartedEventId() {
				ran = event.GetEventTime().AsTime().Sub(events[j].GetEventTime().AsTime())
			}
		}
		all++
		if attrs.GetCause() == enums.WORKFLOW_TASK_FAILED_CAUSE_PENDING_ACTIVITIES_LIMIT_EXCEEDED {
			pendingLimit++
		}
		message := attrs.GetFailure().GetMessage()
		if strings.Contains(strings.ToLower(message), "deadlock") {
			deadlocks++
		}
		if len(message) > 200 {
			message = message[:200]
		}
		causes = append(causes, fmt.Sprintf("event %d: %v after %s, identity %q, following %v: %s", event.GetEventId(),
			attrs.GetCause(), ran.Round(time.Millisecond), attrs.GetIdentity(), before, message))
	}
	return pendingLimit, all, deadlocks, causes
}

// soakSnapshotBytes sums the objects under the run's snapshot prefix: every
// {seq}.pb snapshot and every continuation object the run wrote.
func soakSnapshotBytes(t *testing.T, ctx context.Context, runID string) (int64, int) {
	t.Helper()
	cfg := s3Config()
	raw := newRawS3ClientForCommitE2E(t, cfg)
	prefix, err := artifacts.RunSnapshotPrefix("default", runID)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	var objects int
	var token *string
	for {
		page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.S3Bucket),
			Prefix: aws.String(prefix), ContinuationToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range page.Contents {
			total += aws.ToInt64(object.Size)
			objects++
		}
		if !aws.ToBool(page.IsTruncated) {
			return total, objects
		}
		token = page.NextContinuationToken
	}
}

// soakRun submits one shape through the API, waits for it, measures it and
// retires it; check, when set, runs while the run is live (C32's kills).
func (e *eachSoak) soakRun(t *testing.T, ctx context.Context, shape string, maxParallel int,
	live func(runID string),
) (soakRecord, []*orkstackv1.Event) {
	t.Helper()
	name := "soak-" + shape
	e.api.apply(t, ctx, soakDocument(name, e.items, maxParallel))
	started := time.Now()
	submitted := e.api.submit(t, ctx, name)
	runID := submitted.GetRun().GetId()
	e.transcript.run(runID)
	e.transcript.step("submitted %s: %d items, max_parallel %d, run %s", name, e.items, maxParallel, runID)
	stream := e.openStream(e.reader, runID, 0)

	// Live reads, once half the items have finished: the run's rows are
	// all expanded and half of them carry a result.
	e.waitItemsSettled(t, ctx, stream, e.items/2)
	liveTimes, liveRun := e.getRunTimes(t, ctx, runID, soakGetRunReads)
	e.transcript.step("GetRun live: %v (run %v)", soakSeconds(liveTimes), liveRun.GetStatus())
	if live != nil {
		live(runID)
	}

	status, err := e.wait(runID, soakRunWithin)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	elapsed := time.Since(started)

	rec := soakRecord{Size: e.items, Shape: shape, MaxParallel: maxParallel, RunID: runID,
		Elapsed: elapsed.Round(time.Second).String(), InFlightCap: engine.DefaultRunMaxInFlight,
		EventsLimit: engine.DefaultHistoryMaxEvents, BytesLimit: engine.DefaultHistoryMaxBytes,
		CheckpointCap: engine.DefaultContinuationBytes,
		GetRunLive:    soakSeconds(liveTimes), GetRunLiveP95: soakP95(liveTimes)}

	// Events(0), the stream opened at submit, and the archive.
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

	// Every item SUCCEEDED, and its activity completed exactly once across
	// the generations.
	run, err := e.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := itemsSucceeded(run, "spread"); got != e.items || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %v with %d of %d items SUCCEEDED", run.GetStatus(), got, e.items)
	}
	chain, histories := e.histories(runID)
	completions := map[string]int{}
	for i, h := range histories {
		info, err := e.control.DescribeWorkflowExecution(ctx, runID, chain[i])
		if err != nil {
			t.Fatal(err)
		}
		g := soakGeneration{Execution: chain[i], Events: len(h.GetEvents()),
			Bytes: info.GetWorkflowExecutionInfo().GetHistorySizeBytes()}
		g.PeakPendingItems, g.PeakPending = pendingPeaks(h)
		var deadlocks int
		g.PendingLimitFailed, g.TaskFailures, deadlocks, g.TaskFailureCauses = soakTaskFailures(h)
		rec.DeadlockTrips += deadlocks
		facts := factsOf(t, h)
		for key, done := range facts.completedAt {
			if strings.HasPrefix(key, "spread[") {
				completions[key] += len(done)
				g.ItemsCompleted += len(done)
			}
		}
		if i < len(histories)-1 {
			if open := openActivities(h); len(open) != 0 {
				t.Fatalf("generation %d switched with activities %v open", i, open)
			}
			g.CheckpointBytes = continuedCarryOf(t, h).GetCheckpoint().GetSizeBytes()
		}
		replayStart := time.Now()
		if err := replayRecorded(h, runID); err != nil {
			t.Fatalf("generation %d does not replay (deadlock detection on): %v", i, err)
		}
		g.ReplaySeconds = math.Round(time.Since(replayStart).Seconds()*1000) / 1000
		rec.Generations = append(rec.Generations, g)
		rec.PeakPending = max(rec.PeakPending, g.PeakPending)
		rec.PeakPendingItems = max(rec.PeakPendingItems, g.PeakPendingItems)
		rec.PeakEvents = max(rec.PeakEvents, g.Events)
		rec.PeakBytes = max(rec.PeakBytes, g.Bytes)
		rec.LargestCheckpoint = max(rec.LargestCheckpoint, g.CheckpointBytes)
		rec.PendingLimitExceeded += g.PendingLimitFailed
		rec.SlowestReplaySeconds = max(rec.SlowestReplaySeconds, g.ReplaySeconds)
	}
	rec.GenerationCount = len(chain)
	rec.Holds = len(e.logs.matching(logQuiesce, runID))
	if len(completions) != e.items {
		t.Fatalf("%d items completed, want %d", len(completions), e.items)
	}
	for key, n := range completions {
		if n != 1 {
			t.Fatalf("%s completed %d times across the generations", key, n)
		}
	}
	rec.SnapshotBytes, rec.SnapshotObjects = soakSnapshotBytes(t, ctx, runID)
	defer func() {
		if rec.ItemsOnRealOrkers == 0 {
			t.Errorf("no item ran on a real Orker: the soak exercised no container")
		}
	}()
	for key, on := range e.ranOnAll() {
		if !strings.HasPrefix(key, "spread[") {
			continue
		}
		for _, o := range on {
			if _, real := e.realIDs[o]; real {
				rec.ItemsOnRealOrkers++
			}
		}
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
	rec.GetRunDecodedBytes = proto.Size(archivedRun)

	// E4's thresholds.
	miss := func(format string, args ...any) { rec.Missed = append(rec.Missed, fmt.Sprintf(format, args...)) }
	if rec.PeakPendingItems > int(rec.InFlightCap) {
		miss("peak pending items %d > the in-flight cap %d", rec.PeakPendingItems, rec.InFlightCap)
	}
	if rec.PeakPending >= int(engine.DefaultPendingActivityHardStop) {
		miss("peak pending activities %d >= the hard stop %d", rec.PeakPending, engine.DefaultPendingActivityHardStop)
	}
	if float64(rec.PeakEvents) >= soakEventShare*float64(rec.EventsLimit) {
		miss("peak generation events %d >= 80%% of %d", rec.PeakEvents, rec.EventsLimit)
	}
	if float64(rec.PeakBytes) >= soakEventShare*float64(rec.BytesLimit) {
		miss("peak generation bytes %d >= 80%% of %d", rec.PeakBytes, rec.BytesLimit)
	}
	if rec.LargestCheckpoint > rec.CheckpointCap/2 {
		miss("largest checkpoint %d > cap/2 %d", rec.LargestCheckpoint, rec.CheckpointCap/2)
	}
	if !rec.ArchiveEqualsStream {
		miss("archive != stream")
	}
	if rec.PendingLimitExceeded != 0 {
		miss("%d PENDING_ACTIVITIES_LIMIT_EXCEEDED", rec.PendingLimitExceeded)
	}
	if rec.SnapshotBytes > soakSnapshotBytesMax {
		miss("snapshot bytes written %d > %d", rec.SnapshotBytes, soakSnapshotBytesMax)
	}
	if rec.GetRunLiveP95 > soakGetRunP95Max.Seconds() {
		miss("GetRun p95 live %.3fs > %s", rec.GetRunLiveP95, soakGetRunP95Max)
	}
	if rec.GetRunArchiveP95 > soakGetRunP95Max.Seconds() {
		miss("GetRun p95 from the archive %.3fs > %s", rec.GetRunArchiveP95, soakGetRunP95Max)
	}
	if rec.EventsReadSeconds > soakEventsReadMax.Seconds() {
		miss("Events(0) %.3fs > %s", rec.EventsReadSeconds, soakEventsReadMax)
	}
	if rec.DeadlockTrips != 0 {
		miss("%d live deadlock-detector trips", rec.DeadlockTrips)
	}
	if rec.SlowestReplaySeconds > soakReplayMax.Seconds() {
		miss("slowest generation replay %.3fs > %s", rec.SlowestReplaySeconds, soakReplayMax)
	}
	return rec, events
}

// waitRetired blocks until the cluster no longer knows the run at all: its
// current execution describes NotFound and visibility lists none of its
// executions. Only then is a read the archive's alone, as it is for a run
// the cleanup worker retired long after it finished — not one racing the
// cluster's own asynchronous deletion.
func (e *eachSoak) waitRetired(t *testing.T, ctx context.Context, runID string) {
	t.Helper()
	deadline := time.Now().Add(soakArchiveGetRunWait)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		_, describeErr := e.control.DescribeWorkflowExecution(ctx, runID, "")
		var notFound *serviceerror.NotFound
		gone := errors.As(describeErr, &notFound)
		if gone {
			count, err := e.control.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
				Namespace: e.namespace, Query: fmt.Sprintf("WorkflowId = %q", runID)})
			gone = err == nil && count.GetCount() == 0
		}
		if gone {
			e.transcript.step("run %s retired: no execution describes and none is listed", runID)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s still known to the cluster %s after its executions were deleted (describe: %v)",
				runID, soakArchiveGetRunWait, describeErr)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}

// waitItemsSettled blocks until the stream has carried n SUCCEEDED item
// events, within the soak's deadline.
func (e *eachSoak) waitItemsSettled(t *testing.T, ctx context.Context, stream *streamCollector, n int) {
	t.Helper()
	deadline := time.Now().Add(soakRunWithin)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		stream.mu.Lock()
		settled := 0
		for _, event := range stream.events {
			if event.Item != nil && event.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
				settled++
			}
		}
		stream.mu.Unlock()
		if settled >= n {
			e.transcript.step("%d items SUCCEEDED in the stream", settled)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d items settled within %s", settled, n, soakRunWithin)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}

// ranOnAll is a copy of every attempt key's Orkers.
func (s *chaosStack) ranOnAll() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]string, len(s.ranOn))
	for k, v := range s.ranOn {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// reportSoak writes each-soak/<size>-<shape>.json and fails the test for
// every threshold the run missed.
func reportSoak(t *testing.T, rec soakRecord) {
	t.Helper()
	proof.WriteJSON(t, "each-soak", fmt.Sprintf("%d-%s.json", rec.Size, rec.Shape), rec)
	t.Logf("E4 %d-%s: %d generations in %s; peak pending %d (items %d, cap %d); peak generation %d events, %d bytes; "+
		"largest checkpoint %d; snapshots %d bytes in %d objects; GetRun p95 live %.3fs archive %.3fs (%d bytes); "+
		"Events(0) %.3fs; slowest replay %.3fs; %d items on real Orkers; archive == stream %t",
		rec.Size, rec.Shape, rec.GenerationCount, rec.Elapsed, rec.PeakPending, rec.PeakPendingItems, rec.InFlightCap,
		rec.PeakEvents, rec.PeakBytes, rec.LargestCheckpoint, rec.SnapshotBytes, rec.SnapshotObjects,
		rec.GetRunLiveP95, rec.GetRunArchiveP95, rec.GetRunDecodedBytes, rec.EventsReadSeconds,
		rec.SlowestReplaySeconds, rec.ItemsOnRealOrkers, rec.ArchiveEqualsStream)
	for _, missed := range rec.Missed {
		t.Errorf("E4 threshold missed at %d items (%s): %s", rec.Size, rec.Shape, missed)
	}
}

// TestIntegration_SoakFanOut: one fan-out of ORK_SOAK_ITEMS items (default
// the built-in maximum) with no max_parallel: only the in-flight cap and the
// holds near the hold line bound it.
func TestIntegration_SoakFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), soakTestTimeout)
	defer cancel()
	items := soakItems(t)
	e := startEachSoak(t, ctx, fmt.Sprintf("soak-%d-fan-out", items), items)
	for i := 1; i <= soakRealOrkers; i++ {
		e.startRealItemOrker(fmt.Sprintf("real-%d", i), nil, soakRealOrkerSlots)
	}
	rec, _ := e.soakRun(t, ctx, "fan-out", 0, nil)
	reportSoak(t, rec)
}

// TestIntegration_SoakFanOutMaxParallel: the same fan-out with max_parallel
// 64.
func TestIntegration_SoakFanOutMaxParallel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), soakTestTimeout)
	defer cancel()
	items := soakItems(t)
	e := startEachSoak(t, ctx, fmt.Sprintf("soak-%d-max-parallel", items), items)
	for i := 1; i <= soakRealOrkers; i++ {
		e.startRealItemOrker(fmt.Sprintf("real-%d", i), nil, soakRealOrkerSlots)
	}
	rec, _ := e.soakRun(t, ctx, "max-parallel", 64, nil)
	reportSoak(t, rec)
}
