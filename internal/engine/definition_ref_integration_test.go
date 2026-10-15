//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/coder/websocket"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Stage 5's integration evidence (E5): a definition passed by reference on a
// real cluster with its object in MinIO.

// The definition-by-reference fixture (testdata/history/README.md).
const (
	definitionRefFixture    = "testdata/history/run-definition-ref-v1.json"
	definitionRefObjectsDir = "testdata/history/run-definition-ref-v1.objects"
	definitionRefObject     = "definition.pb.gz"
)

// definitionRefFixtureDocument is the fixture's definition: a fan-in of
// five tasks whose `with:` notes (fixed pseudo-random hex) give its object a
// few KiB to read in several 1 KiB chunks.
func definitionRefFixtureDocument() string {
	r := rand.New(rand.NewSource(5))
	var b strings.Builder
	b.WriteString("name: by-reference\nversion: \"1.0.0\"\ntasks:\n")
	for i, id := range []string{"a", "b", "c", "d"} {
		note := make([]byte, 600)
		r.Read(note)
		fmt.Fprintf(&b, "  - id: %s\n    uses: shell\n    needs: []\n    with: {op: exec, n: %d, note: \"%x\"}\n", id, i, note)
	}
	b.WriteString("  - id: merge\n    uses: shell\n    needs: [a, b, c, d]\n    with: {op: exec}\n")
	return b.String()
}

// storeDefinitionIn writes wf's definition object under runID, as the
// submitting engine does, and returns the stub the start carries and plan
// naming the object.
func storeDefinitionIn(t *testing.T, ctx context.Context, store engine.SnapshotStore, runID string, wf *orkstackv1.Orkflow,
	plan engine.RunPlan,
) (*orkstackv1.Orkflow, engine.RunPlan, string) {
	t.Helper()
	if wf.GetNamespace() == "" {
		wf.Namespace = "default"
	}
	data, ref, err := engine.EncodeDefinitionObject(wf)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := store.WriteContinuation(ctx, wf.GetNamespace(), runID, 0, artifacts.ContinuationKindDefinition, data)
	if err != nil {
		t.Fatalf("write the definition object: %v", err)
	}
	plan.Definition = &ref
	return engine.DefinitionStub(wf), plan, uri
}

// chunksPerWorkflowTask is, for one execution's history, the most
// ReadContinuationChunk markers any one workflow task recorded.
func chunksPerWorkflowTask(t *testing.T, h *historypb.History) (most, total int) {
	t.Helper()
	current := 0
	for _, event := range h.GetEvents() {
		switch {
		case event.GetEventType() == enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			current = 0
		case event.GetMarkerRecordedEventAttributes() != nil && event.GetMarkerRecordedEventAttributes().GetMarkerName() == "LocalActivity":
			var data struct{ ActivityType string }
			_ = converter.GetDefaultDataConverter().FromPayloads(event.GetMarkerRecordedEventAttributes().GetDetails()["data"], &data)
			if data.ActivityType == engine.ActivityReadContinuationChunk {
				current++
				total++
				most = max(most, current)
			}
		}
	}
	return most, total
}

// TestIntegration_RecordDefinitionRefHistory records Stage 5's replay
// fixture from a REAL run on a real cluster, by a versioned worker, with its
// definition object in MinIO: generation 0 of a run whose definition travels
// by reference, read in 1 KiB chunks two per workflow task
// (run-definition-ref-v1.json, and the object byte for byte). It never
// skips: by default the recordings land under ORK_PROOF_DIR and the
// committed fixture is left alone, so every proof shows today's code still
// records and replays the path on the cluster. Only
// ORK_RECORD_GOLDEN_HISTORY=1 also writes the fixture. It FAILS unless the
// recording carries the gate, at least two batches of chunk markers and the
// run's success, and replays.
func TestIntegration_RecordDefinitionRefHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "definition-rec", true, canFixtureBehavior)
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""

	budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, engine.DefaultContinuationBytes, time.Minute)
	budget.ChunkBytes, budget.ChunksPerTask = 1024, 2
	runID := fmt.Sprintf("ork-definition-ref-%d", time.Now().UnixNano())
	wf := compile(t, definitionRefFixtureDocument())
	stub, plan, uri := storeDefinitionIn(t, ctx, s.snapshots, runID, wf, s.plan(engine.DefaultHistoryMaxEvents, &budget))
	if chunks := (plan.Definition.SizeBytes + 1023) / 1024; chunks < 3 {
		t.Fatalf("the definition object is %d bytes: too few chunks (%d) to batch", plan.Definition.SizeBytes, chunks)
	}
	if err := s.starter.StartRun(ctx, runID, stub, nil, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	recorded := s.history(runID, "")
	if markers := versionMarkersOf(t, recorded); markers[engine.DefinitionByReferenceChange] == 0 {
		t.Fatal("the recording carries no definition-by-reference marker")
	}
	most, total := chunksPerWorkflowTask(t, recorded)
	if most > int(budget.ChunksPerTask) || total < 3 {
		t.Fatalf("%d chunk markers, at most %d per workflow task: want at least 3, at most %d per task", total, most, budget.ChunksPerTask)
	}
	if err := replayRecorded(recorded, runID); err != nil {
		t.Fatalf("the recording does not replay against current code: %v", err)
	}
	object, err := s.snapshots.ReadSnapshot(ctx, uri)
	if err != nil || artifacts.ContinuationSHA256(object) != plan.Definition.SHA256 {
		t.Fatalf("read the definition back from MinIO: %v", err)
	}
	files := map[string][]byte{
		definitionRefFixture: encodeHistory(t, recorded),
		filepath.Join(definitionRefObjectsDir, definitionRefObject): object,
	}
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	for path, data := range files {
		proof.Write(t, "definition-by-reference", filepath.Base(path), data)
		if write {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatalf("write %s: %v", path, err)
			}
		}
	}
	if write {
		written = "testdata/history"
	}
	proof.WriteJSON(t, "definition-by-reference", "recorded-fixture.json", map[string]any{
		"run_id": runID, "events": len(recorded.GetEvents()), "definition_bytes": plan.Definition.SizeBytes,
		"chunk_markers": total, "most_per_workflow_task": most,
	})
	t.Logf("recorded %d events of run %s (%d chunk markers, at most %d per workflow task) into %s, replayed clean",
		len(recorded.GetEvents()), runID, total, most, written)
}

// versionMarkersOf is every Version marker's change id in a history, with
// its event id.
func versionMarkersOf(t *testing.T, h *historypb.History) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, event := range h.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "Version" {
			continue
		}
		var change string
		_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["change-id"], &change)
		out[change] = event.GetEventId()
	}
	return out
}

// definitionAPI is the API of a chaos stack with definitions passed by
// reference: the real fleet check against the stack's run channel, whose
// replica advertises this build's capabilities as cmd/ork's client does.
func (s *chaosStack) definitionAPI(t *testing.T, cfg api.Config) *chaosAPI {
	t.Helper()
	cluster, err := engine.NewTemporalReleaseCluster(s.control, s.reader, s.namespace, task.DefaultDeploymentName,
		s.runQueue, s.orkerQueue)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ContinuationEnabled, cfg.DefinitionByReference = true, true
	cfg.DefinitionObjects, cfg.Capabilities = s.store, cluster
	return s.serveAPI(cfg)
}

// capableReplica is replicaOptions whose run worker polls with this build's
// engine identity.
func capableReplica(name string, o replicaOptions) replicaOptions {
	o.identity = engine.EngineIdentity(fmt.Sprintf("%d@%s@", os.Getpid(), name))
	return o
}

// wideDefinitionDoc is chains parallel chains of length tasks each, every
// task's `with:` carrying a note of noteBytes (pseudo-random hex, so the
// object is realistic rather than all one repeated string).
func wideDefinitionDoc(name string, chains, length, noteBytes int) string {
	r := rand.New(rand.NewSource(int64(chains*length + noteBytes)))
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for c := 0; c < chains; c++ {
		for i := 0; i < length; i++ {
			note := make([]byte, noteBytes/2)
			r.Read(note)
			needs := ""
			if i > 0 {
				needs = fmt.Sprintf("c%03dt%02d", c, i-1)
			}
			fmt.Fprintf(&b, "  - id: c%03dt%02d\n    uses: shell\n    needs: [%s]\n    with: {op: exec, note: \"%x\"}\n", c, i, needs, note)
		}
	}
	return b.String()
}

// TestIntegration_ThreeThousandTaskDefinitionRuns: an Orkflow of 3,000 tasks
// (100 chains of 30, about 600 B of `with:` each — a definition well over a
// start payload's 256 KiB) is applied and submitted through the API with the
// flag on, passes the real fleet check, starts with its stub, and succeeds
// across generations, each of which copied the definition into its history.
// It quotes the object's size against a quarter of the cap, the most chunk
// markers any workflow task recorded against ChunksPerTask, and each
// generation's copy (definition plus checkpoint) against the cap, and
// replays every generation.
func TestIntegration_ThreeThousandTaskDefinitionRuns(t *testing.T) {
	// About 16 minutes on an idle dev cluster: every event of a 3,000-row
	// run writes its whole snapshot, which Stage 6's overlays shrink.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "E5/three-thousand")
	s.startReplica("replica-a", capableReplica("replica-a", replicaOptions{}))
	s.startProjection(nil)
	for i := 1; i <= 4; i++ {
		s.startOrker(fmt.Sprintf("orker-%d", i), instant)
	}
	a := s.definitionAPI(t, api.Config{OrkflowMaxTasks: 3000})
	doc := wideDefinitionDoc("three-thousand", 100, 30, 600)
	a.apply(t, ctx, doc)
	submitted := a.submit(t, ctx, "three-thousand")
	runID := submitted.GetRun().GetId()
	s.transcript.run(runID)
	started := time.Now()
	status, err := s.wait(runID, 40*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	took := time.Since(started)
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(run.GetTaskRuns()) != 3000 {
		t.Fatalf("GetRun = %v with %d rows (%v), want SUCCEEDED with 3000", run.GetStatus(), len(run.GetTaskRuns()), err)
	}
	chain, histories := s.histories(runID)
	first := histories[0].GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	var stub *orkstackv1.Orkflow
	var plan engine.RunPlan
	if err := converter.GetDefaultDataConverter().FromPayload(first.GetInput().GetPayloads()[1], &stub); err != nil {
		t.Fatal(err)
	}
	if err := converter.GetDefaultDataConverter().FromPayload(first.GetInput().GetPayloads()[3], &plan); err != nil {
		t.Fatal(err)
	}
	if len(stub.GetTasks()) != 0 || plan.Definition == nil || plan.Continuation == nil {
		t.Fatalf("the run started with %d tasks and definition %v: want the stub, by reference", len(stub.GetTasks()), plan.Definition)
	}
	cap := plan.Continuation.CheckpointMaxBytes
	if plan.Definition.SizeBytes > cap/4 {
		t.Fatalf("the definition object is %d bytes, over a quarter of the %d cap", plan.Definition.SizeBytes, cap)
	}
	type generation struct {
		Execution     string `json:"execution"`
		Events        int    `json:"history_events"`
		ChunkMarkers  int    `json:"chunk_markers"`
		MostPerTask   int    `json:"most_chunk_markers_per_workflow_task"`
		CopiedBytes   int64  `json:"copied_bytes"`
		ReplaySeconds string `json:"replay"`
	}
	var generations []generation
	mostOverall := 0
	for i, h := range histories {
		most, total := chunksPerWorkflowTask(t, h)
		mostOverall = max(mostOverall, most)
		copied := plan.Definition.SizeBytes
		if carry := carryOf(t, h); carry != nil {
			copied += carry.GetCheckpoint().GetSizeBytes()
		}
		if most > int(plan.Continuation.ChunksPerTask) || total == 0 || copied > cap {
			t.Fatalf("generation %d: %d chunk markers, at most %d per workflow task, %d bytes copied: want some, at most %d per "+
				"task, within the %d cap", i, total, most, copied, plan.Continuation.ChunksPerTask, cap)
		}
		replayStart := time.Now()
		if err := replayRecorded(h, runID); err != nil {
			t.Fatalf("generation %d does not replay: %v", i, err)
		}
		generations = append(generations, generation{Execution: chain[i], Events: len(h.GetEvents()), ChunkMarkers: total,
			MostPerTask: most, CopiedBytes: copied, ReplaySeconds: time.Since(replayStart).String()})
	}
	if len(generations) < 2 {
		t.Fatalf("the run had %d generations, want it to carry on", len(generations))
	}
	report := map[string]any{
		"run_id": runID, "tasks": 3000, "definition_bytes": plan.Definition.SizeBytes, "definition_raw_bytes": plan.Definition.RawBytes,
		"definition_parts": len(plan.Definition.Parts), "quarter_of_cap": cap / 4, "cap": cap,
		"chunks_per_task": plan.Continuation.ChunksPerTask, "most_chunk_markers_per_workflow_task": mostOverall,
		"generations": generations, "took": took.String(),
	}
	proof.WriteJSON(t, "definition-by-reference", "three-thousand.json", report)
	encoded, _ := json.Marshal(report)
	s.transcript.step("E5: %s", encoded)
	t.Logf("E5: 3000 tasks by reference: object %d B (≤ %d), %d generations, ≤ %d chunk markers per workflow task (≤ %d), "+
		"largest copy within %d, in %s", plan.Definition.SizeBytes, cap/4, len(generations), mostOverall,
		plan.Continuation.ChunksPerTask, cap, took)
}

// TestIntegration_DefinitionObjectLostBeforeStartFailsExplicitly: a run
// submitted with its definition passed by reference whose object is lost
// between the submit's write and the run's first read ends
// RUN_DEFINITION_UNAVAILABLE after its restore timeout, explicitly: its
// stream — read through the API's StreamRunEvents and the WebSocket bridge,
// both of which pass the tenancy check with the object gone — holds
// RUN_CREATED and RUN_FAILED, and it has an archive body.
func TestIntegration_DefinitionObjectLostBeforeStartFailsExplicitly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "E5/lost")
	hook := newBlockingHook(func(_ string, offset int64) bool { return offset == 0 })
	t.Cleanup(func() { hook.open(nil) })
	s.startReplica("replica-a", capableReplica("replica-a", replicaOptions{beforeRead: hook.read}))
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	a := s.definitionAPI(t, api.Config{OrkflowMaxTasks: 1000, ContinuationRestoreTimeout: time.Minute})
	a.apply(t, ctx, wideDefinitionDoc("lost", 20, 25, 600))
	runID := a.submit(t, ctx, "lost").GetRun().GetId()
	s.transcript.run(runID)
	hook.waitEntered(t, 2*time.Minute)
	start, err := s.reader.RunStart(ctx, runID)
	if err != nil || !start.ByReference {
		t.Fatalf("RunStart = %v (%v), want a start by reference", start, err)
	}
	uri, err := s.store.ContinuationURI(start.Orkflow.GetNamespace(), runID, 0, artifacts.ContinuationKindDefinition,
		start.Plan.Definition.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	raw := newRawS3ClientForCommitE2E(t, s.cfg)
	bucket, key := s3Key(t, uri)
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("delete the definition object: %v", err)
	}
	s.transcript.step("the definition object deleted before the run's first read")
	hook.open(nil)
	status, _ := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	if got := failureType(t, s.history(runID, "")); got != engine.RunDefinitionUnavailableCode {
		t.Fatalf("the execution failed with type %q, want %s", got, engine.RunDefinitionUnavailableCode)
	}
	events := s.requireOneRunRead(runID, nil, orkstackv1.Status_STATUS_FAILED, engine.RunDefinitionUnavailableCode)
	want := []string{"1:RUN_CREATED", "2:RUN_FAILED"}
	if got := definitionEventTypes(events); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Events(0) = %v, want %v", got, want)
	}
	// Through the API: StreamRunEvents and the WebSocket bridge.
	var streamed []*orkstackv1.Event
	if err := a.svc.StreamRunEvents(ctx, runID, "", 0, func(e *orkstackv1.Event) error {
		streamed = append(streamed, e)
		return nil
	}); err != nil || fmt.Sprint(definitionEventTypes(streamed)) != fmt.Sprint(want) {
		t.Fatalf("StreamRunEvents = %v (%v), want %v", definitionEventTypes(streamed), err, want)
	}
	conn, resp, err := websocket.Dial(ctx, strings.Replace(a.baseURL, "http://", "ws://", 1)+"/ws/runs/"+runID, nil)
	if err != nil {
		t.Fatalf("dial the run's socket: %v (%v)", err, resp)
	}
	frames := 0
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			break
		}
		frames++
	}
	conn.CloseNow()
	if frames != 2 || (resp != nil && resp.StatusCode != http.StatusSwitchingProtocols) {
		t.Fatalf("the socket delivered %d frames, want the run's 2", frames)
	}
	if _, err := s.archive.LoadRun(ctx, runID); err != nil {
		t.Fatalf("no archive body: %v", err)
	}
	s.transcript.step("RUN_DEFINITION_UNAVAILABLE: RUN_CREATED and RUN_FAILED through StreamRunEvents and the socket; archived")
	t.Logf("E5: a lost definition object ended the run %s explicitly; 2 events through the API and the socket; archived",
		engine.RunDefinitionUnavailableCode)
}

// TestIntegration_ScheduledFireOfALargeDefinitionRuns: a schedule fires an
// Orkflow whose definition is over a start payload's 256 KiB. The fire's
// preparation — the real api.ScheduleActivities on the engine's run worker —
// fits its own 256 KiB bound with the stub, writes the object under the run
// id it minted, and the run it starts succeeds.
func TestIntegration_ScheduledFireOfALargeDefinitionRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "E5/schedule")
	var acts *api.ScheduleActivities
	prepared := make(chan *orkstackv1.ScheduledRunPreparation, 4)
	register := func(w worker.Worker) {
		w.RegisterActivityWithOptions(func(ctx context.Context, rec *orkstackv1.ScheduleRecord, at time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
			prep, err := acts.PrepareScheduledRun(ctx, rec, at)
			if err == nil {
				prepared <- prep
			}
			return prep, err
		}, activity.RegisterOptions{Name: engine.ActivityPrepareScheduledRun})
	}
	s.startReplica("replica-a", capableReplica("replica-a", replicaOptions{extra: []func(worker.Worker){register}}))
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	scheduler := engine.NewTemporalScheduler(s.control, s.runQueue)
	a := s.definitionAPI(t, api.Config{OrkflowMaxTasks: 1000, Schedules: scheduler})
	acts = api.NewScheduleActivities(a.svc, nil)
	a.apply(t, ctx, wideDefinitionDoc("scheduled", 20, 25, 600))
	name := fmt.Sprintf("large-%d", time.Now().UnixNano())
	rec := &orkstackv1.ScheduleRecord{
		ScheduleId: fmt.Sprintf("sch-%016x", time.Now().UnixNano()), Name: name, Namespace: "default", OrkflowName: "scheduled",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL},
	}
	if _, err := scheduler.Create(ctx, rec, true); err != nil {
		t.Fatalf("create the schedule: %v", err)
	}
	t.Cleanup(func() { _ = scheduler.Delete(context.Background(), "default", name) })
	if err := scheduler.Fire(ctx, "default", name, orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED); err != nil {
		t.Fatalf("fire: %v", err)
	}
	var prep *orkstackv1.ScheduledRunPreparation
	select {
	case prep = <-prepared:
	case <-ctx.Done():
		t.Fatal("the fire never prepared a run")
	}
	runID := prep.GetRun().GetId()
	s.transcript.run(runID)
	if len(prep.GetOrkflow().GetTasks()) != 0 {
		t.Fatalf("the fire's preparation carries %d tasks, want the stub", len(prep.GetOrkflow().GetTasks()))
	}
	full := proto.Size(prep)
	if full > task.MaxActivityInputBytes {
		t.Fatalf("the preparation is %d bytes, over 256 KiB", full)
	}
	var plan engine.RunPlan
	if err := json.Unmarshal(prep.GetPlan(), &plan); err != nil || plan.Definition == nil {
		t.Fatalf("the prepared plan names no definition by reference (%v)", err)
	}
	// The fire starts the run as its child after the preparation returns.
	for deadline := time.Now().Add(time.Minute); ; {
		if _, err := s.control.DescribeWorkflowExecution(ctx, runID, ""); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("the fire never started run %s: %v", runID, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	status, err := s.wait(runID, 5*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the scheduled run closed %v: %v", status, err)
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(run.GetTaskRuns()) != 500 ||
		run.GetOrigin().GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE {
		t.Fatalf("GetRun = %v, %d rows, origin %v (%v); want a SUCCEEDED scheduled run of 500 tasks", run.GetStatus(),
			len(run.GetTaskRuns()), run.GetOrigin(), err)
	}
	s.transcript.figure("preparation_bytes", full)
	s.transcript.figure("definition_bytes", plan.Definition.SizeBytes)
	t.Logf("E5: a scheduled fire of a %d-byte definition (preparation %d B) ran 500 tasks to SUCCEEDED", plan.Definition.SizeBytes, full)
}
