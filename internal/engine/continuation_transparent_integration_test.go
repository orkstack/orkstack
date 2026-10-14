//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// transparentDocument is the 40-task run that must switch at least twice —
// at a natural idle point after its chain and after a hold in its fan-out —
// with an await open from the start and resolved after the first switch, a
// composed child, and a two-member pin.
func transparentDocument() string {
	var b strings.Builder
	b.WriteString("name: transparent\nversion: \"1.0.0\"\npins:\n  build: {}\ntasks:\n")
	b.WriteString("  - id: approve\n    needs: []\n    await: {timeout_seconds: 3600}\n")
	for i := 1; i <= 8; i++ {
		needs := "[]"
		if i > 1 {
			needs = fmt.Sprintf("[c%d]", i-1)
		}
		fmt.Fprintf(&b, "  - id: c%d\n    uses: shell\n    needs: %s\n    with: {op: exec}\n", i, needs)
	}
	b.WriteString("  - id: list\n    uses: shell\n    needs: [c8]\n    with: {op: exec}\n")
	b.WriteString("  - id: convert\n    uses: shell\n    needs: [list]\n" +
		"    each: {over: \"{{ .tasks.list.output.parts }}\", as: part, max_items: 50, max_parallel: 4}\n" +
		"    with: {op: exec, name: \"{{ .each.part.name }}\"}\n")
	b.WriteString("  - id: p1\n    uses: shell\n    pin: build\n    needs: [convert]\n    run_as_orker_user: true\n    with: {command: \"echo p1\"}\n")
	b.WriteString("  - id: p2\n    uses: shell\n    pin: build\n    needs: [p1]\n    run_as_orker_user: true\n    with: {command: \"echo p2\"}\n")
	b.WriteString("  - id: release\n    uses: orkflow:release-block@2.1.0\n    needs: [p2]\n    with: {image: fixed}\n")
	var stage []string
	for i := 1; i <= 20; i++ {
		fmt.Fprintf(&b, "  - id: f%d\n    uses: shell\n    needs: [release]\n    with: {op: exec}\n", i)
		stage = append(stage, fmt.Sprintf("f%d", i))
	}
	fmt.Fprintf(&b, "  - id: merge\n    uses: shell\n    needs: [%s, approve]\n    with: {op: exec}\n", strings.Join(stage, ", "))
	for i := 1; i <= 5; i++ {
		needs := "[merge]"
		if i > 1 {
			needs = fmt.Sprintf("[t%d]", i-1)
		}
		fmt.Fprintf(&b, "  - id: t%d\n    uses: shell\n    needs: %s\n    with: {op: exec}\n", i, needs)
	}
	return b.String()
}

const transparentChildDocument = `
name: release-block
version: "2.1.0"
inputs:
  image: {type: string, required: true}
tasks:
  - id: sign
    uses: shell
    with: {op: exec}
`

func transparentBehavior(_ context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetTaskId() == "list" {
		data := []byte(partsJSON(30))
		return &orkstackv1.ExecuteTaskResult{TaskId: "list", Attempt: 1, OrkerId: "transparent",
			ValuesJson: data, ValuesInlined: true, ValuesRef: s.values.put("mem://values/"+in.GetRunId()+"/list", data)}, nil
	}
	if in.GetEach() != nil {
		r := itemResult(in)
		r.OrkerId = "transparent"
		return r, nil
	}
	data := valuesJSON(map[string]any{"task": in.GetTaskId()})
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "transparent", ValuesJson: data,
		ValuesInlined: true, ValuesRef: s.values.put("mem://values/"+in.GetRunId()+"/"+in.GetTaskId(), data)}, nil
}

// transparentRun is the parent definition and its plan, with the child
// planned as SubmitRun plans one.
func (s *canStack) transparentRun(t *testing.T, runID string, plan engine.RunPlan) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	parent := compile(t, transparentDocument())
	parent.Namespace = "default"
	child := compile(t, transparentChildDocument)
	child.Namespace = "default"
	child.DefinitionDigest = childDigest
	for _, tk := range parent.GetTasks() {
		if tk.GetOrkflow() != nil {
			tk.Orkflow.DefinitionDigest = childDigest
		}
	}
	childPlan := rawPlan(s.orkerQueue)
	childPlan.CommitRoot = "mem://commits"
	cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = runID + "-child"
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	return parent, plan
}

// generationRecord is one generation as generations.json reports it.
type generationRecord struct {
	Execution       string `json:"execution"`
	Length          int    `json:"history_events"`
	SizeBytes       int64  `json:"history_bytes"`
	Switch          string `json:"switch,omitempty"` // natural, held, or empty for the last
	BootstrapEvents int    `json:"bootstrap_events,omitempty"`
	Growth          int    `json:"growth_events"`
	CheckpointBytes int64  `json:"checkpoint_bytes,omitempty"`
	HoldSeconds     string `json:"hold,omitempty"`
}

// publishedOf is every event a history's PublishEvent activities carried, in
// order.
func publishedOf(t *testing.T, h *historypb.History) []*orkstackv1.Event {
	t.Helper()
	var out []*orkstackv1.Event
	for _, event := range h.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != engine.ActivityPublishEvent {
			continue
		}
		var published *orkstackv1.Event
		if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &published); err != nil {
			t.Fatal(err)
		}
		out = append(out, published)
	}
	return out
}

// bootstrapEvents counts the events of a carried generation up to and
// including its last saved-state read.
func bootstrapEvents(t *testing.T, h *historypb.History) int {
	t.Helper()
	last := 0
	for i, event := range h.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "LocalActivity" {
			continue
		}
		var data struct{ ActivityType string }
		_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data)
		if data.ActivityType == engine.ActivityReadContinuationChunk {
			last = i + 1
		}
	}
	return last
}

// openActivities reports activities a closed generation scheduled and never
// closed (completed, failed, timed out or canceled).
func openActivities(h *historypb.History) []int64 {
	open := map[int64]bool{}
	for _, event := range h.GetEvents() {
		if event.GetActivityTaskScheduledEventAttributes() != nil {
			open[event.GetEventId()] = true
		}
		for _, id := range []int64{
			event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskFailedEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId(),
			event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId(),
		} {
			delete(open, id)
		}
	}
	var out []int64
	for id := range open {
		out = append(out, id)
	}
	return out
}

// TestIntegration_ContinuationRunIsTransparent: a 40-task run — a chain, a
// 30-item fan-out at max_parallel 4, an await resolved after the first
// switch, a composed child and a two-member pin — continues as new at least
// twice, at a natural idle point and after a hold, and no reader can tell:
// one run id; GetRun SUCCEEDED with generation 0's created_at; Events(0)
// contiguous and equal to a stream opened at submit and to the archive; each
// segment proto-equal to its generation's published events; one list row; no
// activity left open by a switch; every switch past its budget; every
// generation within its bounds; and every {seq}.pb snapshot agreeing with the
// stream up to its sequence. generations.json records the chain.
func TestIntegration_ContinuationRunIsTransparent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "transparent", false, transparentBehavior)
	const maxEvents, budgetEvents = 1000, 120
	budget := engine.DefaultContinuationBudget(budgetEvents, 1<<20, 5*time.Minute)
	runID := fmt.Sprintf("ork-transparent-%d", time.Now().UnixNano())
	wf, plan := s.transparentRun(t, runID, s.plan(maxEvents, &budget))
	if err := s.starter.StartRun(ctx, runID, wf, nil, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	var (
		streamMu  sync.Mutex
		streamed  []*orkstackv1.Event
		streamErr = make(chan error, 1)
	)
	go func() {
		streamErr <- s.reader.StreamEvents(ctx, runID, 0, func(event *orkstackv1.Event) error {
			streamMu.Lock()
			defer streamMu.Unlock()
			streamed = append(streamed, event)
			return nil
		})
	}()
	s.waitForSwitch(runID, 1, 3*time.Minute)
	if err := s.starter.SignalAwait(ctx, runID, completion("approve", `{"approved":true}`, "transparent")); err != nil {
		t.Fatalf("resolve approve after the first switch: %v", err)
	}
	if err := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	if err := <-streamErr; err != nil {
		t.Fatalf("the stream opened at submit ended with %v", err)
	}

	described, err := s.client.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) < 3 {
		t.Fatalf("%d generations, want at least 3", len(chain))
	}
	histories := make([]*historypb.History, len(chain))
	for i, execution := range chain {
		histories[i] = s.history(runID, execution)
	}

	// One run, its reads unchanged.
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || run.GetId() != runID {
		t.Fatalf("GetRun = %s %v", run.GetId(), run.GetStatus())
	}
	if want := histories[0].GetEvents()[0].GetEventTime().AsTime(); !run.GetCreatedAt().AsTime().Equal(want) {
		t.Fatalf("created_at %v, generation 0 started at %v", run.GetCreatedAt().AsTime(), want)
	}
	events, err := s.reader.Events(ctx, runID, 0)
	if err != nil {
		t.Fatal(err)
	}
	requireContiguous(t, events)
	streamMu.Lock()
	gotStream := eventKeys(streamed)
	streamMu.Unlock()
	if strings.Join(gotStream, "\n") != strings.Join(eventKeys(events), "\n") {
		t.Fatalf("the stream opened at submit saw\n%s\nEvents(0) reads\n%s", strings.Join(gotStream, "\n"), strings.Join(eventKeys(events), "\n"))
	}
	archived := awaitArchivedEvents(t, ctx, s.archive, runID)
	if strings.Join(eventKeys(archived), "\n") != strings.Join(eventKeys(events), "\n") {
		t.Fatal("the archive body's events differ from Events(0)")
	}

	// Every segment is its generation's published events, exactly.
	lastCarry := carryOf(t, histories[len(histories)-1])
	records := make([]generationRecord, len(chain))
	natural, held := 0, 0
	for i, h := range histories {
		info, err := s.client.DescribeWorkflowExecution(ctx, runID, chain[i])
		if err != nil {
			t.Fatal(err)
		}
		records[i] = generationRecord{Execution: chain[i], Length: len(h.GetEvents()),
			SizeBytes: info.GetWorkflowExecutionInfo().GetHistorySizeBytes()}
		if i == len(histories)-1 {
			continue
		}
		if open := openActivities(h); len(open) != 0 {
			t.Fatalf("generation %d switched with activities %v open", i, open)
		}
		seg := lastCarry.GetSegments()[i]
		data, err := s.snapshots.ReadSnapshot(ctx, seg.GetObject().GetUri())
		if err != nil {
			t.Fatal(err)
		}
		segment, err := engine.DecodeEventSegment(data, seg.GetObject())
		if err != nil {
			t.Fatal(err)
		}
		published := publishedOf(t, h)
		if len(published) != len(segment.GetEvents()) {
			t.Fatalf("generation %d published %d events, its segment holds %d", i, len(published), len(segment.GetEvents()))
		}
		for j := range published {
			if !proto.Equal(published[j], segment.GetEvents()[j]) {
				t.Fatalf("generation %d segment event %d differs from its history", i, j)
			}
		}
		handed := continuedCarryOf(t, h)
		records[i].CheckpointBytes = handed.GetCheckpoint().GetSizeBytes()
		snapData, err := s.snapshots.ReadSnapshot(ctx, handed.GetSnapshot().GetUri())
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.DecodeSwitchSnapshot(snapData, handed.GetSnapshot())
		if err != nil {
			t.Fatal(err)
		}
		records[i].Switch = "natural"
		for _, row := range snapshot.GetTaskRuns() {
			if row.GetEach().GetExpanded() && row.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
				records[i].Switch = "held" // items held back: only a quiesce leaves a fan-out so
			}
		}
		if records[i].Switch == "held" {
			held++
		} else {
			natural++
		}
	}
	if natural == 0 || held == 0 {
		t.Fatalf("switches %+v: want at least one natural and one held", records)
	}
	for i, h := range histories {
		base := 0
		if i > 0 {
			base = bootstrapEvents(t, h)
			records[i].BootstrapEvents = base
			bound := int(engine.RestoreHistoryBound(budget)) + 1 // + the await's recreated timer
			if base > bound {
				t.Fatalf("generation %d's bootstrap took %d events, over its bound %d", i, base, bound)
			}
		}
		records[i].Growth = len(h.GetEvents()) - base
		if records[i].Length >= maxEvents {
			t.Fatalf("generation %d reached %d events, at or over the %d limit", i, records[i].Length, maxEvents)
		}
		if i < len(histories)-1 && records[i].Growth < budgetEvents {
			t.Fatalf("generation %d switched after %d events of growth, under its budget of %d", i, records[i].Growth, budgetEvents)
		}
	}

	// One list row, in created_at order.
	var listed []*orkstackv1.Run
	deadline := time.Now().Add(30 * time.Second)
	for {
		listed, err = s.reader.ListRuns(ctx, engine.RunQuery{OrkflowName: "transparent", Complete: true})
		if err == nil && len(listed) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ListRuns = %v, %v", listed, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(listed) != 1 || listed[0].GetId() != runID {
		t.Fatalf("ListRuns returned %d rows, want exactly this run's one", len(listed))
	}

	// Every {seq}.pb snapshot agrees with the stream up to its sequence.
	checked := s.checkSequenceSnapshots(t, ctx, runID, events)

	proof.WriteJSON(t, "continuation", "generations.json", map[string]any{
		"run_id": runID, "max_events": maxEvents, "budget_events": budgetEvents, "events": len(events),
		"sequence_snapshots_checked": checked, "generations": records,
	})
	t.Logf("E2 transparent: %d generations (%d natural, %d held), %d events contiguous, archive == stream == segments, "+
		"one list row, %d {seq}.pb snapshots agree with the stream", len(chain), natural, held, len(events), checked)
}

// awaitArchivedEvents waits for the run's archive body and returns its
// events.
func awaitArchivedEvents(t *testing.T, ctx context.Context, archive *engine.RunArchiver, runID string) []*orkstackv1.Event {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		events, err := archive.LoadRunEvents(ctx, runID)
		if err == nil {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run was never archived: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// checkSequenceSnapshots decodes every {seq}.pb object in the run's snapshot
// prefix and checks each task row's status against the status the stream's
// latest event for that task at or before the sequence set. Workflow
// timestamps are ignored: a retried emit may rewrite its own key.
func (s *canStack) checkSequenceSnapshots(t *testing.T, ctx context.Context, runID string, events []*orkstackv1.Event) int {
	t.Helper()
	return checkSequenceSnapshots(t, ctx, s.snapshots, runID, events)
}

// checkSequenceSnapshots is canStack.checkSequenceSnapshots over any run
// snapshot store in the test binary's bucket.
func checkSequenceSnapshots(t *testing.T, ctx context.Context, snapshots *artifacts.S3RunSnapshotStore, runID string,
	events []*orkstackv1.Event,
) int {
	t.Helper()
	cfg := s3Config()
	raw := newRawS3ClientForCommitE2E(t, cfg)
	prefix, err := artifacts.RunSnapshotPrefix("default", runID)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	var token *string
	for {
		page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.S3Bucket), Prefix: aws.String(prefix), ContinuationToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		token = page.NextContinuationToken
	}
	checked := 0
	for _, key := range keys {
		if strings.Contains(key, "/continuations/") {
			continue
		}
		name := key[strings.LastIndex(key, "/")+1:]
		sequence, err := strconv.ParseInt(strings.TrimSuffix(name, ".pb"), 10, 64)
		if err != nil {
			t.Fatalf("snapshot key %q is neither a sequence nor a continuation", key)
		}
		data, err := snapshots.ReadSnapshot(ctx, "s3://"+cfg.S3Bucket+"/"+key)
		if err != nil {
			t.Fatal(err)
		}
		run := &orkstackv1.Run{}
		if err := proto.Unmarshal(data, run); err != nil {
			t.Fatal(err)
		}
		latest := map[string]orkstackv1.Status{}
		for _, event := range events {
			if event.GetSequence() > sequence || event.GetTaskId() == "" || event.Item != nil {
				continue
			}
			latest[event.GetTaskId()] = event.GetStatus()
		}
		for _, row := range run.GetTaskRuns() {
			want, ok := latest[row.GetTaskId()]
			if !ok {
				want = orkstackv1.Status_STATUS_PENDING
			}
			if row.GetStatus() != want {
				t.Fatalf("snapshot %d: task %s is %v, the stream's latest event at or before it says %v",
					sequence, row.GetTaskId(), row.GetStatus(), want)
			}
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no sequence snapshot was checked")
	}
	return checked
}

// TestIntegration_ContinuationReplaysEveryGeneration: every generation of a
// run that continued as new replays deterministically against current code,
// as the execution it was.
func TestIntegration_ContinuationReplaysEveryGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "replay-gens", false, canFixtureBehavior)
	budget := engine.DefaultContinuationBudget(60, 1<<20, 5*time.Minute)
	runID := fmt.Sprintf("ork-replay-gens-%d", time.Now().UnixNano())
	wf := compile(t, "name: gens\ntasks:\n"+guardChain("g", 30, ""))
	if err := s.starter.StartRun(ctx, runID, wf, nil, s.plan(1000, &budget)); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	described, err := s.client.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) < 2 {
		t.Fatalf("%d generations, want the run to switch", len(chain))
	}
	for i, execution := range chain {
		if err := replayRecorded(s.history(runID, execution), runID); err != nil {
			t.Fatalf("generation %d does not replay: %v", i, err)
		}
	}
	t.Logf("E2 replay: all %d generations of %s replayed deterministically", len(chain), runID)
}

var _ = errors.New
var _ = task.DefaultQueue

// TestIntegration_ComposedChildThatContinuesCompletesItsParent: a composed
// child whose own history continues as new — with the run-tree limits its
// start plan copied from the parent's — is one execution to its parent: the
// parent's wait on it ends when the child's chain does, with its result.
func TestIntegration_ComposedChildThatContinuesCompletesItsParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "composed-can", false, transparentBehavior)
	budget := engine.DefaultContinuationBudget(50, 1<<20, 5*time.Minute)
	runID := fmt.Sprintf("ork-composed-can-%d", time.Now().UnixNano())
	parent := compile(t, `
name: outer
version: "1.0.0"
tasks:
  - id: pre
    uses: shell
    with: {op: exec}
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [pre]
    with: {image: fixed}
  - id: post
    uses: shell
    needs: [release]
    with: {op: exec}
`)
	parent.Namespace = "default"
	child := compile(t, "name: release-block\nversion: \"2.1.0\"\ninputs:\n  image: {type: string, required: true}\ntasks:\n"+
		guardChain("k", 12, ""))
	child.Namespace = "default"
	child.DefinitionDigest = childDigest
	for _, tk := range parent.GetTasks() {
		if tk.GetOrkflow() != nil {
			tk.Orkflow.DefinitionDigest = childDigest
		}
	}
	childPlan := rawPlan(s.orkerQueue)
	childPlan.CommitRoot = "mem://commits"
	cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = runID + "-child"
	plan := s.plan(1000, &budget)
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	if err := s.starter.StartRun(ctx, runID, parent, nil, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the parent did not complete: %v", err)
	}
	described, err := s.client.DescribeWorkflowExecution(ctx, cp.RunID, "")
	if err != nil {
		t.Fatal(err)
	}
	chain := s.chainOf(cp.RunID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) < 2 {
		t.Fatalf("the child ran %d generations, want it to continue as new", len(chain))
	}
	_, _, _, carriedPlan, carry := startArgs(t, s.history(cp.RunID, chain[len(chain)-1]))
	if carry == nil || carriedPlan.Continuation == nil || *carriedPlan.Continuation != budget || carriedPlan.InFlight == nil {
		t.Fatalf("the child's carried generation decides with %+v / %+v, want the parent's copied limits", carriedPlan.Continuation, carriedPlan.InFlight)
	}
	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || taskRunOfRun(run, "release").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("parent %v, release %v", run.GetStatus(), taskRunOfRun(run, "release").GetStatus())
	}
	t.Logf("E2 composed: child %s ran %d generations; its parent %s saw one child run and succeeded", cp.RunID, len(chain), runID)
}

// TestIntegration_ScheduledRunThatContinuesCompletesItsFire: a scheduled
// run that continues as new is one execution to the fire that started it:
// the fire's wait ends when the run's chain does, and the fire succeeds.
func TestIntegration_ScheduledRunThatContinuesCompletesItsFire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var (
		mu     sync.Mutex
		runIDs []string
		orker  string
	)
	budget := engine.DefaultContinuationBudget(50, 1<<20, 5*time.Minute)
	prepare := func(_ context.Context, rec *orkstackv1.ScheduleRecord, at time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		mu.Lock()
		defer mu.Unlock()
		wf, err := dsl.Compile([]byte("name: nightly\nversion: \"1.0.0\"\ntasks:\n" + guardChain("n", 12, "")))
		if err != nil {
			return nil, err
		}
		wf.Namespace = "default"
		runID := fmt.Sprintf("ork-nightly-%d", time.Now().UnixNano())
		runIDs = append(runIDs, runID)
		plan := rawPlan(orker)
		plan.CommitRoot = "mem://commits"
		plan.History = &engine.HistoryLimit{MaxEvents: 1000, MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
		plan.Continuation = &budget
		plan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SCHEDULE", ID: rec.GetScheduleId(),
			Reference: at.UTC().Format(time.RFC3339), Description: rec.GetName()}
		encoded, err := json.Marshal(plan)
		if err != nil {
			return nil, err
		}
		return &orkstackv1.ScheduledRunPreparation{
			Run: &orkstackv1.Run{Id: runID, Namespace: "default", OrkflowName: wf.GetName(), OrkflowVersion: wf.GetVersion(),
				Status: orkstackv1.Status_STATUS_PENDING, CreatedAt: timestamppb.Now()},
			Orkflow: wf, Plan: encoded,
		}, nil
	}
	s := startCANStack(t, ctx, "sched-can", false, transparentBehavior, func(w worker.Worker) {
		w.RegisterActivityWithOptions(prepare, activity.RegisterOptions{Name: engine.ActivityPrepareScheduledRun})
	})
	mu.Lock()
	orker = s.orkerQueue
	mu.Unlock()
	scheduler := engine.NewTemporalScheduler(s.client, s.runQueue)
	name := fmt.Sprintf("nightly-%d", time.Now().UnixNano())
	rec := &orkstackv1.ScheduleRecord{
		ScheduleId: "sch-00000000" + fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff),
		Name:       name, Namespace: "default", OrkflowName: "nightly",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}, Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL},
	}
	if _, err := scheduler.Create(ctx, rec, true); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	defer func() { _ = scheduler.Delete(context.Background(), "default", name) }()
	if err := scheduler.Fire(ctx, "default", name, orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED); err != nil {
		t.Fatalf("fire: %v", err)
	}
	var runID string
	deadline := time.Now().Add(60 * time.Second)
	for runID == "" && time.Now().Before(deadline) {
		mu.Lock()
		if len(runIDs) > 0 {
			runID = runIDs[0]
		}
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	if runID == "" {
		t.Fatal("the fire never prepared a run")
	}
	waitWorkflowStarted(t, ctx, s.client, runID)
	if err := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the scheduled run did not complete: %v", err)
	}
	described, err := s.client.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) < 2 {
		t.Fatalf("the scheduled run ran %d generations, want it to continue as new", len(chain))
	}
	fires := scheduleFireIDs(t, ctx, s.client, rec.GetScheduleId(), 1)
	if err := s.client.GetWorkflow(ctx, fires[0], "").Get(ctx, nil); err != nil {
		t.Fatalf("the fire did not complete with its run: %v", err)
	}
	t.Logf("E2 scheduled: run %s ran %d generations; its fire %s completed", runID, len(chain), fires[0])
}
