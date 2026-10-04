//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The fan-out integration suite (spec each-fan-out, "Integration"): real
// Temporal, real MinIO and — for I1–I5 — the real Orker running real
// containers through Docker, on the shape of examples/each-fan-out.ork.yaml:
// list → convert (each: over list's parts) → merge. Every scenario runs in a
// private Temporal namespace on private channels (AGENTS.md §11.1).

// eachE2E is one deployment: a run worker, one or more real Orkers on one
// private channel, an event worker, and the stores they share.
type eachE2E struct {
	t          *testing.T
	ctx        context.Context
	cfg        artifacts.Config
	temporal   client.Client
	namespace  string
	runQueue   string
	orkerQueue string
	store      artifacts.Store
	commits    artifacts.CommitStore
	attempts   artifacts.AttemptStore
	logs       artifacts.LogStore
	runs       *engine.RunStore
	bus        *engine.EventBus
	snapshots  *testSnapshotStore
	alpine     string
	workRoot   string

	mu         sync.Mutex
	scheduled  map[string]int // ExecuteTask invocations per task or task#item
	runWorker  worker.Worker
	orkers     []worker.Worker
	containers map[string]int
}

func startEachE2E(t *testing.T, withDocker bool) *eachE2E {
	t.Helper()
	cfg := s3Config()
	requireMinIO(t, cfg)
	if withDocker {
		requireDockerForCommitE2E(t)
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	namespace := privateNamespaceForCommitE2E(t)
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	t.Cleanup(temporalClient.Close)
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)

	token := fmt.Sprintf("%d", time.Now().UnixNano())
	e := &eachE2E{
		t: t, ctx: ctx, cfg: cfg, temporal: temporalClient, namespace: namespace,
		runQueue:   "ork-each-run-" + token,
		orkerQueue: "ork-each-tasks-" + token,
		runs:       engine.NewRunStore(),
		bus:        engine.NewEventBus(),
		snapshots:  newTestSnapshotStore(),
		scheduled:  map[string]int{},
		containers: map[string]int{},
	}
	if e.store, err = artifacts.New(cfg); err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	if e.commits, err = artifacts.NewCommitStore(ctx, cfg); err != nil {
		t.Fatalf("open commit store: %v", err)
	}
	if e.attempts, err = artifacts.NewAttemptStore(cfg); err != nil {
		t.Fatalf("open attempt store: %v", err)
	}
	if e.logs, err = artifacts.NewLogStore(cfg); err != nil {
		t.Fatalf("open log store: %v", err)
	}
	if withDocker {
		e.alpine = pinnedAlpineForCommitE2E(t)
		e.workRoot = bindableWorkRootForCommitE2E(t)
	}
	e.startRunWorker()
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(e.bus, e.runs).WithSnapshotStore(e.snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	if err := eventWorker.Start(); err != nil {
		t.Fatalf("start event worker: %v", err)
	}
	t.Cleanup(eventWorker.Stop)
	return e
}

func (e *eachE2E) startRunWorker() {
	w := worker.New(e.temporal, e.runQueue, worker.Options{
		DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
	})
	engine.RegisterWorkflows(w)
	w.RegisterActivityWithOptions(engine.NewSnapshotActivities(e.snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	w.RegisterActivityWithOptions(engine.NewEachActivities(e.store).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	// The compose activities over the same store, as `ork` registers them, so
	// the composed-orkflows suite runs on this deployment too.
	compose := engine.NewComposeActivities(e.store)
	w.RegisterActivityWithOptions(compose.RenderComposedValues,
		activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
	w.RegisterActivityWithOptions(compose.StoreComposedValues,
		activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})
	if err := w.Start(); err != nil {
		e.t.Fatalf("start run worker: %v", err)
	}
	e.mu.Lock()
	e.runWorker = w
	e.mu.Unlock()
	e.t.Cleanup(w.Stop)
}

// startOrker starts one real Orker on the private channel; the tap counts
// every ExecuteTask invocation by task and item before delegating.
func (e *eachE2E) startOrker(id string) (worker.Worker, *orker.Activities) {
	refs, _ := e.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(e.store, orker.Options{
		Logs: e.logs, Commits: e.commits, Attempts: e.attempts, Refs: refs,
		Pool: task.DefaultPool, OrkerID: id, WorkRoot: e.workRoot,
	})
	if err := activities.Boot(e.ctx); err != nil {
		infraAbsent(e.t, "orker boot checks failed: %v", err)
	}
	w := worker.New(e.temporal, e.orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		e.mu.Lock()
		e.scheduled[scheduleKey(in)]++
		e.mu.Unlock()
		return activities.ExecuteTask(actx, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	if err := w.Start(); err != nil {
		e.t.Fatalf("start orker %s: %v", id, err)
	}
	e.mu.Lock()
	e.orkers = append(e.orkers, w)
	e.mu.Unlock()
	e.t.Cleanup(w.Stop)
	return w, activities
}

// eachDefinition is the example's shape over the pinned alpine image:
//   - list writes {"parts":[{"name":"p0"},…]} with n parts;
//   - convert fans out over them, writes part.txt with its rendered name and
//     an empty values.json, and runs itemScript first (empty for none);
//   - merge counts the staged item directories and reports it as "parts".
func (e *eachE2E) eachDefinition(name string, n, maxParallel int, itemScript string) *orkstackv1.Orkflow {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf(`{"name":"p%d"}`, i)
	}
	list := fmt.Sprintf(`printf '%%s' '{"parts":[%s]}' > /orkstack/output/values.json`, strings.Join(parts, ","))
	convert := itemScript + `cat /orkstack/input/config.json > /orkstack/output/artifacts/part.txt && ` +
		`printf '{}' > /orkstack/output/values.json`
	merge := `n=$(ls /orkstack/input/upstream/convert | wc -l) && ` +
		`printf '{"parts":%d}' "$n" > /orkstack/output/values.json`
	image := &orkstackv1.ImageRef{Reference: e.alpine, Pinned: e.alpine}
	with, _ := structpbStruct(map[string]any{"name": "{{ .each.part.name }}", "index": "{{ .each.index }}"})
	return &orkstackv1.Orkflow{
		Name:    name,
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "list", Uses: e.alpine, Image: image, Command: []string{"/bin/sh", "-c", list},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
			{Id: "convert", Uses: e.alpine, Image: image, Needs: []string{"list"},
				Command: []string{"/bin/sh", "-c", convert}, With: with,
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
				Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 1, BackoffCoefficient: 1},
				Each: &orkstackv1.EachSpec{Over: "{{ .tasks.list.output.parts }}", As: "part",
					MaxItems: 50, MaxParallel: int32(maxParallel),
					Source: &orkstackv1.EachSource{TaskId: "list", Path: []string{"parts"}}}},
			{Id: "merge", Uses: e.alpine, Image: image, Needs: []string{"convert"},
				Command:        []string{"/bin/sh", "-c", merge},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
		},
	}
}

func (e *eachE2E) plan() engine.RunPlan {
	plan := rawPlan(e.orkerQueue)
	root, err := artifacts.CommitRoot(e.cfg)
	if err != nil {
		e.t.Fatalf("commit root: %v", err)
	}
	plan.CommitRoot = root
	plan.Limits = engine.DefaultLimits()
	plan.TransferBudgetSeconds = 300
	return plan
}

func (e *eachE2E) start(runID string, wf *orkstackv1.Orkflow) {
	e.t.Helper()
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	if err := starter.StartRun(e.ctx, runID, wf, nil, e.plan()); err != nil {
		e.t.Fatalf("StartRun: %v", err)
	}
}

func (e *eachE2E) wait(runID string) *orkstackv1.Run {
	e.t.Helper()
	var run *orkstackv1.Run
	err := e.temporal.GetWorkflow(e.ctx, runID, "").Get(e.ctx, &run)
	if stored, ok := e.runs.Get(runID); ok {
		run = stored
	}
	if err != nil && run == nil {
		e.t.Fatalf("run %s: %v", runID, err)
	}
	return run
}

// awaitItemRunning blocks until item index of convert is RUNNING in the read
// model, by deadline, never by sleeping a fixed time.
func (e *eachE2E) awaitItemStatus(runID string, index int, want orkstackv1.Status, within time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if run, ok := e.runs.Get(runID); ok {
			node := taskRunNamed(run, "convert")
			if len(node.GetItems()) > index && node.GetItems()[index].GetStatus() == want {
				return
			}
		}
		select {
		case <-e.ctx.Done():
			e.t.Fatalf("context ended waiting for item %d", index)
		case <-time.After(100 * time.Millisecond):
		}
	}
	e.t.Fatalf("item %d of run %s never reached %v within %s", index, runID, want, within)
}

func (e *eachE2E) history(runID string) *history.History {
	e.t.Helper()
	out := &history.History{}
	iter := e.temporal.GetWorkflowHistory(e.ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			e.t.Fatalf("read history: %v", err)
		}
		out.Events = append(out.Events, event)
	}
	return out
}

func (e *eachE2E) commitKey(runID string, index int32) string {
	key, err := artifacts.ItemCommitKey("", runID, "convert", index)
	if err != nil {
		e.t.Fatal(err)
	}
	return key
}

func (e *eachE2E) objectExists(key string) bool {
	raw := newRawS3ClientForCommitE2E(e.t, e.cfg)
	_, err := raw.HeadObject(e.ctx, &s3.HeadObjectInput{Bucket: aws.String(e.cfg.S3Bucket), Key: aws.String(key)})
	return err == nil
}

// I1.
func TestIntegration_EachCommitsEveryItemUnderItsOwnKey(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("each-i1")
	runID := fmt.Sprintf("ork-each-i1-%d", time.Now().UnixNano())
	e.start(runID, e.eachDefinition("each-i1", 4, 2, ""))
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	var lines []string
	for i := int32(0); i < 4; i++ {
		key := e.commitKey(runID, i)
		if !e.objectExists(key) {
			t.Fatalf("no commit.json at %s", key)
		}
		records, err := e.attempts.ListAttempts(e.ctx, runID, "convert", artifacts.ItemAt(i))
		if err != nil || len(records) != 1 {
			t.Fatalf("item %d attempt records = %v (%v)", i, records, err)
		}
		if latest, err := e.logs.LatestLogAttempt(e.ctx, runID, "convert", artifacts.ItemAt(i)); err != nil || latest != 1 {
			t.Logf("item %d log prefix: latest attempt %d (%v) — the item printed nothing", i, latest, err)
		}
		lines = append(lines, fmt.Sprintf("item=%d commit=%s attempts=%d", i, key, len(records)))
	}
	merge := taskRunNamed(run, "merge")
	values := readValues(t, e, merge.GetValuesRef())
	if !strings.Contains(values, `"parts":4`) {
		t.Fatalf("merge read %s, want 4 parts", values)
	}
	proof.Write(t, "engine", "each-I1.txt", []byte(strings.Join(append(lines, "merge="+values), "\n")+"\n"))
}

// I2.
func TestIntegration_EachItemRetriesAloneAndSiblingsRunOnce(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("each-i2")
	runID := fmt.Sprintf("ork-each-i2-%d", time.Now().UnixNano())
	// Item 2 exits 1 on its first attempt, read from task.json.
	fail := `if grep -q '"index": 2,' /orkstack/input/task.json && grep -q '"attempt": 1,' /orkstack/input/task.json; then exit 1; fi; `
	e.start(runID, e.eachDefinition("each-i2", 4, 0, fail))
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	for i := int32(0); i < 4; i++ {
		records, err := e.attempts.ListAttempts(e.ctx, runID, "convert", artifacts.ItemAt(i))
		want := 1
		if i == 2 {
			want = 2
		}
		if err != nil || len(records) != want {
			t.Fatalf("item %d has %d attempt records (%v), want %d", i, len(records), err, want)
		}
	}
	proof.Write(t, "engine", "each-I2.txt", []byte(fmt.Sprintf("run=%s item 2 retried alone; siblings ran once\n", runID)))
}

// I3: kill the Orker while item 1 runs; item 1 retries on another Orker and
// the other items' commits and records are untouched.
func TestIntegration_EachOrkerKilledMidItemRetriesOnlyThatItem(t *testing.T) {
	e := startEachE2E(t, true)
	first, _ := e.startOrker("each-i3-a")
	runID := fmt.Sprintf("ork-each-i3-%d", time.Now().UnixNano())
	// Item 1 holds its container open long enough to be killed under.
	hold := `if grep -q '"index": 1,' /orkstack/input/task.json && grep -q '"attempt": 1,' /orkstack/input/task.json; then sleep 30; fi; `
	e.start(runID, e.eachDefinition("each-i3", 3, 0, hold))
	// Items 0 and 2 settle while item 1 holds its container: both must be
	// committed before the kill, or the kill would legitimately cancel and
	// retry them too. Item 1 reads RUNNING in the first fresh snapshot after
	// a sibling settles (the expansion's snapshots show items PENDING, §5.6).
	e.awaitItemStatus(runID, 0, orkstackv1.Status_STATUS_SUCCEEDED, 3*time.Minute)
	e.awaitItemStatus(runID, 2, orkstackv1.Status_STATUS_SUCCEEDED, 3*time.Minute)
	e.awaitItemStatus(runID, 1, orkstackv1.Status_STATUS_RUNNING, time.Minute)
	first.Stop()
	e.startOrker("each-i3-b")
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	items := taskRunNamed(run, "convert").GetItems()
	if items[1].GetOrkerId() != "each-i3-b" {
		t.Fatalf("item 1 finished on %q, want the second Orker", items[1].GetOrkerId())
	}
	for _, i := range []int32{0, 2} {
		records, _ := e.attempts.ListAttempts(e.ctx, runID, "convert", artifacts.ItemAt(i))
		if len(records) != 1 {
			t.Fatalf("item %d has %d records; a killed sibling re-ran it", i, len(records))
		}
	}
	proof.Write(t, "engine", "each-I3.txt", []byte(fmt.Sprintf("run=%s item1 orker=%s\n", runID, items[1].GetOrkerId())))
}

// I4: kill after item 0's commit.json, before completion: the retry adopts.
func TestIntegration_EachItemCommittedThenOrkerDiesIsAdopted(t *testing.T) {
	e := startEachE2E(t, true)
	runID := fmt.Sprintf("ork-each-i4-%d", time.Now().UnixNano())
	// The first Orker writes item 0's commit and then dies before answering
	// Temporal: its activity returns a heartbeat-less hang the test ends by
	// stopping the worker.
	refs, _ := e.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(e.store, orker.Options{
		Logs: e.logs, Commits: e.commits, Attempts: e.attempts, Refs: refs,
		Pool: task.DefaultPool, OrkerID: "each-i4-a", WorkRoot: e.workRoot,
	})
	if err := activities.Boot(e.ctx); err != nil {
		infraAbsent(t, "orker boot: %v", err)
	}
	committed := make(chan struct{})
	var once sync.Once
	w := worker.New(e.temporal, e.orkerQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result, err := activities.ExecuteTask(actx, in)
		if err == nil && in.GetEach().GetIndex() == 0 && in.GetEach() != nil {
			once.Do(func() { close(committed) })
			<-actx.Done()
			return nil, actx.Err()
		}
		return result, err
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	e.start(runID, e.eachDefinition("each-i4", 2, 1, ""))
	select {
	case <-committed:
	case <-time.After(3 * time.Minute):
		t.Fatal("item 0 never committed")
	}
	w.Stop()
	e.startOrker("each-i4-b")
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	records, _ := e.attempts.ListAttempts(e.ctx, runID, "convert", artifacts.ItemAt(0))
	adopted := false
	for _, r := range records {
		adopted = adopted || r.GetCommitAdopted()
	}
	if !adopted {
		t.Fatalf("item 0's retry did not adopt its commit: %v", records)
	}
	proof.Write(t, "engine", "each-I4.txt", []byte(fmt.Sprintf("run=%s item0 commit_adopted=true records=%d\n", runID, len(records))))
}

// I5: stop the run worker with items in flight, start a fresh one: the run
// completes and every item has exactly one ExecuteTask scheduling in history.
func TestIntegration_EachEngineRestartMidFanOutDispatchesNothingTwice(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("each-i5")
	runID := fmt.Sprintf("ork-each-i5-%d", time.Now().UnixNano())
	hold := `sleep 2; `
	e.start(runID, e.eachDefinition("each-i5", 6, 2, hold))
	// The read model shows an item RUNNING only in a fresh snapshot taken
	// after a sibling settles (§5.6), so "mid-fan-out" is observed as item 0
	// settled: with at most 2 at a time, items are still in flight and items
	// 2–5 are not all dispatched yet.
	e.awaitItemStatus(runID, 0, orkstackv1.Status_STATUS_SUCCEEDED, 3*time.Minute)
	e.mu.Lock()
	old := e.runWorker
	e.mu.Unlock()
	old.Stop()
	e.startRunWorker()
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	counts := map[string]int{}
	for _, key := range recordedSchedule(t, e.history(runID)) {
		counts[key]++
	}
	for i := 0; i < 6; i++ {
		if n := counts[fmt.Sprintf("convert#%d", i)]; n != 1 {
			t.Fatalf("item %d was scheduled %d times in history", i, n)
		}
	}
	proof.Write(t, "engine", "each-I5.txt", []byte(fmt.Sprintf("run=%s schedulings=%v\n", runID, counts)))
}

// I6: a 1000-item fan-out over an input of 1000 small integers, against a
// stand-in ExecuteTask that returns realistic results, stays inside the
// stated history budget, and no workflow task schedules more than 2 MiB.
func TestIntegration_EachHistoryStaysInsideTheBudgetAtTheCeiling(t *testing.T) {
	e := startEachE2E(t, false)
	w := worker.New(e.temporal, e.orkerQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	inline := strings.Repeat("v", 4000)
	w.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "stand-in",
			ValuesJson: []byte(`{"v":"` + inline + `"}`), ValuesInlined: true,
			ValuesRef: &orkstackv1.ArtifactRef{Name: "values.json", Uri: "s3://bucket/orkflows/each-i6/" + in.GetRunId() + "/convert/values.json/" + strings.Repeat("a", 64), SizeBytes: 4100},
			Commit:    &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: strings.Repeat("b", 64)}}
		for i := 0; i < 16; i++ {
			result.Artifacts = append(result.Artifacts, &orkstackv1.ArtifactRef{
				Name: fmt.Sprintf("artifact-%02d.bin", i), SizeBytes: 1024, ContentType: "application/octet-stream",
				Sha256: strings.Repeat("c", 64),
				Uri:    fmt.Sprintf("s3://bucket/orkflows/each-i6/%s/convert/artifact-%02d.bin/%s", in.GetRunId(), i, strings.Repeat("c", 64))})
		}
		return result, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	numbers := make([]any, 1000)
	for i := range numbers {
		numbers[i] = float64(i)
	}
	inputs, _ := structpbStruct(map[string]any{"n": map[string]any{"type": "array"}})
	wf := &orkstackv1.Orkflow{Name: "each-i6", Version: "1.0.0", Inputs: inputs, Tasks: []*orkstackv1.Task{{
		Id: "convert", Uses: "stand-in", TimeoutSeconds: 60,
		Each: &orkstackv1.EachSpec{Over: "{{ .inputs.n }}", As: "item", MaxItems: 1000,
			Source: &orkstackv1.EachSource{Input: "n"}},
	}}}
	data, _ := structpbStruct(map[string]any{"n": numbers})
	runID := fmt.Sprintf("ork-each-i6-%d", time.Now().UnixNano())
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	if err := starter.StartRun(e.ctx, runID, wf, &orkstackv1.Payload{Data: data}, e.plan()); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	run := e.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	h := e.history(runID)
	var totalBytes int
	perTask := map[int64]int{}
	for _, event := range h.GetEvents() {
		totalBytes += proto.Size(event)
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			perTask[attrs.GetWorkflowTaskCompletedEventId()] += proto.Size(attrs.GetInput())
		}
	}
	maxPerTask := 0
	for _, n := range perTask {
		if n > maxPerTask {
			maxPerTask = n
		}
	}
	measurement := fmt.Sprintf("run=%s events=%d history_bytes=%d max_item_input_bytes_per_workflow_task=%d workflow_tasks_scheduling_items=%d\n",
		runID, len(h.GetEvents()), totalBytes, maxPerTask, len(perTask))
	proof.Write(t, "engine", "each-I6-history-budget.txt", []byte(measurement))
	t.Log(measurement)
	if len(h.GetEvents()) >= 20000 {
		t.Fatalf("history has %d events, want fewer than 20000", len(h.GetEvents()))
	}
	if totalBytes >= 32<<20 {
		t.Fatalf("history is %d bytes, want under 32 MiB", totalBytes)
	}
	if maxPerTask > 2<<20 {
		t.Fatalf("one workflow task scheduled %d bytes of item inputs, over 2 MiB", maxPerTask)
	}
}

// I7: a stand-in DescribeOrker answering each_supported: false on the pool's
// channel refuses the submit.
func TestIntegration_EachSubmitRefusesAPoolWhoseOrkerPredatesFanOut(t *testing.T) {
	e := startEachE2E(t, false)
	pool := fmt.Sprintf("old-%d", time.Now().UnixNano()%1_000_000)
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatal(err)
	}
	w := worker.New(e.temporal, queue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	w.RegisterActivityWithOptions(func(context.Context) (*orkstackv1.DescribeOrkerResult, error) {
		return &orkstackv1.DescribeOrkerResult{OrkerId: "old-orker", BuildId: "2026.09.01", Pool: pool}, nil
	}, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	w.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return nil, temporal.NewNonRetryableApplicationError("never", "ActivityFailed", nil)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	resolver := &engine.PoolResolver{Probe: engine.NewTemporalPoolProbe(e.temporal), Orkers: starter}
	inputs, _ := structpbStruct(map[string]any{"n": map[string]any{"type": "array"}})
	wf := &orkstackv1.Orkflow{Name: "each-i7", Version: "1.0.0", Inputs: inputs, Tasks: []*orkstackv1.Task{{
		Id: "convert", Uses: "x", Pool: pool,
		Each: &orkstackv1.EachSpec{Over: "{{ .inputs.n }}", As: "item", MaxItems: 5,
			Source: &orkstackv1.EachSource{Input: "n"}},
	}}}
	deadline := time.Now().Add(time.Minute)
	for {
		_, _, err = resolver.Plan(e.ctx, &orkstackv1.Namespace{Name: "default"}, wf, false)
		var refused *engine.PoolNoEachSupportError
		if asNoEach(err, &refused) {
			proof.Write(t, "engine", "each-I7.txt", []byte(err.Error()+"\n"))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Plan = %v, want PoolNoEachSupportError", err)
		}
		// The pool's poller may not be visible yet: ask again by deadline.
		select {
		case <-e.ctx.Done():
			t.Fatalf("context ended: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// I8: opt-in recorder of testdata/history/run-each-v1.json.
func TestIntegration_RecordEachRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + eachHistory +
			" — the committed fixture is meant to age, not to be refreshed")
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-each-rec-run-" + suffix
	orkerQueue := "ork-each-rec-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-each-rec-" + suffix, BuildID: "v1"}

	values := &memValues{objects: map[string][]byte{}}
	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	runWorker.RegisterActivityWithOptions(engine.NewEachActivities(values).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	var mu sync.Mutex
	tries := map[int32]int{}
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	}))
	orkerWorker.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch {
		case in.GetTaskId() == "list":
			data := []byte(partsJSON(6))
			return &orkstackv1.ExecuteTaskResult{TaskId: "list", Attempt: 1, OrkerId: "fixture",
				ValuesJson: data, ValuesInlined: true, ValuesRef: values.put("mem://values/list", data)}, nil
		case in.GetEach() != nil:
			mu.Lock()
			tries[in.GetEach().GetIndex()]++
			n := tries[in.GetEach().GetIndex()]
			mu.Unlock()
			if in.GetEach().GetIndex() == 3 && n == 1 {
				return nil, temporal.NewApplicationError("transient", task.CategoryInfrastructureError)
			}
			r := itemResult(in)
			r.Attempt = int32(n)
			return r, nil
		default:
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
				ValuesJson: valuesJSON(map[string]any{"staged_from": len(in.GetUpstream())})}, nil
		}
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	wf := compile(t, fmt.Sprintf(eachTaskDocument, ", max_parallel: 2"))
	wf.Tasks[1].Retry = &orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 1, BackoffCoefficient: 1}
	plan := rawPlan(orkerQueue)
	plan.CommitRoot = "mem://commits"
	runID := fmt.Sprintf("ork-each-fixture-%d", time.Now().UnixNano())
	if err := starter.StartRun(ctx, runID, wf, nil, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("fixture run %s did not succeed: %v", runID, err)
	}
	recorded := &history.History{}
	iter := temporalClient.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	if err != nil {
		t.Fatalf("encode history: %v", err)
	}
	if err := os.WriteFile(eachHistory, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", eachHistory, err)
	}
	t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
		len(recorded.Events), runID, eachHistory, deployment)
}

func readValues(t *testing.T, e *eachE2E, ref *orkstackv1.ArtifactRef) string {
	t.Helper()
	reader, err := e.store.Get(e.ctx, ref.GetUri())
	if err != nil {
		t.Fatalf("read %s: %v", ref.GetUri(), err)
	}
	defer func() { _ = reader.Close() }()
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return b.String()
}

func asNoEach(err error, target **engine.PoolNoEachSupportError) bool {
	for err != nil {
		if refused, ok := err.(*engine.PoolNoEachSupportError); ok {
			*target = refused
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

func structpbStruct(values map[string]any) (*structpb.Struct, error) {
	return structpb.NewStruct(values)
}
