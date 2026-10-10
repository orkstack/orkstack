package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// eachReader answers for one fan-out run, and streams its recorded events.
type eachReader struct {
	mu     sync.Mutex
	runs   map[string]*orkstackv1.Run
	events map[string][]*orkstackv1.Event
}

func (r *eachReader) GetRun(_ context.Context, runID string) (*orkstackv1.Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if run, ok := r.runs[runID]; ok {
		return run, nil
	}
	return nil, engine.ErrRunNotFound
}

func (r *eachReader) ListRuns(context.Context, engine.RunQuery) ([]*orkstackv1.Run, error) {
	return nil, nil
}

func (r *eachReader) StreamEvents(_ context.Context, runID string, from int64, send func(*orkstackv1.Event) error) error {
	r.mu.Lock()
	events := append([]*orkstackv1.Event(nil), r.events[runID]...)
	r.mu.Unlock()
	if events == nil {
		return engine.ErrRunNotFound
	}
	for _, e := range events {
		if e.GetSequence() <= from {
			continue
		}
		if err := send(e); err != nil {
			return err
		}
	}
	return nil
}

func (r *eachReader) Executions(context.Context, engine.StatsWindow) (engine.Executions, error) {
	return engine.Executions{}, nil
}

func (r *eachReader) RunningCount(context.Context, string) (int64, error) { return 0, nil }

// RunStart answers what every run's start event says: its definition, naming
// its orkflow and namespace.
func (r *eachReader) RunStart(ctx context.Context, runID string) (*engine.RunStartRecord, error) {
	run, err := r.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	return startOf(run), nil
}

// startOf is the start record a run's first history event holds.
func startOf(run *orkstackv1.Run) *engine.RunStartRecord {
	return &engine.RunStartRecord{Orkflow: &orkstackv1.Orkflow{
		Name: run.GetOrkflowName(), Version: run.GetOrkflowVersion(), Namespace: run.GetNamespace(),
	}}
}

const eachCLIRunID = "ork-each-cli"

// eachCLIRun is the narrative, small: list → convert (4 items, one retried) → merge.
func eachCLIRun() *orkstackv1.Run {
	at := fixedNow.Add(-time.Hour)
	ts := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(at.Add(d)) }
	node := &orkstackv1.TaskRun{
		TaskId: "convert", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "gpu",
		Each:        &orkstackv1.TaskEach{Expanded: true, Count: 4},
		ScheduledAt: ts(5 * time.Second), StartedAt: ts(7 * time.Second), FinishedAt: ts(41 * time.Second),
	}
	for i := int32(0); i < 4; i++ {
		attempt := int32(1)
		if i == 2 {
			attempt = 2
		}
		node.Items = append(node.Items, &orkstackv1.TaskRun{
			TaskId: "convert", Item: proto.Int32(i), Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Pool: "gpu", OrkerId: fmt.Sprintf("orker-%d", i%2+1), Attempt: attempt,
			ScheduledAt: ts(5 * time.Second), StartedAt: ts(time.Duration(7+i) * time.Second),
			FinishedAt: ts(time.Duration(20+i*5) * time.Second),
			Artifacts: []*orkstackv1.ArtifactRef{{Name: "part.txt", SizeBytes: 7,
				Sha256: strings.Repeat(fmt.Sprintf("%x", i+10), 64)[:64],
				Uri:    fmt.Sprintf("file:///objects/n-cGFydC50eHQ/%d", i)}},
		})
	}
	return &orkstackv1.Run{
		Id: eachCLIRunID, Namespace: "data", OrkflowName: "nightly-ingest", OrkflowVersion: "1.2.0",
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: ts(50 * time.Second),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "list", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-1", Attempt: 1,
				ScheduledAt: ts(time.Second), StartedAt: ts(2 * time.Second), FinishedAt: ts(4 * time.Second)},
			node,
			{TaskId: "merge", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-2", Attempt: 1,
				ScheduledAt: ts(42 * time.Second), StartedAt: ts(43 * time.Second), FinishedAt: ts(49 * time.Second)},
		},
	}
}

func eachCLIEvents() []*orkstackv1.Event {
	at := fixedNow.Add(-time.Hour)
	var events []*orkstackv1.Event
	add := func(eventType orkstackv1.EventType, task string, item *int32, status orkstackv1.Status, message string) {
		events = append(events, &orkstackv1.Event{
			RunId: eachCLIRunID, Sequence: int64(len(events) + 1), Type: eventType, TaskId: task, Item: item,
			Status: status, Message: message, Timestamp: timestamppb.New(at.Add(time.Duration(len(events)) * time.Second)),
		})
	}
	add(orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, "convert", nil, orkstackv1.Status_STATUS_RUNNING,
		"fan-out of 4 items scheduled on pool gpu, at most 2 at a time")
	add(orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, "convert", nil, orkstackv1.Status_STATUS_RUNNING, "dispatching 4 items to orkers")
	for i := int32(0); i < 4; i++ {
		add(orkstackv1.EventType_EVENT_TYPE_TASK_LOG, "convert", proto.Int32(i), orkstackv1.Status_STATUS_SUCCEEDED,
			fmt.Sprintf("item %d succeeded", i))
	}
	add(orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, "convert", nil, orkstackv1.Status_STATUS_SUCCEEDED, "all 4 items succeeded")
	add(orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, "", nil, orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded")
	return events
}

type eachCLIFixture struct {
	*fixture
	reader   *eachReader
	attempts *artifacts.MemoryAttemptStore
	logs     *artifacts.LocalLogStore
}

func newEachCLIFixture(t *testing.T) *eachCLIFixture {
	t.Helper()
	reader := &eachReader{
		runs:   map[string]*orkstackv1.Run{eachCLIRunID: eachCLIRun()},
		events: map[string][]*orkstackv1.Event{eachCLIRunID: eachCLIEvents()},
	}
	attempts := artifacts.NewMemoryAttemptStore()
	logs, err := artifacts.NewLocalLogStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = reader
		cfg.Attempts = attempts
		cfg.Logs = logs
	})
	f.run("namespace", "create", "data").requireExit(t, exitOK)
	ctx := context.Background()
	for i := int32(0); i < 4; i++ {
		last := int32(1)
		if i == 2 {
			last = 2
		}
		for attempt := int32(1); attempt <= last; attempt++ {
			outcome := orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
			record := &orkstackv1.TaskAttempt{
				SchemaVersion: artifacts.AttemptSchemaVersion, RunId: eachCLIRunID, TaskId: "convert",
				Item: proto.Int32(i), Attempt: attempt, Outcome: outcome, OrkerId: fmt.Sprintf("orker-%d", i%2+1),
				Phase: "commit", StartedAt: timestamppb.New(fixedNow.Add(-time.Hour)),
				FinishedAt: timestamppb.New(fixedNow.Add(-time.Hour + 4*time.Second)),
			}
			if attempt < last {
				record.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
				record.Phase, record.ExitCode = "run", 1
				record.Error = &orkstackv1.Error{Code: "CONTAINER_EXIT_NONZERO", Message: `task "convert": the container exited 1`,
					Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, Retryable: true}
			}
			if err := attempts.PutAttempt(ctx, record); err != nil {
				t.Fatal(err)
			}
			if err := logs.PutLogChunk(ctx, artifacts.LogChunk{
				LogRef: artifacts.LogRef{RunID: eachCLIRunID, TaskID: "convert", Item: artifacts.ItemAt(i), Attempt: attempt},
				Seq:    1, Data: []byte(fmt.Sprintf("converting part %d (attempt %d)\n", i, attempt)),
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, task := range []string{"list", "merge"} {
		if err := logs.PutLogChunk(ctx, artifacts.LogChunk{
			LogRef: artifacts.LogRef{RunID: eachCLIRunID, TaskID: task, Attempt: 1}, Seq: 1,
			Data: []byte(task + " ran\n"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return &eachCLIFixture{fixture: f, reader: reader, attempts: attempts, logs: logs}
}

func TestRunGetEachGoldens(t *testing.T) {
	f := newEachCLIFixture(t)
	got := f.run("-n", "data", "run", "get", eachCLIRunID).requireExit(t, exitOK)
	requireGolden(t, "run-get-each.golden", got.stdout)
	expanded := f.run("-n", "data", "run", "get", eachCLIRunID, "--expand", "convert").requireExit(t, exitOK)
	requireGolden(t, "run-get-each-expand.golden", expanded.stdout)
	if !strings.Contains(got.stdout, "convert  SUCCEEDED 4/4") && !strings.Contains(got.stdout, "SUCCEEDED 4/4") {
		t.Fatalf("the fan-out row has no fraction:\n%s", got.stdout)
	}
}

func TestRunGet_ExpandRefusesATaskThatIsNotAFanOut(t *testing.T) {
	f := newEachCLIFixture(t)
	got := f.run("-n", "data", "run", "get", eachCLIRunID, "--expand", "list")
	if got.code == exitOK || !strings.Contains(got.stderr, `task "list" is not a fan-out`) {
		t.Fatalf("--expand list = %d %q", got.code, got.stderr)
	}
}

func TestRunGet_ExpandWithJSONIsRefused(t *testing.T) {
	f := newEachCLIFixture(t)
	got := f.run("--json", "-n", "data", "run", "get", eachCLIRunID, "--expand", "convert")
	if got.code == exitOK || !strings.Contains(got.stderr, "--expand only shapes the table; the JSON already carries every item under taskRuns[].items") {
		t.Fatalf("--expand --json = %d %q", got.code, got.stderr)
	}
	machine := f.run("--json", "-n", "data", "run", "get", eachCLIRunID).requireExit(t, exitOK)
	if !strings.Contains(machine.stdout, `"items"`) || !strings.Contains(machine.stdout, `"item": 0`) {
		t.Fatalf("the JSON does not carry items with their item:\n%s", machine.stdout)
	}
}

func TestItemFlag_RequiresTaskAndRefusesNegative(t *testing.T) {
	f := newEachCLIFixture(t)
	before := f.requests()
	for _, args := range [][]string{
		{"run", "logs", eachCLIRunID, "--item", "1"},
		{"run", "artifacts", eachCLIRunID, "--item", "1"},
		{"run", "attempts", eachCLIRunID, "--item", "1"},
		{"run", "timing", eachCLIRunID, "--item", "1"},
		{"run", "inspect", eachCLIRunID, "--item", "1"},
	} {
		got := f.run(append([]string{"-n", "data"}, args...)...)
		if got.code != exitError || !strings.Contains(got.stderr, "item requires task_id") {
			t.Fatalf("%v = %d %q", args, got.code, got.stderr)
		}
	}
	got := f.run("-n", "data", "run", "attempts", eachCLIRunID, "--task", "convert", "--item", "-2")
	if got.code != exitError || !strings.Contains(got.stderr, "item -2 is negative; items are numbered from 0") {
		t.Fatalf("--item -2 = %d %q", got.code, got.stderr)
	}
	if after := f.requests(); len(after) != len(before) {
		t.Fatalf("a refused --item sent %d requests", len(after)-len(before))
	}
}

func TestRunLogsEachHintGolden(t *testing.T) {
	f := newEachCLIFixture(t)
	got := f.run("-n", "data", "run", "logs", eachCLIRunID).requireExit(t, exitOK)
	requireGolden(t, "run-logs-each-hint.golden", got.stdout)
	one := f.run("-n", "data", "run", "logs", eachCLIRunID, "--task", "convert", "--item", "2").requireExit(t, exitOK)
	if one.stdout != "converting part 2 (attempt 2)\n" {
		t.Fatalf("--item 2 printed %q", one.stdout)
	}
	refused := f.run("-n", "data", "run", "logs", eachCLIRunID, "--task", "convert")
	if refused.code == exitOK || !strings.Contains(refused.stderr, "fan-out of 4 items") {
		t.Fatalf("--task convert without --item = %d %q", refused.code, refused.stderr)
	}
}

// A fan-out that has not expanded has no items, so a whole-run read names no
// item command for it: not while it is PENDING, and not once it was SKIPPED
// or FAILED before its array was counted. An expanded zero-item fan-out says
// that nothing ran.
func TestRunLogs_NoItemHintBeforeExpansion(t *testing.T) {
	for name, tc := range map[string]struct {
		status orkstackv1.Status
		each   *orkstackv1.TaskEach
		want   string
	}{
		"Pending":               {orkstackv1.Status_STATUS_PENDING, &orkstackv1.TaskEach{}, ""},
		"Skipped":               {orkstackv1.Status_STATUS_SKIPPED, &orkstackv1.TaskEach{}, ""},
		"FailedBeforeExpansion": {orkstackv1.Status_STATUS_FAILED, &orkstackv1.TaskEach{}, ""},
		"ZeroItems":             {orkstackv1.Status_STATUS_SUCCEEDED, &orkstackv1.TaskEach{Expanded: true}, "convert: fan-out over 0 items; nothing ran\n"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newEachCLIFixture(t)
			run := eachCLIRun()
			node := run.GetTaskRuns()[1]
			node.Status, node.Each, node.Items = tc.status, tc.each, nil
			f.reader.mu.Lock()
			f.reader.runs[eachCLIRunID] = run
			f.reader.mu.Unlock()
			got := f.run("-n", "data", "run", "logs", eachCLIRunID).requireExit(t, exitOK)
			if want := tc.want + "[list] list ran\n[merge] merge ran\n"; got.stdout != want {
				t.Fatalf("run logs printed %q, want %q", got.stdout, want)
			}
		})
	}
}

func TestRunArtifactsAndAttemptsEachGoldens(t *testing.T) {
	f := newEachCLIFixture(t)
	artifactsOut := f.run("-n", "data", "run", "artifacts", eachCLIRunID).requireExit(t, exitOK)
	requireGolden(t, "run-artifacts-each.golden", artifactsOut.stdout)
	attemptsOut := f.run("-n", "data", "run", "attempts", eachCLIRunID, "--task", "convert").requireExit(t, exitOK)
	requireGolden(t, "run-attempts-each.golden", attemptsOut.stdout)
	one := f.run("-n", "data", "run", "attempts", eachCLIRunID, "--task", "convert", "--item", "2").requireExit(t, exitOK)
	if strings.Count(one.stdout, "\nconvert ") != 2 && !strings.Contains(one.stdout, "convert[2] attempt 1") {
		t.Fatalf("--item 2:\n%s", one.stdout)
	}
}

func TestRunWatchEachGolden(t *testing.T) {
	f := newEachCLIFixture(t)
	got := f.run("-n", "data", "run", "watch", eachCLIRunID)
	requireGolden(t, "watch-each.golden", got.stdout)
	if !strings.Contains(got.stdout, "convert[3]") {
		t.Fatalf("item events are not spelled convert[3]:\n%s", got.stdout)
	}
}

func TestRunTiming_FanOutRowsAndItems(t *testing.T) {
	f := newEachCLIFixture(t)
	all := f.run("-n", "data", "run", "timing", eachCLIRunID).requireExit(t, exitOK)
	if !strings.Contains(all.stdout, "fan-out") || strings.Contains(all.stdout, "convert[0]") {
		t.Fatalf("whole-run timing:\n%s", all.stdout)
	}
	task := f.run("-n", "data", "run", "timing", eachCLIRunID, "--task", "convert").requireExit(t, exitOK)
	for _, want := range []string{"convert[0]", "convert[3]"} {
		if !strings.Contains(task.stdout, want) {
			t.Fatalf("--task convert lacks %s:\n%s", want, task.stdout)
		}
	}
	one := f.run("-n", "data", "run", "timing", eachCLIRunID, "--task", "convert", "--item", "2").requireExit(t, exitOK)
	if !strings.Contains(one.stdout, "convert[2]") || strings.Contains(one.stdout, "convert[1]") {
		t.Fatalf("--item 2:\n%s", one.stdout)
	}
}

const eachCLIYAML = `name: each-cli
version: "1.0.0"
inputs:
  regions: {type: array}
tasks:
  - id: list
    uses: shell
    with: {op: exec, command: ["echo", "list"]}
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", as: part, max_items: 500, max_parallel: 16}
    with: {op: exec, command: ["echo", "{{ .each.part.uri }}"]}
  - id: regions
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 50, max_parallel: 2}
    with: {op: exec, command: ["echo", "{{ .each.item }}"]}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec, command: ["echo", "merge"]}
`

func TestRunPlanAndOrkflowGetEachGoldens(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "each.ork.yaml", eachCLIYAML)
	f.run("apply", path).requireExit(t, exitOK)
	plan := f.run("run", "plan", "each-cli", "--input-json", `{"regions":["eu","us","ap"]}`).requireExit(t, exitOK)
	requireGolden(t, "run-plan-each.golden", plan.stdout)
	for _, want := range []string{
		"each over {{ .inputs.regions }}: 3 items (≤ 50, 2 at a time)",
		"each over {{ .tasks.list.output.parts }} (≤ 500 items, 16 at a time; counted when the run reaches it)",
	} {
		if !strings.Contains(plan.stdout, want) {
			t.Fatalf("plan lacks %q:\n%s", want, plan.stdout)
		}
	}
	got := f.run("orkflow", "get", "each-cli").requireExit(t, exitOK)
	requireGolden(t, "orkflow-get-each.golden", got.stdout)
}

const taskRunEachYAML = `name: task-run-each
version: "1"
tasks:
  - id: list
    uses: shell
    with: {op: exec, command: ["echo", "list"]}
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", as: part, max_items: 10}
    with: {op: exec, command: ["echo", "{{ .each.part.name }}", "{{ .each.index }}/{{ .each.count }}"]}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec, command: ["echo", "merge"]}
`

func TestTaskRun_FanOutRequiresItem(t *testing.T) {
	fx := newTaskRunFixture(t)
	doc := fx.writeDoc(t, "each.ork.yaml", taskRunEachYAML)
	rt := &fakeTaskRuntime{}
	missing := fx.runTaskRun(rt, "--file", doc, "--task", "convert", "--render-only",
		"--output", `list={"parts":[{"name":"a"},{"name":"b"}]}`)
	if missing.code != exitError || !strings.Contains(missing.stderr, "is a fan-out (each:): name the item to run with --item N") {
		t.Fatalf("no --item = %d %q", missing.code, missing.stderr)
	}
	notFanOut := fx.runTaskRun(rt, "--file", doc, "--task", "list", "--item", "0", "--render-only")
	if notFanOut.code != exitError || !strings.Contains(notFanOut.stderr, `task "list" is not a fan-out`) {
		t.Fatalf("--item on list = %d %q", notFanOut.code, notFanOut.stderr)
	}
	outside := fx.runTaskRun(rt, "--file", doc, "--task", "convert", "--item", "5", "--render-only",
		"--output", `list={"parts":[{"name":"a"},{"name":"b"}]}`)
	if outside.code != exitError || !strings.Contains(outside.stderr, "has 2 items (0–1)") {
		t.Fatalf("--item 5 = %d %q", outside.code, outside.stderr)
	}
	ok := fx.runTaskRun(rt, "--file", doc, "--task", "convert", "--item", "1", "--render-only",
		"--output", `list={"parts":[{"name":"a"},{"name":"b"}]}`).requireExit(t, exitOK)
	if !strings.Contains(ok.stdout, `"b"`) || !strings.Contains(ok.stdout, "1/2") {
		t.Fatalf("the rendered config.json does not carry item 1:\n%s", ok.stdout)
	}
	if rt.created() != 0 {
		t.Fatal("--render-only created a container")
	}
}

func TestTaskRun_RefusesADirectDependentOfAFanOut(t *testing.T) {
	fx := newTaskRunFixture(t)
	doc := fx.writeDoc(t, "each.ork.yaml", taskRunEachYAML)
	got := fx.runTaskRun(&fakeTaskRuntime{}, "--file", doc, "--task", "merge", "--render-only")
	if got.code != exitError || !strings.Contains(got.stderr, `task run cannot stage the items of fan-out "convert"`) ||
		!strings.Contains(got.stderr, "a local run has no commit store") {
		t.Fatalf("merge = %d %q", got.code, got.stderr)
	}
}
