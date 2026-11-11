package main

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const sleepingCLIYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: sleeping-cli
version: "1.0.0"
tasks:
  - id: deploy
    uses: shell
    with: {op: exec, command: ["echo", "deploy"]}
  - id: soak
    needs: [deploy]
    sleep: {seconds: 900}
  - id: settle
    needs: [deploy]
    join: all_done
    sleep: {seconds: 60}
  - id: promote
    uses: shell
    needs: [soak]
    with: {op: exec, command: ["echo", "promote"]}
`

// sleepCLIFixture is the CLI fixture with one sleeping run seeded: soak
// SLEEPING for 15 minutes since 5 minutes ago, settle woken, promote PENDING.
func sleepCLIFixture(t *testing.T) (*fixture, *awaitReader, *orkstackv1.Run) {
	t.Helper()
	reader := &awaitReader{runs: map[string]*orkstackv1.Run{}, starts: map[string]*engine.RunStartRecord{}}
	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OKf(t, err, "local store")
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = reader
		cfg.ArtifactStore = store
	})
	wf, err := dsl.Compile([]byte(sleepingCLIYAML))
	must.OKf(t, err, "compile")
	opened := fixedNow.Add(-5 * time.Minute)
	run := &orkstackv1.Run{
		Id: "ork-sleep-1", OrkflowName: "sleeping-cli", OrkflowVersion: "1.0.0",
		Status:    orkstackv1.Status_STATUS_RUNNING,
		CreatedAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-1", Attempt: 1,
				ScheduledAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened), FinishedAt: timestamppb.New(opened)},
			{TaskId: "soak", Status: orkstackv1.Status_STATUS_SLEEPING,
				ScheduledAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened),
				Sleep: &orkstackv1.TaskSleep{Seconds: 900, WakeAt: timestamppb.New(opened.Add(15 * time.Minute))}},
			{TaskId: "settle", Status: orkstackv1.Status_STATUS_SUCCEEDED, Join: orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE,
				ScheduledAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened),
				FinishedAt: timestamppb.New(opened.Add(time.Minute)),
				Sleep:      &orkstackv1.TaskSleep{Seconds: 60, WakeAt: timestamppb.New(opened.Add(time.Minute))}},
			{TaskId: "promote", Status: orkstackv1.Status_STATUS_PENDING},
		},
	}
	reader.runs[run.GetId()] = run
	reader.starts[run.GetId()] = &engine.RunStartRecord{Orkflow: wf, Plan: engine.RunPlan{Namespace: "default"}}
	return f, reader, run
}

func TestRunGetSleepGolden(t *testing.T) {
	saved := nowFunc
	nowFunc = func() time.Time { return fixedNow }
	defer func() { nowFunc = saved }()
	f, _, _ := sleepCLIFixture(t)
	out := f.run("run", "get", "ork-sleep-1").requireExit(t, exitOK)
	requireGolden(t, "run-get-sleep.golden", out.stdout)
	if !strings.Contains(out.stdout, "SLEEPING") || !strings.Contains(out.stdout, "10m") {
		t.Fatalf("run get does not show the sleep and its 10 minutes left:\n%s", out.stdout)
	}
}

func TestRunPlanSleepGolden(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "sleeping.ork.yaml", sleepingCLIYAML)
	f.run("orkflow", "create", path).requireExit(t, exitOK)
	out := f.run("run", "plan", "sleeping-cli").requireExit(t, exitOK)
	requireGolden(t, "run-plan-sleep.golden", out.stdout)
	if !strings.Contains(out.stdout, "sleep 15m0s") || !strings.Contains(out.stdout, "2 sleeps") {
		t.Fatalf("run plan does not show the sleeps:\n%s", out.stdout)
	}
}

func TestOrkflowGetSleepGolden(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "sleeping.ork.yaml", sleepingCLIYAML)
	f.run("orkflow", "create", path).requireExit(t, exitOK)
	out := f.run("orkflow", "get", "sleeping-cli").requireExit(t, exitOK)
	requireGolden(t, "orkflow-get-sleep.golden", out.stdout)
}

func TestRunPlan_AssertRunsAcceptsASleep(t *testing.T) {
	f := newFixture(t)
	path := writePlanFile(t, "sleeping.ork.yaml", sleepingCLIYAML)
	f.run("orkflow", "create", path).requireExit(t, exitOK)
	f.run("run", "plan", "sleeping-cli", "--assert-runs", "soak", "--assert-runs", "promote").requireExit(t, exitOK)
	f.run("run", "plan", "sleeping-cli", "--assert-skipped", "soak").requireExit(t, exitPlanAssertionFailed)
}

// Nothing outside a run resolves a sleep: the door says what it is.
func TestRunResolve_ASleepingTaskIsRefused(t *testing.T) {
	f, _, _ := sleepCLIFixture(t)
	out := f.run("run", "resolve", "ork-sleep-1", "--task", "soak", "--values", `{}`).requireExit(t, exitError)
	if !strings.Contains(out.stderr, `task "soak" of run "ork-sleep-1" is a sleeping task, not an awaited one`) {
		t.Fatalf("stderr = %q", out.stderr)
	}
	if len(f.starter.signals) != 0 {
		t.Fatalf("a refused resolution delivered %v", f.starter.signals)
	}
}

func TestRunTiming_ASleepingTaskSleptNotQueued(t *testing.T) {
	opened := fixedNow
	run := &orkstackv1.Run{Id: "ork-t", TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "soak", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		ScheduledAt: timestamppb.New(opened), StartedAt: timestamppb.New(opened),
		FinishedAt: timestamppb.New(opened.Add(15 * time.Minute)),
		Sleep:      &orkstackv1.TaskSleep{Seconds: 900, WakeAt: timestamppb.New(opened.Add(15 * time.Minute))},
	}}}
	timing := computeRunTiming(run, nil, deriveOptions{AttemptStore: true, Now: fixedNow})
	task := timing.Tasks[0]
	if !task.Slept || task.Wait.applies || task.QueueWait.applies || !task.Run.known || task.Run.value != 15*time.Minute {
		t.Fatalf("timing = %+v", task)
	}
	var b strings.Builder
	must.OK(t, renderRunTiming(&b, timing, true))
	if !strings.Contains(b.String(), "slept 15m0s") || strings.Contains(b.String(), "attempts:") {
		t.Fatalf("rendered:\n%s", b.String())
	}
}

func TestSleepRemaining(t *testing.T) {
	wake := timestamppb.New(fixedNow.Add(90 * time.Minute))
	cases := []struct {
		name string
		row  *orkstackv1.TaskRun
		want string
	}{
		{"sleeping", &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_SLEEPING, Sleep: &orkstackv1.TaskSleep{WakeAt: wake}}, "1h30m"},
		{"past its wake-up", &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_SLEEPING,
			Sleep: &orkstackv1.TaskSleep{WakeAt: timestamppb.New(fixedNow.Add(-time.Second))}}, "waking"},
		{"woke", &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_SUCCEEDED, Sleep: &orkstackv1.TaskSleep{WakeAt: wake}}, dash},
		{"pending", &orkstackv1.TaskRun{Status: orkstackv1.Status_STATUS_PENDING, Sleep: &orkstackv1.TaskSleep{}}, dash},
	}
	for _, tc := range cases {
		if got := sleepRemaining(tc.row, fixedNow); got != tc.want {
			t.Errorf("%s: sleepRemaining = %q, want %q", tc.name, got, tc.want)
		}
	}
}
