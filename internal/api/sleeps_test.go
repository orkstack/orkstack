package api_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// sleepBesideAwaitYAML is the awaited fixture's callback await beside a
// sleep, so the run has a valid callback token a request could misaim.
const sleepBesideAwaitYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: awaited-api
version: "1.0.0"
tasks:
  - id: gate
    await:
      timeout_seconds: 3600
      callback: true
  - id: nap
    sleep: {seconds: 600}
`

func newSleepFixture(t *testing.T) *awaitFixture {
	t.Helper()
	f := newAwaitFixture(t)
	wf, err := dsl.Compile([]byte(sleepBesideAwaitYAML))
	must.OKf(t, err, "compile")
	f.wf = wf
	return f
}

// Neither door resolves a sleeping task, whatever its state and whatever
// token the request carries: ResolveAwait refuses it by name, the callback
// door answers as for any task that takes no callback — even with the run's
// own valid token, for the await beside it — and nothing is stored or
// delivered. The positive control: the same token resolves gate.
func TestResolveAwait_NeitherDoorResolvesASleep(t *testing.T) {
	for _, status := range []orkstackv1.Status{
		orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_SLEEPING, orkstackv1.Status_STATUS_SUCCEEDED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			f := newSleepFixture(t)
			f.seedRun(t, "ork-sleep-door", "", map[string]orkstackv1.Status{
				"nap": status, "gate": orkstackv1.Status_STATUS_AWAITING})
			for _, req := range []*orkstackv1.ResolveAwaitRequest{
				{RunId: "ork-sleep-door", TaskId: "nap", ValuesJson: `{}`},
				{RunId: "ork-sleep-door", TaskId: "nap", Reject: true, Reason: "wake up"},
			} {
				_, err := f.resolve(req, "mallory")
				if connect.CodeOf(err) != connect.CodeInvalidArgument ||
					!strings.Contains(err.Error(), `task "nap" of run "ork-sleep-door" is a sleeping task, not an awaited one`) {
					t.Fatalf("ResolveAwait(%v) = %v, want the sleeping-task refusal", req, err)
				}
			}
			for _, path := range []string{awaitPath("ork-sleep-door", "nap"), awaitPath("ork-sleep-door", "nap") + "/reject"} {
				status, body := f.door(t, path, bearer(awaitToken), `{"reason":"wake up"}`, nil)
				if status != 404 || errorCode(t, body) != "not_found" {
					t.Fatalf("POST %s = %d %s, want 404", path, status, body)
				}
			}
			f.untouched(t, "a sleep")
			status, body := f.door(t, awaitPath("ork-sleep-door", "gate"), bearer(awaitToken), `{}`, nil)
			if status != 202 {
				t.Fatalf("the positive control: POST gate with its own token = %d %s, want 202", status, body)
			}
		})
	}
}

func TestStreamTaskLogs_RefusesASleepingTask(t *testing.T) {
	run := runningRun("ork-logs-sleep", "nap")
	run.TaskRuns[0].Status = orkstackv1.Status_STATUS_SLEEPING
	run.TaskRuns[0].Sleep = &orkstackv1.TaskSleep{Seconds: 60}
	fixture := newLogFixture(t, run)
	stream, err := fixture.runClient().StreamTaskLogs(context.Background(), connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "nap", Follow: true}))
	if err == nil {
		for stream.Receive() {
		}
		err = stream.Err()
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), `task "nap" of run "ork-logs-sleep" is a sleeping task: it runs no container and writes no logs`) {
		t.Fatalf("error = %v", err)
	}
}
