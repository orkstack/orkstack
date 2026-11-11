package api_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// stoppingEvents is a Service following a live run in process memory, with one
// event to read, and the context its stream runs under.
func stoppingEvents(t *testing.T) (*api.Service, context.Context, context.CancelCauseFunc) {
	t.Helper()
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: "live", Status: orkstackv1.Status_STATUS_RUNNING})
	bus.Publish(&orkstackv1.Event{RunId: "live", Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		Status: orkstackv1.Status_STATUS_RUNNING}, "started")
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	return api.NewService(api.Config{Runs: runs, Bus: bus}), ctx, cancel
}

// TestStreamRunEvents_AStoppingReplicaAnswersUnavailable: a replica that stops
// mid-stream says so, in the answer every client resumes on, whichever client
// that is. A clean end would read as the run's end.
func TestStreamRunEvents_AStoppingReplicaAnswersUnavailable(t *testing.T) {
	svc, ctx, cancel := stoppingEvents(t)
	err := svc.StreamRunEvents(ctx, "live", "", 0, func(*orkstackv1.Event) error {
		cancel(api.ErrEngineStopping)
		return nil
	})
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("StreamRunEvents = %v, want Unavailable", err)
	}
	if !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("StreamRunEvents = %q, want it to say the replica is shutting down", err)
	}
}

// A client that went away is not told anything: there is nobody to tell.
func TestStreamRunEvents_ACallerThatLeftIsACleanEnd(t *testing.T) {
	svc, ctx, cancel := stoppingEvents(t)
	err := svc.StreamRunEvents(ctx, "live", "", 0, func(*orkstackv1.Event) error {
		cancel(context.Canceled)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamRunEvents = %v, want a clean end", err)
	}
}

// A run that finished ended its stream cleanly, stopping or not: its terminal
// event went out, and nothing is left to resume.
func TestStreamRunEvents_AFinishedRunEndsCleanlyOnAStoppingReplica(t *testing.T) {
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: "done", Status: orkstackv1.Status_STATUS_RUNNING})
	bus.Publish(&orkstackv1.Event{RunId: "done", Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status: orkstackv1.Status_STATUS_SUCCEEDED}, "finished")
	svc := api.NewService(api.Config{Runs: runs, Bus: bus})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := svc.StreamRunEvents(ctx, "done", "", 0, func(event *orkstackv1.Event) error {
		if api.IsTerminalEvent(event) {
			cancel(api.ErrEngineStopping)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("StreamRunEvents = %v, want a clean end after the terminal event", err)
	}
}

// failingLogStore reads chunks as a store does once its context has ended.
type failingLogStore struct {
	*memoryLogStore
}

func (s failingLogStore) ReadLogChunks(ctx context.Context, ref artifacts.LogRef, after int64, limit int, maxBytes int64) ([]artifacts.LogChunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.memoryLogStore.ReadLogChunks(ctx, ref, after, limit, maxBytes)
}

// streamStoppingLogs runs one log query under a context cancelled with cause
// after the first chunk reaches the client.
func streamStoppingLogs(t *testing.T, follow bool, cause error) (sent []*orkstackv1.TaskLogChunk, err error) {
	t.Helper()
	run := runningRun("ork-logs-stop", "build")
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}
	fixture.logs.write(ref, 1, "one\n")
	store := failingLogStore{fixture.logs}
	svc := api.NewService(api.Config{Reader: fixture.reader, Logs: store,
		LogPoll: api.LogPollOptions{Min: 5 * time.Millisecond, Max: 20 * time.Millisecond}})

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err = svc.StreamRunTaskLogs(ctx, api.TaskLogQuery{RunID: run.GetId(), TaskID: "build", Follow: follow},
		func(chunk *orkstackv1.TaskLogChunk) error {
			sent = append(sent, chunk)
			cancel(cause)
			return nil
		})
	return sent, err
}

// TestStreamRunTaskLogs_AStoppingReplicaAnswersUnavailable: a follow and a
// replay alike, and neither claims with `last` that the log is complete when
// the store's last read failed because the replica is going away.
func TestStreamRunTaskLogs_AStoppingReplicaAnswersUnavailable(t *testing.T) {
	for name, follow := range map[string]bool{"a follow": true, "a replay": false} {
		t.Run(name, func(t *testing.T) {
			sent, err := streamStoppingLogs(t, follow, api.ErrEngineStopping)
			if connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("StreamRunTaskLogs = %v, want Unavailable", err)
			}
			for _, chunk := range sent {
				if chunk.GetLast() {
					t.Fatal("the stream said its log was complete")
				}
			}
		})
	}
}

// A caller that left is told nothing, and is not told the log is complete.
func TestStreamRunTaskLogs_ACallerThatLeftIsNotToldTheLogIsComplete(t *testing.T) {
	for name, follow := range map[string]bool{"a follow": true, "a replay": false} {
		t.Run(name, func(t *testing.T) {
			sent, err := streamStoppingLogs(t, follow, context.Canceled)
			if err != nil {
				t.Fatalf("StreamRunTaskLogs = %v, want a clean end", err)
			}
			for _, chunk := range sent {
				if chunk.GetLast() {
					t.Fatal("the stream said its log was complete")
				}
			}
		})
	}
}

// A log the stream finished delivering ends cleanly, stopping or not.
func TestStreamRunTaskLogs_ACompleteLogEndsCleanlyOnAStoppingReplica(t *testing.T) {
	run := finish(runningRun("ork-logs-done", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	fixture.logs.write(artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}, 1, "one\n")
	svc := api.NewService(api.Config{Reader: fixture.reader, Logs: fixture.logs})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := svc.StreamRunTaskLogs(ctx, api.TaskLogQuery{RunID: run.GetId(), TaskID: "build", Follow: true},
		func(chunk *orkstackv1.TaskLogChunk) error {
			if chunk.GetLast() {
				cancel(api.ErrEngineStopping)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("StreamRunTaskLogs = %v, want a clean end after `last`", err)
	}
}

// TestRunSocket_AStoppingReplicaDropsTheSocket: a socket follows the handler's
// context, so a stopping replica's reaches the client as a dropped connection,
// which the UI's run page reconnects on (ui/src/lib/runEvents.ts). What matters
// is what it must not be: a close that says the stream ended.
func TestRunSocket_AStoppingReplicaDropsTheSocket(t *testing.T) {
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: "live", Status: orkstackv1.Status_STATUS_RUNNING})
	bus.Publish(&orkstackv1.Event{RunId: "live", Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		Status: orkstackv1.Status_STATUS_RUNNING}, "started")
	svc := api.NewService(api.Config{Runs: runs, Bus: bus})

	base, stop := context.WithCancelCause(context.Background())
	defer stop(nil)
	server := httptest.NewUnstartedServer(api.NewMux(svc, api.MuxOptions{}))
	server.Config.BaseContext = func(net.Listener) context.Context { return base }
	server.Start()
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws/runs/live", &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": {server.URL}},
	})
	must.OKf(t, err, "dial")
	defer conn.CloseNow()
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	stop(api.ErrEngineStopping)
	_, _, err = conn.Read(ctx)
	if err == nil {
		t.Fatal("the socket sent a frame after the replica stopped")
	}
	if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
		t.Fatalf("the socket ended %v: a normal close says the stream ended", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the socket stayed open after the replica stopped")
	}
}
