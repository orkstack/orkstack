package orker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// errStubFailure is the failure a stub plugin returns when the test is about
// what happens to a failed task rather than about why it failed.
var errStubFailure = errors.New("stub plugin refused")

// The claim of this milestone is "visible WHILE it runs", so the test blocks
// the plugin until the line is readable from the store. A sink that buffered
// everything and wrote at the end would deadlock here and time out — which is
// exactly the failure the old behavior deserves.
func TestExecuteTask_PluginLogsAreReadableFromTheStoreWhileTheTaskRuns(t *testing.T) {
	store := &recordingLogStore{}
	ref := artifacts.LogRef{RunID: "ork-live-1", TaskID: "emit", Attempt: 1}

	observed := make(chan string, 1)
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
			ec.Log("the-marker-line")
			// Wait, inside the plugin, for the line to be durable. Nothing
			// about this task has finished yet.
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				chunks, err := store.ReadLogChunks(context.Background(), ref, 0, 0)
				if err != nil {
					return nil, err
				}
				for _, chunk := range chunks {
					if strings.Contains(string(chunk.Data), "the-marker-line") {
						observed <- string(chunk.Data)
						return &plugin.Result{}, nil
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			return nil, context.DeadlineExceeded
		},
	}

	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{
		Logs: store,
		Log:  LogOptions{FlushInterval: 20 * time.Millisecond, MaxBytes: 1 << 20},
	})
	if _, err := runActivity(t, activities, task.ExecuteTaskInput{
		RunID: ref.RunID, TaskID: ref.TaskID, Plugin: "demo",
	}); err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}
	select {
	case line := <-observed:
		t.Logf("the plugin read its own log line back from the store mid-task: %q", line)
	default:
		t.Fatal("the plugin never saw its line in the store while it was still running")
	}
}

// Everything a task wrote must be durable before ExecuteTask returns, because
// the task's terminal event comes after that and the reader ends its stream on
// the terminal event. A chunk that landed later would be one nobody receives.
func TestExecuteTask_ClosesTheLogSinkBeforeItReturns(t *testing.T) {
	store := &recordingLogStore{}
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
			ec.Log("late line that is never big enough to trip the size bound")
			return &plugin.Result{}, nil
		},
	}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{
		Logs: store,
		// Neither bound can fire on its own inside the task's lifetime, so
		// only the close-time flush can put this line in the store.
		Log: LogOptions{FlushBytes: 1 << 20, FlushInterval: time.Hour, MaxBytes: 1 << 20},
	})
	if _, err := runActivity(t, activities, task.ExecuteTaskInput{
		RunID: "ork-close-1", TaskID: "emit", Plugin: "demo",
	}); err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}
	if !strings.Contains(store.body(), "late line") {
		t.Fatalf("the store holds %q after ExecuteTask returned; the final flush is missing", store.body())
	}
}

// A failing task's output is the output somebody actually wants, so it is
// stored on the failure path too.
func TestExecuteTask_StoresLogsOfAFailedTask(t *testing.T) {
	store := &recordingLogStore{}
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
			ec.Log("about to fail")
			return nil, plugin.Permanent(errStubFailure)
		},
	}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{
		Logs: store,
		Log:  LogOptions{FlushBytes: 1 << 20, FlushInterval: time.Hour, MaxBytes: 1 << 20},
	})
	if _, err := runActivity(t, activities, task.ExecuteTaskInput{
		RunID: "ork-fail-1", TaskID: "emit", Plugin: "demo",
	}); err == nil {
		t.Fatal("ExecuteTask succeeded, want the plugin's permanent failure")
	}
	if !strings.Contains(store.body(), "about to fail") {
		t.Fatalf("a failed task stored %q; its log is what somebody reads next", store.body())
	}
}

// Each retry writes its own attempt, so a resumed reader never sees two tries
// interleaved and "attempt 0 = the latest" has something to resolve to.
func TestExecuteTask_WritesUnderTheActivityAttempt(t *testing.T) {
	store := &recordingLogStore{}
	stub := &stubPlugin{
		name: "demo",
		run: func(_ context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
			ec.Log("try")
			return &plugin.Result{}, nil
		},
	}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{
		Logs: store,
		Log:  LogOptions{FlushBytes: 1 << 20, FlushInterval: time.Hour, MaxBytes: 1 << 20},
	})
	if _, err := runActivity(t, activities, task.ExecuteTaskInput{
		RunID: "ork-attempt-1", TaskID: "emit", Plugin: "demo",
	}); err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}
	chunks := store.stored()
	if len(chunks) == 0 {
		t.Fatal("nothing was stored")
	}
	for _, chunk := range chunks {
		if chunk.Attempt < 1 {
			t.Errorf("chunk %d was written under attempt %d; attempts are 1-based", chunk.Seq, chunk.Attempt)
		}
		if chunk.RunID != "ork-attempt-1" || chunk.TaskID != "emit" {
			t.Errorf("chunk %d carries %+v, want the task's own coordinates", chunk.Seq, chunk.LogRef)
		}
	}
}

// An Orker with no log store configured still runs tasks: logging is
// observability, and losing it must never lose the work.
func TestExecuteTask_RunsWithNoLogStoreConfigured(t *testing.T) {
	stub := &stubPlugin{name: "demo"}
	activities := NewActivitiesWithOptions(registryWith(stub), newTestStore(t), Options{})
	if _, err := runActivity(t, activities, task.ExecuteTaskInput{
		RunID: "ork-nolog-1", TaskID: "emit", Plugin: "demo",
	}); err != nil {
		t.Fatalf("ExecuteTask with no log store: %v", err)
	}
}
