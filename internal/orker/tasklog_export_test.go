package orker

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fakeStdioRuntime is the smallest ContainerRuntime that lets Execute reach
// STEP 9-10 without a daemon: Start writes a couple of framed lines to the two
// streams the dispatcher hands it, and Wait reports a nonzero exit so the run
// stops at STEP 11 without needing a values.json (this test is about the log
// fan-out, not the contract).
type fakeStdioRuntime struct{}

func (fakeStdioRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{Platform: "linux/amd64", Delivery: DeliveryRegistry}, nil
}

func (fakeStdioRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no embedded layout in this test")
}

func (fakeStdioRuntime) Create(context.Context, ContainerSpec) (ContainerID, error) {
	return "fake-container", nil
}

func (fakeStdioRuntime) Start(_ context.Context, _ ContainerID, stdout, stderr io.Writer) error {
	_, _ = stdout.Write([]byte("hello from stdout\n"))
	_, _ = stderr.Write([]byte("hello from stderr\n"))
	return nil
}

func (fakeStdioRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }

func (fakeStdioRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: 1}, nil
}

func (fakeStdioRuntime) Remove(context.Context, ContainerID) error { return nil }

func (fakeStdioRuntime) Reap(context.Context, string) (int, error) { return 0, nil }

func (fakeStdioRuntime) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{}, nil
}

// taskLogEvent is one call the recorder observed, in the order it arrived.
type taskLogEvent struct {
	write string
	close bool
}

// recordingTaskLogExporter is a TaskLogExporter that records every Open and
// every event on the TaskLogWriter it hands back, so a test can assert both
// what was opened and the exact sequence written to it.
type recordingTaskLogExporter struct {
	mu      sync.Mutex
	opens   int
	openIn  *orkstackv1.ExecuteTaskInput
	attempt int32
	events  []taskLogEvent
}

func (r *recordingTaskLogExporter) Open(
	_ context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32,
) TaskLogWriter {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.opens++
	r.openIn = in
	r.attempt = attempt
	return &recordingTaskLogWriter{r: r}
}

func (r *recordingTaskLogExporter) recorded() (int, []taskLogEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.opens, append([]taskLogEvent(nil), r.events...)
}

type recordingTaskLogWriter struct{ r *recordingTaskLogExporter }

func (w *recordingTaskLogWriter) Write(line string) {
	w.r.mu.Lock()
	defer w.r.mu.Unlock()
	w.r.events = append(w.r.events, taskLogEvent{write: line})
}

func (w *recordingTaskLogWriter) Close() {
	w.r.mu.Lock()
	defer w.r.mu.Unlock()
	w.r.events = append(w.r.events, taskLogEvent{close: true})
}

// baseExportInput is a well-formed ExecuteTaskInput for fakeStdioRuntime. It
// needs no real image or values.json: the fake exits nonzero before the
// contract is checked.
func baseExportInput(taskID string) *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{
		RunId:          "ork-export-" + taskID,
		OrkflowName:    "export-test",
		OrkflowVersion: "1.0.0",
		Namespace:      "default",
		Pool:           "main",
		TaskId:         taskID,
		IdempotencyKey: "default/ork-export-" + taskID + "/" + taskID,
		Image: &orkstackv1.ImageRef{
			Reference:   "example/image:latest",
			Pinned:      "example/image@sha256:" + strings.Repeat("a", 64),
			IndexDigest: "sha256:" + strings.Repeat("a", 64),
		},
		Command:                        []string{"/bin/true"},
		TimeoutSeconds:                 30,
		CancellationGracePeriodSeconds: 2,
		Network:                        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		Limits: &orkstackv1.WorkspaceLimits{
			ValuesMaxBytes:   262144,
			ArtifactMaxCount: 256,
			ArtifactMaxBytes: 5368709120,
			LogMaxBytes:      16777216,
			CommitMaxBytes:   4194304,
		},
	}
}

// newExportDispatcher builds a dispatcher wired to fakeStdioRuntime, an
// in-memory artifact store and a recording LogStore, with no commit store —
// the TEST-ONLY configuration the Options doc describes.
func newExportDispatcher(t *testing.T, taskLogs TaskLogExporter) (*OCIDispatcher, *recordingLogStore) {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local artifact store: %v", err)
	}
	logs := &recordingLogStore{}
	return &OCIDispatcher{
		Runtime:  fakeStdioRuntime{},
		Store:    store,
		Refs:     artifacts.MemoryRefPredictor{},
		Logs:     logs,
		Policy:   DefaultSecurityPolicy(),
		WorkRoot: t.TempDir(),
		OrkerID:  "orker-export-test",
		BuildID:  "test",
		TaskLogs: taskLogs,
	}, logs
}

// linesOf decodes a recordingLogStore's stored chunks back into the framed
// lines the sink received, in upload order — the same shape emit() handed to
// both the sink and the exporter.
func linesOf(t *testing.T, logs *recordingLogStore) []string {
	t.Helper()
	var buf strings.Builder
	for _, chunk := range logs.stored() {
		buf.Write(chunk.Data)
	}
	text := strings.TrimSuffix(buf.String(), "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// TestTaskLogExporter_MirrorsTheChunkSink is the seam contract: every line
// the durable LogStore received is also received by the exporter, in the same
// order and framing, and Close runs exactly once, after every Write.
func TestTaskLogExporter_MirrorsTheChunkSink(t *testing.T) {
	exporter := &recordingTaskLogExporter{}
	dispatcher, logs := newExportDispatcher(t, exporter)
	in := baseExportInput("mirrors")

	// The container exits nonzero, so Execute fails the task — that is
	// expected and irrelevant here; only the log fan-out is under test.
	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute: want a failure (the fake container exits 1), got nil")
	}

	opens, events := exporter.recorded()
	if opens != 1 {
		t.Fatalf("Open called %d times, want 1", opens)
	}
	if exporter.openIn != in {
		t.Error("Open did not receive the same ExecuteTaskInput Execute was given")
	}
	if exporter.attempt != 1 {
		t.Errorf("Open attempt = %d, want 1 (no Temporal activity context)", exporter.attempt)
	}

	if len(events) == 0 {
		t.Fatal("exporter recorded no events at all")
	}
	last := events[len(events)-1]
	if !last.close {
		t.Fatalf("last exporter event = %+v, want Close last", last)
	}
	closeCount := 0
	var exportedLines []string
	for _, ev := range events {
		if ev.close {
			closeCount++
			continue
		}
		exportedLines = append(exportedLines, ev.write)
	}
	if closeCount != 1 {
		t.Fatalf("Close called %d times, want exactly 1", closeCount)
	}

	wantLines := linesOf(t, logs)
	if len(wantLines) == 0 {
		t.Fatal("the durable LogStore recorded no lines; the fake runtime should have written two")
	}
	if len(exportedLines) != len(wantLines) {
		t.Fatalf("exporter got %d lines %q, chunk sink got %d lines %q",
			len(exportedLines), exportedLines, len(wantLines), wantLines)
	}
	for i := range wantLines {
		if exportedLines[i] != wantLines[i] {
			t.Errorf("line %d: exporter got %q, chunk sink got %q", i, exportedLines[i], wantLines[i])
		}
	}

	// If the harness produced a teardown line (the deferred container Remove
	// failing), it would be the durable sink's last line and would start with
	// "[ork] "; that path bypasses emit and is not part of this seam, so it
	// is only asserted when present.
	if last := wantLines[len(wantLines)-1]; strings.HasPrefix(last, "[ork] ") {
		if exportedLines[len(exportedLines)-1] != last {
			t.Errorf("chunk sink's last line was a teardown line %q not mirrored to the exporter", last)
		}
	}
}

// TestTaskLogExporter_NilMeansUnchanged is what "nil exports nothing" (the
// TaskLogs doc comment) promises: with no exporter configured, Execute's
// behavior — and in particular its outcome — does not change.
func TestTaskLogExporter_NilMeansUnchanged(t *testing.T) {
	dispatcher, logs := newExportDispatcher(t, nil)
	in := baseExportInput("nil-exporter")

	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute: want a failure (the fake container exits 1), got nil")
	}
	if len(linesOf(t, logs)) == 0 {
		t.Fatal("the durable LogStore recorded no lines with TaskLogs nil; the chunk sink must be unaffected")
	}
}

// nilWriterExporter's Open returns a nil TaskLogWriter interface — an
// implementation that has nothing to say for this attempt. Execute must
// treat that exactly like "no exporter at all" rather than panicking on the
// nil receiver.
type nilWriterExporter struct{ opens int }

func (e *nilWriterExporter) Open(context.Context, *orkstackv1.ExecuteTaskInput, int32) TaskLogWriter {
	e.opens++
	return nil
}

// TestTaskLogExporter_NilWriterFromOpenDoesNotPanic covers an exporter that
// is configured (non-nil TaskLogExporter) but declines to export a given
// attempt by returning a nil TaskLogWriter.
func TestTaskLogExporter_NilWriterFromOpenDoesNotPanic(t *testing.T) {
	exporter := &nilWriterExporter{}
	dispatcher, _ := newExportDispatcher(t, exporter)
	in := baseExportInput("nil-writer")

	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute: want a failure (the fake container exits 1), got nil")
	}
	if exporter.opens != 1 {
		t.Fatalf("Open called %d times, want 1", exporter.opens)
	}
}
