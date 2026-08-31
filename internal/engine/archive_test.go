package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func at(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func newArchiveFixture(t *testing.T) (*engine.RunArchiver, *artifacts.LocalRunArchive, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewLocalRunArchive(dir)
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return engine.NewRunArchiver(store), store, dir
}

// sampleRun is a finished run with the shape the archive must survive: typed
// payloads, task runs, artifacts and an error-free terminal state.
func sampleRun(t *testing.T, id string, status orkstackv1.Status, created time.Time) *orkstackv1.Run {
	t.Helper()
	input, err := structpb.NewStruct(map[string]any{"url": "https://example.test", "retries": float64(2)})
	if err != nil {
		t.Fatalf("input struct: %v", err)
	}
	output, err := structpb.NewStruct(map[string]any{"save": map[string]any{"bytes": float64(11)}})
	if err != nil {
		t.Fatalf("output struct: %v", err)
	}
	return &orkstackv1.Run{
		Id:             id,
		OrkflowName:    "hello-ork",
		OrkflowVersion: "v1",
		Status:         status,
		Input:          &orkstackv1.Payload{Data: input},
		Output:         &orkstackv1.Payload{Data: output},
		CreatedAt:      timestamppb.New(created),
		StartedAt:      timestamppb.New(created.Add(time.Second)),
		FinishedAt:     timestamppb.New(created.Add(3 * time.Second)),
		TaskRuns: []*orkstackv1.TaskRun{{
			TaskId:          "fetch",
			Status:          orkstackv1.Status_STATUS_SUCCEEDED,
			ExitCode:        0,
			Error:           &orkstackv1.Error{Code: "CONTAINER_EXIT_NONZERO", Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED},
			ValuesRef:       &orkstackv1.ArtifactRef{Name: "values.json", Uri: "s3://bucket/values", Sha256: strings.Repeat("a", 64)},
			Commit:          &orkstackv1.CommitRef{Uri: "s3://bucket/metadata/commits/commit.json", Sha256: strings.Repeat("b", 64), Etag: "etag"},
			SystemArtifacts: []*orkstackv1.ArtifactRef{{Name: "stdout.log", Uri: "s3://bucket/stdout"}, {Name: "stderr.log", Uri: "s3://bucket/stderr"}},
			Artifacts: []*orkstackv1.ArtifactRef{{
				Name: "body",
				Uri:  "s3://orkstack-artifacts/orkflows/hello-ork/fetch/" + id + "/body",
			}},
		}},
	}
}

func sampleEvents(t *testing.T, runID string, created time.Time) []*orkstackv1.Event {
	t.Helper()
	return []*orkstackv1.Event{
		{
			RunId: runID, Sequence: 1, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
			Status: orkstackv1.Status_STATUS_PENDING, Message: "run created",
			Timestamp: timestamppb.New(created),
		},
		{
			RunId: runID, Sequence: 2, Type: orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED,
			TaskId: "fetch", Status: orkstackv1.Status_STATUS_SUCCEEDED, Message: "task succeeded",
			Timestamp: timestamppb.New(created.Add(2 * time.Second)),
		},
		{
			RunId: runID, Sequence: 3, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			Status: orkstackv1.Status_STATUS_SUCCEEDED, Message: "run succeeded",
			Timestamp: timestamppb.New(created.Add(3 * time.Second)),
		},
	}
}

// TestMarshalRunArchive_IsOneRunLineThenOneEventPerLine pins the JSONL shape:
// every line is a standalone JSON record, the run snapshot leads, and every
// event follows in sequence order.
func TestMarshalRunArchive_IsOneRunLineThenOneEventPerLine(t *testing.T) {
	created := at(t, "2026-08-14T09:30:15Z")
	run := sampleRun(t, "ork-1-1", orkstackv1.Status_STATUS_SUCCEEDED, created)
	events := sampleEvents(t, "ork-1-1", created)

	body, err := engine.MarshalRunArchive(run, events)
	if err != nil {
		t.Fatalf("MarshalRunArchive: %v", err)
	}
	if !strings.HasSuffix(string(body), "\n") {
		t.Fatal("archive body must end with a newline so appends stay line-aligned")
	}

	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) != 1+len(events) {
		t.Fatalf("archive has %d lines, want 1 run + %d events", len(lines), len(events))
	}
	for i, line := range lines {
		var record struct {
			Kind  string          `json:"kind"`
			Run   json.RawMessage `json:"run"`
			Event json.RawMessage `json:"event"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("line %d is not standalone JSON: %v (%q)", i+1, err, line)
		}
		switch {
		case i == 0:
			if record.Kind != engine.ArchiveKindRun || len(record.Run) == 0 {
				t.Fatalf("line 1 = %q, want the run snapshot", line)
			}
		default:
			if record.Kind != engine.ArchiveKindEvent || len(record.Event) == 0 {
				t.Fatalf("line %d = %q, want an event", i+1, line)
			}
		}
	}
}

// TestRunArchive_RoundTripsEveryRunFieldAndEvent is the recovery guarantee: what
// comes back out of the archive is what went in, including typed payloads,
// task runs and artifact references.
func TestRunArchive_RoundTripsEveryRunFieldAndEvent(t *testing.T) {
	archiver, _, _ := newArchiveFixture(t)
	created := at(t, "2026-08-14T09:30:15Z")
	run := sampleRun(t, "ork-1-1", orkstackv1.Status_STATUS_SUCCEEDED, created)
	events := sampleEvents(t, "ork-1-1", created)

	if err := archiver.Archive(context.Background(), run, events); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	loaded, loadedEvents, err := archiver.Load(context.Background(), "ork-1-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.GetOrkflowName() != run.GetOrkflowName() ||
		loaded.GetOrkflowVersion() != run.GetOrkflowVersion() ||
		loaded.GetStatus() != run.GetStatus() {
		t.Fatalf("loaded run identity = %s@%s %s, want %s@%s %s",
			loaded.GetOrkflowName(), loaded.GetOrkflowVersion(), loaded.GetStatus(),
			run.GetOrkflowName(), run.GetOrkflowVersion(), run.GetStatus())
	}
	if got := loaded.GetInput().GetData().AsMap()["url"]; got != "https://example.test" {
		t.Fatalf("loaded input url = %v, want the archived value", got)
	}
	if got := loaded.GetInput().GetData().AsMap()["retries"]; got != float64(2) {
		t.Fatalf("loaded input retries = %v (%T), want 2 as a number", got, got)
	}
	if !loaded.GetCreatedAt().AsTime().Equal(created) {
		t.Fatalf("loaded created_at = %s, want %s", loaded.GetCreatedAt().AsTime(), created)
	}
	if len(loaded.GetTaskRuns()) != 1 {
		t.Fatalf("loaded %d task runs, want 1", len(loaded.GetTaskRuns()))
	}
	if got := loaded.GetTaskRuns()[0].GetArtifacts(); len(got) != 1 || got[0].GetUri() == "" {
		t.Fatalf("loaded task artifacts = %v, want the archived reference", got)
	}
	if len(loadedEvents) != len(events) {
		t.Fatalf("loaded %d events, want %d", len(loadedEvents), len(events))
	}
	for i, event := range loadedEvents {
		if event.GetSequence() != events[i].GetSequence() || event.GetType() != events[i].GetType() {
			t.Fatalf("event %d = seq %d %s, want seq %d %s", i,
				event.GetSequence(), event.GetType(), events[i].GetSequence(), events[i].GetType())
		}
	}
}

// TestUnmarshalRunArchive_RejectsABodyWithoutARunLine keeps a truncated or
// foreign object from being served as a run.
func TestUnmarshalRunArchive_RejectsABodyWithoutARunLine(t *testing.T) {
	body := []byte("{\"kind\":\"event\",\"event\":{\"runId\":\"ork-1-1\"}}\n")
	if _, _, err := engine.UnmarshalRunArchive(body); err == nil {
		t.Fatal("a body with no run line was accepted")
	}
}

// TestPublishEvent_ArchivesTheRunWhenItEnds is the write-through contract: a
// terminal lifecycle event, and only a terminal one, persists the run and every
// event the bus recorded for it.
func TestPublishEvent_ArchivesTheRunWhenItEnds(t *testing.T) {
	archiver, store, dir := newArchiveFixture(t)
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	activities := engine.NewArchivingEventActivities(bus, runs, archiver)

	created := at(t, "2026-08-14T09:30:15Z")
	runID := "ork-1-1"
	pending := &orkstackv1.Run{
		Id: runID, OrkflowName: "hello-ork", OrkflowVersion: "v1",
		Status: orkstackv1.Status_STATUS_PENDING, CreatedAt: timestamppb.New(created),
	}
	runs.Save(pending)
	bus.Publish(&orkstackv1.Event{
		RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status: orkstackv1.Status_STATUS_PENDING, Message: "run created",
		Timestamp: timestamppb.New(created),
	}, runID+"#created")

	// A non-terminal event projects but must not archive.
	running := sampleRun(t, runID, orkstackv1.Status_STATUS_RUNNING, created)
	running.CreatedAt = nil // the workflow's snapshot does not own CreatedAt
	if err := activities.PublishEvent(context.Background(), &orkstackv1.Event{
		RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, TaskId: "fetch",
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Timestamp: timestamppb.New(created.Add(2 * time.Second)),
	}, running, runID+"#2"); err != nil {
		t.Fatalf("PublishEvent(task succeeded): %v", err)
	}
	if _, err := store.GetRun(context.Background(), runID); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a running run was archived (err = %v); only terminal runs are", err)
	}

	// The terminal event archives.
	finished := sampleRun(t, runID, orkstackv1.Status_STATUS_SUCCEEDED, created)
	finished.CreatedAt = nil
	if err := activities.PublishEvent(context.Background(), &orkstackv1.Event{
		RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Message: "run succeeded",
		Timestamp: timestamppb.New(created.Add(3 * time.Second)),
	}, finished, runID+"#3"); err != nil {
		t.Fatalf("PublishEvent(run succeeded): %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "metadata", "runs", runID+".jsonl"))
	if err != nil {
		t.Fatalf("archived body: %v", err)
	}
	loaded, events, err := engine.UnmarshalRunArchive(body)
	if err != nil {
		t.Fatalf("UnmarshalRunArchive: %v", err)
	}
	if loaded.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("archived status = %s, want SUCCEEDED", loaded.GetStatus())
	}
	// CreatedAt is the API's, not the workflow's: the archive must persist the
	// merged snapshot, or a date-range query would index the wrong instant.
	if !loaded.GetCreatedAt().AsTime().Equal(created) {
		t.Fatalf("archived created_at = %s, want %s", loaded.GetCreatedAt().AsTime(), created)
	}
	archivedTask := loaded.GetTaskRuns()[0]
	if archivedTask.GetValuesRef().GetUri() == "" || archivedTask.GetCommit().GetUri() == "" || len(archivedTask.GetSystemArtifacts()) != 2 || archivedTask.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED {
		t.Fatalf("archive lost durable task fields: %+v", archivedTask)
	}
	if len(events) != 3 {
		t.Fatalf("archived %d events, want the run's whole log (3)", len(events))
	}
	if events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED {
		t.Fatalf("first archived event = %s, want RUN_CREATED", events[0].GetType())
	}
	if events[2].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("last archived event = %s, want the terminal event itself", events[2].GetType())
	}

	entries, err := store.ListRuns(context.Background(), artifacts.RunQuery{
		Orkflow: "hello-ork", Status: "succeeded",
	})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(entries) != 1 || entries[0].RunID != runID {
		t.Fatalf("index entries = %+v, want exactly %s", entries, runID)
	}
	if !entries[0].Time.Equal(created) {
		t.Fatalf("index time = %s, want the run's creation time %s", entries[0].Time, created)
	}
}

// TestPublishEvent_RepeatedTerminalEventArchivesOnce covers the activity retry
// and workflow replay path: the same terminal event twice leaves one run.
func TestPublishEvent_RepeatedTerminalEventArchivesOnce(t *testing.T) {
	archiver, store, _ := newArchiveFixture(t)
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	activities := engine.NewArchivingEventActivities(bus, runs, archiver)

	created := at(t, "2026-08-14T09:30:15Z")
	runID := "ork-2-1"
	runs.Save(&orkstackv1.Run{
		Id: runID, OrkflowName: "hello-ork", OrkflowVersion: "v1",
		Status: orkstackv1.Status_STATUS_PENDING, CreatedAt: timestamppb.New(created),
	})
	terminal := &orkstackv1.Event{
		RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Timestamp: timestamppb.New(created.Add(time.Second)),
	}
	finished := sampleRun(t, runID, orkstackv1.Status_STATUS_SUCCEEDED, created)
	finished.CreatedAt = nil

	for attempt := 0; attempt < 3; attempt++ {
		if err := activities.PublishEvent(context.Background(), terminal, finished, runID+"#1"); err != nil {
			t.Fatalf("PublishEvent attempt %d: %v", attempt+1, err)
		}
	}

	entries, err := store.ListRuns(context.Background(), artifacts.RunQuery{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("three attempts produced %d index entries, want 1", len(entries))
	}
	_, events, err := archiver.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("archived %d events, want 1 (the bus deduplicates by key)", len(events))
	}
}

// TestRunArchiver_ListRunsReturnsWholeRunsNewestFirst is what the API serves
// for `run list`: filtered by orkflow and status, ordered, with full detail.
func TestRunArchiver_ListRunsReturnsWholeRunsNewestFirst(t *testing.T) {
	archiver, _, _ := newArchiveFixture(t)
	ctx := context.Background()

	seed := []struct {
		id      string
		status  orkstackv1.Status
		created string
	}{
		{"ork-1", orkstackv1.Status_STATUS_SUCCEEDED, "2026-08-10T10:00:00Z"},
		{"ork-2", orkstackv1.Status_STATUS_FAILED, "2026-08-11T10:00:00Z"},
		{"ork-3", orkstackv1.Status_STATUS_SUCCEEDED, "2026-08-12T10:00:00Z"},
	}
	for _, s := range seed {
		created := at(t, s.created)
		run := sampleRun(t, s.id, s.status, created)
		if err := archiver.Archive(ctx, run, sampleEvents(t, s.id, created)); err != nil {
			t.Fatalf("Archive(%s): %v", s.id, err)
		}
	}

	runs, err := archiver.ListRuns(ctx, engine.ArchiveQuery{
		OrkflowName: "hello-ork",
		Status:      orkstackv1.Status_STATUS_SUCCEEDED,
	})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("listed %d runs, want the 2 succeeded ones", len(runs))
	}
	if runs[0].GetId() != "ork-3" || runs[1].GetId() != "ork-1" {
		t.Fatalf("listed %s then %s, want ork-3 then ork-1 (newest first)",
			runs[0].GetId(), runs[1].GetId())
	}
	if len(runs[0].GetTaskRuns()) != 1 {
		t.Fatal("a listed run must carry its task runs, not just its id")
	}

	window, err := archiver.ListRuns(ctx, engine.ArchiveQuery{
		Since: at(t, "2026-08-11T00:00:00Z"),
		Until: at(t, "2026-08-12T00:00:00Z"),
	})
	if err != nil {
		t.Fatalf("ListRuns(date range): %v", err)
	}
	if len(window) != 1 || window[0].GetId() != "ork-2" {
		t.Fatalf("date range listed %d runs (%v), want just ork-2", len(window), runIDsOf(window))
	}
}

// TestRunArchiver_ArchiveRejectsARunItCannotIndex fails loudly rather than
// writing a run that no query could ever find.
func TestRunArchiver_ArchiveRejectsARunItCannotIndex(t *testing.T) {
	archiver, _, _ := newArchiveFixture(t)
	created := at(t, "2026-08-14T09:30:15Z")

	nameless := sampleRun(t, "ork-1-1", orkstackv1.Status_STATUS_SUCCEEDED, created)
	nameless.OrkflowName = ""
	if err := archiver.Archive(context.Background(), nameless, nil); err == nil {
		t.Fatal("archived a run with no orkflow name")
	}

	unspecified := sampleRun(t, "ork-1-2", orkstackv1.Status_STATUS_UNSPECIFIED, created)
	if err := archiver.Archive(context.Background(), unspecified, nil); err == nil {
		t.Fatal("archived a run with an unspecified status")
	}
}

func runIDsOf(runs []*orkstackv1.Run) []string {
	out := make([]string, 0, len(runs))
	for _, run := range runs {
		out = append(out, run.GetId())
	}
	return out
}

func TestRunArchiver_IndexesEveryCanonicalTaskObjectReference(t *testing.T) {
	archiver, _, dir := newArchiveFixture(t)
	created := at(t, "2026-08-14T09:30:15Z")
	run := sampleRun(t, "ork-object-refs", orkstackv1.Status_STATUS_SUCCEEDED, created)
	digest := strings.Repeat("a", 64)
	objectIDs := []string{
		"objects/n-YXJ0aWZhY3Q/" + digest,
		"objects/n-c3Rkb3V0/" + digest,
		"objects/n-dmFsdWVz/" + digest,
	}
	run.TaskRuns[0].Artifacts = []*orkstackv1.ArtifactRef{{Uri: "file:///tmp/root/" + objectIDs[0]}}
	run.TaskRuns[0].SystemArtifacts = []*orkstackv1.ArtifactRef{{Uri: "s3://bucket/" + objectIDs[1]}, {Uri: "s3://bucket/not-an-object"}}
	run.TaskRuns[0].ValuesRef = &orkstackv1.ArtifactRef{Uri: "file:///tmp/root/" + objectIDs[2]}
	if err := archiver.Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	for _, objectID := range objectIDs {
		key, err := artifacts.RunObjectRefKey(objectID, run.Id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(key))); err != nil {
			t.Fatalf("reference marker %q: %v", key, err)
		}
	}
}
