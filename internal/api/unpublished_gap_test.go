package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The run an ending before start leaves when object storage refused its
// RUN_CREATED twice: its first execution published RUN_FAILED at 2 and
// nothing at 1 (engine emitRunCreatedFirst).
const unpublishedRunID = "ork-unpublished"

var unpublishedGap = engine.EventGap{First: 1, Last: 1, Reason: engine.UnpublishedEventsReason}

const unpublishedSentence = "events 1–1 of run ork-unpublished were never published " +
	"(the run's history holds no event under these numbers); the run's outcome and its other events are intact"

func unpublishedRunEvents() []*orkstackv1.Event {
	return []*orkstackv1.Event{{RunId: unpublishedRunID, Sequence: 2, Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED}}
}

// unpublishedReader serves the run as engine.TemporalRunReader does
// (TestTemporalRunReader_AnEndingBeforeStartIsServedAndArchivedWithItsGap):
// a stream read from below 2 fails with the numbers the run never published.
type unpublishedReader struct{ *fakeReader }

func (r unpublishedReader) StreamEvents(ctx context.Context, runID string, fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	if runID == unpublishedRunID && fromSequence < unpublishedGap.Last {
		r.mu.Lock()
		r.streamCount++
		r.mu.Unlock()
		return fmt.Errorf("engine: stream: %w",
			&engine.UnpublishedEventsError{RunID: runID, Gaps: []engine.EventGap{unpublishedGap}})
	}
	return r.fakeReader.StreamEvents(ctx, runID, fromSequence, send)
}

// TestUnpublishedStart_LiveAndArchivedReadsAgree: the run's stream read live
// (StreamRunEvents, from the system of record) and read from its archive
// (ArchivedEvents) answer alike. From 0 both are DataLoss with the one
// sentence that names 1 as never published, and nothing is sent; from 1
// both serve RUN_FAILED at 2.
func TestUnpublishedStart_LiveAndArchivedReadsAgree(t *testing.T) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	run := &orkstackv1.Run{Id: unpublishedRunID, Namespace: "default", OrkflowName: "broken",
		Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: engine.RunDefinitionCorruptCode},
		CreatedAt: timestamppb.Now(), FinishedAt: timestamppb.Now()}
	if err := archiver.Archive(context.Background(), run, unpublishedRunEvents(), unpublishedGap); err != nil {
		t.Fatal(err)
	}
	reader := unpublishedReader{&fakeReader{
		runs:   map[string]*orkstackv1.Run{unpublishedRunID: run},
		events: map[string][]*orkstackv1.Event{unpublishedRunID: unpublishedRunEvents()},
	}}
	svc := api.NewService(api.Config{Reader: reader, Archive: archiver})
	ctx := context.Background()

	reads := map[string]func(from int64) ([]*orkstackv1.Event, error){
		"live": func(from int64) ([]*orkstackv1.Event, error) {
			reader.mu.Lock()
			before := reader.streamCount
			reader.mu.Unlock()
			var sent []*orkstackv1.Event
			err := svc.StreamRunEvents(ctx, unpublishedRunID, "", from, func(e *orkstackv1.Event) error {
				sent = append(sent, e)
				return nil
			})
			// The read reached the system of record, never the archive
			// behind it, whose answer is the same sentence.
			reader.mu.Lock()
			defer reader.mu.Unlock()
			if reader.streamCount != before+1 {
				t.Fatalf("from %d: the live read streamed from the reader %d times, want once", from, reader.streamCount-before)
			}
			return sent, err
		},
		"archived": func(from int64) ([]*orkstackv1.Event, error) {
			return svc.ArchivedEvents(ctx, unpublishedRunID, from)
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			sent, err := read(0)
			var connectErr *connect.Error
			if connect.CodeOf(err) != connect.CodeDataLoss || !errors.As(err, &connectErr) ||
				connectErr.Message() != unpublishedSentence {
				t.Fatalf("from 0: err = %v, want DataLoss %q", err, unpublishedSentence)
			}
			if len(sent) != 0 {
				t.Fatalf("from 0: sent %d events before the gap, want none: the gap is the start", len(sent))
			}
			sent, err = read(1)
			if err != nil || len(sent) != 1 || sent[0].GetSequence() != 2 ||
				sent[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
				t.Fatalf("from 1: %v (%v), want RUN_FAILED at 2", sent, err)
			}
		})
	}
}

// TestUnpublishedEventsReason_NamesTheGapInEveryNotice: the engine and the
// evidence bundle carry one reason for numbers a run never published, and
// every notice built on it — the archive's error, a live read's error, the
// bundle's — says never published, never lost from object storage. A lost
// segment's notice keeps its own wording.
func TestUnpublishedEventsReason_NamesTheGapInEveryNotice(t *testing.T) {
	if engine.UnpublishedEventsReason != evidence.UnpublishedEventsReason {
		t.Fatalf("engine says %q, evidence %q: one reason, word for word",
			engine.UnpublishedEventsReason, evidence.UnpublishedEventsReason)
	}
	lost := engine.EventGap{First: 1201, Last: 2400, Reason: "segment object missing"}
	for _, tc := range []struct{ got, want string }{
		{(&engine.ArchivedEventsLostError{RunID: "r", Gaps: []engine.EventGap{unpublishedGap}}).Error(),
			"events 1–1 of run r were never published (the run's history holds no event under these numbers)"},
		{(&engine.ArchivedEventsLostError{RunID: "r", Gaps: []engine.EventGap{unpublishedGap, lost}}).Error(),
			"events 1201–2400 (segment object missing) of run r were lost from object storage before the run was archived; " +
				"events 1–1 of run r were never published (the run's history holds no event under these numbers)"},
		{(&engine.UnpublishedEventsError{RunID: "r", Gaps: []engine.EventGap{unpublishedGap}}).Error(),
			"events 1–1 of run r were never published (the run's history holds no event under these numbers)"},
		{evidence.EventGap{First: 1, Last: 1, Reason: evidence.UnpublishedEventsReason}.Sentence(),
			"events 1–1 of this run were never published (the run's history holds no event under these numbers)"},
		{evidence.EventGap{First: 1201, Last: 2400, Reason: "segment object missing"}.Sentence(),
			"events 1201–2400 of this run were lost from object storage before it was archived (segment object missing)"},
	} {
		if tc.got != tc.want {
			t.Errorf("notice = %q,\nwant     %q", tc.got, tc.want)
		}
	}
}

// TestEvidenceExport_AnUnpublishedStartIsSchemaTwo: the bundle of a run
// archived with its start as a gap is schema_version 2, as every bundle of a
// gapped archive already is (TestEvidenceExport_GappedArchiveIsSchemaTwoWithEventGaps),
// with the gap and its reason, and the offline verifier reports it.
func TestEvidenceExport_AnUnpublishedStartIsSchemaTwo(t *testing.T) {
	f := newEvidenceFixture(t)
	run := &orkstackv1.Run{Id: "ork-ev-unpublished", OrkflowName: "evidence", OrkflowVersion: "0.1.0",
		OrkflowDefinitionDigest: f.digest, Status: orkstackv1.Status_STATUS_FAILED,
		Error:     &orkstackv1.Error{Code: engine.RunDefinitionCorruptCode},
		CreatedAt: timestamppb.New(evidenceNow.Add(-time.Hour)), FinishedAt: timestamppb.New(evidenceNow.Add(-30 * time.Minute))}
	f.reader.setRun(run)
	if err := f.archiver.Archive(context.Background(), run, []*orkstackv1.Event{
		{RunId: "ork-ev-unpublished", Sequence: 2, Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED},
	}, unpublishedGap); err != nil {
		t.Fatal(err)
	}
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-unpublished"})
	m := r.manifest
	want := evidence.EventGap{First: 1, Last: 1, Reason: evidence.UnpublishedEventsReason}
	if m.SchemaVersion != evidence.SchemaVersionEventGaps || len(m.EventGaps) != 1 || m.EventGaps[0] != want {
		t.Fatalf("manifest schema %d gaps %+v, want 2 with %+v", m.SchemaVersion, m.EventGaps, want)
	}
	if report := verifyBundle(t, r.data); len(report.EventGaps) != 1 || report.EventGaps[0] != want {
		t.Fatalf("Verify reported gaps %+v, want %+v", report.EventGaps, want)
	}
}

// TestWebSocketBridge_ClosesWithTheGapItReached: the bridge ends a socket
// whose stream reached a gap with a close that says which events and why,
// within a close frame's 123 bytes, never a bare "event stream failed": a
// run that never published 1, read live from the start, and a run that lost
// 1201–2400 from object storage, read from its archive.
func TestWebSocketBridge_ClosesWithTheGapItReached(t *testing.T) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	run := &orkstackv1.Run{Id: unpublishedRunID, Namespace: "default", OrkflowName: "broken",
		Status: orkstackv1.Status_STATUS_FAILED, CreatedAt: timestamppb.Now(), FinishedAt: timestamppb.Now()}
	live := api.NewService(api.Config{Reader: unpublishedReader{&fakeReader{
		runs:   map[string]*orkstackv1.Run{unpublishedRunID: run},
		events: map[string][]*orkstackv1.Event{unpublishedRunID: unpublishedRunEvents()},
	}}, Archive: engine.NewRunArchiver(store)})
	archived := api.NewService(api.Config{Archive: gappedArchive(t)})
	for _, tc := range []struct {
		name   string
		svc    *api.Service
		path   string
		frames int
		reason string
	}{
		{"never published, live", live, "/ws/runs/" + unpublishedRunID + "?from_sequence=0", 0,
			"events 1–1 of this run were never published; its outcome and other events are intact"},
		{"lost, from the archive", archived, "/ws/runs/ork-gap?from_sequence=1000", 200,
			"events 1201–2400 of this run were lost from object storage before the run was archived"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(api.NewMux(tc.svc, api.MuxOptions{}))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+tc.path, nil)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.CloseNow()
			frames := 0
			for {
				if _, _, err = conn.Read(ctx); err != nil {
					break
				}
				frames++
			}
			var closed websocket.CloseError
			if !errors.As(err, &closed) || closed.Code != websocket.StatusInternalError || closed.Reason != tc.reason {
				t.Fatalf("the socket ended %v, want a close %d %q", err, websocket.StatusInternalError, tc.reason)
			}
			if len(closed.Reason) > 123 || frames != tc.frames {
				t.Fatalf("close reason of %d bytes after %d frames, want at most 123 after %d", len(closed.Reason), frames, tc.frames)
			}
		})
	}
}

// noTerminalReader is the system of record for a run that published no
// terminal event (engine.TemporalRunReader.PublishedTerminalEvent).
type noTerminalReader struct{ *fakeReader }

func (noTerminalReader) PublishedTerminalEvent(context.Context, string) (bool, error) {
	return false, nil
}

// TestEvidenceExport_ARunNeverArchivedSaysSo: a finished run with no archive
// body is "not yet archived; retry shortly" only while an archive can still
// come. A run whose history shows it published no terminal event — its
// ending's snapshot refused, or terminated — never gets one, and the export
// says that instead of asking for a retry that cannot help.
func TestEvidenceExport_ARunNeverArchivedSaysSo(t *testing.T) {
	f := newEvidenceFixture(t, withConfig(func(cfg *api.Config) {
		cfg.Reader = noTerminalReader{cfg.Reader.(*fakeReader)}
	}))
	f.reader.setRun(&orkstackv1.Run{Id: "ork-never", OrkflowName: "evidence", Status: orkstackv1.Status_STATUS_FAILED})
	r := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-never"})
	if connect.CodeOf(r.err) != connect.CodeFailedPrecondition ||
		!strings.Contains(r.err.Error(), `run "ork-never" has no archive and will never have one: it published no terminal event`) ||
		strings.Contains(r.err.Error(), "retry") {
		t.Fatalf("export = %v, want FailedPrecondition saying the run is never archived, with no retry", r.err)
	}
}
