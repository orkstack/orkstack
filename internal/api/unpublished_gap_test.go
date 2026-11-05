package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
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
// (TestTemporalRunReader_AnEndingBeforeStartIsServedAndArchivedWithItsGap): a
// stream read from below 2 is told the numbers the run never published, and
// goes on.
type unpublishedReader struct{ *fakeReader }

func (r unpublishedReader) StreamEvents(ctx context.Context, runID string, fromSequence int64,
	send func(*orkstackv1.Event) error,
) error {
	if runID == unpublishedRunID && fromSequence < unpublishedGap.Last {
		if err := send(engine.StreamGapEvent(runID, unpublishedGap.First, unpublishedGap.Last, nil)); err != nil {
			return err
		}
	}
	return r.fakeReader.StreamEvents(ctx, runID, fromSequence, send)
}

func unpublishedRun() *orkstackv1.Run {
	return &orkstackv1.Run{Id: unpublishedRunID, Namespace: "default", OrkflowName: "broken",
		Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: engine.RunDefinitionCorruptCode},
		CreatedAt: timestamppb.Now(), FinishedAt: timestamppb.Now()}
}

func newUnpublishedReader() unpublishedReader {
	return unpublishedReader{&fakeReader{
		runs:   map[string]*orkstackv1.Run{unpublishedRunID: unpublishedRun()},
		events: map[string][]*orkstackv1.Event{unpublishedRunID: unpublishedRunEvents()},
	}}
}

// sequencesAndTypes renders a stream as "2:RUN_FAILED", one for each event.
func sequencesAndTypes(events []*orkstackv1.Event) string {
	parts := make([]string, 0, len(events))
	for _, e := range events {
		parts = append(parts, fmt.Sprintf("%d:%s", e.GetSequence(), strings.TrimPrefix(e.GetType().String(), "EVENT_TYPE_")))
	}
	return strings.Join(parts, " ")
}

// TestSkippedNumbers_EveryLiveReadShowsThemAndEveryArchivedReadEndsDataLoss:
// the run's stream read live — the Service, the StreamEvents RPC and the
// WebSocket bridge alike — names the numbers the run never published and
// reaches the run's terminal event, with a clean end. Read from its archive,
// the same numbers are the existing DataLoss, with nothing sent before a gap
// at the start. From past the numbers, every read serves the terminal event.
func TestSkippedNumbers_EveryLiveReadShowsThemAndEveryArchivedReadEndsDataLoss(t *testing.T) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	if err := archiver.Archive(context.Background(), unpublishedRun(), unpublishedRunEvents(), unpublishedGap); err != nil {
		t.Fatal(err)
	}
	svc := api.NewService(api.Config{Reader: newUnpublishedReader(), Archive: archiver})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	t.Cleanup(server.Close)
	client := orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)

	const fromStart, pastTheNumbers = "1:STREAM_GAP 2:RUN_FAILED", "2:RUN_FAILED"
	for name, read := range map[string]func(from int64) ([]*orkstackv1.Event, error){
		"the service": func(from int64) ([]*orkstackv1.Event, error) {
			var sent []*orkstackv1.Event
			err := svc.StreamRunEvents(ctx, unpublishedRunID, "", from, func(e *orkstackv1.Event) error {
				sent = append(sent, e)
				return nil
			})
			return sent, err
		},
		"the StreamEvents RPC": func(from int64) ([]*orkstackv1.Event, error) {
			stream, err := client.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
				RunId: unpublishedRunID, FromSequence: from}))
			if err != nil {
				return nil, err
			}
			defer stream.Close()
			var sent []*orkstackv1.Event
			for stream.Receive() {
				sent = append(sent, stream.Msg())
			}
			return sent, stream.Err()
		},
		"the WebSocket bridge": func(from int64) ([]*orkstackv1.Event, error) {
			url := fmt.Sprintf("ws%s/ws/runs/%s?from_sequence=%d", strings.TrimPrefix(server.URL, "http"), unpublishedRunID, from)
			conn, _, err := websocket.Dial(ctx, url, nil)
			if err != nil {
				return nil, err
			}
			defer conn.CloseNow()
			var sent []*orkstackv1.Event
			for {
				_, frame, err := conn.Read(ctx)
				if err != nil {
					var closed websocket.CloseError
					if !errors.As(err, &closed) || closed.Code != websocket.StatusNormalClosure || closed.Reason != "run finished" {
						return sent, fmt.Errorf("the socket ended %w, want a normal close at the run's end", err)
					}
					return sent, nil
				}
				var decoded struct {
					Sequence string `json:"sequence"`
					Type     string `json:"type"`
					Message  string `json:"message"`
				}
				if err := json.Unmarshal(frame, &decoded); err != nil {
					return sent, fmt.Errorf("frame %q: %w", frame, err)
				}
				sequence, err := strconv.ParseInt(decoded.Sequence, 10, 64)
				if err != nil {
					return sent, fmt.Errorf("frame %q: %w", frame, err)
				}
				sent = append(sent, &orkstackv1.Event{Sequence: sequence, Message: decoded.Message,
					Type: orkstackv1.EventType(orkstackv1.EventType_value["EVENT_TYPE_"+decoded.Type])})
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			sent, err := read(0)
			if err != nil || sequencesAndTypes(sent) != fromStart {
				t.Fatalf("from 0: %s (%v), want %s", sequencesAndTypes(sent), err, fromStart)
			}
			if !strings.HasPrefix(sent[0].GetMessage(), "events 1–1 of this run were never published") {
				t.Fatalf("the gap event says %q", sent[0].GetMessage())
			}
			sent, err = read(1)
			if err != nil || sequencesAndTypes(sent) != pastTheNumbers {
				t.Fatalf("from 1: %s (%v), want %s", sequencesAndTypes(sent), err, pastTheNumbers)
			}
		})
	}

	t.Run("the archive", func(t *testing.T) {
		sent, err := svc.ArchivedEvents(ctx, unpublishedRunID, 0)
		var connectErr *connect.Error
		if connect.CodeOf(err) != connect.CodeDataLoss || !errors.As(err, &connectErr) || connectErr.Message() != unpublishedSentence {
			t.Fatalf("from 0: err = %v, want DataLoss %q", err, unpublishedSentence)
		}
		if len(sent) != 0 {
			t.Fatalf("from 0: sent %d events before the gap, want none: the gap is the start", len(sent))
		}
		sent, err = svc.ArchivedEvents(ctx, unpublishedRunID, 1)
		if err != nil || sequencesAndTypes(sent) != pastTheNumbers {
			t.Fatalf("from 1: %s (%v), want %s", sequencesAndTypes(sent), err, pastTheNumbers)
		}
	})
}

// TestRetiredRun_AnyReplicaReadsItFromTheArchive: a run the cluster no longer
// holds, read through a service that never saw it, answers from the shared
// archive: its outcome and its stream. That is how a run archived from its
// history is read once it is retired.
func TestRetiredRun_AnyReplicaReadsItFromTheArchive(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	run := &orkstackv1.Run{Id: "ork-retired", Namespace: "default", OrkflowName: "hello", Status: orkstackv1.Status_STATUS_CANCELED,
		Error: &orkstackv1.Error{Code: "RUN_TERMINATED"}, CreatedAt: timestamppb.Now(), FinishedAt: timestamppb.Now()}
	if err := archiver.Archive(ctx, run, []*orkstackv1.Event{
		{RunId: "ork-retired", Sequence: 1, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED},
		{RunId: "ork-retired", Sequence: 2, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED},
	}); err != nil {
		t.Fatal(err)
	}
	// The cluster answers nothing for it: the fake holds no runs.
	replica := api.NewService(api.Config{Reader: &fakeReader{runs: map[string]*orkstackv1.Run{}, events: map[string][]*orkstackv1.Event{}},
		Archive: engine.NewRunArchiver(store)})
	got, err := replica.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "ork-retired"}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_CANCELED || got.Msg.GetRun().GetError().GetCode() != "RUN_TERMINATED" {
		t.Fatalf("GetRun = %v", got.Msg.GetRun())
	}
	var sent []*orkstackv1.Event
	if err := replica.StreamRunEvents(ctx, "ork-retired", "", 0, func(e *orkstackv1.Event) error {
		sent = append(sent, e)
		return nil
	}); err != nil {
		t.Fatalf("StreamRunEvents: %v", err)
	}
	if got := sequencesAndTypes(sent); got != "1:RUN_CREATED 2:RUN_CANCELED" {
		t.Fatalf("stream = %s", got)
	}
}

// TestUnpublishedEventsReason_NamesTheGapInEveryNotice: the engine and the
// evidence bundle carry one reason for numbers a run never published, and
// every notice the engine builds on it — the archive's error and a live
// read's gap event — says never published, never lost from object storage. A
// lost segment's notice keeps its own wording. The bundle's notice
// (evidence.EventGap.Sentence) is held to the same by the evidence package's
// TestEventGapSentence_NamesTheCause, which is where that sentence lives.
func TestUnpublishedEventsReason_NamesTheGapInEveryNotice(t *testing.T) {
	if engine.UnpublishedEventsReason != evidence.UnpublishedEventsReason {
		t.Fatalf("engine says %q, evidence %q: one reason, word for word",
			engine.UnpublishedEventsReason, evidence.UnpublishedEventsReason)
	}
	lost := engine.EventGap{First: 1201, Last: 2400, Reason: "segment object missing"}
	for _, tc := range []struct{ got, want string }{
		{engine.StreamGapEvent("r", 1, 1, nil).GetMessage(),
			"events 1–1 of this run were never published (the run's history holds no event under these numbers)"},
		{(&engine.ArchivedEventsLostError{RunID: "r", Gaps: []engine.EventGap{unpublishedGap}}).Error(),
			"events 1–1 of run r were never published (the run's history holds no event under these numbers)"},
		{(&engine.ArchivedEventsLostError{RunID: "r", Gaps: []engine.EventGap{unpublishedGap, lost}}).Error(),
			"events 1201–2400 (segment object missing) of run r were lost from object storage before the run was archived; " +
				"events 1–1 of run r were never published (the run's history holds no event under these numbers)"},
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

	// The archive body in the bundle records the same range as a line of its
	// own, between the run's snapshot and the one event it kept: the gap is
	// in the stream a reader replays, not only in the manifest.
	var kinds []string
	var gapLines []engine.EventGap
	for _, line := range bytes.Split(bytes.TrimSpace(tarMembers(t, r.data)["run.jsonl"]), []byte("\n")) {
		var parsed struct {
			Kind string           `json:"kind"`
			Gap  *engine.EventGap `json:"gap"`
		}
		if err := json.Unmarshal(line, &parsed); err != nil {
			t.Fatalf("run.jsonl line %q: %v", line, err)
		}
		kinds = append(kinds, parsed.Kind)
		if parsed.Gap != nil {
			gapLines = append(gapLines, *parsed.Gap)
		}
	}
	if !slices.Equal(kinds, []string{engine.ArchiveKindRun, engine.ArchiveKindGap, engine.ArchiveKindEvent}) ||
		len(gapLines) != 1 || gapLines[0] != unpublishedGap {
		t.Fatalf("run.jsonl lines are %v with gap lines %+v, want run, gap, event with the gap %+v", kinds, gapLines, unpublishedGap)
	}
}

// TestWebSocketBridge_ClosesWithTheGapItReached: the bridge ends a socket
// whose stream reached a gap in an archived run with a close that says which
// events and why, within a close frame's 123 bytes, never a bare "event stream
// failed": a run that never published 1 and a run that lost 1201–2400 from
// object storage, both read from their archives.
func TestWebSocketBridge_ClosesWithTheGapItReached(t *testing.T) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	unpublished := engine.NewRunArchiver(store)
	if err := unpublished.Archive(context.Background(), unpublishedRun(), unpublishedRunEvents(), unpublishedGap); err != nil {
		t.Fatal(err)
	}
	neverPublished := api.NewService(api.Config{Archive: unpublished})
	archived := api.NewService(api.Config{Archive: gappedArchive(t)})
	for _, tc := range []struct {
		name   string
		svc    *api.Service
		path   string
		frames int
		reason string
	}{
		{"never published, from the archive", neverPublished, "/ws/runs/" + unpublishedRunID + "?from_sequence=0", 0,
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

// TestEvidenceExport_ARunNotYetArchivedAsksForARetry: a finished run with no
// archive body will have one: its own terminal publish writes it, or the sweep
// does from its history. Until then the export asks for a retry and says when
// one can help.
func TestEvidenceExport_ARunNotYetArchivedAsksForARetry(t *testing.T) {
	f := newEvidenceFixture(t)
	f.reader.setRun(&orkstackv1.Run{Id: "ork-pending", OrkflowName: "evidence", Status: orkstackv1.Status_STATUS_FAILED})
	r := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-pending"})
	if connect.CodeOf(r.err) != connect.CodeFailedPrecondition ||
		!strings.Contains(r.err.Error(), `run "ork-pending" is finished but not yet archived; retry shortly`) ||
		!strings.Contains(r.err.Error(), "within the cleanup interval") {
		t.Fatalf("export = %v, want FailedPrecondition asking for a retry", r.err)
	}
}
