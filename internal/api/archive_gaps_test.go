package api_test

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// gappedArchive is an archive holding run ork-gap, whose events 1201–2400
// were lost from object storage before it was archived: events 1–1200 and
// 2401–2402 are in the body, with a gap line between them.
func gappedArchive(t *testing.T) *engine.RunArchiver {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	run := &orkstackv1.Run{Id: "ork-gap", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.Now(), FinishedAt: timestamppb.Now()}
	var events []*orkstackv1.Event
	for seq := int64(1); seq <= 2402; seq++ {
		if seq > 1200 && seq <= 2400 {
			continue
		}
		events = append(events, &orkstackv1.Event{RunId: "ork-gap", Sequence: seq, Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG})
	}
	events[len(events)-1].Type = orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED
	if err := archiver.Archive(context.Background(), run, events,
		engine.EventGap{First: 1201, Last: 2400, Reason: "segment object missing"}); err != nil {
		t.Fatal(err)
	}
	return archiver
}

const lostSentence = "events 1201–2400 of run ork-gap were lost from object storage before the run was archived " +
	"(segment object missing); the run's outcome and its other events are intact"

func TestArchivedEvents_OverAGapIsDataLossNamingTheRange(t *testing.T) {
	svc := api.NewService(api.Config{Archive: gappedArchive(t)})
	ctx := context.Background()
	for _, from := range []int64{0, 1200, 1500, 2399} {
		_, err := svc.ArchivedEvents(ctx, "ork-gap", from)
		if connect.CodeOf(err) != connect.CodeDataLoss {
			t.Fatalf("from %d: err = %v, want DataLoss", from, err)
		}
		var connectErr *connect.Error
		if !errors.As(err, &connectErr) || connectErr.Message() != lostSentence {
			t.Fatalf("from %d: message = %q, want %q", from, connectErr.Message(), lostSentence)
		}
	}
	// A range entirely after the gap is whole, and answered.
	events, err := svc.ArchivedEvents(ctx, "ork-gap", 2400)
	if err != nil || len(events) != 2 || events[0].GetSequence() != 2401 {
		t.Fatalf("after the gap = %d events, %v; want 2401–2402", len(events), err)
	}
}

func TestReplayArchivedEvents_SendsEventsBeforeTheGapThenFails(t *testing.T) {
	svc := api.NewService(api.Config{Archive: gappedArchive(t)})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	t.Cleanup(server.Close)
	client := orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)
	stream, err := client.StreamEvents(context.Background(),
		connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: "ork-gap", FromSequence: 1000}))
	if err != nil {
		t.Fatal(err)
	}
	var sent []int64
	for stream.Receive() {
		sent = append(sent, stream.Msg().GetSequence())
	}
	if err := stream.Err(); connect.CodeOf(err) != connect.CodeDataLoss {
		t.Fatalf("stream ended with %v, want DataLoss after the events before the gap", err)
	}
	if len(sent) != 200 || sent[0] != 1001 || sent[len(sent)-1] != 1200 {
		t.Fatalf("sent %d events, want 1001–1200", len(sent))
	}
}

func TestEvidenceExport_GappedArchiveIsSchemaTwoWithEventGaps(t *testing.T) {
	f := newEvidenceFixture(t)
	run := &orkstackv1.Run{Id: "ork-ev-gap", OrkflowName: "evidence", OrkflowVersion: "0.1.0",
		OrkflowDefinitionDigest: f.digest, Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(evidenceNow.Add(-time.Hour)), FinishedAt: timestamppb.New(evidenceNow.Add(-30*time.Minute))}
	f.reader.setRun(run)
	if err := f.archiver.Archive(context.Background(), run, []*orkstackv1.Event{
		{RunId: "ork-ev-gap", Sequence: 1, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED},
		{RunId: "ork-ev-gap", Sequence: 9, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED},
	}, engine.EventGap{First: 2, Last: 8, Reason: "segment object missing"}); err != nil {
		t.Fatal(err)
	}
	body, err := f.archiver.LoadRunBody(context.Background(), "ork-ev-gap")
	if err != nil {
		t.Fatal(err)
	}
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-gap"})
	m := r.manifest
	if m.SchemaVersion != evidence.SchemaVersionEventGaps || len(m.EventGaps) != 1 ||
		m.EventGaps[0] != (evidence.EventGap{First: 2, Last: 8, Reason: "segment object missing"}) {
		t.Fatalf("manifest schema %d gaps %+v, want 2 with 2–8", m.SchemaVersion, m.EventGaps)
	}
	if run := memberAt(t, m, "run.jsonl"); run.State != evidence.StatePresent {
		t.Fatalf("run.jsonl = %+v, want present", run)
	}
	if entries := tarMembers(t, r.data); !bytes.Equal(entries["run.jsonl"], body) {
		t.Fatal("run.jsonl is not the archive body verbatim")
	}
	report := verifyBundle(t, r.data)
	if len(report.EventGaps) != 1 {
		t.Fatalf("Verify reported gaps %+v", report.EventGaps)
	}
}

func TestEvidenceExport_UngappedArchiveStaysSchemaOne(t *testing.T) {
	f := newEvidenceFixture(t)
	f.succeeded("ork-ev-whole")
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-whole"})
	if r.manifest.SchemaVersion != evidence.SchemaVersion || r.manifest.EventGaps != nil {
		t.Fatalf("schema %d gaps %+v, want 1 and none", r.manifest.SchemaVersion, r.manifest.EventGaps)
	}
	document, err := r.manifest.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(document, []byte("event_gaps")) {
		t.Fatalf("an ungapped manifest names event_gaps:\n%s", document)
	}
}
