package engine

import (
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// What SignalPause sends (a RunPauseSignal through the default data
// converter) is what the run decodes: every field arrives.
func TestParsePauseRequest_ReadsWhatSignalPauseSends(t *testing.T) {
	expires := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	sent := &orkstackv1.RunPauseSignal{
		Format: 1, Pause: true, Principal: "alice", Reason: "db maintenance", TimeoutSeconds: 3600,
		RequestId: "req-1", Inherited: true, ParentRunId: "ork-p", CascadeSeq: 7,
		OriginExpiresAt: timestamppb.New(expires), Depth: 2,
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(sent)
	must.OKf(t, err, "encode")
	got, err := parsePauseRequest(payload.GetData())
	must.OKf(t, err, "decode")
	want := pauseRequest{
		Format: 1, Pause: true, Principal: "alice", Reason: "db maintenance", TimeoutSeconds: 3600,
		RequestID: "req-1", Inherited: true, ParentRunID: "ork-p", CascadeSeq: 7,
		OriginExpiresAt: "2026-11-19T10:00:00Z", Depth: 2,
	}
	if *got != want {
		t.Fatalf("decoded %+v, want %+v", *got, want)
	}
}
