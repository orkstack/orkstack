package engine

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestResumePlanDigestOf: every field of a plan's record and block is either
// the plan, and changes its digest, or is of who confirmed it and how it was
// recorded, and does not. A new field must be put on one side.
func TestResumePlanDigestOf(t *testing.T) {
	base := func() *orkstackv1.ResumePlanRecord {
		return &orkstackv1.ResumePlanRecord{
			Format: 1, Namespace: "team", RunId: "ork-r", Scope: 10, N: 1, BaseExecutionRunId: "exec-1", RScheduledEventId: 7, RStartedEventId: 8,
			BaseTerminalSequence: 9, At: timestamppb.New(time.Unix(1, 0)), ExpiresAt: timestamppb.New(time.Unix(2, 0)), Principal: "ada",
			PrincipalVerified: true, Reason: "r", SupersededReason: "RUN_FAILED", PreviousScope: 3, RequestId: "q", PlanDigest: "d",
			Block: &orkstackv1.ResumeBlock{SeqBase: 10, Scope: 10, Action: orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM, FromTaskId: "a",
				Readback: readbackOf(defaultReadbackTunables()), ResumesBefore: 2, PreviousSeqBase: 5, ReopenedAwaits: []string{"w"}, Units: []byte{1}},
		}
	}
	want := ResumePlanDigestOf(base())
	plan := map[protoreflect.Name]bool{
		"namespace": true, "run_id": true, "base_execution_run_id": true, "r_scheduled_event_id": true, "r_started_event_id": true,
		"base_terminal_sequence": true, "block": true,
		"seq_base": true, "action": true, "from_task_id": true, "resumes_before": true, "reopened_awaits": true, "units": true,
	}
	clear := func(t *testing.T, rec *orkstackv1.ResumePlanRecord, m protoreflect.Message, fd protoreflect.FieldDescriptor) {
		t.Helper()
		if !m.Has(fd) {
			t.Fatalf("the fixture does not set %s", fd.FullName())
		}
		m.Clear(fd)
		if changed := ResumePlanDigestOf(rec) != want; changed != plan[fd.Name()] {
			t.Errorf("%s: clearing it changes the digest: %v, want %v", fd.FullName(), changed, plan[fd.Name()])
		}
	}
	fields := base().ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		rec := base()
		clear(t, rec, rec.ProtoReflect(), fields.Get(i))
	}
	fields = base().GetBlock().ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		rec := base()
		clear(t, rec, rec.GetBlock().ProtoReflect(), fields.Get(i))
	}
	// The default namespace is its name, however it is written.
	a, b := base(), base()
	a.Namespace, b.Namespace = "", "default"
	if ResumePlanDigestOf(a) != ResumePlanDigestOf(b) {
		t.Error("the default namespace has two digests")
	}
	if ResumePlanDigestOf(proto.Clone(base()).(*orkstackv1.ResumePlanRecord)) != want {
		t.Error("the digest is not stable")
	}
}
