package engine

import (
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestResumeHolder_EntryNamesWhoForceCanceled: the resume a run's snapshots
// list says who force-cancelled the run it set aside, as its plan's record
// keeps them.
func TestResumeHolder_EntryNamesWhoForceCanceled(t *testing.T) {
	by := &orkstackv1.ResumeForceCanceledBy{Principal: "user:corp:alice", PrincipalVerified: true, RequestId: "fc-1"}
	h := &resumeHolder{record: &orkstackv1.ResumePlanRecord{SupersededReason: forceCanceledReason, ForceCanceledBy: by,
		Block: &orkstackv1.ResumeBlock{SeqBase: 10}}}
	e := h.entry()
	if e.GetFromState() != orkstackv1.ResumeFromState_RESUME_FROM_STATE_FORCE_CANCELED || !proto.Equal(e.GetForceCanceledBy(), by) {
		t.Fatalf("entry = %v, want it force-cancelled by %v", e, by)
	}
}
