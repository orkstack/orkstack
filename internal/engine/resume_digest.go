package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ResumePlanDigestOf names the plan rec holds: the base it starts from and
// what it does with each unit. It is what a preview shows and a confirm sends
// back, so nothing of who asked, when, or under which number it was recorded
// is in it, nor the read-back's bounds, which a release may change. The reset
// run computes it again from the record it loaded: what the record says of
// itself is never trusted. Every field is hashed by name, so two releases
// agree on it whatever their encoders do.
func ResumePlanDigestOf(rec *orkstackv1.ResumePlanRecord) string {
	h := sha256.New()
	write := func(name string, value any) { fmt.Fprintf(h, "%s=%q\n", name, fmt.Sprint(value)) }
	b := rec.GetBlock()
	write("format", 1)
	write("namespace", artifacts.ResolveNamespace(rec.GetNamespace()))
	write("run_id", rec.GetRunId())
	write("base_execution_run_id", rec.GetBaseExecutionRunId())
	write("base_terminal_sequence", rec.GetBaseTerminalSequence())
	write("r_scheduled_event_id", rec.GetRScheduledEventId())
	write("r_started_event_id", rec.GetRStartedEventId())
	write("seq_base", b.GetSeqBase())
	write("action", b.GetAction().String())
	write("from_task_id", b.GetFromTaskId())
	write("resumes_before", b.GetResumesBefore())
	write("units", hex.EncodeToString(b.GetUnits()))
	for _, id := range b.GetReopenedAwaits() {
		write("reopened_await", id)
	}
	return hex.EncodeToString(h.Sum(nil))
}
