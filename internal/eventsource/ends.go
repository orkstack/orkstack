package eventsource

import (
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// Classify judges how an attempt's activity completed, by the table and by
// nothing the Orker says beside it: an end whose reason the table does not
// hold, or holds for another class or another end, is another release's word
// and decides nothing but when the index is tried again.
func Classify(res *orkstackv1.HostEventSourceResult, onTrial bool, now time.Time, providerRefusedMax time.Duration) Result {
	out := Result{Kind: EndUnknown, Reason: res.GetReason(), Cause: eventsources.InstanceCause(res.GetCause())}
	if res.GetEnd() == orkstackv1.InstanceEnd_INSTANCE_END_ROTATED {
		return Result{Kind: EndRotated}
	}
	row, ok := eventsources.LookupInstanceEnd(res.GetReason())
	if !ok || row.Class != res.GetClass() {
		out.Reason = eventsources.SafeText(res.GetReason(), maxUnknownReason)
		return out
	}
	switch end := res.GetEnd(); {
	case end == orkstackv1.InstanceEnd_INSTANCE_END_FAILED_AFTER_HEALTHY:
		if row.Retried != eventsources.RetryNone {
			out.Kind = EndAfterHealthy
		}
	case end != row.Completes:
	case end == orkstackv1.InstanceEnd_INSTANCE_END_STOPPED:
		out.Kind = EndStopped
	case end == orkstackv1.InstanceEnd_INSTANCE_END_STUCK:
		out.Kind = EndStuck
	case end == orkstackv1.InstanceEnd_INSTANCE_END_JUDGMENT_UNAVAILABLE:
		out.Kind = EndUnmet
		if u := res.GetUnread(); u == unreadSpec || u == unreadRecord {
			out.Kind, out.Unread = EndJudgment, u
		}
	case end == orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE:
		out.Kind = EndUnmet
		if eventsources.EndIsUnchangeable(res, eventsources.EndFacts{OnTrial: onTrial, Now: now, ProviderRefusedMax: providerRefusedMax}) {
			out.Kind = EndUnchangeable
		}
	}
	return out
}

const (
	unreadSpec   = "spec"
	unreadRecord = "record"
	// maxUnknownReason bounds what is kept of a reason no row holds, which is
	// whatever took the task wrote.
	maxUnknownReason = 64
)
