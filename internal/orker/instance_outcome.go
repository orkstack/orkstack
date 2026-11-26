package orker

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// healthyAfter is how long an instance must have been ready for its failure
	// to start its back-off afresh.
	healthyAfter = 10 * time.Minute
	// handBackDelay is how soon a task handed back is offered again.
	handBackDelay = time.Second
	// endingSentence is what the cluster's history holds of a failure beside its
	// reason: what a tenant wrote and where a host keeps things stay in the
	// Orker's log and the attempt's record.
	endingSentence = "this attempt of an event source's instance ended; its Orker's log and its record say why"
)

// instanceEnding is how an attempt of an instance ended, as its Orker saw it.
type instanceEnding struct {
	instanceFailure
	// Cancelled: the cluster asked for its end, and nothing else ended it.
	Cancelled bool
	// Rotated: it reached the bound of its age.
	Rotated bool
	// ReadyFor is how long it had been ready when it ended, none if it was not.
	ReadyFor time.Duration
	// Unread is spec or record (sourcehost.JudgmentEnd).
	Unread string
	// RefusedSince is when a provider began to refuse its credential, and
	// PastBound that it has for longer than its bound.
	RefusedSince time.Time
	PastBound    bool
	// FreshRead: an identity's refusal was confirmed by a read made after the
	// request arrived.
	FreshRead bool
}

// instanceOutcome is what the attempt returns to the cluster: an error it
// retries, never one it does not, or what the activity completes with. Which, is
// the table's; whether a completion pauses a source is the workflow's.
func instanceOutcome(in *orkstackv1.HostEventSourceInput, attempt int32, e instanceEnding) (*orkstackv1.HostEventSourceResult, error) {
	res := &orkstackv1.HostEventSourceResult{Reason: e.Reason, Cause: eventsources.InstanceCause(e.Cause), Attempt: attempt}
	if e.Rotated {
		res.End, res.Reason, res.Cause = orkstackv1.InstanceEnd_INSTANCE_END_ROTATED, "", ""
		res.Class = orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_HOST
		return res, nil
	}
	row, ok := eventsources.LookupInstanceEnd(e.Reason)
	if !ok {
		// A reason of this Orker's that the table does not hold decides
		// nothing: it is a failure that may pass, and the host's.
		row, _ = eventsources.LookupInstanceEnd(eventsources.ReasonServiceHostStalled)
	}
	res.Reason, res.Class, res.End = row.Reason, row.Class, row.Completes
	completes := row.Completes != orkstackv1.InstanceEnd_INSTANCE_END_UNSPECIFIED
	switch row.Condition {
	case eventsources.EndPastBound:
		if completes = e.PastBound && !e.RefusedSince.IsZero(); completes {
			res.PastBound, res.RefusedSince = true, timestamppb.New(e.RefusedSince)
		}
	case eventsources.EndUnread:
		if completes = e.Unread != ""; completes {
			res.Unread = e.Unread
		}
	case eventsources.EndFreshRead:
		res.FreshRead = e.FreshRead
	case eventsources.EndOnTrial:
		completes = completes && in.GetReadyWithinSeconds() > 0
	}
	if completes {
		return res, nil
	}
	retry := eventsources.InstanceRetry(row.Reason, res.Cause)
	if retry != eventsources.RetryNow && in.GetCompleteAfterHealthy() && e.ReadyFor >= healthyAfter {
		res.End = orkstackv1.InstanceEnd_INSTANCE_END_FAILED_AFTER_HEALTHY
		return res, nil
	}
	opts := temporal.ApplicationErrorOptions{}
	if retry == eventsources.RetryNow {
		opts.NextRetryDelay = handBackDelay
	}
	return nil, temporal.NewApplicationErrorWithOptions(endingSentence, row.Reason, opts)
}
