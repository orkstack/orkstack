package orker

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// TestInstanceOutcome: every reason of the table is returned as its row says, and
// nothing an Orker returns can stop the cluster's retries.
func TestInstanceOutcome(t *testing.T) {
	since := time.Date(2026, 11, 25, 1, 2, 3, 0, time.UTC)
	for _, row := range eventsources.InstanceEnds() {
		in := &orkstackv1.HostEventSourceInput{CompleteAfterHealthy: true, ReadyWithinSeconds: 60}
		e := instanceEnding{instanceFailure: instanceFailure{Reason: row.Reason, Detail: "why"}, Unread: "spec", PastBound: true, RefusedSince: since, FreshRead: true}
		res, err := instanceOutcome(in, 7, e)
		if row.Completes != orkstackv1.InstanceEnd_INSTANCE_END_UNSPECIFIED {
			if err != nil || res.GetEnd() != row.Completes || res.GetReason() != row.Reason || res.GetClass() != row.Class || res.GetAttempt() != 7 {
				t.Errorf("%s: %v, %v", row.Reason, res, err)
			}
			if res.GetUnread() != "" && row.Condition != eventsources.EndUnread || res.GetPastBound() && row.Condition != eventsources.EndPastBound ||
				res.GetFreshRead() && row.Condition != eventsources.EndFreshRead {
				t.Errorf("%s carries another's condition: %v", row.Reason, res)
			}
			continue
		}
		checkRetried(t, row.Reason, res, err, map[bool]time.Duration{true: time.Second}[row.Retried == eventsources.RetryNow])
		// After a healthy run only what is not a hand-back starts afresh.
		e.ReadyFor = 10 * time.Minute
		res, err = instanceOutcome(in, 7, e)
		if row.Retried == eventsources.RetryNow {
			checkRetried(t, row.Reason, res, err, time.Second)
		} else if err != nil || res.GetEnd() != orkstackv1.InstanceEnd_INSTANCE_END_FAILED_AFTER_HEALTHY || res.GetReason() != row.Reason || res.GetClass() != row.Class {
			t.Errorf("%s after a healthy run: %v, %v", row.Reason, res, err)
		}
		e.ReadyFor -= time.Second
		res, err = instanceOutcome(in, 7, e)
		checkRetried(t, row.Reason, res, err, map[bool]time.Duration{true: time.Second}[row.Retried == eventsources.RetryNow])
		e.ReadyFor, in.CompleteAfterHealthy = time.Hour, false
		res, err = instanceOutcome(in, 7, e)
		checkRetried(t, row.Reason, res, err, map[bool]time.Duration{true: time.Second}[row.Retried == eventsources.RetryNow])
	}
}

func checkRetried(t *testing.T, name string, res *orkstackv1.HostEventSourceResult, err error, next time.Duration) {
	t.Helper()
	var app *temporal.ApplicationError
	if res != nil || !errors.As(err, &app) || app.Type() != name || app.NonRetryable() || app.NextRetryDelay() != next {
		t.Errorf("%s: %v, %v; want it retried after %v", name, res, err, next)
	}
}

// TestInstanceOutcome_Conditions: a row whose condition does not hold is the
// failure that may pass, and one of no row decides nothing.
func TestInstanceOutcome_Conditions(t *testing.T) {
	in := &orkstackv1.HostEventSourceInput{}
	at := time.Now()
	for name, tc := range map[string]struct {
		e    instanceEnding
		in   *orkstackv1.HostEventSourceInput
		want string
		next time.Duration
	}{
		"an engine's judgment, not the source's own": {e: instanceEnding{instanceFailure: instanceFailure{Reason: "IDENTITY_UNAVAILABLE"}}, want: "IDENTITY_UNAVAILABLE"},
		"a credential refused, within its bound":     {e: instanceEnding{instanceFailure: instanceFailure{Reason: "CREDENTIAL_REFUSED"}, RefusedSince: at}, want: "CREDENTIAL_REFUSED"},
		"past a bound that began nowhere":            {e: instanceEnding{instanceFailure: instanceFailure{Reason: "CREDENTIAL_REFUSED"}, PastBound: true}, want: "CREDENTIAL_REFUSED"},
		"a host unfit":                               {e: instanceEnding{instanceFailure: instanceFailure{Reason: "SERVICE_HOST_UNFIT"}}, want: "SERVICE_HOST_UNFIT", next: time.Second},
		"a host unfit for identity waits":            {e: instanceEnding{instanceFailure: instanceFailure{Reason: "SERVICE_HOST_UNFIT", Cause: "IDENTITY_CLOCK_SKEW"}}, want: "SERVICE_HOST_UNFIT"},
		"a reason of no row":                         {e: instanceEnding{instanceFailure: instanceFailure{Reason: "SOMETHING_NEW", Detail: "x"}}, want: "SERVICE_HOST_STALLED"},
		"no reason":                                  {want: "SERVICE_HOST_STALLED"},
	} {
		res, err := instanceOutcome(in, 1, tc.e)
		checkRetried(t, tc.want, res, err, tc.next)
		if t.Failed() {
			t.Fatalf("%s", name)
		}
	}
	// Not ready in time is a replacement's alone: any other instance's attempt
	// never ends with it, and one that did would be no verdict on the source.
	if res, err := instanceOutcome(in, 1, instanceEnding{instanceFailure: instanceFailure{Reason: "SERVICE_NOT_READY_IN_TIME"}}); err == nil || res != nil {
		t.Errorf("not ready in time, on no trial: %v, %v", res, err)
	}
	res, err := instanceOutcome(in, 3, instanceEnding{Rotated: true, instanceFailure: instanceFailure{Reason: "SERVICE_FAILED", Cause: "CONFIG_INVALID"}})
	if err != nil || res.GetEnd() != orkstackv1.InstanceEnd_INSTANCE_END_ROTATED || res.GetReason() != "" || res.GetCause() != "" || res.GetAttempt() != 3 {
		t.Errorf("rotated: %v, %v", res, err)
	}
	res, _ = instanceOutcome(in, 1, instanceEnding{instanceFailure: instanceFailure{Reason: "SERVICE_CONFIG_REJECTED", Cause: "nats://user:pw@host"}})
	if res.GetCause() != "" {
		t.Errorf("a cause of no set was sent: %v", res)
	}
	// What a tenant wrote, and where this host keeps things, is the Orker's log's
	// and never the cluster's history's.
	_, err = instanceOutcome(in, 1, instanceEnding{instanceFailure: instanceFailure{Reason: "SERVICE_FAILED", Detail: "read /etc/pay/key"}})
	if strings.Contains(err.Error(), "/etc/pay") {
		t.Errorf("the failure says %q", err)
	}
}
