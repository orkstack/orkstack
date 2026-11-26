package eventsources

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestInstanceEnds_TheTable: every reason an attempt of an instance ends with,
// once, with a class, and either retried by the cluster, completing, or both by
// its condition; and the rows the source's workflow may pause on are exactly the
// ones that name a condition's holder.
func TestInstanceEnds_TheTable(t *testing.T) {
	seen := map[string]bool{}
	var unchangeable []string
	for _, e := range InstanceEnds() {
		if seen[e.Reason] {
			t.Errorf("%s is listed twice", e.Reason)
		}
		seen[e.Reason] = true
		if e.Class != orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_HOST && e.Class != orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE {
			t.Errorf("%s has no class", e.Reason)
		}
		if e.Retried == RetryNone && e.Completes == orkstackv1.InstanceEnd_INSTANCE_END_UNSPECIFIED {
			t.Errorf("%s neither is retried nor completes", e.Reason)
		}
		if e.Completes == orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE {
			unchangeable = append(unchangeable, e.Reason)
			if e.Class != orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE {
				t.Errorf("%s can pause a source and is the host's", e.Reason)
			}
		}
		if got, ok := LookupInstanceEnd(e.Reason); !ok || got != e {
			t.Errorf("LookupInstanceEnd(%s) = %+v, %v", e.Reason, got, ok)
		}
	}
	slices.Sort(unchangeable)
	want := []string{"CREDENTIAL_REFUSED", "GRANT_NOT_PERMITTED", "IDENTITY_NOT_ADMITTED", "IMAGE_NOT_PERMITTED", "MASK_CAPACITY_EXCEEDED",
		"RESOURCES_UNSATISFIABLE", "SECRET_NOT_PERMITTED", "SERVICE_CONFIG_REJECTED", "SERVICE_CONTRACT_UNSUPPORTED",
		"SERVICE_CONTRACT_VIOLATION", "SERVICE_NOT_READY_IN_TIME", "SERVICE_PROFILE_UNSUPPORTED", "SOURCE_REFRESH_BEST_EFFORT",
		"SPOOL_UNSATISFIABLE"}
	if !slices.Equal(unchangeable, want) {
		t.Errorf("what trying again cannot change = %q; want %q", unchangeable, want)
	}
	// What the door stops an instance with is never the Orker's word alone, so
	// each is a stop, and one of a deleted source is the source's.
	for reason, class := range map[string]orkstackv1.InstanceEndClass{
		"EVENT_SOURCE_NOT_HELD": classHost, "EVENT_SOURCES_DISABLED": classHost, "EVENT_SOURCE_PAUSED": classSource,
		"EVENT_SOURCE_DELETING": classSource, "EVENT_SOURCE_DELETED_DOCUMENT_REAPPEARED": classSource,
	} {
		if e, _ := LookupInstanceEnd(reason); e.Completes != endStopped || e.Class != class || e.Retried != RetryNone {
			t.Errorf("%s: %+v", reason, e)
		}
	}
	for _, reason := range []string{"", "IDENTITY_REVOKED", "ORKER_LOST", "service_failed"} {
		if e, ok := LookupInstanceEnd(reason); ok {
			t.Errorf("LookupInstanceEnd(%q) = %+v; want none", reason, e)
		}
	}
}

// TestInstanceEnd_PausesOnlyOnItsCondition: what an Orker returns pauses a
// source only where its reason is one trying again cannot change, its class is
// the table's, its end is that one, and its condition holds. Everything else an
// Orker can say is no pause.
func TestInstanceEnd_PausesOnlyOnItsCondition(t *testing.T) {
	const (
		unchangeable = orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE
		source       = orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE
		host         = orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_HOST
	)
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	refused := func(past bool, ago time.Duration) *orkstackv1.HostEventSourceResult {
		return &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "CREDENTIAL_REFUSED", Class: source, PastBound: past, RefusedSince: timestamppb.New(now.Add(-ago))}
	}
	for name, tc := range map[string]struct {
		res     *orkstackv1.HostEventSourceResult
		onTrial bool
		want    bool
	}{
		"a rejected configuration":            {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "SERVICE_CONFIG_REJECTED", Class: source}, want: true},
		"another class":                       {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "SERVICE_CONFIG_REJECTED", Class: host}},
		"no class":                            {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "SERVICE_CONFIG_REJECTED"}},
		"another end":                         {res: &orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_STOPPED, Reason: "SERVICE_CONFIG_REJECTED", Class: source}},
		"an end of a later release":           {res: &orkstackv1.HostEventSourceResult{End: 99, Reason: "SERVICE_CONFIG_REJECTED", Class: source}},
		"a reason nobody lists":               {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "ORKER_SAYS_SO", Class: source}},
		"a host's reason":                     {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "SERVICE_HOST_STALLED", Class: host}},
		"a refused credential":                {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "CREDENTIAL_REFUSED", Class: source}},
		"a refused credential past its bound": {res: refused(true, time.Hour), want: true},
		"an Orker's word within the bound":    {res: refused(true, time.Hour-time.Second)},
		"past the bound, the Orker not":       {res: refused(false, time.Hour)},
		"past the bound since the future":     {res: refused(true, -time.Hour)},
		"past the bound since nothing":        {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "CREDENTIAL_REFUSED", Class: source, PastBound: true}},
		"not ready in time, on trial":         {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "SERVICE_NOT_READY_IN_TIME", Class: source}, onTrial: true, want: true},
		"not ready in time, not on trial":     {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "SERVICE_NOT_READY_IN_TIME", Class: source}},
		"not admitted, freshly read":          {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "IDENTITY_NOT_ADMITTED", Class: source, FreshRead: true}, want: true},
		"not admitted, read before":           {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "IDENTITY_NOT_ADMITTED", Class: source}},
		"secrets no mask holds":               {res: &orkstackv1.HostEventSourceResult{End: unchangeable, Reason: "MASK_CAPACITY_EXCEEDED", Class: source}, want: true},
		"nothing":                             {},
	} {
		if got := EndIsUnchangeable(tc.res, EndFacts{OnTrial: tc.onTrial, Now: now, ProviderRefusedMax: time.Hour}); got != tc.want {
			t.Errorf("%s: EndIsUnchangeable = %v, want %v", name, got, tc.want)
		}
	}
}

// TestInstanceCause: the finer code behind a reason is sent only from a closed
// set; what an image or a later release says is sent empty.
func TestInstanceCause(t *testing.T) {
	for _, code := range []string{"CONFIG_INVALID", "OBJECT_MISSING", "IDENTITY_CLOCK_SKEW", "IMAGE_NOT_SIGNED"} {
		if InstanceCause(code) != code {
			t.Errorf("InstanceCause(%q) = %q", code, InstanceCause(code))
		}
	}
	for _, code := range []string{"", "nats://user:pw@host", "config_invalid", "CONFIG_INVALID\n"} {
		if got := InstanceCause(code); got != "" {
			t.Errorf("InstanceCause(%q) = %q, want nothing", code, got)
		}
	}
}

// TestInstanceEnd_RetriedFor: a host unfit for identity is offered the task
// again as soon as it hands it back, so that one waits the back-off; any other
// hand-back goes on to another host at once, and nothing else is retried so.
func TestInstanceEnd_RetriedFor(t *testing.T) {
	for _, tc := range []struct {
		reason, cause string
		want          Retry
	}{
		{"SERVICE_HOST_UNFIT", "", RetryNow}, {"SERVICE_HOST_UNFIT", "SOME_LATER_CAUSE", RetryNow},
		{"SERVICE_HOST_UNFIT", "IDENTITY_NOT_CONFIGURED", RetryBackoff}, {"SERVICE_HOST_UNFIT", "IDENTITY_CLOCK_SKEW", RetryBackoff},
		{"SERVICE_HOST_UNFIT", "ORKER_STORE_UNSCOPED", RetryBackoff}, {"SERVICE_HOST_UNFIT", "IDENTITY_SPEC_SKEW", RetryBackoff},
		{"ORKER_STOPPED", "IDENTITY_CLOCK_SKEW", RetryNow}, {"SERVICE_FAILED", "", RetryBackoff},
		{"SERVICE_STOP_STUCK", "", RetryNone}, {"NOBODYS", "", RetryNone},
	} {
		if got := InstanceRetry(tc.reason, tc.cause); got != tc.want {
			t.Errorf("InstanceRetry(%s, %q) = %v, want %v", tc.reason, tc.cause, got, tc.want)
		}
	}
}
