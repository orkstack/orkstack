package eventsource

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// TestClassify: every row of the table is judged as its row says, under its
// condition alone, and nothing that is not a row's is ever one that pauses.
func TestClassify(t *testing.T) {
	now := epoch0
	const refusedMax = time.Hour
	kinds := map[orkstackv1.InstanceEnd]EndKind{
		orkstackv1.InstanceEnd_INSTANCE_END_STOPPED: EndStopped, orkstackv1.InstanceEnd_INSTANCE_END_STUCK: EndStuck,
		orkstackv1.InstanceEnd_INSTANCE_END_JUDGMENT_UNAVAILABLE: EndJudgment, orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE: EndUnchangeable,
	}
	rows := eventsources.InstanceEnds()
	if len(rows) < 30 {
		t.Fatalf("%d rows", len(rows))
	}
	for _, row := range rows {
		met := &orkstackv1.HostEventSourceResult{End: row.Completes, Reason: row.Reason, Class: row.Class}
		onTrial := row.Condition == eventsources.EndOnTrial
		switch row.Condition {
		case eventsources.EndPastBound:
			met.PastBound, met.RefusedSince = true, timestamppb.New(now.Add(-2*refusedMax))
		case eventsources.EndFreshRead:
			met.FreshRead = true
		case eventsources.EndUnread:
			met.Unread = "record"
		}
		if row.Completes != orkstackv1.InstanceEnd_INSTANCE_END_UNSPECIFIED {
			if got := Classify(met, onTrial, now, refusedMax); got.Kind != kinds[row.Completes] || got.Reason != row.Reason {
				t.Errorf("%s, its condition met: %+v", row.Reason, got)
			}
			// Its condition not met, it is a listed end that decides nothing.
			unmet := &orkstackv1.HostEventSourceResult{End: row.Completes, Reason: row.Reason, Class: row.Class}
			if row.Condition != eventsources.EndAlways {
				if got := Classify(unmet, false, now, refusedMax); got.Kind != EndUnmet {
					t.Errorf("%s, its condition not met: %+v", row.Reason, got)
				}
			}
		}
		after := &orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_FAILED_AFTER_HEALTHY, Reason: row.Reason, Class: row.Class}
		want := EndAfterHealthy
		if row.Retried == eventsources.RetryNone {
			want = EndUnknown
		}
		if got := Classify(after, false, now, refusedMax); got.Kind != want {
			t.Errorf("%s after a healthy run: %+v; want %v", row.Reason, got, want)
		}
		// Of another class, or at another end, it is nothing the table holds.
		other := orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_HOST
		if row.Class == other {
			other = orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE
		}
		for end := range kinds {
			if got := Classify(&orkstackv1.HostEventSourceResult{End: end, Reason: row.Reason, Class: other, FreshRead: true, Unread: "spec"}, true, now, refusedMax); got.Kind != EndUnknown {
				t.Errorf("%s of another class at %v: %+v", row.Reason, end, got)
			}
			if end != row.Completes {
				if got := Classify(&orkstackv1.HostEventSourceResult{End: end, Reason: row.Reason, Class: row.Class, FreshRead: true, Unread: "spec"}, true, now, refusedMax); got.Kind != EndUnknown {
					t.Errorf("%s at %v: %+v", row.Reason, end, got)
				}
			}
		}
	}
	if got := Classify(&orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_ROTATED}, false, now, refusedMax); got.Kind != EndRotated {
		t.Errorf("%+v", got)
	}
	for _, res := range []*orkstackv1.HostEventSourceResult{nil, {}, {Reason: "SOMETHING", End: orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE}, {End: 99, Reason: eventsources.ReasonServiceConfigRejected}} {
		if got := Classify(res, true, now, refusedMax); got.Kind != EndUnknown {
			t.Errorf("%v: %+v", res, got)
		}
	}
	long := Classify(&orkstackv1.HostEventSourceResult{Reason: strings.Repeat("x", 4096) + "\n"}, false, now, refusedMax)
	if len(long.Reason) > maxUnknownReason+len("…") || strings.Contains(long.Reason, "\n") {
		t.Errorf("%d bytes of a reason nobody knows were kept", len(long.Reason))
	}
	// A cause is one of the closed set or nothing.
	if got := Classify(&orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE, Class: orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE,
		Reason: eventsources.ReasonServiceConfigRejected, Cause: "an image's words"}, false, now, refusedMax); got.Cause != "" || got.Kind != EndUnchangeable {
		t.Errorf("%+v", got)
	}
	// Nearly an hour of refusals is not past the bound, whatever the Orker says.
	if got := Classify(&orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE, Class: orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE,
		Reason: eventsources.ReasonCredentialRefused, PastBound: true, RefusedSince: timestamppb.New(now.Add(-refusedMax + time.Second))}, false, now, refusedMax); got.Kind != EndUnmet {
		t.Errorf("%+v", got)
	}
}
