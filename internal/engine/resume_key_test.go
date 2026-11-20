package engine

import (
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
)

var testDigest = strings.Repeat("ab", 32)

var resumeKeyCases = []struct {
	name   string
	reason string
	want   ResumePlanKey
	ok     bool
}{
	{"a plan key", "ork-resume:v1:42.001:" + testDigest, ResumePlanKey{Scope: 42, N: 1, PlanDigest: testDigest}, true},
	{"the first plan", "ork-resume:v1:42.000:" + testDigest, ResumePlanKey{Scope: 42, N: 0, PlanDigest: testDigest}, true},
	{"the largest scope and plan", "ork-resume:v1:9007199254740991.998:" + testDigest, ResumePlanKey{Scope: 9007199254740991, N: 998, PlanDigest: testDigest}, true},
	{"plan 999", "ork-resume:v1:42.999:" + testDigest, ResumePlanKey{}, false},
	{"a scope past 2^53 - 1", "ork-resume:v1:9007199254740992.001:" + testDigest, ResumePlanKey{}, false},
	{"a reset by hand", "reset by operator", ResumePlanKey{}, false},
	{"another version", "ork-resume:v2:42.001:" + testDigest, ResumePlanKey{}, false},
	{"scope 0", "ork-resume:v1:0.001:" + testDigest, ResumePlanKey{}, false},
	{"a padded scope", "ork-resume:v1:042.001:" + testDigest, ResumePlanKey{}, false},
	{"n not three digits", "ork-resume:v1:42.1:" + testDigest, ResumePlanKey{}, false},
	{"a signed scope", "ork-resume:v1:+42.001:" + testDigest, ResumePlanKey{}, false},
	{"a scope past uint64", "ork-resume:v1:18446744073709551616.001:" + testDigest, ResumePlanKey{}, false},
	{"an upper-case digest", "ork-resume:v1:42.001:" + strings.ToUpper(testDigest), ResumePlanKey{}, false},
	{"a short digest", "ork-resume:v1:42.001:abc", ResumePlanKey{}, false},
	{"trailing bytes", "ork-resume:v1:42.001:" + testDigest + "x", ResumePlanKey{}, false},
	{"empty", "", ResumePlanKey{}, false},
}

func TestResumePlanKey_ParsesOnlyWhatItRenders(t *testing.T) {
	for _, tc := range resumeKeyCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseResumePlanKey(tc.reason)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("ParseResumePlanKey(%q) = %+v, %v; want %+v, %v", tc.reason, got, ok, tc.want, tc.ok)
			}
			if ok && got.String() != tc.reason {
				t.Fatalf("String() = %q, want the reason %q", got.String(), tc.reason)
			}
			if ok && len(tc.reason) > maxResumeKeyBytes {
				t.Fatalf("a %d-byte key parsed", len(tc.reason))
			}
		})
	}
}

// A reason is written by whoever reset the run: the parser holds it to
// exactly the form String renders, and never panics.
func FuzzParseResumePlanKey(f *testing.F) {
	for _, tc := range resumeKeyCases {
		f.Add(tc.reason)
	}
	f.Fuzz(func(t *testing.T, reason string) {
		k, ok := ParseResumePlanKey(reason)
		if !ok {
			return
		}
		if k.String() != reason {
			t.Fatalf("%q parsed to %+v, which renders %q", reason, k, k.String())
		}
		if k.Scope == 0 || k.N < 0 || k.N > maxResumePlanNumber || len(reason) > maxResumeKeyBytes {
			t.Fatalf("%q parsed to an impossible key %+v", reason, k)
		}
	})
}

func resetEvent(id, scheduled, started int64, newRun, base, reason string, cause enumspb.WorkflowTaskFailedCause) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   id,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: scheduled, StartedEventId: started, Cause: cause,
			Failure: &failurepb.Failure{Message: reason}, BaseRunId: base, NewRunId: newRun,
		}},
	}
}

func TestFindResetEvent_IsThisRunsReset(t *testing.T) {
	other := &historypb.HistoryEvent{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}
	timeout := resetEvent(5, 3, 4, "run-2", "", "timed out", enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED)
	// A run reset again copies its base's prefix, the base's own reset
	// included; that one names the base as its new run.
	baseReset := resetEvent(9, 7, 8, "run-1", "run-0", "ork-resume:v1:12.001:"+testDigest, enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW)
	own := resetEvent(20, 18, 19, "run-2", "run-1", "ork-resume:v1:30.001:"+testDigest, enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW)

	got, ok := findResetEvent([]*historypb.HistoryEvent{other, timeout, baseReset, own}, "run-2")
	want := resetFacts{BaseRunID: "run-1", Reason: "ork-resume:v1:30.001:" + testDigest, ScheduledEventID: 18, StartedEventID: 19}
	if !ok || got != want {
		t.Fatalf("findResetEvent = %+v, %v; want %+v", got, ok, want)
	}
	if _, ok := findResetEvent([]*historypb.HistoryEvent{other, timeout, baseReset}, "run-2"); ok {
		t.Fatal("found a reset of another run")
	}
}
