package engine

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

func TestJoinAdmits_Table(t *testing.T) {
	needs := []string{"a", "b"}
	outcomes := map[string]map[string]needOutcome{
		"all succeeded": {"a": needSucceeded, "b": needSucceeded},
		"some skipped":  {"a": needSucceeded, "b": needSkipped},
		"all skipped":   {"a": needSkipped, "b": needSkipped},
		"one failed":    {"a": needFailed, "b": needSucceeded},
		"one canceled":  {"a": needSucceeded, "b": needCanceled},
		"mixed":         {"a": needSkipped, "b": needFailed},
	}
	type want struct {
		admitted bool
		reason   string
	}
	noneFailed := "join any_failed: none of its needs failed (a, b); it runs only when one of them fails"
	cases := map[orkstackv1.JoinPolicy]map[string]want{
		orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED: {
			"all succeeded": {true, ""},
			"some skipped":  {false, `upstream task "b" was skipped`},
			"all skipped":   {false, `upstream task "a" was skipped`},
			"one failed":    {true, ""},
			"one canceled":  {true, ""},
			"mixed":         {false, `upstream task "a" was skipped`},
		},
		orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED: {
			"all succeeded": {true, ""},
			"some skipped":  {true, ""},
			"all skipped":   {false, "join none_failed: every need was skipped (a, b)"},
			"one failed":    {false, `join none_failed: need "a" did not succeed (FAILED)`},
			"one canceled":  {false, `join none_failed: need "b" did not succeed (CANCELED)`},
			"mixed":         {false, `join none_failed: need "b" did not succeed (FAILED)`},
		},
		orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED: {
			"all succeeded": {false, noneFailed},
			"some skipped":  {false, noneFailed},
			"all skipped":   {false, noneFailed},
			"one failed":    {true, ""},
			"one canceled":  {false, noneFailed},
			"mixed":         {true, ""},
		},
		orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE: {
			"all succeeded": {true, ""}, "some skipped": {true, ""}, "all skipped": {true, ""},
			"one failed": {true, ""}, "one canceled": {true, ""}, "mixed": {true, ""},
		},
	}
	for policy, rows := range cases {
		for name, w := range rows {
			o := outcomes[name]
			admitted, reason := joinAdmits(policy, needs, func(id string) needOutcome { return o[id] })
			if admitted != w.admitted || reason != w.reason {
				t.Errorf("%v / %s = (%v, %q), want (%v, %q)", policy, name, admitted, reason, w.admitted, w.reason)
			}
		}
	}
}

func TestBoundEscaped_CutsOnTheEscapedLengthAtAUTF8Boundary(t *testing.T) {
	multi := strings.Repeat("é€😀", 1024) // ~10 KiB of multi-byte runes
	got := boundEscaped(multi, needErrorMessageBytes)
	if escapedLen(got) > needErrorMessageBytes || !strings.HasSuffix(got, "…") || !utf8.ValidString(got) {
		t.Fatalf("cut multi-byte text: escaped %d bytes, suffix ok %v, valid %v",
			escapedLen(got), strings.HasSuffix(got, "…"), utf8.ValidString(got))
	}
	if escapedLen(got) < needErrorMessageBytes-8 {
		t.Errorf("cut too early: %d escaped bytes", escapedLen(got))
	}

	// Control characters escape to 6 bytes each: the cut is on the escaped
	// length, not the raw one.
	control := strings.Repeat("\x01", 300)
	cut := boundEscaped(control, needErrorDetailsBytes)
	if escapedLen(cut) > needErrorDetailsBytes || len(cut) >= needErrorDetailsBytes {
		t.Fatalf("control text: raw %d bytes, escaped %d", len(cut), escapedLen(cut))
	}
	if n := strings.Count(cut, "\x01"); n != (needErrorDetailsBytes-3)/6 {
		t.Errorf("kept %d control characters, want %d", n, (needErrorDetailsBytes-3)/6)
	}

	exact := strings.Repeat("a", needErrorCodeBytes)
	if boundEscaped(exact, needErrorCodeBytes) != exact {
		t.Error("a string exactly at the bound was changed")
	}

	e := boundedError(&orkstackv1.Error{
		Code: strings.Repeat("C", 500), Message: strings.Repeat("m", 5000), Details: strings.Repeat("d", 900),
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT, Retryable: true,
	})
	if escapedLen(e.GetCode()) > needErrorCodeBytes || escapedLen(e.GetMessage()) > needErrorMessageBytes ||
		escapedLen(e.GetDetails()) > needErrorDetailsBytes {
		t.Errorf("bounded error = %d/%d/%d escaped bytes", escapedLen(e.GetCode()), escapedLen(e.GetMessage()), escapedLen(e.GetDetails()))
	}
	if e.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT || !e.GetRetryable() {
		t.Errorf("category/retryable not copied: %v", e)
	}
}

func TestJoinContextFor_WorstCase(t *testing.T) {
	escapeHeavy := func(n int) string { return strings.Repeat("\"\\\n\x01é", n) }
	handler := &orkstackv1.Task{Id: "notify", Join: orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE}
	rows := map[string]*orkstackv1.TaskRun{}
	for i := range 32 {
		id := strings.Repeat(string(rune('a'+i%26)), 199) + string(rune('a'+i/26))
		handler.Needs = append(handler.Needs, id)
		rows[id] = &orkstackv1.TaskRun{
			TaskId: id,
			Status: orkstackv1.Status_STATUS_FAILED,
			Error: &orkstackv1.Error{
				Code: escapeHeavy(200), Message: escapeHeavy(2000), Details: escapeHeavy(500),
				Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, Retryable: true,
			},
		}
	}
	jc := joinContextFor(handler, func(id string) *orkstackv1.TaskRun { return rows[id] })
	if len(jc.GetNeeds()) != 32 || len(jc.GetNeeds()[0].GetTaskId()) != 200 {
		t.Fatalf("context = %d needs", len(jc.GetNeeds()))
	}
	size, err := task.PayloadSize(jc)
	if err != nil {
		t.Fatal(err)
	}
	if size > joinContextMaxBytes {
		t.Fatalf("the worst-case JoinContext is %d bytes, over the %d-byte bound", size, joinContextMaxBytes)
	}
	t.Logf("worst-case JoinContext: %d bytes of %d", size, joinContextMaxBytes)
}

func TestFailedNeedRef_OnlyAFailedContainerNeedWithArtifacts(t *testing.T) {
	artifact := []*orkstackv1.ArtifactRef{{Name: "log", Uri: "mem://log"}}
	cases := map[string]struct {
		row  *orkstackv1.TaskRun
		want bool
	}{
		"failed with artifacts": {&orkstackv1.TaskRun{TaskId: "d", Status: orkstackv1.Status_STATUS_FAILED, Artifacts: artifact}, true},
		"failed without":        {&orkstackv1.TaskRun{TaskId: "d", Status: orkstackv1.Status_STATUS_FAILED}, false},
		"succeeded":             {&orkstackv1.TaskRun{TaskId: "d", Status: orkstackv1.Status_STATUS_SUCCEEDED, Artifacts: artifact}, false},
		"canceled":              {&orkstackv1.TaskRun{TaskId: "d", Status: orkstackv1.Status_STATUS_CANCELED, Artifacts: artifact}, false},
		"a fan-out": {&orkstackv1.TaskRun{TaskId: "d", Status: orkstackv1.Status_STATUS_FAILED, Artifacts: artifact,
			Each: &orkstackv1.TaskEach{}}, false},
		"a composed task": {&orkstackv1.TaskRun{TaskId: "d", Status: orkstackv1.Status_STATUS_FAILED, Artifacts: artifact,
			Child: &orkstackv1.TaskChild{}}, false},
	}
	for name, tc := range cases {
		ref := failedNeedRef(tc.row)
		if (ref != nil) != tc.want {
			t.Errorf("%s: ref = %v", name, ref)
		}
		if ref != nil && (ref.GetValues() != nil || ref.GetCommitUri() != "" || len(ref.GetArtifacts()) != 1) {
			t.Errorf("%s: ref = %v, want task_id and artifacts only", name, ref)
		}
	}
}

// The condition truthiness rule is exactly the documented eleven spellings
// (docs/DSL.md "Templating"): a change needs this test and the document
// together.
func TestFalsyValues_AreExactlyTheDocumentedSet(t *testing.T) {
	var got []string
	for v := range falsyValues {
		got = append(got, v)
	}
	sort.Strings(got)
	want := []string{"", "<nil>", "<no value>", "[]", "0", "false", "map[]", "no", "null", "off", "unspecified"}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("falsyValues = %q, want %q", got, want)
	}
}

func TestJoinSentences(t *testing.T) {
	if got := failingMessage("deploy", []string{"rollback", "notify"}); got !=
		`run failing after task "deploy" failed; waiting for its failure handlers: rollback, notify` {
		t.Errorf("failingMessage = %q", got)
	}
	if got := cancelReasonNotAHandler("deploy"); got !=
		`the run is failing (task "deploy" failed) and this task is not a failure handler` {
		t.Errorf("cancelReasonNotAHandler = %q", got)
	}
}
