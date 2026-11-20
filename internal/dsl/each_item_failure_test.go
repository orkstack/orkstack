package dsl

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// arrayFanOut is an Orkflow with one fan-out over an input array, its each:
// block ending in extra.
func arrayFanOut(extra string) string {
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: x\ninputs:\n  l:\n    type: array\ntasks:\n" +
		"  - id: convert\n    uses: busybox:1\n    each:\n      over: '{{ .inputs.l }}'\n      max_items: 10\n" + extra
}

// TestValidateEach_OnItemFailure: on_item_failure is fail_fast or continue,
// on any fan-out; max_failed_items takes 1 … max_items−1 and only with
// continue.
func TestValidateEach_OnItemFailure(t *testing.T) {
	for _, tc := range []struct {
		name, doc, refusal string
		want               orkstackv1.EachItemFailure
		maxFailed          int32
	}{
		{name: "continue over an array", doc: arrayFanOut("      on_item_failure: continue\n"),
			want: orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_CONTINUE},
		{name: "continue over lines", doc: linesOrkflow("      over_artifact: list/a.jsonl\n      format: jsonl\n      max_items: 4\n      on_item_failure: continue\n", ""),
			want: orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_CONTINUE},
		{name: "fail_fast is the default, unset", doc: arrayFanOut("      on_item_failure: fail_fast\n")},
		{name: "a bound", doc: arrayFanOut("      on_item_failure: continue\n      max_failed_items: 3\n"),
			want: orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_CONTINUE, maxFailed: 3},
		{name: "the largest bound", doc: arrayFanOut("      on_item_failure: continue\n      max_failed_items: 9\n"),
			want: orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_CONTINUE, maxFailed: 9},
		{name: "an unknown word", doc: arrayFanOut("      on_item_failure: ignore\n"), refusal: `each.on_item_failure "ignore" is not fail_fast or continue`},
		{name: "a bound without continue", doc: arrayFanOut("      max_failed_items: 3\n"), refusal: "each.max_failed_items needs on_item_failure: continue"},
		{name: "a bound with fail_fast", doc: arrayFanOut("      on_item_failure: fail_fast\n      max_failed_items: 3\n"),
			refusal: "each.max_failed_items needs on_item_failure: continue"},
		{name: "a zero bound", doc: arrayFanOut("      on_item_failure: continue\n      max_failed_items: 0\n"), refusal: "out of range (1 … 9)"},
		{name: "a bound at max_items", doc: arrayFanOut("      on_item_failure: continue\n      max_failed_items: 10\n"),
			refusal: "out of range (1 … 9)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf, err := Compile([]byte(tc.doc))
			if tc.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refusal) {
					t.Fatalf("Compile = %v; want a refusal containing %q", err, tc.refusal)
				}
				return
			}
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			spec := wf.GetTasks()[len(wf.GetTasks())-1].GetEach()
			if spec.GetOnItemFailure() != tc.want || spec.GetMaxFailedItems() != tc.maxFailed {
				t.Fatalf("each = %v; want on_item_failure %v, max_failed_items %d", spec, tc.want, tc.maxFailed)
			}
		})
	}
}

// TestValidateEach_RefusesAHandBuiltItemFailure: a definition that is not
// the compiler's — FAIL_FAST spelt out, or a bound without CONTINUE — is
// refused by validation, so one fan-out has one digest.
func TestValidateEach_RefusesAHandBuiltItemFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		edit    func(*orkstackv1.EachSpec)
		refusal string
	}{
		{"FAIL_FAST spelt out", func(s *orkstackv1.EachSpec) { s.OnItemFailure = orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_FAIL_FAST },
			"the compiler writes fail_fast as unset"},
		{"an unknown value", func(s *orkstackv1.EachSpec) { s.OnItemFailure = 7 }, "is not fail_fast or continue"},
		{"a bound without continue", func(s *orkstackv1.EachSpec) { s.MaxFailedItems = 2 }, "needs on_item_failure: continue"},
		{"a negative bound", func(s *orkstackv1.EachSpec) {
			s.OnItemFailure = orkstackv1.EachItemFailure_EACH_ITEM_FAILURE_CONTINUE
			s.MaxFailedItems = -1
		}, "out of range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf, err := Compile([]byte(arrayFanOut("")))
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(wf.GetTasks()[0].GetEach())
			if err := validate(wf, Options{}); err == nil || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("Validate = %v; want a refusal containing %q", err, tc.refusal)
			}
		})
	}
}

// TestCompileEach_FailFastNormalises: on_item_failure: fail_fast and no
// on_item_failure compile to one definition and one digest, which names no
// on_item_failure at all, and continue compiles to another.
func TestCompileEach_FailFastNormalises(t *testing.T) {
	canonical := func(doc string) string {
		t.Helper()
		wf, err := Compile([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		wf.Version = "" // minted from the document's text, which differs
		b, err := CanonicalOrkflow(wf)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	none := canonical(arrayFanOut(""))
	if a := canonical(arrayFanOut("      on_item_failure: fail_fast\n")); a != none {
		t.Fatalf("on_item_failure: fail_fast and its absence compile to different definitions:\n%s\n%s", a, none)
	}
	if a := canonical(arrayFanOut("      on_item_failure: continue\n")); a == none {
		t.Fatal("on_item_failure: continue compiles to the default's definition")
	}
	if strings.Contains(none, "on_item_failure") || strings.Contains(none, "onItemFailure") {
		t.Fatal("a fan-out without on_item_failure names it in its canonical definition")
	}
}
