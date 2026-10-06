package dsl

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A hand-built definition meets rule 37 too: a compiled join is one of
// JoinPolicy's defined values.
func TestValidate_Rule37_RefusesAnUndefinedCompiledJoin(t *testing.T) {
	wf := &orkstackv1.Orkflow{
		Name: "hand-built",
		Tasks: []*orkstackv1.Task{
			{Id: "a", Uses: "shell"},
			{Id: "b", Uses: "shell", Needs: []string{"a"}, Join: orkstackv1.JoinPolicy(9)},
		},
	}
	err := validate(wf, Options{})
	var verr *ValidationError
	if err == nil || !errorsAs(err, &verr) || verr.Field != "tasks[1].join" ||
		!strings.Contains(err.Error(), `task "b": join JoinPolicy(9) is not a defined join policy`) {
		t.Fatalf("JoinPolicy(9) = %v", err)
	}
	wf.Tasks[1].Join = orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE
	if err := validate(wf, Options{}); err != nil {
		t.Fatalf("a defined join = %v, want accepted", err)
	}
}

func errorsAs(err error, target **ValidationError) bool {
	v, ok := err.(*ValidationError)
	if ok {
		*target = v
	}
	return ok
}
