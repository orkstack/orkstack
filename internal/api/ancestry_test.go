package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// deepChainLength is a default-join chain of plain tasks rule 44 refuses:
// its last task's minimal upstream references alone are over 256 KiB
// (internal/dsl TestValidate_MinimalRefSize measures it at 1,529).
const deepChainLength = 1600

// deepChainDoc is a default-join chain of n plain tasks, each needing the
// one before; withNeeds false drops every needs: line.
func deepChainDoc(name string, n int, withNeeds bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for i := range n {
		fmt.Fprintf(&b, "  - id: c%d\n    uses: busybox:latest\n", i)
		if withNeeds && i > 0 {
			fmt.Fprintf(&b, "    needs: [c%d]\n", i-1)
		}
	}
	return b.String()
}

// TestAncestryRule_RefusedAtValidateApplyAndInlinePlan: rule 44 is a
// compiler rule, so it refuses the document wherever a document is compiled —
// validate, apply and a plan of an inline document (orkctl run plan --file) —
// naming the task, invalid_argument, and writes nothing.
func TestAncestryRule_RefusedAtValidateApplyAndInlinePlan(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: orkflows, OrkflowMaxTasks: 2000})
	doc := deepChainDoc("deep", deepChainLength, true)
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatal(err)
	}
	if issues := resp.Msg.GetIssues(); len(issues) != 1 || !strings.Contains(issues[0].GetMessage(), "required ancestors") ||
		!strings.HasPrefix(issues[0].GetField(), "tasks[") {
		t.Fatalf("validate issues = %v, want rule 44's, anchored at a task's needs", issues)
	}
	_, err = svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "Split the pipeline into composed Orkflows") {
		t.Fatalf("apply = %v, want invalid_argument with rule 44's refusal", err)
	}
	if listed, _ := orkflows.List(context.Background(), "default"); len(listed) != 0 {
		t.Fatalf("a refused apply stored %d orkflows", len(listed))
	}
	_, err = svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{Yaml: doc}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "required ancestors") {
		t.Fatalf("plan of the inline document = %v, want invalid_argument with rule 44's refusal", err)
	}
}

// TestSubmit_StoredVersionIsNotRecheckedForAncestry: rule 44 applies at
// validate and apply only. A version stored before the rule existed — here
// written straight to the store — keeps starting; a task whose real
// references overflow fails INPUT_TOO_LARGE when it is dispatched, as before.
func TestSubmit_StoredVersionIsNotRecheckedForAncestry(t *testing.T) {
	// The chain the rule refuses, compiled without its needs and given them
	// back, as a definition stored by a release without rule 44.
	wf, err := dsl.Compile([]byte(deepChainDoc("deep", deepChainLength, false)))
	if err != nil {
		t.Fatal(err)
	}
	for i, task := range wf.GetTasks() {
		if i > 0 {
			task.Needs = []string{fmt.Sprintf("c%d", i-1)}
		}
	}
	if _, err := dsl.Compile([]byte(deepChainDoc("deep", deepChainLength, true))); !errors.Is(err, dsl.ErrAncestryTooLarge) {
		t.Fatalf("the same chain as a document = %v, want rule 44's refusal", err)
	}
	wf.SourceYaml = deepChainDoc("deep", deepChainLength, true)
	wf.Namespace = "default"
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		t.Fatal(err)
	}
	wf.DefinitionDigest = digest
	orkflows := engine.NewMemoryOrkflowStore()
	if _, _, err := orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{Orkflows: orkflows, Starter: starter, OrkflowMaxTasks: 2000})
	if _, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "deep"})); err != nil {
		t.Fatalf("submit of the stored version = %v, want it started", err)
	}
	if got := len(starter.startedRuns()); got != 1 {
		t.Fatalf("%d runs started, want 1", got)
	}
}
