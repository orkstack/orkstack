package api_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// refusedRetries are retry: blocks docs/DSL.md rule 47 refuses, each with the
// field its refusal names, a phrase of it, and the policy a release before
// the rule compiled the block to.
var refusedRetries = []struct {
	name, retry, field, phrase string
	stored                     *orkstackv1.RetryPolicy
}{
	{"a maximum below the initial interval", "{maximum_attempts: 3, initial_interval_seconds: 30, maximum_interval_seconds: 10}",
		"tasks[0].retry.maximum_interval_seconds", "maximum_interval_seconds 10 is below retry.initial_interval_seconds 30",
		&orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 30, MaximumIntervalSeconds: 10}},
	{"a coefficient below 1", "{maximum_attempts: 3, backoff_coefficient: 0.5}",
		"tasks[0].retry.backoff_coefficient", "backoff_coefficient 0.5 is below 1",
		&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 0.5}},
	{"a negative maximum", "{maximum_attempts: 3, maximum_interval_seconds: -1}",
		"tasks[0].retry.maximum_interval_seconds", "maximum_interval_seconds -1 is negative",
		&orkstackv1.RetryPolicy{MaximumAttempts: 3, MaximumIntervalSeconds: -1}},
	{"a negative initial interval", "{maximum_attempts: 3, initial_interval_seconds: -5}",
		"tasks[0].retry.initial_interval_seconds", "initial_interval_seconds -5 is negative",
		&orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: -5}},
	{"a coefficient that is not a number", "{maximum_attempts: 3, backoff_coefficient: .nan}",
		"tasks[0].retry.backoff_coefficient", "backoff_coefficient NaN is not a finite number",
		&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN()}},
}

func retriedDoc(retry string) string {
	return fmt.Sprintf("apiVersion: orkstack/v1\nkind: Orkflow\nname: retried\nversion: \"1.0.0\"\n"+
		"tasks:\n  - id: fetch\n    uses: busybox:latest\n    retry: %s\n", retry)
}

// TestRetryRule_RefusedAtValidateApplyAndInlinePlan: rule 47 is a compiler
// rule, so it refuses the document wherever a document is compiled. Validate
// answers with one ERROR issue at the retry field, apply and a plan of an
// inline document (orkctl run plan --file) with invalid_argument, and nothing
// is stored.
func TestRetryRule_RefusedAtValidateApplyAndInlinePlan(t *testing.T) {
	for _, tc := range refusedRetries {
		t.Run(tc.name, func(t *testing.T) {
			orkflows := engine.NewMemoryOrkflowStore()
			svc := api.NewService(api.Config{Orkflows: orkflows})
			doc := retriedDoc(tc.retry)
			resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
			if err != nil {
				t.Fatalf("validate = %v, want issues", err)
			}
			if issues := resp.Msg.GetIssues(); len(issues) != 1 || issues[0].GetField() != tc.field ||
				issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR ||
				!strings.Contains(issues[0].GetMessage(), tc.phrase) {
				t.Fatalf("validate issues = %v, want one ERROR at %s saying %q", issues, tc.field, tc.phrase)
			}
			_, err = svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
			if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), tc.phrase) {
				t.Fatalf("apply = %v, want invalid_argument saying %q", err, tc.phrase)
			}
			listed, err := orkflows.List(context.Background(), "default")
			must.OKf(t, err, "list the store after a refused apply")
			if len(listed) != 0 {
				t.Fatalf("a refused apply stored %d orkflows", len(listed))
			}
			_, err = svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{Yaml: doc}))
			if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), tc.phrase) {
				t.Fatalf("plan of the inline document = %v, want invalid_argument saying %q", err, tc.phrase)
			}
		})
	}
}

// TestSubmit_StoredVersionWithARefusedRetryStillStarts: rule 47 applies where
// a document is compiled, never to a version already stored. A version
// applied before the rule — here written straight to the store — still plans
// and starts, and its definition reaches the engine unchanged, which
// schedules the nearest policy the cluster accepts
// (internal/engine TestRunWorkflow_AVersionStoredBeforeTheRetryRuleIsScheduled
// and TestIntegration_AVersionStoredBeforeTheRetryRuleIsScheduled).
func TestSubmit_StoredVersionWithARefusedRetryStillStarts(t *testing.T) {
	for _, tc := range refusedRetries {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := dsl.Compile([]byte(retriedDoc(tc.retry))); err == nil || !strings.Contains(err.Error(), tc.phrase) {
				t.Fatalf("the retry as a document = %v, want rule 47's refusal", err)
			}
			wf, err := dsl.Compile([]byte(retriedDoc("{maximum_attempts: 3}")))
			must.OK(t, err)
			wf.GetTasks()[0].Retry = proto.Clone(tc.stored).(*orkstackv1.RetryPolicy)
			wf.SourceYaml = retriedDoc(tc.retry)
			wf.Namespace = "default"
			digest, err := dsl.DefinitionDigest(wf)
			must.OK(t, err)
			wf.DefinitionDigest = digest
			orkflows := engine.NewMemoryOrkflowStore()
			if _, _, err := orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
				t.Fatal(err)
			}
			starter := &fakeStarter{}
			svc := api.NewService(api.Config{Orkflows: orkflows, Starter: starter})
			if _, err := svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{OrkflowName: "retried"})); err != nil {
				t.Fatalf("plan of the stored version = %v, want it planned", err)
			}
			if _, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "retried"})); err != nil {
				t.Fatalf("submit of the stored version = %v, want it started", err)
			}
			starter.mu.Lock()
			defer starter.mu.Unlock()
			if len(starter.started) != 1 {
				t.Fatalf("%d runs started, want 1", len(starter.started))
			}
			if got := starter.orkflows[0].GetTasks()[0].GetRetry(); !proto.Equal(got, tc.stored) {
				t.Fatalf("the run started with retry %v, want the stored %v unchanged", got, tc.stored)
			}
			if got, want := starter.plans[0].RetryMaxIntervalSeconds, int32(engine.DefaultRetryMaxInterval.Seconds()); got != want {
				t.Fatalf("the run's plan carries a maximum retry interval of %d s, want the default %d", got, want)
			}
		})
	}
}
