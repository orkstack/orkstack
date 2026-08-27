package api_test

import (
	"context"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestSubmitRunPinsDurabilityConfigurationIntoRunPlan(t *testing.T) {
	localDir := filepath.Join(t.TempDir(), "artifacts")
	tests := []struct {
		name      string
		artifacts artifacts.Config
		wantRoot  string
	}{
		{
			name: "local",
			artifacts: artifacts.Config{
				Backend:  artifacts.BackendLocal,
				LocalDir: localDir,
			},
			wantRoot: "file://" + filepath.ToSlash(localDir),
		},
		{
			name: "s3",
			artifacts: artifacts.Config{
				Backend:  artifacts.BackendS3,
				S3Bucket: "durable-results",
			},
			wantRoot: "s3://durable-results",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			starter := &fakeStarter{}
			limits := engine.WorkspaceLimits{
				ValuesMaxBytes:   131072,
				ArtifactMaxCount: 12,
				ArtifactMaxBytes: 1048576,
				LogMaxBytes:      8192,
				CommitMaxBytes:   32768,
			}
			svc := api.NewService(api.Config{
				Orkflows:              engine.NewMemoryOrkflowStore(),
				Starter:               starter,
				Artifacts:             testCase.artifacts,
				Limits:                limits,
				TransferBudgetSeconds: 901,
				NewRunID:              func() string { return "run-plan" },
			})
			if _, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: `
name: plan-test
version: 1.0.0
tasks:
  - id: build
    uses: shell
    with: {command: "echo build"}
`})); err != nil {
				t.Fatalf("ApplyOrkflow: %v", err)
			}
			if _, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName: "plan-test",
			})); err != nil {
				t.Fatalf("SubmitRun: %v", err)
			}

			plans := starter.startedPlans()
			if len(plans) != 1 {
				t.Fatalf("started plans = %d, want 1", len(plans))
			}
			plan := plans[0]
			if plan.CommitRoot != testCase.wantRoot {
				t.Errorf("CommitRoot = %q, want %q", plan.CommitRoot, testCase.wantRoot)
			}
			if plan.Limits != limits {
				t.Errorf("Limits = %+v, want %+v", plan.Limits, limits)
			}
			if plan.TransferBudgetSeconds != 901 {
				t.Errorf("TransferBudgetSeconds = %d, want 901", plan.TransferBudgetSeconds)
			}
		})
	}
}
