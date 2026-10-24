package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

// The document header at the API (docs/DSL.md, "The document header").

const headerMissing = `the document has no apiVersion or kind: an Orkflow begins with the two lines "apiVersion: orkstack/v1" and "kind: Orkflow" — add them at the top, after any leading comments (docs/DSL.md, "The document header")`

const (
	headerlessPinned   = "# deploy\nname: deploy\nversion: \"1.0.0\"\ntasks:\n  - id: build\n    uses: busybox:latest\n"
	headerlessUnpinned = "# derive\nname: derive\ntasks:\n  - id: build\n    uses: busybox:latest\n"
)

func applyHeaderDoc(svc *api.Service, yaml string) (*orkstackv1.ApplyOrkflowResponse, error) {
	resp, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestApplyOrkflow_RefusesADocumentWithoutTheHeader(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: store})
	_, err := applyHeaderDoc(svc, headerlessPinned)
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v (%v), want InvalidArgument", connect.CodeOf(err), err)
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Message() != headerMissing {
		t.Fatalf("message = %q, want %q", err, headerMissing)
	}
	if _, err := store.Get(context.Background(), "default", "deploy", ""); err == nil {
		t.Fatal("a refused document was stored")
	}
}

func TestValidateOrkflow_ReportsTheHeaderOnItsField(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	headered := dsl.OrkflowHeader + headerlessPinned
	for _, tc := range []struct{ yaml, field, prefix string }{
		{headerlessPinned, "apiVersion", headerMissing},
		{strings.Replace(headered, "kind: Orkflow", "kind: Automation", 1), "kind",
			"kind: Automation is an automation document, not an Orkflow"},
		{strings.Replace(headered, "orkstack/v1", "orkstack/v2", 1), "apiVersion",
			`apiVersion "orkstack/v2" is not supported: this engine reads apiVersion: orkstack/v1`},
	} {
		resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: tc.yaml}))
		if err != nil {
			t.Fatalf("ValidateOrkflow: %v", err)
		}
		issues := resp.Msg.GetIssues()
		if len(issues) != 1 || issues[0].GetField() != tc.field || !strings.HasPrefix(issues[0].GetMessage(), tc.prefix) ||
			issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
			t.Errorf("issues = %v, want one ERROR on %s starting %q", issues, tc.field, tc.prefix)
		}
	}
}

func TestPlanRun_RefusesADocumentWithoutTheHeader(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	_, err := planRun(svc, &orkstackv1.PlanRunRequest{Yaml: strings.Replace(planDocument, dsl.OrkflowHeader, "", 1)})
	requirePlanCode(t, err, connect.CodeInvalidArgument, headerMissing)
}

// TestApplyOrkflow_AHeaderOnlyChangeToAPinnedVersionIsRefused: the header is
// part of the source, so moving it under a pinned version is an edit like any
// other and the store refuses it, never silently treating it as the stored
// document.
func TestApplyOrkflow_AHeaderOnlyChangeToAPinnedVersionIsRefused(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: store})
	first, err := applyHeaderDoc(svc, dsl.OrkflowHeader+headerlessPinned)
	if err != nil || !first.GetCreated() {
		t.Fatalf("first apply: %v", err)
	}
	moved := "# deploy\n" + dsl.OrkflowHeader + strings.TrimPrefix(headerlessPinned, "# deploy\n")
	_, err = applyHeaderDoc(svc, moved)
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("moving the header = %v (%v), want AlreadyExists", connect.CodeOf(err), err)
	}
	if !strings.Contains(err.Error(), "a version is immutable, so publish a new version") {
		t.Fatalf("the refusal does not carry the plain advice: %v", err)
	}
	if stored, err := store.Get(context.Background(), "default", "deploy", "1.0.0"); err != nil ||
		stored.GetSourceYaml() != dsl.OrkflowHeader+headerlessPinned {
		t.Fatalf("the stored source moved: %q, %v", stored.GetSourceYaml(), err)
	}
}

// TestApplyOrkflow_MovingTheHeaderOnAnUnpinnedDocumentIsANewVersion: without
// a pinned version the whole source names the version, so moving the header
// mints another version instead of being refused as a rewrite.
func TestApplyOrkflow_MovingTheHeaderOnAnUnpinnedDocumentIsANewVersion(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	first, err := applyHeaderDoc(svc, dsl.OrkflowHeader+headerlessUnpinned)
	if err != nil || !first.GetCreated() {
		t.Fatalf("first apply: %v", err)
	}
	moved := "# derive\n" + dsl.OrkflowHeader + strings.TrimPrefix(headerlessUnpinned, "# derive\n")
	second, err := applyHeaderDoc(svc, moved)
	if err != nil {
		t.Fatalf("moving the header: %v", err)
	}
	if !second.GetCreated() || second.GetOrkflow().GetVersion() == first.GetOrkflow().GetVersion() {
		t.Fatalf("created=%v version %s; want a new version beside %s",
			second.GetCreated(), second.GetOrkflow().GetVersion(), first.GetOrkflow().GetVersion())
	}
}
