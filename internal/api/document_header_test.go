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
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The document header at the API (docs/DSL.md, "The document header").

const headerMissing = `the document has no apiVersion or kind: an Orkflow begins with the two lines "apiVersion: orkstack/v1" and "kind: Orkflow" — add them at the top, after any leading comments (docs/DSL.md, "The document header")`

// legacyPinned and legacyUnpinned are sources as a version stored before the
// header existed holds them: leading comments, then the first key.
const (
	legacyPinned   = "# deploy\n# stored before the header\nname: deploy\nversion: \"1.0.0\"\ntasks:\n  - id: build\n    uses: busybox:latest\n"
	legacyUnpinned = "# derive\n# stored before the header\nname: derive\ntasks:\n  - id: build\n    uses: busybox:latest\n"
)

func withHeader(t *testing.T, legacy string) string {
	t.Helper()
	added, ok := dsl.AddHeader(legacy)
	if !ok {
		t.Fatalf("AddHeader refused %q", legacy)
	}
	return added
}

// seedLegacy stores a version exactly as an apply before the header stored
// it: the definition a header-less apply prepared, under its source bytes.
// The definition is prepared by a scratch service, since this release refuses
// the header-less document itself.
func seedLegacy(t *testing.T, store *engine.MemoryOrkflowStore, legacy string) *orkstackv1.Orkflow {
	t.Helper()
	scratch := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	resp, err := scratch.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: withHeader(t, legacy)}))
	if err != nil {
		t.Fatalf("prepare the legacy version: %v", err)
	}
	wf := resp.Msg.GetOrkflow()
	wf.SourceYaml = legacy
	stored, created, err := store.Put(context.Background(), wf, artifacts.NoHeadExpectation())
	if err != nil || !created {
		t.Fatalf("seed the legacy version: created=%v, %v", created, err)
	}
	return stored
}

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
	_, err := applyHeaderDoc(svc, legacyPinned)
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
	for _, tc := range []struct{ yaml, field, prefix string }{
		{legacyPinned, "apiVersion", headerMissing},
		{strings.Replace(withHeader(t, legacyPinned), "kind: Orkflow", "kind: Automation", 1), "kind",
			"kind: Automation is an automation document, not an Orkflow"},
		{strings.Replace(withHeader(t, legacyPinned), "orkstack/v1", "orkstack/v2", 1), "apiVersion",
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

// TestOrkflowReads_ShowAVersionStoredBeforeTheHeaderWithIt: every read that
// shows YAML shows a legacy version with the header after its comments, and
// the run evidence keeps the stored bytes verbatim.
func TestOrkflowReads_ShowAVersionStoredBeforeTheHeaderWithIt(t *testing.T) {
	ctx := context.Background()
	store := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: store})
	seedLegacy(t, store, legacyPinned)
	shown := withHeader(t, legacyPinned)

	got, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: "1.0.0"}))
	if err != nil || got.Msg.GetOrkflow().GetSourceYaml() != shown {
		t.Fatalf("GetOrkflow source = %q, %v; want %q", got.Msg.GetOrkflow().GetSourceYaml(), err, shown)
	}
	listed, err := svc.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{}))
	if err != nil || len(listed.Msg.GetOrkflows()) != 1 || listed.Msg.GetOrkflows()[0].GetSourceYaml() != shown {
		t.Fatalf("ListOrkflows = %v, %v", listed.Msg.GetOrkflows(), err)
	}
	again, err := applyHeaderDoc(svc, shown)
	if err != nil || again.GetCreated() || again.GetOrkflow().GetSourceYaml() != shown {
		t.Fatalf("re-applying the shown document: created=%v source=%q, %v", again.GetCreated(), again.GetOrkflow().GetSourceYaml(), err)
	}
	if stored, err := store.Get(ctx, "default", "deploy", "1.0.0"); err != nil || stored.GetSourceYaml() != legacyPinned {
		t.Fatalf("the stored source moved: %q, %v", stored.GetSourceYaml(), err)
	}

	// A v2 whose header stands where EmittedSource puts v1's: no header line
	// is changed.
	v2 := strings.Replace(shown, `"1.0.0"`, `"2.0.0"`, 1)
	if _, err := applyHeaderDoc(svc, v2); err != nil {
		t.Fatalf("apply v2: %v", err)
	}
	diff, err := svc.DiffOrkflowVersions(ctx, connect.NewRequest(&orkstackv1.DiffOrkflowVersionsRequest{Name: "deploy", FromVersion: "1.0.0", ToVersion: "2.0.0"}))
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	for _, line := range strings.Split(diff.Msg.GetUnifiedDiff(), "\n") {
		if (strings.HasPrefix(line, "-") || strings.HasPrefix(line, "+")) && !strings.HasPrefix(line, "---") && !strings.HasPrefix(line, "+++") &&
			(strings.Contains(line, "apiVersion") || strings.Contains(line, "kind: Orkflow")) {
			t.Errorf("the diff changes a header line: %q\n%s", line, diff.Msg.GetUnifiedDiff())
		}
	}
	// A v3 written with its header on line 1, above its comments: the diff
	// moves the two lines, and each side is that version's GetOrkflow text.
	v3 := dsl.OrkflowHeader + strings.Replace(legacyPinned, `"1.0.0"`, `"3.0.0"`, 1)
	if _, err := applyHeaderDoc(svc, v3); err != nil {
		t.Fatalf("apply v3: %v", err)
	}
	moved, err := svc.DiffOrkflowVersions(ctx, connect.NewRequest(&orkstackv1.DiffOrkflowVersionsRequest{Name: "deploy", FromVersion: "1.0.0", ToVersion: "3.0.0"}))
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	// The two texts differ only in where the header stands (and the version),
	// so the diff moves the header relative to the comments; which side a
	// line-diff moves is its own choice.
	text := moved.Msg.GetUnifiedDiff()
	if want := dsl.Unified("deploy@1.0.0", "deploy@3.0.0", shown, v3); text != want {
		t.Fatalf("the diff is not of the two shown texts:\n%s\nwant\n%s", text, want)
	}
	if !strings.Contains(text, "-# deploy\n") && !strings.Contains(text, "-apiVersion: orkstack/v1\n") {
		t.Fatalf("the diff does not show the header moving:\n%s", text)
	}
}

// TestExportRunEvidence_KeepsAVersionStoredBeforeTheHeaderVerbatim: the
// evidence fixture's definition is stored without the header; GetOrkflow
// shows it with the header, and the bundle's orkflow/source.yaml is the
// stored bytes.
func TestExportRunEvidence_KeepsAVersionStoredBeforeTheHeaderVerbatim(t *testing.T) {
	f := newEvidenceFixture(t)
	f.succeeded("ork-ev-hdr")
	got, err := f.svc.GetOrkflow(context.Background(), connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "evidence", Version: "0.1.0"}))
	if err != nil || got.Msg.GetOrkflow().GetSourceYaml() != dsl.OrkflowHeader+evidenceSource {
		t.Fatalf("GetOrkflow source = %q, %v", got.Msg.GetOrkflow().GetSourceYaml(), err)
	}
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-hdr"})
	if entries := tarMembers(t, r.data); string(entries["orkflow/source.yaml"]) != evidenceSource {
		t.Fatalf("orkflow/source.yaml = %q, want the stored bytes %q", entries["orkflow/source.yaml"], evidenceSource)
	}
}

func TestApplyOrkflow_ReapplyingAVersionStoredBeforeTheHeaderIsANoOp(t *testing.T) {
	for _, legacy := range []string{legacyPinned, legacyUnpinned} {
		for where, place := range map[string]func(string) string{
			"below the comments": func(s string) string { return withHeader(t, s) },
			"above the comments": func(s string) string { return dsl.OrkflowHeader + s },
		} {
			store := engine.NewMemoryOrkflowStore()
			svc := api.NewService(api.Config{Orkflows: store})
			stored := seedLegacy(t, store, legacy)
			resp, err := applyHeaderDoc(svc, place(legacy))
			if err != nil {
				t.Fatalf("%s header %s: %v", stored.GetName(), where, err)
			}
			if resp.GetCreated() || resp.GetOrkflow().GetVersion() != stored.GetVersion() ||
				resp.GetOrkflow().GetDefinitionDigest() != stored.GetDefinitionDigest() {
				t.Errorf("%s header %s: created=%v version %s digest %s; want the stored %s %s",
					stored.GetName(), where, resp.GetCreated(), resp.GetOrkflow().GetVersion(),
					resp.GetOrkflow().GetDefinitionDigest(), stored.GetVersion(), stored.GetDefinitionDigest())
			}
		}
	}
}

// TestApplyOrkflow_MovingTheHeaderAmongLeadingCommentsIsANoOp is the
// regression test for an unpinned document refused already_exists when its
// header moved: one identity for the hash and the store.
func TestApplyOrkflow_MovingTheHeaderAmongLeadingCommentsIsANoOp(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	first, err := applyHeaderDoc(svc, withHeader(t, legacyUnpinned))
	if err != nil || !first.GetCreated() {
		t.Fatalf("first apply: %v", err)
	}
	second, err := applyHeaderDoc(svc, dsl.OrkflowHeader+legacyUnpinned)
	if err != nil {
		t.Fatalf("the header moved to line 1: %v", err)
	}
	if second.GetCreated() || second.GetOrkflow().GetVersion() != first.GetOrkflow().GetVersion() {
		t.Fatalf("created=%v version %s, want a no-op on %s", second.GetCreated(), second.GetOrkflow().GetVersion(), first.GetOrkflow().GetVersion())
	}
}

func TestApplyOrkflow_AHeaderOutOfFormSaysWhereItGoes(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: store})
	seedLegacy(t, store, legacyPinned)
	afterName := strings.Replace(legacyPinned, "version:", dsl.OrkflowHeader+"version:", 1)
	_, err := applyHeaderDoc(svc, afterName)
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("code = %v (%v), want AlreadyExists", connect.CodeOf(err), err)
	}
	if !strings.Contains(err.Error(), `its header is not the two lines "apiVersion: orkstack/v1" and "kind: Orkflow" written first (after any leading comments) — the one form a version leaves out`) {
		t.Fatalf("the refusal does not say where the header goes: %v", err)
	}
}
