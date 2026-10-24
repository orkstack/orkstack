package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
)

// fileLabel is the label images/activity-file ships, read from its
// Dockerfile so these tests exercise the interface a real image declares.
func fileLabel(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "images", "activity-file", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	match := regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(dsl.InterfaceLabel) + `='([^']*)'$`).FindSubmatch(raw)
	if match == nil {
		t.Fatal("images/activity-file/Dockerfile carries no interface label")
	}
	return string(match[1])
}

// interfaceService wires a Service whose `file` catalog entry resolves
// against a registry serving an image labelled with label ("" = no label).
func interfaceService(t *testing.T, label string) (*api.Service, *movableRegistry, string) {
	t.Helper()
	registry := newMovableRegistry(t)
	var labels map[string]string
	if label != "" {
		labels = map[string]string{dsl.InterfaceLabel: label}
	}
	registry.setImage(t, labels, "linux/amd64", "linux/arm64")
	server := httptest.NewServer(registry)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	catalog := dsl.Catalog{
		"file":  host + "/orkstack/activity-file:dev",
		"shell": host + "/orkstack/activity-shell:dev",
	}
	svc := api.NewService(api.Config{
		Catalog:    catalog,
		Resolver:   dsl.NewResolver(""),
		Activities: &api.CatalogLister{Catalog: catalog, Resolver: dsl.NewResolver("")},
	})
	return svc, registry, host
}

const missingSrcAndExtraSrcs = `apiVersion: orkstack/v1
kind: Orkflow
name: copier
tasks:
  - id: one
    uses: file
    with:
      op: copy
      srcs: [b]
`

const extraSrcs = `apiVersion: orkstack/v1
kind: Orkflow
name: copier
tasks:
  - id: one
    uses: file
    with:
      op: copy
      src: a
      srcs: [b]
`

func validate(t *testing.T, svc *api.Service, yaml string) *orkstackv1.ValidateOrkflowResponse {
	t.Helper()
	resp, err := svc.ValidateOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: yaml}))
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	return resp.Msg
}

func requireNothingStored(t *testing.T, svc *api.Service, name string) {
	t.Helper()
	_, err := svc.GetOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: name}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetOrkflow(%q) after a refused apply = %v, want NotFound — nothing may be stored", name, err)
	}
}

func TestValidateOrkflow_ReportsInterfaceIssuesWithFieldPaths(t *testing.T) {
	svc, _, _ := interfaceService(t, fileLabel(t))

	got := validate(t, svc, missingSrcAndExtraSrcs)
	if got.GetOrkflow() != nil {
		t.Fatal("a document with an interface ERROR carried an orkflow")
	}
	issues := got.GetIssues()
	if len(issues) != 2 {
		t.Fatalf("issues = %v, want the ERROR and the WARNING together", issues)
	}
	if issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR ||
		issues[0].GetField() != "tasks[0].with.src" ||
		!strings.HasPrefix(issues[0].GetMessage(), "with.src is required by the interface of ") ||
		!strings.HasSuffix(issues[0].GetMessage(), ` for op "copy" and is not supplied`) {
		t.Fatalf("issue 0 = %v", issues[0])
	}
	if issues[1].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		issues[1].GetField() != "tasks[0].with.srcs" ||
		!strings.Contains(issues[1].GetMessage(), `(declared: as, op, src)`) {
		t.Fatalf("issue 1 = %v", issues[1])
	}
	// The message names exactly which image made the claim: the full pin.
	if !regexp.MustCompile(`/orkstack/activity-file@sha256:[a-f0-9]{64} for op`).MatchString(issues[0].GetMessage()) {
		t.Fatalf("message does not name the pinned image: %q", issues[0].GetMessage())
	}
}

func TestValidateOrkflow_WarningsAloneKeepTheOrkflow(t *testing.T) {
	svc, _, _ := interfaceService(t, fileLabel(t))

	got := validate(t, svc, extraSrcs)
	if got.GetOrkflow() == nil {
		t.Fatal("a warned document lost its orkflow")
	}
	issues := got.GetIssues()
	if len(issues) != 1 || issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		issues[0].GetField() != "tasks[0].with.srcs" {
		t.Fatalf("issues = %v, want exactly the srcs WARNING (and no pin warnings: no pins)", issues)
	}
}

func TestApplyOrkflow_RefusesAMissingRequiredWithKeyBeforeWriting(t *testing.T) {
	svc, _, _ := interfaceService(t, fileLabel(t))

	_, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: missingSrcAndExtraSrcs}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("apply = %v, want InvalidArgument", err)
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) ||
		!strings.HasPrefix(connectErr.Message(), "tasks[0].with.src: with.src is required by the interface of") {
		t.Fatalf("message = %q, want it to begin with the first ERROR", err)
	}
	if !strings.HasSuffix(connectErr.Message(), "(and 1 more issues; run orkctl orkflow validate to see them all)") {
		t.Fatalf("message = %q, want the count of the rest", connectErr.Message())
	}
	requireNothingStored(t, svc, "copier")
}

func TestApplyOrkflow_CarriesInterfaceWarningsAndStores(t *testing.T) {
	svc, _, _ := interfaceService(t, fileLabel(t))

	applied := applyOnce(t, svc, extraSrcs)
	if !applied.GetCreated() {
		t.Fatal("a warned document was not stored")
	}
	warnings := applied.GetWarnings()
	if len(warnings) != 1 || !strings.HasPrefix(warnings[0], "tasks[0].with.srcs: with.srcs is not declared by the interface of ") {
		t.Fatalf("warnings = %q, want the one srcs warning as <field>: <message>", warnings)
	}
	issues := validate(t, svc, extraSrcs).GetIssues()
	if warnings[0] != issues[0].GetField()+": "+issues[0].GetMessage() {
		t.Fatalf("apply's warning %q is not validate's issue %v", warnings[0], issues[0])
	}
}

// An image without the label is resolved and applied exactly as before: no
// warnings, no issues, and a definition digest and version that are a pure
// function of the stored document — which has no field the label could reach.
//
// A labelled and an unlabelled image cannot share an index digest (the label
// lives in the config the index addresses), so the "same values" the contract
// asks for are checked as the property that makes them equal: in both cases
// the stored ImageRef differs only in its content addresses, and the digest
// and version are recomputed from the stored document alone.
func TestApplyOrkflow_AnImageWithoutALabelAppliesExactlyAsBefore(t *testing.T) {
	const doc = `apiVersion: orkstack/v1
kind: Orkflow
name: copier
tasks:
  - id: one
    uses: file
    with:
      op: copy
      src: a
`
	plainSvc, _, _ := interfaceService(t, "")
	labelledSvc, _, _ := interfaceService(t, fileLabel(t))

	if issues := validate(t, plainSvc, doc).GetIssues(); len(issues) != 0 {
		t.Fatalf("an unlabelled image produced issues: %v", issues)
	}
	plain := applyOnce(t, plainSvc, doc)
	labelled := applyOnce(t, labelledSvc, doc)
	if len(plain.GetWarnings()) != 0 || len(labelled.GetWarnings()) != 0 {
		t.Fatalf("warnings = %q / %q, want none for a conforming document", plain.GetWarnings(), labelled.GetWarnings())
	}
	for _, applied := range []*orkstackv1.ApplyOrkflowResponse{plain, labelled} {
		stored := applied.GetOrkflow()
		digest, err := dsl.DefinitionDigest(stored)
		if err != nil {
			t.Fatalf("digest: %v", err)
		}
		if digest != stored.GetDefinitionDigest() {
			t.Fatalf("definition digest %q is not the stored document's own %q", stored.GetDefinitionDigest(), digest)
		}
		version, err := dsl.DerivedVersion([]byte(doc), stored)
		if err != nil {
			t.Fatalf("version: %v", err)
		}
		if version != stored.GetVersion() {
			t.Fatalf("version %q is not derived from the stored document (%q)", stored.GetVersion(), version)
		}
	}
	a := proto.Clone(plain.GetOrkflow().GetTasks()[0].GetImage()).(*orkstackv1.ImageRef)
	b := proto.Clone(labelled.GetOrkflow().GetTasks()[0].GetImage()).(*orkstackv1.ImageRef)
	for _, image := range []*orkstackv1.ImageRef{a, b} {
		image.IndexDigest, image.Pinned, image.Registry = "", "", ""
	}
	if !proto.Equal(a, b) {
		t.Fatalf("the label reached the stored ImageRef:\n%v\n%v", a, b)
	}
}

func TestApplyOrkflow_AnUnservableConfigBlobRefusesTheApply(t *testing.T) {
	svc, registry, _ := interfaceService(t, fileLabel(t))
	registry.blobStatus.Store(http.StatusServiceUnavailable)

	_, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: extraSrcs}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("apply = %v, want Unavailable", err)
	}
	if !strings.Contains(err.Error(), dsl.ReasonImageResolveUnavailable) {
		t.Fatalf("error does not carry %s: %v", dsl.ReasonImageResolveUnavailable, err)
	}
	requireNothingStored(t, svc, "copier")
}

func TestListActivities_CarriesTheInterfaceAndItsError(t *testing.T) {
	serve := func(label string) string {
		registry := newMovableRegistry(t)
		var labels map[string]string
		if label != "" {
			labels = map[string]string{dsl.InterfaceLabel: label}
		}
		registry.setImage(t, labels, "linux/amd64")
		server := httptest.NewServer(registry)
		t.Cleanup(server.Close)
		return strings.TrimPrefix(server.URL, "http://")
	}
	catalog := dsl.Catalog{
		"labelled":   serve(fileLabel(t)) + "/orkstack/activity-file:dev",
		"unlabelled": serve("") + "/orkstack/activity-shell:dev",
		"malformed":  serve(`{"schemaVersion":2}`) + "/orkstack/activity-http:dev",
	}
	svc := api.NewService(api.Config{
		Catalog:    catalog,
		Activities: &api.CatalogLister{Catalog: catalog, Resolver: dsl.NewResolver("")},
	})
	resp, err := svc.ListActivities(context.Background(), connect.NewRequest(&orkstackv1.ListActivitiesRequest{}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	got := resp.Msg.GetActivities()
	if len(got) != 3 || got[0].GetName() != "labelled" || got[1].GetName() != "malformed" || got[2].GetName() != "unlabelled" {
		t.Fatalf("activities = %v, want ordered by name", got)
	}
	if len(got[0].GetInterface().GetOps()) != 7 || got[0].GetInterfaceError() != "" {
		t.Fatalf("labelled = %v, want the file interface's 7 ops", got[0])
	}
	if got[1].GetInterface() != nil || got[1].GetInterfaceError() != "schemaVersion 2 is not supported (this engine reads 1)" {
		t.Fatalf("malformed = %v, want no interface and the parser's reason", got[1])
	}
	if got[2].GetInterface() != nil || got[2].GetInterfaceError() != "" || got[2].GetIndexDigest() == "" {
		t.Fatalf("unlabelled = %v, want a resolved entry declaring nothing", got[2])
	}
}

func TestInspectActivity_ResolvesACatalogNameOrAReference(t *testing.T) {
	svc, _, host := interfaceService(t, fileLabel(t))

	byName, err := svc.InspectActivity(context.Background(),
		connect.NewRequest(&orkstackv1.InspectActivityRequest{Reference: "file"}))
	if err != nil {
		t.Fatalf("inspect file: %v", err)
	}
	reference := host + "/orkstack/activity-file:dev"
	byReference, err := svc.InspectActivity(context.Background(),
		connect.NewRequest(&orkstackv1.InspectActivityRequest{Reference: reference}))
	if err != nil {
		t.Fatalf("inspect reference: %v", err)
	}
	a, b := byName.Msg.GetActivity(), byReference.Msg.GetActivity()
	if a.GetName() != "file" || b.GetName() != "" {
		t.Fatalf("names = %q / %q, want the catalog key and then empty", a.GetName(), b.GetName())
	}
	if a.GetReference() != reference || b.GetReference() != reference {
		t.Fatalf("references = %q / %q, want %q", a.GetReference(), b.GetReference(), reference)
	}
	if a.GetIndexDigest() == "" || a.GetIndexDigest() != b.GetIndexDigest() {
		t.Fatalf("digests = %q / %q", a.GetIndexDigest(), b.GetIndexDigest())
	}
	if a.GetInterface() == nil || !proto.Equal(a.GetInterface(), b.GetInterface()) {
		t.Fatalf("interfaces differ or are empty:\n%v\n%v", a.GetInterface(), b.GetInterface())
	}
	if a.GetResolvedAt() == nil || len(a.GetPlatforms()) != 2 || a.GetMediaType() != dsl.MediaTypeOCIIndex {
		t.Fatalf("inspect answer = %v, want it shaped as ListActivities would", a)
	}
}

func TestInspectActivity_RefusesWhatUsesWouldRefuse(t *testing.T) {
	svc, _, _ := interfaceService(t, fileLabel(t))
	for _, tc := range []struct {
		reference, mention string
	}{
		{"", "reference is required"},
		{"{{ .inputs.x }}", dsl.ReasonImageNotPinned},
		{"nosuch", dsl.ReasonUnknownActivity},
	} {
		_, err := svc.InspectActivity(context.Background(),
			connect.NewRequest(&orkstackv1.InspectActivityRequest{Reference: tc.reference}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), tc.mention) {
			t.Fatalf("inspect %q = %v, want InvalidArgument mentioning %s", tc.reference, err, tc.mention)
		}
	}

	bare := api.NewService(api.Config{Catalog: dsl.Catalog{"file": "localhost:5001/x:dev"}})
	_, err := bare.InspectActivity(context.Background(),
		connect.NewRequest(&orkstackv1.InspectActivityRequest{Reference: "file"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "no image resolver") {
		t.Fatalf("inspect with no resolver = %v, want FailedPrecondition naming the missing resolver", err)
	}
}
