package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
)

const (
	fakeRegistry  = "localhost:5001"
	fileReference = fakeRegistry + "/orkstack/activity-file:dev"
	fileDigest    = "sha256:2b1e0a7c9d3f" + "4e5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e"
)

// firstPartyInterface is the interface an images/<image>/Dockerfile ships.
func firstPartyInterface(t *testing.T, image string) *orkstackv1.ActivityInterface {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "images", image, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	match := regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(dsl.InterfaceLabel) + `='([^']*)'$`).FindSubmatch(raw)
	if match == nil {
		t.Fatalf("images/%s/Dockerfile carries no interface label", image)
	}
	iface, err := dsl.ParseInterfaceLabel(string(match[1]))
	if err != nil {
		t.Fatalf("parse %s label: %v", image, err)
	}
	return iface
}

// stubResolver is the api.ImageResolver seam with fixed answers per
// reference: the registry is the one thing the CLI fixture does not run.
type stubResolver map[string]*dsl.Resolution

func (s stubResolver) Inspect(_ context.Context, uses, reference string) (*dsl.Resolution, error) {
	res, ok := s[reference]
	if !ok {
		return nil, &dsl.ResolveError{Reference: reference, Code: dsl.ReasonImageNotFound, Detail: "the stub registry has no such image"}
	}
	image := proto.Clone(res.Image).(*orkstackv1.ImageRef)
	image.Reference = uses
	return &dsl.Resolution{Image: image, Interface: res.Interface, InterfaceError: res.InterfaceError}, nil
}

func (s stubResolver) ResolveOrkflow(ctx context.Context, catalog dsl.Catalog, wf *orkstackv1.Orkflow) (dsl.Resolutions, error) {
	out := dsl.Resolutions{}
	for _, task := range wf.GetTasks() {
		// As the real resolver: an await and a composed task run no image.
		if task.GetAwait() != nil || dsl.IsComposedTask(task) {
			continue
		}
		reference, err := catalog.ResolveUses(task.GetId(), task.GetUses())
		if err != nil {
			return nil, err
		}
		res, err := s.Inspect(ctx, task.GetUses(), reference)
		if err != nil {
			return nil, err
		}
		task.Image = res.Image
		out[task.GetUses()] = res
	}
	return out, nil
}

func fileResolution(t *testing.T) *dsl.Resolution {
	return &dsl.Resolution{
		Image: &orkstackv1.ImageRef{
			Registry: fakeRegistry, Repository: "orkstack/activity-file",
			IndexDigest: fileDigest, Pinned: fakeRegistry + "/orkstack/activity-file@" + fileDigest,
			MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64", "linux/arm64"},
		},
		Interface: firstPartyInterface(t, "activity-file"),
	}
}

// interfaceFixture wires the real service with a catalog and a stub resolver
// holding the given references.
func interfaceFixture(t *testing.T, resolutions stubResolver) *fixture {
	t.Helper()
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Catalog = dsl.Catalog{"file": fileReference}
		cfg.Resolver = resolutions
	})
}

func TestActivityList_RendersTheInterfaceColumn(t *testing.T) {
	resolved := timestamppb.New(fixedNow)
	f := newFixture(t,
		&orkstackv1.ActivityInfo{Name: "file", Reference: fileReference, IndexDigest: fileDigest,
			MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64", "linux/arm64"}, ResolvedAt: resolved,
			Interface: firstPartyInterface(t, "activity-file")},
		&orkstackv1.ActivityInfo{Name: "http", Reference: fakeRegistry + "/orkstack/activity-http:dev", IndexDigest: "sha256:" + strings.Repeat("c", 64),
			MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64"}, ResolvedAt: resolved,
			InterfaceError: "schemaVersion 2 is not supported (this engine reads 1)"},
		&orkstackv1.ActivityInfo{Name: "migrate", Reference: "ghcr.io/acme/migrate:v3"},
		&orkstackv1.ActivityInfo{Name: "shell", Reference: fakeRegistry + "/orkstack/activity-shell:dev", IndexDigest: "sha256:" + strings.Repeat("d", 64),
			MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64"}, ResolvedAt: resolved},
	)
	human := f.run("activity", "list").requireExit(t, exitOK).stdout
	requireGolden(t, "activity-list.golden", human)
	for _, want := range []string{"7 ops", "none", "malformed"} {
		if !strings.Contains(human, want) {
			t.Fatalf("INTERFACE column lacks %q:\n%s", want, human)
		}
	}
	requireGolden(t, "activity-list.json.golden", f.run("--json", "activity", "list").requireExit(t, exitOK).stdout)
}

func TestActivityInspect_RendersDeclaredNoneAndMalformed(t *testing.T) {
	keysOnly, err := dsl.ParseInterfaceLabel(`{"schemaVersion":1,"fields":{"target":{"type":"string","required":true},"dryRun":{"type":"boolean","default":false},"region":{"type":"string","default":"eu-west-1"}},"values":{"deployed":{"type":"boolean"}}}`)
	if err != nil {
		t.Fatalf("keys-only label: %v", err)
	}
	plainImage := func(repository string) *orkstackv1.ImageRef {
		digest := "sha256:" + strings.Repeat("e", 64)
		return &orkstackv1.ImageRef{Registry: "ghcr.io", Repository: repository, IndexDigest: digest,
			Pinned: "ghcr.io/" + repository + "@" + digest, MediaType: dsl.MediaTypeOCIManifest}
	}
	f := interfaceFixture(t, stubResolver{
		fileReference:             fileResolution(t),
		"ghcr.io/acme/deploy:4.0": {Image: plainImage("acme/deploy"), Interface: keysOnly},
		"ghcr.io/acme/http:1":     {Image: plainImage("acme/http"), Interface: firstPartyInterface(t, "activity-http")},
		"ghcr.io/acme/plain:1":    {Image: plainImage("acme/plain")},
		"ghcr.io/acme/broken:1":   {Image: plainImage("acme/broken"), InterfaceError: `label is not the JSON encoding of an ActivityInterface: proto: (line 1:27): unknown field "pattern"`},
	})
	for _, tc := range []struct {
		args   []string
		golden string
	}{
		{[]string{"activity", "inspect", "file"}, "activity-inspect.golden"},
		{[]string{"activity", "inspect", "ghcr.io/acme/deploy:4.0"}, "activity-inspect-keys.golden"},
		// Shared keys list under OP "*", first; an op reading nothing more is one "-" row.
		{[]string{"activity", "inspect", "ghcr.io/acme/http:1"}, "activity-inspect-shared.golden"},
		{[]string{"activity", "inspect", "ghcr.io/acme/plain:1"}, "activity-inspect-none.golden"},
		{[]string{"activity", "inspect", "ghcr.io/acme/broken:1"}, "activity-inspect-malformed.golden"},
		{[]string{"--json", "activity", "inspect", "file"}, "activity-inspect.json.golden"},
	} {
		requireGolden(t, tc.golden, f.run(tc.args...).requireExit(t, exitOK).stdout)
	}
}

func TestActivityInspect_ReportsTheServerError(t *testing.T) {
	f := newFixture(t)
	// An engine that predates InspectActivity answers Unimplemented.
	mux := http.NewServeMux()
	path, handler := orkstackv1connect.NewActivityServiceHandler(orkstackv1connect.UnimplementedActivityServiceHandler{})
	mux.Handle(path, handler)
	server := api.NewServer("127.0.0.1:0", mux)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	f.baseURL = "http://" + listener.Addr().String()

	out := f.run("activity", "inspect", "file")
	if out.code == exitOK {
		t.Fatalf("inspect against an engine without the RPC exited 0:\n%s", out.stdout)
	}
	out.requireStderrContains(t, "inspect activity", "unimplemented")

	// And a reference the engine refuses is the engine's own words.
	g := interfaceFixture(t, stubResolver{})
	g.run("activity", "inspect", "{{ .x }}").requireStderrContains(t, dsl.ReasonImageNotPinned)
}

const validateErrorsDocument = `name: deploy
version: "3"
tasks:
  - id: copy
    uses: file
    with:
      op: copy
      srcs: [b]
`

const validateWarningsDocument = `name: deploy
version: "3"
tasks:
  - id: copy
    uses: file
    with:
      op: copy
      src: a
      srcs: [b]
`

func writeDocument(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "deploy.ork.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write document: %v", err)
	}
	return path
}

func TestOrkflowValidate_ExitStatusFollowsSeverity(t *testing.T) {
	f := interfaceFixture(t, stubResolver{fileReference: fileResolution(t)})

	errored := f.run("orkflow", "validate", writeDocument(t, validateErrorsDocument))
	if errored.code == exitOK {
		t.Fatalf("a document with an ERROR exited 0:\n%s", errored.stdout)
	}
	requireGolden(t, "orkflow-validate-errors.golden", errored.stdout)

	warned := f.run("orkflow", "validate", writeDocument(t, validateWarningsDocument)).requireExit(t, exitOK)
	requireGolden(t, "orkflow-validate-warnings.golden", warned.stdout)
	if !strings.HasSuffix(warned.stdout, "Valid orkflow deploy (version 3), 1 warning\n") {
		t.Fatalf("a warned document does not end with the valid line:\n%s", warned.stdout)
	}
}

func TestApply_PrintsInterfaceWarnings(t *testing.T) {
	f := interfaceFixture(t, stubResolver{fileReference: fileResolution(t)})
	applied := f.run("apply", writeDocument(t, validateWarningsDocument)).requireExit(t, exitOK)
	requireGolden(t, "apply-warnings.golden", applied.stdout)
	if !strings.Contains(applied.stdout, "Warnings:\n  - tasks[0].with.srcs: with.srcs is not declared by the interface of") {
		t.Fatalf("apply did not print the interface warning:\n%s", applied.stdout)
	}
}
