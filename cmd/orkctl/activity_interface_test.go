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
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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

// declarationOf is everything a testdata activity interface document
// declares.
func declarationOf(t *testing.T, name string) *dsl.ActivityDeclaration {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	decl, err := dsl.ParseActivityDocument(source)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return decl
}

// hostileText is what an image or a registry could send a terminal: an
// escape that retitles the window, a C1 control sequence, a bidirectional
// override, a tag character past U+FFFF, and a line break, a tab, a
// vertical tab and a form feed that would forge a line or shift a column.
// (A byte that is not UTF-8 cannot cross the API: a proto string is UTF-8.)
const hostileText = "\x1b]0;pwned\x07 \u009b31m \u202eevil \U000e0041 a\nb\tc\vd\fe\rf"

// requireTerminalSafe fails when text holds any control or format
// character, or a byte that is not UTF-8.
func requireTerminalSafe(t *testing.T, what, text string) {
	t.Helper()
	if !utf8.ValidString(text) {
		t.Fatalf("%s holds a byte that is not UTF-8:\n%q", what, text)
	}
	for _, r := range text {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			t.Fatalf("%s holds %U raw:\n%q", what, r, text)
		}
	}
}

// requireTerminalSafeOutput fails when a command's output holds anything
// requireTerminalSafe refuses on any of its lines.
func requireTerminalSafeOutput(t *testing.T, what, out string) {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		requireTerminalSafe(t, what, line)
	}
}

// hostileActivity is an image that sends text in every field inspect and
// list print: its name, reference, media type, platforms, keys, ops, types,
// default, descriptions and reasons.
func hostileActivity(t *testing.T, text string) (described, unreadable *dsl.Resolution, listed *orkstackv1.ActivityInfo) {
	t.Helper()
	value, err := structpb.NewValue(text)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("f", 64)
	image := &orkstackv1.ImageRef{Registry: "ghcr.io", Repository: "acme/hostile", IndexDigest: digest,
		Pinned: "ghcr.io/acme/hostile@" + digest, MediaType: text, Platforms: []string{text}}
	described = &dsl.Resolution{Image: image,
		Interface: &orkstackv1.ActivityInterface{SchemaVersion: 1,
			Fields: map[string]*orkstackv1.FieldSchema{"k": {Type: "string", Default: value}, "key" + text: {Type: "type" + text}},
			Ops: map[string]*orkstackv1.OpSchema{"op" + text: {Fields: map[string]*orkstackv1.FieldSchema{
				"opkey" + text: {Type: "string"}}}},
			DefaultOp: "op" + text,
			Values:    map[string]*orkstackv1.FieldSchema{"value" + text: {Type: "number"}}},
		Docs: &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: text,
			Fields: map[string]string{"k": text}, Ops: map[string]*orkstackv1.OpDocs{"op" + text: {Description: text}},
			Values: map[string]string{"value" + text: text}},
		IconError: text}
	unreadable = &dsl.Resolution{Image: image, InterfaceError: text, DocsError: text}
	listed = &orkstackv1.ActivityInfo{Name: "hostile" + text, Reference: "ghcr.io/acme/hostile:1" + text,
		IndexDigest: digest, MediaType: text, Platforms: []string{text}, InterfaceError: text}
	return described, unreadable, listed
}

// TestActivityCommands_PrintNothingATerminalWouldObey: every string a
// server, an image or a registry supplies reaches the terminal escaped, in
// inspect and in list — shown, never dropped — and its line breaks and tabs
// forge no line and shift no column: the output has exactly the lines and
// columns of the same image sending plain text. Inspect is asked about the
// image with descriptions, without them (its tables then have no
// DESCRIPTION column), and with labels it could not read.
func TestActivityCommands_PrintNothingATerminalWouldObey(t *testing.T) {
	run := func(text string) (inspected []string, listed string) {
		described, unreadable, info := hostileActivity(t, text)
		undescribed := *described
		undescribed.Docs = nil
		f := interfaceFixture(t, stubResolver{"ghcr.io/acme/hostile:1": described, "ghcr.io/acme/hostile:2": unreadable,
			"ghcr.io/acme/hostile:3": &undescribed})
		for _, reference := range []string{"ghcr.io/acme/hostile:1", "ghcr.io/acme/hostile:2", "ghcr.io/acme/hostile:3"} {
			inspected = append(inspected, f.run("activity", "inspect", reference).requireExit(t, exitOK).stdout)
		}
		return inspected, newFixture(t, info).run("activity", "list").requireExit(t, exitOK).stdout
	}
	hostile, hostileList := run(hostileText)
	plain, plainList := run("plain")
	for i, out := range hostile {
		requireTerminalSafeOutput(t, "activity inspect", out)
		if !strings.Contains(out, `\u001b]0;pwned\u0007`) || !strings.Contains(out, `a\u000ab\u0009c`) {
			t.Fatalf("activity inspect shows no escaped form of the hostile text:\n%s", out)
		}
		if got, want := strings.Count(out, "\n"), strings.Count(plain[i], "\n"); got != want {
			t.Fatalf("activity inspect printed %d lines for the hostile image, %d for the plain one:\n%s", got, want, out)
		}
	}
	requireTerminalSafeOutput(t, "activity list", hostileList)
	lines := strings.Split(strings.TrimSuffix(hostileList, "\n"), "\n")
	if want := len(strings.Split(strings.TrimSuffix(plainList, "\n"), "\n")); len(lines) != want {
		t.Fatalf("activity list printed %d lines for the hostile image, %d for the plain one:\n%s", len(lines), want, hostileList)
	}
	// The escaped name widens its column; the next column starts where the
	// header says it does.
	if header, row := lines[0], lines[1]; strings.Index(header, "REFERENCE") != strings.Index(row, "ghcr.io/acme/hostile:1") {
		t.Fatalf("activity list's REFERENCE column is out of line:\n%s", hostileList)
	}
}

// TestActivityInspect_CollapsesAndCutsDescriptions: a description's runs of
// space, line breaks and tabs show as one space, and a long one is cut to
// its cell before it is escaped, so the cut never splits an escape.
func TestActivityInspect_CollapsesAndCutsDescriptions(t *testing.T) {
	long := strings.Repeat("a", maxCellRunes-2) + "\u202e" + "bcd"
	info := &orkstackv1.ActivityInfo{IndexDigest: "sha256:" + strings.Repeat("e", 64), Reference: "ghcr.io/acme/x:1",
		Interface: &orkstackv1.ActivityInterface{SchemaVersion: 1, Fields: map[string]*orkstackv1.FieldSchema{
			"k": {Type: "string"}, "long": {Type: "string"}}},
		Docs: &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: "a\n\tb  c",
			Fields: map[string]string{"k": "d\n\te  f", "long": long}}}
	var out strings.Builder
	if err := renderActivityInspect(&out, info); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Description a b c\n", " d e f\n", strings.Repeat("a", maxCellRunes-2) + `\u202e` + "…\n"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("inspect lacks %q:\n%s", want, out.String())
		}
	}
}

// TestActivityInspect_AnUnresolvedReferenceDeclaresNothing: a reference that
// did not resolve has no description line and an Icon line of "-", not
// "none": nothing was read, so nothing is said about what it declares.
func TestActivityInspect_AnUnresolvedReferenceDeclaresNothing(t *testing.T) {
	var out strings.Builder
	info := &orkstackv1.ActivityInfo{Name: "migrate", Reference: "ghcr.io/acme/migrate:v3",
		Docs: &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: "never read"}, DocsError: "never read either"}
	if err := renderActivityInspect(&out, info); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Description") || !strings.Contains(out.String(), "Icon        -\n") {
		t.Fatalf("inspect of an unresolved reference:\n%s", out.String())
	}
}

// TestCellText_CutsAtTheCellWidth: a description of exactly the cell's
// width is whole; one character more is cut with an ellipsis.
func TestCellText_CutsAtTheCellWidth(t *testing.T) {
	fits := strings.Repeat("a", maxCellRunes)
	if got := cellText(fits); got != fits {
		t.Fatalf("cellText of %d characters = %q, want it whole", maxCellRunes, got)
	}
	if got := cellText(fits + "b"); got != strings.Repeat("a", maxCellRunes-1)+"…" {
		t.Fatalf("cellText of %d characters = %q, want it cut with an ellipsis", maxCellRunes+1, got)
	}
}

// FuzzTerminalSafe: whatever a server, an image or a registry sends, what
// terminalSafe returns is valid UTF-8 with no control or format character.
func FuzzTerminalSafe(f *testing.F) {
	for _, seed := range []string{"plain", "\x1b]0;pwn\x07", "\u009b31m\u202eevil", "\U000e0041", "\xff\xfe", "日本語"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		requireTerminalSafe(t, "terminalSafe", terminalSafe(text))
	})
}

// stubResolver is the api.ImageResolver seam with fixed answers per
// reference: the registry is the one thing the CLI fixture does not run.
type stubResolver map[string]*dsl.Resolution

func (s stubResolver) Inspect(_ context.Context, uses, reference string) (*dsl.Resolution, error) {
	res, ok := s[reference]
	if !ok {
		return nil, &dsl.ResolveError{Reference: reference, Code: dsl.ReasonImageNotFound, Detail: "the stub registry has no such image"}
	}
	out := *res
	out.Image = proto.Clone(res.Image).(*orkstackv1.ImageRef)
	out.Image.Reference = uses
	return &out, nil
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
		&orkstackv1.ActivityInfo{Name: "json", Reference: fakeRegistry + "/orkstack/activity-json:dev", IndexDigest: "sha256:" + strings.Repeat("1", 64),
			MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64"}, ResolvedAt: resolved,
			Interface: firstPartyInterface(t, "activity-json")},
		&orkstackv1.ActivityInfo{Name: "git", Reference: fakeRegistry + "/orkstack/activity-git:dev", IndexDigest: "sha256:" + strings.Repeat("2", 64),
			MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64"}, ResolvedAt: resolved,
			Interface: firstPartyInterface(t, "activity-git")},
		&orkstackv1.ActivityInfo{Name: "template", Reference: fakeRegistry + "/orkstack/activity-template:dev", IndexDigest: "sha256:" + strings.Repeat("3", 64),
			MediaType: dsl.MediaTypeOCIIndex, Platforms: []string{"linux/amd64"}, ResolvedAt: resolved,
			Interface: firstPartyInterface(t, "activity-template")},
	)
	human := f.run("activity", "list").requireExit(t, exitOK).stdout
	requireGolden(t, "activity-list.golden", human)
	for _, want := range []string{"7 ops", "none", "malformed", "2 ops", "5 keys, 2 ops", "1 op"} {
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
	described := declarationOf(t, "activity-described.ork.yaml")
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
		"ghcr.io/acme/described:1": {Image: plainImage("acme/described"), Interface: described.Interface,
			Icon: described.Icon, Docs: described.Docs},
		"ghcr.io/acme/unreadable:1": {Image: plainImage("acme/unreadable"), Interface: keysOnly,
			IconError: "holds the element \"script\"\x1b]0;title\x07; an icon holds only path, circle, ellipse, rect, line, polyline, polygon and g",
			DocsError: `describes the shared with: key "dest", which the interface does not declare; an image built FROM another inherits every label it does not set`},
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
		// An icon and descriptions: a Description line, the Icon's element count,
		// and a DESCRIPTION column on each table that has one.
		{[]string{"activity", "inspect", "ghcr.io/acme/described:1"}, "activity-inspect-described.golden"},
		{[]string{"--json", "activity", "inspect", "ghcr.io/acme/described:1"}, "activity-inspect-described.json.golden"},
		// An icon or descriptions the engine could not read say why, and the
		// interface still renders.
		{[]string{"activity", "inspect", "ghcr.io/acme/unreadable:1"}, "activity-inspect-unreadable.golden"},
	} {
		requireGolden(t, tc.golden, f.run(tc.args...).requireExit(t, exitOK).stdout)
	}
}

// TestActivityInspect_RendersTheTransformImages: the headline of each
// transform image's declared interface, through the real inspect command.
func TestActivityInspect_RendersTheTransformImages(t *testing.T) {
	resolution := func(image, digit string) *dsl.Resolution {
		digest := "sha256:" + strings.Repeat(digit, 64)
		return &dsl.Resolution{
			Image: &orkstackv1.ImageRef{Registry: fakeRegistry, Repository: "orkstack/" + image, IndexDigest: digest,
				Pinned: fakeRegistry + "/orkstack/" + image + "@" + digest, MediaType: dsl.MediaTypeOCIIndex},
			Interface: firstPartyInterface(t, image),
		}
	}
	f := interfaceFixture(t, stubResolver{
		"ghcr.io/acme/json:1":     resolution("activity-json", "1"),
		"ghcr.io/acme/git:1":      resolution("activity-git", "2"),
		"ghcr.io/acme/template:1": resolution("activity-template", "3"),
	})
	for reference, headline := range map[string]string{
		"ghcr.io/acme/json:1":     "schema 1 · selected by op: (2 ops, op required)",
		"ghcr.io/acme/git:1":      "schema 1 · selected by op: (2 ops, op required) · 5 keys shared by every op",
		"ghcr.io/acme/template:1": "schema 1 · selected by op: (1 op, op required)",
	} {
		out := f.run("activity", "inspect", reference).requireExit(t, exitOK).stdout
		var line string
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, "schema 1") {
				line = l
			}
		}
		if !strings.HasSuffix(strings.TrimSpace(line), headline) {
			t.Errorf("activity inspect %s: interface line %q, want it to read %q\n%s", reference, line, headline, out)
		}
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

const validateErrorsDocument = `apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: "3"
tasks:
  - id: copy
    uses: file
    with:
      op: copy
      srcs: [b]
`

const validateWarningsDocument = `apiVersion: orkstack/v1
kind: Orkflow
name: deploy
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

// TestTerminalSafe_EscapesWhatATerminalWouldObey: a control, a C1 control, a
// bidirectional or zero-width character, a tag character past U+FFFF and a
// byte that is not UTF-8 never reach the terminal raw; every other character
// does, as itself.
func TestTerminalSafe_EscapesWhatATerminalWouldObey(t *testing.T) {
	for in, want := range map[string]string{
		"linux/amd64":              "linux/amd64",
		"\x1b]0;pwned\x07/amd64":   `\u001b]0;pwned\u0007/amd64`,
		"x\u009b31m\u202eevil":     `x\u009b31m\u202eevil`,
		"a\u200bb\u2028c\u2029d":   `a\u200bb\u2028c\u2029d`,
		"a\xffb":                   `a\ufffdb`,
		"a\U000e0041b":             `a\U000e0041b`,
		"ünïcödé — 日本語 \"quoted\"": "ünïcödé — 日本語 \"quoted\"",
	} {
		if got := terminalSafe(in); got != want {
			t.Errorf("terminalSafe(%q) = %q, want %q", in, got, want)
		}
	}
	requireTerminalSafe(t, "terminalSafe", terminalSafe("a\xffb\xc3"))
}

// TestActivityInspect_ShowsNoOpsTableWithoutAnOpDescription: an op whose
// keys are described but which has no description of its own adds no OPS
// table.
func TestActivityInspect_ShowsNoOpsTableWithoutAnOpDescription(t *testing.T) {
	info := &orkstackv1.ActivityInfo{IndexDigest: "sha256:" + strings.Repeat("e", 64), Reference: "ghcr.io/acme/x:1",
		Interface: &orkstackv1.ActivityInterface{SchemaVersion: 1, Ops: map[string]*orkstackv1.OpSchema{
			"copy": {Fields: map[string]*orkstackv1.FieldSchema{"src": {Type: "string"}}}}},
		Docs: &orkstackv1.ActivityDocs{SchemaVersion: 1, Ops: map[string]*orkstackv1.OpDocs{
			"copy": {Fields: map[string]string{"src": "The file."}}}}}
	var out strings.Builder
	if err := renderActivityInspect(&out, info); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "OPS") || !strings.Contains(out.String(), "The file.") {
		t.Fatalf("inspect:\n%s", out.String())
	}
}
