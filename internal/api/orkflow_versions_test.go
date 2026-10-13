package api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// versionedService wires a Service over the in-memory catalog for the version
// operations, which need no cluster: a promotion is a store write, not a run.
func versionedService(t *testing.T) *api.Service {
	t.Helper()
	counter := 0
	return api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
}

// versionedDocument is one orkflow whose body carries `marker`, so two versions
// are distinguishable by what they would run rather than by their number alone.
func versionedDocument(name, version, marker string) string {
	return fmt.Sprintf(`name: %s
version: "%s"
tasks:
  - id: build
    uses: shell
    with:
      op: exec
      command: echo %s
`, name, version, marker)
}

func apply(t *testing.T, svc *api.Service, document string) *orkstackv1.ApplyOrkflowResponse {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: document}))
	if err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return resp.Msg
}

// TestOrkflowVersions_PointerDecidesWhatRunsAndListsIt is the versioning
// contract at the API: versions are immutable, a pointer decides which one an
// empty version means, and an orkflow nobody promoted answers with its newest
// applied version.
func TestOrkflowVersions_PointerDecidesWhatRunsAndListsIt(t *testing.T) {
	svc := versionedService(t)
	ctx := context.Background()

	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))

	// Before any promotion: the most recently applied version, and the listing
	// says the pointer was never set.
	listed, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	if got := listed.Msg.GetCurrentVersion(); got != "" {
		t.Errorf("current_version before any promotion = %q, want \"\"", got)
	}
	if got := listed.Msg.GetHeadVersion(); got != "2.0.0" {
		t.Errorf("head_version = %q, want 2.0.0", got)
	}
	if got := versionRows(listed.Msg.GetVersions()); got != "2.0.0*,1.0.0" {
		t.Errorf("versions = %s, want 2.0.0*,1.0.0 (newest first, * = current)", got)
	}
	requireResolvedVersion(t, svc, "deploy", "2.0.0", "before any promotion")

	// Promote the older version: the pointer decides, not the apply order.
	promoted, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "1.0.0"}))
	if err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(1.0.0): %v", err)
	}
	if promoted.Msg.GetPreviousVersion() != "" || promoted.Msg.GetCurrentVersion() != "1.0.0" {
		t.Errorf("promotion reported %q -> %q, want \"\" -> 1.0.0",
			promoted.Msg.GetPreviousVersion(), promoted.Msg.GetCurrentVersion())
	}
	requireResolvedVersion(t, svc, "deploy", "1.0.0", "after promoting 1.0.0")

	listed, err = svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	if got := versionRows(listed.Msg.GetVersions()); got != "2.0.0,1.0.0*" {
		t.Errorf("versions after promotion = %s, want 2.0.0,1.0.0*", got)
	}
	if listed.Msg.GetCurrentVersion() != "1.0.0" || listed.Msg.GetHeadVersion() != "2.0.0" {
		t.Errorf("current/head = %q/%q, want 1.0.0/2.0.0",
			listed.Msg.GetCurrentVersion(), listed.Msg.GetHeadVersion())
	}

	// The catalog listing agrees with what a run would execute: showing head
	// while submit runs the promoted version would report a version this
	// deployment does not run.
	catalog, err := svc.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{}))
	if err != nil {
		t.Fatalf("ListOrkflows: %v", err)
	}
	if len(catalog.Msg.GetOrkflows()) != 1 ||
		catalog.Msg.GetOrkflows()[0].GetVersion() != "1.0.0" {
		t.Errorf("ListOrkflows reports %v, want deploy@1.0.0",
			renderVersions(catalog.Msg.GetOrkflows()))
	}

	// Rollback reports what it replaced, which is what makes a rollback a
	// single command rather than a lookup and a guess.
	rolled, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "2.0.0"}))
	if err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(2.0.0): %v", err)
	}
	if rolled.Msg.GetPreviousVersion() != "1.0.0" {
		t.Errorf("rollback replaced %q, want 1.0.0", rolled.Msg.GetPreviousVersion())
	}
	requireResolvedVersion(t, svc, "deploy", "2.0.0", "after rolling forward")
}

// TestSetCurrentOrkflowVersion_RefusesAVersionThatDoesNotExist is the read-side
// guarantee stated as a write-side refusal: a pointer is never written to a
// version nobody applied, so no reader can observe one.
func TestSetCurrentOrkflowVersion_RefusesAVersionThatDoesNotExist(t *testing.T) {
	svc := versionedService(t)
	ctx := context.Background()
	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))

	_, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "9.9.9"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("promote to an unknown version = %v (code %s), want NotFound",
			err, connect.CodeOf(err))
	}
	requireResolvedVersion(t, svc, "deploy", "1.0.0", "after a refused promotion")

	// An unknown orkflow, and an empty version, are both refused before
	// anything is written.
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "ghost", Version: "1.0.0"},
	)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("promote an unknown orkflow = %v, want NotFound", err)
	}
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy"},
	)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("promote with no version = %v, want InvalidArgument", err)
	}
}

// TestApplyOrkflow_NeverRewritesAStoredVersion is immutability at the API: an
// edit is a new version, a repeat writes nothing, and a changed document under
// a stored version is refused rather than silently replacing what a pinned run
// reads.
func TestApplyOrkflow_NeverRewritesAStoredVersion(t *testing.T) {
	svc := versionedService(t)
	ctx := context.Background()

	first := apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	if !first.GetCreated() {
		t.Fatal("the first apply reported that it created nothing")
	}
	appliedAt := first.GetOrkflow().GetCreatedAt().AsTime()

	// The identical document again: no new version, and the answer is the
	// STORED document — same created_at, not this apply's clock.
	repeat := apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	if repeat.GetCreated() {
		t.Error("re-applying an identical document reported that it created a version")
	}
	if got := repeat.GetOrkflow().GetCreatedAt().AsTime(); !got.Equal(appliedAt) {
		t.Errorf("a repeat answered with created_at %s, want the stored %s", got, appliedAt)
	}
	listed, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	if len(listed.Msg.GetVersions()) != 1 {
		t.Fatalf("a repeat created a version: %s", versionRows(listed.Msg.GetVersions()))
	}

	// A changed document under the SAME pinned version is refused. The
	// alternative is rewriting the bytes a started run is still reading.
	_, err = svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: versionedDocument("deploy", "1.0.0", "something-else"),
	}))
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("re-applying a changed 1.0.0 = %v (code %s), want AlreadyExists",
			err, connect.CodeOf(err))
	}
	stored, err := svc.GetOrkflow(ctx, connect.NewRequest(
		&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: "1.0.0"}))
	if err != nil {
		t.Fatalf("GetOrkflow(1.0.0): %v", err)
	}
	if !strings.Contains(stored.Msg.GetOrkflow().GetSourceYaml(), "echo one") {
		t.Fatalf("the refused apply changed the stored version:\n%s",
			stored.Msg.GetOrkflow().GetSourceYaml())
	}

	// An edit under a NEW version is stored beside it, and both remain
	// readable: this is what "an edit creates a version" means.
	edited := apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))
	if !edited.GetCreated() {
		t.Error("applying a new version reported that it created nothing")
	}
	for version, marker := range map[string]string{"1.0.0": "echo one", "2.0.0": "echo two"} {
		got, err := svc.GetOrkflow(ctx, connect.NewRequest(
			&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: version}))
		if err != nil {
			t.Fatalf("GetOrkflow(%s): %v", version, err)
		}
		if !strings.Contains(got.Msg.GetOrkflow().GetSourceYaml(), marker) {
			t.Errorf("version %s does not carry %q:\n%s",
				version, marker, got.Msg.GetOrkflow().GetSourceYaml())
		}
	}
}

// TestApplyOrkflow_UnpinnedEditsBecomeANewVersionAutomatically is the editor's
// path: a document that pins no version gets one from its content, so editing
// and applying produces a new version without the user inventing a number, and
// re-applying the same text produces the same version and no write.
func TestApplyOrkflow_UnpinnedEditsBecomeANewVersionAutomatically(t *testing.T) {
	svc := versionedService(t)
	ctx := context.Background()
	unpinned := func(marker string) string {
		return fmt.Sprintf(`name: deploy
tasks:
  - id: build
    uses: shell
    with:
      op: exec
      command: echo %s
`, marker)
	}

	first := apply(t, svc, unpinned("one"))
	repeat := apply(t, svc, unpinned("one"))
	if first.GetOrkflow().GetVersion() != repeat.GetOrkflow().GetVersion() {
		t.Fatalf("the same document produced two versions: %q then %q",
			first.GetOrkflow().GetVersion(), repeat.GetOrkflow().GetVersion())
	}
	if repeat.GetCreated() {
		t.Error("re-applying the same unpinned document created a version")
	}

	edited := apply(t, svc, unpinned("two"))
	if edited.GetOrkflow().GetVersion() == first.GetOrkflow().GetVersion() {
		t.Fatal("an edited document kept the old version")
	}
	listed, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	if len(listed.Msg.GetVersions()) != 2 {
		t.Fatalf("two documents produced %d versions: %s",
			len(listed.Msg.GetVersions()), versionRows(listed.Msg.GetVersions()))
	}
	if listed.Msg.GetHeadVersion() != edited.GetOrkflow().GetVersion() {
		t.Errorf("head_version = %q, want the version just applied (%q)",
			listed.Msg.GetHeadVersion(), edited.GetOrkflow().GetVersion())
	}
}

// TestApplyOrkflow_ACommentOnlyEditOfAnUnpinnedDocumentIsANewVersion is
// immutability for the edit that a version derived from less than the source
// could not store at all.
//
// The two notions of "the same document" have to be one notion. An unpinned
// document's version comes from its source, and the store decides that an
// apply is a repeat by comparing the source it holds; while the derivation
// ignored comments and the comparison did not, a comment-only edit compiled to
// the SAME version carrying DIFFERENT bytes and was refused as an attempt to
// rewrite an immutable version — a refusal whose advice ("publish a new
// version") nobody could follow, because a derived version is not the author's
// to choose. Editing a comment is editing the document, so it is a version.
func TestApplyOrkflow_ACommentOnlyEditOfAnUnpinnedDocumentIsANewVersion(t *testing.T) {
	svc := versionedService(t)
	ctx := context.Background()
	const document = `name: deploy
tasks:
  - id: build
    uses: shell
    with:
      op: exec
      command: echo one
`
	const commented = "# remind me why this exists\n" + document

	first := apply(t, svc, document)
	edited := apply(t, svc, commented)
	if !edited.GetCreated() {
		t.Error("a comment-only edit reported that it created nothing")
	}
	if edited.GetOrkflow().GetVersion() == first.GetOrkflow().GetVersion() {
		t.Fatalf("a comment-only edit kept the old version %q",
			first.GetOrkflow().GetVersion())
	}

	// Each version carries its own source, and the earlier one is untouched:
	// this is the difference between adding a version and rewriting one.
	for version, want := range map[string]bool{
		first.GetOrkflow().GetVersion():  false,
		edited.GetOrkflow().GetVersion(): true,
	} {
		got, err := svc.GetOrkflow(ctx, connect.NewRequest(
			&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: version}))
		if err != nil {
			t.Fatalf("GetOrkflow(%s): %v", version, err)
		}
		if has := strings.Contains(
			got.Msg.GetOrkflow().GetSourceYaml(), "# remind me why this exists"); has != want {
			t.Errorf("version %s carries the comment = %v, want %v:\n%s",
				version, has, want, got.Msg.GetOrkflow().GetSourceYaml())
		}
	}

	// The comment is now the head, and re-applying it is still the no-op a
	// repeat has always been.
	repeat := apply(t, svc, commented)
	if repeat.GetCreated() {
		t.Error("re-applying the commented document created a second version of it")
	}
	listed, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	if len(listed.Msg.GetVersions()) != 2 {
		t.Fatalf("want the two documents that were applied: %s",
			versionRows(listed.Msg.GetVersions()))
	}
	if listed.Msg.GetHeadVersion() != edited.GetOrkflow().GetVersion() {
		t.Errorf("head_version = %q, want the edited version (%q)",
			listed.Msg.GetHeadVersion(), edited.GetOrkflow().GetVersion())
	}
}

// TestApplyOrkflow_DetectsAnEditDerivedFromAStaleVersion: an edit that says
// which head it was fetched under is refused when that is no longer the head,
// and the refusal names both versions so the user can diff them.
func TestApplyOrkflow_DetectsAnEditDerivedFromAStaleVersion(t *testing.T) {
	svc := versionedService(t)
	ctx := context.Background()

	base := apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	// Somebody else applies in the meantime — the editor is now stale.
	apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))

	_, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml:                versionedDocument("deploy", "3.0.0", "three"),
		BaseVersion:         base.GetOrkflow().GetVersion(),
		ExpectedHeadVersion: base.GetOrkflow().GetVersion(),
	}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("a stale edit = %v (code %s), want FailedPrecondition", err, connect.CodeOf(err))
	}
	for _, want := range []string{"1.0.0", "2.0.0", "orkctl orkflow diff"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	// Nothing was written: the refusal is a refusal, not a partial apply.
	if _, err := svc.GetOrkflow(ctx, connect.NewRequest(
		&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: "3.0.0"},
	)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("a refused stale edit stored 3.0.0 anyway: %v", err)
	}

	// The same edit, fetched under the CURRENT head, is accepted.
	if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml:                versionedDocument("deploy", "3.0.0", "three"),
		BaseVersion:         "2.0.0",
		ExpectedHeadVersion: "2.0.0",
	})); err != nil {
		t.Fatalf("an edit derived from the head was refused: %v", err)
	}

	// The check is against HEAD, not the pointer: promoting an older version
	// is an operator's decision about what runs, and it must not make every
	// open editor stale.
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "1.0.0"},
	)); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(1.0.0): %v", err)
	}
	if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml:                versionedDocument("deploy", "4.0.0", "four"),
		BaseVersion:         "3.0.0",
		ExpectedHeadVersion: "3.0.0",
	})); err != nil {
		t.Fatalf("an edit derived from the head was refused after a rollback: %v", err)
	}

	// Empty tokens stay unchecked: a plain `orkctl apply file.yaml` keeps
	// working with no knowledge of versions at all.
	if _, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: versionedDocument("deploy", "5.0.0", "five"),
	})); err != nil {
		t.Fatalf("an apply with no tokens was refused: %v", err)
	}
}

// TestDiffOrkflowVersions_ShowsWhatChangedBetweenTwoVersions: the diff is the
// server's, so every client renders the same answer.
func TestDiffOrkflowVersions_ShowsWhatChangedBetweenTwoVersions(t *testing.T) {
	svc := versionedService(t)
	ctx := context.Background()
	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))

	diff, err := svc.DiffOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.DiffOrkflowVersionsRequest{
			Name: "deploy", FromVersion: "1.0.0", ToVersion: "2.0.0",
		}))
	if err != nil {
		t.Fatalf("DiffOrkflowVersions: %v", err)
	}
	unified := diff.Msg.GetUnifiedDiff()
	for _, want := range []string{
		"--- a/deploy@1.0.0", "+++ b/deploy@2.0.0",
		"-      command: echo one", "+      command: echo two",
		`-version: "1.0.0"`, `+version: "2.0.0"`,
	} {
		if !strings.Contains(unified, want) {
			t.Errorf("the diff does not contain %q:\n%s", want, unified)
		}
	}
	if diff.Msg.GetFromVersion() != "1.0.0" || diff.Msg.GetToVersion() != "2.0.0" {
		t.Errorf("the diff reports %s -> %s, want 1.0.0 -> 2.0.0",
			diff.Msg.GetFromVersion(), diff.Msg.GetToVersion())
	}

	// One version against itself is an empty diff, not a page of headers.
	same, err := svc.DiffOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.DiffOrkflowVersionsRequest{
			Name: "deploy", FromVersion: "1.0.0", ToVersion: "1.0.0",
		}))
	if err != nil {
		t.Fatalf("DiffOrkflowVersions(same): %v", err)
	}
	if same.Msg.GetUnifiedDiff() != "" {
		t.Errorf("a version against itself diffed as:\n%s", same.Msg.GetUnifiedDiff())
	}

	// An empty side means the resolved version, which makes "what would
	// promoting change" one call.
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "1.0.0"},
	)); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion: %v", err)
	}
	againstCurrent, err := svc.DiffOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.DiffOrkflowVersionsRequest{Name: "deploy", ToVersion: "2.0.0"}))
	if err != nil {
		t.Fatalf("DiffOrkflowVersions(current -> 2.0.0): %v", err)
	}
	if againstCurrent.Msg.GetFromVersion() != "1.0.0" {
		t.Errorf("an empty from_version resolved to %q, want the current 1.0.0",
			againstCurrent.Msg.GetFromVersion())
	}

	// A version nobody applied is NotFound, naming what was asked for.
	if _, err := svc.DiffOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.DiffOrkflowVersionsRequest{
			Name: "deploy", FromVersion: "1.0.0", ToVersion: "9.9.9",
		},
	)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("diff against an unknown version = %v, want NotFound", err)
	}
}

// TestOrkflowVersionOperations_AreScopedToOneNamespace: two namespaces may each
// hold a "deploy", and promoting one must not decide what the other runs.
func TestOrkflowVersionOperations_AreScopedToOneNamespace(t *testing.T) {
	reg := newFakeRegistry(
		&orkstackv1.Namespace{Name: "alpha"},
		&orkstackv1.Namespace{Name: "bravo"},
	)
	svc, _ := namespacedService(t, reg)
	ctx := context.Background()

	for _, namespace := range []string{"alpha", "bravo"} {
		for _, version := range []string{"1.0.0", "2.0.0"} {
			mustApply(t, svc, versionedDocument("deploy", version, namespace+"-"+version), namespace)
		}
	}

	// Promote 1.0.0 in alpha only.
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{
			Name: "deploy", Version: "1.0.0", Namespace: "alpha",
		})); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(alpha): %v", err)
	}

	alpha, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy", Namespace: "alpha"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions(alpha): %v", err)
	}
	bravo, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy", Namespace: "bravo"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions(bravo): %v", err)
	}
	if alpha.Msg.GetCurrentVersion() != "1.0.0" {
		t.Errorf("alpha's current version = %q, want 1.0.0", alpha.Msg.GetCurrentVersion())
	}
	if bravo.Msg.GetCurrentVersion() != "" {
		t.Errorf("promoting in alpha moved bravo's pointer to %q", bravo.Msg.GetCurrentVersion())
	}

	// And the definitions themselves never cross: the diff of bravo's two
	// versions describes bravo's documents.
	diff, err := svc.DiffOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.DiffOrkflowVersionsRequest{
			Name: "deploy", FromVersion: "1.0.0", ToVersion: "2.0.0", Namespace: "bravo",
		}))
	if err != nil {
		t.Fatalf("DiffOrkflowVersions(bravo): %v", err)
	}
	if !strings.Contains(diff.Msg.GetUnifiedDiff(), "bravo-2.0.0") ||
		strings.Contains(diff.Msg.GetUnifiedDiff(), "alpha") {
		t.Errorf("bravo's diff is not bravo's documents:\n%s", diff.Msg.GetUnifiedDiff())
	}

	// A namespace nobody registered is NotFound on every one of the three.
	for _, call := range []struct {
		name string
		err  error
	}{
		{"ListOrkflowVersions", listVersionsErr(svc, "ghost")},
		{"SetCurrentOrkflowVersion", promoteErr(svc, "ghost")},
		{"DiffOrkflowVersions", diffErr(svc, "ghost")},
	} {
		if connect.CodeOf(call.err) != connect.CodeNotFound {
			t.Errorf("%s in an unknown namespace = %v, want NotFound", call.name, call.err)
		}
	}
}

func listVersionsErr(svc *api.Service, namespace string) error {
	_, err := svc.ListOrkflowVersions(context.Background(), connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy", Namespace: namespace}))
	return err
}

func promoteErr(svc *api.Service, namespace string) error {
	_, err := svc.SetCurrentOrkflowVersion(context.Background(), connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{
			Name: "deploy", Version: "1.0.0", Namespace: namespace,
		}))
	return err
}

func diffErr(svc *api.Service, namespace string) error {
	_, err := svc.DiffOrkflowVersions(context.Background(), connect.NewRequest(
		&orkstackv1.DiffOrkflowVersionsRequest{
			Name: "deploy", FromVersion: "1.0.0", ToVersion: "2.0.0", Namespace: namespace,
		}))
	return err
}

// TestSubmitRun_ExecutesTheCurrentVersion is the pointer's whole point: a
// submission that names no version runs what the pointer names, and a
// submission that pins one runs that one whatever the pointer says.
func TestSubmitRun_ExecutesTheCurrentVersion(t *testing.T) {
	counter := 0
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  starter,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
	ctx := context.Background()
	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "1.0.0"},
	)); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion: %v", err)
	}

	unpinned, err := svc.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: "deploy"}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	if got := unpinned.Msg.GetRun().GetOrkflowVersion(); got != "1.0.0" {
		t.Errorf("an unpinned submission ran version %q, want the promoted 1.0.0", got)
	}

	pinned, err := svc.SubmitRun(ctx, connect.NewRequest(
		&orkstackv1.SubmitRunRequest{OrkflowName: "deploy", OrkflowVersion: "2.0.0"}))
	if err != nil {
		t.Fatalf("SubmitRun(2.0.0): %v", err)
	}
	if got := pinned.Msg.GetRun().GetOrkflowVersion(); got != "2.0.0" {
		t.Errorf("a submission pinned to 2.0.0 ran %q", got)
	}

	// The definition handed to Temporal is the version's, not the name's: the
	// starter received the tasks the pointer selected.
	starter.mu.Lock()
	defer starter.mu.Unlock()
	if len(starter.orkflows) != 2 {
		t.Fatalf("the starter received %d orkflows, want 2", len(starter.orkflows))
	}
	// Read from the tasks, never the source: a run starts without its
	// Orkflow's source (PoolResolver.Plan clears it; the stored version keeps
	// it).
	for i, want := range []string{"echo one", "echo two"} {
		encoded, err := protojson.Marshal(&orkstackv1.Orkflow{Tasks: starter.orkflows[i].GetTasks()})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(encoded), want) {
			t.Errorf("run %d carried the wrong definition, want %q in its tasks:\n%s", i, want, encoded)
		}
		if starter.orkflows[i].GetSourceYaml() != "" {
			t.Errorf("run %d started with its Orkflow's source", i)
		}
	}
}

// requireResolvedVersion asserts what GetOrkflow with an empty version answers.
func requireResolvedVersion(t *testing.T, svc *api.Service, name, want, when string) {
	t.Helper()
	got, err := svc.GetOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.GetOrkflowRequest{Name: name}))
	if err != nil {
		t.Fatalf("GetOrkflow(resolved) %s: %v", when, err)
	}
	if got.Msg.GetOrkflow().GetVersion() != want {
		t.Fatalf("an empty version resolves to %q %s, want %q",
			got.Msg.GetOrkflow().GetVersion(), when, want)
	}
}

// versionRows renders a version listing as "v1*,v2" with * marking current.
func versionRows(versions []*orkstackv1.OrkflowVersion) string {
	out := make([]string, 0, len(versions))
	for _, v := range versions {
		row := v.GetVersion()
		if v.GetIsCurrent() {
			row += "*"
		}
		out = append(out, row)
	}
	return strings.Join(out, ",")
}

func renderVersions(orkflows []*orkstackv1.Orkflow) []string {
	out := make([]string, 0, len(orkflows))
	for _, wf := range orkflows {
		out = append(out, wf.GetName()+"@"+wf.GetVersion())
	}
	return out
}
