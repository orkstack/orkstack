package api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// Editing an old version: the two tokens an apply carries and what each one
// decides.
//
// base_version says where the edit came from — any stored version, because
// editing an old one is how a user forks forward from it. expected_head_version
// says which head the editor observed, and it is the only thing a competing
// apply can invalidate. Every assertion below is made over the STORED versions
// (the store handle this file holds), not over what the apply response claims:
// the acceptance is about what is in the store afterwards.

// storedService is versionedService with the store kept, so a test can ask the
// store what it holds rather than asking the service what it did.
func storedService(t *testing.T) (*api.Service, *engine.MemoryOrkflowStore) {
	t.Helper()
	store := engine.NewMemoryOrkflowStore()
	counter := 0
	svc := api.NewService(api.Config{
		Orkflows: store,
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	})
	return svc, store
}

// applyEdit is the request an editor sends: a document, the version it was
// derived from, and the head it saw when it fetched.
func applyEdit(
	t *testing.T,
	svc *api.Service,
	document, base, expectedHead string,
) (*orkstackv1.ApplyOrkflowResponse, error) {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml:                document,
			BaseVersion:         base,
			ExpectedHeadVersion: expectedHead,
		}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// storedVersions reads what the store holds for one orkflow: the versions
// newest first, the head, and the pointer.
func storedVersions(
	t *testing.T,
	store *engine.MemoryOrkflowStore,
	name string,
) (list string, head, current string) {
	t.Helper()
	versions, err := store.Versions(context.Background(), "", name)
	if err != nil {
		t.Fatalf("read stored versions of %s: %v", name, err)
	}
	rows := make([]string, 0, len(versions.All))
	for _, wf := range versions.All {
		rows = append(rows, wf.GetVersion())
	}
	return strings.Join(rows, ","), versions.Head, versions.Current
}

// TestApplyOrkflow_ANonHeadBaseAppliesAsTheNewHead is the milestone's first
// acceptance, and the owner's scenario in miniature: two versions, the OLDER
// one promoted, then an edit of that promoted (current, non-head) version
// applied as a new version.
//
// It lands as the new head — head is "most recently applied", and this apply is
// the most recent one — and the `current` pointer does not move, because
// applying is not promoting. Before expected_head_version existed the apply was
// refused outright (base 1.0.0 ≠ head 2.0.0), which is what made editing an old
// version impossible rather than merely unusual.
func TestApplyOrkflow_ANonHeadBaseAppliesAsTheNewHead(t *testing.T) {
	svc, store := storedService(t)
	ctx := context.Background()

	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "1.0.0"},
	)); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(1.0.0): %v", err)
	}
	list, head, current := storedVersions(t, store, "deploy")
	if list != "2.0.0,1.0.0" || head != "2.0.0" || current != "1.0.0" {
		t.Fatalf("before the edit the store holds %s (head %s, current %s), "+
			"want 2.0.0,1.0.0 head 2.0.0 current 1.0.0", list, head, current)
	}

	// The edit is derived from 1.0.0 — the version that RUNS — while the head
	// is 2.0.0 and stays 2.0.0 until this apply lands.
	edited, err := applyEdit(t, svc,
		versionedDocument("deploy", "3.0.0", "one-edited"), "1.0.0", "2.0.0")
	if err != nil {
		t.Fatalf("an edit derived from the current, non-head version was refused: %v", err)
	}
	if !edited.GetCreated() || edited.GetOrkflow().GetVersion() != "3.0.0" {
		t.Fatalf("the edit answered created=%v version=%q, want true and 3.0.0",
			edited.GetCreated(), edited.GetOrkflow().GetVersion())
	}

	// The acceptance, over the stored versions: three of them, head moved to
	// the new one, the pointer untouched.
	list, head, current = storedVersions(t, store, "deploy")
	if list != "3.0.0,2.0.0,1.0.0" {
		t.Errorf("the store holds %s, want 3.0.0,2.0.0,1.0.0", list)
	}
	if head != "3.0.0" {
		t.Errorf("head = %q, want the version just applied (3.0.0)", head)
	}
	if current != "1.0.0" {
		t.Errorf("applying moved the current pointer to %q; only a promotion moves it", current)
	}

	// The fork carries the base's body, not the head's: this is what "derived
	// from 1.0.0" means, and it is why the base is provenance rather than a
	// token to be filled in with whatever head happened to be.
	forked, err := svc.GetOrkflow(ctx, connect.NewRequest(
		&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: "3.0.0"}))
	if err != nil {
		t.Fatalf("GetOrkflow(3.0.0): %v", err)
	}
	if !strings.Contains(forked.Msg.GetOrkflow().GetSourceYaml(), "echo one-edited") {
		t.Errorf("3.0.0 does not carry the edited body:\n%s",
			forked.Msg.GetOrkflow().GetSourceYaml())
	}
	// And the version it was derived from is untouched — a fork adds, never
	// rewrites.
	base, err := svc.GetOrkflow(ctx, connect.NewRequest(
		&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: "1.0.0"}))
	if err != nil {
		t.Fatalf("GetOrkflow(1.0.0): %v", err)
	}
	if !strings.Contains(base.Msg.GetOrkflow().GetSourceYaml(), "echo one") ||
		strings.Contains(base.Msg.GetOrkflow().GetSourceYaml(), "one-edited") {
		t.Errorf("the base version changed under the fork:\n%s",
			base.Msg.GetOrkflow().GetSourceYaml())
	}

	// What runs is still what the operator promoted: an edit is not a release.
	listed, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	if listed.Msg.GetCurrentVersion() != "1.0.0" || listed.Msg.GetHeadVersion() != "3.0.0" {
		t.Errorf("the listing reports current %q head %q, want 1.0.0 and 3.0.0",
			listed.Msg.GetCurrentVersion(), listed.Msg.GetHeadVersion())
	}
}

// TestApplyOrkflow_RefusesWhenHeadMovedWhateverTheBase is the other half of the
// same contract: choosing an old base buys a fork, not an exemption from
// concurrency. If a version lands between the fetch and the apply, the apply is
// refused — whether the editor was working on the head or on a version older
// than it — and the refusal names the head that was expected and the head that
// is there.
func TestApplyOrkflow_RefusesWhenHeadMovedWhateverTheBase(t *testing.T) {
	svc, store := storedService(t)

	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))
	// Somebody else applies while both editors below have their files open.
	apply(t, svc, versionedDocument("deploy", "3.0.0", "three"))

	for _, editor := range []struct {
		what string
		base string
	}{
		{"an editor working on the version that runs", "1.0.0"},
		{"an editor working on what was the head", "2.0.0"},
	} {
		_, err := applyEdit(t, svc,
			versionedDocument("deploy", "9.0.0", editor.what), editor.base, "2.0.0")
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("%s: apply = %v (code %s), want FailedPrecondition",
				editor.what, err, connect.CodeOf(err))
		}
		for _, want := range []string{editor.base, "2.0.0", "3.0.0", "orkctl orkflow diff"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: the refusal does not mention %q: %v", editor.what, want, err)
			}
		}
	}

	// A refusal is a refusal: nothing was stored, and the head is where the
	// competing apply left it.
	list, head, current := storedVersions(t, store, "deploy")
	if list != "3.0.0,2.0.0,1.0.0" || head != "3.0.0" || current != "" {
		t.Errorf("after two refusals the store holds %s (head %s, current %q), "+
			"want the three applied versions with head 3.0.0 and no pointer",
			list, head, current)
	}

	// Re-reading the head is all it takes: the same edit, retargeted at the
	// head that is really there, lands from the same old base.
	if _, err := applyEdit(t, svc,
		versionedDocument("deploy", "9.0.0", "retargeted"), "1.0.0", "3.0.0"); err != nil {
		t.Fatalf("the retargeted edit was refused: %v", err)
	}
	if list, head, _ = storedVersions(t, store, "deploy"); head != "9.0.0" ||
		list != "9.0.0,3.0.0,2.0.0,1.0.0" {
		t.Errorf("after the retargeted edit the store holds %s (head %s), "+
			"want 9.0.0,3.0.0,2.0.0,1.0.0 with head 9.0.0", list, head)
	}
}

// TestApplyOrkflow_RefusesABaseThatNamesNoStoredVersion: provenance is a claim
// about the store, so it is checked against the store. A base nobody applied is
// NotFound naming it — a typo in `--version` is answered where it was made,
// not stored as the ancestry of a version that never existed.
func TestApplyOrkflow_RefusesABaseThatNamesNoStoredVersion(t *testing.T) {
	svc, store := storedService(t)
	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))

	_, err := applyEdit(t, svc,
		versionedDocument("deploy", "2.0.0", "two"), "0.9.9", "1.0.0")
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("a base nobody applied = %v (code %s), want NotFound", err, connect.CodeOf(err))
	}
	if !strings.Contains(err.Error(), "0.9.9") {
		t.Errorf("the refusal does not name the base that is missing: %v", err)
	}
	if list, head, _ := storedVersions(t, store, "deploy"); list != "1.0.0" || head != "1.0.0" {
		t.Errorf("a refused apply stored something: %s (head %s)", list, head)
	}
}

// TestApplyOrkflow_EmptyExpectedHeadKeepsTheOldBaseCheck pins the back-compat
// row of the rule table. A caller written before this slice sends a base and no
// expected head; it must keep EXACTLY the check it had — base equals head, or
// refused — because that caller's base is its concurrency token and nothing
// else says which head it saw.
func TestApplyOrkflow_EmptyExpectedHeadKeepsTheOldBaseCheck(t *testing.T) {
	svc, store := storedService(t)
	apply(t, svc, versionedDocument("deploy", "1.0.0", "one"))
	apply(t, svc, versionedDocument("deploy", "2.0.0", "two"))

	// base ≠ head, no expected head: the pre-M11.2 refusal, unchanged.
	_, err := applyEdit(t, svc, versionedDocument("deploy", "3.0.0", "three"), "1.0.0", "")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("an old caller's stale edit = %v (code %s), want FailedPrecondition",
			err, connect.CodeOf(err))
	}
	for _, want := range []string{"1.0.0", "2.0.0", "orkctl orkflow diff"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}

	// base == head, no expected head: accepted, as it always was.
	if _, err := applyEdit(t, svc,
		versionedDocument("deploy", "3.0.0", "three"), "2.0.0", ""); err != nil {
		t.Fatalf("an old caller's edit from the head was refused: %v", err)
	}

	// Neither token: a plain `orkctl apply file.yaml`, unchecked.
	if _, err := applyEdit(t, svc,
		versionedDocument("deploy", "4.0.0", "four"), "", ""); err != nil {
		t.Fatalf("a plain apply was refused: %v", err)
	}

	// An expected head with NO base is checked too: the UI's "retarget" action
	// sends one when the document it holds came from no stored version.
	if _, err := applyEdit(t, svc,
		versionedDocument("deploy", "5.0.0", "five"), "", "3.0.0"); connect.CodeOf(err) !=
		connect.CodeFailedPrecondition {
		t.Fatalf("an expected head that is stale = %v (code %s), want FailedPrecondition",
			err, connect.CodeOf(err))
	}
	if _, err := applyEdit(t, svc,
		versionedDocument("deploy", "5.0.0", "five"), "", "4.0.0"); err != nil {
		t.Fatalf("an expected head that is the head was refused: %v", err)
	}

	if list, head, _ := storedVersions(t, store, "deploy"); list != "5.0.0,4.0.0,3.0.0,2.0.0,1.0.0" ||
		head != "5.0.0" {
		t.Errorf("the store holds %s (head %s), want the five applied versions with head 5.0.0",
			list, head)
	}
}

// TestOrkflowReadPath_AnyVersionAnswersWithItsSourceAndTheListingMarksHeadAndCurrent
// settles the read path a version-scoped editor makes — the CLI's `edit
// --version` today and the UI's version-seeded editor next.
//
// It is exactly two calls: ListOrkflowVersions to learn what exists, which is
// head and which is current; GetOrkflow(name, version) for the bytes of the one
// the user picked. "Is head" is `version == head_version`, so no per-version
// flag is needed and none is added: head is a property of the listing, and a
// second place to keep it is a second place for it to be wrong.
func TestOrkflowReadPath_AnyVersionAnswersWithItsSourceAndTheListingMarksHeadAndCurrent(
	t *testing.T,
) {
	svc, _ := storedService(t)
	ctx := context.Background()
	for _, version := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		apply(t, svc, versionedDocument("deploy", version, "body-"+version))
	}
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "deploy", Version: "1.0.0"},
	)); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion(1.0.0): %v", err)
	}

	listed, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(
		&orkstackv1.ListOrkflowVersionsRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("ListOrkflowVersions: %v", err)
	}
	if listed.Msg.GetHeadVersion() != "3.0.0" || listed.Msg.GetCurrentVersion() != "1.0.0" {
		t.Fatalf("the listing reports head %q current %q, want 3.0.0 and 1.0.0",
			listed.Msg.GetHeadVersion(), listed.Msg.GetCurrentVersion())
	}
	if got := versionRows(listed.Msg.GetVersions()); got != "3.0.0,2.0.0,1.0.0*" {
		t.Errorf("versions = %s, want 3.0.0,2.0.0,1.0.0* (newest first, * = current)", got)
	}

	// Every row is fetchable, and each answers with ITS OWN source: an editor
	// seeded from a version must never be handed the head's text.
	for _, row := range listed.Msg.GetVersions() {
		got, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{
			Name: "deploy", Version: row.GetVersion(),
		}))
		if err != nil {
			t.Fatalf("GetOrkflow(%s): %v", row.GetVersion(), err)
		}
		source := got.Msg.GetOrkflow().GetSourceYaml()
		if !strings.Contains(source, "echo body-"+row.GetVersion()) {
			t.Errorf("version %s answered with somebody else's source:\n%s",
				row.GetVersion(), source)
		}
		// The three facts a version-scoped editor renders in its header.
		isHead := row.GetVersion() == listed.Msg.GetHeadVersion()
		if want := row.GetVersion() == "3.0.0"; isHead != want {
			t.Errorf("version %s: is-head = %v, want %v", row.GetVersion(), isHead, want)
		}
		if want := row.GetVersion() == "1.0.0"; row.GetIsCurrent() != want {
			t.Errorf("version %s: is_current = %v, want %v",
				row.GetVersion(), row.GetIsCurrent(), want)
		}
		if row.GetAppliedAt() == nil {
			t.Errorf("version %s carries no applied_at", row.GetVersion())
		}
	}

	// An empty version is still the resolved one — the pointer, then head —
	// so a reader that names none gets what RUNS, not what was last applied.
	resolved, err := svc.GetOrkflow(ctx, connect.NewRequest(
		&orkstackv1.GetOrkflowRequest{Name: "deploy"}))
	if err != nil {
		t.Fatalf("GetOrkflow(\"\"): %v", err)
	}
	if resolved.Msg.GetOrkflow().GetVersion() != "1.0.0" {
		t.Errorf("an empty version resolved to %q, want the promoted 1.0.0",
			resolved.Msg.GetOrkflow().GetVersion())
	}

	// A version nobody applied is NotFound rather than a silent fallback to
	// head: an editor seeded from a fallback would edit bytes it never showed.
	if _, err := svc.GetOrkflow(ctx, connect.NewRequest(
		&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: "9.9.9"},
	)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("GetOrkflow(9.9.9) = %v, want NotFound", err)
	}
}
