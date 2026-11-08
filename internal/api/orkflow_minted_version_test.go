package api_test

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// mintedVersionShape is a UUIDv7 in lowercase base32: 26 characters, no prefix.
var mintedVersionShape = regexp.MustCompile(`^[0-7][0-9a-hjkmnp-tv-z]{25}$`)

// legacyVersionShape is what an earlier release derived: a hex sha256.
var legacyVersionShape = regexp.MustCompile(`^[0-9a-f]{64}$`)

const (
	unpinnedDoc = "apiVersion: orkstack/v1\nkind: Orkflow\nname: minted\ntasks:\n  - id: one\n    uses: busybox:latest\n"
	editedDoc   = unpinnedDoc + "# an edit\n"
	pinnedDoc   = "apiVersion: orkstack/v1\nkind: Orkflow\nname: minted\nversion: 1.2.3\ntasks:\n  - id: one\n    uses: busybox:latest\n"
)

func mintedService(t *testing.T) (*api.Service, *engine.MemoryOrkflowStore) {
	t.Helper()
	store := engine.NewMemoryOrkflowStore()
	return api.NewService(api.Config{
		Orkflows: store,
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
	}), store
}

func applyMintedDoc(t *testing.T, svc *api.Service, doc string) *orkstackv1.ApplyOrkflowResponse {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return resp.Msg
}

// contentHashOf is what the apply path keys a document's version by.
func contentHashOf(t *testing.T, svc *api.Service, doc string) string {
	t.Helper()
	hash, err := dsl.ContentHash([]byte(doc), validate(t, svc, doc).GetOrkflow())
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	return hash
}

func TestApplyOrkflow_AnUnpinnedDocumentGetsAMintedVersion(t *testing.T) {
	svc, _ := mintedService(t)
	applied := applyMintedDoc(t, svc, unpinnedDoc)
	wf := applied.GetOrkflow()
	if !applied.GetCreated() {
		t.Fatal("the first apply reported created=false")
	}
	if !mintedVersionShape.MatchString(wf.GetVersion()) {
		t.Fatalf("version = %q, want a match for %s", wf.GetVersion(), mintedVersionShape)
	}
	// The digest covers the version, so it has to be the digest of what landed.
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		t.Fatalf("DefinitionDigest: %v", err)
	}
	if digest != wf.GetDefinitionDigest() {
		t.Fatalf("stored digest %q is not the digest of the stored document (%q)", wf.GetDefinitionDigest(), digest)
	}
}

// TestApplyOrkflow_ReapplyingAnUnchangedDocumentIsANoOp is docs/DSL.md's promise:
// a repeat lands on the version it already has, keeps its stamp, adds no version
// and moves no head. A second service over the same store stands for a replica
// that never saw the first apply: the answer is in the store, not in a process.
func TestApplyOrkflow_ReapplyingAnUnchangedDocumentIsANoOp(t *testing.T) {
	svc, store := mintedService(t)
	first := applyMintedDoc(t, svc, unpinnedDoc).GetOrkflow()

	replica := api.NewService(api.Config{Orkflows: store, Runs: engine.NewRunStore(), Bus: engine.NewEventBus(), Starter: &fakeStarter{}})
	for _, s := range []*api.Service{svc, replica} {
		again := applyMintedDoc(t, s, unpinnedDoc)
		if again.GetCreated() {
			t.Fatal("re-applying an unchanged document reported created=true")
		}
		if !proto.Equal(again.GetOrkflow(), first) {
			t.Fatalf("re-apply returned %v, want the stored %v", again.GetOrkflow(), first)
		}
	}
	catalog, err := store.Versions(context.Background(), "", "minted")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 1 || catalog.Head != first.GetVersion() {
		t.Fatalf("catalog = %d versions, head %q; want one, head %q", len(catalog.All), catalog.Head, first.GetVersion())
	}
}

func TestApplyOrkflow_AnEditMintsANewVersionThatSortsAfterTheOld(t *testing.T) {
	svc, store := mintedService(t)
	first := applyMintedDoc(t, svc, unpinnedDoc).GetOrkflow().GetVersion()
	second := applyMintedDoc(t, svc, editedDoc)
	if !second.GetCreated() || second.GetOrkflow().GetVersion() == first {
		t.Fatalf("an edit = created %v, version %q; want a new version beside %q",
			second.GetCreated(), second.GetOrkflow().GetVersion(), first)
	}
	if second.GetOrkflow().GetVersion() <= first {
		t.Fatalf("version %q does not sort after %q", second.GetOrkflow().GetVersion(), first)
	}
	// Going back to the earlier bytes lands on their version again. It is a
	// repeat of a stored version, so the head stays where it is.
	back := applyMintedDoc(t, svc, unpinnedDoc)
	if back.GetCreated() || back.GetOrkflow().GetVersion() != first {
		t.Fatalf("restoring the earlier bytes = created %v, version %q; want no write and %q",
			back.GetCreated(), back.GetOrkflow().GetVersion(), first)
	}
	catalog, err := store.Versions(context.Background(), "", "minted")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 2 || catalog.Head != second.GetOrkflow().GetVersion() {
		t.Fatalf("catalog = %d versions, head %q", len(catalog.All), catalog.Head)
	}
}

// TestApplyOrkflow_APinnedVersionAndItsDigestAreUnchanged: minting is for a
// document that pins nothing. A pinned one keeps its version and the digest it
// always had, and leaves no claim behind.
func TestApplyOrkflow_APinnedVersionAndItsDigestAreUnchanged(t *testing.T) {
	svc, store := mintedService(t)
	wf := applyMintedDoc(t, svc, pinnedDoc).GetOrkflow()
	if wf.GetVersion() != "1.2.3" {
		t.Fatalf("version = %q, want the pinned 1.2.3", wf.GetVersion())
	}
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		t.Fatalf("DefinitionDigest: %v", err)
	}
	if digest != wf.GetDefinitionDigest() || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("digest = %q, want the document's own %q", wf.GetDefinitionDigest(), digest)
	}
	hash, err := dsl.ContentHash([]byte(pinnedDoc), wf)
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	if found, err := store.FindDerivedVersion(context.Background(), "", "minted", hash); err != nil || found != "" {
		t.Fatalf("FindDerivedVersion after a pinned apply = %q, %v; want no claim", found, err)
	}
}

// TestValidateOrkflow_ReportsOnlyAVersionThatExists settles what validation says
// without storing anything: no version for content never applied, and exactly
// the stored version once it has been.
func TestValidateOrkflow_ReportsOnlyAVersionThatExists(t *testing.T) {
	svc, store := mintedService(t)
	ctx := context.Background()

	before := validate(t, svc, unpinnedDoc).GetOrkflow()
	if before.GetVersion() != "" || before.GetDefinitionDigest() != "" {
		t.Fatalf("before any apply: version %q, digest %q; want both empty", before.GetVersion(), before.GetDefinitionDigest())
	}
	hash, err := dsl.ContentHash([]byte(unpinnedDoc), before)
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	if found, err := store.FindDerivedVersion(ctx, "", "minted", hash); err != nil || found != "" {
		t.Fatalf("validation wrote a claim: %q, %v", found, err)
	}
	if _, err := store.Get(ctx, "", "minted", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("validation wrote a definition: %v", err)
	}

	stored := applyMintedDoc(t, svc, unpinnedDoc).GetOrkflow()
	after := validate(t, svc, unpinnedDoc).GetOrkflow()
	if after.GetVersion() != stored.GetVersion() || after.GetDefinitionDigest() != stored.GetDefinitionDigest() {
		t.Fatalf("after the apply: version %q, digest %q; want the stored %q, %q",
			after.GetVersion(), after.GetDefinitionDigest(), stored.GetVersion(), stored.GetDefinitionDigest())
	}
	if edited := validate(t, svc, editedDoc).GetOrkflow(); edited.GetVersion() != "" {
		t.Fatalf("an unapplied edit reports version %q, want none", edited.GetVersion())
	}
}

// TestPlanRun_AnUnappliedDocumentHasNoVersion: a plan of a file is a read, so it
// reports what validation does, and its refusal names the Orkflow without a
// dangling "@".
func TestPlanRun_AnUnappliedDocumentHasNoVersion(t *testing.T) {
	svc, _ := mintedService(t)
	plan, err := svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{Yaml: unpinnedDoc}))
	if err != nil {
		t.Fatalf("PlanRun: %v", err)
	}
	if plan.Msg.GetOrkflowVersion() != "" || plan.Msg.GetDefinitionDigest() != "" {
		t.Fatalf("plan version %q, digest %q; want both empty", plan.Msg.GetOrkflowVersion(), plan.Msg.GetDefinitionDigest())
	}
	_, err = svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{
		Yaml: unpinnedDoc, AssumeFailed: []string{"nope"},
	}))
	if err == nil || !strings.Contains(err.Error(), "is not a task of minted") || strings.Contains(err.Error(), "minted@") {
		t.Fatalf("refusal = %v, want it to name minted with no version", err)
	}
}

// seedLegacyVersion stores a document the way an earlier release did: its version
// IS its content hash, and no claim exists.
func seedLegacyVersion(t *testing.T, svc *api.Service, store *engine.MemoryOrkflowStore, doc string) *orkstackv1.Orkflow {
	t.Helper()
	wf := proto.Clone(validate(t, svc, doc).GetOrkflow()).(*orkstackv1.Orkflow)
	hash, err := dsl.ContentHash([]byte(doc), wf)
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	wf.Version = hash
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		t.Fatalf("DefinitionDigest: %v", err)
	}
	wf.DefinitionDigest = digest
	wf.CreatedAt = timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	stored, created, err := store.Put(context.Background(), wf, artifacts.NoHeadExpectation())
	if err != nil || !created {
		t.Fatalf("seed legacy version: created %v, %v", created, err)
	}
	return stored
}

// TestApplyOrkflow_AStoredSHA256VersionStaysValidAndCoexists is the upgrade: a
// document stored under its hash stays a no-op to re-apply, validation names that
// version, runs can pin it, and an edit lands a minted version beside it.
func TestApplyOrkflow_AStoredSHA256VersionStaysValidAndCoexists(t *testing.T) {
	svc, store := mintedService(t)
	ctx := context.Background()
	legacy := seedLegacyVersion(t, svc, store, unpinnedDoc)
	if !legacyVersionShape.MatchString(legacy.GetVersion()) {
		t.Fatalf("seeded version = %q, want a hex sha256", legacy.GetVersion())
	}

	if got := validate(t, svc, unpinnedDoc).GetOrkflow(); got.GetVersion() != legacy.GetVersion() {
		t.Fatalf("validate reports %q, want the stored %q", got.GetVersion(), legacy.GetVersion())
	}
	again := applyMintedDoc(t, svc, unpinnedDoc)
	if again.GetCreated() || !proto.Equal(again.GetOrkflow(), legacy) {
		t.Fatalf("re-apply = created %v, %v; want the untouched %v", again.GetCreated(), again.GetOrkflow(), legacy)
	}

	edited := applyMintedDoc(t, svc, editedDoc).GetOrkflow()
	if !mintedVersionShape.MatchString(edited.GetVersion()) {
		t.Fatalf("edit landed as %q, want a minted version", edited.GetVersion())
	}
	catalog, err := store.Versions(ctx, "", "minted")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 2 || catalog.Head != edited.GetVersion() {
		t.Fatalf("catalog = %d versions, head %q; want both, head %q", len(catalog.All), catalog.Head, edited.GetVersion())
	}

	// Each shape can be read, pinned by a run, and promoted to.
	for _, version := range []string{legacy.GetVersion(), edited.GetVersion()} {
		got, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "minted", Version: version}))
		if err != nil || got.Msg.GetOrkflow().GetVersion() != version {
			t.Fatalf("GetOrkflow(%s) = %v, %v", version, got, err)
		}
		run, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "minted", OrkflowVersion: version}))
		if err != nil || run.Msg.GetRun().GetOrkflowVersion() != version {
			t.Fatalf("SubmitRun pinned to %s = %v, %v", version, run, err)
		}
		if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(&orkstackv1.SetCurrentOrkflowVersionRequest{Name: "minted", Version: version})); err != nil {
			t.Fatalf("SetCurrentOrkflowVersion(%s): %v", version, err)
		}
	}
}

// TestApplyOrkflow_ConcurrentAppliesOfOneNewDocumentConverge is the in-memory
// half of the race; the bucket's half is
// TestIntegration_ConcurrentAppliesAcrossReplicasConverge.
func TestApplyOrkflow_ConcurrentAppliesOfOneNewDocumentConverge(t *testing.T) {
	svc, store := mintedService(t)
	const appliers = 16
	versions := make([]string, appliers)
	created := make([]bool, appliers)
	var wg sync.WaitGroup
	for i := range appliers {
		wg.Go(func() {
			resp, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: unpinnedDoc}))
			if err != nil {
				t.Errorf("ApplyOrkflow: %v", err)
				return
			}
			versions[i], created[i] = resp.Msg.GetOrkflow().GetVersion(), resp.Msg.GetCreated()
		})
	}
	wg.Wait()
	creators := 0
	for i, version := range versions {
		if version != versions[0] {
			t.Fatalf("applier %d landed on %q, applier 0 on %q", i, version, versions[0])
		}
		if created[i] {
			creators++
		}
	}
	if creators != 1 {
		t.Fatalf("%d appliers reported created=true, want exactly one", creators)
	}
	catalog, err := store.Versions(context.Background(), "", "minted")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 1 {
		t.Fatalf("%d versions stored, want one", len(catalog.All))
	}
}

// TestApplyOrkflow_ADeletedOrkflowStartsOver: deleting an Orkflow forgets its
// content too, so the same document applied afterwards is a new version.
func TestApplyOrkflow_ADeletedOrkflowStartsOver(t *testing.T) {
	svc, store := mintedService(t)
	ctx := context.Background()
	first := applyMintedDoc(t, svc, unpinnedDoc).GetOrkflow().GetVersion()
	if err := store.Delete(ctx, "", "minted"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	again := applyMintedDoc(t, svc, unpinnedDoc)
	if !again.GetCreated() || again.GetOrkflow().GetVersion() == first {
		t.Fatalf("apply after Delete = created %v, version %q; want a new version, not %q",
			again.GetCreated(), again.GetOrkflow().GetVersion(), first)
	}
}

// claimFailingStore answers reads but refuses a claim.
type claimFailingStore struct {
	*engine.MemoryOrkflowStore
	puts int
}

var errClaimRefused = errors.New("object store refused the claim")

func (s *claimFailingStore) ClaimDerivedVersion(context.Context, string, string, string, string) (string, error) {
	return "", errClaimRefused
}

func (s *claimFailingStore) Put(ctx context.Context, wf *orkstackv1.Orkflow, expected artifacts.HeadExpectation) (*orkstackv1.Orkflow, bool, error) {
	s.puts++
	return s.MemoryOrkflowStore.Put(ctx, wf, expected)
}

// TestApplyOrkflow_AClaimThatCannotBeWrittenStoresNothing: object storage failing
// must not produce a success, and no version lands without a claim.
func TestApplyOrkflow_AClaimThatCannotBeWrittenStoresNothing(t *testing.T) {
	store := &claimFailingStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}
	svc := api.NewService(api.Config{Orkflows: store})
	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: unpinnedDoc}))
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), errClaimRefused.Error()) {
		t.Fatalf("ApplyOrkflow = %v, want Unavailable naming the refusal", err)
	}
	if store.puts != 0 {
		t.Fatalf("Put was called %d times, want none", store.puts)
	}
}

type findFailingStore struct{ *engine.MemoryOrkflowStore }

func (s *findFailingStore) FindDerivedVersion(context.Context, string, string, string) (string, error) {
	return "", errClaimRefused
}

// TestValidateOrkflow_AStoreThatCannotAnswerIsAFailure: validation cannot say
// which version a document has while the store is down, and does not guess.
func TestValidateOrkflow_AStoreThatCannotAnswerIsAFailure(t *testing.T) {
	store := &findFailingStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}
	svc := api.NewService(api.Config{Orkflows: store})
	_, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: unpinnedDoc}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("ValidateOrkflow = %v, want Unavailable", err)
	}
	// A pinned document asks the store nothing about its version.
	if _, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: pinnedDoc})); err != nil {
		t.Fatalf("ValidateOrkflow of a pinned document: %v", err)
	}
}

// TestApplyOrkflow_ARefusedEditLeavesNoVersionAndItsRetryLands: a head refusal
// comes before the claim, and a claim stranded by a Put that failed afterwards
// names a version the next apply stores.
func TestApplyOrkflow_ARefusedEditLeavesNoVersionAndItsRetryLands(t *testing.T) {
	svc, store := mintedService(t)
	ctx := context.Background()
	head := applyMintedDoc(t, svc, unpinnedDoc).GetOrkflow().GetVersion()

	_, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: editedDoc, ExpectedHeadVersion: "not-the-head", BaseVersion: head,
	}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("stale edit = %v, want FailedPrecondition", err)
	}
	if got := validate(t, svc, editedDoc).GetOrkflow().GetVersion(); got != "" {
		t.Fatalf("a refused edit left version %q", got)
	}

	const strandedVersion = "01h455vb4pex5vsknk084sn02q"
	stranded, err := store.ClaimDerivedVersion(ctx, "", "minted", contentHashOf(t, svc, editedDoc), strandedVersion)
	if err != nil || stranded != strandedVersion {
		t.Fatalf("claim = %q, %v", stranded, err)
	}
	landed := applyMintedDoc(t, svc, editedDoc)
	if !landed.GetCreated() || landed.GetOrkflow().GetVersion() != stranded {
		t.Fatalf("apply after a stranded claim = created %v, version %q; want it stored under %q",
			landed.GetCreated(), landed.GetOrkflow().GetVersion(), stranded)
	}
}

// TestValidateOrkflow_InterfaceAdviceNamesAnUnappliedDocumentPlainly: the advice
// for a document with no version yet never reads "name@", and apply's warning
// still names the version it landed.
func TestValidateOrkflow_InterfaceAdviceNamesAnUnappliedDocumentPlainly(t *testing.T) {
	const before = "apiVersion: orkstack/v1\nkind: Orkflow\nname: minted\ntasks:\n  - id: one\n    uses: busybox:latest\n"
	const breaking = "apiVersion: orkstack/v1\nkind: Orkflow\nname: minted\ninputs:\n  region:\n    type: string\n    required: true\ntasks:\n  - id: one\n    uses: busybox:latest\n"
	svc, _ := mintedService(t)
	applyMintedDoc(t, svc, before)

	var advice string
	for _, issue := range validate(t, svc, breaking).GetIssues() {
		if issue.GetField() == "version" {
			advice = issue.GetMessage()
		}
	}
	if !strings.HasPrefix(advice, "this document breaks the declared interface") || strings.Contains(advice, "minted@ ") {
		t.Fatalf("validation's advice = %q, want it to speak of the document", advice)
	}

	applied := applyMintedDoc(t, svc, breaking)
	var warned string
	for _, warning := range applied.GetWarnings() {
		if strings.HasPrefix(warning, "version: ") {
			warned = warning
		}
	}
	if !strings.Contains(warned, "minted@"+applied.GetOrkflow().GetVersion()+" breaks") {
		t.Fatalf("apply's warning = %q, want it to name %s", warned, applied.GetOrkflow().GetVersion())
	}
}
