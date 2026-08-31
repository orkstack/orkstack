package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The durable store is the production implementation of the engine's contract.
// This assignment is the contract check: pkg/artifacts never imports
// internal/engine, so only the method set keeps the two sides together.
var _ engine.OrkflowStore = (*artifacts.S3OrkflowStore)(nil)

func orkflowFixture(name, version string, appliedAt time.Time) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:      name,
		Version:   version,
		Tasks:     []*orkstackv1.Task{{Id: "fetch", Uses: "http"}},
		CreatedAt: timestamppb.New(appliedAt),
	}
}

// inNamespace places a fixture in one namespace, so a test can say which
// namespace it means without repeating the message literal.
func inNamespace(wf *orkstackv1.Orkflow, namespace string) *orkstackv1.Orkflow {
	wf.Namespace = namespace
	return wf
}

func TestMemoryOrkflowStore_ReadsBackWhatWasApplied(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	applied := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	if _, _, err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", applied), artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, "", "hello-ork", "0.1.0")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetName() != "hello-ork" || got.GetVersion() != "0.1.0" {
		t.Fatalf("Get = %s@%s, want hello-ork@0.1.0", got.GetName(), got.GetVersion())
	}

	// The store hands out copies: a caller mutating a result cannot corrupt it.
	got.Tasks[0].Id = "mutated"
	again, err := store.Get(ctx, "", "hello-ork", "0.1.0")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if again.GetTasks()[0].GetId() != "fetch" {
		t.Error("a mutation of a returned orkflow reached the store")
	}
}

func TestMemoryOrkflowStore_EmptyVersionIsTheMostRecentlyApplied(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	if _, _, err := store.Put(ctx, orkflowFixture("hello-ork", "0.2.0", base), artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put(0.2.0): %v", err)
	}
	if _, _, err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", base.Add(time.Second)), artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put(0.1.0): %v", err)
	}

	latest, err := store.Get(ctx, "", "hello-ork", "")
	if err != nil {
		t.Fatalf("Get(latest): %v", err)
	}
	if latest.GetVersion() != "0.1.0" {
		t.Errorf("latest = %q, want 0.1.0 (applied last)", latest.GetVersion())
	}

	// The earlier version is still addressable: versions are independent.
	pinned, err := store.Get(ctx, "", "hello-ork", "0.2.0")
	if err != nil {
		t.Fatalf("Get(0.2.0): %v", err)
	}
	if pinned.GetVersion() != "0.2.0" {
		t.Errorf("pinned = %q, want 0.2.0", pinned.GetVersion())
	}
}

func TestMemoryOrkflowStore_ListsTheLatestVersionOfEveryOrkflowByName(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	for _, wf := range []*orkstackv1.Orkflow{
		orkflowFixture("zeta", "1.0.0", base),
		orkflowFixture("alpha", "0.1.0", base.Add(time.Second)),
		orkflowFixture("alpha", "0.2.0", base.Add(2*time.Second)),
	} {
		if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s@%s): %v", wf.GetName(), wf.GetVersion(), err)
		}
	}

	listed, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("List returned %d orkflows, want 2", len(listed))
	}
	if listed[0].GetName() != "alpha" || listed[0].GetVersion() != "0.2.0" {
		t.Errorf("List[0] = %s@%s, want alpha@0.2.0", listed[0].GetName(), listed[0].GetVersion())
	}
	if listed[1].GetName() != "zeta" {
		t.Errorf("List[1] = %s, want zeta", listed[1].GetName())
	}
}

func TestMemoryOrkflowStore_MissingOrkflowIsNotFound(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()

	if _, err := store.Get(ctx, "", "ghost", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get(ghost) = %v, want artifacts.ErrNotFound", err)
	}
	if _, _, err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", time.Now()), artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := store.Get(ctx, "", "hello-ork", "9.9.9"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get(unknown version) = %v, want artifacts.ErrNotFound", err)
	}
	empty, err := engine.NewMemoryOrkflowStore().List(ctx, "")
	if err != nil || len(empty) != 0 {
		t.Errorf("List(empty store) = %v, %v; want no orkflows", empty, err)
	}
}

func TestMemoryOrkflowStore_RejectsAnOrkflowItCannotAddress(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()

	for _, wf := range []*orkstackv1.Orkflow{
		{Version: "0.1.0"},
		{Name: "hello-ork"},
	} {
		if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); !errors.Is(err, artifacts.ErrInvalidOrkflow) {
			t.Errorf("Put(%q@%q) = %v, want artifacts.ErrInvalidOrkflow", wf.GetName(), wf.GetVersion(), err)
		}
	}
}

// TestMemoryOrkflowStore_TwoNamespacesHoldTheSameNameIndependently is the
// isolation property, checked against the store every unit test runs on so a
// test cannot pass here for a reason the bucket would not reproduce
// (pkg/artifacts holds the same property against the durable store).
func TestMemoryOrkflowStore_TwoNamespacesHoldTheSameNameIndependently(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	payments := inNamespace(orkflowFixture("deploy", "1.0.0", base), "payments")
	payments.Tasks[0].Id = "charge"
	shipping := inNamespace(orkflowFixture("deploy", "2.0.0", base.Add(time.Second)), "shipping")
	shipping.Tasks[0].Id = "pack"
	for _, wf := range []*orkstackv1.Orkflow{payments, shipping} {
		if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s/%s): %v", wf.GetNamespace(), wf.GetName(), err)
		}
	}

	// Same name, different namespace, different document.
	for _, tc := range []struct{ namespace, version, task string }{
		{"payments", "1.0.0", "charge"},
		{"shipping", "2.0.0", "pack"},
	} {
		got, err := store.Get(ctx, tc.namespace, "deploy", "")
		if err != nil {
			t.Fatalf("Get(%s/deploy): %v", tc.namespace, err)
		}
		if got.GetVersion() != tc.version || got.GetTasks()[0].GetId() != tc.task {
			t.Errorf("%s/deploy = %s task %q, want %s task %q",
				tc.namespace, got.GetVersion(), got.GetTasks()[0].GetId(), tc.version, tc.task)
		}
		listed, err := store.List(ctx, tc.namespace)
		if err != nil {
			t.Fatalf("List(%s): %v", tc.namespace, err)
		}
		if len(listed) != 1 || listed[0].GetVersion() != tc.version {
			t.Errorf("List(%s) = %v, want only %s", tc.namespace, listed, tc.version)
		}
	}

	// Neither is visible from the namespace that holds neither.
	if _, err := store.Get(ctx, "", "deploy", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get(default/deploy) = %v, want ErrNotFound", err)
	}
	if listed, err := store.List(ctx, ""); err != nil || len(listed) != 0 {
		t.Errorf("List(default) = %v, %v; want nothing", listed, err)
	}
}

// TestMemoryOrkflowStore_AnUnnamedNamespaceIsTheDefaultOne pins the one rule
// that makes "empty means default" safe: the two spellings are the same
// namespace, so a document written without one is readable with it.
func TestMemoryOrkflowStore_AnUnnamedNamespaceIsTheDefaultOne(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()

	if _, _, err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", time.Now()), artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, artifacts.DefaultNamespace, "hello-ork", "0.1.0")
	if err != nil {
		t.Fatalf("Get(default/hello-ork): %v", err)
	}
	// The stored document names the namespace it lives in, so a reader never
	// has to reconstruct it from the key it read.
	if got.GetNamespace() != artifacts.DefaultNamespace {
		t.Errorf("stored namespace = %q, want %q", got.GetNamespace(), artifacts.DefaultNamespace)
	}
}

// TestMemoryOrkflowStore_AnApplyCommitsAgainstTheHeadItNamed holds the memory
// store to the durable store's edit contract, which is the reason it exists: a
// unit test that passes here must not pass for a reason the bucket would not
// reproduce.
//
// The head an apply names is a condition on the WRITE. Naming the head that is
// there commits; naming one that has moved is refused and stores nothing; and
// an apply that names no head at all is never refused, because it claimed
// nothing that could turn out to be false.
func TestMemoryOrkflowStore_AnApplyCommitsAgainstTheHeadItNamed(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	if _, _, err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", base),
		artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put(0.1.0): %v", err)
	}
	if _, _, err := store.Put(ctx, orkflowFixture("hello-ork", "0.2.0", base.Add(time.Second)),
		artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put(0.2.0): %v", err)
	}

	stale := orkflowFixture("hello-ork", "0.9.0", base.Add(time.Minute))
	_, _, err := store.Put(ctx, stale, artifacts.ExpectHead("0.1.0"))
	if !errors.Is(err, artifacts.ErrHeadMoved) {
		t.Fatalf("Put naming a head that moved = %v, want artifacts.ErrHeadMoved", err)
	}
	var moved *artifacts.HeadMovedError
	if !errors.As(err, &moved) || moved.Expected != "0.1.0" || moved.Head != "0.2.0" {
		t.Fatalf("the refusal = %v, want one naming expected 0.1.0 and head 0.2.0", err)
	}
	if _, err := store.Get(ctx, "", "hello-ork", "0.9.0"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("a refused apply stored its version anyway: %v", err)
	}

	if _, created, err := store.Put(ctx, stale, artifacts.ExpectHead("0.2.0")); err != nil || !created {
		t.Fatalf("Put naming the head that is there = created %v, %v; want created", created, err)
	}
	versions, err := store.Versions(ctx, "", "hello-ork")
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if versions.Head != "0.9.0" || len(versions.All) != 3 {
		t.Errorf("after the retargeted apply the store holds %d versions with head %q, "+
			"want 3 and 0.9.0", len(versions.All), versions.Head)
	}
}

func TestMemoryOrkflowStore_DeleteRemovesEveryVersionAndPointerOnlyInTheSelectedNamespace(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	base := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	for _, wf := range []*orkstackv1.Orkflow{
		orkflowFixture("deploy", "1.0.0", base),
		orkflowFixture("deploy", "2.0.0", base.Add(time.Second)),
		inNamespace(orkflowFixture("deploy", "9.0.0", base), "payments"),
	} {
		if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s/%s@%s): %v", wf.GetNamespace(), wf.GetName(), wf.GetVersion(), err)
		}
	}
	if _, err := store.SetCurrentVersion(ctx, "", "deploy", "1.0.0"); err != nil {
		t.Fatalf("SetCurrentVersion: %v", err)
	}

	if err := store.Delete(ctx, "", "deploy"); err != nil {
		t.Fatalf("Delete(default/deploy): %v", err)
	}
	for _, version := range []string{"", "1.0.0", "2.0.0"} {
		if _, err := store.Get(ctx, "default", "deploy", version); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("Get(default/deploy@%q) after delete = %v, want ErrNotFound", version, err)
		}
	}
	listed, err := store.List(ctx, "default")
	if err != nil || len(listed) != 0 {
		t.Errorf("List(default) after delete = %v, %v; want empty", listed, err)
	}
	versions, err := store.Versions(ctx, "default", "deploy")
	if err != nil {
		t.Fatalf("Versions(default/deploy): %v", err)
	}
	if len(versions.All) != 0 || versions.Current != "" || versions.Head != "" {
		t.Errorf("Versions after delete = %+v, want no versions, current, or head", versions)
	}
	if got, err := store.Get(ctx, "payments", "deploy", ""); err != nil || got.GetVersion() != "9.0.0" {
		t.Errorf("Get(payments/deploy) after deleting default = %v, %v; want 9.0.0", got, err)
	}
}

func TestMemoryOrkflowStore_DeleteRefusesUnknownAndInvalidNames(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	if err := store.Delete(ctx, "", "unknown"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Delete(unknown) = %v, want ErrNotFound", err)
	}
	for _, name := range []string{"Deploy", "1deploy", strings.Repeat("a", 201)} {
		if err := store.Delete(ctx, "", name); !errors.Is(err, artifacts.ErrInvalidOrkflow) {
			t.Errorf("Delete(%q) = %v, want ErrInvalidOrkflow", name, err)
		}
	}
}
