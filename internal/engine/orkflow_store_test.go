package engine_test

import (
	"context"
	"errors"
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
		Tasks:     []*orkstackv1.Task{{Id: "fetch", Plugin: "http"}},
		CreatedAt: timestamppb.New(appliedAt),
	}
}

func TestMemoryOrkflowStore_ReadsBackWhatWasApplied(t *testing.T) {
	store := engine.NewMemoryOrkflowStore()
	ctx := context.Background()
	applied := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

	if err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", applied)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, "hello-ork", "0.1.0")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetName() != "hello-ork" || got.GetVersion() != "0.1.0" {
		t.Fatalf("Get = %s@%s, want hello-ork@0.1.0", got.GetName(), got.GetVersion())
	}

	// The store hands out copies: a caller mutating a result cannot corrupt it.
	got.Tasks[0].Id = "mutated"
	again, err := store.Get(ctx, "hello-ork", "0.1.0")
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

	if err := store.Put(ctx, orkflowFixture("hello-ork", "0.2.0", base)); err != nil {
		t.Fatalf("Put(0.2.0): %v", err)
	}
	if err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", base.Add(time.Second))); err != nil {
		t.Fatalf("Put(0.1.0): %v", err)
	}

	latest, err := store.Get(ctx, "hello-ork", "")
	if err != nil {
		t.Fatalf("Get(latest): %v", err)
	}
	if latest.GetVersion() != "0.1.0" {
		t.Errorf("latest = %q, want 0.1.0 (applied last)", latest.GetVersion())
	}

	// The earlier version is still addressable: versions are independent.
	pinned, err := store.Get(ctx, "hello-ork", "0.2.0")
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
		if err := store.Put(ctx, wf); err != nil {
			t.Fatalf("Put(%s@%s): %v", wf.GetName(), wf.GetVersion(), err)
		}
	}

	listed, err := store.List(ctx)
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

	if _, err := store.Get(ctx, "ghost", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get(ghost) = %v, want artifacts.ErrNotFound", err)
	}
	if err := store.Put(ctx, orkflowFixture("hello-ork", "0.1.0", time.Now())); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := store.Get(ctx, "hello-ork", "9.9.9"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get(unknown version) = %v, want artifacts.ErrNotFound", err)
	}
	empty, err := engine.NewMemoryOrkflowStore().List(ctx)
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
		if err := store.Put(ctx, wf); !errors.Is(err, artifacts.ErrInvalidOrkflow) {
			t.Errorf("Put(%q@%q) = %v, want artifacts.ErrInvalidOrkflow", wf.GetName(), wf.GetVersion(), err)
		}
	}
}
