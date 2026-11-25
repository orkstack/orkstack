package artifacts

import (
	"context"
	"slices"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestListResumePlans: the plan records of a run, by scope then number, and
// nothing else its snapshot prefix holds.
func TestListResumePlans(t *testing.T) {
	ctx := context.Background()
	store, err := NewRecordStore(ctx, Config{Backend: "local", LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.ListResumePlans(ctx, "default", "ork-r"); err != nil || len(got) != 0 {
		t.Fatalf("a run never resumed = %v, %v", got, err)
	}
	for _, p := range []ResumePlanRef{{Scope: 100, N: 0}, {Scope: 9, N: 1}, {Scope: 9, N: 0}} {
		if _, err := store.WriteResumePlan(ctx, &orkstackv1.ResumePlanRecord{Format: ResumePlanFormat, Namespace: "default", RunId: "ork-r",
			Scope: p.Scope, N: int32(p.N), Block: &orkstackv1.ResumeBlock{}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.WriteResumePlan(ctx, &orkstackv1.ResumePlanRecord{Format: ResumePlanFormat, Namespace: "default", RunId: "ork-r2",
		Scope: 5, Block: &orkstackv1.ResumeBlock{}}); err != nil {
		t.Fatal(err)
	}
	key, err := ResumePlanKey("default", "ork-r", 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.b.create(ctx, key+".tmp", "application/json", []byte("x")); err != nil {
		t.Fatal(err)
	}
	got, err := store.ListResumePlans(ctx, "default", "ork-r")
	if want := []ResumePlanRef{{Scope: 9, N: 0}, {Scope: 9, N: 1}, {Scope: 100, N: 0}}; err != nil || !slices.Equal(got, want) {
		t.Fatalf("plans = %v, %v; want %v", got, err, want)
	}
}
