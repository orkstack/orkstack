package artifacts

import (
	"context"
	"errors"
	"testing"
)

// TestOpenAttemptClaims: every claim of a run with no release marker, whose
// Orker it names, in key order; another run's are not its own, a claim that
// does not decode is still one, and a run with none reads empty.
func TestOpenAttemptClaims(t *testing.T) {
	ctx := context.Background()
	for name, store := range claimStores(t) {
		t.Run(name, func(t *testing.T) {
			reader, ok := store.(RunClaimReader)
			if !ok {
				t.Fatalf("%T reads no open claims", store)
			}
			if got, err := reader.OpenAttemptClaims(ctx, "ork-none"); err != nil || len(got) != 0 {
				t.Fatalf("a run with no claims = %v, %v", got, err)
			}
			build, item := AttemptRef{RunID: "ork-r", TaskID: "build"}, AttemptRef{RunID: "ork-r", TaskID: "fan", Item: ItemAt(7)}
			for _, scope := range []AttemptRef{build, build, item, {RunID: "ork-other", TaskID: "build"}} {
				if _, err := store.ClaimAttempt(ctx, scope, "orker-"+scope.TaskID); err != nil {
					t.Fatal(err)
				}
			}
			released := build
			released.Attempt = 1
			if err := store.ReleaseAttempt(ctx, released, "orker-build"); err != nil {
				t.Fatal(err)
			}
			got, err := reader.OpenAttemptClaims(ctx, "ork-r")
			if err != nil || len(got) != 2 {
				t.Fatalf("open claims = %+v, %v; want build 2 and fan[7] 1", got, err)
			}
			if c := got[0]; c.Ref.TaskID != "build" || c.Ref.Attempt != 2 || c.OrkerID != "orker-build" || c.ClaimedAt.IsZero() {
				t.Errorf("first = %+v", c)
			}
			if c := got[1]; c.Ref != (AttemptRef{RunID: "ork-r", TaskID: "fan", Item: ItemAt(7), Attempt: 1}) || c.OrkerID != "orker-fan" {
				t.Errorf("second = %+v", c)
			}
		})
	}
	memory := NewMemoryAttemptStore()
	ref := AttemptRef{RunID: "ork-r", TaskID: "build", Attempt: 1}
	key, err := AttemptClaimKey(ref)
	if err != nil {
		t.Fatal(err)
	}
	memory.create(key, []byte("{"))
	got, err := memory.OpenAttemptClaims(ctx, "ork-r")
	if err != nil || len(got) != 1 || got[0].Ref != ref || got[0].OrkerID != "" {
		t.Fatalf("a claim that does not decode = %+v, %v; want it open, of no known Orker", got, err)
	}
}

// TestOpenAttemptClaims_Bounds: a run with more open claims than anyone could
// answer for is said to have too many, before they are read.
func TestOpenAttemptClaims_Bounds(t *testing.T) {
	var keys []string
	for n := range int32(MaxOpenAttemptClaims + 1) {
		key, err := AttemptClaimKey(AttemptRef{RunID: "ork-r", TaskID: "build", Attempt: n + 1})
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	read := 0
	get := func(string) ([]byte, error) { read++; return []byte(`{"orker_id":"a"}`), nil }
	if _, err := openClaims("ork-r", keys, get); !errors.Is(err, ErrTooManyOpenClaims) || read != 0 {
		t.Fatalf("%d claims = %v after %d reads, want ErrTooManyOpenClaims before any", len(keys), err, read)
	}
	if got, err := openClaims("ork-r", keys[1:], get); err != nil || len(got) != MaxOpenAttemptClaims {
		t.Fatalf("%d claims = %d, %v", MaxOpenAttemptClaims, len(got), err)
	}
}
