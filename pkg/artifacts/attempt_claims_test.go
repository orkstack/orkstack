package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

type claimingStore interface {
	AttemptClaimer
	AttemptStore
}

// claimStores is every AttemptClaimer this package builds, over a fresh store.
func claimStores(t *testing.T) map[string]claimingStore {
	t.Helper()
	local, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]claimingStore{"memory": NewMemoryAttemptStore(), "local": local}
}

func TestClaimAttempt_OrdinalsRunFromOneAndSkipEveryTakenNumber(t *testing.T) {
	ctx := context.Background()
	for name, store := range claimStores(t) {
		t.Run(name, func(t *testing.T) {
			task := AttemptRef{RunID: "ork-r1", TaskID: "build"}
			item := AttemptRef{RunID: "ork-r1", TaskID: "build", Item: ItemAt(3)}

			for want := int32(1); want <= 3; want++ {
				got, err := store.ClaimAttempt(ctx, task, "orker-a")
				if err != nil || got.Ordinal != want {
					t.Fatalf("claim %d = %d, %v", want, got, err)
				}
			}
			// An item is its own scope, and a task's scope never sees its items.
			if got, err := store.ClaimAttempt(ctx, item, "orker-a"); err != nil || got.Ordinal != 1 {
				t.Fatalf("first item claim = %d, %v; want 1", got, err)
			}
			if got, _ := store.ClaimAttempt(ctx, task, "orker-a"); got.Ordinal != 4 {
				t.Fatalf("task claim after an item claim = %d, want 4", got)
			}

			// A record an older release wrote under Temporal's count is passed
			// over, never claimed again.
			for _, n := range []int32{1, 2, 5} {
				if err := store.PutAttempt(ctx, &orkstackv1.TaskAttempt{
					SchemaVersion: AttemptSchemaVersion, RunId: "ork-r2", TaskId: "build", Attempt: n,
					Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
				}); err != nil {
					t.Fatal(err)
				}
			}
			legacy := AttemptRef{RunID: "ork-r2", TaskID: "build"}
			got, err := store.ClaimAttempt(ctx, legacy, "orker-b")
			if err != nil || got.Ordinal != 6 || !slices.Equal(got.Legacy, []int32{1, 2, 5}) {
				t.Fatalf("claim past legacy records = %+v, %v; want ordinal 6 past legacy [1 2 5]", got, err)
			}
			// A record under a claimed ordinal is not legacy.
			if got, _ := store.ClaimAttempt(ctx, task, "orker-a"); len(got.Legacy) != 0 {
				t.Errorf("claimed scope reports legacy %v", got.Legacy)
			}
		})
	}
}

func TestClaimAttempt_ALostRaceTakesTheNextOrdinal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryAttemptStore()
	scope := AttemptRef{RunID: "ork-r1", TaskID: "t"}
	calls := 0
	got, err := claimNext(ctx, scope, "orker-a",
		func(string) ([]string, error) { return nil, nil },
		func(key string, body []byte) (bool, error) {
			calls++
			if calls <= 2 { // two other attempts got there first
				return true, nil
			}
			return store.create(key, body), nil
		})
	if err != nil || got.Ordinal != 3 {
		t.Fatalf("claim after two lost races = %d, %v; want 3", got, err)
	}

	_, err = claimNext(ctx, scope, "orker-a",
		func(string) ([]string, error) { return nil, nil },
		func(string, []byte) (bool, error) { return true, nil })
	if !errors.Is(err, ErrAttemptClaimContended) {
		t.Fatalf("every ordinal taken: err = %v, want ErrAttemptClaimContended", err)
	}

	broken := errors.New("store down")
	_, err = claimNext(ctx, scope, "orker-a",
		func(string) ([]string, error) { return nil, broken },
		func(string, []byte) (bool, error) { return false, nil })
	if !errors.Is(err, broken) {
		t.Fatalf("listing fails: err = %v, want the store's error", err)
	}
	_, err = claimNext(ctx, scope, "orker-a",
		func(string) ([]string, error) { return nil, nil },
		func(string, []byte) (bool, error) { return false, broken })
	if !errors.Is(err, broken) {
		t.Fatalf("create fails: err = %v, want the store's error", err)
	}
}

func TestClaimAttempt_TheClaimNamesItsOrkerAndRecordsAreUnaffected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryAttemptStore()
	ref := AttemptRef{RunID: "ork-r1", TaskID: "t", Item: ItemAt(0)}
	claimed, err := store.ClaimAttempt(ctx, ref, "orker-a")
	if err != nil {
		t.Fatal(err)
	}
	n := claimed.Ordinal
	ref.Attempt = n
	key, _ := AttemptClaimKey(ref)
	var claim AttemptClaim
	if err := json.Unmarshal(store.objects[key], &claim); err != nil {
		t.Fatal(err)
	}
	if claim.OrkerID != "orker-a" || claim.Ordinal != n || claim.Item == nil || *claim.Item != 0 ||
		claim.Nonce == "" || claim.ClaimedAt.IsZero() {
		t.Errorf("claim body %+v", claim)
	}
	if err := store.ReleaseAttempt(ctx, ref, "orker-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseAttempt(ctx, ref, "orker-a"); err != nil {
		t.Fatalf("a second release: %v, want success", err)
	}
	if released, _ := AttemptReleasedKey(ref); !store.HasObject(released) {
		t.Error("no release marker")
	}
	records, err := store.ListAttempts(ctx, "ork-r1", "t", ItemAt(0))
	if err != nil || len(records) != 0 {
		t.Errorf("records = %v, %v; a claim and a marker are not records", records, err)
	}
}

func TestReleaseAttempt_LocalStoreKeepsTheFirstMarker(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref := AttemptRef{RunID: "ork-r1", TaskID: "t", Attempt: 1}
	for range 2 {
		if err := store.ReleaseAttempt(ctx, ref, "orker-a"); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
}

func TestParseAttemptMarkerKeys_RoundTripAndStayApart(t *testing.T) {
	for _, ref := range []AttemptRef{
		{RunID: "ork-r1", TaskID: "t", Attempt: 1},
		{RunID: "ork-r1", TaskID: "t", Item: ItemAt(12), Attempt: 1234},
	} {
		claim, err := AttemptClaimKey(ref)
		if err != nil {
			t.Fatal(err)
		}
		released, err := AttemptReleasedKey(ref)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(claim, ".claim") || !strings.HasSuffix(released, ".released") {
			t.Fatalf("keys %q %q", claim, released)
		}
		if got, ok := ParseAttemptClaimKey(claim); !ok || got != ref {
			t.Errorf("ParseAttemptClaimKey(%q) = %+v, %v", claim, got, ok)
		}
		if got, ok := ParseAttemptReleasedKey(released); !ok || got != ref {
			t.Errorf("ParseAttemptReleasedKey(%q) = %+v, %v", released, got, ok)
		}
		for _, key := range []string{claim, released} {
			if _, ok := ParseAttemptKey(key); ok {
				t.Errorf("ParseAttemptKey(%q) reads a marker as a record", key)
			}
		}
		if _, ok := ParseAttemptClaimKey(released); ok {
			t.Errorf("a marker parses as a claim")
		}
	}
	if _, err := AttemptClaimKey(AttemptRef{RunID: "ork-r1", TaskID: "t"}); err == nil {
		t.Error("a claim key for ordinal 0")
	}
}

// FuzzParseAttemptObjectKeys: whatever key a listing returns, at most one of
// the three parsers accepts it, and an accepted key renders back to itself.
func FuzzParseAttemptObjectKeys(f *testing.F) {
	for _, seed := range []string{
		"metadata/attempts/ork-r1/t/0000000001.json",
		"metadata/attempts/ork-r1/t/0000000001.claim",
		"metadata/attempts/ork-r1/t/00012/0000000007.released",
		"metadata/attempts/ork-r1/t/0000000000.claim",
		"metadata/attempts/ork-r1/t/1.claim",
		"metadata/attempts/x",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		accepted := 0
		for _, p := range []struct {
			parse  func(string) (AttemptRef, bool)
			render func(AttemptRef) (string, error)
		}{
			{ParseAttemptKey, AttemptKey},
			{ParseAttemptClaimKey, AttemptClaimKey},
			{ParseAttemptReleasedKey, AttemptReleasedKey},
		} {
			ref, ok := p.parse(key)
			if !ok {
				continue
			}
			accepted++
			if ref.Attempt < 1 {
				t.Fatalf("%q parsed with ordinal %d", key, ref.Attempt)
			}
			back, err := p.render(ref)
			if err != nil || back != key {
				t.Fatalf("%q parsed to %+v, which renders %q, %v", key, ref, back, err)
			}
		}
		if accepted > 1 {
			t.Fatalf("%q parsed as %d kinds of object", key, accepted)
		}
	})
}

// TestClaimAttempt_ALostAnswerReleasesTheOrdinal: a create whose answer was
// lost may have landed, so the claim releases that ordinal before it reports
// the error.
func TestClaimAttempt_ALostAnswerReleasesTheOrdinal(t *testing.T) {
	store := NewMemoryAttemptStore()
	scope := AttemptRef{RunID: "ork-r1", TaskID: "t"}
	lost := errors.New("connection reset after the PUT was sent")
	_, err := claimNext(context.Background(), scope, "orker-a",
		func(string) ([]string, error) { return nil, nil },
		func(key string, body []byte) (bool, error) {
			store.create(key, body) // it landed
			if strings.HasSuffix(key, AttemptClaimSuffix) {
				return false, lost
			}
			return false, nil
		})
	if !errors.Is(err, lost) {
		t.Fatalf("err = %v, want the lost answer", err)
	}
	released, _ := AttemptReleasedKey(AttemptRef{RunID: "ork-r1", TaskID: "t", Attempt: 1})
	if !store.HasObject(released) {
		t.Error("the ordinal whose claim may have landed was not released")
	}
}

// TestPutAttempt_NeverOverwritesARecord: records are create-only, so one an
// earlier release wrote under Temporal's count is kept.
func TestPutAttempt_NeverOverwritesARecord(t *testing.T) {
	ctx := context.Background()
	for name, store := range claimStores(t) {
		t.Run(name, func(t *testing.T) {
			record := func(outcome orkstackv1.AttemptOutcome) *orkstackv1.TaskAttempt {
				return &orkstackv1.TaskAttempt{SchemaVersion: AttemptSchemaVersion, RunId: "ork-r1", TaskId: "t", Attempt: 1, Outcome: outcome}
			}
			if err := store.PutAttempt(ctx, record(orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED)); err != nil {
				t.Fatal(err)
			}
			err := store.PutAttempt(ctx, record(orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED))
			if !errors.Is(err, ErrAttemptExists) {
				t.Fatalf("second put = %v, want ErrAttemptExists", err)
			}
			records, _ := store.ListAttempts(ctx, "ork-r1", "t", NoItem)
			if len(records) != 1 || records[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
				t.Errorf("records = %v, want the first one kept", records)
			}
		})
	}
}
