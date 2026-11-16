package identity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

func (h *harness) keyRingWithCopy(engine, copyFile string, keys ...*jwt.Signer) *Issuer {
	h.t.Helper()
	i, err := New(Config{Keys: keys, Algorithm: jwt.ES256, Engine: engine, CopyFile: copyFile}, h.store, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	i.now = h.clock.now
	return i
}

// TestKeysCopy_WrittenAfterEveryRead: every read the engine adopts is kept
// on disk, 0600, with the instant it was fetched.
func TestKeysCopy_WrittenAfterEveryRead(t *testing.T) {
	h := newHarness(t)
	file := filepath.Join(t.TempDir(), "keys-copy.json")
	r := h.keyRingWithCopy("e1", file, newKey(t))
	if err := r.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(file)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("copy %v, %v", info, err)
	}
	raw, _ := os.ReadFile(file)
	c, err := parseKeysCopy(raw)
	if err != nil || !c.FetchedAt.Equal(h.clock.now()) || len(c.Keys.Keys) != 1 {
		t.Fatalf("copy = %+v, %v", c, err)
	}
}

// TestKeysCopy_ABootWithTheStoreAwaySignsFromIt: an engine restarted while
// the store is away loads its copy and signs and verifies for as long as
// the copy's age allows, counted from when it was fetched; a copy fetched
// in the future, or one that does not parse, is never used.
func TestKeysCopy_ABootWithTheStoreAwaySignsFromIt(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	file := filepath.Join(t.TempDir(), "keys-copy.json")
	first := h.keyRingWithCopy("e1", file, k)
	if err := first.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.store.set(true)
	h.clock.add(30 * time.Minute)
	restarted := h.keyRingWithCopy("e1", file, k)
	if err := restarted.Refresh(context.Background()); err == nil {
		t.Fatal("a refresh with the store away succeeded")
	}
	msg, err := restarted.SignDecision(jwt.KindForceCancelRoot, []byte("d"))
	if err != nil {
		t.Fatalf("signing from the copy: %v", err)
	}
	if _, _, v := restarted.VerifyDecision(context.Background(), msg, jwt.KindForceCancelRoot, h.clock.now(), NoBinding); v != VerdictValid {
		t.Fatalf("verifying from the copy: %v", v)
	}
	h.clock.add(31 * time.Minute) // the copy is now 61 minutes old
	if _, err := restarted.SignDecision(jwt.KindForceCancelRoot, nil); !errors.Is(err, ErrCannotSign) {
		t.Fatalf("past the copy's age: %v", err)
	}

	for name, body := range map[string]string{
		"fetched in the future": `{"v":1,"fetchedAt":"2099-01-01T00:00:00Z","keys":{"v":1,"sequence":1,"keys":[]}}`,
		"not JSON":              `{`,
		"another format":        `{"v":2}`,
	} {
		bad := filepath.Join(t.TempDir(), "copy.json")
		if err := os.WriteFile(bad, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		r := h.keyRingWithCopy("e1", bad, k)
		_ = r.Refresh(context.Background())
		if _, err := r.SignDecision(jwt.KindForceCancelRoot, nil); !errors.Is(err, ErrCannotSign) {
			t.Errorf("%s: signed from it: %v", name, err)
		}
	}
}

type staleCount struct {
	mu sync.Mutex
	n  int
}

func (s *staleCount) KeysCopyStale() { s.mu.Lock(); s.n++; s.mu.Unlock() }
func (s *staleCount) count() int     { s.mu.Lock(); defer s.mu.Unlock(); return s.n }

// TestRefreshTick_CountsAStaleCopy: each refresh whose copy of the
// published keys is past identity.keys_copy_max_age is reported to the
// observer, and one within it is not.
func TestRefreshTick_CountsAStaleCopy(t *testing.T) {
	h := newHarness(t)
	seen := &staleCount{}
	i, err := New(Config{Keys: []*jwt.Signer{newKey(t)}, Algorithm: jwt.ES256, Engine: "e1", Observer: seen}, h.store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	i.now = h.clock.now
	ctx := context.Background()
	i.tick(ctx)
	if seen.count() != 0 {
		t.Fatalf("a fresh copy was reported stale %d times", seen.count())
	}
	h.store.set(true)
	h.clock.add(DefaultKeysCopyMaxAge / 2)
	i.tick(ctx)
	if seen.count() != 0 {
		t.Fatalf("a copy within its age was reported stale %d times", seen.count())
	}
	h.clock.add(DefaultKeysCopyMaxAge)
	i.tick(ctx)
	i.tick(ctx)
	if seen.count() != 2 {
		t.Fatalf("a stale copy was reported %d times over two refreshes, want 2", seen.count())
	}
	h.store.set(false)
	i.tick(ctx)
	if seen.count() != 2 {
		t.Fatalf("the store's answer still reported stale: %d", seen.count())
	}
}
