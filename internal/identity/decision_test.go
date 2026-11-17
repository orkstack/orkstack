package identity

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// keyRing boots one engine that holds keys and has no issuer URL: it signs
// decisions and no token.
func (h *harness) keyRing(engine string, keys ...*jwt.Signer) *Issuer {
	h.t.Helper()
	i, err := New(Config{Keys: keys, Algorithm: jwt.ES256, ClockSkew: time.Minute, Engine: engine}, h.store, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	i.now = h.clock.now
	if err := i.Publish(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	return i
}

// TestDecisions_WithNoIssuer: an engine with keys and no issuer URL signs
// decisions at once, with no activation delay, and signs no token and
// serves nothing.
func TestDecisions_WithNoIssuer(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r := h.keyRing("e1", k)
	msg, err := r.SignDecision(jwt.KindForceCancelRoot, []byte("decision"))
	if err != nil {
		t.Fatal(err)
	}
	body, kid, v := r.VerifyDecision(context.Background(), msg, jwt.KindForceCancelRoot, h.clock.now(), NoBinding)
	if v != VerdictValid || kid != k.KeyID() || string(body) != "decision" {
		t.Fatalf("verify = %v %s %q", v, kid, body)
	}
	if _, err := r.Signer(); !errors.Is(err, ErrNoIssuer) {
		t.Fatalf("a token signer with no issuer: %v", err)
	}
	if _, err := r.JWKS(); !errors.Is(err, ErrNoIssuer) {
		t.Fatalf("a key set with no issuer: %v", err)
	}
}

// TestDecisions_TheEarliestPublishedKeySigns: among the unretired keys an
// engine holds, the one published first signs, whatever order its files
// list them in, so two replicas agree.
func TestDecisions_TheEarliestPublishedKeySigns(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	h.keyRing("e1", a)
	h.clock.add(time.Minute)
	r := h.keyRing("e2", b, a)
	msg, err := r.SignDecision(jwt.KindRunGap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, kid, v := r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding); v != VerdictValid || kid != a.KeyID() {
		t.Fatalf("signed with %s (%v), want the earliest published %s", kid, v, a.KeyID())
	}
	if _, err := r.Retire(context.Background(), a.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	msg, _ = r.SignDecision(jwt.KindRunGap, nil)
	if _, kid, _ := r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding); kid != b.KeyID() {
		t.Fatalf("after retiring the first, signed with %s", kid)
	}
}

// TestDecisions_AKeysLife: a signature verifies only for an instant inside
// its key's life: from its publication less the skew; for a rotated key to
// its retirement plus the copy's age and the skew; for a compromised key
// before compromised_from. Outside it the answer is unknown, never bad.
func TestDecisions_AKeysLife(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	published := h.clock.now()
	r := h.keyRing("e1", k)
	msg, err := r.SignDecision(jwt.KindForceCancelRecord, []byte("r"))
	if err != nil {
		t.Fatal(err)
	}
	verify := func(at time.Time) Verdict {
		_, _, v := r.VerifyDecision(context.Background(), msg, jwt.KindForceCancelRecord, at, NoBinding)
		return v
	}
	if v := verify(published.Add(-2 * time.Minute)); v != VerdictUnknown {
		t.Errorf("before its publication less the skew: %v", v)
	}
	if v := verify(published.Add(-30 * time.Second)); v != VerdictValid {
		t.Errorf("inside the skew: %v", v)
	}
	h.clock.add(time.Hour)
	retired := h.clock.now()
	if _, err := r.Retire(context.Background(), k.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if v := verify(retired.Add(DefaultKeysCopyMaxAge)); v != VerdictValid {
		t.Errorf("a rotated key within its copy's age of retirement: %v", v)
	}
	if v := verify(retired.Add(DefaultKeysCopyMaxAge + 2*time.Minute)); v != VerdictUnknown {
		t.Errorf("a rotated key past it: %v", v)
	}
	from := published.Add(30 * time.Minute)
	if _, err := r.Retire(context.Background(), k.KeyID(), "alice", from); err != nil {
		t.Fatal(err)
	}
	if v := verify(from.Add(-time.Second)); v != VerdictValid {
		t.Errorf("before compromised_from: %v", v)
	}
	if v := verify(from); v != VerdictUnknown {
		t.Errorf("at compromised_from: %v", v)
	}
	// A forgery under the key is bad whatever the instant.
	if _, _, v := r.VerifyDecision(context.Background(), flipSignatureBit(t, msg), jwt.KindForceCancelRecord, published, NoBinding); v != VerdictBad {
		t.Errorf("a changed signature: %v", v)
	}
}

// TestDecisions_UnknownKeysAreReadAgainAndCopiesAgeOut: a kid the engine
// does not hold in its copy is looked for in a fresh read before it is
// called unknown; with no copy, or one past its age, the answer is
// unavailable and nothing is signed.
func TestDecisions_UnknownKeysAreReadAgainAndCopiesAgeOut(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r1 := h.keyRing("e1", a)
	r2 := h.keyRing("e2", b) // published after r1 last read
	msg, err := r2.SignDecision(jwt.KindRunEnding, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, kid, v := r1.VerifyDecision(context.Background(), msg, jwt.KindRunEnding, h.clock.now(), NoBinding); v != VerdictValid || kid != b.KeyID() {
		t.Fatalf("a key published after the last read: %v %s", v, kid)
	}
	h.store.set(true)
	h.clock.add(DefaultKeysCopyMaxAge + time.Second)
	if _, _, v := r1.VerifyDecision(context.Background(), msg, jwt.KindRunEnding, h.clock.now(), NoBinding); v != VerdictUnavailable {
		t.Fatalf("with a copy past its age: %v", v)
	}
	if _, err := r1.SignDecision(jwt.KindRunEnding, nil); !errors.Is(err, ErrCannotSign) {
		t.Fatalf("signing with a copy past its age: %v", err)
	}
}

// TestDecisions_TheUnsignedRule: an unsigned decision or record is bad in
// observe and enforce, except a termination record whose run closed before
// any key was published; in asserted it is accepted as asserted, never as
// verified; and without a copy of the document the answer is unavailable.
func TestDecisions_TheUnsignedRule(t *testing.T) {
	h := newHarness(t)
	r := h.keyRing("e1", newKey(t))
	first := h.clock.now()
	for _, tc := range []struct {
		mode   string
		closed time.Time
		want   UnsignedVerdict
	}{
		{"enforce", first.Add(time.Hour), UnsignedBad},
		{"observe", first.Add(time.Hour), UnsignedBad},
		{"enforce", first.Add(-time.Hour), UnsignedAsserted},
		{"asserted", first.Add(time.Hour), UnsignedAsserted},
	} {
		if got := r.UnsignedRecord(tc.mode, tc.closed); got != tc.want {
			t.Errorf("%s, closed %s: %v, want %v", tc.mode, tc.closed.Sub(first), got, tc.want)
		}
	}
	h.store.set(true)
	h.clock.add(DefaultKeysCopyMaxAge + time.Second)
	if got := r.UnsignedRecord("enforce", first.Add(-time.Hour)); got != UnsignedUnavailable {
		t.Errorf("enforce with no usable copy: %v", got)
	}
	if got := r.UnsignedRecord("asserted", first); got != UnsignedAsserted {
		t.Errorf("asserted needs no copy: %v", got)
	}
}

// TestMayIssue: an engine with no signing key refuses to issue a decision
// under a mode above asserted, and in asserted issues it unsigned.
func TestMayIssue(t *testing.T) {
	var none *Issuer
	if err := MayIssue(none, "enforce"); !errors.Is(err, ErrCannotSign) || !strings.Contains(err.Error(), "verifies its callers") {
		t.Fatalf("enforce with no key: %v", err)
	}
	if err := MayIssue(none, "observe"); !errors.Is(err, ErrCannotSign) {
		t.Fatalf("observe with no key: %v", err)
	}
	if err := MayIssue(none, ""); !errors.Is(err, ErrCannotSign) {
		t.Fatalf("an unknown mode with no key: %v", err)
	}
	if err := MayIssue(none, "asserted"); err != nil {
		t.Fatalf("asserted with no key: %v", err)
	}
	h := newHarness(t)
	if err := MayIssue(h.keyRing("e1", newKey(t)), "enforce"); err != nil {
		t.Fatalf("enforce with a key: %v", err)
	}
}

// TestDecisions_ABodyIsBoundToWhatItIsAbout: a valid decision verifies only
// where its body begins with the binding the caller expects, so one copied
// into another run's start is not taken there.
func TestDecisions_ABodyIsBoundToWhatItIsAbout(t *testing.T) {
	h := newHarness(t)
	r := h.keyRing("e1", newKey(t))
	msg, err := r.SignDecision(jwt.KindForceCancelRoot, append(Bind("ns", "run-1"), "rest"...))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, v := r.VerifyDecision(context.Background(), msg, jwt.KindForceCancelRoot, h.clock.now(), Bind("ns", "run-1")); v != VerdictValid {
		t.Fatalf("its own run: %v", v)
	}
	if _, _, v := r.VerifyDecision(context.Background(), msg, jwt.KindForceCancelRoot, h.clock.now(), Bind("ns", "run-10")); v != VerdictBad {
		t.Fatalf("replayed into another run: %v", v)
	}
}

// TestBind_IsLengthPrefixed: each part is framed by its length, so two
// different lists of as many parts never make bindings one of which
// begins with the other: a body bound to one run does not begin with
// another's binding.
func TestBind_IsLengthPrefixed(t *testing.T) {
	cases := [][2][]string{
		{{"ab", "c"}, {"a", "bc"}},
		{{"run-1"}, {"run-10"}},
		{{"ns", "run-1"}, {"ns-run", "1"}},
		{{"", "x"}, {"x", ""}},
	}
	for _, c := range cases {
		x, y := Bind(c[0]...), Bind(c[1]...)
		if bytes.HasPrefix(x, y) || bytes.HasPrefix(y, x) {
			t.Errorf("Bind(%q) = %x and Bind(%q) = %x: one begins with the other", c[0], x, c[1], y)
		}
	}
	if got := Bind(); got == nil || len(got) != 0 {
		t.Fatalf("Bind() = %v, want an empty, non-nil binding", got)
	}
	if got, want := Bind("ns", "run-1"), []byte("\x00\x00\x00\x02ns\x00\x00\x00\x05run-1"); !bytes.Equal(got, want) {
		t.Fatalf("Bind = %q, want %q", got, want)
	}
}

// TestDecisions_UnknownKidsCostOneReadAtATime: kids the copy lacks are
// read for at most once per interval, so a flood of them costs the store
// nothing more.
func TestDecisions_UnknownKidsCostOneReadAtATime(t *testing.T) {
	h := newHarness(t)
	counted := &countingStore{KeyStore: h.store}
	r := h.keyRing("e1", newKey(t))
	r.store = counted
	unknown := func(kid string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(`{"kid":"`+kid+`"}`)) + ".a.b"
	}
	for i := range 50 {
		_, _, _ = r.VerifyDecision(context.Background(), unknown(fmt.Sprintf("made-up-%d", i)), jwt.KindRunGap, h.clock.now(), NoBinding)
	}
	if counted.reads != 1 {
		t.Fatalf("%d store reads for 50 unknown kids, want 1", counted.reads)
	}
	h.clock.add(UnknownKidReadInterval + time.Second)
	_, _, _ = r.VerifyDecision(context.Background(), unknown("another"), jwt.KindRunGap, h.clock.now(), NoBinding)
	if counted.reads != 2 {
		t.Fatalf("after the interval: %d reads", counted.reads)
	}
}

type countingStore struct {
	KeyStore
	reads int
}

func (c *countingStore) Read(ctx context.Context) (artifacts.PublishedKeys, error) {
	c.reads++
	return c.KeyStore.Read(ctx)
}

// TestDecisions_UnsignedFailsClosed: an unsigned decision is bad in
// observe and enforce whatever its time; a record with no close time, or
// a mode this release does not know, is bad too; and an engine with no
// keys answers a decision unavailable rather than failing.
func TestDecisions_UnsignedFailsClosed(t *testing.T) {
	h := newHarness(t)
	r := h.keyRing("e1", newKey(t))
	if got := r.UnsignedDecision("enforce"); got != UnsignedBad {
		t.Errorf("an unsigned decision under enforce: %v", got)
	}
	if got := r.UnsignedDecision("asserted"); got != UnsignedAsserted {
		t.Errorf("an unsigned decision under asserted: %v", got)
	}
	if got := r.UnsignedRecord("enforce", time.Time{}); got != UnsignedUnavailable {
		t.Errorf("a record with no close time: %v", got)
	}
	for _, mode := range []string{"Enforce", "", "other"} {
		if got := r.UnsignedRecord(mode, h.clock.now().Add(-time.Hour)); got != UnsignedUnavailable {
			t.Errorf("mode %q: %v", mode, got)
		}
	}
	var none *Issuer
	if _, _, v := none.VerifyDecision(context.Background(), "a.b.c", jwt.KindRunGap, time.Now(), NoBinding); v != VerdictUnavailable {
		t.Errorf("an engine with no keys: %v", v)
	}
	if Verdict(9).String() == "" || UnsignedVerdict(9).String() == "" {
		t.Error("an out-of-range verdict has no name")
	}
}

// TestKeysCopy_AStoreReadReplacesACopy: a copy loaded from disk is
// replaced by the store's document when the store answers, whatever its
// sequence, so a store restored from backup does not wedge the engine.
func TestKeysCopy_AStoreReadReplacesACopy(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	file := filepath.Join(t.TempDir(), "copy.json")
	first := h.keyRingWithCopy("e1", file, k)
	if err := first.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(file)
	c, _ := parseKeysCopy(raw)
	c.Keys.Sequence += 100
	if err := writeKeysCopy(file, c); err != nil {
		t.Fatal(err)
	}
	h.store.set(true)
	r := h.keyRingWithCopy("e1", file, k)
	_ = r.Refresh(context.Background())
	h.store.set(false)
	h.clock.add(time.Minute)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.Status().ReadAt; !got.Equal(h.clock.now()) {
		t.Fatalf("the store's read was not taken over the copy: read at %s", got)
	}
}

// TestKeysCopy_AWritableCopyIsRefused: a copy file others can write, or a
// link, is never read.
func TestKeysCopy_AWritableCopyIsRefused(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "copy.json")
	first := h.keyRingWithCopy("e1", file, k)
	if err := first.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	h.store.set(true)
	for name, path := range map[string]string{"a link": link} {
		r := h.keyRingWithCopy("e1", path, k)
		_ = r.Refresh(context.Background())
		if _, err := r.SignDecision(jwt.KindRunGap, nil); !errors.Is(err, ErrCannotSign) {
			t.Errorf("%s was read: %v", name, err)
		}
	}
	if err := os.Chmod(file, 0o622); err != nil {
		t.Fatal(err)
	}
	r := h.keyRingWithCopy("e1", file, k)
	_ = r.Refresh(context.Background())
	if _, err := r.SignDecision(jwt.KindRunGap, nil); !errors.Is(err, ErrCannotSign) {
		t.Errorf("a copy others can write was read: %v", err)
	}
}

// TestDecisions_ACompromiseSeenStandsInThisProcess: once this engine has
// read a key as compromised from an instant, a document that says less
// (a store restored from a backup) does not make later signatures good.
func TestDecisions_ACompromiseSeenStandsInThisProcess(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r := h.keyRing("e1", a, b)
	msg, _ := r.SignDecision(jwt.KindRunGap, nil)
	_, kid, _ := r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding)
	before, _ := h.store.Read(context.Background())
	from := h.clock.now().Add(-time.Minute)
	if _, err := r.Retire(context.Background(), kid, "alice", from); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.fromCopy = true // as after a copy: the next store answer is taken whatever its sequence
	r.mu.Unlock()
	r.take(before, h.clock.now(), nil)
	if _, _, v := r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding); v == VerdictValid {
		t.Fatal("a signature from after a compromise this engine saw verified")
	}
}

// TestDecisions_TheLifeBoundaries holds each edge of a key's life exactly.
func TestDecisions_TheLifeBoundaries(t *testing.T) {
	h := newHarness(t)
	r := h.keyRing("e1", newKey(t))
	p := artifacts.PublishedKey{PublishedAt: h.clock.now(), RetiredAt: h.clock.now().Add(time.Hour), CompromisedFrom: h.clock.now().Add(30 * time.Minute)}
	skew := time.Minute
	for name, tc := range map[string]struct {
		at   time.Time
		want bool
	}{
		"publication less the skew": {p.PublishedAt.Add(-skew), true},
		"before it":                 {p.PublishedAt.Add(-skew - time.Nanosecond), false},
		"just before compromise":    {p.CompromisedFrom.Add(-time.Nanosecond), true},
		"at compromise":             {p.CompromisedFrom, false},
		"zero":                      {time.Time{}, false},
	} {
		if got := r.withinLife(p, tc.at); got != tc.want {
			t.Errorf("%s: %v", name, got)
		}
	}
	rotated := artifacts.PublishedKey{PublishedAt: p.PublishedAt, RetiredAt: p.RetiredAt}
	edge := rotated.RetiredAt.Add(DefaultKeysCopyMaxAge + skew)
	if !r.withinLife(rotated, edge) || r.withinLife(rotated, edge.Add(time.Nanosecond)) {
		t.Error("the rotated key's last instant is not retirement plus the copy's age and the skew")
	}
}

// TestDecisions_AFailedKidReadReleasesItsTurn: a read for an unknown kid
// that fails does not hold the next one back.
func TestDecisions_AFailedKidReadReleasesItsTurn(t *testing.T) {
	h := newHarness(t)
	r := h.keyRing("e1", newKey(t))
	h.store.set(true)
	msg := base64.RawURLEncoding.EncodeToString([]byte(`{"kid":"made-up"}`)) + ".a.b"
	_, _, _ = r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding)
	h.clock.add(failedKidReadRetry)
	if !r.mayReadForKid(h.clock.now()) {
		t.Fatal("a failed read kept its turn past the retry")
	}
}

// TestIdentityRetire_CompromiseMovesOnlyEarlier: added to a retired key,
// moved earlier, repeated, and refused later.
func TestIdentityRetire_CompromiseMovesOnlyEarlier(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r := h.keyRing("e1", k)
	ctx := context.Background()
	if _, err := r.Retire(ctx, k.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	t0 := h.clock.now().Add(-time.Hour)
	for _, step := range []struct {
		at      time.Time
		wantErr bool
	}{{t0, false}, {t0, false}, {t0.Add(-time.Minute), false}, {t0, true}} {
		_, err := r.Retire(ctx, k.KeyID(), "alice", step.at)
		if (err != nil) != step.wantErr || (err != nil && !errors.Is(err, ErrAlreadyCompromised)) {
			t.Fatalf("compromise at %s: %v", step.at, err)
		}
	}
}

// TestIdentity_ConcurrentRetireAndReads runs retirements, refreshes and
// verifications at once; under -race the issuer's state is guarded, and
// the copy on disk ends at the store's sequence.
func TestIdentity_ConcurrentRetireAndReads(t *testing.T) {
	h := newHarness(t)
	keys := []*jwt.Signer{newKey(t), newKey(t), newKey(t)}
	file := filepath.Join(t.TempDir(), "copy.json")
	r := h.keyRingWithCopy("e1", file, keys...)
	if err := r.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	msg, _ := r.SignDecision(jwt.KindRunGap, nil)
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Go(func() { _, _ = r.Retire(context.Background(), keys[i].KeyID(), "alice", time.Time{}) })
		wg.Go(func() {
			for range 10 {
				h.clock.add(time.Millisecond)
				_ = r.Refresh(context.Background())
				_, _, _ = r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding)
			}
		})
	}
	wg.Wait()
	_ = r.Refresh(context.Background())
	doc, _ := h.store.Read(context.Background())
	raw, _ := os.ReadFile(file)
	c, err := parseKeysCopy(raw)
	if err != nil || c.Keys.Sequence != doc.Sequence {
		t.Fatalf("the copy holds sequence %d, the store %d (%v)", c.Keys.Sequence, doc.Sequence, err)
	}
}

// TestDecisions_ACheckThatCouldNotBeMadeIsUnavailable: a kid the copy lacks
// whose fresh read fails, or is held back by the limit, a zero instant, and
// an unsigned judgement under a mode this release does not know, are each
// unavailable: no conclusion, and a reason to ask again.
func TestDecisions_ACheckThatCouldNotBeMadeIsUnavailable(t *testing.T) {
	h := newHarness(t)
	r := h.keyRing("e1", newKey(t))
	other := h.keyRing("e2", newKey(t)) // published after r last read
	msg, _ := other.SignDecision(jwt.KindRunGap, nil)
	h.store.set(true)
	if _, _, v := r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding); v != VerdictUnavailable {
		t.Errorf("a fresh read that failed: %v", v)
	}
	if _, _, v := r.VerifyDecision(context.Background(), msg, jwt.KindRunGap, h.clock.now(), NoBinding); v != VerdictUnavailable {
		t.Errorf("a fresh read held back: %v", v)
	}
	h.store.set(false)
	own, _ := r.SignDecision(jwt.KindRunGap, nil)
	if _, _, v := r.VerifyDecision(context.Background(), own, jwt.KindRunGap, time.Time{}, NoBinding); v != VerdictUnavailable {
		t.Errorf("a zero instant: %v", v)
	}
	if got := r.UnsignedRecord("", h.clock.now()); got != UnsignedUnavailable {
		t.Errorf("an empty mode: %v", got)
	}
	if got := r.UnsignedDecision("Enforce"); got != UnsignedUnavailable {
		t.Errorf("an unknown mode: %v", got)
	}
	var none *Issuer
	if _, err := none.SignDecision(jwt.KindRunGap, nil); !errors.Is(err, ErrCannotSign) {
		t.Errorf("signing with no keys: %v", err)
	}
	if got := none.UnsignedRecord("enforce", h.clock.now()); got != UnsignedUnavailable {
		t.Errorf("an engine with no keys judging a record: %v", got)
	}
}

// TestKeysCopy_AnUnusableCopyIsNotTaken: a copy fetched in the future, or
// older than its age allows, is not adopted, so the store's next answer is
// taken at once.
func TestKeysCopy_AnUnusableCopyIsNotTaken(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	file := filepath.Join(t.TempDir(), "copy.json")
	first := h.keyRingWithCopy("e1", file, k)
	if err := first.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(file)
	c, _ := parseKeysCopy(raw)
	c.FetchedAt = h.clock.now().Add(time.Hour)
	if err := writeKeysCopy(file, c); err != nil {
		t.Fatal(err)
	}
	h.store.set(true)
	r := h.keyRingWithCopy("e1", file, k)
	_ = r.Refresh(context.Background())
	if !r.Status().ReadAt.IsZero() {
		t.Fatalf("a copy fetched in the future was taken: %s", r.Status().ReadAt)
	}
	h.store.set(false)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.SignDecision(jwt.KindRunGap, nil); err != nil {
		t.Fatalf("after the store answered: %v", err)
	}
}

// TestKeysCopy_AWriteThatFailsIsReported: the last failure to write the
// copy is in the issuer's status.
func TestKeysCopy_AWriteThatFailsIsReported(t *testing.T) {
	h := newHarness(t)
	file := filepath.Join(t.TempDir(), "missing-dir", "copy.json")
	r := h.keyRingWithCopy("e1", file, newKey(t))
	if err := r.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.Status().CopyErr == nil {
		t.Fatal("a copy that could not be written is not reported")
	}
}

// flipSignatureBit returns msg with one bit of its decoded signature
// flipped, so the signature always differs, whatever bytes it held.
func flipSignatureBit(t *testing.T, msg string) string {
	t.Helper()
	i := strings.LastIndexByte(msg, '.')
	sig, err := base64.RawURLEncoding.Strict().DecodeString(msg[i+1:])
	if err != nil || len(sig) == 0 {
		t.Fatalf("the signature of %q does not decode: %v", msg, err)
	}
	sig[len(sig)-1] ^= 1
	out := msg[:i+1] + base64.RawURLEncoding.EncodeToString(sig)
	if out == msg {
		t.Fatal("the tampered message equals the original")
	}
	return out
}
