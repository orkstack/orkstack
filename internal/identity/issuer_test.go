package identity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var _ = filepath.Join

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// liveIssuer stands in for the issuer URL: the kids it serves, or an error.
type liveIssuer struct {
	mu   sync.Mutex
	kids map[string]bool
	err  error
}

func (l *liveIssuer) PublishedKeyIDs(context.Context) (map[string]bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	out := map[string]bool{}
	for k, v := range l.kids {
		out[k] = v
	}
	return out, nil
}

func (l *liveIssuer) serve(kids ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = nil
	for _, k := range kids {
		l.kids[k] = true
	}
}

func newKey(t *testing.T) *jwt.Signer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.NewSigner(k)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// failingStore fails every call while fail is set.
type failingStore struct {
	*artifacts.PublishedKeysStore
	mu   sync.Mutex
	fail bool
}

func (f *failingStore) set(v bool) { f.mu.Lock(); f.fail = v; f.mu.Unlock() }
func (f *failingStore) down() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.fail }
func (f *failingStore) Read(ctx context.Context) (artifacts.PublishedKeys, error) {
	if f.down() {
		return artifacts.PublishedKeys{}, errors.New("store unreachable")
	}
	return f.PublishedKeysStore.Read(ctx)
}
func (f *failingStore) Update(ctx context.Context, fn func(*artifacts.PublishedKeys) (bool, error)) (artifacts.PublishedKeys, error) {
	if f.down() {
		return artifacts.PublishedKeys{}, errors.New("store unreachable")
	}
	return f.PublishedKeysStore.Update(ctx, fn)
}

type harness struct {
	t     *testing.T
	dir   string
	store *failingStore
	clock *clock
	live  *liveIssuer
}

func newHarness(t *testing.T) *harness {
	dir := t.TempDir()
	s, err := artifacts.NewPublishedKeysStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, dir: dir, store: &failingStore{PublishedKeysStore: s},
		clock: &clock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}, live: &liveIssuer{kids: map[string]bool{}}}
}

// replica boots one engine with keys, in order, and publishes them.
func (h *harness) replica(engine string, keys ...*jwt.Signer) *Issuer {
	h.t.Helper()
	i, err := New(Config{URL: "https://ork-id.acme.dev", Keys: keys, Algorithm: jwt.ES256,
		ActivationDelay: 24 * time.Hour, VerifyPublished: true, Engine: engine}, h.store, h.live, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	i.now = h.clock.now
	if err := i.Publish(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	return i
}

func refresh(t *testing.T, issuers ...*Issuer) {
	t.Helper()
	for _, i := range issuers {
		if err := i.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func signsWith(t *testing.T, i *Issuer, want *jwt.Signer) {
	t.Helper()
	got, err := i.Signer()
	if err != nil {
		t.Fatalf("Signer() = %v, want %s", err, want.KeyID())
	}
	if got.KeyID() != want.KeyID() {
		t.Fatalf("signs with %s, want %s", got.KeyID(), want.KeyID())
	}
}

// TestIssuer_PublishesItsKeysAndNeverRemovesOne: replicas add the keys
// they hold, each recorded with who holds it; a key a replica stops listing
// stays published; publishing again changes nothing.
func TestIssuer_PublishesItsKeysAndNeverRemovesOne(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r1 := h.replica("e1", a)
	h.replica("e2", b, a)
	doc, err := h.store.Read(context.Background())
	if err != nil || len(doc.Keys) != 2 {
		t.Fatalf("%d keys, %v", len(doc.Keys), err)
	}
	ka, _ := doc.Key(a.KeyID())
	if len(ka.Holders) != 2 || ka.PublishedBy != "e1" {
		t.Fatalf("key a: %+v", ka)
	}
	seq := doc.Sequence
	if err := r1.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.replica("e3", b) // a no longer listed by e3
	doc, _ = h.store.Read(context.Background())
	if _, ok := doc.Key(a.KeyID()); !ok || len(doc.Keys) != 2 {
		t.Fatalf("a key nobody lists now was removed: %+v", doc.Keys)
	}
	if doc.Sequence != seq+1 {
		t.Fatalf("publishing again moved the sequence %d to %d; only e3's holder is new", seq, doc.Sequence)
	}
}

// TestIssuer_ActivationRule: the only key signs as soon as the live issuer
// serves it; a second key waits out the activation delay and the live
// issuer, while the first goes on signing; then the first listed qualifying
// key signs.
func TestIssuer_ActivationRule(t *testing.T) {
	h := newHarness(t)
	old, fresh := newKey(t), newKey(t)
	r := h.replica("e1", old)
	refresh(t, r)
	if _, err := r.Signer(); !errors.Is(err, ErrKeyNotActive) || !strings.Contains(err.Error(), "not served at") {
		t.Fatalf("before the issuer URL serves it: %v", err)
	}
	h.live.serve(old.KeyID())
	refresh(t, r)
	signsWith(t, r, old)

	// The new key first, the old second, on a restarted replica.
	r = h.replica("e1", fresh, old)
	refresh(t, r)
	signsWith(t, r, old)
	st := r.Keys()
	if st[0].KeyID != fresh.KeyID() || st[0].Active || !strings.Contains(st[0].Why, "activation delay") {
		t.Fatalf("the new key's status: %+v", st[0])
	}
	h.clock.add(24 * time.Hour)
	refresh(t, r)
	signsWith(t, r, old) // eligible, but the live issuer does not serve it yet
	if st := r.Keys(); !strings.Contains(st[0].Why, "not served at") {
		t.Fatalf("the new key's status: %+v", st[0])
	}
	h.live.serve(fresh.KeyID())
	refresh(t, r)
	signsWith(t, r, fresh)
}

// TestIssuer_AnActivatedKeySignsWhateverTheLiveIssuerSays: activation is
// remembered in the document, so a replica restarted while the issuer URL
// is unreachable still signs with it.
func TestIssuer_AnActivatedKeySignsWhateverTheLiveIssuerSays(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r1 := h.replica("e1", k)
	h.live.serve(k.KeyID())
	refresh(t, r1)
	signsWith(t, r1, k)
	h.live.mu.Lock()
	h.live.err = errors.New("CDN unreachable")
	h.live.mu.Unlock()
	r2 := h.replica("e2", k)
	refresh(t, r1, r2)
	signsWith(t, r1, k)
	signsWith(t, r2, k)
}

// TestIssuer_ARetiredKeyNeverSignsAgain: retired on one replica, the key
// stops signing on another at its next read, whatever that one's files
// still list, and the next key that qualifies takes over. A retired key is
// never served again but stays in the document.
func TestIssuer_ARetiredKeyNeverSignsAgain(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r1 := h.replica("e1", a, b)
	r2 := h.replica("e2", a, b)
	h.live.serve(a.KeyID(), b.KeyID())
	refresh(t, r1, r2)
	if _, err := r2.Signer(); !errors.Is(err, ErrKeyNotActive) {
		t.Fatalf("two new keys, each with the other unretired, signed before the delay: %v", err)
	}
	h.clock.add(24 * time.Hour)
	refresh(t, r1, r2)
	signsWith(t, r2, a)
	if _, err := r1.Retire(context.Background(), a.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	refresh(t, r2)
	signsWith(t, r2, b)
	if _, err := r1.Retire(context.Background(), b.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	refresh(t, r2)
	if _, err := r2.Signer(); !errors.Is(err, ErrKeyNotActive) || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("with every key retired: %v", err)
	}
	if set, err := r2.JWKS(); !errors.Is(err, ErrNoServedKeys) {
		t.Fatalf("with every key retired the key set = %s, %v; want ErrNoServedKeys", set, err)
	}
	doc, _ := h.store.Read(context.Background())
	if len(doc.Keys) != 2 || doc.Keys[0].RetiredBy != "alice" {
		t.Fatalf("the document after retiring: %+v", doc.Keys)
	}
	if _, err := r1.Retire(context.Background(), "nope", "alice", time.Time{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("retiring an unknown key: %v", err)
	}
	if again, err := r1.Retire(context.Background(), a.KeyID(), "bob", time.Time{}); err != nil || again.RetiredBy != "alice" {
		t.Fatalf("retiring a retired key again = %+v, %v", again, err)
	}
}

// TestIssuer_AStaleDocumentStopsSigning: while the document cannot be read
// a replica signs from its last copy for at most DefaultKeysCopyMaxAge, then
// refuses: a retirement must reach every replica that goes on signing.
func TestIssuer_AStaleDocumentStopsSigning(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r := h.replica("e1", k)
	h.live.serve(k.KeyID())
	refresh(t, r)
	h.store.set(true)
	h.clock.add(DefaultKeysCopyMaxAge - time.Second)
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("a refresh with the store down succeeded")
	}
	signsWith(t, r, k)
	h.clock.add(2 * time.Second)
	if _, err := r.Signer(); !errors.Is(err, ErrKeyNotActive) || !strings.Contains(err.Error(), "could not be read") {
		t.Fatalf("past the bound: %v", err)
	}
	h.store.set(false)
	refresh(t, r)
	signsWith(t, r, k)
}

// TestIssuer_BeforeTheFirstReadNothingSigns: a replica that has not read
// the document refuses, naming why.
func TestIssuer_BeforeTheFirstReadNothingSigns(t *testing.T) {
	h := newHarness(t)
	h.store.set(true)
	i, err := New(Config{URL: "https://ork-id.acme.dev", Keys: []*jwt.Signer{newKey(t)}, Algorithm: jwt.ES256,
		ActivationDelay: time.Hour, VerifyPublished: true, Engine: "e1"}, h.store, h.live, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Publish(context.Background()); err == nil {
		t.Fatal("published with the store down")
	}
	if _, err := i.Signer(); !errors.Is(err, ErrKeyNotActive) || !strings.Contains(err.Error(), "not been read") {
		t.Fatalf("before any read: %v", err)
	}
}

// TestIssuer_Documents: the discovery document names the issuer and its
// key set; the served set and the bundle hold the unretired keys; the
// bundle's sequence is the document's sequence.
func TestIssuer_Documents(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r := h.replica("e1", k)
	r.cfg.TrustDomain = "ork.acme.dev"
	refresh(t, r)
	var disc struct {
		Issuer   string   `json:"issuer"`
		JWKSURI  string   `json:"jwks_uri"`
		Algs     []string `json:"id_token_signing_alg_values_supported"`
		Subjects []string `json:"subject_types_supported"`
		Claims   []string `json:"claims_supported"`
	}
	raw, err := r.Discovery()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &disc); err != nil {
		t.Fatal(err)
	}
	if disc.Issuer != "https://ork-id.acme.dev" || disc.JWKSURI != "https://ork-id.acme.dev/jwks.json" ||
		len(disc.Algs) != 1 || disc.Algs[0] != "ES256" || len(disc.Subjects) != 1 || len(disc.Claims) == 0 {
		t.Fatalf("discovery: %+v", disc)
	}
	set, err := r.JWKS()
	if err != nil {
		t.Fatal(err)
	}
	if ks, err := jwt.ParseKeySet(set); err != nil || ks.Len() != 1 {
		t.Fatalf("served set %s, %v", set, err)
	}
	bundle, err := r.Bundle()
	if err != nil {
		t.Fatal(err)
	}
	var b struct {
		Sequence int64 `json:"spiffe_sequence"`
		Hint     int64 `json:"spiffe_refresh_hint"`
	}
	doc, _ := h.store.Read(context.Background())
	if err := json.Unmarshal(bundle, &b); err != nil || b.Sequence != doc.Sequence || b.Hint != int64(BundleRefreshHint/time.Second) {
		t.Fatalf("bundle %s", bundle)
	}
	r.cfg.TrustDomain = ""
	if _, err := r.Bundle(); !errors.Is(err, ErrNoTrustDomain) {
		t.Fatalf("a bundle with no trust domain: %v", err)
	}
}

// TestIssuer_AKeyNoEngineHoldsIsReported: a published key no engine has
// confirmed holding within HolderForget is reported, not refused.
func TestIssuer_AKeyNoEngineHoldsIsReported(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	h.replica("gone", a)
	h.clock.add(HolderForget + time.Hour)
	r := h.replica("e1", b)
	refresh(t, r)
	for _, st := range r.Keys() {
		if st.KeyID == a.KeyID() && (!st.Unheld || st.HeldHere) {
			t.Fatalf("key a: %+v", st)
		}
		if st.KeyID == b.KeyID() && (st.Unheld || !st.HeldHere) {
			t.Fatalf("key b: %+v", st)
		}
	}
}

// TestNew_RefusesABadConfiguration names the setting each refusal is about.
func TestNew_RefusesABadConfiguration(t *testing.T) {
	h := newHarness(t)
	ec := newKey(t)
	good := Config{URL: "https://ork-id.acme.dev", Keys: []*jwt.Signer{ec}, Algorithm: jwt.ES256, ActivationDelay: time.Hour, Engine: "e1"}
	nilLive := map[string]bool{"issuer URL": true}
	for want, change := range map[string]func(*Config){
		"identity.issuer":                   func(c *Config) { c.URL = "http://ork-id.acme.dev" },
		"identity.issuer ":                  func(c *Config) { c.URL = "https://ork-id.acme.dev/?x=1" },
		"identity.issuer  ":                 func(c *Config) { c.URL = "https://ork-id.acme.dev/#f" },
		"identity.issuer   ":                func(c *Config) { c.URL = "https://user@ork-id.acme.dev" },
		"identity.signing.key_files":        func(c *Config) { c.Keys = nil },
		"identity.signing.algorithm":        func(c *Config) { c.Algorithm = jwt.RS256 },
		"identity.signing.activation_delay": func(c *Config) { c.ActivationDelay = -time.Second },
		"identity.spiffe.trust_domain":      func(c *Config) { c.SubjectFormat = FormatSPIFFE },
		"identity.spiffe.trust_domain ":     func(c *Config) { c.TrustDomain = "Bad" },
	} {
		c := good
		change(&c)
		var live LiveIssuer = h.live
		if nilLive[want] {
			live = nil
		}
		if _, err := New(c, h.store, live, nil); err == nil || !strings.Contains(err.Error(), strings.TrimSpace(want)) {
			t.Errorf("%s: %v", want, err)
		}
	}
	if _, err := New(good, h.store, h.live, nil); err != nil {
		t.Fatal(err)
	}
	good.URL = "https://ork-id.acme.dev/tenant/"
	i, err := New(good, h.store, h.live, nil)
	if err != nil || i.cfg.URL != "https://ork-id.acme.dev/tenant" {
		t.Fatalf("a path's trailing slash: %v, %+v", err, i)
	}
}

// TestIssuer_ServedDocumentsNameNoEngine: who holds a key, who published or
// retired it, and any engine's identity live only in the stored document;
// the discovery document, the key set and the SPIFFE bundle carry none of
// them.
func TestIssuer_ServedDocumentsNameNoEngine(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r := h.replica("1234@engine-host-a@", a, b)
	h.replica("5678@engine-host-b@", b)
	r.cfg.TrustDomain = "ork.acme.dev"
	refresh(t, r)
	if _, err := r.Retire(context.Background(), b.KeyID(), "retiring-operator", time.Time{}); err != nil {
		t.Fatal(err)
	}
	doc, _ := h.store.Read(context.Background())
	if k, _ := doc.Key(a.KeyID()); len(k.Holders) == 0 || k.PublishedBy == "" {
		t.Fatalf("the stored document lost its holders: %+v", k)
	}
	disc, err := r.Discovery()
	if err != nil {
		t.Fatal(err)
	}
	jwks, err := r.JWKS()
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := r.Bundle()
	if err != nil {
		t.Fatal(err)
	}
	for name, served := range map[string][]byte{"discovery": disc, "jwks": jwks, "bundle": bundle} {
		for _, secret := range []string{"engine-host-a", "engine-host-b", "1234@", "5678@", "retiring-operator",
			"holders", "publishedBy", "retiredBy", "confirmedAt", "activatedAt"} {
			if strings.Contains(string(served), secret) {
				t.Errorf("the served %s names %q: %s", name, secret, served)
			}
		}
	}
}

// TestIssuer_AnOlderReadNeverReplacesANewerOne: a refresh that read the
// document before a retirement on the same replica finishes after it; the
// retirement stands, and the sequence never goes back.
func TestIssuer_AnOlderReadNeverReplacesANewerOne(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r := h.replica("e1", a, b)
	h.live.serve(a.KeyID(), b.KeyID())
	h.clock.add(24 * time.Hour)
	refresh(t, r)
	signsWith(t, r, a)
	before, _ := h.store.Read(context.Background())
	if _, err := r.Retire(context.Background(), a.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	r.take(before, h.clock.now(), nil) // the late answer of a read begun before
	signsWith(t, r, b)
	if r.Status().Document.Sequence <= before.Sequence {
		t.Fatalf("the sequence went back to %d", r.Status().Document.Sequence)
	}
}

// TestIssuer_AReadThatCannotBeWrittenStillCounts: when the store answers a
// read but refuses every write, the replica keeps the fresh read: signing
// does not stop at DefaultKeysCopyMaxAge while the document is readable.
func TestIssuer_AReadThatCannotBeWrittenStillCounts(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r := h.replica("e1", k)
	h.live.serve(k.KeyID())
	refresh(t, r)
	r.store = readOnly{h.store}
	h.clock.add(HolderRefresh + time.Hour) // the refresh then wants to confirm, and cannot
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("a refresh whose write failed succeeded")
	}
	signsWith(t, r, k)
	if age := h.clock.now().Sub(r.Status().ReadAt); age != 0 {
		t.Fatalf("the read was not kept: %s old", age)
	}
}

type readOnly struct{ KeyStore }

func (readOnly) Update(context.Context, func(*artifacts.PublishedKeys) (bool, error)) (artifacts.PublishedKeys, error) {
	return artifacts.PublishedKeys{}, errors.New("write refused")
}

// TestIssuer_NothingIsServedBeforeTheFirstRead: an engine that has not read
// the document answers the key set and the bundle with ErrKeysNotRead,
// never an empty set a relying party would cache.
func TestIssuer_NothingIsServedBeforeTheFirstRead(t *testing.T) {
	h := newHarness(t)
	i, err := New(Config{URL: "https://ork-id.acme.dev", Keys: []*jwt.Signer{newKey(t)}, Algorithm: jwt.ES256,
		TrustDomain: "ork.acme.dev", Engine: "e1"}, h.store, h.live, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.JWKS(); !errors.Is(err, ErrKeysNotRead) {
		t.Fatalf("JWKS before a read: %v", err)
	}
	if _, err := i.Bundle(); !errors.Is(err, ErrKeysNotRead) {
		t.Fatalf("Bundle before a read: %v", err)
	}
}

// TestIssuer_AMalformedStoredKeyIsLeftOut: a stored key the jwt package
// will not read is left out of what is served, and the others still are.
func TestIssuer_AMalformedStoredKeyIsLeftOut(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r := h.replica("e1", k)
	if _, err := h.store.Update(context.Background(), func(d *artifacts.PublishedKeys) (bool, error) {
		d.Keys = append(d.Keys, artifacts.PublishedKey{KID: "not-a-thumbprint", Algorithm: "ES256", JWK: []byte(`{"kty":"EC"}`), PublishedAt: h.clock.now()})
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	refresh(t, r)
	set, err := r.JWKS()
	if err != nil {
		t.Fatal(err)
	}
	if ks, _ := jwt.ParseKeySet(set); ks.Len() != 1 {
		t.Fatalf("served %s", set)
	}
}

// TestIssuer_ABackwardClockSignsNothing: a read that seems to lie in the
// future (the clock stepped back) is no proof the copy is fresh.
func TestIssuer_ABackwardClockSignsNothing(t *testing.T) {
	h := newHarness(t)
	k := newKey(t)
	r := h.replica("e1", k)
	h.live.serve(k.KeyID())
	refresh(t, r)
	h.clock.add(-time.Minute)
	if _, err := r.Signer(); !errors.Is(err, ErrKeyNotActive) || !strings.Contains(err.Error(), "clock") {
		t.Fatalf("after the clock stepped back: %v", err)
	}
}

// TestIssuer_ConcurrentUse runs refreshes, retirements, reads and signing
// at once; under -race it proves the issuer's state is guarded.
func TestIssuer_ConcurrentUse(t *testing.T) {
	h := newHarness(t)
	keys := []*jwt.Signer{newKey(t), newKey(t), newKey(t)}
	r := h.replica("e1", keys...)
	h.live.serve(keys[0].KeyID(), keys[1].KeyID(), keys[2].KeyID())
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for range 20 {
				_ = r.Refresh(context.Background())
				_, _ = r.Signer()
				_ = r.Keys()
				_, _ = r.JWKS()
				_ = r.Status()
			}
		})
		wg.Go(func() { _, _ = r.Retire(context.Background(), keys[i%3].KeyID(), "alice", time.Time{}) })
	}
	wg.Wait()
	doc, _ := h.store.Read(context.Background())
	if r.Status().Document.Sequence != doc.Sequence {
		t.Fatalf("the replica holds sequence %d, the store %d", r.Status().Document.Sequence, doc.Sequence)
	}
}

// TestIssuer_AStoredKeyThatIsNotTheOneHeldNeverSigns: an entry under a
// held key's id whose public key is another's (a store written around the
// engines) neither signs nor is served under that id.
func TestIssuer_AStoredKeyThatIsNotTheOneHeldNeverSigns(t *testing.T) {
	h := newHarness(t)
	held, other := newKey(t), newKey(t)
	r := h.replica("e1", held)
	h.live.serve(held.KeyID())
	refresh(t, r)
	signsWith(t, r, held)
	// Rewrite the stored entry around the CAS rules, as a store writer could.
	doc, _ := h.store.Read(context.Background())
	forged, _ := other.Public().MarshalJWK(held.KeyID())
	doc.Keys[0].JWK = forged
	doc.Sequence++
	raw, _ := json.Marshal(doc)
	must(t, os.WriteFile(filepath.Join(h.dir, artifacts.PublishedKeysKey), raw, 0o600))
	refresh(t, r)
	if _, err := r.Signer(); !errors.Is(err, ErrKeyNotActive) || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("a forged entry: %v", err)
	}
	set, _ := r.JWKS()
	if ks, _ := jwt.ParseKeySet(set); ks.Len() != 0 {
		t.Fatalf("a forged entry is served: %s", set)
	}
}

// TestIssuer_ARetirementSeenStandsInThisProcess: once this replica has read
// a key as retired, a document that un-retires it does not make it sign.
func TestIssuer_ARetirementSeenStandsInThisProcess(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r := h.replica("e1", a, b)
	h.live.serve(a.KeyID(), b.KeyID())
	h.clock.add(24 * time.Hour)
	refresh(t, r)
	if _, err := r.Retire(context.Background(), a.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	doc, _ := h.store.Read(context.Background())
	doc.Keys[0].RetiredAt, doc.Keys[0].RetiredBy = time.Time{}, ""
	doc.Sequence++
	raw, _ := json.Marshal(doc)
	must(t, os.WriteFile(filepath.Join(h.dir, artifacts.PublishedKeysKey), raw, 0o600))
	refresh(t, r)
	signsWith(t, r, b)
}

// TestIssuer_ServedDocumentsAreBuiltOncePerSequence: the routes answer the
// same bytes without re-encoding until the document changes.
func TestIssuer_ServedDocumentsAreBuiltOncePerSequence(t *testing.T) {
	h := newHarness(t)
	r := h.replica("e1", newKey(t))
	refresh(t, r)
	first, _ := r.JWKS()
	again, _ := r.JWKS()
	if &first[0] != &again[0] {
		t.Fatal("the key set was built again for the same sequence")
	}
	h.replica("e2", newKey(t))
	refresh(t, r)
	third, _ := r.JWKS()
	if ks, _ := jwt.ParseKeySet(third); ks.Len() != 2 {
		t.Fatalf("after a change: %s", third)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestIssuer_AStaleCopyIsNotServed: past DefaultKeysCopyMaxAge without a read the
// key set and bundle are not served, as nothing signs: a key retired
// elsewhere must not stay trusted through this replica.
func TestIssuer_AStaleCopyIsNotServed(t *testing.T) {
	h := newHarness(t)
	r := h.replica("e1", newKey(t))
	r.cfg.TrustDomain = "ork.acme.dev"
	refresh(t, r)
	if _, err := r.JWKS(); err != nil {
		t.Fatal(err)
	}
	h.store.set(true)
	h.clock.add(DefaultKeysCopyMaxAge + time.Second)
	_ = r.Refresh(context.Background())
	if _, err := r.JWKS(); !errors.Is(err, ErrKeysStale) {
		t.Fatalf("JWKS past the bound: %v", err)
	}
	if _, err := r.Bundle(); !errors.Is(err, ErrKeysStale) {
		t.Fatalf("Bundle past the bound: %v", err)
	}
}

// TestIssuer_AnOlderReadMovesNoClock: a read older than the copy held is
// not adopted, and neither moves when the copy was read nor clears the last
// failure.
func TestIssuer_AnOlderReadMovesNoClock(t *testing.T) {
	h := newHarness(t)
	a, b := newKey(t), newKey(t)
	r := h.replica("e1", a, b)
	refresh(t, r)
	old, _ := h.store.Read(context.Background())
	if _, err := r.Retire(context.Background(), a.KeyID(), "alice", time.Time{}); err != nil {
		t.Fatal(err)
	}
	readAt := r.Status().ReadAt
	h.clock.add(time.Minute)
	r.take(old, h.clock.now(), nil)
	if got := r.Status().ReadAt; !got.Equal(readAt) {
		t.Fatalf("an older read moved readAt from %s to %s", readAt, got)
	}
}
