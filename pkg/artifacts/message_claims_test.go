package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	claimScope = ClaimScope{Namespace: "payments", Source: "orders", Orkflow: "settle"}
	claimAt    = time.Date(2026, 11, 25, 9, 30, 0, 0, time.UTC)
)

const (
	claimMessage = "jetstream:ORDERS:48213"
	claimRun     = "ork-01k2"
)

// messageClaimStores is the claim store over both backends, so nothing below holds
// on one and not on the other.
func messageClaimStores(t *testing.T) map[string]*MessageClaimStore {
	t.Helper()
	local, err := NewLocalMessageClaimStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := newS3Double(t)
	remote, err := NewMessageClaimStore(context.Background(), d.config())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []*MessageClaimStore{local, remote} {
		s.now = func() time.Time { return claimAt }
	}
	return map[string]*MessageClaimStore{"local": local, "s3": remote}
}

func TestMessageClaim_ConditionalCreateAndReadBack(t *testing.T) {
	for name, s := range messageClaimStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, found, err := s.Get(ctx, claimScope, claimMessage); err != nil || found {
				t.Fatalf("before any claim: found %v, %v", found, err)
			}
			first, err := s.Create(ctx, claimScope, claimMessage, claimRun)
			if err != nil || first.RunID != claimRun || !first.ClaimedAt.Equal(claimAt) || !first.Started.IsZero() {
				t.Fatalf("the first claim: %+v, %v", first, err)
			}
			// Another delivery gets the claim that holds, not its own.
			second, err := s.Create(ctx, claimScope, claimMessage, "ork-other")
			if err != nil || second != first {
				t.Fatalf("a second claim: %+v, %v; want %+v", second, err, first)
			}
			got, found, err := s.Get(ctx, claimScope, claimMessage)
			if err != nil || !found || got != first {
				t.Fatalf("read back: %+v, %v, %v; want %+v", got, found, err, first)
			}
		})
	}
}

func TestMessageClaim_OneOfManyHolds(t *testing.T) {
	for name, s := range messageClaimStores(t) {
		t.Run(name, func(t *testing.T) {
			const n = 12
			got := make([]MessageClaim, n)
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := range n {
				wg.Go(func() {
					got[i], errs[i] = s.Create(context.Background(), claimScope, claimMessage, "ork-"+strings.Repeat("a", i+1))
				})
			}
			wg.Wait()
			for i := range n {
				if errs[i] != nil || got[i] != got[0] {
					t.Fatalf("claim %d: %+v, %v; want the one that holds, %+v", i, got[i], errs[i], got[0])
				}
			}
		})
	}
}

func TestMessageClaim_NonceDrawnOnceNeverRewritten(t *testing.T) {
	for name, s := range messageClaimStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			a, err := s.Create(ctx, claimScope, claimMessage, claimRun)
			if err != nil || !nonceHex.MatchString(a.Nonce) {
				t.Fatalf("nonce %q, %v; want 32 hex digits", a.Nonce, err)
			}
			b, err := s.Create(ctx, claimScope, "another", claimRun)
			if err != nil || b.Nonce == a.Nonce {
				t.Fatalf("two claims share a nonce, or %v", err)
			}
			s.now = func() time.Time { return claimAt.Add(time.Hour) }
			if err := s.MarkStarted(ctx, claimScope, claimMessage, claimRun, claimAt.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			again, err := s.Create(ctx, claimScope, claimMessage, claimRun)
			if err != nil || again.Nonce != a.Nonce || !again.ClaimedAt.Equal(a.ClaimedAt) {
				t.Fatalf("the claim was rewritten: %+v, %v; was %+v", again, err, a)
			}
		})
	}
}

// Two namespaces, two names, and one name bound to two Orkflows never share
// a key, and no part can hold the byte that joins them.
func TestMessageClaim_ScopeIsNamespaceNameAndOrkflow(t *testing.T) {
	keys := map[string]ClaimScope{}
	for _, sc := range []ClaimScope{
		claimScope,
		{Namespace: "orders", Source: "payments", Orkflow: "settle"},
		{Namespace: "payments", Source: "orders", Orkflow: "refund"},
		{Namespace: "payments", Source: "settle", Orkflow: "orders"},
		{Namespace: "pay", Source: "mentsorders", Orkflow: "settle"},
	} {
		key, err := messageClaimKey(sc, claimMessage)
		if err != nil {
			t.Fatal(err)
		}
		if other, taken := keys[key]; taken {
			t.Errorf("%+v and %+v share %s", sc, other, key)
		}
		keys[key] = sc
	}
	for _, sc := range []ClaimScope{
		{}, {Namespace: "payments\x1forders", Source: "a", Orkflow: "b"}, {Namespace: "p", Source: "a\x1fb", Orkflow: "c"},
		{Namespace: "p", Source: "a", Orkflow: "../c"}, {Namespace: "P", Source: "a", Orkflow: "c"},
	} {
		if key, err := messageClaimKey(sc, claimMessage); err == nil {
			t.Errorf("%+v: key %s", sc, key)
		}
	}
	for name, id := range map[string]string{"empty": "", "long": strings.Repeat("i", 257)} {
		if key, err := messageClaimKey(claimScope, id); err == nil {
			t.Errorf("%s identity: key %s", name, key)
		}
	}
}

func TestStartedMarker_WriteOnceAndReadWithTheClaim(t *testing.T) {
	for name, s := range messageClaimStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := s.Create(ctx, claimScope, claimMessage, claimRun); err != nil {
				t.Fatal(err)
			}
			// A marker of another run, left by an earlier claim of this
			// message, is another object and is never read.
			if err := s.MarkStarted(ctx, claimScope, claimMessage, "ork-earlier", claimAt.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			if c, _, err := s.Get(ctx, claimScope, claimMessage); err != nil || !c.Started.IsZero() {
				t.Fatalf("another run's marker was read: %+v, %v", c, err)
			}
			started := claimAt.Add(time.Second)
			for _, at := range []time.Time{started, started.Add(time.Hour)} {
				if err := s.MarkStarted(ctx, claimScope, claimMessage, claimRun, at); err != nil {
					t.Fatal(err)
				}
			}
			c, found, err := s.Get(ctx, claimScope, claimMessage)
			if err != nil || !found || !c.Started.Equal(started) {
				t.Fatalf("started %v, %v, %v; want the first marker's %v", c.Started, found, err, started)
			}
			for _, bad := range []string{"", "ork/../x", "Ork-1", strings.Repeat("a", 200)} {
				if err := s.MarkStarted(ctx, claimScope, claimMessage, bad, started); err == nil {
					t.Errorf("a marker of run %q was written", bad)
				}
				if _, err := s.Create(ctx, claimScope, "m-"+claimMessage, bad); err == nil {
					t.Errorf("a claim of run %q was written", bad)
				}
			}
			if err := s.MarkStarted(ctx, claimScope, claimMessage, claimRun, time.Time{}); err == nil {
				t.Error("a marker without an instant was written")
			}
		})
	}
}

// What a claim that reads badly means differs: a version this build does not
// know waits for one that does; a complete claim of a known version that does
// not decode never will; anything else is a read that failed.
func TestMessageClaim_UnreadableIsTyped(t *testing.T) {
	good := `{"schemaVersion":1,"identity":"` + claimMessage + `","runId":"ork-1","claimedAt":"2026-11-25T09:30:00Z","nonce":"` + strings.Repeat("a", 32) + `"}`
	for _, tt := range []struct {
		name, body string
		want       error
	}{
		{"good", good, nil},
		{"a newer version", `{"schemaVersion":2,"run":{}}`, ErrClaimVersion},
		{"a field of no version", strings.Replace(good, `"nonce"`, `"more":1,"nonce"`, 1), ErrClaimCorrupt},
		{"another message's", strings.Replace(good, claimMessage, "other", 1), ErrClaimCorrupt},
		{"no run", strings.Replace(good, "ork-1", "", 1), ErrClaimCorrupt},
		{"no instant", strings.Replace(good, "2026-11-25T09:30:00Z", "0001-01-01T00:00:00Z", 1), ErrClaimCorrupt},
		{"a short nonce", strings.Replace(good, strings.Repeat("a", 32), "aa", 1), ErrClaimCorrupt},
		{"no version", `{"identity":"x"}`, ErrClaimCorrupt},
		{"too large", strings.Replace(good, `"nonce"`, strings.Repeat(" ", maxClaimBytes)+`"nonce"`, 1), ErrClaimCorrupt},
		{"a newer version, larger than this one's", `{"schemaVersion":2,"more":"` + strings.Repeat("x", 2*maxClaimBytes) + `"}`, ErrClaimVersion},
		{"larger than is read", `{"schemaVersion":2,"more":"` + strings.Repeat("x", maxClaimRead) + `"}`, ErrClaimIncomplete},
		{"a name twice", strings.Replace(good, `"nonce"`, `"runId":"ork-9","nonce"`, 1), ErrClaimCorrupt},
		{"a broken character", strings.Replace(good, "ork-1", "ork-\xff", 1), ErrClaimCorrupt},
		{"a newer version's broken character", "{\"schemaVersion\":2,\"x\":\"\xff\"}", ErrClaimVersion},
		{"cut short", good[:len(good)-5], ErrClaimIncomplete},
		{"empty", "", ErrClaimIncomplete},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for backend, s := range messageClaimStores(t) {
				key, err := messageClaimKey(claimScope, claimMessage)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.b.create(context.Background(), key, "application/json", []byte(tt.body)); err != nil {
					t.Fatal(err)
				}
				for what, call := range map[string]func() error{
					"get": func() error { _, _, err := s.Get(context.Background(), claimScope, claimMessage); return err },
					"create": func() error {
						_, err := s.Create(context.Background(), claimScope, claimMessage, "ork-2")
						return err
					},
				} {
					if err := call(); !errors.Is(err, tt.want) || (tt.want == nil) != (err == nil) {
						t.Errorf("%s %s: %v; want %v", backend, what, err, tt.want)
					}
				}
			}
		})
	}
}

// A marker is a shortcut past asking whether the run exists, so one that
// cannot be read is as good as none, and is said to be there: it never makes
// a message whose run may exist unprocessable. Only a newer build's is left
// to one that knows it.
func TestStartedMarker_UnreadableIsNoMarker(t *testing.T) {
	good := `{"schemaVersion":1,"runId":"` + claimRun + `","startedAt":"2026-11-25T09:30:00Z"}`
	for _, tt := range []struct {
		name, body string
		unread     bool
		want       error
	}{
		{name: "good", body: good},
		{name: "a newer version", body: `{"schemaVersion":9}`, want: ErrClaimVersion},
		{name: "another run's", body: strings.Replace(good, claimRun, "ork-other", 1), unread: true},
		{name: "no instant", body: `{"schemaVersion":1,"runId":"` + claimRun + `"}`, unread: true},
		{name: "cut short", body: good[:10], unread: true},
		{name: "empty", unread: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for backend, s := range messageClaimStores(t) {
				ctx := context.Background()
				if _, err := s.Create(ctx, claimScope, claimMessage, claimRun); err != nil {
					t.Fatal(err)
				}
				key, err := startedMarkerKey(claimScope, claimMessage, claimRun)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.b.create(ctx, key, "application/json", []byte(tt.body)); err != nil {
					t.Fatal(err)
				}
				c, found, err := s.Get(ctx, claimScope, claimMessage)
				if !errors.Is(err, tt.want) || (tt.want == nil) != (err == nil) {
					t.Errorf("%s: %v; want %v", backend, err, tt.want)
				}
				if err == nil && (!found || c.RunID != claimRun || c.MarkerUnreadable != tt.unread || c.Started.IsZero() != tt.unread) {
					t.Errorf("%s: %+v, found %v", backend, c, found)
				}
			}
		})
	}
}

// A create whose answer was lost, and that was stored, is this delivery's
// claim and not a rival's.
func TestMessageClaim_LostAnswerIsItsOwn(t *testing.T) {
	d := newS3Double(t)
	s, err := NewMessageClaimStore(context.Background(), d.config())
	if err != nil {
		t.Fatal(err)
	}
	s.b.(*s3Blobs).client = d.fastClient(t)
	key, err := messageClaimKey(claimScope, claimMessage)
	if err != nil {
		t.Fatal(err)
	}
	d.loseAnswer(key, 0)
	got, err := s.Create(context.Background(), claimScope, claimMessage, claimRun)
	if err != nil || got.RunID != claimRun {
		t.Fatalf("%+v, %v; want the claim of %s", got, err, claimRun)
	}
	read, found, err := s.Get(context.Background(), claimScope, claimMessage)
	if err != nil || !found || read != got {
		t.Fatalf("read %+v, %v, %v; want %+v", read, found, err, got)
	}
}

func FuzzMessageClaim(f *testing.F) {
	f.Add([]byte(`{"schemaVersion":1,"identity":"m","runId":"ork-1","claimedAt":"2026-11-25T09:30:00Z","nonce":"` + strings.Repeat("a", 32) + `"}`))
	f.Add([]byte(`{"schemaVersion":1,"runId":"ork-1","startedAt":"2026-11-25T09:30:00Z"}`))
	f.Add([]byte(`{"schemaVersion":2}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if rec, err := decodeClaim(raw); err == nil {
			again, merr := json.Marshal(rec)
			if merr != nil {
				t.Fatalf("a claim that reads does not write: %v", merr)
			}
			if back, err := decodeClaim(again); err != nil || back != rec {
				t.Fatalf("a claim does not round-trip: %+v, %v", back, err)
			}
			if !runIDKey.MatchString(rec.RunID) || rec.Identity == "" || rec.ClaimedAt.IsZero() || !nonceHex.MatchString(rec.Nonce) {
				t.Fatalf("a bad claim read: %+v", rec)
			}
		} else if !errors.Is(err, ErrClaimVersion) && !errors.Is(err, ErrClaimCorrupt) && !errors.Is(err, ErrClaimIncomplete) {
			t.Fatalf("an untyped refusal: %v", err)
		}
		if rec, err := decodeMarker(raw); err == nil && (!runIDKey.MatchString(rec.RunID) || rec.StartedAt.IsZero()) {
			t.Fatalf("a bad marker read: %+v", rec)
		}
	})
}

// TestMessageClaims_ProbeWrite: the probe writes and removes one object of its
// own, outside the claims.
func TestMessageClaims_ProbeWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalMessageClaimStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	counted := &createdKeys{blobs: store.b}
	store.b = counted
	for range 2 {
		if err := store.ProbeWrite(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(counted.keys) != 2 || counted.keys[0] == counted.keys[1] || !strings.HasPrefix(counted.keys[0], MessageClaimProbePrefix+"/") {
		t.Fatalf("%v", counted.keys)
	}
	if left, err := listKeys(context.Background(), store.b, MessageClaimProbePrefix+"/"); err != nil || len(left) != 0 {
		t.Fatalf("%v, %v", left, err)
	}
	counted.err = errors.New("503")
	if err := store.ProbeWrite(context.Background()); !errors.Is(err, counted.err) {
		t.Fatalf("%v", err)
	}
	counted.err = errBlobExists
	if err := store.ProbeWrite(context.Background()); err != nil {
		t.Fatalf("a key that is taken is an answer: %v", err)
	}
}

// createdKeys is a backend that notes what is created, or refuses it.
type createdKeys struct {
	blobs
	keys []string
	err  error
}

func (c *createdKeys) create(ctx context.Context, key, contentType string, body []byte) error {
	if c.err != nil {
		return c.err
	}
	c.keys = append(c.keys, key)
	return c.blobs.create(ctx, key, contentType, body)
}
