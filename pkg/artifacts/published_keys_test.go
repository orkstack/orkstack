package artifacts

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func publishedKeysStores(t *testing.T) map[string]*PublishedKeysStore {
	t.Helper()
	local, err := NewPublishedKeysStore(context.Background(), Config{Backend: BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s3, err := NewPublishedKeysStore(context.Background(), newS3Double(t).config())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*PublishedKeysStore{"local": local, "s3": s3}
}

func testPublishedKey(kid string, at time.Time) PublishedKey {
	return PublishedKey{KID: kid, Algorithm: "RS256", JWK: []byte(`{"kty":"RSA","kid":"` + kid + `"}`), PublishedAt: at}
}

// TestPublishedKeys_StartEmptyAndCountEachChange: no document reads as one
// with no key at sequence 0; each change is one more in its sequence, an update
// that changes nothing writes nothing, and what was written reads back.
func TestPublishedKeys_StartEmptyAndCountEachChange(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	for name, s := range publishedKeysStores(t) {
		t.Run(name, func(t *testing.T) {
			doc, err := s.Read(ctx)
			if err != nil || doc.Sequence != 0 || len(doc.Keys) != 0 {
				t.Fatalf("an absent document read %+v, %v", doc, err)
			}
			doc, err = s.Update(ctx, func(d *PublishedKeys) (bool, error) {
				d.Keys = append(d.Keys, testPublishedKey("k1", at))
				return true, nil
			})
			if err != nil || doc.Sequence != 1 || len(doc.Keys) != 1 {
				t.Fatalf("the first update = %+v, %v", doc, err)
			}
			doc, err = s.Update(ctx, func(*PublishedKeys) (bool, error) { return false, nil })
			if err != nil || doc.Sequence != 1 {
				t.Fatalf("an update that changed nothing = %+v, %v", doc, err)
			}
			doc, err = s.Update(ctx, func(d *PublishedKeys) (bool, error) {
				d.Keys[0].RetiredAt = at.Add(time.Hour)
				d.Keys[0].RetiredBy = "alice"
				return true, nil
			})
			if err != nil || doc.Sequence != 2 {
				t.Fatalf("a retire = %+v, %v", doc, err)
			}
			got, err := s.Read(ctx)
			if err != nil || got.Sequence != 2 || len(got.Keys) != 1 || got.Keys[0].RetiredBy != "alice" ||
				!got.Keys[0].RetiredAt.Equal(at.Add(time.Hour)) || string(got.Keys[0].JWK) != string(testPublishedKey("k1", at).JWK) {
				t.Fatalf("read back %+v, %v", got, err)
			}
		})
	}
}

// TestPublishedKeys_NeverDropsAKey: an update that would leave out a key, or
// change what it is, is refused and writes nothing. The document is what a
// signature is verified against for as long as a run it names can be read.
func TestPublishedKeys_NeverDropsAKey(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	for name, s := range publishedKeysStores(t) {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
				d.Keys = append(d.Keys, testPublishedKey("k1", at), testPublishedKey("k2", at))
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			for what, change := range map[string]func(*PublishedKeys){
				"a key left out":          func(d *PublishedKeys) { d.Keys = d.Keys[1:] },
				"another public key":      func(d *PublishedKeys) { d.Keys[0].JWK = []byte(`{"kty":"EC"}`) },
				"another algorithm":       func(d *PublishedKeys) { d.Keys[0].Algorithm = "ES256" },
				"its publication moved":   func(d *PublishedKeys) { d.Keys[0].PublishedAt = at.Add(time.Minute) },
				"a retire undone":         func(d *PublishedKeys) { d.Keys[0].RetiredAt = time.Time{} },
				"an activation undone":    func(d *PublishedKeys) { d.Keys[1].ActivatedAt = time.Time{} },
				"a retired key activated": func(d *PublishedKeys) { d.Keys[0].ActivatedAt = at },
				"a duplicate key id":      func(d *PublishedKeys) { d.Keys = append(d.Keys, testPublishedKey("k1", at)) },
				"an unknown algorithm": func(d *PublishedKeys) {
					d.Keys = append(d.Keys, PublishedKey{KID: "k3", Algorithm: "HS256", JWK: []byte(`{}`), PublishedAt: at})
				},
				"a key with no key id": func(d *PublishedKeys) {
					d.Keys = append(d.Keys, PublishedKey{Algorithm: "RS256", JWK: []byte(`{}`), PublishedAt: at})
				},
				"a public key not JSON": func(d *PublishedKeys) {
					d.Keys = append(d.Keys, PublishedKey{KID: "k3", Algorithm: "RS256", JWK: []byte(`[1]`), PublishedAt: at})
				},
				"a key never published": func(d *PublishedKeys) {
					d.Keys = append(d.Keys, PublishedKey{KID: "k3", Algorithm: "RS256", JWK: []byte(`{}`)})
				},
			} {
				// One retire and one activation stand before each case.
				if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
					if !d.Keys[0].RetiredAt.IsZero() {
						return false, nil
					}
					d.Keys[0].RetiredAt, d.Keys[1].ActivatedAt = at, at
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
				before, _ := s.Read(ctx)
				_, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) { change(d); return true, nil })
				if err == nil {
					t.Errorf("%s was written", what)
				}
				if after, _ := s.Read(ctx); after.Sequence != before.Sequence {
					t.Errorf("%s moved the sequence from %d to %d", what, before.Sequence, after.Sequence)
				}
			}
		})
	}
}

// TestPublishedKeys_UnreadableIsNeverOverwritten: a document that does not
// decode, or is of a format this release does not know, is an error on read
// and on update, and an update leaves it as it is.
func TestPublishedKeys_UnreadableIsNeverOverwritten(t *testing.T) {
	ctx := context.Background()
	for what, body := range map[string]string{
		"not JSON":           `{`,
		"a newer format":     `{"v":2,"sequence":3,"keys":[]}`,
		"a duplicate key id": `{"v":1,"sequence":1,"keys":[{"kid":"a","alg":"RS256","jwk":{},"publishedAt":"2026-11-15T12:00:00Z"},{"kid":"a","alg":"RS256","jwk":{},"publishedAt":"2026-11-15T12:00:00Z"}]}`,
		"a negative count":   `{"v":1,"sequence":-1,"keys":[]}`,
	} {
		t.Run(what, func(t *testing.T) {
			d := newS3Double(t)
			s, err := NewPublishedKeysStore(ctx, d.config())
			if err != nil {
				t.Fatal(err)
			}
			d.objects[PublishedKeysKey] = []byte(body)
			if _, err := s.Read(ctx); !errors.Is(err, ErrPublishedKeysUnreadable) {
				t.Fatalf("read = %v", err)
			}
			if _, err := s.Update(ctx, func(*PublishedKeys) (bool, error) { return true, nil }); !errors.Is(err, ErrPublishedKeysUnreadable) {
				t.Fatalf("update = %v", err)
			}
			if string(d.objects[PublishedKeysKey]) != body {
				t.Fatalf("the document was overwritten: %s", d.objects[PublishedKeysKey])
			}
		})
	}
}

// TestPublishedKeys_UpdatesFromTwoEnginesBothLand: engines that add their
// keys at once each keep their own; a lost conditional write is read again
// and redone, never answered as success.
func TestPublishedKeys_UpdatesFromTwoEnginesBothLand(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for i := range 6 {
		s, err := NewPublishedKeysStore(ctx, d.config())
		if err != nil {
			t.Fatal(err)
		}
		kid := string(rune('a' + i))
		wg.Go(func() {
			_, err := s.Update(ctx, func(doc *PublishedKeys) (bool, error) {
				for _, k := range doc.Keys {
					if k.KID == kid {
						return false, nil
					}
				}
				doc.Keys = append(doc.Keys, testPublishedKey(kid, at))
				return true, nil
			})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	contended := 0
	for err := range errs {
		if errors.Is(err, ErrPublishedKeysContended) {
			contended++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	s, _ := NewPublishedKeysStore(ctx, d.config())
	doc, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Keys)+contended != 6 || doc.Sequence != int64(len(doc.Keys)) {
		t.Fatalf("%d keys at sequence %d with %d contended", len(doc.Keys), doc.Sequence, contended)
	}
}

// TestPublishedKeys_ACallersErrorWritesNothing: an update whose function
// fails returns that error and leaves the document unchanged.
func TestPublishedKeys_ACallersErrorWritesNothing(t *testing.T) {
	ctx := context.Background()
	s := publishedKeysStores(t)["s3"]
	boom := errors.New("boom")
	if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
		d.Keys = append(d.Keys, testPublishedKey("k1", time.Now()))
		return true, boom
	}); !errors.Is(err, boom) {
		t.Fatalf("update = %v", err)
	}
	if doc, err := s.Read(ctx); err != nil || len(doc.Keys) != 0 {
		t.Fatalf("after a failed update %+v, %v", doc, err)
	}
	if !strings.HasPrefix(PublishedKeysKey, IdentityPrefix+"/issuer/") {
		t.Fatalf("the document lies at %s", PublishedKeysKey)
	}
}

// FuzzPublishedKeys_ADocumentReadKeepsItsRules: whatever bytes lie at the
// document's key, a read either fails as unreadable or answers a document
// that holds every rule, and an update that changes nothing leaves the
// stored bytes exactly as they were.
func FuzzPublishedKeys_ADocumentReadKeepsItsRules(f *testing.F) {
	for _, seed := range []string{
		``, `{`, `{"v":1,"sequence":0,"keys":[]}`, `{"v":2,"sequence":3,"keys":[]}`,
		`{"v":1,"sequence":1,"keys":[{"kid":"a","alg":"RS256","jwk":{"kty":"RSA"},"publishedAt":"2026-11-15T12:00:00Z"}]}`,
		`{"v":1,"sequence":1,"keys":[{"kid":"a","alg":"HS256","jwk":{},"publishedAt":"2026-11-15T12:00:00Z"}]}`,
		`{"v":1,"sequence":1,"keys":[{"kid":"a","alg":"RS256","jwk":[],"publishedAt":"2026-11-15T12:00:00Z"}]}`,
		`{"v":1,"sequence":-5,"keys":null}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		ctx := context.Background()
		d := newS3Double(t)
		s, err := NewPublishedKeysStore(ctx, d.config())
		if err != nil {
			t.Fatal(err)
		}
		d.objects[PublishedKeysKey] = body
		doc, err := s.Read(ctx)
		if err != nil {
			if !errors.Is(err, ErrPublishedKeysUnreadable) {
				t.Fatalf("a stored document failed as %v", err)
			}
			return
		}
		if err := checkPublishedKeys(doc); err != nil {
			t.Fatalf("a document read breaks its rules: %v", err)
		}
		if _, err := s.Update(ctx, func(*PublishedKeys) (bool, error) { return false, nil }); err != nil {
			t.Fatalf("an update that changes nothing failed: %v", err)
		}
		if !bytes.Equal(d.objects[PublishedKeysKey], body) {
			t.Fatal("an update that changes nothing rewrote the document")
		}
	})
}

// TestPublishedKeys_ACompromiseIsMarkedOnceOnARetiredKey: compromised_from
// is set only on a retired key, may be added after a plain retirement, and
// once set never moves or goes.
func TestPublishedKeys_ACompromiseIsMarkedOnceOnARetiredKey(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	s := publishedKeysStores(t)["s3"]
	if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
		d.Keys = append(d.Keys, testPublishedKey("k1", at), testPublishedKey("k2", at))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
		d.Keys[1].CompromisedFrom = at
		return true, nil
	}); err == nil {
		t.Fatal("an unretired key was marked compromised")
	}
	if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
		d.Keys[0].RetiredAt, d.Keys[0].RetiredBy = at.Add(time.Hour), "alice"
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
		d.Keys[0].CompromisedFrom = at.Add(-time.Hour)
		return true, nil
	}); err != nil {
		t.Fatalf("a compromise added to a retired key: %v", err)
	}
	if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) {
		d.Keys[0].CompromisedFrom = at.Add(-2 * time.Hour)
		return true, nil
	}); err != nil {
		t.Fatalf("a compromise moved earlier: %v", err)
	}
	for what, change := range map[string]func(*PublishedKeys){
		"moved later": func(d *PublishedKeys) { d.Keys[0].CompromisedFrom = at },
		"removed":     func(d *PublishedKeys) { d.Keys[0].CompromisedFrom = time.Time{} },
	} {
		if _, err := s.Update(ctx, func(d *PublishedKeys) (bool, error) { change(d); return true, nil }); !errors.Is(err, ErrPublishedKeysRefused) {
			t.Errorf("a compromise %s: %v", what, err)
		}
	}
	doc, _ := s.Read(ctx)
	if !doc.Keys[0].CompromisedFrom.Equal(at.Add(-2 * time.Hour)) {
		t.Fatalf("stored %v", doc.Keys[0].CompromisedFrom)
	}
}
