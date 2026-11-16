package artifacts

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// PublishedKeysKey is the one document of the engines' public signing keys
// (docs/STORAGE.md): every key any replica was configured with, each with
// when it was published, when it began to sign and when it was retired.
// Replicas add their keys by compare-and-swap and never remove one: it is
// what a relying party is served and what a signature is verified against
// for as long as anything it signed can be read. Only an engine's
// credential writes under IdentityPrefix.
const PublishedKeysKey = IdentityPrefix + "/issuer/jwks.json"

const (
	publishedKeysFormat   = 1
	publishedKeysTries    = 8
	publishedKeysMaxBytes = 4 << 20
	// PublishedKeysMaxKeys bounds the document: a key is never removed, so
	// this is how many keys a deployment may ever publish.
	PublishedKeysMaxKeys = 1024
	// PublishedKeyMaxHolders bounds one key's list of the engines that
	// confirmed holding it.
	PublishedKeyMaxHolders = 256
)

// ErrPublishedKeysUnreadable is a stored document that does not decode, is
// of a format this release does not know, or breaks the document's rules.
// It is never read as "no keys" and never overwritten.
var ErrPublishedKeysUnreadable = errors.New("the published signing keys document is unreadable")

// ErrPublishedKeysRefused is a change the document's rules refuse: a key
// removed or changed, an activation or a retirement undone, or a bound
// passed.
var ErrPublishedKeysRefused = errors.New("the change to the published signing keys is refused")

// ErrPublishedKeysContended is an update that lost its conditional write
// every time.
var ErrPublishedKeysContended = errors.New("the published signing keys document is being changed by another engine; try again")

// PublishedKeys is the stored document.
type PublishedKeys struct {
	V int `json:"v"`
	// Sequence counts the document's changes; it is a SPIFFE bundle's
	// sequence.
	Sequence int64          `json:"sequence"`
	Keys     []PublishedKey `json:"keys"`
}

// PublishedKey is one public key and its life.
type PublishedKey struct {
	// KID is the key's RFC 7638 thumbprint.
	KID       string `json:"kid"`
	Algorithm string `json:"alg"`
	// JWK is the public key as a JSON Web Key, written by internal/jwt and
	// opaque here.
	JWK         jsontext.Value `json:"jwk"`
	PublishedAt time.Time      `json:"publishedAt"`
	// PublishedBy is the engine that first published it.
	PublishedBy string `json:"publishedBy,omitzero"`
	// ActivatedAt is when it first became eligible to sign. Once set it
	// stands: the key signs on every replica whatever later checks of the
	// live issuer say.
	ActivatedAt time.Time `json:"activatedAt,omitzero"`
	// RetiredAt and RetiredBy: a retired key never signs again.
	RetiredAt time.Time `json:"retiredAt,omitzero"`
	RetiredBy string    `json:"retiredBy,omitzero"`
	// CompromisedFrom is the instant from which a key retired as
	// compromised verifies nothing it signed; what it signed for an
	// instant before it still verifies (authn-authz.md §6.6). It is set
	// only on a retired key, and once set it only ever moves earlier.
	CompromisedFrom time.Time `json:"compromisedFrom,omitzero"`
	// Holders are the engines that confirmed holding the private key, each
	// with when it last did. A report, never a control.
	Holders []KeyHolder `json:"holders,omitzero"`
}

// KeyHolder is one engine that holds a key.
type KeyHolder struct {
	Engine      string    `json:"engine"`
	ConfirmedAt time.Time `json:"confirmedAt"`
}

// Retired says whether the key may never sign again.
func (k PublishedKey) Retired() bool { return !k.RetiredAt.IsZero() }

// Key finds kid's entry.
func (d PublishedKeys) Key(kid string) (PublishedKey, bool) {
	for _, k := range d.Keys {
		if k.KID == kid {
			return k, true
		}
	}
	return PublishedKey{}, false
}

// PublishedKeysStore reads and changes the published keys document.
type PublishedKeysStore struct {
	blobBase
}

// NewPublishedKeysStore opens the store on the deployment's backend.
func NewPublishedKeysStore(ctx context.Context, cfg Config) (*PublishedKeysStore, error) {
	b, err := openBlobs(ctx, cfg, "published keys store", IdentityPrefix)
	if err != nil {
		return nil, err
	}
	return &PublishedKeysStore{blobBase: blobBase{b}}, nil
}

// Read answers the document; none stored reads as no keys at sequence 0.
func (s *PublishedKeysStore) Read(ctx context.Context) (PublishedKeys, error) {
	doc, _, err := s.get(ctx)
	return doc, err
}

// Update reads the document, hands it to change, and writes what change
// left when it reports a change, one more in its sequence, while the document
// is still the one read; otherwise it reads again and calls change again.
// A change that would remove a key, change what a key is, or undo an
// activation or a retirement is refused, and nothing is written. An error
// from change is returned as it is.
func (s *PublishedKeysStore) Update(ctx context.Context, change func(*PublishedKeys) (bool, error)) (PublishedKeys, error) {
	for range publishedKeysTries {
		before, version, err := s.get(ctx)
		if err != nil {
			return PublishedKeys{}, err
		}
		next := clonePublishedKeys(before)
		changed, err := change(&next)
		if err != nil {
			return PublishedKeys{}, err
		}
		if !changed {
			return before, nil
		}
		next.V, next.Sequence = publishedKeysFormat, before.Sequence+1
		if err := checkPublishedKeys(next); err != nil {
			return PublishedKeys{}, fmt.Errorf("artifacts: %s: %w: %w", s.b.uri(PublishedKeysKey), ErrPublishedKeysRefused, err)
		}
		if err := checkKeysKept(before, next); err != nil {
			return PublishedKeys{}, fmt.Errorf("artifacts: %s: %w: %w", s.b.uri(PublishedKeysKey), ErrPublishedKeysRefused, err)
		}
		body, err := stablejson.Marshal(next)
		if err != nil {
			return PublishedKeys{}, fmt.Errorf("artifacts: encode %s: %w", s.b.uri(PublishedKeysKey), err)
		}
		if len(body) > publishedKeysMaxBytes {
			return PublishedKeys{}, fmt.Errorf("artifacts: %s would be %d bytes, more than %d: %w", s.b.uri(PublishedKeysKey), len(body), publishedKeysMaxBytes, ErrPublishedKeysRefused)
		}
		if version == "" {
			err = s.b.create(ctx, PublishedKeysKey, "application/json", body)
		} else {
			err = s.b.replace(ctx, PublishedKeysKey, "application/json", body, version)
		}
		if errors.Is(err, errBlobExists) || errors.Is(err, errBlobChanged) {
			continue
		}
		if err != nil {
			return PublishedKeys{}, fmt.Errorf("artifacts: write %s: %w", s.b.uri(PublishedKeysKey), err)
		}
		return next, nil
	}
	return PublishedKeys{}, fmt.Errorf("artifacts: %s: %w", s.b.uri(PublishedKeysKey), ErrPublishedKeysContended)
}

func (s *PublishedKeysStore) get(ctx context.Context) (PublishedKeys, string, error) {
	raw, version, err := s.b.get(ctx, PublishedKeysKey, publishedKeysMaxBytes)
	if errors.Is(err, errBlobNotFound) {
		return PublishedKeys{V: publishedKeysFormat}, "", nil
	}
	if err != nil {
		return PublishedKeys{}, "", fmt.Errorf("artifacts: read %s: %w", s.b.uri(PublishedKeysKey), err)
	}
	doc, err := ParsePublishedKeys(raw)
	if err != nil {
		return PublishedKeys{}, "", fmt.Errorf("artifacts: %s: %w", s.b.uri(PublishedKeysKey), err)
	}
	return doc, version, nil
}

// ParsePublishedKeys reads the document's bytes, wherever they were kept
// (the store, an engine's copy on disk), and holds them to its rules. Any
// failure is ErrPublishedKeysUnreadable.
func ParsePublishedKeys(raw []byte) (PublishedKeys, error) {
	if len(raw) > publishedKeysMaxBytes {
		return PublishedKeys{}, fmt.Errorf("larger than %d bytes: %w", publishedKeysMaxBytes, ErrPublishedKeysUnreadable)
	}
	var doc PublishedKeys
	if err := json.Unmarshal(raw, &doc); err != nil {
		return PublishedKeys{}, fmt.Errorf("decode: %w: %w", ErrPublishedKeysUnreadable, err)
	}
	if doc.V != publishedKeysFormat {
		return PublishedKeys{}, fmt.Errorf("format %d, which this release does not read: %w", doc.V, ErrPublishedKeysUnreadable)
	}
	if err := checkPublishedKeys(doc); err != nil {
		return PublishedKeys{}, fmt.Errorf("%w: %w", ErrPublishedKeysUnreadable, err)
	}
	return doc, nil
}

// checkPublishedKeys holds a document to its rules on its own.
func checkPublishedKeys(d PublishedKeys) error {
	if d.Sequence < 0 {
		return fmt.Errorf("sequence %d is negative", d.Sequence)
	}
	if len(d.Keys) > PublishedKeysMaxKeys {
		return fmt.Errorf("%d keys, more than %d", len(d.Keys), PublishedKeysMaxKeys)
	}
	seen := make(map[string]bool, len(d.Keys))
	for i, k := range d.Keys {
		switch {
		case k.KID == "":
			return fmt.Errorf("key %d has no kid", i)
		case seen[k.KID]:
			return fmt.Errorf("key %q is listed twice", k.KID)
		case k.Algorithm != "RS256" && k.Algorithm != "ES256":
			return fmt.Errorf("key %q has algorithm %q, not RS256 or ES256", k.KID, k.Algorithm)
		case k.PublishedAt.IsZero():
			return fmt.Errorf("key %q has no publication time", k.KID)
		case !k.CompromisedFrom.IsZero() && !k.Retired():
			return fmt.Errorf("key %q is marked compromised and is not retired", k.KID)
		case len(k.Holders) > PublishedKeyMaxHolders:
			return fmt.Errorf("key %q lists %d holders, more than %d", k.KID, len(k.Holders), PublishedKeyMaxHolders)
		}
		if v := k.JWK; len(v) == 0 || v.Kind() != '{' || !v.IsValid() {
			return fmt.Errorf("key %q's public key is not a JSON object", k.KID)
		}
		seen[k.KID] = true
	}
	return nil
}

// checkKeysKept refuses a change that drops a key or rewrites what a key
// is, its publication, an activation or a retirement.
func checkKeysKept(before, after PublishedKeys) error {
	for _, old := range before.Keys {
		now, ok := after.Key(old.KID)
		switch {
		case !ok:
			return fmt.Errorf("key %q would be removed; a published key is never removed", old.KID)
		case now.Algorithm != old.Algorithm || !bytes.Equal(now.JWK, old.JWK) || !now.PublishedAt.Equal(old.PublishedAt) || now.PublishedBy != old.PublishedBy:
			return fmt.Errorf("key %q would change; a published key never changes", old.KID)
		case !old.ActivatedAt.IsZero() && !now.ActivatedAt.Equal(old.ActivatedAt):
			return fmt.Errorf("key %q's activation would change", old.KID)
		case !old.CompromisedFrom.IsZero() && (now.CompromisedFrom.IsZero() || now.CompromisedFrom.After(old.CompromisedFrom)):
			return fmt.Errorf("key %q's compromise would move later or go; it only ever moves earlier", old.KID)
		case old.Retired() && old.ActivatedAt.IsZero() && !now.ActivatedAt.IsZero():
			return fmt.Errorf("key %q is retired and can never be activated", old.KID)
		case old.Retired() && (!now.RetiredAt.Equal(old.RetiredAt) || now.RetiredBy != old.RetiredBy):
			return fmt.Errorf("key %q's retirement would change; a retired key stays retired", old.KID)
		}
	}
	return nil
}

func clonePublishedKeys(d PublishedKeys) PublishedKeys {
	out := d
	out.Keys = make([]PublishedKey, len(d.Keys))
	for i, k := range d.Keys {
		k.JWK = bytes.Clone(k.JWK)
		k.Holders = append([]KeyHolder(nil), k.Holders...)
		out.Keys[i] = k
	}
	return out
}
