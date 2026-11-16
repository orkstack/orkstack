package identity

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	// RefreshInterval is how often a replica reads the published keys
	// document again (workload-identity.md §5).
	RefreshInterval = time.Minute
	// DefaultKeysCopyMaxAge is identity.keys_copy_max_age's default: how
	// old a copy of the document, in memory or on disk, may be and still
	// decide. Past it no key signs a token or a decision and no decision
	// verifies, so a retirement reaches every replica that goes on signing.
	DefaultKeysCopyMaxAge = time.Hour
	// HolderRefresh is how old a replica lets its own confirmation of a key
	// grow before it writes it again; HolderForget is the age past which a
	// confirmation no longer counts and is dropped from the document.
	HolderRefresh = 24 * time.Hour
	HolderForget  = 7 * 24 * time.Hour
	// BundleRefreshHint is the SPIFFE bundle's spiffe_refresh_hint.
	BundleRefreshHint = 5 * time.Minute
)

// ErrKeyNotActive is a request to sign when no key qualifies
// (IDENTITY_KEY_NOT_ACTIVE). Its message names, for each configured key,
// the condition that fails.
var ErrKeyNotActive = errors.New("no signing key is active")

// ErrNoIssuer is a token, or an issuer document, asked of an engine that
// holds signing keys with identity.issuer unset: its keys sign decisions
// only (authn-authz.md §6.6).
var ErrNoIssuer = errors.New("task identity is off: identity.issuer is not set")

// ErrCannotSign is a decision this engine cannot sign: it holds no key the
// published keys list unretired, or its copy of them is past its age.
var ErrCannotSign = errors.New("this engine cannot sign a decision")

// ErrKeyNotFound is a key id the document does not hold.
var ErrKeyNotFound = errors.New("no such signing key")

// ErrKeysNotRead is a served document asked of an engine that has not read
// the published keys yet: an empty set a relying party would cache is never
// answered in its place.
var ErrKeysNotRead = errors.New("the published signing keys have not been read yet")

// ErrKeysStale is a served document asked of an engine whose copy of the
// published keys is older than identity.keys_copy_max_age: it signs nothing, and a key
// retired meanwhile must not stay trusted through it.
var ErrKeysStale = errors.New("the published signing keys could not be read for longer than an engine signs from its last copy")

// ErrNoServedKeys is a key set or bundle with no key in it: every key is
// retired or cannot be served. An empty set a relying party would cache is
// never answered in its place.
var ErrNoServedKeys = errors.New("no published signing key can be served")

// ErrNoTrustDomain is a SPIFFE bundle asked of a deployment with no trust
// domain.
var ErrNoTrustDomain = errors.New("no SPIFFE trust domain is set (identity.spiffe.trust_domain)")

// KeyStore is the published keys document (artifacts.PublishedKeysStore).
type KeyStore interface {
	Read(ctx context.Context) (artifacts.PublishedKeys, error)
	Update(ctx context.Context, change func(*artifacts.PublishedKeys) (bool, error)) (artifacts.PublishedKeys, error)
}

// LiveIssuer answers which key ids the issuer URL serves now.
type LiveIssuer interface {
	PublishedKeyIDs(ctx context.Context) (map[string]bool, error)
}

// Config is one engine's issuer settings.
type Config struct {
	// URL is identity.issuer: https, with no query, fragment or user.
	// Empty, the keys sign decisions only: no token, nothing served.
	URL string
	// Keys are identity.signing.key_files in order: the first that
	// qualifies signs.
	Keys []*jwt.Signer
	// Algorithm is identity.signing.algorithm; every key must be of it.
	Algorithm       jwt.Algorithm
	ActivationDelay time.Duration
	VerifyPublished bool
	SubjectFormat   Format
	TrustDomain     string
	// Engine names this replica in the document's holders.
	Engine string
	// ClockSkew is auth.clock_skew, the leeway a key's life gets at each
	// end; CopyMaxAge is identity.keys_copy_max_age (zero:
	// DefaultKeysCopyMaxAge).
	ClockSkew  time.Duration
	CopyMaxAge time.Duration
	// CopyFile is identity.keys_copy_file: where the last good read of the
	// document is kept on disk. Empty, there is no copy on disk.
	CopyFile string
	// Observer is told of each refresh after which the copy of the
	// published keys is past CopyMaxAge (nil: nobody is).
	Observer CopyObserver
}

// KeyStatus is one key as this replica sees it.
type KeyStatus struct {
	KeyID     string
	Algorithm jwt.Algorithm
	// Published is false for a key this replica holds that is not in the
	// document yet.
	Published       bool
	PublishedAt     time.Time
	PublishedBy     string
	ActivatedAt     time.Time
	RetiredAt       time.Time
	RetiredBy       string
	CompromisedFrom time.Time
	// Active says this replica would sign with it now; Why says why not.
	Active bool
	Why    string
	// HeldHere: this replica's configuration lists the key. Holders are
	// the engines that confirmed it within HolderForget; Unheld says none
	// did and it is not retired.
	HeldHere bool
	Holders  []artifacts.KeyHolder
	Unheld   bool
}

// Issuer keeps one replica's signing keys published and says which signs.
type Issuer struct {
	cfg   Config
	store KeyStore
	live  LiveIssuer
	log   *slog.Logger
	now   func() time.Time

	mu        sync.Mutex
	doc       artifacts.PublishedKeys
	readAt    time.Time
	readErr   error
	liveErr   error
	liveKids  map[string]bool
	liveAt    time.Time
	reported  map[string]bool
	published bool
	// retiredSeen are the keys this process has read as retired: a document
	// that later says otherwise (one written around the engines) is not
	// believed while it runs.
	retiredSeen map[string]bool
	// compromisedSeen is the earliest compromise this process has read for
	// each key: a document that says less is not believed while it runs.
	compromisedSeen map[string]time.Time
	// served caches the documents built for one sequence.
	served servedCache
	// fromCopy says the document held was loaded from the copy on disk: the
	// store's next answer replaces it whatever its sequence, so a store
	// restored from a backup cannot wedge the engine.
	fromCopy bool
	// lastKidRead is when an unknown kid last read the document.
	lastKidRead time.Time
	// copyMu orders writes of the copy file; copyAt is the fetchedAt of the
	// last one written, never lowered.
	copyMu  sync.Mutex
	copySeq int64
	copyAt  time.Time
	copyErr error
}

// servedCache is the key set and bundle built for one sequence of the
// document, so an unauthenticated route never re-encodes per request.
type servedCache struct {
	seq          int64
	jwks, bundle []byte
}

// New checks cfg and returns an Issuer that has read nothing yet.
func New(cfg Config, store KeyStore, live LiveIssuer, log *slog.Logger) (*Issuer, error) {
	if cfg.URL != "" {
		u, err := CheckIssuerURL(cfg.URL)
		if err != nil {
			return nil, err
		}
		cfg.URL = u
	} else {
		cfg.VerifyPublished = false
	}
	if len(cfg.Keys) == 0 {
		return nil, errors.New("identity.signing.key_files lists no key")
	}
	if cfg.CopyMaxAge == 0 {
		cfg.CopyMaxAge = DefaultKeysCopyMaxAge
	}
	if cfg.CopyMaxAge < 0 || cfg.ClockSkew < 0 {
		return nil, fmt.Errorf("identity.keys_copy_max_age %s and auth.clock_skew %s must not be negative", cfg.CopyMaxAge, cfg.ClockSkew)
	}
	if cfg.Algorithm != jwt.RS256 && cfg.Algorithm != jwt.ES256 {
		return nil, fmt.Errorf("identity.signing.algorithm %q is not RS256 or ES256", cfg.Algorithm)
	}
	seen := map[string]bool{}
	for i, k := range cfg.Keys {
		if k.Algorithm() != cfg.Algorithm {
			return nil, fmt.Errorf("identity.signing.key_files entry %d (kid %s) is a %s key; identity.signing.algorithm is %s", i+1, k.KeyID(), k.Algorithm(), cfg.Algorithm)
		}
		if seen[k.KeyID()] {
			return nil, fmt.Errorf("identity.signing.key_files lists the key %s twice", k.KeyID())
		}
		seen[k.KeyID()] = true
	}
	if cfg.ActivationDelay < 0 {
		return nil, fmt.Errorf("identity.signing.activation_delay %s is negative", cfg.ActivationDelay)
	}
	if cfg.SubjectFormat == "" {
		cfg.SubjectFormat = FormatOrk
	}
	if _, err := ParseFormat(string(cfg.SubjectFormat)); err != nil {
		return nil, fmt.Errorf("identity.subject_format: %w", err)
	}
	if cfg.TrustDomain != "" {
		if err := ValidTrustDomain(cfg.TrustDomain); err != nil {
			return nil, fmt.Errorf("identity.spiffe.trust_domain: %w", err)
		}
	} else if cfg.SubjectFormat == FormatSPIFFE {
		return nil, errors.New("identity.subject_format is spiffe and identity.spiffe.trust_domain is not set")
	}
	if cfg.Engine == "" {
		return nil, errors.New("an issuer needs the engine's name")
	}
	if cfg.VerifyPublished && live == nil {
		return nil, errors.New("identity.signing.verify_published needs a reader of the issuer URL")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Issuer{cfg: cfg, store: store, live: live, log: log, now: time.Now, reported: map[string]bool{}, retiredSeen: map[string]bool{},
		compromisedSeen: map[string]time.Time{}}, nil
}

// CheckIssuerURL holds identity.issuer to its rule and answers it without
// a trailing slash: an https URL with a host, and no user, query or
// fragment.
func CheckIssuerURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return "", fmt.Errorf("identity.issuer %q is not an https URL with a host and no user, query or fragment", raw)
	}
	return strings.TrimRight(raw, "/"), nil
}

// URL is the issuer.
func (i *Issuer) URL() string { return i.cfg.URL }

// SubjectFormat and TrustDomain are the deployment's subject settings.
func (i *Issuer) SubjectFormat() Format { return i.cfg.SubjectFormat }
func (i *Issuer) TrustDomain() string   { return i.cfg.TrustDomain }

// Publish adds this replica's keys to the document and records it as their
// holder, then takes the document as read. Run at boot; it changes
// nothing when every key is there with a fresh confirmation.
func (i *Issuer) Publish(ctx context.Context) error {
	now := i.now().UTC()
	doc, err := i.store.Update(ctx, func(d *artifacts.PublishedKeys) (bool, error) {
		changed := false
		for _, k := range i.cfg.Keys {
			at := slices.IndexFunc(d.Keys, func(p artifacts.PublishedKey) bool { return p.KID == k.KeyID() })
			if at < 0 {
				jwk, err := k.Public().MarshalJWK(k.KeyID())
				if err != nil {
					return false, err
				}
				d.Keys = append(d.Keys, artifacts.PublishedKey{KID: k.KeyID(), Algorithm: string(k.Algorithm()), JWK: jwk,
					PublishedAt: now, PublishedBy: i.cfg.Engine})
				at = len(d.Keys) - 1
				changed = true
			}
			if i.confirm(&d.Keys[at], now) {
				changed = true
			}
		}
		if forget(d, now) {
			changed = true
		}
		return changed, nil
	})
	if err != nil {
		i.mu.Lock()
		i.readErr = err
		i.mu.Unlock()
		return fmt.Errorf("publish the signing keys: %w", err)
	}
	i.adopt(doc, now)
	i.mu.Lock()
	i.published = true
	i.mu.Unlock()
	return nil
}

// confirm records this replica as holding k at now, unless its own
// confirmation is younger than HolderRefresh. It reports a change.
func (i *Issuer) confirm(k *artifacts.PublishedKey, now time.Time) bool {
	for j, h := range k.Holders {
		if h.Engine == i.cfg.Engine {
			if now.Sub(h.ConfirmedAt) < HolderRefresh {
				return false
			}
			k.Holders[j].ConfirmedAt = now
			return true
		}
	}
	k.Holders = append(k.Holders, artifacts.KeyHolder{Engine: i.cfg.Engine, ConfirmedAt: now})
	if len(k.Holders) > artifacts.PublishedKeyMaxHolders {
		slices.SortFunc(k.Holders, func(a, b artifacts.KeyHolder) int { return b.ConfirmedAt.Compare(a.ConfirmedAt) })
		k.Holders = k.Holders[:artifacts.PublishedKeyMaxHolders]
	}
	return true
}

// confirmedLately says this replica's confirmation of p is younger than
// HolderRefresh.
func (i *Issuer) confirmedLately(p artifacts.PublishedKey, now time.Time) bool {
	return slices.ContainsFunc(p.Holders, func(h artifacts.KeyHolder) bool {
		return h.Engine == i.cfg.Engine && now.Sub(h.ConfirmedAt) < HolderRefresh
	})
}

// forget drops confirmations older than HolderForget.
func forget(d *artifacts.PublishedKeys, now time.Time) bool {
	changed := false
	for j := range d.Keys {
		kept := d.Keys[j].Holders[:0]
		for _, h := range d.Keys[j].Holders {
			if now.Sub(h.ConfirmedAt) <= HolderForget {
				kept = append(kept, h)
			} else {
				changed = true
			}
		}
		if len(kept) == 0 {
			kept = nil
		}
		d.Keys[j].Holders = kept
	}
	return changed
}

// Refresh reads the document again. A key this replica holds that has
// become eligible and has never signed is checked against the live issuer
// (identity.signing.verify_published) and, when it is served there,
// marked activated in the document, after which it signs on every
// replica. A key this replica holds that it has not yet published (its
// boot's Publish failed) is published now.
func (i *Issuer) Refresh(ctx context.Context) error {
	i.mu.Lock()
	published := i.published
	i.mu.Unlock()
	if !published {
		err := i.Publish(ctx)
		if err == nil {
			return nil
		}
		// A publish that cannot be written still leaves a readable
		// document to sign from.
		if doc, rerr := i.store.Read(ctx); rerr == nil {
			i.adopt(doc, i.now().UTC())
			i.mu.Lock()
			i.readErr = err
			i.mu.Unlock()
		} else if errors.Is(rerr, artifacts.ErrPublishedKeysUnreadable) {
			// The store answers with a document that breaks its rules: a
			// copy must not mask that.
			i.log.Error("identity: the published signing keys document is unreadable; not deciding from the copy", "err", rerr)
			err = errors.Join(err, rerr)
		} else {
			// The store is away: decide from the copy on disk, if there is
			// one, until it answers.
			i.loadCopy()
			err = errors.Join(err, rerr)
		}
		i.mu.Lock()
		i.readErr = err
		i.mu.Unlock()
		return err
	}
	now := i.now().UTC()
	doc, err := i.store.Read(ctx)
	if err != nil {
		i.mu.Lock()
		i.readErr = err
		i.mu.Unlock()
		return fmt.Errorf("read the signing keys (signing from the last copy for at most %s): %w", i.cfg.CopyMaxAge, err)
	}
	// The read counts on its own: a write that fails below must not stop
	// signing while the document is readable.
	i.adopt(doc, now)
	var waiting []string
	for _, k := range i.cfg.Keys {
		p, ok := doc.Key(k.KeyID())
		if ok && p.ActivatedAt.IsZero() && eligible(doc, p, now, i.cfg.ActivationDelay) == "" {
			waiting = append(waiting, k.KeyID())
		}
	}
	var liveErr error
	var liveKids map[string]bool
	if i.cfg.URL == "" {
		waiting = nil // no token signs, so no key activates for one
	}
	if len(waiting) > 0 && i.cfg.VerifyPublished {
		liveKids, liveErr = i.live.PublishedKeyIDs(ctx)
		if liveErr != nil {
			i.log.Warn("identity: could not read the issuer URL; a key that has never signed waits", "issuer", i.cfg.URL, "err", liveErr)
		}
		waiting = slices.DeleteFunc(waiting, func(kid string) bool { return !liveKids[kid] })
	}
	needsConfirm := slices.ContainsFunc(i.cfg.Keys, func(k *jwt.Signer) bool {
		p, ok := doc.Key(k.KeyID())
		return ok && !i.confirmedLately(p, now)
	})
	if i.cfg.VerifyPublished && (liveKids != nil || liveErr != nil) {
		i.mu.Lock()
		i.liveKids, i.liveErr, i.liveAt = liveKids, liveErr, now
		i.mu.Unlock()
	}
	if len(waiting) > 0 || needsConfirm {
		doc, err = i.store.Update(ctx, func(d *artifacts.PublishedKeys) (bool, error) {
			changed := false
			for j := range d.Keys {
				k := &d.Keys[j]
				if slices.Contains(waiting, k.KID) && k.ActivatedAt.IsZero() && !k.Retired() {
					k.ActivatedAt = now
					changed = true
				}
				if i.holds(k.KID) && i.confirm(k, now) {
					changed = true
				}
			}
			if forget(d, now) {
				changed = true
			}
			return changed, nil
		})
		if err != nil {
			i.mu.Lock()
			i.readErr = err
			i.mu.Unlock()
			return fmt.Errorf("record the signing keys' activation: %w", err)
		}
		for _, kid := range waiting {
			i.log.Info("identity: a signing key is active", "kid", kid)
		}
	}
	i.adopt(doc, now)
	return nil
}

func (i *Issuer) holds(kid string) bool {
	return slices.ContainsFunc(i.cfg.Keys, func(k *jwt.Signer) bool { return k.KeyID() == kid })
}

// take adopts doc as read at now, unless this replica already holds a
// newer one: a read begun before a write this replica made (a retirement)
// may answer after it, and must not undo it or move the sequence back.
func (i *Issuer) take(doc artifacts.PublishedKeys, now time.Time, also func()) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if doc.Sequence < i.doc.Sequence && !i.readAt.IsZero() && !i.fromCopy {
		// An older read: neither adopted nor counted as one.
		return false
	}
	i.fromCopy = false
	i.doc = doc
	for _, k := range i.doc.Keys {
		if k.Retired() {
			i.retiredSeen[k.KID] = true
		}
		if seen, ok := i.compromisedSeen[k.KID]; !k.CompromisedFrom.IsZero() && (!ok || k.CompromisedFrom.Before(seen)) {
			i.compromisedSeen[k.KID] = k.CompromisedFrom
		}
	}
	if now.After(i.readAt) || i.readAt.After(i.now().UTC()) {
		// A read in the future (a clock that stepped back) never holds a
		// newer read back.
		i.readAt = now
	}
	i.readErr = nil
	if also != nil {
		also()
	}
	for _, k := range doc.Keys {
		if !k.Retired() && !held(k, now) && !i.reported[k.KID] {
			i.reported[k.KID] = true
			i.log.Warn("identity: a published signing key is held by no engine that confirmed it lately", "kid", k.KID, "published_by", k.PublishedBy)
		}
	}
	return true
}

func held(k artifacts.PublishedKey, now time.Time) bool {
	return slices.ContainsFunc(k.Holders, func(h artifacts.KeyHolder) bool { return now.Sub(h.ConfirmedAt) <= HolderForget })
}

// eligible answers why p may not sign at now, or "" when it may:
// published and not retired, and published for the activation delay or the
// only unretired key.
func eligible(doc artifacts.PublishedKeys, p artifacts.PublishedKey, now time.Time, delay time.Duration) string {
	if p.Retired() {
		return fmt.Sprintf("retired at %s by %s", p.RetiredAt.Format(time.RFC3339), p.RetiredBy)
	}
	if !p.ActivatedAt.IsZero() || now.Sub(p.PublishedAt) >= delay {
		return ""
	}
	others := slices.ContainsFunc(doc.Keys, func(o artifacts.PublishedKey) bool { return o.KID != p.KID && !o.Retired() })
	if !others {
		return ""
	}
	return fmt.Sprintf("published at %s; another key is unretired, so it signs from %s, after the activation delay",
		p.PublishedAt.Format(time.RFC3339), p.PublishedAt.Add(delay).Format(time.RFC3339))
}

// Signer answers the first configured key that qualifies: in the document,
// not retired, activated, from a copy of the document no older than
// i.cfg.CopyMaxAge. Otherwise ErrKeyNotActive, naming each key's reason.
func (i *Issuer) Signer() (*jwt.Signer, error) {
	if i.cfg.URL == "" {
		return nil, ErrNoIssuer
	}
	now := i.now().UTC()
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.readAt.IsZero() {
		return nil, fmt.Errorf("%w: the published signing keys have not been read yet (%v)", ErrKeyNotActive, i.readErr)
	}
	if age := now.Sub(i.readAt); age < 0 {
		return nil, fmt.Errorf("%w: the clock is %s behind the last read of the published signing keys; nothing signs until it is read again", ErrKeyNotActive, (-age).Round(time.Second))
	} else if age > i.cfg.CopyMaxAge {
		return nil, fmt.Errorf("%w: the published signing keys could not be read for %s, more than %s (%v)", ErrKeyNotActive, age.Round(time.Second), i.cfg.CopyMaxAge, i.readErr)
	}
	var why []string
	for _, k := range i.cfg.Keys {
		reason := i.whyNot(k.KeyID(), now)
		if reason == "" {
			return k, nil
		}
		why = append(why, k.KeyID()+": "+reason)
	}
	return nil, fmt.Errorf("%w: %s", ErrKeyNotActive, strings.Join(why, "; "))
}

// whyNot answers why kid does not sign now, or "". Callers hold i.mu.
func (i *Issuer) whyNot(kid string, now time.Time) string {
	p, ok := i.doc.Key(kid)
	if !ok {
		return "not in the published keys yet"
	}
	if i.retiredSeen[kid] && !p.Retired() {
		return "retired, as this engine read it earlier; the published keys no longer say so, and that is not believed"
	}
	if !i.storedIsHeld(p) {
		i.reportOnce("forged:"+kid, "identity: the published key under a held key's id does not match the key this engine holds; the published keys were written around the engines", "kid", kid)
		return "the published key under this id does not match the key this engine holds"
	}
	if reason := eligible(i.doc, p, now, i.cfg.ActivationDelay); reason != "" {
		return reason
	}
	if !p.ActivatedAt.IsZero() {
		return ""
	}
	if !i.cfg.VerifyPublished {
		return "eligible; it signs once the next read of the published keys records it active"
	}
	if i.liveErr != nil {
		return fmt.Sprintf("not served at %s yet: the issuer URL could not be read (%v)", i.cfg.URL, i.liveErr)
	}
	return fmt.Sprintf("not served at %s yet; export and upload the issuer's documents", i.cfg.URL)
}

// Keys answers every key of the document and every key this replica holds,
// the replica's own first, in its configured order.
func (i *Issuer) Keys() []KeyStatus {
	now := i.now().UTC()
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []KeyStatus
	add := func(kid string, alg jwt.Algorithm) {
		st := KeyStatus{KeyID: kid, Algorithm: alg, HeldHere: i.holds(kid)}
		if p, ok := i.doc.Key(kid); ok {
			st.Published, st.PublishedAt, st.PublishedBy = true, p.PublishedAt, p.PublishedBy
			st.ActivatedAt, st.RetiredAt, st.RetiredBy, st.CompromisedFrom = p.ActivatedAt, p.RetiredAt, p.RetiredBy, p.CompromisedFrom
			for _, h := range p.Holders {
				if now.Sub(h.ConfirmedAt) <= HolderForget {
					st.Holders = append(st.Holders, h)
				}
			}
			st.Unheld = !p.Retired() && len(st.Holders) == 0
		}
		if st.HeldHere {
			st.Why = i.whyNot(kid, now)
			st.Active = st.Why == ""
		} else if st.Published {
			if reason := eligible(i.doc, mustKey(i.doc, kid), now, i.cfg.ActivationDelay); reason != "" {
				st.Why = reason
			} else {
				st.Why = "not listed in this engine's identity.signing.key_files"
			}
		}
		out = append(out, st)
	}
	for _, k := range i.cfg.Keys {
		add(k.KeyID(), k.Algorithm())
	}
	for _, p := range i.doc.Keys {
		if !i.holds(p.KID) {
			add(p.KID, jwt.Algorithm(p.Algorithm))
		}
	}
	return out
}

func mustKey(d artifacts.PublishedKeys, kid string) artifacts.PublishedKey {
	k, _ := d.Key(kid)
	return k
}

// Status says when this replica last read the document and the live
// issuer, and what each answered.
type Status struct {
	ReadAt   time.Time
	ReadErr  error
	LiveAt   time.Time
	LiveKids []string
	LiveErr  error
	Document artifacts.PublishedKeys
	// CopyErr is why the copy on disk could not last be written, nil when
	// it was or there is none.
	CopyErr error
}

// Status answers what this replica last read.
func (i *Issuer) Status() Status {
	i.mu.Lock()
	defer i.mu.Unlock()
	st := Status{ReadAt: i.readAt, ReadErr: i.readErr, LiveAt: i.liveAt, LiveErr: i.liveErr, Document: i.doc, CopyErr: i.copyErr}
	for kid := range i.liveKids {
		st.LiveKids = append(st.LiveKids, kid)
	}
	slices.Sort(st.LiveKids)
	return st
}

// Retire retires kid as of now, by whom; a retired key never signs again
// on any replica, and is no longer served. With compromisedFrom set it
// also marks the key compromised from that instant, which may lie in the
// past: what it signed for an instant before it still verifies, and
// nothing from it on does. A compromise may be added to a key retired
// before, and once set it only ever moves earlier. Retiring a retired key with nothing to
// add answers it as it is.
func (i *Issuer) Retire(ctx context.Context, kid, by string, compromisedFrom time.Time) (artifacts.PublishedKey, error) {
	now := i.now().UTC()
	compromisedFrom = compromisedFrom.UTC()
	doc, err := i.store.Update(ctx, func(d *artifacts.PublishedKeys) (bool, error) {
		for j := range d.Keys {
			k := &d.Keys[j]
			if k.KID != kid {
				continue
			}
			changed := false
			if !k.Retired() {
				k.RetiredAt, k.RetiredBy = now, by
				changed = true
			}
			if !compromisedFrom.IsZero() {
				switch {
				case k.CompromisedFrom.IsZero() || compromisedFrom.Before(k.CompromisedFrom):
					// Earlier only: a later instant would make good again
					// what was declared compromised.
					k.CompromisedFrom = compromisedFrom
					changed = true
				case compromisedFrom.After(k.CompromisedFrom):
					return false, fmt.Errorf("%w: key %s is marked compromised from %s, and that only ever moves earlier", ErrAlreadyCompromised, kid, k.CompromisedFrom.Format(time.RFC3339))
				}
			}
			return changed, nil
		}
		return false, fmt.Errorf("%w: %s", ErrKeyNotFound, kid)
	})
	if err != nil {
		return artifacts.PublishedKey{}, err
	}
	i.adopt(doc, now)
	k, _ := doc.Key(kid)
	return k, nil
}

// ErrAlreadyCompromised is a compromise asked of a key already marked
// compromised from another instant.
var ErrAlreadyCompromised = errors.New("the key is already marked compromised")

// servedKeys are the unretired keys of the last copy of the document.
// storedIsHeld says p's public key is the key this engine holds under its
// id, of its algorithm. Callers hold i.mu.
func (i *Issuer) storedIsHeld(p artifacts.PublishedKey) bool {
	for _, k := range i.cfg.Keys {
		if k.KeyID() == p.KID {
			stored, err := jwt.ParsePublicJWK(p.JWK)
			return err == nil && stored.KeyID() == p.KID && string(stored.Algorithm()) == p.Algorithm && k.Algorithm() == stored.Algorithm()
		}
	}
	return false
}

// reportOnce logs msg once per key for this process. Callers hold i.mu.
func (i *Issuer) reportOnce(key, msg string, args ...any) {
	if !i.reported[key] {
		i.reported[key] = true
		i.log.Error(msg, args...)
	}
}

// Before the first read it answers ErrKeysNotRead. A stored key the jwt
// package will not read is logged and left out, so one bad entry does not
// take every other key off the air.
//
// A key is served only under its own thumbprint and algorithm, and a key
// this engine holds only when the stored public key is the one it holds.
// Callers hold i.mu.
func (i *Issuer) servedKeys() ([]jwt.PublicKey, int64, error) {
	if i.cfg.URL == "" {
		return nil, 0, ErrNoIssuer
	}
	if i.readAt.IsZero() {
		return nil, 0, ErrKeysNotRead
	}
	if i.now().UTC().Sub(i.readAt) > i.cfg.CopyMaxAge {
		return nil, 0, ErrKeysStale
	}
	var out []jwt.PublicKey
	for _, p := range i.doc.Keys {
		if p.Retired() || i.retiredSeen[p.KID] {
			continue
		}
		k, err := jwt.ParsePublicJWK(p.JWK)
		if err == nil && (k.KeyID() != p.KID || string(k.Algorithm()) != p.Algorithm) {
			err = fmt.Errorf("stored under kid %s as %s, it is %s, a %s key", p.KID, p.Algorithm, k.KeyID(), k.Algorithm())
		}
		if err == nil && i.holds(p.KID) && !i.storedIsHeld(p) {
			err = errors.New("it is not the key this engine holds under that id")
		}
		if err != nil {
			i.reportOnce("bad:"+p.KID, "identity: a published signing key is not served", "kid", p.KID, "err", err)
			continue
		}
		out = append(out, k)
	}
	if len(out) == 0 {
		return nil, 0, ErrNoServedKeys
	}
	return out, i.doc.Sequence, nil
}

// JWKS is the key set relying parties are served: the unretired keys.
func (i *Issuer) JWKS() ([]byte, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.served.jwks != nil && i.served.seq == i.doc.Sequence && i.now().UTC().Sub(i.readAt) <= i.cfg.CopyMaxAge {
		return i.served.jwks, nil
	}
	keys, seq, err := i.servedKeys()
	if err != nil {
		return nil, err
	}
	out, err := jwt.MarshalKeySet(keys)
	if err != nil {
		return nil, err
	}
	if i.served.seq != seq {
		i.served = servedCache{seq: seq}
	}
	i.served.jwks = out
	return out, nil
}

// Bundle is the trust domain's SPIFFE bundle: the served keys, the
// document's sequence as its sequence.
func (i *Issuer) Bundle() ([]byte, error) {
	if i.cfg.TrustDomain == "" {
		return nil, ErrNoTrustDomain
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.served.bundle != nil && i.served.seq == i.doc.Sequence && i.now().UTC().Sub(i.readAt) <= i.cfg.CopyMaxAge {
		return i.served.bundle, nil
	}
	keys, seq, err := i.servedKeys()
	if err != nil {
		return nil, err
	}
	out, err := jwt.MarshalSPIFFEBundle(keys, seq, int64(BundleRefreshHint/time.Second))
	if err != nil {
		return nil, err
	}
	if i.served.seq != seq {
		i.served = servedCache{seq: seq}
	}
	i.served.bundle = out
	return out, nil
}

// Claims are the claims a task's token may carry (workload-identity.md §4).
var Claims = []string{"iss", "sub", "aud", "exp", "iat", "nbf", "jti",
	"ork_deployment", "ork_namespace", "ork_namespace_id", "ork_orkflow", "ork_orkflow_id",
	ClaimSubject, ClaimSPIFFEID, "ork_orkflow_namespace", "ork_orkflow_version", "ork_definition_digest",
	"ork_run_id", "ork_root_run_id", "ork_parent_run_id", "ork_task", "ork_item", "ork_attempt",
	"ork_identity", "ork_pool", "ork_orker", "ork_origin", "ork_submitted_by"}

// Discovery is the issuer's OpenID discovery document. It is the same
// wherever it is served or exported: jwks_uri is the issuer's own
// jwks.json.
func (i *Issuer) Discovery() ([]byte, error) {
	if i.cfg.URL == "" {
		return nil, ErrNoIssuer
	}
	doc := struct {
		Issuer        string   `json:"issuer"`
		JWKSURI       string   `json:"jwks_uri"`
		ResponseTypes []string `json:"response_types_supported"`
		SubjectTypes  []string `json:"subject_types_supported"`
		Algs          []string `json:"id_token_signing_alg_values_supported"`
		Claims        []string `json:"claims_supported"`
	}{i.cfg.URL, i.cfg.URL + "/jwks.json", []string{"id_token"}, []string{"public"}, []string{string(i.cfg.Algorithm)}, Claims}
	out, err := json.Marshal(doc, json.Deterministic(true))
	if err != nil {
		return nil, fmt.Errorf("encode the discovery document: %w", err)
	}
	return out, nil
}

// tick is one refresh: a failed read is logged, and a copy past
// i.cfg.CopyMaxAge after it is logged and reported to the observer, since
// the engine then signs nothing and verifies nothing.
func (i *Issuer) tick(ctx context.Context) {
	if err := i.Refresh(ctx); err != nil && ctx.Err() == nil {
		i.log.Error("identity: signing keys", "err", err)
	}
	if ctx.Err() != nil {
		return
	}
	now := i.now().UTC()
	i.mu.Lock()
	stale, readAt := !i.usable(now), i.readAt
	i.mu.Unlock()
	if !stale {
		return
	}
	i.log.Error("identity: the copy of the published signing keys is older than identity.keys_copy_max_age; this engine signs nothing and verifies nothing until the store answers",
		"read_at", readAt, "max_age", i.cfg.CopyMaxAge)
	if i.cfg.Observer != nil {
		i.cfg.Observer.KeysCopyStale()
	}
}

// CopyObserver is told when an engine's copy of the published keys is too
// old to decide from (ork.identity.keys_copy_stale). It must not block.
type CopyObserver interface {
	KeysCopyStale()
}

// Run publishes this replica's keys and reads the document again each
// RefreshInterval until ctx ends. A failed read is logged; signing goes on
// from the last copy within i.cfg.CopyMaxAge.
func (i *Issuer) Run(ctx context.Context) {
	t := time.NewTicker(RefreshInterval)
	defer t.Stop()
	for {
		i.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// The identity.* settings' defaults (docs/DEPLOY.md §1).
const (
	DefaultActivationDelay = 24 * time.Hour
	DefaultAlgorithm       = jwt.RS256
	DefaultServeDiscovery  = true
	DefaultVerifyPublished = true
	DefaultSubjectFormat   = FormatOrk
)
