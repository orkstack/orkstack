package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// A message's claim and its started marker (docs/STORAGE.md, "Event
// sources").
//
// The claim binds one message of one source to a run id, and to nothing
// else: it is created once, after admission passed and before the run is
// started, and is never rewritten or deleted by OrkStack, so every delivery
// of the message starts, or finds, that one run. The store arbitrates it, as
// it does a commit: a record, never a lock, and nothing is decided from its
// age. The marker beside it says the run the claim names exists. Its key
// holds the run id, so a marker left by an earlier claim of the message is
// another object and is never read.
const (
	// MessageClaimPrefix holds claims and their markers, expired together by
	// a bucket lifecycle rule and by nothing else.
	MessageClaimPrefix = "metadata/message-claims"
	// MessageClaimProbePrefix holds the object a probe of the claims writes
	// and removes.
	MessageClaimProbePrefix = "metadata/probes/event-sources"
	// claimSchema is the one version of a claim and a marker this build
	// writes and reads.
	claimSchema = 1
	// maxClaimBytes bounds a claim and a marker of this build's version.
	maxClaimBytes = 4 << 10
	// maxClaimRead is how much of one is read: room for a newer build's to
	// say it is newer.
	maxClaimRead = 64 << 10
	// claimScopeSep joins a scope's parts; no slug holds it.
	claimScopeSep = "\x1f"
	// maxClaimReads bounds how often Create looks again for a claim that
	// held its key and was gone when it was read.
	maxClaimReads = 3
)

var (
	// ErrClaimVersion is a claim, or its marker, of a version this build
	// does not know: a newer engine wrote it, and one that knows it reads it.
	ErrClaimVersion = errors.New("artifacts: message claim: written in a version this build does not know")
	// ErrClaimCorrupt is a complete claim, or marker, of a version this build
	// knows that does not decode or does not belong where it is: it never
	// will, whoever reads it.
	ErrClaimCorrupt = errors.New("artifacts: message claim: does not decode")
	// ErrClaimIncomplete is one that is not a whole document, so nothing can
	// be told of its version: it is neither read nor judged. No store the
	// claims are kept in writes half an object, so another read does not
	// change it and a person must.
	ErrClaimIncomplete = errors.New("artifacts: message claim: not a whole document")
)

// runIDKey is the form a run id has where it is part of a key. Nothing else
// is read from it.
var runIDKey = regexp.MustCompile(`^[a-z][a-z0-9-]{0,127}$`)

// ClaimScope is what a message's identity is unique in: the namespace, the
// source's name, and the Orkflow the source binds. All three are the
// engine's own, from its registry, never a container's word.
type ClaimScope struct {
	Namespace string
	Source    string
	Orkflow   string
}

// MessageClaim is one message's claim.
type MessageClaim struct {
	RunID     string
	ClaimedAt time.Time
	// Nonce is 128 random bits drawn by Create and never rewritten. Nothing
	// is decided from it.
	Nonce string
	// Started is when the claim's run was recorded as started; zero until
	// the marker exists, and while it cannot be read.
	Started time.Time
	// MarkerUnreadable is a marker that is there and cannot be read, for the
	// caller to report. A marker is a shortcut past asking whether the run
	// exists, so without one the caller asks.
	MarkerUnreadable bool
}

type claimRecord struct {
	SchemaVersion int       `json:"schemaVersion"`
	Identity      string    `json:"identity"`
	RunID         string    `json:"runId"`
	ClaimedAt     time.Time `json:"claimedAt"`
	Nonce         string    `json:"nonce"`
}

type markerRecord struct {
	SchemaVersion int       `json:"schemaVersion"`
	RunID         string    `json:"runId"`
	StartedAt     time.Time `json:"startedAt"`
}

// MessageClaimStore keeps the claims and their markers. The engine's alone:
// an Orker's credential reaches none of it.
type MessageClaimStore struct {
	blobBase
	now func() time.Time
}

// NewMessageClaimStore opens the claims on the deployment's artifact backend.
func NewMessageClaimStore(ctx context.Context, cfg Config) (*MessageClaimStore, error) {
	b, err := openBlobs(ctx, cfg, "message claim store", MessageClaimPrefix, MessageClaimProbePrefix)
	if err != nil {
		return nil, err
	}
	return &MessageClaimStore{blobBase: blobBase{b}, now: time.Now}, nil
}

// NewLocalMessageClaimStore opens (creating if needed) the claims under dir.
func NewLocalMessageClaimStore(dir string) (*MessageClaimStore, error) {
	b, err := openLocalBlobs("message claim store", dir, MessageClaimPrefix, MessageClaimProbePrefix)
	if err != nil {
		return nil, err
	}
	return &MessageClaimStore{blobBase: blobBase{b}, now: time.Now}, nil
}

// Get reads the message's claim and its run's marker. A claim that is not
// there is found false; one that cannot be read is an error, typed where
// what to do differs (ErrClaimVersion, ErrClaimCorrupt).
func (s *MessageClaimStore) Get(ctx context.Context, scope ClaimScope, identity string) (claim MessageClaim, found bool, err error) {
	key, err := messageClaimKey(scope, identity)
	if err != nil {
		return MessageClaim{}, false, err
	}
	raw, _, err := s.b.get(ctx, key, maxClaimRead)
	if errors.Is(err, errBlobNotFound) {
		return MessageClaim{}, false, nil
	}
	if err != nil {
		return MessageClaim{}, false, fmt.Errorf("artifacts: read message claim %s: %w", s.b.uri(key), err)
	}
	rec, err := decodeClaim(raw)
	if err == nil && rec.Identity != identity {
		err = fmt.Errorf("%w: it is another message's", ErrClaimCorrupt)
	}
	if err != nil {
		return MessageClaim{}, false, fmt.Errorf("%w (%s)", err, s.b.uri(key))
	}
	claim = MessageClaim{RunID: rec.RunID, ClaimedAt: rec.ClaimedAt, Nonce: rec.Nonce}
	claim.Started, err = s.started(ctx, scope, identity, rec.RunID)
	if errors.Is(err, ErrClaimCorrupt) || errors.Is(err, ErrClaimIncomplete) {
		claim.MarkerUnreadable, err = true, nil
	}
	if err != nil {
		return MessageClaim{}, false, err
	}
	return claim, true, nil
}

// started reads the marker of runID, zero when there is none.
func (s *MessageClaimStore) started(ctx context.Context, scope ClaimScope, identity, runID string) (time.Time, error) {
	key, err := startedMarkerKey(scope, identity, runID)
	if err != nil {
		return time.Time{}, err
	}
	raw, _, err := s.b.get(ctx, key, maxClaimRead)
	if errors.Is(err, errBlobNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("artifacts: read started marker %s: %w", s.b.uri(key), err)
	}
	rec, err := decodeMarker(raw)
	if err == nil && rec.RunID != runID {
		err = fmt.Errorf("%w: the marker is another run's", ErrClaimCorrupt)
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("%w (%s)", err, s.b.uri(key))
	}
	return rec.StartedAt, nil
}

// Create binds the message to runID unless it is already bound, and returns
// the claim that holds: its own, or the earlier one with its marker read. The
// run to start is the RunID it returns, whatever it was given. A
// write whose answer was lost and whose object cannot be read back is an
// error: whether it holds is not known.
func (s *MessageClaimStore) Create(ctx context.Context, scope ClaimScope, identity, runID string) (MessageClaim, error) {
	key, err := messageClaimKey(scope, identity)
	if err != nil {
		return MessageClaim{}, err
	}
	if !runIDKey.MatchString(runID) {
		return MessageClaim{}, fmt.Errorf("artifacts: message claim: %q is not a run id", runID)
	}
	var nonce [16]byte
	_, _ = rand.Read(nonce[:])
	rec := claimRecord{SchemaVersion: claimSchema, Identity: identity, RunID: runID, ClaimedAt: s.now().UTC(), Nonce: hex.EncodeToString(nonce[:])}
	// The nonce makes these bytes this call's own, so a create whose answer
	// was lost is confirmed as this claim and no other (create).
	body, err := stablejson.Marshal(rec)
	if err != nil {
		return MessageClaim{}, fmt.Errorf("artifacts: encode message claim: %w", err)
	}
	for range maxClaimReads {
		err := s.b.create(ctx, key, "application/json", body)
		if err == nil {
			return MessageClaim{RunID: rec.RunID, ClaimedAt: rec.ClaimedAt, Nonce: rec.Nonce}, nil
		}
		if !errors.Is(err, errBlobExists) {
			return MessageClaim{}, fmt.Errorf("artifacts: create message claim %s: %w", s.b.uri(key), err)
		}
		holder, found, err := s.Get(ctx, scope, identity)
		if err != nil || found {
			return holder, err
		}
		// It expired between the two: the key is free again.
	}
	return MessageClaim{}, fmt.Errorf("artifacts: create message claim %s: something holds the key and nothing is read there", s.b.uri(key))
}

// MarkStarted records that the run the claim names exists, as of at. A
// marker already there is success and stays as it is.
func (s *MessageClaimStore) MarkStarted(ctx context.Context, scope ClaimScope, identity, runID string, at time.Time) error {
	key, err := startedMarkerKey(scope, identity, runID)
	if err != nil {
		return err
	}
	if at.IsZero() {
		return errors.New("artifacts: started marker: the instant the run started is required")
	}
	body, err := stablejson.Marshal(markerRecord{SchemaVersion: claimSchema, RunID: runID, StartedAt: at.UTC()})
	if err != nil {
		return fmt.Errorf("artifacts: encode started marker: %w", err)
	}
	if err := s.b.create(ctx, key, "application/json", body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: write started marker %s: %w", s.b.uri(key), err)
	}
	return nil
}

// ProbeWrite writes a few bytes the way Create writes a claim, at a key of its
// own outside the claims, and removes them: whether a claim could be written
// now. A process killed between the two leaves them.
func (s *MessageClaimStore) ProbeWrite(ctx context.Context) error {
	key := MessageClaimProbePrefix + "/" + strings.ToLower(rand.Text())
	if err := s.b.create(ctx, key, "text/plain; charset=utf-8", []byte("message claim probe\n")); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: probe message claims at %s: %w", s.b.uri(key), err)
	}
	refused, err := s.b.removeKeys(ctx, []string{key})
	if err == nil {
		err = errors.Join(slices.Collect(maps.Values(refused))...)
	}
	if err != nil {
		return fmt.Errorf("artifacts: remove the message claims' probe %s: %w", s.b.uri(key), err)
	}
	return nil
}

// messageClaimKey is metadata/message-claims/{sha256(scope)}/{sha256(identity)}.json.
func messageClaimKey(scope ClaimScope, identity string) (string, error) {
	stem, err := claimStem(scope, identity)
	if err != nil {
		return "", err
	}
	return stem + ".json", nil
}

// startedMarkerKey is the claim's key with the run id in it:
// …/{sha256(identity)}.{run-id}.started.json.
func startedMarkerKey(scope ClaimScope, identity, runID string) (string, error) {
	stem, err := claimStem(scope, identity)
	if err != nil {
		return "", err
	}
	if !runIDKey.MatchString(runID) {
		return "", fmt.Errorf("artifacts: started marker: %q is not a run id", runID)
	}
	return stem + "." + runID + ".started.json", nil
}

func claimStem(scope ClaimScope, identity string) (string, error) {
	for _, part := range []string{scope.Namespace, scope.Source, scope.Orkflow} {
		if !eventSourceSlug.MatchString(part) {
			return "", fmt.Errorf("artifacts: message claim: %+v: a namespace, a source and an Orkflow are slugs", scope)
		}
	}
	if identity == "" || len(identity) > eventsources.MaxIdentityBytes {
		return "", fmt.Errorf("artifacts: message claim: a message identity is 1 to %d bytes", eventsources.MaxIdentityBytes)
	}
	return MessageClaimPrefix + "/" + sha256Hex(scope.Namespace+claimScopeSep+scope.Source+claimScopeSep+scope.Orkflow) + "/" + sha256Hex(identity), nil
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func decodeClaim(raw []byte) (claimRecord, error) {
	var rec claimRecord
	if err := decodeClaimDocument(raw, &rec); err != nil {
		return claimRecord{}, err
	}
	at, ok := storedInstant(rec.ClaimedAt)
	if !ok || rec.Identity == "" || len(rec.Identity) > eventsources.MaxIdentityBytes || !runIDKey.MatchString(rec.RunID) || !nonceHex.MatchString(rec.Nonce) {
		return claimRecord{}, fmt.Errorf("%w: a field is missing or out of its form", ErrClaimCorrupt)
	}
	rec.ClaimedAt = at
	return rec, nil
}

func decodeMarker(raw []byte) (markerRecord, error) {
	var rec markerRecord
	if err := decodeClaimDocument(raw, &rec); err != nil {
		return markerRecord{}, err
	}
	at, ok := storedInstant(rec.StartedAt)
	if !ok || !runIDKey.MatchString(rec.RunID) {
		return markerRecord{}, fmt.Errorf("%w: a field is missing or out of its form", ErrClaimCorrupt)
	}
	rec.StartedAt = at
	return rec, nil
}

// decodeClaimDocument reads a claim or a marker strictly, and says which of
// three things a document that does not read is: not whole, of a version this
// build does not know, or a known version's that does not decode. The version
// is read first, so a newer build's document is never judged by this one's
// fields.
func decodeClaimDocument(raw []byte, into any) error {
	// Read loosely here: a name written twice or a broken character is a
	// whole document's, and the strict read below refuses it.
	loose := []json.Options{jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true)}
	if !jsontext.Value(raw).IsValid(loose...) {
		return ErrClaimIncomplete
	}
	var head struct {
		SchemaVersion int64 `json:"schemaVersion"`
	}
	if err := json.Unmarshal(raw, &head, loose...); err != nil || head.SchemaVersion < claimSchema {
		return fmt.Errorf("%w: it names no version", ErrClaimCorrupt)
	}
	if head.SchemaVersion > claimSchema {
		return fmt.Errorf("%w (%d)", ErrClaimVersion, head.SchemaVersion)
	}
	if len(raw) > maxClaimBytes {
		return fmt.Errorf("%w: it is over %d bytes", ErrClaimCorrupt, maxClaimBytes)
	}
	if err := json.Unmarshal(raw, into, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("%w: %w", ErrClaimCorrupt, err)
	}
	return nil
}

// storedInstant is at in UTC, false when it is none or cannot be written
// again: an offset can put a year JSON has no form for.
func storedInstant(at time.Time) (time.Time, bool) {
	utc := at.UTC()
	return utc, !at.IsZero() && utc.Year() >= 1 && utc.Year() <= 9999
}
