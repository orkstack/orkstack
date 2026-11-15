package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AccessAcksPrefix holds one create-only record per acknowledged revision
// written around the API, {revision:012}-{sha256}.json. A record decides
// whether a banner shows, never access: no decision reads one.
const AccessAcksPrefix = IdentityPrefix + "/access-acks/"

// AccessAckFormatVersion is the only acknowledgement format this build
// writes.
const AccessAckFormatVersion = 1

// MaxAccessAcksListed bounds one ListAcknowledged. There is one record per
// revision written around the API and acknowledged, so a deployment never
// comes near it; past it the listing says it is truncated.
const MaxAccessAcksListed = 10000

// AccessAck is one acknowledgement: who acknowledged which revision, and
// when.
type AccessAck struct {
	Revision   int64
	SHA256     string
	At         time.Time
	By         string
	ByVerified bool
}

// AccessAckRef is one acknowledgement as a listing names it.
type AccessAckRef struct {
	Revision int64
	SHA256   string
	Key      string
}

type accessAckRecord struct {
	FormatVersion int    `json:"format_version"`
	Revision      int64  `json:"revision"`
	SHA256        string `json:"sha256"`
	At            string `json:"acknowledged_at"`
	By            string `json:"acknowledged_by,omitzero"`
	ByVerified    bool   `json:"acknowledged_by_verified,omitzero"`
}

// AccessAckKey is the key of one acknowledgement.
func AccessAckKey(revision int64, sha string) string {
	return fmt.Sprintf("%s%012d-%s.json", AccessAcksPrefix, revision, sha)
}

// ParseAccessAckKey reads a listed key back: it is an acknowledgement only
// when it is exactly the key AccessAckKey writes for a revision of at least
// one and a lowercase SHA-256.
func ParseAccessAckKey(key string) (AccessAckRef, bool) {
	rest, ok := strings.CutPrefix(key, AccessAcksPrefix)
	if !ok {
		return AccessAckRef{}, false
	}
	rest, ok = strings.CutSuffix(rest, ".json")
	if !ok || len(rest) != 12+1+64 || rest[12] != '-' {
		return AccessAckRef{}, false
	}
	digits, digest := rest[:12], rest[13:]
	for _, c := range digits {
		if c < '0' || c > '9' {
			return AccessAckRef{}, false
		}
	}
	revision, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || revision < 1 || !shaPattern.MatchString(digest) {
		return AccessAckRef{}, false
	}
	return AccessAckRef{Revision: revision, SHA256: digest, Key: key}, true
}

// Acknowledge records ack create-only. A revision already acknowledged
// keeps its first record: created is then false and err nil.
func (s *AccessStore) Acknowledge(ctx context.Context, ack AccessAck) (created bool, err error) {
	if ack.Revision < 1 || !shaPattern.MatchString(ack.SHA256) {
		return false, fmt.Errorf("artifacts: acknowledge access revision: %w: revision %d, digest %q", ErrAccessInvalid, ack.Revision, ack.SHA256)
	}
	body, err := json.Marshal(accessAckRecord{FormatVersion: AccessAckFormatVersion, Revision: ack.Revision, SHA256: ack.SHA256,
		At: ack.At.UTC().Format(time.RFC3339Nano), By: ack.By, ByVerified: ack.ByVerified})
	if err != nil {
		return false, fmt.Errorf("artifacts: acknowledge access revision %d: %w", ack.Revision, err)
	}
	key := AccessAckKey(ack.Revision, ack.SHA256)
	if _, err := s.blobs.put(ctx, key, body, PutConditions{IfAbsent: true}); err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			return false, nil
		}
		return false, fmt.Errorf("artifacts: acknowledge access revision %q: %w", s.blobs.location(key), err)
	}
	return true, nil
}

// ListAcknowledged lists the acknowledgements in revision order, at most
// MaxAccessAcksListed of them; truncated says there are more. A key that is
// not an acknowledgement's is skipped.
func (s *AccessStore) ListAcknowledged(ctx context.Context) (acks []AccessAckRef, truncated bool, err error) {
	keys, err := s.blobs.list(ctx, AccessAcksPrefix, "", MaxAccessAcksListed+1)
	if err != nil {
		return nil, false, fmt.Errorf("artifacts: list access acknowledgements %q: %w", s.blobs.location(AccessAcksPrefix), err)
	}
	if len(keys) > MaxAccessAcksListed {
		keys, truncated = keys[:MaxAccessAcksListed], true
	}
	acks = make([]AccessAckRef, 0, len(keys))
	for _, key := range keys {
		if ref, ok := ParseAccessAckKey(key); ok {
			acks = append(acks, ref)
		}
	}
	return acks, truncated, nil
}

// IsAcknowledged reports whether revision, with digest sha, has a record.
func (s *AccessStore) IsAcknowledged(ctx context.Context, revision int64, sha string) (bool, error) {
	if revision < 1 || !shaPattern.MatchString(sha) {
		return false, nil
	}
	return s.blobs.exists(ctx, AccessAckKey(revision, sha))
}
