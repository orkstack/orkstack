package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// ModeRecordKey holds the highest authentication mode the deployment runs.
// It is a plain object that needs no key to read, because the engine it
// must stop is the one started from an old or mistyped configuration
// (docs/AUTHENTICATION.md, "The mode record").
const ModeRecordKey = IdentityPrefix + "/mode.json"

// ModeRecordFormat is the only mode record format this build reads or
// writes.
const ModeRecordFormat = 1

// The authentication modes, lowest first.
const (
	AuthModeAsserted = "asserted"
	AuthModeObserve  = "observe"
	AuthModeEnforce  = "enforce"
)

// AuthModeRank orders the modes; 0 for one this build does not know.
func AuthModeRank(mode string) int {
	switch mode {
	case AuthModeAsserted:
		return 1
	case AuthModeObserve:
		return 2
	case AuthModeEnforce:
		return 3
	}
	return 0
}

// ErrModeRecordUnreadable is a mode record that does not decode, or names a
// mode or a format this build does not know. It is never read as "no
// record": an engine refuses to start over it.
var ErrModeRecordUnreadable = errors.New("mode record unreadable")

// maxModeRecordBytes bounds the record; it is a few hundred bytes.
const maxModeRecordBytes = 64 << 10

// ModeRecord is the mode record.
type ModeRecord struct {
	FormatVersion int    `json:"format_version"`
	Mode          string `json:"mode"`
	// RaisedAt is when the deployment was first raised to Mode.
	RaisedAt time.Time `json:"raised_at"`
	// LeftEnforceAt is when the deployment last left enforce; zero while it
	// enforces, or when it never did. Lowering to asserted waits one
	// idempotency key's life from it.
	LeftEnforceAt time.Time `json:"left_enforce_at,omitzero"`
	// UpdatedAt and UpdatedBy say who wrote the record last: an engine's
	// boot ("engine <host>"), LowerAuthMode's principal, or
	// `ork --set-mode-record`.
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy string    `json:"updated_by,omitzero"`
	// Reason is the reason a lowering gave.
	Reason string `json:"reason,omitzero"`
}

// EncodeModeRecord renders the record's bytes.
func EncodeModeRecord(r ModeRecord) ([]byte, error) {
	r.FormatVersion = ModeRecordFormat
	if AuthModeRank(r.Mode) == 0 {
		return nil, fmt.Errorf("artifacts: encode mode record: mode %q is not asserted, observe or enforce", r.Mode)
	}
	body, err := stablejson.MarshalIndent(r, "  ")
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode mode record: %w", err)
	}
	return append(body, '\n'), nil
}

// DecodeModeRecord reads the record's bytes. Anything it does not know is
// ErrModeRecordUnreadable, never "no record".
func DecodeModeRecord(body []byte) (ModeRecord, error) {
	if len(body) > maxModeRecordBytes {
		return ModeRecord{}, fmt.Errorf("%w: %d bytes", ErrModeRecordUnreadable, len(body))
	}
	var r ModeRecord
	if err := json.Unmarshal(body, &r, json.RejectUnknownMembers(true)); err != nil {
		return ModeRecord{}, fmt.Errorf("%w: %w", ErrModeRecordUnreadable, err)
	}
	if r.FormatVersion != ModeRecordFormat {
		return ModeRecord{}, fmt.Errorf("%w: format_version %d is not one this release reads (%d)", ErrModeRecordUnreadable, r.FormatVersion, ModeRecordFormat)
	}
	if AuthModeRank(r.Mode) == 0 {
		return ModeRecord{}, fmt.Errorf("%w: mode %q is not one this release knows", ErrModeRecordUnreadable, r.Mode)
	}
	return r, nil
}

// ModeStore reads and writes the mode record by compare-and-swap.
type ModeStore struct {
	blobs blobCAS
}

// NewS3ModeStore opens the mode record in the deployment's bucket.
func NewS3ModeStore(ctx context.Context, cfg Config) (*ModeStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &ModeStore{blobs: &s3CAS{client: client, bucket: bucket}}, nil
}

// NewModeStoreOn is a mode store over a MemoryCAS, for tests.
func NewModeStoreOn(m *MemoryCAS) *ModeStore { return &ModeStore{blobs: m} }

// Location is where the record lives.
func (s *ModeStore) Location() string { return s.blobs.location(ModeRecordKey) }

// Get reads the record and its etag. No record is ErrNotFound; one that
// does not decode is ErrModeRecordUnreadable, with the etag of the bytes
// read, so a forced overwrite can still be conditional.
func (s *ModeStore) Get(ctx context.Context) (ModeRecord, string, error) {
	body, etag, _, err := s.blobs.get(ctx, ModeRecordKey, "")
	if err != nil {
		return ModeRecord{}, "", fmt.Errorf("artifacts: get mode record %q: %w", s.Location(), err)
	}
	r, err := DecodeModeRecord(body)
	if err != nil {
		return ModeRecord{}, etag, fmt.Errorf("artifacts: get mode record %q: %w", s.Location(), err)
	}
	return r, etag, nil
}

// Put writes the record under cond: IfAbsent for the first, IfMatch the
// etag a read returned otherwise. A refused condition is
// ErrPreconditionFailed: the caller reads again.
func (s *ModeStore) Put(ctx context.Context, r ModeRecord, cond PutConditions) (string, error) {
	body, err := EncodeModeRecord(r)
	if err != nil {
		return "", err
	}
	etag, err := s.blobs.put(ctx, ModeRecordKey, body, cond)
	if err != nil {
		return "", fmt.Errorf("artifacts: put mode record %q: %w", s.Location(), err)
	}
	return etag, nil
}
