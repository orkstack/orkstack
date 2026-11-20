package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
	"google.golang.org/protobuf/encoding/protojson"
)

// A resume plan's record (E85 §11, §30.3): the plan ResumeRun confirmed,
// written create-only before the reset, and named in the reset's reason by
// the SHA-256 of its stored bytes. It lives under the run's snapshot prefix,
// so it is reclaimed with the run's saved state, beside but apart from the
// identity records' resume scope (docs/STORAGE.md).
const (
	// ResumePlanFormat is the record format this build writes and reads.
	ResumePlanFormat = 1
	// maxResumePlanBytes bounds a stored plan record: its block is at most
	// 64 KiB (RESUME_TOO_LARGE), and the rest is small.
	maxResumePlanBytes = 256 << 10
)

// ErrResumePlanDiffers: another plan record is at this plan's key.
var ErrResumePlanDiffers = errors.New("another resume plan record is at this plan's key")

// ResumePlanKey renders the key of a resume plan's record:
// metadata/snapshots/{namespace}/{run-id}/resume-plans/{scope:020}.{n:03}.json.
func ResumePlanKey(namespace, runID string, scope uint64, n int) (string, error) {
	if scope == 0 || n < 0 || n > maxResumePlan {
		return "", fmt.Errorf("artifacts: resume plan key: scope %d plan %d: %w", scope, n, ErrInvalidRecordKey)
	}
	prefix, err := RunSnapshotPrefix(namespace, runID)
	if err != nil {
		return "", errors.Join(ErrInvalidRecordKey, err)
	}
	return fmt.Sprintf("%sresume-plans/%020d.%03d.json", prefix, scope, n), nil
}

// MarshalResumePlan renders the exact bytes a plan record is stored as,
// as MarshalCommit renders a commit. The digest a reset names is of the
// bytes stored, never of a re-marshalling.
func MarshalResumePlan(r *orkstackv1.ResumePlanRecord) ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("artifacts: encode resume plan: nil record: %w", ErrInvalidRecordKey)
	}
	raw, err := protojson.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode resume plan of run %q: %w", r.GetRunId(), err)
	}
	var out bytes.Buffer
	if err := stablejson.Indent(&out, raw, "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode resume plan of run %q: %w", r.GetRunId(), err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// ResumePlanDigest is the lowercase hex SHA-256 of stored bytes.
func ResumePlanDigest(stored []byte) string {
	sum := sha256.Sum256(stored)
	return hex.EncodeToString(sum[:])
}

// DecodeResumePlan reads stored bytes as the record of (namespace, runID,
// scope, n). Bytes that do not decode, or a record naming another plan, are
// RecordInvalid; a format this build does not know is RecordUnsupported.
func DecodeResumePlan(raw []byte, namespace, runID string, scope uint64, n int) RecordRead[*orkstackv1.ResumePlanRecord] {
	r := &orkstackv1.ResumePlanRecord{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, r); err != nil {
		return invalidRead[*orkstackv1.ResumePlanRecord]("resume plan", "does not decode: %v", err)
	}
	if r.GetFormat() != ResumePlanFormat {
		return RecordRead[*orkstackv1.ResumePlanRecord]{Answer: RecordUnsupported, Object: "resume plan", Version: int64(r.GetFormat())}
	}
	switch {
	case ResolveNamespace(r.GetNamespace()) != ResolveNamespace(namespace):
		return invalidRead[*orkstackv1.ResumePlanRecord]("resume plan", "names namespace %q, not %q", r.GetNamespace(), namespace)
	case r.GetRunId() != runID:
		return invalidRead[*orkstackv1.ResumePlanRecord]("resume plan", "names run %q, not %q", r.GetRunId(), runID)
	case r.GetScope() != scope || int(r.GetN()) != n:
		return invalidRead[*orkstackv1.ResumePlanRecord]("resume plan", "names plan %d.%03d, not %d.%03d", r.GetScope(), r.GetN(), scope, n)
	case r.GetBlock() == nil:
		return invalidRead[*orkstackv1.ResumePlanRecord]("resume plan", "holds no block")
	}
	return RecordRead[*orkstackv1.ResumePlanRecord]{Answer: RecordFound, Value: r}
}

// WriteResumePlan writes r create-only and answers the digest of its stored
// bytes. The same bytes there already succeed; other bytes are
// ErrResumePlanDiffers. A store that does not answer is
// *RecordStoreUnanswered.
func (s *RecordStore) WriteResumePlan(ctx context.Context, r *orkstackv1.ResumePlanRecord) (string, error) {
	key, err := ResumePlanKey(r.GetNamespace(), r.GetRunId(), r.GetScope(), int(r.GetN()))
	if err != nil {
		return "", err
	}
	body, err := MarshalResumePlan(r)
	if err != nil {
		return "", err
	}
	if len(body) > maxResumePlanBytes {
		return "", fmt.Errorf("artifacts: resume plan of run %q is %d bytes, over %d: %w",
			r.GetRunId(), len(body), maxResumePlanBytes, ErrInvalidRecordKey)
	}
	if err := s.createOrSame(ctx, key, body, maxResumePlanBytes); err != nil {
		if errors.Is(err, errBlobExists) {
			return "", fmt.Errorf("artifacts: %s: %w", key, ErrResumePlanDiffers)
		}
		return "", err
	}
	return ResumePlanDigest(body), nil
}

// ReadResumePlan reads the record of plan (scope, n) of a run, with the
// digest of the bytes it read. A store that does not answer is
// *RecordStoreUnanswered.
func (s *RecordStore) ReadResumePlan(ctx context.Context, namespace, runID string, scope uint64, n int) (RecordRead[*orkstackv1.ResumePlanRecord], string, error) {
	key, err := ResumePlanKey(namespace, runID, scope, n)
	if err != nil {
		return RecordRead[*orkstackv1.ResumePlanRecord]{}, "", err
	}
	raw, found, err := s.getIn(ctx, key, maxResumePlanBytes+1)
	if err != nil {
		return RecordRead[*orkstackv1.ResumePlanRecord]{}, "", err
	}
	if !found {
		return RecordRead[*orkstackv1.ResumePlanRecord]{Answer: RecordNone}, "", nil
	}
	if len(raw) > maxResumePlanBytes {
		return invalidRead[*orkstackv1.ResumePlanRecord]("resume plan", "is over %d bytes", maxResumePlanBytes), "", nil
	}
	return DecodeResumePlan(raw, namespace, runID, scope, n), ResumePlanDigest(raw), nil
}
