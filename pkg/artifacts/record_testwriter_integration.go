//go:build integration

package artifacts

import (
	"context"
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// RecordTestWriter lays down identity records, good and corrupt, with the
// real keys and the real encoding, so another design's fixtures need no
// door (docs/TASK_IDENTITY.md). It exists only in binaries built with the
// integration tag; TestRecordTestWriter_OnlyTestsUseIt holds that.
type RecordTestWriter struct {
	b blobs
}

// NewRecordTestWriter opens the records of the store cfg names.
func NewRecordTestWriter(ctx context.Context, cfg Config) (*RecordTestWriter, error) {
	b, err := openBlobs(ctx, cfg, "identity record test writer")
	if err != nil {
		return nil, err
	}
	return &RecordTestWriter{b: b}, nil
}

// EnableRecordFormat2 makes this binary's reader read records and details
// of format 2, a test-only format: the same message with schema_version 2,
// which a "v2" engine in a rollout test writes and reads. The function it
// answers turns format 2 off again.
func EnableRecordFormat2() (restore func()) {
	before := recordFormats.Swap(2)
	return func() { recordFormats.Store(before) }
}

// Values stores values by their digest and answers the reference a record
// names them by.
func (w *RecordTestWriter) Values(ctx context.Context, values []byte) (*orkstackv1.ArtifactRef, error) {
	sum := hexDigest(values)
	key := "artifacts/identity-test-values/" + sum
	if err := w.b.put(ctx, key, "application/json", values); err != nil {
		return nil, err
	}
	return &orkstackv1.ArtifactRef{Uri: w.b.locate(key), Sha256: sum, SizeBytes: int64(len(values))}, nil
}

// Detail stores a resolution's detail under its digest, which it answers.
func (w *RecordTestWriter) Detail(ctx context.Context, d *orkstackv1.ResolutionDetail) (string, error) {
	raw, err := MarshalResolutionDetail(d)
	if err != nil {
		return "", err
	}
	sum := hexDigest(raw)
	key, err := ResolutionDetailKey(d.GetNamespace(), d.GetRunId(), d.GetTaskId(), sum)
	if err != nil {
		return "", err
	}
	return sum, w.b.create(ctx, key, "application/json", raw)
}

// Record creates rec at key and answers the SHA-256 of its stored bytes.
func (w *RecordTestWriter) Record(ctx context.Context, key string, rec *orkstackv1.RecordedValues) (string, error) {
	raw, err := MarshalRecordedValues(rec)
	if err != nil {
		return "", err
	}
	return hexDigest(raw), w.b.create(ctx, key, "application/json", raw)
}

// Raw creates body at key as it is: a corrupt, oversized or foreign
// object a reader must refuse. Only identity record keys are written.
func (w *RecordTestWriter) Raw(ctx context.Context, key string, body []byte) error {
	if !isRecordKey(key) || strings.Contains("/"+key+"/", "/../") || strings.Contains("/"+key+"/", "/./") {
		return fmt.Errorf("artifacts: test writer: %q is no identity record key: %w", key, ErrInvalidRecordKey)
	}
	return w.b.create(ctx, key, "application/json", body)
}

// Resume writes a resume's record and link as WriteResume does.
func (w *RecordTestWriter) Resume(ctx context.Context, r ResumeRecord) error {
	return newRecordStore(w.b).WriteResume(ctx, r)
}

// isRecordKey says key is one of the identity records' keys.
func isRecordKey(key string) bool {
	return strings.HasPrefix(key, AttestedPrefix+"/") ||
		(strings.HasPrefix(key, SnapshotPrefix+"/") && strings.Contains(key, "/identity/"))
}

// Admission writes a run's admission record, as the submit path will, and
// answers its digest.
func (w *RecordTestWriter) Admission(ctx context.Context, a Admission) (string, error) {
	sha, _, err := newRecordStore(w.b).WriteAdmission(ctx, a)
	return sha, err
}
