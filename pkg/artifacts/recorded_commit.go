package artifacts

import (
	"context"
	"errors"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ErrCommitTooLarge is a commit over CommitMaxBytes, the commit's own bound.
var ErrCommitTooLarge = errors.New("commit is over its bound")

// RecordedCommit is the answer to the engine's write of an attempt's
// result: the attested key, the record now there, and whether this write
// made it. Created false is a record already there, this attempt's repeat
// or another attempt's that came first, answered as it is stored.
type RecordedCommit struct {
	Key     string
	Record  CommitRecord
	Created bool
}

// RecordCommit writes c, create-only, at the attested twin of commitKey
// (AttestedCommitKey): the engine's write of an attempt's result for a run
// whose results the engine records (docs/TASK_IDENTITY.md, "Results the
// engine records"). An object already there is read back and answered
// with Created false, its exact bytes and digest; one that does not decode
// is ErrCommitUndecodable and is never taken for a result. A key that is
// not a commit key is ErrInvalidRecordKey; a commit over the commit's own
// bound is ErrCommitTooLarge; a store that does not answer is
// *RecordStoreUnanswered. The caller sets c's recording before it writes.
func (s *RecordStore) RecordCommit(ctx context.Context, commitKey string, c *orkstackv1.ActivityCommit) (RecordedCommit, error) {
	key, err := AttestedCommitKey(commitKey)
	if err != nil {
		return RecordedCommit{}, err
	}
	if c == nil {
		return RecordedCommit{}, fmt.Errorf("artifacts: record commit %q: no commit: %w", key, ErrInvalidCommitKey)
	}
	document, err := MarshalCommit(c)
	if err != nil {
		return RecordedCommit{}, err
	}
	if len(document) > CommitMaxBytes {
		return RecordedCommit{}, fmt.Errorf("artifacts: record commit %q: %d bytes, the bound is %d: %w", key, len(document), CommitMaxBytes, ErrCommitTooLarge)
	}
	err = s.createIn(ctx, key, document)
	if err == nil {
		rec, err := recordFor(document, c)
		return RecordedCommit{Key: key, Record: rec, Created: true}, err
	}
	if !errors.Is(err, errBlobExists) {
		return RecordedCommit{}, err
	}
	there, found, err := s.getIn(ctx, key, CommitMaxBytes+1)
	if err != nil {
		return RecordedCommit{}, err
	}
	if !found {
		// Records are never removed while their run exists; a create that
		// found one and a read that does not is a store that cannot be
		// trusted to answer.
		return RecordedCommit{}, &RecordStoreUnanswered{Class: UnansweredUnavailable,
			Err: fmt.Errorf("artifacts: record commit %q: the store refused the create and then found nothing there", key)}
	}
	if len(there) > CommitMaxBytes {
		return RecordedCommit{}, fmt.Errorf("artifacts: record commit %q: the record there is over %d bytes: %w", key, CommitMaxBytes, ErrCommitUndecodable)
	}
	existing, err := UnmarshalCommit(there)
	if err != nil {
		return RecordedCommit{}, fmt.Errorf("artifacts: record commit %q: the record there: %w", key, err)
	}
	rec, err := recordFor(there, existing)
	return RecordedCommit{Key: key, Record: rec}, err
}

// AttestedWant names the engine's record of one task's result: its run,
// the resume scope it was made under and the run's admission.
type AttestedWant struct {
	Namespace, RunID, TaskID string
	Scope                    uint64
	AdmissionSHA256          string
}

// ReadAttestedCommit reads the engine's record of a task's result, with the
// run's spacings (nil: the reader's own). It is found only when it decodes,
// is of the format this build writes, names exactly what want says, names
// its values, and was recorded under want's admission. A store that does
// not answer is *RecordStoreUnanswered.
func (s *RecordStore) ReadAttestedCommit(ctx context.Context, class SlotClass, spacing *RecordSpacings, want AttestedWant) (RecordRead[CommitRecord], error) {
	key, err := ScopedCommitKey(want.Namespace, want.RunID, want.TaskID, want.Scope)
	if err != nil {
		return RecordRead[CommitRecord]{}, err
	}
	if key, err = AttestedCommitKey(key); err != nil {
		return RecordRead[CommitRecord]{}, err
	}
	body, _, found, err := s.readerWith(spacing).readObject(ctx, class, key, CommitMaxBytes+1)
	if err != nil {
		return RecordRead[CommitRecord]{}, err
	}
	if !found {
		return RecordRead[CommitRecord]{Answer: RecordNone}, nil
	}
	if len(body) > CommitMaxBytes {
		return invalidRead[CommitRecord]("commit", "%d bytes, more than %d", len(body), CommitMaxBytes), nil
	}
	c, err := UnmarshalCommit(body)
	if err != nil {
		return invalidRead[CommitRecord]("commit", "%v", err), nil
	}
	if v := c.GetSchemaVersion(); v != CommitSchemaVersion {
		return RecordRead[CommitRecord]{Answer: RecordUnsupported, Object: "commit", Version: int64(v)}, nil
	}
	ns := ResolveNamespace(want.Namespace)
	switch {
	case ResolveNamespace(c.GetNamespace()) != ns || c.GetRunId() != want.RunID || c.GetTaskId() != want.TaskID:
		return invalidRead[CommitRecord]("commit", "names %s/%s/%s, not %s/%s/%s", c.GetNamespace(), c.GetRunId(), c.GetTaskId(),
			ns, want.RunID, want.TaskID), nil
	case !lowerHex64(want.AdmissionSHA256) || c.GetRecorded().GetAdmissionSha256() != want.AdmissionSHA256:
		return invalidRead[CommitRecord]("commit", "not recorded under this run's admission"), nil
	case !lowerHex64(c.GetValues().GetSha256()):
		return invalidRead[CommitRecord]("commit", "names no values"), nil
	}
	rec, err := recordFor(body, c)
	if err != nil {
		return RecordRead[CommitRecord]{}, err
	}
	return RecordRead[CommitRecord]{Answer: RecordFound, Value: rec}, nil
}

// readerWith is the replica's reader, with spacing when it is given.
func (s *RecordStore) readerWith(spacing *RecordSpacings) *recordReader {
	if spacing == nil {
		return s.reader
	}
	r := *s.reader
	r.spacing = *spacing
	return &r
}
