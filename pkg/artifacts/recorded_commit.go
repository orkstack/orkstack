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
