package artifacts

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CommitAnswer is what one read of a commit found.
type CommitAnswer int

const (
	// CommitFound: a commit of a schema this build reads.
	CommitFound CommitAnswer = iota
	// CommitAbsent: no object at the uri.
	CommitAbsent
	// CommitUndecodable: an object that is no commit this build reads, after
	// one reread.
	CommitUndecodable
	// CommitUnanswered: the store did not answer; nothing is known.
	CommitUnanswered
)

func (a CommitAnswer) String() string {
	switch a {
	case CommitFound:
		return "found"
	case CommitAbsent:
		return "absent"
	case CommitUndecodable:
		return "undecodable"
	default:
		return "unanswered"
	}
}

// ReadCommit reads the commit at uri as a retry-start check reads it: bytes
// that do not decode are read once more after rereadDelay, because an
// object can be read while it is still being written, and only a second
// undecodable read makes them corrupt. A commit of a schema version this
// build does not know is undecodable, never adopted. It is the one read a
// resume's read-back (AdoptRecorded) and ResumeRun's preflight share (E85
// §3.5, §4.3); the caller compares the commit's identity.
//
// The error is nil only with CommitFound.
func ReadCommit(ctx context.Context, store CommitStore, uri string, rereadDelay time.Duration) (CommitRecord, CommitAnswer, error) {
	record, err := store.Get(ctx, uri)
	if errors.Is(err, ErrCommitUndecodable) {
		timer := time.NewTimer(rereadDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return CommitRecord{}, CommitUnanswered, fmt.Errorf("artifacts: reread the commit at %s: %w", uri, ctx.Err())
		case <-timer.C:
		}
		record, err = store.Get(ctx, uri)
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		return CommitRecord{}, CommitAbsent, fmt.Errorf("artifacts: no commit at %s: %w", uri, err)
	case errors.Is(err, ErrCommitUndecodable):
		return CommitRecord{}, CommitUndecodable, fmt.Errorf(
			"artifacts: the object at %s is not a valid commit after a reread (%d bytes): %w", uri, record.Bytes, err)
	default:
		return CommitRecord{}, CommitUnanswered, fmt.Errorf("artifacts: read the commit at %s: %w", uri, err)
	}
	if v := record.Commit.GetSchemaVersion(); v != CommitSchemaVersion {
		return CommitRecord{}, CommitUndecodable, fmt.Errorf(
			"artifacts: the commit at %s has schema_version %d; this build reads %d: %w",
			uri, v, CommitSchemaVersion, ErrCommitUndecodable)
	}
	return record, CommitFound, nil
}
