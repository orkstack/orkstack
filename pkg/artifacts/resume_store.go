package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// The refusals of a resume's write (docs/TASK_IDENTITY.md). Each means the
// run changed under the resume, which resets nothing.
var (
	// ErrResumeLinkElsewhere: the scope before already links to another
	// scope.
	ErrResumeLinkElsewhere = errors.New("the scope before already leads to another resume")
	// ErrResumeScopeHasSuccessor: a later resume already left this scope.
	ErrResumeScopeHasSuccessor = errors.New("a later resume already left this scope")
	// ErrResumeRecordDiffers: another record is at this plan's key.
	ErrResumeRecordDiffers = errors.New("another record is at this resume plan's key")
)

// maxResumeWalk bounds how many links the newest-scope reader follows: a
// run is resumed at most this many times.
const maxResumeWalk = 20

// resumeScopePage is the one page a scope's listing reads: every plan's
// record a scope may hold (0 to maxResumePlan) and its link, the most one
// listing request returns.
const resumeScopePage = maxResumePlan + 2

// planRecordName matches a resume record's leaf, {n:03}.json.
var planRecordName = regexp.MustCompile(`^[0-9]{3}\.json$`)

// RecordStore reads and writes the identity records (docs/TASK_IDENTITY.md).
// Every object it writes is create-only, and nothing it reads is execution
// state: Temporal alone says whether a run is open.
type RecordStore struct {
	b blobs
	// reader reads records through the replica's slots, with the reader's
	// own spacings unless a read is given its run's.
	reader *recordReader
}

// NewRecordStore opens the identity records in the deployment's store.
func NewRecordStore(ctx context.Context, cfg Config) (*RecordStore, error) {
	b, err := openBlobs(ctx, cfg, "identity record store")
	if err != nil {
		return nil, err
	}
	return newRecordStore(b), nil
}

func newRecordStore(b blobs) *RecordStore {
	return &RecordStore{b: b, reader: &recordReader{b: b, slots: replicaRecordSlots, spacing: DefaultRecordSpacings, many: defaultManyLimits}}
}

// WriteResume writes a resume's record under the scope it opened, then the
// link from the scope before naming that record (its scope, plan number and
// SHA-256), each create-only, before anything of the run is reset. A write
// repeated with the same record succeeds. It refuses a scope a later
// resume already left (ErrResumeScopeHasSuccessor), other bytes at the
// record's key (ErrResumeRecordDiffers), and a link from the scope before
// that names anything but this record (ErrResumeLinkElsewhere), which it
// checks before writing the record as well as after. A store that does not
// answer is *RecordStoreUnanswered. An error after the record was written
// leaves it unlinked, so the resume is not the newest: the caller writes it
// again, or abandons the resume and resets nothing.
func (s *RecordStore) WriteResume(ctx context.Context, r ResumeRecord) error {
	raw, err := EncodeResumeRecord(r)
	if err != nil {
		return err
	}
	recordKey, err := ResumeRecordKey(r.Namespace, r.RunID, r.Scope, r.N)
	if err != nil {
		return err
	}
	successor, err := ResumeLinkKey(r.Namespace, r.RunID, r.Scope)
	if err != nil {
		return err
	}
	linkKey, err := ResumeLinkKey(r.Namespace, r.RunID, r.PreviousScope)
	if err != nil {
		return err
	}
	link := resumeLink{Scope: r.Scope, N: r.N, RecordSHA256: hexDigest(raw)}
	// A link leaving this scope means a later resume exists: no further
	// record is written for it. Its miss is believed once.
	if _, found, err := s.getIn(ctx, successor, maxResumeLinkBytes+1); err != nil {
		return err
	} else if found {
		return fmt.Errorf("artifacts: resume of %s scope %d: %w", r.RunID, r.Scope, ErrResumeScopeHasSuccessor)
	}
	if other, err := s.linkOtherThan(ctx, linkKey, link); err != nil {
		return err
	} else if other {
		return fmt.Errorf("artifacts: resume of %s from scope %d: %w", r.RunID, r.PreviousScope, ErrResumeLinkElsewhere)
	}
	if err := s.createOrSame(ctx, recordKey, raw, maxResumeRecordBytes); err != nil {
		if errors.Is(err, errBlobExists) {
			return fmt.Errorf("artifacts: resume of %s scope %d plan %d: %w", r.RunID, r.Scope, r.N, ErrResumeRecordDiffers)
		}
		return err
	}
	if err := s.createIn(ctx, linkKey, encodeResumeLink(link)); err == nil {
		return nil
	} else if !errors.Is(err, errBlobExists) {
		return err
	}
	if other, err := s.linkOtherThan(ctx, linkKey, link); err != nil {
		return err
	} else if other {
		return fmt.Errorf("artifacts: resume of %s from scope %d: %w", r.RunID, r.PreviousScope, ErrResumeLinkElsewhere)
	}
	return nil
}

// linkOtherThan says the link at key exists and names anything but want.
// A link this build does not read names something else.
func (s *RecordStore) linkOtherThan(ctx context.Context, key string, want resumeLink) (bool, error) {
	body, found, err := s.getIn(ctx, key, maxResumeLinkBytes+1)
	if err != nil || !found {
		return false, err
	}
	named, answer, _ := decodeResumeLink(body)
	return answer != RecordFound || named != want, nil
}

// createOrSame creates body at key; an object already there with the same
// bytes is success, and one with other bytes is errBlobExists.
func (s *RecordStore) createOrSame(ctx context.Context, key string, body []byte, max int64) error {
	err := s.createIn(ctx, key, body)
	if err == nil || !errors.Is(err, errBlobExists) {
		return err
	}
	there, found, err := s.getIn(ctx, key, max+1)
	if err != nil {
		return err
	}
	if !found || !bytes.Equal(there, body) {
		return errBlobExists
	}
	return nil
}

// getIn reads key in one of the replica's slots: found false on "no such
// key", believed once. A store that does not answer is unanswered.
func (s *RecordStore) getIn(ctx context.Context, key string, max int64) ([]byte, bool, error) {
	release, err := s.reader.slots.acquireWithin(ctx, SlotRun, s.reader.spacing.AttemptMargin)
	if err != nil {
		return nil, false, err
	}
	defer release()
	body, _, err := s.b.get(ctx, key, max)
	if errors.Is(err, errBlobNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, unansweredIn(ctx, err)
	}
	return body, true, nil
}

// createIn creates body at key in one of the replica's slots: an object
// already there is errBlobExists, and a store that does not answer is
// unanswered.
func (s *RecordStore) createIn(ctx context.Context, key string, body []byte) error {
	release, err := s.reader.slots.acquireWithin(ctx, SlotRun, s.reader.spacing.AttemptMargin)
	if err != nil {
		return err
	}
	defer release()
	err = s.b.create(ctx, key, "application/json", body)
	if err == nil || errors.Is(err, errBlobExists) {
		return err
	}
	return unansweredIn(ctx, err)
}

// NewestResume answers the newest scope of a run, the one no link leaves,
// walking the links from scope 0, and the record the last link names: its
// plan, and bytes that hash to its digest. A run never resumed answers
// scope 0 and RecordNone. A link that does not lead to a higher scope, more
// links than a run has resumes, a record the link names that is missing or
// does not hash, or one whose previous scope is not the one that linked to
// it, is RecordInvalid.
func (s *RecordStore) NewestResume(ctx context.Context, namespace, runID string) (uint64, RecordRead[ResumeRecord], error) {
	var (
		scope, previous uint64
		last            resumeLink
	)
	for walked := 0; ; walked++ {
		key, err := ResumeLinkKey(namespace, runID, scope)
		if err != nil {
			return 0, RecordRead[ResumeRecord]{}, err
		}
		body, found, err := s.getIn(ctx, key, maxResumeLinkBytes+1)
		if err != nil {
			return 0, RecordRead[ResumeRecord]{}, err
		}
		if !found {
			break // believed when said once
		}
		next, answer, reason := decodeResumeLink(body)
		switch {
		case answer == RecordUnsupported:
			return 0, RecordRead[ResumeRecord]{Answer: RecordUnsupported, Object: "resume link"}, nil
		case answer != RecordFound:
			return 0, invalidRead[ResumeRecord]("resume link", "from scope %d: %s", scope, reason), nil
		case next.Scope <= scope:
			return 0, invalidRead[ResumeRecord]("resume link", "from scope %d leads to scope %d", scope, next.Scope), nil
		case walked >= maxResumeWalk:
			return 0, invalidRead[ResumeRecord]("resume link", "more than %d resumes", maxResumeWalk), nil
		}
		previous, scope, last = scope, next.Scope, next
	}
	if scope == 0 {
		return 0, RecordRead[ResumeRecord]{Answer: RecordNone}, nil
	}
	key, err := ResumeRecordKey(namespace, runID, scope, last.N)
	if err != nil {
		return scope, invalidRead[ResumeRecord]("resume link", "names plan %d, which is no plan", last.N), nil
	}
	body, found, err := s.getIn(ctx, key, maxResumeRecordBytes+1)
	if err != nil {
		return scope, RecordRead[ResumeRecord]{}, err
	}
	if !found {
		return scope, invalidRead[ResumeRecord]("resume record", "scope %d plan %d is linked to and has no record", scope, last.N), nil
	}
	if hexDigest(body) != last.RecordSHA256 {
		return scope, invalidRead[ResumeRecord]("resume record", "scope %d plan %d does not hash to its link's digest", scope, last.N), nil
	}
	read := DecodeResumeRecord(body, namespace, runID, scope, last.N)
	if read.Answer == RecordFound && read.Value.PreviousScope != previous {
		return scope, invalidRead[ResumeRecord]("resume record", "scope %d names previous scope %d, and scope %d linked to it",
			scope, read.Value.PreviousScope, previous), nil
	}
	return scope, read, nil
}

// ResumeOf answers a scope's record: the one of its highest plan number,
// found by listing the scope, so a plan abandoned before its record leaves
// no gap that ends the search. A scope with no record is RecordNone, on
// one listing's answer: the store lists what it has written
// (TestIntegration_S3_IdentityRecords). A record name that is no plan is
// invalid.
func (s *RecordStore) ResumeOf(ctx context.Context, namespace, runID string, scope uint64) (RecordRead[ResumeRecord], error) {
	first, err := ResumeRecordKey(namespace, runID, scope, 0)
	if err != nil {
		return RecordRead[ResumeRecord]{}, err
	}
	prefix := path.Dir(first) + "/"
	highest := -1
	// One page: a scope holds at most its plans' records and its link,
	// fewer than a page, so a page cut short means a scope this store must
	// not hold, never a reason to read on.
	page, err := s.b.list(ctx, blobListInput{Prefix: prefix, MaxKeys: resumeScopePage})
	if err != nil {
		return RecordRead[ResumeRecord]{}, unansweredIn(ctx, err)
	}
	if page.Truncated {
		return invalidRead[ResumeRecord]("resume record", "scope %d holds more than %d objects", scope, resumeScopePage), nil
	}
	for _, o := range page.Objects {
		leaf := strings.TrimPrefix(o.Key, prefix)
		if !planRecordName.MatchString(leaf) {
			continue // the link, or anything nested: no plan's record
		}
		if n, err := strconv.Atoi(leaf[:3]); err == nil && n > highest {
			highest = n
		}
	}
	if highest < 0 {
		return RecordRead[ResumeRecord]{Answer: RecordNone}, nil
	}
	key, err := ResumeRecordKey(namespace, runID, scope, highest)
	if err != nil {
		return invalidRead[ResumeRecord]("resume record", "scope %d holds a record of plan %d, which is no plan", scope, highest), nil
	}
	body, _, err := s.b.get(ctx, key, maxResumeRecordBytes+1)
	if err != nil {
		// A listed record that cannot be read is not an answer.
		return RecordRead[ResumeRecord]{}, unansweredIn(ctx, err)
	}
	return DecodeResumeRecord(body, namespace, runID, scope, highest), nil
}
