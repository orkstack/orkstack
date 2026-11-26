package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/sync/errgroup"
)

// ActivityAdoptRecorded is the local activity of a resume's read-back
// (E85 §4.3): it runs on the engine's workflow worker, never on an Orker.
const ActivityAdoptRecorded = "AdoptRecorded"

// The read-back's failure reasons (§4.3, docs/REASONS.md). A missing or
// undecodable commit is final; a store that does not answer is retried by
// the call's retry policy, and the workflow fails the first unread entry
// COMMIT_READ_FAILED once the phase's deadline passes.
const (
	ReasonCommitMissing = "COMMIT_MISSING"
	// ReasonCommitIdentity: a commit is there and is another activity's,
	// as the Orker's retry-start check names it.
	ReasonCommitIdentity    = "COMMIT_IDENTITY_MISMATCH"
	ReasonCommitUndecodable = "COMMIT_UNDECODABLE"
	ReasonCommitReadFailed  = "COMMIT_READ_FAILED"
	ReasonValuesMissing     = "VALUES_MISSING"
)

// adoptInlineValuesLimit is the largest values.json an adopted result
// carries inline, as an adopting Orker's does (internal/orker
// InlineValuesLimit): the same task reads the same row whether its result
// was run or kept.
const adoptInlineValuesLimit = 4096

// errValuesMismatch is a values.json whose bytes are not the recorded ones.
var errValuesMismatch = errors.New("its values are not the recorded ones")

// undecodableReread is how long a read of undecodable bytes waits before its
// one reread, as the Orker's retry-start check does.
const undecodableReread = 250 * time.Millisecond

// adoptRetrySpacing is the first wait between two tries of one read; each
// later wait doubles it, within the read's bound.
const adoptRetrySpacing = 200 * time.Millisecond

// AdoptStore is the slice of artifacts.Store the read-back reads values from.
type AdoptStore interface {
	Get(ctx context.Context, uri string) (io.ReadCloser, error)
}

// ResumeActivities are the local activities a resumed run runs at R and in
// its read-back.
type ResumeActivities struct {
	commits artifacts.CommitStore
	values  AdoptStore

	history           historyReader
	temporalNamespace string
	records           resumePlanSource
	observer          ResumeObserver
	// recorded reads the records an admitted run's settled tasks are read back
	// from (RESOLUTION and COMPOSED entries).
	recorded ResumeRecordStore
}

// WithRecordReads gives the activities the identity records an admitted
// run's settled tasks are read back from.
func (a *ResumeActivities) WithRecordReads(store ResumeRecordStore) *ResumeActivities {
	a.recorded = store
	return a
}

// NewResumeActivities builds them over the deployment's commit and
// artifact stores.
func NewResumeActivities(commits artifacts.CommitStore, values AdoptStore) *ResumeActivities {
	return &ResumeActivities{commits: commits, values: values}
}

// AdoptRecorded reads one call's kept results back (§4.3, step 3): each
// entry's commit, checked against the identity dispatch gave it, as the
// result an adopting Orker would return. At most in.Concurrency reads are in
// flight, each tried at most in.GetTries times within in.GetTimeout. An
// absent commit, or one that names another activity, is COMMIT_MISSING; one
// that does not decode is COMMIT_UNDECODABLE; both are final. A store that
// does not answer is a retryable error. The answer holds one result per
// entry, in order, and never more than in.Budget encoded bytes.
func (a *ResumeActivities) AdoptRecorded(ctx context.Context, in *orkstackv1.AdoptRecordedInput) (*orkstackv1.AdoptRecordedResult, error) {
	out, err := a.adoptRecorded(ctx, in)
	if a.observer != nil && activity.IsActivity(ctx) && (err == nil || isFinal(err)) && len(in.GetEntries()) > 0 {
		if namespace := in.GetEntries()[0].GetExpected().GetNamespace(); namespace != "" {
			a.observer.ObserveReadBackCall(ctx, namespace, int(activity.GetInfo(ctx).Attempt))
		}
	}
	return out, err
}

func (a *ResumeActivities) adoptRecorded(ctx context.Context, in *orkstackv1.AdoptRecordedInput) (*orkstackv1.AdoptRecordedResult, error) {
	if why := adoptInputFault(in); why != "" {
		// The workflow pins every bound; an input without them is a fault of
		// the build that made it, and reading on unbounded would hide it.
		return nil, temporal.NewNonRetryableApplicationError("read-back input: "+why, ReasonCommitReadFailed, nil)
	}
	entries := in.GetEntries()
	out := &orkstackv1.AdoptRecordedResult{Entries: make([]*orkstackv1.AdoptedEntry, len(entries))}
	failures := make([]error, len(entries))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, int(in.GetConcurrency())))
	for i, e := range entries {
		g.Go(func() error {
			result, err := a.adoptOne(gctx, e, in.GetGetTimeout().AsDuration(), int(in.GetGetTries()))
			if err != nil {
				failures[i] = err
				return err
			}
			out.Entries[i] = &orkstackv1.AdoptedEntry{TaskId: e.GetTaskId(), Item: e.GetItem(), Result: result}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, callFailure(entries, out.Entries, failures, err)
	}
	{
		budget := in.GetBudget()
		size, err := task.PayloadSize(out)
		if err != nil {
			return nil, fmt.Errorf("measure the read-back: %w", err)
		}
		if size > budget {
			// ResumeRun bounds every entry to fit alone and the workflow packs
			// by those sizes, so this is a commit that changed under the
			// resume: commits are create-only, so it never fits on a retry.
			return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf(
				"the read-back of %d entries encodes to %d bytes, over its %d-byte call", len(entries), size, budget),
				ReasonCommitReadFailed, nil)
		}
	}
	return out, nil
}

// callFailure is what a call that failed fails (§4.3): the first entry whose
// failure is final, which a retry cannot change, or else the first entry it
// did not read back, with the first failure the call met. Which read gave up
// first is a race; which entry is failed is not, and a failure that is not
// final blames nothing a retry would not read again.
func callFailure(entries []*orkstackv1.AdoptEntry, read []*orkstackv1.AdoptedEntry, failures []error, waited error) error {
	for i, err := range failures {
		if err != nil && isFinal(ofUnit(err, entries[i])) {
			return ofUnit(err, entries[i])
		}
	}
	first := slices.IndexFunc(read, func(e *orkstackv1.AdoptedEntry) bool { return e == nil })
	if first < 0 {
		return waited
	}
	if own := failures[first]; own != nil && categoryOf(own).isApplication() {
		return ofUnit(own, entries[first])
	}
	// Its read was given up when another's failed: that failure is its cause.
	for _, err := range failures {
		if reading := categoryOf(err); err != nil && reading.isApplication() {
			return ofUnit(temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("task %q was not read back: %s", entries[first].GetTaskId(), reading.appMessage()), reading.appType(), err), entries[first])
		}
	}
	return waited
}

// adoptFailedUnit is the unit a read-back call failed on, carried with its
// failure: the calls pack many, and blaming another would rerun a result that
// is there.
type adoptFailedUnit struct {
	Task string `json:"task"`
	Item int32  `json:"item"`
}

// ofUnit is err naming e's unit.
func ofUnit(err error, e *orkstackv1.AdoptEntry) error {
	reading := categoryOf(err)
	if !reading.isApplication() {
		return err
	}
	unit := adoptFailedUnit{Task: e.GetTaskId(), Item: e.GetItem()}
	if reading.nonRetryable() {
		return temporal.NewNonRetryableApplicationError(reading.appMessage(), reading.appType(), err, unit)
	}
	return temporal.NewApplicationErrorWithCause(reading.appMessage(), reading.appType(), err, unit)
}

// adoptInputFault is why in cannot be read back, or "".
func adoptInputFault(in *orkstackv1.AdoptRecordedInput) string {
	switch {
	case in.GetBudget() <= 0:
		return "no budget"
	case in.GetConcurrency() <= 0:
		return "no concurrency"
	case in.GetGetTries() <= 0 || in.GetGetTimeout().AsDuration() <= 0:
		return "no read bound"
	}
	for _, e := range in.GetEntries() {
		x := e.GetExpected()
		switch e.GetKind() {
		case orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_VALUES:
			if x.GetRunId() == "" || x.GetTaskId() == "" || e.GetValues().GetUri() == "" || e.GetInlineLimit() <= 0 {
				return fmt.Sprintf("%s names no values to read, or no bound on them", unitLabel(e))
			}
			continue
		case orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_RESOLUTION, orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMPOSED:
			if x.GetRunId() == "" || x.GetTaskId() == "" || e.GetValues().GetSha256() == "" || e.GetAdmissionSha256() == "" || e.GetInlineLimit() <= 0 {
				return fmt.Sprintf("%s names no record to read, its values or no bound on them", unitLabel(e))
			}
			continue
		}
		if x.GetRunId() == "" || x.GetTaskId() == "" || x.GetIdempotencyKey() == "" {
			return fmt.Sprintf("%s names no identity to check its commit against", unitLabel(e))
		}
	}
	return ""
}

func isFinal(err error) bool { return categoryOf(err).nonRetryable() }

// adoptOne reads one entry, its tries bounded by getTimeout and tries.
func (a *ResumeActivities) adoptOne(ctx context.Context, e *orkstackv1.AdoptEntry, getTimeout time.Duration, tries int) (*orkstackv1.ExecuteTaskResult, error) {
	switch e.GetKind() {
	case orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_VALUES:
		return a.adoptValues(ctx, e, getTimeout, tries)
	case orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_RESOLUTION, orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMPOSED:
		return a.adoptRecordedValues(ctx, e, getTimeout, tries)
	}
	if e.GetKind() != orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMMIT {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: read-back entry kind %v is not one this build reads", unitLabel(e), e.GetKind()), ReasonCommitReadFailed, nil)
	}
	if a.commits == nil || e.GetCommitUri() == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: no commit to read back (no commit store, or no commit uri)", unitLabel(e)), ReasonCommitMissing, nil)
	}
	tctx := ctx
	if getTimeout > 0 {
		var cancel context.CancelFunc
		tctx, cancel = context.WithTimeout(ctx, getTimeout)
		defer cancel()
	}
	var last error
	for try := 0; try < max(1, tries); try++ {
		record, answer, err := artifacts.ReadCommit(tctx, a.commits, e.GetCommitUri(), undecodableReread)
		switch answer {
		case artifacts.CommitFound:
			return a.resultOf(tctx, e, record)
		case artifacts.CommitAbsent:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("%s: its kept result is gone: %v", unitLabel(e), err), ReasonCommitMissing, err)
		case artifacts.CommitUndecodable:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("%s: %v", unitLabel(e), err), ReasonCommitUndecodable, err)
		}
		last = err
		if tctx.Err() != nil || try == max(1, tries)-1 {
			break
		}
		retryWait(tctx, try, getTimeout)
	}
	return nil, temporal.NewApplicationErrorWithCause(
		fmt.Sprintf("%s: the commit store did not answer: %v", unitLabel(e), last), ReasonCommitReadFailed, last)
}

// resultOf is the result an adopting Orker returns for record: identity
// checked, the committing attempt's facts, and small values inline.
func (a *ResumeActivities) resultOf(ctx context.Context, e *orkstackv1.AdoptEntry, record artifacts.CommitRecord) (*orkstackv1.ExecuteTaskResult, error) {
	commit := record.Commit
	want := CommitIdentity{
		Namespace:        e.GetExpected().GetNamespace(),
		RunID:            e.GetExpected().GetRunId(),
		TaskID:           e.GetExpected().GetTaskId(),
		DefinitionDigest: e.GetExpected().GetDefinitionDigest(),
		IdempotencyKey:   e.GetExpected().GetIdempotencyKey(),
		ImageIndexDigest: e.GetExpected().GetImageIndexDigest(),
	}
	if mismatch := want.Mismatch(commit); mismatch != "" {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: the commit at %s is for a different activity: %s", unitLabel(e), e.GetCommitUri(), mismatch),
			ReasonCommitIdentity, nil)
	}
	if commit.GetRecorded() == nil && artifacts.IsAttestedURI(e.GetCommitUri()) {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: the record at %s carries no engine's recording, so no engine wrote it", unitLabel(e), e.GetCommitUri()),
			ReasonCommitIdentity, nil)
	}
	result := &orkstackv1.ExecuteTaskResult{
		TaskId:          e.GetTaskId(),
		ExitCode:        commit.GetExitCode(),
		Commit:          &orkstackv1.CommitRef{Uri: e.GetCommitUri(), Sha256: record.SHA256, Etag: record.ETag, CreatedAt: commit.GetFinishedAt()},
		Artifacts:       commit.GetArtifacts(),
		SystemArtifacts: commit.GetSystemArtifacts(),
		ValuesRef:       commit.GetValues(),
		Image:           commit.GetImage(),
		CommitAdopted:   true,
		StartedAt:       commit.GetStartedAt(),
		FinishedAt:      commit.GetFinishedAt(),
		OrkerId:         commit.GetOrkerId(),
		Attempt:         commit.GetAttempt(),
		Cache:           commit.GetCache(),
	}
	values := commit.GetValues()
	if values.GetSizeBytes() > 0 && values.GetSizeBytes() <= adoptInlineValuesLimit {
		raw, err := a.readValues(ctx, e, values, adoptInlineValuesLimit)
		if err != nil {
			return nil, err
		}
		result.ValuesJson = raw
		result.ValuesInlined = true
	}
	return result, nil
}

// adoptValues is the result of a settled task: its values, inline up to the
// entry's bound, and read as a commit's are, each read tried within getTimeout.
func (a *ResumeActivities) adoptValues(ctx context.Context, e *orkstackv1.AdoptEntry, getTimeout time.Duration, tries int) (*orkstackv1.ExecuteTaskResult, error) {
	ref := e.GetValues()
	result := &orkstackv1.ExecuteTaskResult{TaskId: e.GetTaskId(), ValuesRef: ref}
	if ref.GetSizeBytes() > e.GetInlineLimit() {
		return result, nil
	}
	tctx := ctx
	if getTimeout > 0 {
		var cancel context.CancelFunc
		tctx, cancel = context.WithTimeout(ctx, getTimeout)
		defer cancel()
	}
	reread := false
	for try := 0; ; try++ {
		raw, err := a.readValues(tctx, e, ref, e.GetInlineLimit())
		if errors.Is(err, errValuesMismatch) && !reread {
			// Other bytes than recorded may be a read cut short: read once more
			// before they decide an answer is gone, as a commit's are.
			reread = true
			retryWait(tctx, 0, undecodableReread)
			continue
		}
		switch {
		case err == nil:
			result.ValuesJson, result.ValuesInlined = raw, len(raw) > 0
			return result, nil
		case isFinal(err) || tctx.Err() != nil || try >= max(1, tries)-1:
			return nil, err
		}
		retryWait(tctx, try, getTimeout)
	}
}

// adoptRecordedValues is the result of a settled task of an admitted run: its
// record, at the scope the entry names and under the run's admission, read
// as ResumeRun verified it (design §28.9). The record is create-only, so one
// that verified then stands now; its values must be the ones the resume kept,
// and are then read as a VALUES entry's are. A record that is not there, or
// whose values are not, is VALUES_MISSING; one that is not this task's, or
// that no longer settles it, is RESOLUTION_RECORD_INVALID; both are final. A
// store that does not answer is retried by the call.
func (a *ResumeActivities) adoptRecordedValues(ctx context.Context, e *orkstackv1.AdoptEntry, getTimeout time.Duration, tries int) (*orkstackv1.ExecuteTaskResult, error) {
	if a.recorded == nil {
		return nil, temporal.NewApplicationError(fmt.Sprintf("%s: this engine reads no identity records; another replica may", unitLabel(e)), ReasonCommitReadFailed)
	}
	x := e.GetExpected()
	of, want := artifacts.RecordComposed, orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED
	if e.GetKind() == orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_RESOLUTION {
		of, want = artifacts.RecordResolution, orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION
	}
	key, err := artifacts.RecordKey(of, x.GetNamespace(), x.GetRunId(), x.GetTaskId(), e.GetRecordScope())
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf("%s: %v", unitLabel(e), err), ResolutionRecordInvalidCode, err)
	}
	tctx := ctx
	if getTimeout > 0 {
		var cancel context.CancelFunc
		tctx, cancel = context.WithTimeout(ctx, getTimeout)
		defer cancel()
	}
	read, err := a.recorded.ReadRecordedValues(tctx, artifacts.SlotRun, nil, key, artifacts.RecordWant{Kind: want, Namespace: x.GetNamespace(),
		RunID: x.GetRunId(), TaskID: x.GetTaskId(), Scope: e.GetRecordScope(), AdmissionSHA256: e.GetAdmissionSha256()})
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(fmt.Sprintf("%s: its record could not be read: %v", unitLabel(e), err), ReasonCommitReadFailed, err)
	}
	switch {
	case read.Answer == artifacts.RecordNone,
		read.Answer == artifacts.RecordInvalid && read.Object == "values" && read.Reason == artifacts.RecordValuesMissing:
		return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf("%s: its record, or the values it names, is gone", unitLabel(e)), ReasonValuesMissing, nil)
	case read.Answer == artifacts.RecordUnsupported:
		// A newer release's record: a replica of that release reads it.
		return nil, temporal.NewApplicationError(fmt.Sprintf("%s: its record is of a newer format than this engine reads", unitLabel(e)), ReasonCommitReadFailed)
	case read.Answer != artifacts.RecordFound:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: its record cannot be used (%s %s: %s)", unitLabel(e), read.Answer, read.Object, read.Reason), ResolutionRecordInvalidCode, nil)
	}
	rejected, valuesSHA256, _ := read.Value.Standing()
	if rejected || valuesSHA256 != e.GetValues().GetSha256() {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: its record is not of the values the resume kept", unitLabel(e)), ResolutionRecordInvalidCode, nil)
	}
	return a.adoptValues(ctx, e, getTimeout, tries)
}

// retryWait spaces two tries of one read, so a store that fails fast does not
// take every try at once; doubling, and never past the read's own bound.
func retryWait(ctx context.Context, try int, bound time.Duration) {
	wait := time.NewTimer(min(adoptRetrySpacing<<min(try, 8), bound))
	defer wait.Stop()
	select {
	case <-ctx.Done():
	case <-wait.C:
	}
}

// readValues reads a small committed values.json, checked against its
// recorded digest.
func (a *ResumeActivities) readValues(ctx context.Context, e *orkstackv1.AdoptEntry, ref *orkstackv1.ArtifactRef, limit int64) ([]byte, error) {
	if a.values == nil {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: no artifact store to read its values from", unitLabel(e)), ReasonValuesMissing, nil)
	}
	reader, err := a.values.Get(ctx, ref.GetUri())
	if errors.Is(err, artifacts.ErrNotFound) {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: its values.json at %s is gone", unitLabel(e), ref.GetUri()), ReasonValuesMissing, err)
	}
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("%s: read its values.json at %s: %v", unitLabel(e), ref.GetUri(), err), ReasonCommitReadFailed, err)
	}
	defer func() { _ = reader.Close() }()
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("%s: read its values.json at %s: %v", unitLabel(e), ref.GetUri(), err), ReasonCommitReadFailed, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("%s: read its values.json at %s: %v", unitLabel(e), ref.GetUri(), err), ReasonCommitReadFailed, err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != ref.GetSha256() {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: its values.json at %s hashes to %s, the run recorded %s", unitLabel(e), ref.GetUri(), got, ref.GetSha256()),
			ReasonValuesMissing, errValuesMismatch)
	}
	return raw, nil
}

func unitLabel(e *orkstackv1.AdoptEntry) string {
	if e.GetItem() < 0 {
		return fmt.Sprintf("task %q", e.GetTaskId())
	}
	return fmt.Sprintf("task %q item %d", e.GetTaskId(), e.GetItem())
}
