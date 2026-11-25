package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
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
	var mu sync.Mutex
	var final error
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, int(in.GetConcurrency())))
	for i, e := range entries {
		g.Go(func() error {
			result, err := a.adoptOne(gctx, e, in.GetGetTimeout().AsDuration(), int(in.GetGetTries()))
			if err != nil {
				mu.Lock()
				if final == nil || !isFinal(final) && isFinal(err) {
					final = err
				}
				mu.Unlock()
				return err
			}
			out.Entries[i] = &orkstackv1.AdoptedEntry{TaskId: e.GetTaskId(), Item: e.GetItem(), Result: result}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		mu.Lock()
		defer mu.Unlock()
		if final != nil {
			return nil, final
		}
		return nil, err
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
		if x.GetRunId() == "" || x.GetTaskId() == "" || x.GetIdempotencyKey() == "" {
			return fmt.Sprintf("%s names no identity to check its commit against", unitLabel(e))
		}
	}
	return ""
}

func isFinal(err error) bool { return categoryOf(err).nonRetryable() }

// adoptOne reads one entry, its tries bounded by getTimeout and tries.
func (a *ResumeActivities) adoptOne(ctx context.Context, e *orkstackv1.AdoptEntry, getTimeout time.Duration, tries int) (*orkstackv1.ExecuteTaskResult, error) {
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
		// Spaced, so a store that fails fast does not take every try at once;
		// doubling, and never past the read's own bound.
		wait := time.NewTimer(min(adoptRetrySpacing<<min(try, 8), getTimeout))
		select {
		case <-tctx.Done():
			wait.Stop()
		case <-wait.C:
		}
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
		raw, err := a.readValues(ctx, e, values)
		if err != nil {
			return nil, err
		}
		result.ValuesJson = raw
		result.ValuesInlined = true
	}
	return result, nil
}

// readValues reads a small committed values.json, checked against its
// recorded digest.
func (a *ResumeActivities) readValues(ctx context.Context, e *orkstackv1.AdoptEntry, ref *orkstackv1.ArtifactRef) ([]byte, error) {
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
	raw, err := io.ReadAll(io.LimitReader(reader, adoptInlineValuesLimit+1))
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("%s: read its values.json at %s: %v", unitLabel(e), ref.GetUri(), err), ReasonCommitReadFailed, err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != ref.GetSha256() {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: its values.json at %s hashes to %s, the commit records %s", unitLabel(e), ref.GetUri(), got, ref.GetSha256()),
			ReasonValuesMissing, nil)
	}
	return raw, nil
}

func unitLabel(e *orkstackv1.AdoptEntry) string {
	if e.GetItem() < 0 {
		return fmt.Sprintf("task %q", e.GetTaskId())
	}
	return fmt.Sprintf("task %q item %d", e.GetTaskId(), e.GetItem())
}
