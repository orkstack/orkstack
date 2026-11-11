package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ErrCommitStoreUnavailable marks a probe that got no answer about conditional
// writes, because the store could not be used at all: no response, a refused
// credential, no such bucket, or a server error. It proves nothing about the
// store's conditional create, so the caller must not report it as such.
var ErrCommitStoreUnavailable = errors.New("the store could not answer whether it supports conditional writes")

// ProbeCommitStore checks the configured backend's conditional-create
// behavior before accepting activity work. The first caller for one commit
// schema version races two writers against that version's absent probe key and
// requires one winner and one conflict. Later callers race against that
// durable winner, require both writes to be refused, and read the exact
// candidate back. Those later calls prove refusal and readback rather than
// repeating fresh-key arbitration; the first call is the full two-writer
// proof. One key per schema version bounds storage across upgrades without
// making an older durable winner a boot blocker, and the dedicated prefix
// keeps capability probes out of activity completions.
//
// On an S3 store it then asks the two conditional answers an orkflow
// rollback and promotion rely on (probeConditionalAnswers), so a store that
// gets either wrong never boots an Orker.
//
// An error matching ErrCommitStoreUnavailable means the probe could not ask;
// any other error means the store answered and failed it.
func ProbeCommitStore(ctx context.Context, store CommitStore) error {
	if store == nil {
		return fmt.Errorf("commit store is not configured")
	}
	provider, ok := store.(interface{ commitProbeURI() string })
	if !ok {
		return nil
	} // in-memory/custom stores prove their own contract
	uri := provider.commitProbeURI()
	candidate := commitProbeCandidate()
	// A key that already holds the candidate is a durable winner: no create
	// can land over it, so a writer whose answer was lost there was refused,
	// whatever its read found. Only on a new key is that writer's outcome
	// unknown.
	durable := false
	switch existing, err := store.Get(ctx, uri); {
	case err == nil:
		durable = sameProbeCommit(existing.Commit, candidate)
	case !errors.Is(err, ErrNotFound):
		return commitProbeError(uri, "read before the race", err)
	}
	start := make(chan struct{})
	errs := make([]error, 2)
	records := make([]CommitRecord, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-start; records[i], errs[i] = store.Create(ctx, uri, candidate) }(i)
	}
	close(start)
	wg.Wait()
	wins, conflicts, unknown := 0, 0, 0
	for i, err := range errs {
		switch {
		case err == nil && records[i].confirmedByRead && durable:
			conflicts++
		case err == nil && records[i].confirmedByRead:
			// This writer's answer was lost, and the fixed candidate a read
			// found there is what the other writer sent too: whether this
			// writer won is not known, and is never counted as a win.
			unknown++
		case err == nil:
			wins++
		case errors.Is(err, ErrPreconditionFailed):
			conflicts++
		default:
			return commitProbeError(uri, "unexpected writer result", err)
		}
	}
	// A refusal of one writer's only send proves the store arbitrated, whether
	// the other won cleanly or only a read says the candidate is there. A
	// writer known only by a read beside anything else proves nothing.
	if unknown > 0 && !(unknown == 1 && conflicts == 1) {
		return fmt.Errorf("%w: conditional commit probe %s: a writer's answer was lost and the "+
			"candidate stored may be the other writer's, so the outcome is unknown", ErrCommitStoreUnavailable, uri)
	}
	if !((wins == 1 && conflicts == 1) || (wins == 0 && conflicts == 2) || (unknown == 1 && conflicts == 1)) {
		return fmt.Errorf("conditional commit probe %s: got %d winners and %d precondition failures, want one winner and one failure for a new probe or two failures for its durable winner", uri, wins, conflicts)
	}
	record, err := store.Get(ctx, uri)
	if err != nil {
		return commitProbeError(uri, "winner is unreadable", err)
	}
	if !sameProbeCommit(record.Commit, candidate) {
		return fmt.Errorf("conditional commit probe %s: stored winner does not match the fixed probe candidate", uri)
	}
	if prober, ok := store.(conditionalAnswerProber); ok {
		return prober.probeConditionalAnswers(ctx)
	}
	return nil
}

// conditionalAnswerProber is a store whose other conditional answers the
// boot probe asks too (S3CommitStore): the ones the orkflow store's rollback
// and promotion rely on, which share the bucket and its store.
type conditionalAnswerProber interface {
	probeConditionalAnswers(ctx context.Context) error
}

// sameProbeCommit reports whether got is the fixed probe candidate.
func sameProbeCommit(got, candidate *orkstackv1.ActivityCommit) bool {
	return got != nil && got.GetSchemaVersion() == candidate.GetSchemaVersion() &&
		got.GetNamespace() == candidate.GetNamespace() && got.GetRunId() == candidate.GetRunId() &&
		got.GetTaskId() == candidate.GetTaskId()
}

// CheckCommitStore asks whether the store still answers this process, for an
// Orker's store watch: one conditional create against the probe key, which a
// live store refuses because the boot probe's winner is there, and one read of
// that winner. So it exercises the write path and the read path a task's
// completion needs, with the Orker's own credentials, and stores nothing new:
// should the key be gone, the create writes that same fixed object again, as
// the next boot would. A store that answered is reachable whatever it answered
// about the key, and any other failure is an error.
func CheckCommitStore(ctx context.Context, store CommitStore) error {
	if store == nil {
		return fmt.Errorf("commit store is not configured")
	}
	provider, ok := store.(interface{ commitProbeURI() string })
	if !ok {
		return nil
	} // in-memory/custom stores prove their own contract
	uri := provider.commitProbeURI()
	if _, err := store.Create(ctx, uri, commitProbeCandidate()); err != nil && !errors.Is(err, ErrPreconditionFailed) {
		return fmt.Errorf("commit store check %s: create: %w", uri, err)
	}
	if _, err := store.Get(ctx, uri); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrCommitUndecodable) {
		return fmt.Errorf("commit store check %s: read: %w", uri, err)
	}
	return nil
}

func commitProbeError(uri, what string, err error) error {
	if isStoreUnavailable(err) || errors.Is(err, errOutcomeUnknown) {
		return fmt.Errorf("%w: conditional commit probe %s: %s: %w", ErrCommitStoreUnavailable, uri, what, err)
	}
	return fmt.Errorf("conditional commit probe %s: %s: %w", uri, what, err)
}

func commitProbeCandidate() *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{
		SchemaVersion: CommitSchemaVersion,
		Namespace:     DefaultNamespace,
		RunId:         "commit-store-probe",
		TaskId:        "conditional-create",
	}
}
