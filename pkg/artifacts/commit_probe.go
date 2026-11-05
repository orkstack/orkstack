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
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-start; _, errs[i] = store.Create(ctx, uri, candidate) }(i)
	}
	close(start)
	wg.Wait()
	wins, conflicts := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrPreconditionFailed):
			conflicts++
		default:
			return commitProbeError(uri, "unexpected writer result", err)
		}
	}
	if !((wins == 1 && conflicts == 1) || (wins == 0 && conflicts == 2)) {
		return fmt.Errorf("conditional commit probe %s: got %d winners and %d precondition failures, want one winner and one failure for a new probe or two failures for its durable winner", uri, wins, conflicts)
	}
	record, err := store.Get(ctx, uri)
	if err != nil {
		return commitProbeError(uri, "winner is unreadable", err)
	}
	got := record.Commit
	if got == nil || got.GetSchemaVersion() != candidate.GetSchemaVersion() || got.GetNamespace() != candidate.GetNamespace() || got.GetRunId() != candidate.GetRunId() || got.GetTaskId() != candidate.GetTaskId() {
		return fmt.Errorf("conditional commit probe %s: stored winner does not match the fixed probe candidate", uri)
	}
	return nil
}

func commitProbeError(uri, what string, err error) error {
	if isStoreUnavailable(err) {
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
