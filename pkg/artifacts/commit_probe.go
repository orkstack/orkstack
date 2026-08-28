package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ProbeCommitStore checks the configured backend's conditional-create
// behavior before accepting activity work. The first caller races two writers
// against the absent fixed probe key and requires one winner and one conflict.
// Later callers race against that durable winner, require both writes to be
// refused, and read the exact candidate back. Those later calls prove refusal
// and readback rather than repeating fresh-key arbitration; the first call is
// the full two-writer proof. The fixed key bounds storage at one object, and
// its dedicated prefix keeps capability probes out of activity completions.
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
			return fmt.Errorf("conditional commit probe %s: unexpected writer result: %w", uri, err)
		}
	}
	if !((wins == 1 && conflicts == 1) || (wins == 0 && conflicts == 2)) {
		return fmt.Errorf("conditional commit probe %s: got %d winners and %d precondition failures, want one winner and one failure for a new probe or two failures for its durable winner", uri, wins, conflicts)
	}
	record, err := store.Get(ctx, uri)
	if err != nil {
		return fmt.Errorf("conditional commit probe %s: winner is unreadable: %w", uri, err)
	}
	got := record.Commit
	if got == nil || got.GetSchemaVersion() != candidate.GetSchemaVersion() || got.GetNamespace() != candidate.GetNamespace() || got.GetRunId() != candidate.GetRunId() || got.GetTaskId() != candidate.GetTaskId() {
		return fmt.Errorf("conditional commit probe %s: stored winner does not match the fixed probe candidate", uri)
	}
	return nil
}

func commitProbeCandidate() *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{
		SchemaVersion: CommitSchemaVersion,
		Namespace:     DefaultNamespace,
		RunId:         "commit-store-probe",
		TaskId:        "conditional-create",
	}
}
