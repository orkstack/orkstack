package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ProbeCommitStore proves that the configured backend arbitrates two
// simultaneous create-if-absent writers. A sequential HEAD/PUT check would not
// prove atomicity. Deployments must call this before accepting activity work.
func ProbeCommitStore(ctx context.Context, store CommitStore) error {
	if store == nil {
		return fmt.Errorf("commit store is not configured")
	}
	provider, ok := store.(interface{ commitProbeURI(string) string })
	if !ok {
		return nil
	} // in-memory/custom stores prove their own contract
	token := randomToken()
	uri := provider.commitProbeURI(token)
	candidate := &orkstackv1.ActivityCommit{SchemaVersion: CommitSchemaVersion, Namespace: "default", RunId: "probe-" + token, TaskId: "probe"}
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
	if wins != 1 || conflicts != 1 {
		return fmt.Errorf("conditional commit probe %s: got %d winners and %d precondition failures, want exactly one of each", uri, wins, conflicts)
	}
	if _, err := store.Get(ctx, uri); err != nil {
		return fmt.Errorf("conditional commit probe %s: winner is unreadable: %w", uri, err)
	}
	return nil
}
