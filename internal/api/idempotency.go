package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// IdempotencyClaimer decides which run an idempotency key names. It is the
// seam between the API and whatever holds that decision.
//
// The decision cannot live in the API process. Two engine replicas behind one
// address hold two of everything, so a key that reached replica A is unseen at
// replica B and each starts its own run — one key, two runs. So `ork` wires
// engine.TemporalIdempotency, which puts the decision on the Temporal cluster
// both replicas already share.
//
// A submission asks Lookup first, so a retry or a double click costs a
// lookup and mints nothing; Claim decides whenever Lookup cannot.
type IdempotencyClaimer interface {
	// Lookup reads what key names without claiming it. bound is false when
	// no claim holds the key. An error is "no answer", never "unbound".
	Lookup(ctx context.Context, key string) (binding engine.Claim, bound bool, err error)

	// Claim binds key to candidate and returns the binding, which is the
	// winner's and need not be candidate. Asking again with the same
	// candidate after an error is safe.
	Claim(ctx context.Context, key string, candidate engine.Claim) (engine.Claim, error)

	// Rebind moves key onto next, but only if it currently names supersedes.
	// It is how a key leaves an id held by a run that is not its own.
	Rebind(ctx context.Context, key string, next engine.Claim, supersedes string) error

	// ClaimedAt is when key's running claim was made.
	ClaimedAt(ctx context.Context, key string) (time.Time, error)
}

// The production claimer has to satisfy the seam here, not only where `ork`
// wires it, so a signature drifting apart from this interface is a compile
// error in the package that defines the contract.
var _ IdempotencyClaimer = (*engine.TemporalIdempotency)(nil)

// inMemoryClaimer is the process-local claimer, and the default when nothing
// is wired.
//
// It is correct for exactly one engine process, which makes it right for unit
// tests and wrong for production — `cmd/ork` always wires the Temporal
// claimer, so this type is unreachable there. It is a named seam so the
// single-process behaviour it provides is a deliberate test double rather than
// a fallback something could silently degrade to.
type inMemoryClaimer struct {
	mu     sync.Mutex
	claims map[string]memoryClaim
}

type memoryClaim struct {
	claim engine.Claim
	at    time.Time
}

func newInMemoryClaimer() *inMemoryClaimer {
	return &inMemoryClaimer{claims: map[string]memoryClaim{}}
}

func (c *inMemoryClaimer) Lookup(_ context.Context, key string) (engine.Claim, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	held, seen := c.claims[key]
	return held.claim, seen, nil
}

// Claim is first candidate wins, everyone else is told the winner.
func (c *inMemoryClaimer) Claim(_ context.Context, key string, candidate engine.Claim) (engine.Claim, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if held, seen := c.claims[key]; seen {
		return held.claim, nil
	}
	c.claims[key] = memoryClaim{claim: candidate, at: time.Now()}
	return candidate, nil
}

func (c *inMemoryClaimer) Rebind(_ context.Context, key string, next engine.Claim, supersedes string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	held, seen := c.claims[key]
	if seen && held.claim.RunID != supersedes {
		return fmt.Errorf("rebind idempotency key to run %s: the key names run %s", next.RunID, held.claim.RunID)
	}
	at := held.at
	if !seen {
		at = time.Now()
	}
	c.claims[key] = memoryClaim{claim: next, at: at}
	return nil
}

func (c *inMemoryClaimer) ClaimedAt(_ context.Context, key string) (time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	held, seen := c.claims[key]
	if !seen {
		return time.Time{}, engine.ErrClaimEnded
	}
	return held.at, nil
}
