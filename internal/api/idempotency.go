package api

import (
	"context"
	"sync"

	"github.com/orkstack/orkstack/internal/engine"
)

// IdempotencyClaimer decides which run an idempotency key names. It is the
// seam between the API and whatever holds that decision.
//
// The decision cannot live in the API process. Two engine replicas behind one
// address hold two of everything, so a key that reached replica A is unseen at
// replica B and each starts its own run — one key, two runs, which is the bug
// M8.2 removes. `ork` therefore wires engine.TemporalIdempotency, which puts
// the decision on the Temporal cluster both replicas already share.
//
// Claim takes a callback rather than a candidate because the two
// implementations need one at different moments: a server-side authority must
// offer a candidate before the server can choose between candidates, while an
// in-process map can answer a key it has already seen without minting a run id
// at all. propose is called at most once.
type IdempotencyClaimer interface {
	// Claim binds key to a run and returns the binding, which is the winner's
	// and need not be this caller's candidate.
	Claim(ctx context.Context, key string, propose func() (engine.Claim, error)) (engine.Claim, error)

	// Rebind moves key onto next, but only if it currently names supersedes.
	// It is the reissue path: Temporal refused the holder's candidate run id,
	// so the binding has to follow the run that actually started.
	Rebind(ctx context.Context, key string, next engine.Claim, supersedes string) error
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
// claimer, so this type is unreachable there. It is what api.Service used to
// do inline, kept as a named seam so the single-process behaviour it provides
// is a deliberate test double rather than a fallback something could silently
// degrade to.
type inMemoryClaimer struct {
	mu     sync.Mutex
	claims map[string]engine.Claim
}

func newInMemoryClaimer() *inMemoryClaimer {
	return &inMemoryClaimer{claims: map[string]engine.Claim{}}
}

// Claim holds the lock across propose, so a key is reserved before its
// candidate run id exists and concurrent submissions of one key never mint a
// second id.
func (c *inMemoryClaimer) Claim(
	_ context.Context,
	key string,
	propose func() (engine.Claim, error),
) (engine.Claim, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, seen := c.claims[key]; seen {
		return existing, nil
	}
	candidate, err := propose()
	if err != nil {
		return engine.Claim{}, err
	}
	c.claims[key] = candidate
	return candidate, nil
}

func (c *inMemoryClaimer) Rebind(
	_ context.Context,
	key string,
	next engine.Claim,
	supersedes string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, seen := c.claims[key]; seen && current.RunID != supersedes {
		return nil
	}
	c.claims[key] = next
	return nil
}
