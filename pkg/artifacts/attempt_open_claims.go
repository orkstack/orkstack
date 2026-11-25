package artifacts

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// maxAttemptClaimBytes bounds a claim's body: ids, a nonce and two times.
const maxAttemptClaimBytes = 64 << 10

// MaxOpenAttemptClaims bounds the open claims one listing reads, whatever
// tasks they name: it counts before any is read.
const MaxOpenAttemptClaims = 10_000

// ErrTooManyOpenClaims: a run has more open claims than MaxOpenAttemptClaims.
var ErrTooManyOpenClaims = fmt.Errorf("more than %d attempts are claimed and not released", MaxOpenAttemptClaims)

// OpenAttemptClaim is an attempt that was claimed and not released: its
// container may still exist. ResumeRun asks its Orker.
type OpenAttemptClaim struct {
	Ref AttemptRef
	// OrkerID and ClaimedAt are empty for a claim that does not decode: one
	// whose Orker nobody can ask.
	OrkerID   string
	ClaimedAt time.Time
}

// RunClaimReader reads a run's open claims in one listing.
type RunClaimReader interface {
	// OpenAttemptClaims returns, in key order, every claim of runID with no
	// release marker beside it. A run with none reads empty, and one with more
	// than MaxOpenAttemptClaims is ErrTooManyOpenClaims.
	OpenAttemptClaims(ctx context.Context, runID string) ([]OpenAttemptClaim, error)
}

// compile-time contract checks.
var (
	_ RunClaimReader = (*BlobAttemptStore)(nil)
	_ RunClaimReader = (*MemoryAttemptStore)(nil)
)

// openClaims are the claims among keys that no release marker answers, read
// with get; one that is gone by then is passed by.
func openClaims(runID string, keys []string, get func(key string) ([]byte, error)) ([]OpenAttemptClaim, error) {
	slices.Sort(keys)
	released := map[AttemptRef]bool{}
	for _, key := range keys {
		if ref, ok := ParseAttemptReleasedKey(key); ok {
			released[ref] = true
		}
	}
	type claimKey struct {
		key string
		ref AttemptRef
	}
	var claims []claimKey
	for _, key := range keys {
		if ref, ok := ParseAttemptClaimKey(key); ok && ref.RunID == runID && !released[ref] {
			claims = append(claims, claimKey{key, ref})
		}
	}
	if len(claims) > MaxOpenAttemptClaims {
		return nil, ErrTooManyOpenClaims
	}
	var out []OpenAttemptClaim
	for _, c := range claims {
		key, ref := c.key, c.ref
		body, err := get(key)
		if errors.Is(err, errBlobNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("artifacts: attempt claim %q: %w", key, err)
		}
		open := OpenAttemptClaim{Ref: ref}
		var claim AttemptClaim
		if json.Unmarshal(body, &claim) == nil {
			open.OrkerID, open.ClaimedAt = claim.OrkerID, claim.ClaimedAt
		}
		out = append(out, open)
	}
	return out, nil
}

// OpenAttemptClaims implements RunClaimReader over the shared store.
func (s *BlobAttemptStore) OpenAttemptClaims(ctx context.Context, runID string) ([]OpenAttemptClaim, error) {
	prefix, err := RunAttemptPrefix(runID)
	if err != nil {
		return nil, err
	}
	var keys []string
	err = listObjects(ctx, s.b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
		if strings.HasSuffix(object.Key, AttemptClaimSuffix) || strings.HasSuffix(object.Key, AttemptReleasedSuffix) {
			keys = append(keys, object.Key)
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: attempt claims of run %q: %w", runID, err)
	}
	return openClaims(runID, keys, func(key string) ([]byte, error) {
		body, _, err := s.b.get(ctx, key, maxAttemptClaimBytes)
		return body, err
	})
}

// OpenAttemptClaims implements RunClaimReader in memory.
func (s *MemoryAttemptStore) OpenAttemptClaims(ctx context.Context, runID string) ([]OpenAttemptClaim, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix, err := RunAttemptPrefix(runID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return openClaims(runID, keys, func(key string) ([]byte, error) { return bytes.Clone(s.objects[key]), nil })
}
