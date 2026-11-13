package artifacts

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// An attempt's ordinal, its claim and its release marker.
//
// An Orker numbers the attempts of one scope (a task, or one item of a
// fan-out) itself: before it starts the attempt's container it CLAIMS the
// next ordinal by creating
//
//	metadata/attempts/{run-id}/{task-id}/[{item:05}/]{n}.claim
//
// create-if-absent, and once that container is removed (or the attempt ends
// after its claim without one) it writes {n}.released beside it. The claim
// and the marker are protocol objects, like a commit: the claim decides the
// next ordinal, and ResumeRun reads the pair only to warn about an attempt
// whose outcome is unknown. The record ({n}.json) stays a record.
//
// The next ordinal is one past every claim, marker and record already in the
// scope, so ordinals run 1, 2, 3… in practice: a gap appears only when an
// object is lost. A record written by a release that numbered attempts with
// Temporal's own count is passed over, never overwritten.
const (
	// AttemptClaimSuffix ends a claim's key.
	AttemptClaimSuffix = ".claim"
	// AttemptReleasedSuffix ends a release marker's key.
	AttemptReleasedSuffix = ".released"
	// AttemptMarkerContentType is what a claim and a marker are served as.
	AttemptMarkerContentType = "application/json"

	// maxClaimContention bounds how many ordinals one claim tries after a
	// lost race before it reports the scope contended: only another attempt
	// of the same scope claiming at the same moment loses one, and Temporal
	// runs one attempt of an activity at a time.
	maxClaimContention = 8
)

// ErrAttemptClaimContended is ClaimAttempt losing maxClaimContention races in
// a row: a store error, as far as the caller is concerned.
var ErrAttemptClaimContended = errors.New("artifacts: attempt claim: every ordinal tried was taken")

// AttemptClaim is the body of a claim and of a release marker: who took
// which ordinal of which scope. The nonce makes every writer's bytes its own,
// so a create whose answer was lost is confirmed as this writer's and no
// other's.
type AttemptClaim struct {
	OrkerID    string    `json:"orker_id"`
	RunID      string    `json:"run_id"`
	TaskID     string    `json:"task_id"`
	Item       *int32    `json:"item,omitempty"`
	Ordinal    int32     `json:"ordinal"`
	Nonce      string    `json:"nonce"`
	ClaimedAt  time.Time `json:"claimed_at,omitzero"`
	ReleasedAt time.Time `json:"released_at,omitzero"`
}

// AttemptClaimed is what a claim took, and what it found in its scope.
type AttemptClaimed struct {
	// Ordinal is the ordinal claimed.
	Ordinal int32
	// Legacy is every ordinal of the scope that has a record but no claim: a
	// record written by an Orker of a release that numbered attempts as
	// Temporal does. During a rollout it says the pool runs both; the claim
	// passed over every one of them.
	Legacy []int32
}

// AttemptClaimer allocates ordinals and closes them.
type AttemptClaimer interface {
	// ClaimAttempt takes the next ordinal of scope (scope.Attempt is
	// ignored) for orkerID. A store error is returned as it is; the caller
	// decides how often to ask again.
	ClaimAttempt(ctx context.Context, scope AttemptRef, orkerID string) (AttemptClaimed, error)
	// ReleaseAttempt writes ref's release marker. A marker already there is
	// success: the attempt is released either way.
	ReleaseAttempt(ctx context.Context, ref AttemptRef, orkerID string) error
}

// compile-time contract checks.
var (
	_ AttemptClaimer = (*BlobAttemptStore)(nil)
	_ AttemptClaimer = (*MemoryAttemptStore)(nil)
)

// AttemptClaimKey is the key of ref's claim.
func AttemptClaimKey(ref AttemptRef) (string, error) {
	return attemptMarkerKey(ref, AttemptClaimSuffix)
}

// AttemptReleasedKey is the key of ref's release marker.
func AttemptReleasedKey(ref AttemptRef) (string, error) {
	return attemptMarkerKey(ref, AttemptReleasedSuffix)
}

func attemptMarkerKey(ref AttemptRef, suffix string) (string, error) {
	key, err := AttemptKey(ref)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(key, AttemptObjectSuffix) + suffix, nil
}

// ParseAttemptClaimKey reverses AttemptClaimKey.
func ParseAttemptClaimKey(key string) (AttemptRef, bool) {
	return parseAttemptObjectKey(key, AttemptClaimSuffix)
}

// ParseAttemptReleasedKey reverses AttemptReleasedKey.
func ParseAttemptReleasedKey(key string) (AttemptRef, bool) {
	return parseAttemptObjectKey(key, AttemptReleasedSuffix)
}

// scanOrdinals reads a scope's keys: the highest ordinal any claim, marker or
// record names (0 for none), and the ordinals with a record and no claim.
func scanOrdinals(keys []string, scope AttemptRef) (highest int32, legacy []int32) {
	records, claims := map[int32]bool{}, map[int32]bool{}
	for _, key := range keys {
		for _, suffix := range []string{AttemptObjectSuffix, AttemptClaimSuffix, AttemptReleasedSuffix} {
			ref, ok := parseAttemptObjectKey(key, suffix)
			if !ok || ref.RunID != scope.RunID || ref.TaskID != scope.TaskID || ref.Item != scope.Item {
				continue
			}
			highest = max(highest, ref.Attempt)
			switch suffix {
			case AttemptObjectSuffix:
				records[ref.Attempt] = true
			case AttemptClaimSuffix:
				claims[ref.Attempt] = true
			}
		}
	}
	for n := range records {
		if !claims[n] {
			legacy = append(legacy, n)
		}
	}
	slices.Sort(legacy)
	return highest, legacy
}

func claimBody(ref AttemptRef, orkerID string, now time.Time, released bool) ([]byte, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("artifacts: attempt claim nonce: %w", err)
	}
	c := AttemptClaim{
		OrkerID: orkerID, RunID: ref.RunID, TaskID: ref.TaskID,
		Ordinal: ref.Attempt, Nonce: hex.EncodeToString(nonce),
	}
	if ref.Item.Set {
		index := ref.Item.Index
		c.Item = &index
	}
	if released {
		c.ReleasedAt = now.UTC()
	} else {
		c.ClaimedAt = now.UTC()
	}
	return stablejson.Marshal(c)
}

// claimNext runs the allocation loop over one store's two primitives.
func claimNext(
	ctx context.Context, scope AttemptRef, orkerID string,
	list func(prefix string) ([]string, error),
	create func(key string, body []byte) (exists bool, err error),
) (AttemptClaimed, error) {
	prefix, err := AttemptScopePrefix(scope.RunID, scope.TaskID, scope.Item)
	if err != nil {
		return AttemptClaimed{}, err
	}
	keys, err := list(prefix)
	if err != nil {
		return AttemptClaimed{}, fmt.Errorf("artifacts: attempt claim: list %s: %w", prefix, err)
	}
	highest, legacy := scanOrdinals(keys, scope)
	next := highest + 1
	for range maxClaimContention {
		if err := ctx.Err(); err != nil {
			return AttemptClaimed{}, err
		}
		ref := scope
		ref.Attempt = next
		key, err := AttemptClaimKey(ref)
		if err != nil {
			return AttemptClaimed{}, err
		}
		body, err := claimBody(ref, orkerID, time.Now(), false)
		if err != nil {
			return AttemptClaimed{}, err
		}
		exists, err := create(key, body)
		if err != nil {
			// The create may have landed though its answer did not (a lost
			// response, a timeout). This attempt will not use the ordinal, so
			// it is released at once, best-effort: a claim nobody holds must
			// not read as an attempt that may still be running.
			if releasedKey, kerr := AttemptReleasedKey(ref); kerr == nil {
				if marker, berr := claimBody(ref, orkerID, time.Now(), true); berr == nil {
					_, _ = create(releasedKey, marker)
				}
			}
			return AttemptClaimed{}, fmt.Errorf("artifacts: attempt claim %s: %w", key, err)
		}
		if !exists {
			return AttemptClaimed{Ordinal: next, Legacy: legacy}, nil
		}
		next++
	}
	return AttemptClaimed{}, ErrAttemptClaimContended
}

// ClaimAttempt implements AttemptClaimer over the shared store.
func (s *BlobAttemptStore) ClaimAttempt(ctx context.Context, scope AttemptRef, orkerID string) (AttemptClaimed, error) {
	return claimNext(ctx, scope, orkerID,
		func(prefix string) ([]string, error) {
			var keys []string
			err := listObjects(ctx, s.b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
				keys = append(keys, object.Key)
				return false, nil
			})
			return keys, err
		},
		func(key string, body []byte) (bool, error) {
			err := s.b.create(ctx, key, AttemptMarkerContentType, body)
			if errors.Is(err, errBlobExists) {
				return true, nil
			}
			return false, err
		})
}

// ReleaseAttempt implements AttemptClaimer over the shared store.
func (s *BlobAttemptStore) ReleaseAttempt(ctx context.Context, ref AttemptRef, orkerID string) error {
	key, err := AttemptReleasedKey(ref)
	if err != nil {
		return err
	}
	body, err := claimBody(ref, orkerID, time.Now(), true)
	if err != nil {
		return err
	}
	if err := s.b.create(ctx, key, AttemptMarkerContentType, body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: attempt release %q: %w", s.b.uri(key), err)
	}
	return nil
}

// ClaimAttempt implements AttemptClaimer in memory.
func (s *MemoryAttemptStore) ClaimAttempt(ctx context.Context, scope AttemptRef, orkerID string) (AttemptClaimed, error) {
	return claimNext(ctx, scope, orkerID,
		func(prefix string) ([]string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			var keys []string
			for key := range s.objects {
				if strings.HasPrefix(key, prefix) {
					keys = append(keys, key)
				}
			}
			return keys, nil
		},
		func(key string, body []byte) (bool, error) { return s.create(key, body), nil })
}

// ReleaseAttempt implements AttemptClaimer in memory.
func (s *MemoryAttemptStore) ReleaseAttempt(ctx context.Context, ref AttemptRef, orkerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := AttemptReleasedKey(ref)
	if err != nil {
		return err
	}
	body, err := claimBody(ref, orkerID, time.Now(), true)
	if err != nil {
		return err
	}
	s.create(key, body)
	return nil
}

// HasObject reports whether key is stored; tests read claims and markers
// with it.
func (s *MemoryAttemptStore) HasObject(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key]
	return ok
}

// create stores body under key unless something is there; it reports
// whether something was.
func (s *MemoryAttemptStore) create(key string, body []byte) (exists bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	if _, ok := s.objects[key]; ok {
		return true
	}
	s.objects[key] = body
	return false
}
