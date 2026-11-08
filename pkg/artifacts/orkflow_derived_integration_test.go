//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

const derivedHash = "cf134121b6e90cc5e646b415e804152ff8d32dae245f6b924d0e35d351f3dfbc"

// derivedFixture is a store, a raw client, and a name of its own with the prefix
// it writes under cleaned up.
func derivedFixture(t *testing.T) (*artifacts.S3OrkflowStore, artifacts.Config, string, string) {
	t.Helper()
	store, cfg := newS3OrkflowStore(t)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	raw := newRawS3Client(t, cfg)
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })
	return store, cfg, name, prefix
}

// TestS3OrkflowStore_ConcurrentClaimsAcrossReplicasAgreeOnOneVersion is the race
// the claim exists for, on the real conditional write: replicas, each with its
// own client and its own candidate, all come away with one version, and one
// object holds it.
func TestS3OrkflowStore_ConcurrentClaimsAcrossReplicasAgreeOnOneVersion(t *testing.T) {
	_, cfg, name, prefix := derivedFixture(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)

	const replicas, rounds = 8, 5
	for round := range rounds {
		hash := fmt.Sprintf("%s%02d", derivedHash[:62], round)
		start := make(chan struct{})
		versions := make([]string, replicas)
		var wg sync.WaitGroup
		for i := range replicas {
			store, err := artifacts.NewS3OrkflowStore(ctx, cfg)
			if err != nil {
				t.Fatalf("NewS3OrkflowStore: %v", err)
			}
			wg.Go(func() {
				<-start
				version, err := store.ClaimDerivedVersion(ctx, "", name, hash, fmt.Sprintf("candidate-%d", i))
				if err != nil {
					t.Errorf("replica %d: %v", i, err)
					return
				}
				versions[i] = version
			})
		}
		close(start)
		wg.Wait()
		for i, version := range versions {
			if version == "" || version != versions[0] {
				t.Fatalf("round %d: replicas answered %v (replica %d differs)", round, versions, i)
			}
		}
	}
	if keys := sortedKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(keys) != rounds {
		t.Fatalf("objects = %v, want one claim per round", keys)
	}
}

// TestS3OrkflowStore_FindDerivedVersionAnswersForAClaimAndAStoredHash: nothing,
// then a version stored under the hash the way an earlier release stored it, and
// a claim takes precedence once there is one.
func TestS3OrkflowStore_FindDerivedVersionAnswersForAClaimAndAStoredHash(t *testing.T) {
	store, _, name, _ := derivedFixture(t)
	ctx := context.Background()

	if found, err := store.FindDerivedVersion(ctx, "", name, derivedHash); err != nil || found != "" {
		t.Fatalf("nothing stored: %q, %v", found, err)
	}
	if _, _, err := store.Put(ctx, testOrkflow(t, name, derivedHash, time.Now().UTC().Truncate(time.Millisecond)),
		artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put under the hash: %v", err)
	}
	if found, err := store.FindDerivedVersion(ctx, "", name, derivedHash); err != nil || found != derivedHash {
		t.Fatalf("a version stored under the hash: %q, %v", found, err)
	}
	// A claim for content already stored under its hash lands on that version:
	// the candidate is not used and no claim object is written.
	if version, err := store.ClaimDerivedVersion(ctx, "", name, derivedHash, "unused"); err != nil || version != derivedHash {
		t.Fatalf("claim over a stored hash = %q, %v", version, err)
	}
	other := strings.Repeat("a", 64)
	if version, err := store.ClaimDerivedVersion(ctx, "", name, other, "minted-1"); err != nil || version != "minted-1" {
		t.Fatalf("first claim = %q, %v", version, err)
	}
	if found, err := store.FindDerivedVersion(ctx, "", name, other); err != nil || found != "minted-1" {
		t.Fatalf("after a claim: %q, %v", found, err)
	}
	if version, err := store.ClaimDerivedVersion(ctx, "", name, other, "minted-2"); err != nil || version != "minted-1" {
		t.Fatalf("second claim = %q, %v; want the first", version, err)
	}
}

// TestS3OrkflowStore_AClaimIsNeverReadAsAVersion: listings and catalogs see only
// versions, a claim beside them changes nothing a reader gets, and a claim with
// no stored version leaves the Orkflow not found.
func TestS3OrkflowStore_AClaimIsNeverReadAsAVersion(t *testing.T) {
	store, _, name, _ := derivedFixture(t)
	ctx := context.Background()

	if _, err := store.ClaimDerivedVersion(ctx, "", name, derivedHash, "minted-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	catalog, err := store.Versions(ctx, "", name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 0 || catalog.Head != "" {
		t.Fatalf("catalog of a claim alone = %+v, want empty", catalog)
	}
	if _, err := store.Get(ctx, "", name, ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get of a claim alone = %v, want not found", err)
	}

	wf := testOrkflow(t, name, "minted-1", time.Now().UTC().Truncate(time.Millisecond))
	if _, created, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil || !created {
		t.Fatalf("Put under the claim: created %v, %v", created, err)
	}
	catalog, err = store.Versions(ctx, "", name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 1 || catalog.Head != "minted-1" {
		t.Fatalf("catalog = %d versions, head %q", len(catalog.All), catalog.Head)
	}
}

// TestS3OrkflowStore_DeleteRemovesClaims: deleting an Orkflow leaves nothing under
// its prefix, so a later apply of the same document starts over; and an Orkflow
// holding only a claim is not found, and is cleaned up all the same.
func TestS3OrkflowStore_DeleteRemovesClaims(t *testing.T) {
	store, cfg, name, prefix := derivedFixture(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)

	if _, err := store.ClaimDerivedVersion(ctx, "", name, derivedHash, "minted-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	wf := testOrkflow(t, name, "minted-1", time.Now().UTC().Truncate(time.Millisecond))
	if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, "", name); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if keys := sortedKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(keys) != 0 {
		t.Fatalf("after Delete: %v", keys)
	}
	if version, err := store.ClaimDerivedVersion(ctx, "", name, derivedHash, "minted-2"); err != nil || version != "minted-2" {
		t.Fatalf("claim after Delete = %q, %v; want the new candidate", version, err)
	}

	if err := store.Delete(ctx, "", name); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Delete of a claim alone = %v, want not found", err)
	}
	if keys := sortedKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(keys) != 0 {
		t.Fatalf("a claim alone survived Delete: %v", keys)
	}
}

// damageClaim overwrites a claim with bytes that do not decode, or name no
// addressable version.
func damageClaim(t *testing.T, ctx context.Context, raw *s3.Client, bucket, name, hash, body string) {
	t.Helper()
	key, err := artifacts.OrkflowDerivedKey("", name, hash)
	if err != nil {
		t.Fatalf("OrkflowDerivedKey: %v", err)
	}
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	}); err != nil {
		t.Fatalf("damage the claim: %v", err)
	}
}

// TestS3OrkflowStore_ADamagedClaimIsReplacedAndNeverUsed: bytes that do not name
// an addressable version answer as no claim, are never used as a version, and
// are replaced by one claim however many replicas arrive together. Nobody has to
// edit the bucket (AGENTS.md §15).
func TestS3OrkflowStore_ADamagedClaimIsReplacedAndNeverUsed(t *testing.T) {
	for label, body := range map[string]string{
		"truncated":      `{"version":"01h455`,
		"an escape":      `{"version":"../../escape"}`,
		"an empty claim": ``,
	} {
		t.Run(label, func(t *testing.T) {
			store, cfg, name, _ := derivedFixture(t)
			ctx := context.Background()
			raw := newRawS3Client(t, cfg)
			damageClaim(t, ctx, raw, cfg.S3Bucket, name, derivedHash, body)

			if found, err := store.FindDerivedVersion(ctx, "", name, derivedHash); err != nil || found != "" {
				t.Fatalf("FindDerivedVersion over a damaged claim = %q, %v; want no version and no error", found, err)
			}

			const replicas = 8
			start := make(chan struct{})
			versions := make([]string, replicas)
			var wg sync.WaitGroup
			for i := range replicas {
				replica, err := artifacts.NewS3OrkflowStore(ctx, cfg)
				if err != nil {
					t.Fatalf("NewS3OrkflowStore: %v", err)
				}
				wg.Go(func() {
					<-start
					version, err := replica.ClaimDerivedVersion(ctx, "", name, derivedHash, fmt.Sprintf("candidate-%d", i))
					if err != nil {
						t.Errorf("replica %d: %v", i, err)
						return
					}
					versions[i] = version
				})
			}
			close(start)
			wg.Wait()
			for i, version := range versions {
				if !strings.HasPrefix(version, "candidate-") || version != versions[0] {
					t.Fatalf("replicas answered %v (replica %d differs)", versions, i)
				}
			}
			// It is an ordinary claim now: found, and kept against a later one.
			if found, err := store.FindDerivedVersion(ctx, "", name, derivedHash); err != nil || found != versions[0] {
				t.Fatalf("after the repair: %q, %v; want %q", found, err, versions[0])
			}
			if version, err := store.ClaimDerivedVersion(ctx, "", name, derivedHash, "too-late"); err != nil || version != versions[0] {
				t.Fatalf("a later claim = %q, %v; want %q", version, err, versions[0])
			}
		})
	}
}

// TestS3OrkflowStore_ADamagedClaimDefersToAVersionStoredUnderTheHash: an earlier
// release's version needs no claim, so a damaged one beside it changes nothing.
func TestS3OrkflowStore_ADamagedClaimDefersToAVersionStoredUnderTheHash(t *testing.T) {
	store, cfg, name, _ := derivedFixture(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	if _, _, err := store.Put(ctx, testOrkflow(t, name, derivedHash, time.Now().UTC().Truncate(time.Millisecond)),
		artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put under the hash: %v", err)
	}
	damageClaim(t, ctx, raw, cfg.S3Bucket, name, derivedHash, `nope`)

	if found, err := store.FindDerivedVersion(ctx, "", name, derivedHash); err != nil || found != derivedHash {
		t.Fatalf("FindDerivedVersion = %q, %v; want the stored hash", found, err)
	}
	if version, err := store.ClaimDerivedVersion(ctx, "", name, derivedHash, "unused"); err != nil || version != derivedHash {
		t.Fatalf("ClaimDerivedVersion = %q, %v; want the stored hash", version, err)
	}
}

// TestS3OrkflowStore_AClaimThatCannotBeReachedIsAnError: storage failing says
// nothing about what a claim holds, so it must not read as "no claim" and mint
// a second version for content that already has one.
func TestS3OrkflowStore_AClaimThatCannotBeReachedIsAnError(t *testing.T) {
	store, cfg, name, _ := derivedFixture(t)
	if _, err := store.ClaimDerivedVersion(context.Background(), "", name, derivedHash, "minted-1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	down := cfg
	down.S3Endpoint = "http://127.0.0.1:1"
	unreachable, err := artifacts.NewS3OrkflowStore(context.Background(), down)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if found, err := unreachable.FindDerivedVersion(ctx, "", name, derivedHash); err == nil {
		t.Fatalf("FindDerivedVersion with storage down = %q, want an error", found)
	}
	if version, err := unreachable.ClaimDerivedVersion(ctx, "", name, derivedHash, "minted-2"); err == nil {
		t.Fatalf("ClaimDerivedVersion with storage down = %q, want an error", version)
	}
}
