//go:build integration

// Integration coverage for the current-version pointer: promotion is a
// compare-and-swap on one small object beside the immutable versions.
//
// Run against the repo's local infrastructure (`make infra-up`):
//
//	go test -tags=integration ./pkg/artifacts/... -count=1 -run TestS3OrkflowStore_
//
// The tests skip (never fail) when MinIO is unreachable — unless
// ORK_REQUIRE_INFRA is set, where an absent prerequisite IS the failure.
package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestS3OrkflowStore_CurrentPointerDecidesWhatAnEmptyVersionMeans is the
// milestone's first acceptance against a real bucket: versions stay immutable,
// a pointer decides what resolves, and an orkflow nobody promoted keeps
// answering exactly as it did before this milestone existed.
func TestS3OrkflowStore_CurrentPointerDecidesWhatAnEmptyVersionMeans(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	applied := time.Now().UTC().Truncate(time.Millisecond)
	first := testOrkflow(t, name, "1.0.0", applied)
	second := testOrkflow(t, name, "2.0.0", applied.Add(time.Second))
	second.Description = "the second version"
	for _, wf := range []*orkstackv1.Orkflow{first, second} {
		if _, _, err := store.Put(ctx, wf); err != nil {
			t.Fatalf("Put(%s): %v", wf.GetVersion(), err)
		}
	}

	// Nobody has promoted anything: today's rule, unchanged.
	if current, err := store.CurrentVersion(ctx, artifacts.DefaultNamespace, name); err != nil || current != "" {
		t.Fatalf("CurrentVersion before any promotion = %q, %v; want \"\", nil", current, err)
	}
	requireResolves(t, ctx, store, name, "2.0.0", "before any promotion")

	// Promote back to 1.0.0: the pointer decides, not the application order.
	previous, err := store.SetCurrentVersion(ctx, artifacts.DefaultNamespace, name, "1.0.0")
	if err != nil {
		t.Fatalf("SetCurrentVersion(1.0.0): %v", err)
	}
	if previous != "" {
		t.Errorf("the first promotion replaced %q, want \"\" (there was no pointer)", previous)
	}
	requireResolves(t, ctx, store, name, "1.0.0", "after promoting 1.0.0")

	// The pointer is one small object beside the versions, and it holds only
	// the version string — never a copy of the document.
	pointerKey := prefix + "@current.json"
	document := string(readKey(t, ctx, raw, cfg.S3Bucket, pointerKey))
	if !strings.Contains(document, `"version": "1.0.0"`) {
		t.Errorf("pointer document does not name 1.0.0:\n%s", document)
	}
	if strings.Contains(document, "tasks") || strings.Contains(document, "sourceYaml") {
		t.Errorf("the pointer carries a copy of the definition, which can drift:\n%s", document)
	}

	// A promoted orkflow lists as what it resolves to, so `orkflow list` and
	// `run submit` cannot disagree about which version this deployment runs.
	listed, err := store.List(ctx, artifacts.DefaultNamespace)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	found := false
	for _, wf := range listed {
		if wf.GetName() != name {
			continue
		}
		found = true
		if wf.GetVersion() != "1.0.0" {
			t.Errorf("List reports %s@%s after promoting 1.0.0", wf.GetName(), wf.GetVersion())
		}
	}
	if !found {
		t.Errorf("List does not report %q at all", name)
	}

	// The catalog reports both questions separately: what runs, and what an
	// editor should edit from.
	versions, err := store.Versions(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(versions.All) != 2 || versions.All[0].GetVersion() != "2.0.0" {
		t.Errorf("Versions.All = %v, want 2.0.0 then 1.0.0", versionStrings(versions.All))
	}
	if versions.Current != "1.0.0" || versions.Head != "2.0.0" {
		t.Errorf("Versions current/head = %q/%q, want 1.0.0/2.0.0", versions.Current, versions.Head)
	}

	// Rollforward, and the call reports what it replaced.
	previous, err = store.SetCurrentVersion(ctx, artifacts.DefaultNamespace, name, "2.0.0")
	if err != nil {
		t.Fatalf("SetCurrentVersion(2.0.0): %v", err)
	}
	if previous != "1.0.0" {
		t.Errorf("promotion replaced %q, want 1.0.0", previous)
	}
	requireResolves(t, ctx, store, name, "2.0.0", "after promoting 2.0.0")

	// Promoting to a version nobody applied is refused, and the pointer does
	// not move: this is the write-side half of "a reader never observes a
	// pointer to a version that does not exist".
	if _, err := store.SetCurrentVersion(ctx, artifacts.DefaultNamespace, name, "9.9.9"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("SetCurrentVersion(9.9.9) = %v, want artifacts.ErrNotFound", err)
	}
	requireResolves(t, ctx, store, name, "2.0.0", "after a refused promotion")
}

// TestS3OrkflowStore_ReApplyingIdenticalYAMLChurnsNothing measures the
// milestone's no-op claim the only way it can be measured: the stored bytes,
// before and after.
//
// It is the check that would have failed before this slice, when ApplyOrkflow
// stamped created_at into the document ahead of the write and every re-apply
// therefore produced different bytes under the same key.
func TestS3OrkflowStore_ReApplyingIdenticalYAMLChurnsNothing(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	wf := testOrkflow(t, name, "1.0.0", time.Now().UTC().Truncate(time.Millisecond))
	if _, created, err := store.Put(ctx, wf); err != nil || !created {
		t.Fatalf("Put(first) = created %v, %v; want created, nil", created, err)
	}
	key := prefix + "1.0.0.json"
	before := readKey(t, ctx, raw, cfg.S3Bucket, key)
	beforeStamp := objectModified(t, ctx, raw, cfg.S3Bucket, key)

	// Re-apply the SAME definition, a moment later and with a later created_at
	// — which is exactly what a second `orkctl apply` of an unchanged file
	// sends, because the API stamps the time of the apply.
	time.Sleep(1100 * time.Millisecond) // S3 LastModified has second resolution
	again := testOrkflow(t, name, "1.0.0", time.Now().UTC().Truncate(time.Millisecond))
	stored, created, err := store.Put(ctx, again)
	if err != nil {
		t.Fatalf("Put(repeat): %v", err)
	}
	if created {
		t.Error("a byte-identical re-apply reported that it created a version")
	}
	if !stored.GetCreatedAt().AsTime().Equal(wf.GetCreatedAt().AsTime()) {
		t.Errorf("a repeat returned created_at %s, want the STORED %s",
			stored.GetCreatedAt().AsTime(), wf.GetCreatedAt().AsTime())
	}

	after := readKey(t, ctx, raw, cfg.S3Bucket, key)
	if string(before) != string(after) {
		t.Fatalf("re-applying identical YAML rewrote the object:\nbefore:\n%s\nafter:\n%s",
			before, after)
	}
	if afterStamp := objectModified(t, ctx, raw, cfg.S3Bucket, key); !afterStamp.Equal(beforeStamp) {
		t.Fatalf("re-applying identical YAML touched the object: LastModified moved %s -> %s",
			beforeStamp, afterStamp)
	}
	// And no second object appeared: a repeat creates no version.
	if keys := listKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(keys) != 1 {
		t.Fatalf("a repeat left %v under the orkflow's prefix, want one object", keys)
	}
}

// TestS3OrkflowStore_ConcurrentPromotionsConvergeOnOneExistingVersion is the
// store-level half of the two-replica acceptance: many writers, one bucket, and
// every observation is of a version that exists.
//
// The two-replica claim itself is proved end to end against two real `ork`
// processes in cmd/orkctl (TestIntegration_TwoReplicasPromoteConcurrently…).
// This one holds the primitive those replicas rely on, where a failure is
// diagnosable.
func TestS3OrkflowStore_ConcurrentPromotionsConvergeOnOneExistingVersion(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	applied := time.Now().UTC().Truncate(time.Millisecond)
	stored := map[string]bool{}
	for i, version := range []string{"1.0.0", "2.0.0"} {
		wf := testOrkflow(t, name, version, applied.Add(time.Duration(i)*time.Second))
		wf.Description = "version " + version
		if _, _, err := store.Put(ctx, wf); err != nil {
			t.Fatalf("Put(%s): %v", version, err)
		}
		stored[version] = true
	}

	// A SECOND store with its own S3 client, which is what a second replica
	// has: no shared state, no shared cache, one bucket.
	other, err := artifacts.NewS3OrkflowStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore (second replica): %v", err)
	}
	writers := []*artifacts.S3OrkflowStore{store, other}

	const rounds = 12
	var wg sync.WaitGroup
	results := make([]error, 2*rounds)
	observations := make(chan string, 8*rounds)
	stop := make(chan struct{})

	// A reader running throughout: every value it sees must be a version that
	// exists. This is the property a dangling pointer would break.
	var readers sync.WaitGroup
	for _, reader := range writers {
		readers.Add(1)
		go func(reader *artifacts.S3OrkflowStore) {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				wf, err := reader.Get(ctx, artifacts.DefaultNamespace, name, "")
				if err != nil {
					observations <- "ERROR: " + err.Error()
					return
				}
				observations <- wf.GetVersion()
			}
		}(reader)
	}

	for round := range rounds {
		for w, writer := range writers {
			wg.Add(1)
			go func(index int, writer *artifacts.S3OrkflowStore, version string) {
				defer wg.Done()
				_, err := writer.SetCurrentVersion(ctx, artifacts.DefaultNamespace, name, version)
				results[index] = err
			}(round*2+w, writer, []string{"1.0.0", "2.0.0"}[w])
		}
	}
	wg.Wait()
	close(stop)
	readers.Wait()
	close(observations)

	// Every promotion either landed or reported that it lost the race. A lost
	// race is a fact the caller is told, never a silent overwrite.
	refused := 0
	for i, err := range results {
		switch {
		case err == nil:
		case errors.Is(err, artifacts.ErrPreconditionFailed):
			refused++
		default:
			t.Fatalf("promotion %d failed for an unexpected reason: %v", i, err)
		}
	}
	t.Logf("%d concurrent promotions: %d landed, %d reported a lost race",
		len(results), len(results)-refused, refused)

	// No reader ever saw a version that does not exist.
	seen := map[string]int{}
	for observation := range observations {
		if strings.HasPrefix(observation, "ERROR: ") {
			t.Fatalf("a read during concurrent promotion failed: %s", observation)
		}
		if !stored[observation] {
			t.Fatalf("a reader observed version %q, which is not stored", observation)
		}
		seen[observation]++
	}
	t.Logf("reads during the race resolved to %v", seen)

	// Both replicas converge on one final value, and it is one of the two.
	finalA, err := store.CurrentVersion(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("CurrentVersion (A): %v", err)
	}
	finalB, err := other.CurrentVersion(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("CurrentVersion (B): %v", err)
	}
	if finalA != finalB {
		t.Fatalf("the two replicas disagree about the current version: %q vs %q", finalA, finalB)
	}
	if !stored[finalA] {
		t.Fatalf("the final pointer names %q, which is not stored", finalA)
	}
	// Exactly one object holds the pointer: concurrency never forked it.
	pointers := 0
	for _, key := range listKeys(t, ctx, raw, cfg.S3Bucket, prefix) {
		if strings.HasSuffix(key, "@current.json") {
			pointers++
		}
	}
	if pointers != 1 {
		t.Fatalf("the orkflow has %d pointer objects, want exactly 1", pointers)
	}
	writePromotionProof(t, name, finalA, len(results)-refused, refused, seen)
}

// requireResolves asserts what an empty version means right now, through both
// the resolved read and the pointer itself.
func requireResolves(
	t *testing.T,
	ctx context.Context,
	store *artifacts.S3OrkflowStore,
	name, want, when string,
) {
	t.Helper()
	wf, err := store.Get(ctx, artifacts.DefaultNamespace, name, "")
	if err != nil {
		t.Fatalf("Get(resolved) %s: %v", when, err)
	}
	if wf.GetVersion() != want {
		t.Fatalf("an empty version resolves to %q %s, want %q", wf.GetVersion(), when, want)
	}
}

func versionStrings(versions []*orkstackv1.Orkflow) []string {
	out := make([]string, 0, len(versions))
	for _, wf := range versions {
		out = append(out, wf.GetVersion())
	}
	return out
}

// objectModified reports one object's LastModified, which is what tells a
// rewritten object from an untouched one when the bytes are equal either way.
func objectModified(
	t *testing.T,
	ctx context.Context,
	client *s3.Client,
	bucket, key string,
) time.Time {
	t.Helper()
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("head %q: %v", key, err)
	}
	return aws.ToTime(head.LastModified)
}

// writePromotionProof records the concurrency transcript when the milestone's
// proof asks for it (ORK_PROOF_DIR); a plain test run writes nothing into the
// tree.
func writePromotionProof(t *testing.T, name, final string, landed, refused int, seen map[string]int) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "# concurrent promotion against one MinIO bucket (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&out, "# generated by: go test -tags=integration ./pkg/artifacts/ "+
		"-run TestS3OrkflowStore_ConcurrentPromotionsConvergeOnOneExistingVersion\n\n")
	fmt.Fprintf(&out, "orkflow: %s\n", name)
	fmt.Fprintf(&out, "promotions: %d landed, %d reported a lost race\n", landed, refused)
	fmt.Fprintf(&out, "reads taken during the race, by version observed: %v\n", seen)
	fmt.Fprintf(&out, "every observation named a stored version; the two stores agree "+
		"the current version is %s\n", final)
	path := filepath.Join(dir, "concurrent-promotion.txt")
	if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
		t.Fatalf("write promotion proof: %v", err)
	}
	t.Logf("wrote %s", path)
}
