//go:build integration

// Integration coverage for the S3 orkflow store: orkflow definitions live in
// the bucket, one immutable JSON object per version.
//
// Run against the repo's local infrastructure (`make infra-up`):
//
//	go test -tags=integration ./pkg/artifacts/... -count=1
//
// The tests skip (never fail) when MinIO is unreachable.
package artifacts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// newS3OrkflowStore builds the orkflow store under test, skipping when the dev
// MinIO is not running.
func newS3OrkflowStore(t *testing.T) (*artifacts.S3OrkflowStore, artifacts.Config) {
	t.Helper()
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	store, err := artifacts.NewS3OrkflowStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore: %v", err)
	}
	return store, cfg
}

// testOrkflow is a compiled orkflow shaped like internal/dsl output.
func testOrkflow(t *testing.T, name, version string, appliedAt time.Time) *orkstackv1.Orkflow {
	t.Helper()
	with, err := structpb.NewStruct(map[string]any{
		"op":              "get",
		"url":             "{{ .inputs.url }}",
		"capture_body_as": "body",
	})
	if err != nil {
		t.Fatalf("with: %v", err)
	}
	return &orkstackv1.Orkflow{
		Name:        name,
		Version:     version,
		Description: "M7.5 durable orkflow store",
		Tasks: []*orkstackv1.Task{{
			Id:             "fetch",
			Plugin:         "http",
			With:           with,
			TimeoutSeconds: 300,
		}},
		SourceYaml: fmt.Sprintf("name: %s\nversion: %q\n", name, version),
		CreatedAt:  timestamppb.New(appliedAt),
	}
}

// testOrkflowName is a slug unique to one test run, so repeated runs against
// the shared dev bucket never collide.
func testOrkflowName(t *testing.T) string {
	t.Helper()
	return "m75-" + uniqueToken(t)
}

// sortedKeys is every key under one prefix, in a stable order, so an assertion
// can name the whole set an operation is expected to have left behind.
func sortedKeys(t *testing.T, ctx context.Context, client *s3.Client, bucket, prefix string) []string {
	t.Helper()
	keys := listKeys(t, ctx, client, bucket, prefix)
	sort.Strings(keys)
	return keys
}

func readKey(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string) []byte {
	t.Helper()
	object, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("raw get %q: %v", key, err)
	}
	defer object.Body.Close()
	body, err := io.ReadAll(object.Body)
	if err != nil {
		t.Fatalf("raw read %q: %v", key, err)
	}
	return body
}

// TestS3OrkflowStore_WritesEachVersionToItsOwnImmutableObject is the milestone's
// storage claim: an applied orkflow becomes
// s3://orkstack-artifacts/metadata/orkflows/{namespace}/{name}/{version}.json, and
// publishing another version adds an object rather than rewriting one.
func TestS3OrkflowStore_WritesEachVersionToItsOwnImmutableObject(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	applied := time.Now().UTC().Truncate(time.Millisecond)
	first := testOrkflow(t, name, "0.1.0", applied)
	if _, _, err := store.Put(ctx, first, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put(0.1.0): %v", err)
	}

	// The key is the documented path, and the URI names it exactly. The head
	// pointer sits beside it: the head is a committed pointer, not a comparison
	// of the documents' own timestamps (pkg/artifacts/orkflow_head.go).
	firstKey := fmt.Sprintf("metadata/orkflows/%s/%s/0.1.0.json", artifacts.DefaultNamespace, name)
	headKey := fmt.Sprintf("metadata/orkflows/%s/%s/@head.json", artifacts.DefaultNamespace, name)
	if got, want := sortedKeys(t, ctx, raw, cfg.S3Bucket, prefix),
		[]string{firstKey, headKey}; !slices.Equal(got, want) {
		t.Fatalf("bucket keys = %v, want %v", got, want)
	}
	wantURI := "s3://" + cfg.S3Bucket + "/" + firstKey
	gotURI, err := store.URI(artifacts.DefaultNamespace, name, "0.1.0")
	if err != nil {
		t.Fatalf("URI: %v", err)
	}
	if gotURI != wantURI {
		t.Fatalf("URI = %q, want %q", gotURI, wantURI)
	}

	// The object is JSON a person (or another tool) can read.
	document := readKey(t, ctx, raw, cfg.S3Bucket, firstKey)
	var decoded map[string]any
	if err := json.Unmarshal(document, &decoded); err != nil {
		t.Fatalf("stored document is not JSON: %v\n%s", err, document)
	}
	if decoded["name"] != name || decoded["version"] != "0.1.0" {
		t.Fatalf("stored document names %v@%v, want %s@0.1.0", decoded["name"], decoded["version"], name)
	}

	// Publishing a second version adds an object; the first one's bytes are
	// exactly as they were.
	second := testOrkflow(t, name, "0.2.0", applied.Add(time.Second))
	second.Description = "second version, different content"
	if _, _, err := store.Put(ctx, second, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put(0.2.0): %v", err)
	}
	secondKey := fmt.Sprintf("metadata/orkflows/%s/%s/0.2.0.json", artifacts.DefaultNamespace, name)
	if got, want := sortedKeys(t, ctx, raw, cfg.S3Bucket, prefix),
		[]string{firstKey, secondKey, headKey}; !slices.Equal(got, want) {
		t.Fatalf("bucket keys = %v, want %v", got, want)
	}
	if after := readKey(t, ctx, raw, cfg.S3Bucket, firstKey); !bytes.Equal(document, after) {
		t.Fatalf("publishing 0.2.0 rewrote 0.1.0:\nbefore:\n%s\nafter:\n%s", document, after)
	}

	// Re-applying the SAME definition writes nothing: same object, same bytes,
	// same created_at, and the call says it created nothing.
	restored, created, err := store.Put(ctx, first, artifacts.NoHeadExpectation())
	if err != nil {
		t.Fatalf("Put(0.1.0) again: %v", err)
	}
	if created {
		t.Error("re-applying an existing version reported that it created it")
	}
	if restored.GetCreatedAt().AsTime() != first.GetCreatedAt().AsTime() {
		t.Errorf("re-applying returned created_at %s, want the stored %s",
			restored.GetCreatedAt().AsTime(), first.GetCreatedAt().AsTime())
	}
	if again := readKey(t, ctx, raw, cfg.S3Bucket, firstKey); !bytes.Equal(document, again) {
		t.Fatalf("re-applying the same definition changed its object:\nbefore:\n%s\nafter:\n%s",
			document, again)
	}

	// A DIFFERENT definition under a stored version is refused. A version is
	// immutable: a run pinned to 0.1.0 reads this object long after the apply,
	// so rewriting it would change what an already-started run executes.
	amended := testOrkflow(t, name, "0.1.0", applied)
	amended.Description = "amended in place"
	if _, _, err := store.Put(ctx, amended, artifacts.NoHeadExpectation()); !errors.Is(err, artifacts.ErrVersionExists) {
		t.Fatalf("Put(0.1.0) amended = %v, want artifacts.ErrVersionExists", err)
	}
	if got := readKey(t, ctx, raw, cfg.S3Bucket, firstKey); !bytes.Equal(document, got) {
		t.Fatalf("a refused amendment still changed the object:\nbefore:\n%s\nafter:\n%s",
			document, got)
	}
	if got := string(readKey(t, ctx, raw, cfg.S3Bucket, secondKey)); !strings.Contains(got, "second version") {
		t.Fatalf("re-applying 0.1.0 disturbed 0.2.0:\n%s", got)
	}

	// Reads: an exact version, and "latest" meaning the most recently applied.
	pinned, err := store.Get(ctx, artifacts.DefaultNamespace, name, "0.1.0")
	if err != nil {
		t.Fatalf("Get(0.1.0): %v", err)
	}
	if pinned.GetDescription() != "M7.5 durable orkflow store" {
		t.Errorf("Get(0.1.0) description = %q, want the first version's", pinned.GetDescription())
	}
	if len(pinned.GetTasks()) != 1 || pinned.GetTasks()[0].GetWith().AsMap()["url"] != "{{ .inputs.url }}" {
		t.Errorf("Get(0.1.0) lost the raw task template: %v", pinned.GetTasks())
	}
	latest, err := store.Get(ctx, artifacts.DefaultNamespace, name, "")
	if err != nil {
		t.Fatalf("Get(latest): %v", err)
	}
	if latest.GetVersion() != "0.2.0" {
		t.Errorf("Get(latest) = %q, want 0.2.0", latest.GetVersion())
	}

	// List reports one entry per orkflow: the latest version.
	listed, err := store.List(ctx, artifacts.DefaultNamespace)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	seen := 0
	for _, wf := range listed {
		if wf.GetName() != name {
			continue
		}
		seen++
		if wf.GetVersion() != "0.2.0" {
			t.Errorf("List reports %s@%s, want 0.2.0", wf.GetName(), wf.GetVersion())
		}
	}
	if seen != 1 {
		t.Errorf("List reports %s %d times, want once", name, seen)
	}

	writeOrkflowStoreProof(t, cfg.S3Bucket, map[string][]byte{
		firstKey:  document,
		secondKey: readKey(t, ctx, raw, cfg.S3Bucket, secondKey),
	})
}

// TestS3OrkflowStore_OutlivesTheStoreThatWroteIt is the restart claim at
// library level: a second store, built fresh against the same bucket the way a
// restarted engine builds one, reads what the first store wrote.
func TestS3OrkflowStore_OutlivesTheStoreThatWroteIt(t *testing.T) {
	writer, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket,
			artifacts.OrkflowPrefix+"/"+artifacts.DefaultNamespace+"/"+name+"/")
	})

	wf := testOrkflow(t, name, "1.0.0", time.Now().UTC())
	if _, _, err := writer.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A fresh store with its own client: no state carries over from the writer.
	reader, err := artifacts.NewS3OrkflowStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore (restart): %v", err)
	}
	got, err := reader.Get(ctx, artifacts.DefaultNamespace, name, "")
	if err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
	if got.GetVersion() != "1.0.0" || got.GetSourceYaml() != wf.GetSourceYaml() {
		t.Fatalf("restarted store read %s@%s (source %q), want the applied definition",
			got.GetName(), got.GetVersion(), got.GetSourceYaml())
	}
	listed, err := reader.List(ctx, artifacts.DefaultNamespace)
	if err != nil {
		t.Fatalf("List after restart: %v", err)
	}
	found := false
	for _, candidate := range listed {
		if candidate.GetName() == name {
			found = true
		}
	}
	if !found {
		t.Fatalf("restarted store does not list %q", name)
	}
	if location := reader.Location(); location != "s3://"+cfg.S3Bucket+"/metadata/orkflows/" {
		t.Errorf("Location = %q, want the metadata/orkflows prefix", location)
	}
}

func TestS3OrkflowStore_ReportsAMissingOrkflowAsNotFound(t *testing.T) {
	store, _ := newS3OrkflowStore(t)
	ctx := context.Background()

	if _, err := store.Get(ctx, artifacts.DefaultNamespace, testOrkflowName(t), ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get(unknown, latest) = %v, want ErrNotFound", err)
	}
	if _, err := store.Get(ctx, artifacts.DefaultNamespace, testOrkflowName(t), "0.1.0"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get(unknown, 0.1.0) = %v, want ErrNotFound", err)
	}
	if _, err := store.Get(ctx, artifacts.DefaultNamespace, "../../etc/passwd", "0.1.0"); !errors.Is(err, artifacts.ErrInvalidOrkflow) {
		t.Errorf("Get(traversal) = %v, want ErrInvalidOrkflow", err)
	}
	if _, _, err := store.Put(ctx, &orkstackv1.Orkflow{Name: "no-version"}, artifacts.NoHeadExpectation()); !errors.Is(err, artifacts.ErrInvalidOrkflow) {
		t.Errorf("Put(no version) = %v, want ErrInvalidOrkflow", err)
	}
}

// TestOrkflowStore_TwoNamespacesHoldTheSameNameIndependently is M10.1's
// storage isolation canary: the namespace is part of the object key, not a
// label filtered after reading a global catalog.
func TestOrkflowStore_TwoNamespacesHoldTheSameNameIndependently(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	token := uniqueToken(t)
	nsA, nsB := "m101-a-"+token, "m101-b-"+token
	t.Cleanup(func() {
		for _, namespace := range []string{nsA, nsB} {
			deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket,
				artifacts.OrkflowPrefix+"/"+namespace+"/")
		}
	})

	inA := testOrkflow(t, name, "1.0.0", time.Now().UTC())
	inA.Namespace = nsA
	inA.SourceYaml = "namespace: " + nsA + "\nname: " + name + "\n# A\n"
	inB := testOrkflow(t, name, "1.0.0", time.Now().UTC().Add(time.Second))
	inB.Namespace = nsB
	inB.SourceYaml = "namespace: " + nsB + "\nname: " + name + "\n# B\n"
	for _, wf := range []*orkstackv1.Orkflow{inA, inB} {
		if _, _, err := store.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s): %v", wf.GetNamespace(), err)
		}
	}

	for _, tc := range []struct {
		namespace string
		want      *orkstackv1.Orkflow
	}{{nsA, inA}, {nsB, inB}} {
		key := fmt.Sprintf("metadata/orkflows/%s/%s/1.0.0.json", tc.namespace, name)
		head := fmt.Sprintf("metadata/orkflows/%s/%s/@head.json", tc.namespace, name)
		keys := sortedKeys(t, ctx, raw, cfg.S3Bucket,
			artifacts.OrkflowPrefix+"/"+tc.namespace+"/")
		if want := []string{key, head}; !slices.Equal(keys, want) {
			t.Fatalf("keys for %s = %v, want %v", tc.namespace, keys, want)
		}
		got, err := store.Get(ctx, tc.namespace, name, "")
		if err != nil {
			t.Fatalf("Get(%s): %v", tc.namespace, err)
		}
		if got.GetSourceYaml() != tc.want.GetSourceYaml() {
			t.Errorf("Get(%s) returned %q, want its own document", tc.namespace, got.GetSourceYaml())
		}
		listed, err := store.List(ctx, tc.namespace)
		if err != nil {
			t.Fatalf("List(%s): %v", tc.namespace, err)
		}
		if len(listed) != 1 || listed[0].GetSourceYaml() != tc.want.GetSourceYaml() {
			t.Errorf("List(%s) = %v, want its one document", tc.namespace, listed)
		}
	}
	if _, err := store.Get(ctx, "m101-empty-"+token, name, ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get from an unused namespace = %v, want ErrNotFound", err)
	}
}

// writeOrkflowStoreProof records the real objects this test wrote, so the
// milestone's proof directory holds samples produced by the store itself.
func writeOrkflowStoreProof(t *testing.T, bucket string, documents map[string][]byte) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_ORKFLOW_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	keys := make([]string, 0, len(documents))
	for key := range documents {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var index strings.Builder
	fmt.Fprintf(&index, "# orkflow definitions written by the S3 orkflow store (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&index, "# generated by: go test -tags=integration ./pkg/artifacts/ -run S3OrkflowStore\n\n")
	for i, key := range keys {
		fmt.Fprintf(&index, "s3://%s/%s\n", bucket, key)
		file := filepath.Join(dir, fmt.Sprintf("orkflow-version-%d.json", i+1))
		if err := os.WriteFile(file, documents[key], 0o644); err != nil {
			t.Fatalf("write proof document: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "orkflow-uris.txt"), []byte(index.String()), 0o644); err != nil {
		t.Fatalf("write proof index: %v", err)
	}
	t.Logf("wrote orkflow store proof to %s", dir)
}

// TestS3OrkflowStore_TheLastApplyIsTheHeadWhateverTheWriterClocksSay is the
// property a derived head cannot have.
//
// Every API replica stamps created_at from its own clock
// (internal/api.Service.now), so a head chosen as "the greatest stamp" is
// chosen by whichever replica's clock runs fastest. Here a replica an HOUR
// behind applies last, exactly as a machine with a drifting clock would: the
// version it applied is the head, its stamp reads after the head it extended,
// and the catalog's "newest first" order agrees with both.
//
// The two stores are two clients on one bucket, which is what two replicas are.
func TestS3OrkflowStore_TheLastApplyIsTheHeadWhateverTheWriterClocksSay(t *testing.T) {
	ahead, cfg := newS3OrkflowStore(t)
	behind, err := artifacts.NewS3OrkflowStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore (second replica): %v", err)
	}
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	fast := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	slow := fast.Add(-time.Hour)
	for _, version := range []string{"1.0.0", "2.0.0"} {
		if _, _, err := ahead.Put(ctx, testOrkflow(t, name, version, fast),
			artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s) from the fast replica: %v", version, err)
		}
	}

	// The lagging replica applies last, believing it is an hour earlier.
	fork := testOrkflow(t, name, "1.1.0", slow)
	fork.Description = "applied by a replica whose clock is an hour behind"
	if _, _, err := behind.Put(ctx, fork, artifacts.ExpectHead("2.0.0")); err != nil {
		t.Fatalf("Put(1.1.0) from the lagging replica: %v", err)
	}

	// Read through the OTHER store, so the answer is the bucket's and not one
	// client's memory of what it wrote.
	versions, err := ahead.Versions(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if versions.Head != "1.1.0" {
		t.Errorf("head = %q, want 1.1.0 — the version applied last", versions.Head)
	}
	if len(versions.All) != 3 || versions.All[0].GetVersion() != "1.1.0" {
		t.Errorf("versions newest-first = %v, want 1.1.0 first", versionNumbers(versions.All))
	}
	if stamp := versions.All[0].GetCreatedAt().AsTime(); !stamp.After(fast) {
		t.Errorf("the last apply is stamped %s, which is not after the head it extended (%s); "+
			"a catalog ordered by stamp would put it in the wrong place", stamp, fast)
	}
	// And what an empty version resolves to follows the head, because nobody
	// promoted anything.
	resolved, err := ahead.Get(ctx, artifacts.DefaultNamespace, name, "")
	if err != nil {
		t.Fatalf("Get(latest): %v", err)
	}
	if resolved.GetVersion() != "1.1.0" {
		t.Errorf("Get(latest) = %q, want 1.1.0", resolved.GetVersion())
	}
}

// TestS3OrkflowStore_AnApplyWhoseExpectedHeadMovedIsRefused is the durable half
// of the edit contract: the expected head is a condition ON THE WRITE, arbitrated
// by the object store's own conditional PUT, so an edit written against a head
// that has since moved stores nothing at all.
func TestS3OrkflowStore_AnApplyWhoseExpectedHeadMovedIsRefused(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	applied := time.Now().UTC().Truncate(time.Millisecond)
	for i, version := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		if _, _, err := store.Put(ctx, testOrkflow(t, name, version, applied.Add(time.Duration(i)*time.Second)),
			artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s): %v", version, err)
		}
	}

	stale := testOrkflow(t, name, "9.0.0", applied.Add(time.Minute))
	_, _, err := store.Put(ctx, stale, artifacts.ExpectHead("2.0.0"))
	if !errors.Is(err, artifacts.ErrHeadMoved) {
		t.Fatalf("Put with a stale expected head = %v, want artifacts.ErrHeadMoved", err)
	}
	var moved *artifacts.HeadMovedError
	if !errors.As(err, &moved) || moved.Expected != "2.0.0" || moved.Head != "3.0.0" {
		t.Fatalf("the refusal = %v, want one naming expected 2.0.0 and head 3.0.0", err)
	}
	if _, err := store.Get(ctx, artifacts.DefaultNamespace, name, "9.0.0"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("a refused apply stored its version anyway: %v", err)
	}

	// Re-read the head and the same document lands, with nothing else changed.
	if _, created, err := store.Put(ctx, stale, artifacts.ExpectHead("3.0.0")); err != nil || !created {
		t.Fatalf("the retargeted apply = created %v, %v; want created and no error", created, err)
	}
	versions, err := store.Versions(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if got, want := versionNumbers(versions.All), "9.0.0,3.0.0,2.0.0,1.0.0"; got != want {
		t.Errorf("versions = %s, want %s", got, want)
	}
	if versions.Head != "9.0.0" {
		t.Errorf("head = %q, want 9.0.0", versions.Head)
	}
}

// versionNumbers renders a catalog's versions in the order it is in.
func versionNumbers(versions []*orkstackv1.Orkflow) string {
	numbers := make([]string, 0, len(versions))
	for _, wf := range versions {
		numbers = append(numbers, wf.GetVersion())
	}
	return strings.Join(numbers, ",")
}
