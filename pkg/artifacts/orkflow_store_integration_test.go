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
	if _, _, err := store.Put(ctx, first); err != nil {
		t.Fatalf("Put(0.1.0): %v", err)
	}

	// The key is the documented path, and the URI names it exactly.
	firstKey := fmt.Sprintf("metadata/orkflows/%s/%s/0.1.0.json", artifacts.DefaultNamespace, name)
	if got := listKeys(t, ctx, raw, cfg.S3Bucket, prefix); len(got) != 1 || got[0] != firstKey {
		t.Fatalf("bucket keys = %v, want [%s]", got, firstKey)
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
	if _, _, err := store.Put(ctx, second); err != nil {
		t.Fatalf("Put(0.2.0): %v", err)
	}
	secondKey := fmt.Sprintf("metadata/orkflows/%s/%s/0.2.0.json", artifacts.DefaultNamespace, name)
	keys := listKeys(t, ctx, raw, cfg.S3Bucket, prefix)
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != firstKey || keys[1] != secondKey {
		t.Fatalf("bucket keys = %v, want [%s %s]", keys, firstKey, secondKey)
	}
	if after := readKey(t, ctx, raw, cfg.S3Bucket, firstKey); !bytes.Equal(document, after) {
		t.Fatalf("publishing 0.2.0 rewrote 0.1.0:\nbefore:\n%s\nafter:\n%s", document, after)
	}

	// Re-applying the SAME definition writes nothing: same object, same bytes,
	// same created_at, and the call says it created nothing.
	restored, created, err := store.Put(ctx, first)
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
	if _, _, err := store.Put(ctx, amended); !errors.Is(err, artifacts.ErrVersionExists) {
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
	if _, _, err := writer.Put(ctx, wf); err != nil {
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
	if _, _, err := store.Put(ctx, &orkstackv1.Orkflow{Name: "no-version"}); !errors.Is(err, artifacts.ErrInvalidOrkflow) {
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
		if _, _, err := store.Put(ctx, wf); err != nil {
			t.Fatalf("Put(%s): %v", wf.GetNamespace(), err)
		}
	}

	for _, tc := range []struct {
		namespace string
		want      *orkstackv1.Orkflow
	}{{nsA, inA}, {nsB, inB}} {
		key := fmt.Sprintf("metadata/orkflows/%s/%s/1.0.0.json", tc.namespace, name)
		keys := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.OrkflowPrefix+"/"+tc.namespace+"/")
		if len(keys) != 1 || keys[0] != key {
			t.Fatalf("keys for %s = %v, want [%s]", tc.namespace, keys, key)
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
