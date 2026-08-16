//go:build integration

// Package artifacts_test integration coverage for the s3 provider.
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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	minioHealthURL = "/minio/health/live"
	skipReason     = "MinIO not reachable at %s (%v); run `make infra-up`"
)

// s3TestConfig mirrors deploy/docker-compose.yml, overridable via ORK_S3_* env.
func s3TestConfig() artifacts.Config {
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	return cfg
}

func requireMinIO(t *testing.T, cfg artifacts.Config) {
	t.Helper()
	base, err := url.Parse(cfg.S3Endpoint)
	if err != nil {
		t.Skipf(skipReason, cfg.S3Endpoint, err)
	}
	health := base.JoinPath(minioHealthURL).String()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, health, nil)
	if err != nil {
		t.Skipf(skipReason, health, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf(skipReason, health, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Skipf(skipReason, health, fmt.Errorf("health status %d", resp.StatusCode))
	}
	t.Logf("MinIO health %s -> %d", health, resp.StatusCode)
}

func newS3Store(t *testing.T) (artifacts.Store, artifacts.Config) {
	t.Helper()
	cfg := s3TestConfig()
	requireMinIO(t, cfg)

	st, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("New(s3): %v", err)
	}
	return st, cfg
}

// uniqueToken keeps concurrent/repeated runs from colliding in the shared bucket.
func uniqueToken(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// uniquePrefix is a logical-name prefix unique to one test.
func uniquePrefix(t *testing.T) string {
	t.Helper()
	return "it/" + uniqueToken(t)
}

// testRun invents run coordinates shaped like the real ones: orkflow names and
// task ids are slugs (internal/dsl), run ids are ork-{timestamp}-{sequence}.
func testRun(t *testing.T, orkflow, taskID string) artifacts.Provenance {
	t.Helper()
	return artifacts.Provenance{
		Orkflow: orkflow,
		TaskID:  taskID,
		RunID:   fmt.Sprintf("ork-%d-%s", time.Now().UnixNano(), uniqueToken(t)[:4]),
	}
}

func putS3(
	t *testing.T,
	st artifacts.Store,
	prov artifacts.Provenance,
	name string,
	data []byte,
	opts artifacts.PutOptions,
) artifacts.ArtifactRef {
	t.Helper()
	opts.Metadata = prov.Metadata(opts.Metadata)
	ref, err := st.Put(context.Background(), name, nonSeekableReader{bytes.NewReader(data)}, opts)
	if err != nil {
		t.Fatalf("Put(%q): %v", name, err)
	}
	t.Cleanup(func() {
		if err := st.Delete(context.Background(), ref.URI); err != nil {
			t.Logf("cleanup Delete(%s): %v", ref.URI, err)
		}
	})
	return ref
}

// cleanSegmentForTest mirrors the documented layout rule, so the expected URI
// below is derived from the spec rather than from the store's own helpers.
var cleanSegmentForTest = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,55}$`)

// s3URIForTest builds the URI M7.4 promises, independently of the store:
//
//	s3://{bucket}/orkflows/{orkflow-name}/{run-id}/{task-id}/{artifact-name}
func s3URIForTest(t *testing.T, bucket string, prov artifacts.Provenance, name string) string {
	t.Helper()
	for _, segment := range []string{prov.Orkflow, prov.RunID, prov.TaskID, name} {
		if !cleanSegmentForTest.MatchString(segment) {
			t.Fatalf("s3URIForTest: %q cannot appear verbatim in a key", segment)
		}
	}
	key := path.Join("orkflows", prov.Orkflow, prov.RunID, prov.TaskID, name)
	return (&url.URL{Scheme: "s3", Host: bucket, Path: "/" + key}).String()
}

// runPrefixForTest is the URI prefix every artifact of one task run shares.
func runPrefixForTest(bucket string, prov artifacts.Provenance) string {
	return fmt.Sprintf("s3://%s/orkflows/%s/%s/%s/", bucket, prov.Orkflow, prov.RunID, prov.TaskID)
}

// recordProofURI appends a sample to the committed M7.4 proof when
// ORK_PROOF_M74_DIR is set.
func recordProofURI(t *testing.T, label string, ref artifacts.ArtifactRef) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_M74_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	line := fmt.Sprintf("%-34s %s\n", label, ref.URI)
	f, err := os.OpenFile(filepath.Join(dir, "artifact-uris.txt"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open proof file: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString(line); err != nil {
		t.Fatalf("write proof file: %v", err)
	}
}

// M7.4 acceptance 1: a run's artifact is addressed by the run that produced it.
func TestS3Store_PutsRunArtifactsUnderOrkflowTaskAndRun(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "hello-ork", "fetch")
	data := []byte(`{"slideshow": {"title": "Sample Slide Show"}}`)

	ref := putS3(t, st, prov, "body", data, artifacts.PutOptions{ContentType: "application/json"})

	want := s3URIForTest(t, cfg.S3Bucket, prov, "body")
	if ref.URI != want {
		t.Fatalf("URI = %q, want %q", ref.URI, want)
	}
	if strings.Contains(ref.URI, ref.SHA256) {
		t.Errorf("URI %q still carries the content digest", ref.URI)
	}
	t.Logf("put uri=%s size=%d sha256=%s", ref.URI, ref.SizeBytes, ref.SHA256)
	recordProofURI(t, "acceptance-1 run artifact", ref)

	// Every operation addresses the artifact by that readable URI.
	if got := readAllS3(t, ctx, st, ref.URI); !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}
	stat, err := st.Stat(ctx, ref.URI)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Name != "body" || stat.SHA256 != hexSHA(data) || stat.SizeBytes != int64(len(data)) {
		t.Errorf("Stat = %+v, want name body, sha %s, size %d", stat, hexSHA(data), len(data))
	}
	// The object states its own provenance, and the store's reserved key never
	// leaks into a reference.
	if stat.Metadata["orkflow"] != prov.Orkflow ||
		stat.Metadata["task_id"] != prov.TaskID ||
		stat.Metadata["run_id"] != prov.RunID {
		t.Errorf("Stat Metadata = %v, want the run coordinates", stat.Metadata)
	}
	for key := range stat.Metadata {
		if strings.HasPrefix(strings.ToLower(key), "ork-content-") {
			t.Errorf("Stat Metadata leaks the store's reserved key %q: %v", key, stat.Metadata)
		}
	}

	// A second task of the same run sits beside it, under the same orkflow.
	saveRef := putS3(t, st, artifacts.Provenance{
		Orkflow: prov.Orkflow, TaskID: "save", RunID: prov.RunID,
	}, "result.txt", []byte("Sample Slide Show"), artifacts.PutOptions{})
	recordProofURI(t, "acceptance-1 sibling task", saveRef)
	orkflowPrefix := fmt.Sprintf("s3://%s/orkflows/%s/", cfg.S3Bucket, prov.Orkflow)
	for _, r := range []artifacts.ArtifactRef{ref, saveRef} {
		if !strings.HasPrefix(r.URI, orkflowPrefix) {
			t.Errorf("URI %q is not under the orkflow prefix %q", r.URI, orkflowPrefix)
		}
	}
}

// M7.4 acceptance 3: an S3 lifecycle rule scoped to one orkflow selects that
// orkflow's artifacts and nothing else.
func TestS3Store_LifecycleRuleExpiresExactlyOneOrkflow(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)

	const (
		deprecated       = "deprecated-flow"
		kept             = "kept-flow"
		artifactPrefix   = "orkflows/" + deprecated + "/"
		manifestPrefix   = "manifests/orkflows/" + deprecated + "/"
		lifecycleRuleIDs = "expire-deprecated-flow"
	)
	// Start from a clean slate so the assertions below describe this run only.
	deleteByPrefix(t, ctx, raw, cfg.S3Bucket, artifactPrefix)
	deleteByPrefix(t, ctx, raw, cfg.S3Bucket, manifestPrefix)

	fetchRun := artifacts.Provenance{Orkflow: deprecated, TaskID: "fetch", RunID: "ork-1755302400000000001-1"}
	saveRun := artifacts.Provenance{Orkflow: deprecated, TaskID: "save", RunID: "ork-1755302400000000001-1"}
	laterRun := artifacts.Provenance{Orkflow: deprecated, TaskID: "save", RunID: "ork-1755302400000000002-1"}
	keptRun := testRun(t, kept, "save")

	doomed := []artifacts.ArtifactRef{
		putS3(t, st, fetchRun, "body", []byte("deprecated body"), artifacts.PutOptions{}),
		putS3(t, st, saveRun, "result.txt", []byte("deprecated result"), artifacts.PutOptions{}),
		putS3(t, st, laterRun, "result.txt", []byte("deprecated result, later run"), artifacts.PutOptions{}),
		// An opaque name keeps a companion manifest; the manifest rule must
		// sweep that up too, or expiring an orkflow leaves debris behind.
		putS3(t, st, saveRun, "nested/report copy.txt", []byte("deprecated nested"), artifacts.PutOptions{}),
	}
	survivor := putS3(t, st, keptRun, "result.txt", []byte("kept result"), artifacts.PutOptions{})
	for _, ref := range doomed {
		recordProofURI(t, "acceptance-3 deprecated-flow", ref)
	}
	recordProofURI(t, "acceptance-3 kept-flow", survivor)

	// 1. The policy an operator would write, applied to the real server.
	policy := &s3types.BucketLifecycleConfiguration{Rules: []s3types.LifecycleRule{
		{
			ID:         aws.String(lifecycleRuleIDs),
			Status:     s3types.ExpirationStatusEnabled,
			Filter:     &s3types.LifecycleRuleFilter{Prefix: aws.String(artifactPrefix)},
			Expiration: &s3types.LifecycleExpiration{Days: aws.Int32(1)},
		},
		{
			ID:         aws.String(lifecycleRuleIDs + "-manifests"),
			Status:     s3types.ExpirationStatusEnabled,
			Filter:     &s3types.LifecycleRuleFilter{Prefix: aws.String(manifestPrefix)},
			Expiration: &s3types.LifecycleExpiration{Days: aws.Int32(1)},
		},
	}}
	if _, err := raw.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(cfg.S3Bucket),
		LifecycleConfiguration: policy,
	}); err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration: %v", err)
	}
	t.Cleanup(func() {
		if _, err := raw.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(cfg.S3Bucket),
		}); err != nil {
			t.Logf("cleanup DeleteBucketLifecycle: %v", err)
		}
	})

	stored, err := raw.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(cfg.S3Bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLifecycleConfiguration: %v", err)
	}
	gotPrefixes := map[string]bool{}
	for _, rule := range stored.Rules {
		gotPrefixes[aws.ToString(rule.Filter.Prefix)] = rule.Status == s3types.ExpirationStatusEnabled
	}
	for _, prefix := range []string{artifactPrefix, manifestPrefix} {
		if !gotPrefixes[prefix] {
			t.Fatalf("server stored rules %v, want an enabled rule for %q", gotPrefixes, prefix)
		}
	}
	t.Logf("lifecycle policy accepted by the server: %v", gotPrefixes)

	// 2. The rule's prefix selects exactly the deprecated orkflow's objects.
	selected := listKeys(t, ctx, raw, cfg.S3Bucket, artifactPrefix)
	selected = append(selected, listKeys(t, ctx, raw, cfg.S3Bucket, manifestPrefix)...)
	sort.Strings(selected)
	wantSelected := []string{
		"manifests/orkflows/deprecated-flow/ork-1755302400000000001-1/save/nested-report-copy.txt~9031737d4770a2f1",
		"orkflows/deprecated-flow/ork-1755302400000000001-1/fetch/body",
		"orkflows/deprecated-flow/ork-1755302400000000001-1/save/nested-report-copy.txt~9031737d4770a2f1",
		"orkflows/deprecated-flow/ork-1755302400000000001-1/save/result.txt",
		"orkflows/deprecated-flow/ork-1755302400000000002-1/save/result.txt",
	}
	if strings.Join(selected, "\n") != strings.Join(wantSelected, "\n") {
		t.Fatalf("prefix %q selected\n%s\nwant\n%s", artifactPrefix,
			strings.Join(selected, "\n"), strings.Join(wantSelected, "\n"))
	}
	if survivorKey := strings.TrimPrefix(survivor.URI, "s3://"+cfg.S3Bucket+"/"); contains(selected, survivorKey) {
		t.Fatalf("the rule for %q also selected %q", deprecated, survivorKey)
	}
	recordProofLines(t, "lifecycle-selection.txt", append(
		[]string{fmt.Sprintf("# objects an S3 lifecycle rule for s3://%s/%s** expires", cfg.S3Bucket, artifactPrefix)},
		selected...))

	// 3. Applying what the rule describes removes that orkflow and only it.
	deleteByPrefix(t, ctx, raw, cfg.S3Bucket, artifactPrefix)
	deleteByPrefix(t, ctx, raw, cfg.S3Bucket, manifestPrefix)

	for _, ref := range doomed {
		if _, err := st.Get(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("Get(%s) after expiry err = %v, want ErrNotFound", ref.URI, err)
		}
		if _, err := st.Stat(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("Stat(%s) after expiry err = %v, want ErrNotFound", ref.URI, err)
		}
	}
	if left := listKeys(t, ctx, raw, cfg.S3Bucket, "manifests/orkflows/"+deprecated+"/"); len(left) != 0 {
		t.Errorf("expiring %q left %v behind", deprecated, left)
	}
	if got := readAllS3(t, ctx, st, survivor.URI); string(got) != "kept result" {
		t.Errorf("kept orkflow artifact = %q, want it untouched", got)
	}
}

func TestS3Store_PutGetRoundTripIsByteIdentical(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "roundtrip-flow", "emit")
	data := bytes.Repeat([]byte("orkstack-s3-roundtrip\n"), 5000)

	ref := putS3(t, st, prov, "roundtrip.txt", data, artifacts.PutOptions{
		ContentType: "text/plain; charset=utf-8",
		Metadata:    map[string]string{"milestone": "m1"},
	})

	wantURI := s3URIForTest(t, cfg.S3Bucket, prov, "roundtrip.txt")
	if ref.URI != wantURI {
		t.Errorf("URI = %q, want %q", ref.URI, wantURI)
	}
	if ref.SHA256 != hexSHA(data) {
		t.Errorf("SHA256 = %q, want %q", ref.SHA256, hexSHA(data))
	}
	if ref.SizeBytes != int64(len(data)) {
		t.Errorf("SizeBytes = %d, want %d", ref.SizeBytes, len(data))
	}
	t.Logf("put uri=%s size=%d sha256=%s content_type=%s", ref.URI, ref.SizeBytes, ref.SHA256, ref.ContentType)
	recordProofURI(t, "roundtrip", ref)

	rc, err := st.Get(ctx, ref.URI)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %d bytes, want %d identical bytes", len(got), len(data))
	}
	t.Logf("get bytes=%d sha256=%s (identical=%t)", len(got), hexSHA(got), bytes.Equal(got, data))

	stat, err := st.Stat(ctx, ref.URI)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.SHA256 != ref.SHA256 || stat.SizeBytes != ref.SizeBytes {
		t.Errorf("Stat = %+v, want sha %s size %d", stat, ref.SHA256, ref.SizeBytes)
	}
	if stat.ContentType != "text/plain; charset=utf-8" {
		t.Errorf("Stat ContentType = %q, want text/plain; charset=utf-8", stat.ContentType)
	}
	if stat.Metadata["milestone"] != "m1" {
		t.Errorf("Stat Metadata = %v, want milestone=m1", stat.Metadata)
	}
}

func TestS3Store_ListByPrefix(t *testing.T) {
	st, _ := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "list-flow", "emit")
	prefix := uniquePrefix(t)

	a := putS3(t, st, prov, prefix+"/dir-a/one.txt", []byte("one"), artifacts.PutOptions{})
	b := putS3(t, st, prov, prefix+"/dir-a/two.txt", []byte("two"), artifacts.PutOptions{})
	c := putS3(t, st, prov, prefix+"/dir-b/three.txt", []byte("three"), artifacts.PutOptions{})

	refs, err := st.List(ctx, prefix+"/dir-a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("List(dir-a) = %+v, want 2 entries", refs)
	}
	if !containsArtifactURI(refs, a.URI) || !containsArtifactURI(refs, b.URI) {
		t.Errorf("List(dir-a) = %+v, want refs %s and %s", refs, a.URI, b.URI)
	}
	for _, r := range refs {
		if r.SHA256 == "" || r.SizeBytes == 0 || r.Name == "" {
			t.Errorf("List entry incomplete: %+v", r)
		}
	}

	all, err := st.List(ctx, prefix+"/")
	if err != nil {
		t.Fatalf("List(all): %v", err)
	}
	if len(all) != 3 {
		t.Errorf("List(all) = %+v, want 3 entries including %s", all, c.URI)
	}

	none, err := st.List(ctx, prefix+"/missing/")
	if err != nil {
		t.Fatalf("List(missing): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("List(missing) = %+v, want none", none)
	}
}

func TestS3Store_DeleteRemovesObject(t *testing.T) {
	st, _ := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "delete-flow", "emit")
	name := uniquePrefix(t) + "/delete-me.txt"

	ref := putS3(t, st, prov, name, []byte("delete me"), artifacts.PutOptions{})

	if err := st.Delete(ctx, ref.URI); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Stat(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Stat after Delete err = %v, want ErrNotFound", err)
	}
	if _, err := st.Get(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get after Delete err = %v, want ErrNotFound", err)
	}
	refs, err := st.List(ctx, name)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("List after Delete = %+v, want empty", refs)
	}
	if err := st.Delete(ctx, ref.URI); err != nil {
		t.Errorf("second Delete: %v", err)
	}
}

func TestS3Store_PresignedGetURLReturnsBytes(t *testing.T) {
	st, _ := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "presign-flow", "emit")
	data := []byte("presigned-payload-" + strings.Repeat("z", 1024))

	ref := putS3(t, st, prov, "presign.txt", data, artifacts.PutOptions{ContentType: "text/plain"})

	signed, err := st.Presign(ctx, ref.URI, 5*time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	parsed, err := url.Parse(signed)
	if err != nil {
		t.Fatalf("parse presigned url: %v", err)
	}
	q := parsed.Query()
	for _, param := range []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Signature", "X-Amz-Expires"} {
		if q.Get(param) == "" {
			t.Errorf("presigned URL missing %s: %s", param, parsed.Path)
		}
	}
	// Log a redacted URL: signature material never goes into committed proof.
	t.Logf("presigned GET %s://%s%s?X-Amz-Expires=%s&X-Amz-Signature=<redacted>",
		parsed.Scheme, parsed.Host, parsed.Path, q.Get("X-Amz-Expires"))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signed, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch presigned URL: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read presigned body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned GET status = %d, want 200 (body %q)", resp.StatusCode, string(body))
	}
	if !bytes.Equal(body, data) {
		t.Fatalf("presigned GET returned %d bytes, want %d identical bytes", len(body), len(data))
	}
	t.Logf("presigned GET status=%d bytes=%d sha256=%s (identical=%t)",
		resp.StatusCode, len(body), hexSHA(body), bytes.Equal(body, data))
}

func TestS3Store_PreservesOpaqueNames(t *testing.T) {
	cases := []struct {
		label string
		name  string
	}{
		{label: "empty", name: ""},
		{label: "backslash", name: `\`},
		{label: "repeated separators", name: "a//b"},
		{label: "dot segment", name: "a/./b"},
		{label: "parent segment", name: "a/../b"},
		{label: "dot", name: "."},
		{label: "trailing slash", name: "a/"},
		{label: "absolute looking", name: "/abs"},
		{label: "reserved physical prefix", name: ".tmp/report.txt"},
		{label: "hash", name: "hash#tag.txt"},
		{label: "query", name: "query?a=1.txt"},
		{label: "percent", name: "pct%41.txt"},
		{label: "digest looking", name: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{label: "NUL", name: "nul\x00name"},
		{label: "invalid UTF-8", name: string([]byte{0xff, 0xfe, 'x'})},
		{label: "long", name: strings.Repeat("x", 400)},
		{label: "plain file", name: "report.txt"},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			st, cfg := newS3Store(t)
			ctx := context.Background()
			prov := testRun(t, "opaque-flow", "emit")
			data := []byte(uniquePrefix(t) + ": opaque s3 name: " + tc.name)
			opts := artifacts.PutOptions{Metadata: map[string]string{"case": tc.label}}

			ref := putS3(t, st, prov, tc.name, data, opts)
			if ref.Name != tc.name {
				t.Errorf("Put Name = %q, want exact %q", ref.Name, tc.name)
			}
			// However opaque the name, the run is still readable in the URI and
			// the artifact is still exactly one segment below it.
			runPrefix := runPrefixForTest(cfg.S3Bucket, prov)
			if !strings.HasPrefix(ref.URI, runPrefix) {
				t.Fatalf("Put URI = %q, want it under %q", ref.URI, runPrefix)
			}
			if segment := strings.TrimPrefix(ref.URI, runPrefix); segment == "" || strings.Contains(segment, "/") {
				t.Fatalf("artifact segment = %q, want exactly one non-empty segment", segment)
			}
			parsed, err := url.Parse(ref.URI)
			if err != nil || parsed.String() != ref.URI || parsed.RawQuery != "" || parsed.Fragment != "" {
				t.Fatalf("URI %q is not canonical and reversible: parsed=%+v err=%v", ref.URI, parsed, err)
			}

			rc, err := st.Get(ctx, ref.URI)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if err := rc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("Get returned %q, want %q", got, data)
			}

			stat, err := st.Stat(ctx, ref.URI)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if stat.Name != tc.name || stat.SHA256 != hexSHA(data) || stat.Metadata["case"] != tc.label {
				t.Errorf("Stat = %+v, want exact name %q, sha %s, metadata case=%q", stat, tc.name, hexSHA(data), tc.label)
			}

			refs, err := st.List(ctx, tc.name)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if !containsArtifactURI(refs, ref.URI) {
				t.Errorf("List(%q) = %+v, want ref %s", tc.name, refs, ref.URI)
			}

			signed, err := st.Presign(ctx, ref.URI, time.Minute)
			if err != nil {
				t.Fatalf("Presign: %v", err)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, signed, nil)
			if err != nil {
				t.Fatalf("new presigned request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("fetch presigned URL: %v", err)
			}
			body, readErr := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if readErr != nil {
				t.Fatalf("read presigned body: %v", readErr)
			}
			if closeErr != nil {
				t.Fatalf("close presigned body: %v", closeErr)
			}
			if resp.StatusCode != http.StatusOK || !bytes.Equal(body, data) {
				t.Fatalf("presigned GET status=%d body=%q, want 200 and %q", resp.StatusCode, body, data)
			}

			if err := st.Delete(ctx, ref.URI); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := st.Get(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
				t.Errorf("Get after Delete err = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestS3Store_PreservesVeryLongNameWithBoundedKey(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "long-name-flow", "emit")
	name := strings.Repeat("long-name/", 410)
	data := []byte("bounded S3 key with binary name manifest")
	opts := artifacts.PutOptions{
		ContentType: "text/plain",
		Metadata:    prov.Metadata(map[string]string{"case": "very-long-name"}),
	}

	ref, err := st.Put(ctx, name, nonSeekableReader{bytes.NewReader(data)}, opts)
	if err != nil {
		t.Fatalf("Put(%d-byte name): %v", len(name), err)
	}
	t.Cleanup(func() {
		if err := st.Delete(context.Background(), ref.URI); err != nil {
			t.Logf("cleanup Delete(%s): %v", ref.URI, err)
		}
	})
	if ref.Name != name {
		t.Errorf("Put Name length/content changed: got %d bytes exact=%t", len(ref.Name), ref.Name == name)
	}
	parsed, err := url.Parse(ref.URI)
	if err != nil {
		t.Fatalf("parse URI: %v", err)
	}
	keyLen := len(strings.TrimPrefix(parsed.EscapedPath(), "/"))
	if keyLen > 256 {
		t.Errorf("physical S3 key length = %d, want bounded at 256 bytes", keyLen)
	}
	// Bounding the key must not cost the run coordinates.
	if !strings.HasPrefix(ref.URI, runPrefixForTest(cfg.S3Bucket, prov)) {
		t.Errorf("URI %q lost the run coordinates while bounding the key", ref.URI)
	}
	t.Logf("very long logical name bytes=%d physical key bytes=%d exact_name=%t uri=%s",
		len(name), keyLen, ref.Name == name, ref.URI)
	recordProofURI(t, "4100-byte artifact name", ref)

	rc, err := st.Get(ctx, ref.URI)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}

	stat, err := st.Stat(ctx, ref.URI)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Name != name || stat.Metadata["case"] != "very-long-name" || stat.SHA256 != hexSHA(data) {
		t.Errorf("Stat did not restore exact long name and attributes: name bytes=%d exact=%t ref=%+v", len(stat.Name), stat.Name == name, stat)
	}
	refs, err := st.List(ctx, name[:128])
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !containsArtifactURI(refs, ref.URI) {
		t.Errorf("List(long prefix) missing %s", ref.URI)
	}

	signed, err := st.Presign(ctx, ref.URI, time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signed, nil)
	if err != nil {
		t.Fatalf("new presigned request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch presigned URL: %v", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		t.Fatalf("read presigned body: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close presigned body: %v", closeErr)
	}
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("presigned GET status=%d body=%q, want 200 and %q", resp.StatusCode, body, data)
	}
	t.Logf("very long name presigned GET status=%d bytes=%d identical=%t", resp.StatusCode, len(body), bytes.Equal(body, data))

	if err := st.Delete(ctx, ref.URI); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get after Delete err = %v, want ErrNotFound", err)
	}
}

// Two artifacts of one task run never share an object, however adversarial
// their names — including a name shaped like the pre-M7.4 digest layout.
func TestS3Store_DistinctNamesNeverShareAnObject(t *testing.T) {
	st, _ := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "collision-flow", "emit")
	seed := uniquePrefix(t)

	first := putS3(t, st, prov, "foo", []byte(seed+": first"), artifacts.PutOptions{})
	secondName := "foo/" + first.SHA256 + "/child"
	second := putS3(t, st, prov, secondName, []byte(seed+": second"), artifacts.PutOptions{})
	third := putS3(t, st, prov, "foo.txt", []byte(seed+": third"), artifacts.PutOptions{})

	uris := map[string]string{}
	for _, ref := range []artifacts.ArtifactRef{first, second, third} {
		if previous, clash := uris[ref.URI]; clash {
			t.Fatalf("names %q and %q share URI %s", previous, ref.Name, ref.URI)
		}
		uris[ref.URI] = ref.Name
		if body := readAllS3(t, ctx, st, ref.URI); !bytes.Contains(body, []byte(seed)) {
			t.Errorf("artifact %q holds %q, want this test's bytes", ref.Name, body)
		}
	}

	refs, err := st.List(ctx, "foo")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, ref := range []artifacts.ArtifactRef{first, second, third} {
		if !containsArtifactURI(refs, ref.URI) {
			t.Errorf("List(foo) missing %+v from %+v", ref, refs)
		}
		stat, err := st.Stat(ctx, ref.URI)
		if err != nil {
			t.Fatalf("Stat(%s): %v", ref.URI, err)
		}
		if stat.Name != ref.Name {
			t.Errorf("Stat(%s).Name = %q, want %q", ref.URI, stat.Name, ref.Name)
		}
	}
}

// A library Put outside a run still lands somewhere readable and reserved, so
// it can never be swept up by an orkflow's lifecycle rule.
func TestS3Store_FilesPutsWithoutProvenanceUnderTheReservedPrefix(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	name := "loose-" + uniqueToken(t) + ".txt"

	ref, err := st.Put(ctx, name, nonSeekableReader{strings.NewReader("no run produced me")}, artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Delete(context.Background(), ref.URI); err != nil {
			t.Logf("cleanup Delete(%s): %v", ref.URI, err)
		}
	})

	want := fmt.Sprintf("s3://%s/orkflows/_unattributed/_unattributed/_unattributed/%s", cfg.S3Bucket, name)
	if ref.URI != want {
		t.Errorf("URI = %q, want %q", ref.URI, want)
	}
	recordProofURI(t, "put without provenance", ref)
	if stat, err := st.Stat(ctx, ref.URI); err != nil || stat.Name != name {
		t.Errorf("Stat = %+v, %v, want name %q", stat, err, name)
	}
}

func containsArtifactURI(refs []artifacts.ArtifactRef, uri string) bool {
	for _, ref := range refs {
		if ref.URI == uri {
			return true
		}
	}
	return false
}

func readAllS3(t *testing.T, ctx context.Context, st artifacts.Store, uri string) []byte {
	t.Helper()
	rc, err := st.Get(ctx, uri)
	if err != nil {
		t.Fatalf("Get(%s): %v", uri, err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%s): %v", uri, err)
	}
	return body
}

func TestS3Store_RejectsRawQueryAndFragmentInURI(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "reject-flow", "emit")
	ref := putS3(t, st, prov, "plain.txt", []byte("plain"), artifacts.PutOptions{})

	digest := strings.Repeat("a", 64)
	cases := []string{
		ref.URI + "?raw=true",
		ref.URI + "#raw",
		// The pre-M7.4 content-hash layout is gone: M7.4 is a hard break.
		fmt.Sprintf("s3://%s/objects/%s/%s", cfg.S3Bucket, digest, digest),
		fmt.Sprintf("s3://%s/orkflows/hello-ork/fetch/ork-1-1/dir/body", cfg.S3Bucket),
		fmt.Sprintf("s3://%s/orkflows/hello-ork/fetch/body", cfg.S3Bucket),
	}
	for _, uri := range cases {
		if _, err := st.Stat(ctx, uri); !errors.Is(err, artifacts.ErrInvalidURI) {
			t.Errorf("Stat(%q) err = %v, want ErrInvalidURI", uri, err)
		}
	}
}

func TestS3Store_StatMissingReturnsErrNotFound(t *testing.T) {
	st, cfg := newS3Store(t)
	prov := testRun(t, "missing-flow", "emit")
	uri := s3URIForTest(t, cfg.S3Bucket, prov, "absent.txt")

	if _, err := st.Stat(context.Background(), uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Stat missing err = %v, want ErrNotFound", err)
	}
	if _, err := st.Get(context.Background(), uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get missing err = %v, want ErrNotFound", err)
	}
	foreign := "s3://other-bucket/orkflows/hello-ork/fetch/ork-1-1/body"
	if _, err := st.Stat(context.Background(), foreign); !errors.Is(err, artifacts.ErrInvalidURI) {
		t.Errorf("Stat foreign bucket err = %v, want ErrInvalidURI", err)
	}
}

// newRawS3Client talks to the same server without going through the store, so
// lifecycle configuration and raw key listings are observed independently.
func newRawS3Client(t *testing.T, cfg artifacts.Config) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
}

func listKeys(t *testing.T, ctx context.Context, client *s3.Client, bucket, prefix string) []string {
	t.Helper()
	var keys []string
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	sort.Strings(keys)
	return keys
}

// deleteByPrefix performs exactly what an expiration rule for prefix does.
func deleteByPrefix(t *testing.T, ctx context.Context, client *s3.Client, bucket, prefix string) {
	t.Helper()
	for _, key := range listKeys(t, ctx, client, bucket, prefix) {
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}); err != nil {
			t.Fatalf("delete %q: %v", key, err)
		}
	}
}

func recordProofLines(t *testing.T, file string, lines []string) {
	t.Helper()
	dir := os.Getenv("ORK_PROOF_M74_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create proof dir: %v", err)
	}
	body := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", file, err)
	}
}
