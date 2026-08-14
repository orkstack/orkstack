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
	"strings"
	"testing"
	"time"

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

// uniquePrefix keeps concurrent/repeated runs from colliding in the shared bucket.
func uniquePrefix(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return "it/" + hex.EncodeToString(b[:])
}

func putS3(t *testing.T, st artifacts.Store, name string, data []byte, opts artifacts.PutOptions) artifacts.ArtifactRef {
	t.Helper()
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

func TestS3Store_PutGetRoundTripIsByteIdentical(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	prefix := uniquePrefix(t)
	data := bytes.Repeat([]byte("orkstack-s3-roundtrip\n"), 5000)

	ref := putS3(t, st, prefix+"/roundtrip.txt", data, artifacts.PutOptions{
		ContentType: "text/plain; charset=utf-8",
		Metadata:    map[string]string{"milestone": "m1"},
	})

	wantURI := s3URIForTest(cfg.S3Bucket, prefix+"/roundtrip.txt", hexSHA(data))
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
	prefix := uniquePrefix(t)

	a := putS3(t, st, prefix+"/dir-a/one.txt", []byte("one"), artifacts.PutOptions{})
	b := putS3(t, st, prefix+"/dir-a/two.txt", []byte("two"), artifacts.PutOptions{})
	c := putS3(t, st, prefix+"/dir-b/three.txt", []byte("three"), artifacts.PutOptions{})

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
	prefix := uniquePrefix(t)

	ref := putS3(t, st, prefix+"/delete-me.txt", []byte("delete me"), artifacts.PutOptions{})

	if err := st.Delete(ctx, ref.URI); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Stat(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Stat after Delete err = %v, want ErrNotFound", err)
	}
	if _, err := st.Get(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get after Delete err = %v, want ErrNotFound", err)
	}
	refs, err := st.List(ctx, prefix+"/")
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
	prefix := uniquePrefix(t)
	data := []byte("presigned-payload-" + strings.Repeat("z", 1024))

	ref := putS3(t, st, prefix+"/presign.txt", data, artifacts.PutOptions{ContentType: "text/plain"})

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
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			st, cfg := newS3Store(t)
			ctx := context.Background()
			data := []byte(uniquePrefix(t) + ": opaque s3 name: " + tc.name)
			opts := artifacts.PutOptions{Metadata: map[string]string{"case": tc.label}}

			ref := putS3(t, st, tc.name, data, opts)
			if ref.Name != tc.name {
				t.Errorf("Put Name = %q, want exact %q", ref.Name, tc.name)
			}
			wantURI := s3URIForTest(cfg.S3Bucket, tc.name, ref.SHA256)
			if ref.URI != wantURI {
				t.Fatalf("Put URI = %q, want canonical %q", ref.URI, wantURI)
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
	st, _ := newS3Store(t)
	ctx := context.Background()
	name := strings.Repeat("long-name/", 410)
	data := []byte("bounded S3 key with binary name manifest")
	opts := artifacts.PutOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"case": "very-long-name"},
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
	t.Logf("very long logical name bytes=%d physical key bytes=%d exact_name=%t", len(name), keyLen, ref.Name == name)

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

func TestS3Store_NamesDoNotCollideWithPriorDigestPaths(t *testing.T) {
	st, _ := newS3Store(t)
	ctx := context.Background()
	seed := uniquePrefix(t)
	first := putS3(t, st, "foo", []byte(seed+": first"), artifacts.PutOptions{})
	secondName := "foo/" + first.SHA256 + "/child"
	second := putS3(t, st, secondName, []byte(seed+": second"), artifacts.PutOptions{})

	refs, err := st.List(ctx, "foo")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, ref := range []artifacts.ArtifactRef{first, second} {
		if !containsArtifactURI(refs, ref.URI) {
			t.Errorf("List(foo) missing %+v from %+v", ref, refs)
		}
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

func TestS3Store_RejectsRawQueryAndFragmentInURI(t *testing.T) {
	st, _ := newS3Store(t)
	ctx := context.Background()
	ref := putS3(t, st, uniquePrefix(t)+"/plain.txt", []byte("plain"), artifacts.PutOptions{})

	for _, uri := range []string{ref.URI + "?raw=true", ref.URI + "#raw"} {
		if _, err := st.Stat(ctx, uri); !errors.Is(err, artifacts.ErrInvalidURI) {
			t.Errorf("Stat(%q) err = %v, want ErrInvalidURI", uri, err)
		}
	}
}

func TestS3Store_StatMissingReturnsErrNotFound(t *testing.T) {
	st, cfg := newS3Store(t)
	uri := s3URIForTest(cfg.S3Bucket, uniquePrefix(t)+"/absent.txt", strings.Repeat("a", 64))

	if _, err := st.Stat(context.Background(), uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Stat missing err = %v, want ErrNotFound", err)
	}
	if _, err := st.Get(context.Background(), uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get missing err = %v, want ErrNotFound", err)
	}
	if _, err := st.Stat(context.Background(), "s3://other-bucket/x/"+strings.Repeat("a", 64)); !errors.Is(err, artifacts.ErrInvalidURI) {
		t.Errorf("Stat foreign bucket err = %v, want ErrInvalidURI", err)
	}
}
