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

	wantURI := "s3://" + cfg.S3Bucket + "/" + prefix + "/roundtrip.txt/" + hexSHA(data)
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
	if refs[0].URI != a.URI || refs[1].URI != b.URI {
		t.Errorf("List(dir-a) = [%s %s], want [%s %s]", refs[0].URI, refs[1].URI, a.URI, b.URI)
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

func TestS3Store_PreservesSpecialAndDigestLookingNames(t *testing.T) {
	cases := []struct {
		name       string
		wantEscape string
	}{
		{name: "hash#tag.txt", wantEscape: "hash%23tag.txt"},
		{name: "query?a=1.txt", wantEscape: "query%3Fa=1.txt"},
		{name: "pct%41.txt", wantEscape: "pct%2541.txt"},
		{name: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", wantEscape: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, cfg := newS3Store(t)
			ctx := context.Background()
			name := uniquePrefix(t) + "/" + tc.name
			data := []byte("special s3 name: " + tc.name)

			ref := putS3(t, st, name, data, artifacts.PutOptions{})
			wantURI := (&url.URL{
				Scheme: "s3",
				Host:   cfg.S3Bucket,
				Path:   "/" + name + "/" + ref.SHA256,
			}).String()
			if ref.URI != wantURI {
				t.Fatalf("Put URI = %q, want canonical %q", ref.URI, wantURI)
			}
			if !strings.Contains(ref.URI, tc.wantEscape) {
				t.Errorf("Put URI = %q, want escaped name %q", ref.URI, tc.wantEscape)
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
			if stat.Name != name || stat.SHA256 != hexSHA(data) {
				t.Errorf("Stat = %+v, want name %q sha256 %s", stat, name, hexSHA(data))
			}

			refs, err := st.List(ctx, name)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(refs) != 1 || refs[0].URI != ref.URI {
				t.Errorf("List(%q) = %+v, want only %s", name, refs, ref.URI)
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
	uri := "s3://" + cfg.S3Bucket + "/" + uniquePrefix(t) + "/absent.txt/" + strings.Repeat("a", 64)

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
