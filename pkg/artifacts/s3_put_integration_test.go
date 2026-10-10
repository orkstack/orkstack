//go:build integration

package artifacts_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fileWith writes data to a fresh file and opens it, as the Orker opens an
// output artifact it already hashed.
func fileWith(t *testing.T, data []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "artifact.bin")
	if err := os.WriteFile(path, data, 0o444); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

// TestS3Store_AWrongExpectedDigestIsRefusedByTheServer is
// F-orker-storage-perf-3 against MinIO: the declared digest rides as
// x-amz-checksum-sha256, the server recomputes it, and bytes that do not match
// are refused with no object left under the digest-named key.
func TestS3Store_AWrongExpectedDigestIsRefusedByTheServer(t *testing.T) {
	st, cfg := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "digest-flow", "emit")
	data := []byte("the bytes actually on disk\n")
	wrongSum := sha256.Sum256([]byte("the bytes that were hashed\n"))
	wrong := hex.EncodeToString(wrongSum[:])

	_, err := st.Put(ctx, "claimed.txt", fileWith(t, data), artifacts.PutOptions{
		Metadata: prov.Metadata(nil), ExpectedSHA256: wrong, ExpectedSize: int64(len(data)),
	})
	if err == nil {
		t.Fatal("MinIO accepted bytes that do not hash to the declared digest")
	}
	uri := s3URIForTest(t, cfg.S3Bucket, prov, "claimed.txt", wrong)
	if _, statErr := st.Stat(ctx, uri); statErr == nil {
		t.Fatalf("an object exists at %s after the refusal", uri)
	}
	proof.Write(t, "artifacts", "s3-expected-digest-refused.txt",
		[]byte(fmt.Sprintf("declared=%s actual=%s refused=%v\n", wrong, hexSHA(data), err)))
}

// TestS3Store_LargeFileMultipartRoundTrip is F-orker-storage-perf-7 against
// MinIO: a 200 MiB file goes up in parts read straight from the file and
// reads back byte-identical, with the digest and content type intact.
func TestS3Store_LargeFileMultipartRoundTrip(t *testing.T) {
	st, _ := newS3Store(t)
	ctx := context.Background()
	prov := testRun(t, "multipart-flow", "emit")
	data := make([]byte, 200<<20)
	for i := range data {
		data[i] = byte(i*31 + i>>13)
	}
	sum := hexSHA(data)
	spool := t.TempDir()

	ref, err := st.Put(ctx, "large.bin", fileWith(t, data), artifacts.PutOptions{
		Metadata: prov.Metadata(nil), ContentType: "application/octet-stream",
		ExpectedSHA256: sum, ExpectedSize: int64(len(data)), SpoolDir: spool,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { _ = st.Delete(context.Background(), ref.URI) })
	if entries, _ := os.ReadDir(spool); len(entries) != 0 {
		t.Fatalf("a seekable upload left %d spool file(s)", len(entries))
	}
	stat, err := st.Stat(ctx, ref.URI)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.SHA256 != sum || stat.SizeBytes != int64(len(data)) || stat.ContentType != "application/octet-stream" {
		t.Fatalf("Stat = %+v; want sha %s size %d", stat, sum, len(data))
	}
	rc, err := st.Get(ctx, ref.URI)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	hasher := sha256.New()
	n, err := io.Copy(hasher, rc)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got := hex.EncodeToString(hasher.Sum(nil)); got != sum || n != int64(len(data)) {
		t.Fatalf("read back %d bytes hashing %s; want %d bytes hashing %s", n, got, len(data), sum)
	}
	proof.Write(t, "artifacts", "s3-multipart-roundtrip.txt",
		[]byte(fmt.Sprintf("bytes=%d sha256=%s uri=%s\n", n, sum, ref.URI)))
}
