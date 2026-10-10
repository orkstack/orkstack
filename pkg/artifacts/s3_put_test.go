package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func doubleStore(t *testing.T, d *s3Double) *S3Store {
	t.Helper()
	store, err := NewS3Store(context.Background(), d.config())
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	return store
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// writeTempFile writes data to a file and reopens it read-only, as the Orker
// does for an output artifact it already hashed.
func writeTempFile(t *testing.T, data []byte) *os.File {
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

// spoolWatch records how many files the spool directory held at every request
// the double received.
type spoolWatch struct {
	mu   sync.Mutex
	dir  string
	seen []int
}

func (w *spoolWatch) observe(*http.Request) {
	entries, _ := os.ReadDir(w.dir)
	w.mu.Lock()
	w.seen = append(w.seen, len(entries))
	w.mu.Unlock()
}

func (w *spoolWatch) max() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	most := 0
	for _, n := range w.seen {
		if n > most {
			most = n
		}
	}
	return most
}

// countingReadSeeker counts the bytes read through it. It is deliberately not
// an io.ReaderAt.
type countingReadSeeker struct {
	r    *bytes.Reader
	read int64
}

func (c *countingReadSeeker) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += int64(n)
	return n, err
}

func (c *countingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return c.r.Seek(offset, whence)
}

// TestS3Put_SeekableWithExpectationSkipsTheSpool is F-orker-storage-perf-3:
// an already-hashed file is uploaded straight from itself, with its digest as
// the object's server-verified checksum, and nothing is written to the spool.
func TestS3Put_SeekableWithExpectationSkipsTheSpool(t *testing.T) {
	d := newS3Double(t)
	store := doubleStore(t, d)
	spool := t.TempDir()
	// The OS temp directory too, so a spool that ignored SpoolDir is seen.
	t.Setenv("TMPDIR", spool)
	watch := &spoolWatch{dir: spool}
	d.onRequest = watch.observe
	data := bytes.Repeat([]byte("orkstack "), 4096)
	sum := digestOf(data)
	ref, err := store.Put(context.Background(), "report.txt", writeTempFile(t, data), PutOptions{
		ExpectedSHA256: sum, ExpectedSize: int64(len(data)), SpoolDir: spool,
		Metadata: map[string]string{MetaRunID: "ork-1", MetaTaskID: "t", MetaOrkflow: "o"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if watch.max() != 0 {
		t.Fatalf("the spool held %d file(s) during a seekable, pre-hashed upload", watch.max())
	}
	if ref.SHA256 != sum || ref.SizeBytes != int64(len(data)) || ref.ContentType != "text/plain; charset=utf-8" {
		t.Fatalf("ref = %+v", ref)
	}
	key, _, err := store.parseURI(ref.URI)
	if err != nil {
		t.Fatal(err)
	}
	body, ok := d.object(key)
	if !ok || !bytes.Equal(body, data) {
		t.Fatalf("stored %d bytes; want the file's %d", len(body), len(data))
	}
	raw, _ := hex.DecodeString(sum)
	header := d.headers[key]
	if header.Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(raw) {
		t.Fatalf("x-amz-checksum-sha256 = %q; the store must verify the digest server-side", header.Get("X-Amz-Checksum-Sha256"))
	}
	if header.Get("X-Amz-Meta-"+metaContentSHA256) != sum {
		t.Fatalf("the digest metadata is %q", header.Get("X-Amz-Meta-"+metaContentSHA256))
	}
	// Signed with the known digest, so the SDK did not read the file again to
	// compute it.
	if header.Get("X-Amz-Content-Sha256") != sum {
		t.Fatalf("x-amz-content-sha256 = %q; want the known digest %s", header.Get("X-Amz-Content-Sha256"), sum)
	}
}

// TestS3Put_AWrongExpectationIsRefused: the server verifies the declared
// digest, so bytes that do not match it are never stored under its key.
func TestS3Put_AWrongExpectationIsRefused(t *testing.T) {
	d := newS3Double(t)
	store := doubleStore(t, d)
	data := []byte("the bytes on disk")
	wrong := digestOf([]byte("the bytes that were hashed"))
	_, err := store.Put(context.Background(), "a.txt", writeTempFile(t, data), PutOptions{
		ExpectedSHA256: wrong, ExpectedSize: int64(len(data)),
	})
	if err == nil {
		t.Fatal("a digest the bytes do not have was accepted")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.objects) != 0 {
		t.Fatalf("objects stored after a refused digest: %d", len(d.objects))
	}
}

// TestS3Put_ReadsTheSourceOnceOrTwice: a pre-hashed reader is read once (plus
// the 512-byte sniff); an unhashed seekable one twice — hash, then upload —
// rather than read, spool-write and read again.
func TestS3Put_ReadsTheSourceOnceOrTwice(t *testing.T) {
	d := newS3Double(t)
	store := doubleStore(t, d)
	data := bytes.Repeat([]byte{0xA5}, 1<<20)
	size := int64(len(data))

	known := &countingReadSeeker{r: bytes.NewReader(data)}
	if _, err := store.Put(context.Background(), "known.bin", known, PutOptions{
		ExpectedSHA256: digestOf(data), ExpectedSize: size}); err != nil {
		t.Fatal(err)
	}
	if known.read > size+sniffLen {
		t.Fatalf("a pre-hashed upload read %d bytes of a %d-byte source; want at most one pass plus the sniff", known.read, size)
	}

	unknown := &countingReadSeeker{r: bytes.NewReader(data)}
	ref, err := store.Put(context.Background(), "stdout.log", unknown, PutOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if unknown.read > 2*size {
		t.Fatalf("an unhashed seekable upload read %d bytes of a %d-byte source; want two passes", unknown.read, size)
	}
	if ref.SHA256 != digestOf(data) || ref.SizeBytes != size {
		t.Fatalf("ref = %+v", ref)
	}
}

// TestS3Put_ANonSeekableReaderSpoolsIntoTheSpoolDir: the one reader shape that
// still needs a spool uses the directory the caller named — which the Orker
// reaps at boot — and leaves nothing behind.
func TestS3Put_ANonSeekableReaderSpoolsIntoTheSpoolDir(t *testing.T) {
	d := newS3Double(t)
	store := doubleStore(t, d)
	spool := filepath.Join(t.TempDir(), "ork-spool")
	watch := &spoolWatch{dir: spool}
	d.onRequest = watch.observe
	data := []byte(strings.Repeat("streamed ", 1000))
	ref, err := store.Put(context.Background(), "s.txt", io.MultiReader(bytes.NewReader(data)), PutOptions{SpoolDir: spool})
	if err != nil {
		t.Fatal(err)
	}
	if watch.max() != 1 {
		t.Fatalf("the spool held %d file(s) during a non-seekable upload; want 1", watch.max())
	}
	if entries, _ := os.ReadDir(spool); len(entries) != 0 {
		t.Fatalf("the spool kept %d file(s)", len(entries))
	}
	if ref.SHA256 != digestOf(data) {
		t.Fatalf("ref = %+v", ref)
	}
}

// TestS3Put_LargeFileIsMultipartAndRetriesOnlyTheFailedPart is
// F-orker-storage-perf-7: above the threshold a seekable file is uploaded in
// parts read straight from the file; a failed part is retried alone; the
// object keeps its digest metadata and content type.
func TestS3Put_LargeFileIsMultipartAndRetriesOnlyTheFailedPart(t *testing.T) {
	d := newS3Double(t)
	store := doubleStore(t, d)
	store.multipartThreshold = 6 << 20
	store.partSize = 5 << 20 // the S3 minimum
	spool := t.TempDir()
	watch := &spoolWatch{dir: spool}
	d.onRequest = watch.observe
	d.failParts[2] = 1

	data := make([]byte, 12<<20)
	for i := range data {
		data[i] = byte(i * 7)
	}
	sum := digestOf(data)
	ref, err := store.Put(context.Background(), "big.bin", writeTempFile(t, data), PutOptions{
		ExpectedSHA256: sum, ExpectedSize: int64(len(data)), SpoolDir: spool, ContentType: "application/x-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if watch.max() != 0 {
		t.Fatal("a multipart upload spooled")
	}
	d.mu.Lock()
	parts := map[int]int{}
	for n, c := range d.partRequests {
		parts[n] = c
	}
	completes, puts := d.completes, d.puts
	d.mu.Unlock()
	if parts[1] != 1 || parts[2] != 2 || parts[3] != 1 || len(parts) != 3 {
		t.Fatalf("part requests = %v; want part 2 retried alone", parts)
	}
	if completes != 1 || puts != 0 {
		t.Fatalf("completes = %d, single PUTs = %d; want one multipart upload", completes, puts)
	}
	key, _, err := store.parseURI(ref.URI)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := d.object(key)
	if digestOf(body) != sum || ref.SHA256 != sum {
		t.Fatalf("the stored object does not hash to the expected digest")
	}
	header := d.headers[key]
	if header.Get("X-Amz-Meta-"+metaContentSHA256) != sum || header.Get("Content-Type") != "application/x-test" {
		t.Fatalf("multipart lost the metadata or content type: %v", header)
	}
}

// TestS3Put_UnderTheThresholdIsOnePut: a file at or below the threshold is
// still a single PutObject.
func TestS3Put_UnderTheThresholdIsOnePut(t *testing.T) {
	d := newS3Double(t)
	store := doubleStore(t, d)
	store.multipartThreshold = 6 << 20
	store.partSize = 5 << 20
	data := make([]byte, 6<<20)
	if _, err := store.Put(context.Background(), "edge.bin", writeTempFile(t, data), PutOptions{
		ExpectedSHA256: digestOf(data), ExpectedSize: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.puts != 1 || len(d.partRequests) != 0 {
		t.Fatalf("puts = %d, parts = %v; want one PUT", d.puts, d.partRequests)
	}
}

// TestS3Put_AFailedMultipartIsAborted: a part that never succeeds aborts the
// upload rather than leaving its parts behind.
func TestS3Put_AFailedMultipartIsAborted(t *testing.T) {
	d := newS3Double(t)
	store := doubleStore(t, d)
	store.multipartThreshold = 6 << 20
	store.partSize = 5 << 20
	d.failParts[1] = 100
	data := make([]byte, 11<<20)
	if _, err := store.Put(context.Background(), "doomed.bin", writeTempFile(t, data), PutOptions{
		ExpectedSHA256: digestOf(data), ExpectedSize: int64(len(data))}); err == nil {
		t.Fatal("a multipart upload whose part always fails succeeded")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.aborts != 1 || len(d.objects) != 0 {
		t.Fatalf("aborts = %d, objects = %d; want the upload aborted", d.aborts, len(d.objects))
	}
}

// TestLocalPut_RefusesAWrongExpectation keeps the two stores' contracts one:
// a declared digest the bytes do not have is refused, and nothing is stored.
func TestLocalPut_RefusesAWrongExpectation(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	data := []byte("local bytes")
	if _, err := store.Put(context.Background(), "x", bytes.NewReader(data), PutOptions{
		ExpectedSHA256: digestOf([]byte("other")), ExpectedSize: int64(len(data))}); err == nil {
		t.Fatal("a wrong digest was accepted")
	}
	refs, err := store.List(context.Background(), "")
	if err != nil || len(refs) != 0 {
		t.Fatalf("List = %v, %v; want nothing stored", refs, err)
	}
	if _, err := store.Put(context.Background(), "x", bytes.NewReader(data), PutOptions{
		ExpectedSHA256: digestOf(data), ExpectedSize: int64(len(data))}); err != nil {
		t.Fatalf("the right digest was refused: %v", err)
	}
}
