package artifacts_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// nonSeekableReader hides io.Seeker/io.ReaderAt from the store so tests prove
// the provider streams an arbitrary reader.
type nonSeekableReader struct{ r io.Reader }

func (n nonSeekableReader) Read(p []byte) (int, error) { return n.r.Read(p) }

func newLocalStore(t *testing.T) (artifacts.Store, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := artifacts.New(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatalf("New(local): %v", err)
	}
	return st, dir
}

func mustPut(t *testing.T, st artifacts.Store, name string, data []byte, opts artifacts.PutOptions) artifacts.ArtifactRef {
	t.Helper()
	ref, err := st.Put(context.Background(), name, nonSeekableReader{bytes.NewReader(data)}, opts)
	if err != nil {
		t.Fatalf("Put(%q): %v", name, err)
	}
	return ref
}

func hexSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

const encodedNameChunkSizeForTest = 200

func encodedNamePathForTest(name string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(name))
	if encoded == "" {
		return path.Join("objects", "n-")
	}

	parts := []string{"objects"}
	for first := true; encoded != ""; first = false {
		n := encodedNameChunkSizeForTest
		if n > len(encoded) {
			n = len(encoded)
		}
		marker := "c-"
		if first {
			marker = "n-"
		}
		parts = append(parts, marker+encoded[:n])
		encoded = encoded[n:]
	}
	return path.Join(parts...)
}

func objectKeyForTest(name, sha string) string {
	return path.Join(encodedNamePathForTest(name), sha)
}

func localURIForTest(root, name, sha string) string {
	return (&url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(filepath.Join(root, filepath.FromSlash(objectKeyForTest(name, sha)))),
	}).String()
}

func s3URIForTest(bucket, name, sha string) string {
	nameSum := sha256.Sum256([]byte(name))
	key := path.Join("objects", hex.EncodeToString(nameSum[:]), sha)
	return (&url.URL{Scheme: "s3", Host: bucket, Path: "/" + key}).String()
}

func TestLocalStore_PutGetRoundTripIsByteIdentical(t *testing.T) {
	cases := []struct {
		testName    string
		name        string
		data        []byte
		opts        artifacts.PutOptions
		wantType    string
		wantTypePfx string
	}{
		{
			testName:    "text detected",
			name:        "outputs/report.txt",
			data:        []byte("hello orkstack\n"),
			wantTypePfx: "text/plain",
		},
		{
			testName: "explicit content type wins",
			name:     "outputs/payload.json",
			data:     []byte(`{"ok":true}`),
			opts:     artifacts.PutOptions{ContentType: "application/json"},
			wantType: "application/json",
		},
		{
			testName:    "binary",
			name:        "outputs/blob.bin",
			data:        []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 0x7f, 0x00},
			wantTypePfx: "application/octet-stream",
		},
		{
			testName:    "empty",
			name:        "outputs/empty",
			data:        []byte{},
			wantTypePfx: "text/plain",
		},
		{
			testName:    "large multi-buffer",
			name:        "outputs/large.bin",
			data:        bytes.Repeat([]byte("orkstack-"), 200_000),
			wantTypePfx: "text/plain",
		},
	}

	for _, tc := range cases {
		t.Run(tc.testName, func(t *testing.T) {
			st, root := newLocalStore(t)
			ctx := context.Background()

			ref := mustPut(t, st, tc.name, tc.data, tc.opts)

			if got, want := ref.Name, tc.name; got != want {
				t.Errorf("Name = %q, want %q", got, want)
			}
			if got, want := ref.SHA256, hexSHA(tc.data); got != want {
				t.Errorf("SHA256 = %q, want %q", got, want)
			}
			if got, want := ref.SizeBytes, int64(len(tc.data)); got != want {
				t.Errorf("SizeBytes = %d, want %d", got, want)
			}
			if tc.wantType != "" && ref.ContentType != tc.wantType {
				t.Errorf("ContentType = %q, want %q", ref.ContentType, tc.wantType)
			}
			if tc.wantTypePfx != "" && !strings.HasPrefix(ref.ContentType, tc.wantTypePfx) {
				t.Errorf("ContentType = %q, want prefix %q", ref.ContentType, tc.wantTypePfx)
			}

			wantURI := localURIForTest(root, tc.name, ref.SHA256)
			if ref.URI != wantURI {
				t.Errorf("URI = %q, want %q", ref.URI, wantURI)
			}
			t.Logf("put uri=%s size=%d sha256=%s content_type=%s",
				ref.URI, ref.SizeBytes, ref.SHA256, ref.ContentType)

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
			if !bytes.Equal(got, tc.data) {
				t.Errorf("Get returned %d bytes, want %d identical bytes", len(got), len(tc.data))
			}
			t.Logf("get bytes=%d sha256=%s (identical=%t)", len(got), hexSHA(got), bytes.Equal(got, tc.data))
		})
	}
}

func TestLocalStore_StatReturnsStoredMetadata(t *testing.T) {
	st, _ := newLocalStore(t)
	ctx := context.Background()
	data := []byte("stat me")

	put := mustPut(t, st, "outputs/stat.txt", data, artifacts.PutOptions{
		ContentType: "text/csv",
		Metadata:    map[string]string{"task": "t1"},
	})

	got, err := st.Stat(ctx, put.URI)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got.URI != put.URI || got.Name != "outputs/stat.txt" {
		t.Errorf("Stat ref = %+v, want URI %q name %q", got, put.URI, "outputs/stat.txt")
	}
	if got.SHA256 != hexSHA(data) || got.SizeBytes != int64(len(data)) {
		t.Errorf("Stat sha/size = %q/%d, want %q/%d", got.SHA256, got.SizeBytes, hexSHA(data), len(data))
	}
	if got.ContentType != "text/csv" {
		t.Errorf("Stat ContentType = %q, want %q", got.ContentType, "text/csv")
	}
	if got.Metadata["task"] != "t1" {
		t.Errorf("Stat Metadata = %v, want task=t1", got.Metadata)
	}
}

func TestLocalStore_StatMissingReturnsErrNotFound(t *testing.T) {
	st, root := newLocalStore(t)
	uri := localURIForTest(root, "outputs/none", strings.Repeat("a", 64))

	if _, err := st.Stat(context.Background(), uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Stat missing err = %v, want ErrNotFound", err)
	}
	if _, err := st.Get(context.Background(), uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get missing err = %v, want ErrNotFound", err)
	}
}

func TestLocalStore_ListByPrefix(t *testing.T) {
	st, _ := newLocalStore(t)
	ctx := context.Background()

	a := mustPut(t, st, "run-1/a.txt", []byte("a"), artifacts.PutOptions{})
	b := mustPut(t, st, "run-1/nested/b.txt", []byte("b"), artifacts.PutOptions{})
	c := mustPut(t, st, "run-2/c.txt", []byte("c"), artifacts.PutOptions{})

	cases := []struct {
		prefix string
		want   []string
	}{
		{prefix: "run-1/", want: []string{a.URI, b.URI}},
		{prefix: "run-1/nested", want: []string{b.URI}},
		{prefix: "run-2", want: []string{c.URI}},
		{prefix: "", want: []string{a.URI, b.URI, c.URI}},
		{prefix: "nope/", want: nil},
	}

	for _, tc := range cases {
		t.Run("prefix="+tc.prefix, func(t *testing.T) {
			refs, err := st.List(ctx, tc.prefix)
			if err != nil {
				t.Fatalf("List(%q): %v", tc.prefix, err)
			}
			var got []string
			for _, r := range refs {
				got = append(got, r.URI)
				if r.SHA256 == "" || r.Name == "" {
					t.Errorf("List entry missing name/sha: %+v", r)
				}
			}
			if len(got) != len(tc.want) {
				t.Fatalf("List(%q) = %v, want %v", tc.prefix, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("List(%q) = %v, want %v", tc.prefix, got, tc.want)
				}
			}
		})
	}
}

func TestLocalStore_DeleteRemovesObject(t *testing.T) {
	st, root := newLocalStore(t)
	ctx := context.Background()

	ref := mustPut(t, st, "outputs/gone.txt", []byte("bye"), artifacts.PutOptions{Metadata: map[string]string{"k": "v"}})

	if err := st.Delete(ctx, ref.URI); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get after Delete err = %v, want ErrNotFound", err)
	}
	if _, err := st.Stat(ctx, ref.URI); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Stat after Delete err = %v, want ErrNotFound", err)
	}
	refs, err := st.List(ctx, "outputs/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("List after Delete = %+v, want empty", refs)
	}
	// Delete is idempotent.
	if err := st.Delete(ctx, ref.URI); err != nil {
		t.Errorf("second Delete: %v", err)
	}
	// No object or metadata file may survive under their physical namespaces.
	var remaining []string
	for _, subtree := range []string{"objects", ".metadata"} {
		err := filepath.WalkDir(filepath.Join(root, subtree), func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				remaining = append(remaining, p)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", subtree, err)
		}
	}
	if len(remaining) != 0 {
		t.Errorf("files remain after Delete: %v", remaining)
	}
}

func TestLocalStore_RejectsForeignURI(t *testing.T) {
	st, _ := newLocalStore(t)
	ctx := context.Background()
	other := t.TempDir()

	bad := []string{
		"s3://bucket/key/" + strings.Repeat("a", 64),
		"file://" + filepath.ToSlash(filepath.Join(other, "x", strings.Repeat("a", 64))),
		"not a uri",
		"file:///",
	}
	for _, uri := range bad {
		if _, err := st.Stat(ctx, uri); !errors.Is(err, artifacts.ErrInvalidURI) {
			t.Errorf("Stat(%q) err = %v, want ErrInvalidURI", uri, err)
		}
	}
}

func TestLocalStore_PresignIsUnsupported(t *testing.T) {
	st, _ := newLocalStore(t)
	ref := mustPut(t, st, "outputs/p.txt", []byte("p"), artifacts.PutOptions{})

	url, err := st.Presign(context.Background(), ref.URI, time.Minute)
	if !errors.Is(err, artifacts.ErrUnsupported) {
		t.Fatalf("Presign err = %v, want ErrUnsupported", err)
	}
	if url != "" {
		t.Errorf("Presign url = %q, want empty", url)
	}
}

func TestLocalStore_PreservesOpaqueNames(t *testing.T) {
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
			st, root := newLocalStore(t)
			ctx := context.Background()
			data := []byte("opaque local name: " + tc.name)
			opts := artifacts.PutOptions{Metadata: map[string]string{"case": tc.label}}

			ref := mustPut(t, st, tc.name, data, opts)
			if ref.Name != tc.name {
				t.Errorf("Put Name = %q, want exact %q", ref.Name, tc.name)
			}
			if ref.SHA256 != hexSHA(data) || ref.SizeBytes != int64(len(data)) {
				t.Errorf("Put sha/size = %s/%d, want %s/%d", ref.SHA256, ref.SizeBytes, hexSHA(data), len(data))
			}
			wantURI := localURIForTest(root, tc.name, ref.SHA256)
			if ref.URI != wantURI {
				t.Fatalf("Put URI = %q, want canonical %q", ref.URI, wantURI)
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
			if len(refs) != 1 || refs[0].URI != ref.URI || refs[0].Name != tc.name {
				t.Errorf("List(%q) = %+v, want only %+v", tc.name, refs, ref)
			}

			if _, err := st.Presign(ctx, ref.URI, time.Minute); !errors.Is(err, artifacts.ErrUnsupported) {
				t.Errorf("Presign err = %v, want ErrUnsupported", err)
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

func TestLocalStore_NamesDoNotCollideWithPriorDigestPaths(t *testing.T) {
	st, _ := newLocalStore(t)
	ctx := context.Background()
	first := mustPut(t, st, "foo", []byte("first"), artifacts.PutOptions{})
	secondName := "foo/" + first.SHA256 + "/child"
	second := mustPut(t, st, secondName, []byte("second"), artifacts.PutOptions{})

	for _, ref := range []artifacts.ArtifactRef{first, second} {
		rc, err := st.Get(ctx, ref.URI)
		if err != nil {
			t.Fatalf("Get(%q): %v", ref.Name, err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("Close(%q): %v", ref.Name, err)
		}
	}
	refs, err := st.List(ctx, "foo")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("List(foo) = %+v, want both collision-sequence refs", refs)
	}
}

func TestNewLocalStore_PreservesWhitespaceDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	st, err := artifacts.NewLocalStore("   ")
	if err != nil {
		t.Fatalf("NewLocalStore(whitespace): %v", err)
	}
	if filepath.Base(st.Dir()) != "   " {
		t.Errorf("Dir base = %q, want three spaces", filepath.Base(st.Dir()))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := artifacts.NewLocalStore(""); err == nil {
		t.Fatal("NewLocalStore(empty) succeeded, want error")
	}
}

func TestLocalStore_PutLeavesNoTempFiles(t *testing.T) {
	st, root := newLocalStore(t)
	mustPut(t, st, "outputs/tidy.txt", []byte("tidy"), artifacts.PutOptions{})

	var stray []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.Contains(d.Name(), "tmp") {
			stray = append(stray, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(stray) != 0 {
		t.Errorf("temp files left behind: %v", stray)
	}
}
