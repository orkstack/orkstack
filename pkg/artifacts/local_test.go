package artifacts_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
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

			wantURI := "file://" + filepath.ToSlash(filepath.Join(root, filepath.FromSlash(tc.name), ref.SHA256))
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
	uri := "file://" + filepath.ToSlash(filepath.Join(root, "outputs/none", strings.Repeat("a", 64)))

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
	st, _ := newLocalStore(t)
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
	// No sidecar or object file may survive under the store root.
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(strings.TrimPrefix(ref.URI, "file://")), "*"))
	if len(matches) != 0 {
		t.Errorf("files remain after Delete: %v", matches)
	}
}

func TestLocalStore_RejectsUnsafeNames(t *testing.T) {
	st, _ := newLocalStore(t)
	ctx := context.Background()

	names := []string{"", "/abs/path", "../escape", "outputs/../../escape", ".", "outputs/"}
	for _, name := range names {
		t.Run("name="+name, func(t *testing.T) {
			if _, err := st.Put(ctx, name, strings.NewReader("x"), artifacts.PutOptions{}); err == nil {
				t.Fatalf("Put(%q) succeeded, want error", name)
			}
		})
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

func TestLocalStore_PreservesSpecialAndDigestLookingNames(t *testing.T) {
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
			st, root := newLocalStore(t)
			ctx := context.Background()
			data := []byte("special local name: " + tc.name)

			ref := mustPut(t, st, tc.name, data, artifacts.PutOptions{})
			wantURI := (&url.URL{
				Scheme: "file",
				Path:   filepath.ToSlash(filepath.Join(root, filepath.FromSlash(tc.name), ref.SHA256)),
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
			if stat.Name != tc.name || stat.SHA256 != hexSHA(data) {
				t.Errorf("Stat = %+v, want name %q sha256 %s", stat, tc.name, hexSHA(data))
			}

			refs, err := st.List(ctx, tc.name)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(refs) != 1 || refs[0].URI != ref.URI {
				t.Errorf("List(%q) = %+v, want only %s", tc.name, refs, ref.URI)
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
