package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
)

// E3
func TestWrite_MembersAppearInManifestOrderWithManifestFirst(t *testing.T) {
	f := newFixture()
	bundle := writeFixture(t, f)
	gz, err := gzip.NewReader(bytes.NewReader(bundle))
	if err != nil {
		t.Fatal(err)
	}
	if !gz.ModTime.Equal(fixtureExportedAt) {
		t.Errorf("gzip header time = %v, want exported_at %v", gz.ModTime, fixtureExportedAt)
	}
	tr := tar.NewReader(gz)
	var names []string
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
		if h.Typeflag != tar.TypeReg || h.Mode != 0o644 || !h.ModTime.Equal(fixtureExportedAt) || h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" {
			t.Errorf("entry %s: type %c mode %o mtime %v uid %d gid %d uname %q", h.Name, h.Typeflag, h.Mode, h.ModTime, h.Uid, h.Gid, h.Uname)
		}
		body, _ := io.ReadAll(tr)
		if h.Name != ManifestPath && !bytes.Equal(body, f.bytes[h.Name]) {
			t.Errorf("entry %s carries other bytes than its member", h.Name)
		}
	}
	want := []string{ManifestPath}
	for _, m := range f.manifest.Members {
		if m.State == StatePresent {
			want = append(want, m.Path)
		}
	}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("entries = %v\nwant    %v", names, want)
	}
}

// E4
func TestWrite_AbortsOnDigestOrSizeMismatchAndOnVanish(t *testing.T) {
	f := newFixture()
	artifact := "tasks/reconcile/artifacts/rates.parquet"
	sameLength := bytes.Repeat([]byte("R"), 3000)
	cases := []struct {
		name string
		src  mapSource
		want error
	}{
		{"digest", mapSource{bytes: f.bytes, override: map[string][]byte{artifact: sameLength}}, ErrMemberMismatch},
		{"short", mapSource{bytes: f.bytes, override: map[string][]byte{artifact: sameLength[:10]}}, ErrMemberMismatch},
		{"long", mapSource{bytes: f.bytes, override: map[string][]byte{artifact: append(bytes.Clone(f.bytes[artifact]), 'x')}}, ErrMemberMismatch},
		{"vanished", mapSource{bytes: f.bytes, missing: map[string]bool{artifact: true}}, ErrMemberVanished},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := Write(context.Background(), &out, f.manifest, tc.src)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Write = %v, want %v", err, tc.want)
			}
			var memberErr *MemberError
			if !errors.As(err, &memberErr) || memberErr.Member.Path != artifact {
				t.Fatalf("error does not name the member: %v", err)
			}
			// The archive was not finished: reading it back fails.
			if _, err := Verify(bytes.NewReader(out.Bytes())); err == nil {
				t.Fatal("a torn bundle read back as a complete one")
			}
		})
	}
}
