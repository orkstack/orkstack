package dist_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
)

// commitTime is the instant every archive in these tests is stamped with.
var commitTime = time.Date(2026, 10, 24, 9, 30, 15, 0, time.UTC)

var (
	fakeBinary  = []byte("\x7fELF a binary that is not one, \x00\x01\x02")
	fakeLicense = []byte("Apache License, Version 2.0 (a stand-in)\n")
)

func readTarGz(t *testing.T, data []byte) ([]*tar.Header, map[string][]byte) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	var headers []*tar.Header
	contents := map[string][]byte{}
	reader := tar.NewReader(gz)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		headers = append(headers, header)
		contents[header.Name] = body
	}
	return headers, contents
}

func linuxTarget() dist.Target {
	return dist.Target{Binary: dist.Orkctl, GOOS: "linux", GOARCH: "amd64"}
}

// TestPack_TarGzIsByteIdentical: the same inputs give the same bytes, so a
// rebuild of a commit publishes the same archive.
func TestPack_TarGzIsByteIdentical(t *testing.T) {
	t.Parallel()
	forward, err := dist.Pack(linuxTarget(), fakeBinary, fakeLicense, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		again, err := dist.Pack(linuxTarget(), fakeBinary, fakeLicense, commitTime)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(forward.Data, again.Data) {
			t.Fatalf("pack %d differs from the first", i+2)
		}
	}
	other, err := dist.Pack(linuxTarget(), fakeBinary, fakeLicense, commitTime.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(forward.Data, other.Data) {
		t.Fatal("a different commit time gave the same bytes, so the time is not in the archive")
	}
	changed, err := dist.Pack(linuxTarget(), append([]byte("x"), fakeBinary...), fakeLicense, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(forward.Data, changed.Data) {
		t.Fatal("a different binary gave the same bytes")
	}
}

// TestPack_TarGzEntries: sorted, root-owned, the binary executable, the
// licence not, every mtime the commit's, and nothing else in the header.
func TestPack_TarGzEntries(t *testing.T) {
	t.Parallel()
	asset, err := dist.Pack(linuxTarget(), fakeBinary, fakeLicense, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	headers, contents := readTarGz(t, asset.Data)
	var names []string
	for _, header := range headers {
		names = append(names, header.Name)
	}
	if want := []string{"LICENSE", "orkctl"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("entries %q, want %q (sorted, flat, no directory)", names, want)
	}
	if !bytes.Equal(contents["orkctl"], fakeBinary) || !bytes.Equal(contents["LICENSE"], fakeLicense) {
		t.Fatal("an entry's bytes differ from what was packed")
	}
	for _, header := range headers {
		if header.Typeflag != tar.TypeReg {
			t.Errorf("%s: type %q, want a regular file", header.Name, header.Typeflag)
		}
		if header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" {
			t.Errorf("%s: owner %d:%d (%q:%q), want root and unnamed", header.Name, header.Uid, header.Gid, header.Uname, header.Gname)
		}
		if !header.ModTime.Equal(commitTime) {
			t.Errorf("%s: mtime %v, want the commit time %v", header.Name, header.ModTime, commitTime)
		}
		if !header.AccessTime.IsZero() || !header.ChangeTime.IsZero() {
			t.Errorf("%s: carries an access or change time", header.Name)
		}
		if len(header.PAXRecords) != 0 || header.Format != tar.FormatUSTAR {
			t.Errorf("%s: format %v with PAX records %v, want plain USTAR", header.Name, header.Format, header.PAXRecords)
		}
	}
	modes := map[string]int64{}
	for _, header := range headers {
		modes[header.Name] = header.Mode
	}
	if modes["orkctl"] != 0o755 {
		t.Errorf("binary mode %o, want 755", modes["orkctl"])
	}
	if modes["LICENSE"] != 0o644 {
		t.Errorf("LICENSE mode %o, want 644", modes["LICENSE"])
	}
}

// TestPack_TarGzHeaderCarriesNoNameOrTime: gzip would otherwise record the
// file name and the time it was compressed, and the bytes would change with
// the clock.
func TestPack_TarGzHeaderCarriesNoNameOrTime(t *testing.T) {
	t.Parallel()
	asset, err := dist.Pack(linuxTarget(), fakeBinary, fakeLicense, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	head := asset.Data
	if len(head) < 10 || head[0] != 0x1f || head[1] != 0x8b || head[2] != 0x08 {
		t.Fatalf("not a gzip stream: % x", head[:min(10, len(head))])
	}
	if head[3] != 0 {
		t.Errorf("gzip flags %#x: a name, comment or extra field is present", head[3])
	}
	if !bytes.Equal(head[4:8], []byte{0, 0, 0, 0}) {
		t.Errorf("gzip mtime % x, want zero", head[4:8])
	}
	gz, err := gzip.NewReader(bytes.NewReader(head))
	if err != nil {
		t.Fatal(err)
	}
	if gz.Name != "" || gz.Comment != "" || !gz.ModTime.IsZero() {
		t.Errorf("gzip header name %q comment %q mtime %v, want none", gz.Name, gz.Comment, gz.ModTime)
	}
}

// TestPack_WindowsIsAZipWithAnExe: a zip, because that is what Windows opens,
// holding orkctl.exe and LICENSE, sorted, stamped with the commit time and
// byte for byte repeatable.
func TestPack_WindowsIsAZipWithAnExe(t *testing.T) {
	t.Parallel()
	target := dist.Target{Binary: dist.Orkctl, GOOS: "windows", GOARCH: "arm64"}
	asset, err := dist.Pack(target, fakeBinary, fakeLicense, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	if asset.Name != "orkctl-windows-arm64.zip" {
		t.Fatalf("asset %q, want orkctl-windows-arm64.zip", asset.Name)
	}
	again, err := dist.Pack(target, fakeBinary, fakeLicense, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(asset.Data, again.Data) {
		t.Fatal("two zips of the same inputs differ")
	}
	reader, err := zip.NewReader(bytes.NewReader(asset.Data), int64(len(asset.Data)))
	if err != nil {
		t.Fatalf("not a zip: %v", err)
	}
	var names []string
	for _, file := range reader.File {
		names = append(names, file.Name)
		if !file.Modified.Equal(commitTime) {
			t.Errorf("%s: modified %v, want the commit time %v", file.Name, file.Modified, commitTime)
		}
		if file.Method != zip.Deflate {
			t.Errorf("%s: method %d, want deflate", file.Name, file.Method)
		}
		open, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(open)
		_ = open.Close()
		if err != nil {
			t.Fatal(err)
		}
		want := fakeLicense
		if file.Name == "orkctl.exe" {
			want = fakeBinary
		}
		if !bytes.Equal(body, want) {
			t.Errorf("%s: bytes differ from what was packed", file.Name)
		}
	}
	if want := []string{"LICENSE", "orkctl.exe"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("entries %q, want %q", names, want)
	}
	for _, file := range reader.File {
		if file.Name == "orkctl.exe" && file.Mode().Perm() != 0o755 {
			t.Errorf("orkctl.exe mode %v, want 0755", file.Mode().Perm())
		}
	}
	other, err := dist.Pack(target, fakeBinary, fakeLicense, commitTime.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(asset.Data, other.Data) {
		t.Fatal("a different commit time gave the same zip")
	}
}

// TestPack_RefusesAnEmptyBinary: an archive of nothing is a build that went
// wrong, not a release.
func TestPack_RefusesAnEmptyBinary(t *testing.T) {
	t.Parallel()
	if _, err := dist.Pack(linuxTarget(), nil, fakeLicense, commitTime); err == nil {
		t.Error("an empty binary was packed")
	}
	if _, err := dist.Pack(linuxTarget(), fakeBinary, nil, commitTime); err == nil {
		t.Error("a missing LICENSE was packed")
	}
	if _, err := dist.Pack(linuxTarget(), fakeBinary, fakeLicense, time.Time{}); err == nil {
		t.Error("a zero commit time was packed")
	}
}

// TestAssetNames_CarryNoVersion: a name is the binary and the platform, so
// releases/latest/download/<name> keeps working from one release to the next.
func TestAssetNames_CarryNoVersion(t *testing.T) {
	t.Parallel()
	want := map[string]string{
		"ork-linux-amd64":      "ork-linux-amd64.tar.gz",
		"ork-linux-arm64":      "ork-linux-arm64.tar.gz",
		"orker-linux-amd64":    "orker-linux-amd64.tar.gz",
		"orker-linux-arm64":    "orker-linux-arm64.tar.gz",
		"orkctl-linux-amd64":   "orkctl-linux-amd64.tar.gz",
		"orkctl-linux-arm64":   "orkctl-linux-arm64.tar.gz",
		"orkctl-darwin-amd64":  "orkctl-darwin-amd64.tar.gz",
		"orkctl-darwin-arm64":  "orkctl-darwin-arm64.tar.gz",
		"orkctl-windows-amd64": "orkctl-windows-amd64.zip",
		"orkctl-windows-arm64": "orkctl-windows-arm64.zip",
	}
	got := map[string]string{}
	for _, target := range dist.Targets() {
		got[target.String()] = target.Asset()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("asset names\n got: %v\nwant: %v", got, want)
	}
	asset, err := dist.Pack(dist.Target{Binary: dist.Orker, GOOS: "linux", GOARCH: "arm64"}, fakeBinary, fakeLicense, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	if asset.Name != "orker-linux-arm64.tar.gz" {
		t.Errorf("Pack named its asset %q, want the target's Asset()", asset.Name)
	}
}

// TestTarget_ExecutableName: the file inside the archive, and the one the
// images copy, is the binary's name, with .exe on Windows only.
func TestTarget_ExecutableName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		target dist.Target
		want   string
	}{
		{dist.Target{Binary: dist.Orkctl, GOOS: "windows", GOARCH: "amd64"}, "orkctl.exe"},
		{dist.Target{Binary: dist.Orkctl, GOOS: "darwin", GOARCH: "arm64"}, "orkctl"},
		{dist.Target{Binary: dist.Orker, GOOS: "linux", GOARCH: "amd64"}, "orker"},
	}
	for _, tc := range tests {
		if got := tc.target.Executable(); got != tc.want {
			t.Errorf("%s: Executable() = %q, want %q", tc.target, got, tc.want)
		}
	}
}

// TestTarGz_SortsAndValidatesEntries: entries are written in byte order of
// their names, whatever order they arrive in, and a name that could escape the
// directory an archive is unpacked into, or that appears twice, is refused.
func TestTarGz_SortsAndValidatesEntries(t *testing.T) {
	t.Parallel()
	entries := []dist.Entry{
		{Name: "zeta", Data: []byte("z"), Mode: 0o644},
		{Name: "alpha", Data: []byte("a"), Mode: 0o755},
		{Name: "LICENSE", Data: []byte("l"), Mode: 0o644},
	}
	data, err := dist.TarGz(entries, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	headers, _ := readTarGz(t, data)
	var names []string
	for _, header := range headers {
		names = append(names, header.Name)
	}
	if want := []string{"LICENSE", "alpha", "zeta"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("entries %q, want %q", names, want)
	}
	if entries[0].Name != "zeta" {
		t.Error("TarGz reordered the caller's slice")
	}
	reversed := []dist.Entry{entries[2], entries[1], entries[0]}
	again, err := dist.TarGz(reversed, commitTime)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, again) {
		t.Error("the order entries arrive in changed the bytes")
	}
	for _, name := range []string{"", "/etc/passwd", "../x", "a/../b", "dir/", `a\b`, "a/b"} {
		if _, err := dist.TarGz([]dist.Entry{{Name: name, Data: []byte("x"), Mode: 0o644}}, commitTime); err == nil {
			t.Errorf("TarGz accepted the entry name %q", name)
		}
		if _, err := dist.Zip([]dist.Entry{{Name: name, Data: []byte("x"), Mode: 0o644}}, commitTime); err == nil {
			t.Errorf("Zip accepted the entry name %q", name)
		}
	}
	dup := []dist.Entry{entries[0], entries[0]}
	if _, err := dist.TarGz(dup, commitTime); err == nil {
		t.Error("TarGz accepted a duplicate name")
	}
	if _, err := dist.Zip(dup, commitTime); err == nil {
		t.Error("Zip accepted a duplicate name")
	}
}
