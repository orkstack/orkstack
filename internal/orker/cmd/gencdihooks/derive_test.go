package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/clearsign"
	"github.com/google/rpmpack"
	"github.com/ulikunitz/xz"
	"pault.ag/go/debian/deb"
)

// memRepo is a package repository the test builds byte for byte: real deb
// archives, real rpms (rpmpack), real metadata signed with a throwaway key.
// It stands in for NVIDIA's repository, which no unit test may reach; the
// nightly's -check run reads the real one.
type memRepo map[string][]byte

func (m memRepo) get(_ context.Context, rel string) ([]byte, error) {
	b, ok := m[rel]
	if !ok {
		return nil, fmt.Errorf("GET %s: 404 Not Found", rel)
	}
	return b, nil
}

// fixture is a repository with the given releases of PackageName, each for
// both arches and both packagings, and the hook bytes each build carries.
type fixture struct {
	repo        memRepo
	key         *openpgp.Entity
	fingerprint string
	hooks       map[string][]byte // "1.20.2/amd64/deb" -> hook bytes
}

func newKey(t *testing.T) *openpgp.Entity {
	t.Helper()
	e, err := openpgp.NewEntity("test repository", "", "repo@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func armoredPublic(t *testing.T, e *openpgp.Entity) []byte {
	t.Helper()
	var b bytes.Buffer
	w, err := armor.Encode(&b, openpgp.PublicKeyType, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Serialize(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func fingerprintOf(e *openpgp.Entity) string {
	return strings.ToUpper(hex.EncodeToString(e.PrimaryKey.Fingerprint))
}

func newFixture(t *testing.T, releases ...string) *fixture {
	t.Helper()
	f := &fixture{repo: memRepo{}, key: newKey(t), hooks: map[string][]byte{}}
	f.fingerprint = fingerprintOf(f.key)
	f.repo["gpgkey"] = armoredPublic(t, f.key)
	for _, a := range arches {
		var packages bytes.Buffer
		var primaryXML bytes.Buffer
		primaryXML.WriteString(`<?xml version="1.0"?><metadata xmlns="http://linux.duke.edu/metadata/common">`)
		for _, rel := range releases {
			hook := []byte("hook " + rel + " " + a.orker + " deb")
			f.hooks[rel+"/"+a.orker+"/deb"] = hook
			deb := buildDeb(t, a.deb, hook)
			name := fmt.Sprintf("%s_%s-1_%s.deb", PackageName, rel, a.deb)
			f.repo["stable/deb/"+a.deb+"/"+name] = deb
			sum := sha256.Sum256(deb)
			fmt.Fprintf(&packages, "Package: %s\nVersion: %s-1\nArchitecture: %s\nFilename: ./%s\nSHA256: %x\nDescription: base\n continued line\n\n",
				PackageName, rel, a.deb, name, sum)

			hook = []byte("hook " + rel + " " + a.orker + " rpm")
			f.hooks[rel+"/"+a.orker+"/rpm"] = hook
			compressor := "xz"
			if rel == "1.19.1" {
				compressor = "zstd" // as NVIDIA's 1.19.1 rpms are
			}
			r := buildRPM(t, rel, a.rpm, compressor, hook)
			rname := fmt.Sprintf("%s-%s-1.%s.rpm", PackageName, rel, a.rpm)
			f.repo["stable/rpm/"+a.rpm+"/"+rname] = r
			rsum := sha512.Sum512(r)
			fmt.Fprintf(&primaryXML, `<package type="rpm"><name>%s</name><arch>%s</arch><version epoch="0" ver="%s" rel="1"/>`+
				`<checksum type="sha512" pkgid="YES">%x</checksum><location href="%s"/></package>`, PackageName, a.rpm, rel, rsum, rname)
		}
		// Another package beside it is ignored.
		packages.WriteString("Package: libnvidia-container1\nVersion: 1.20.2-1\nFilename: ./libnvidia-container1_1.20.2-1_x.deb\nSHA256: 00\n\n")
		f.repo["stable/deb/"+a.deb+"/Packages"] = packages.Bytes()
		psum := sha512.Sum512(packages.Bytes())
		release := fmt.Sprintf("Origin: test\nCodename: stable\nSHA512:\n %x %d Packages\n %x 10 Packages.xz\n", psum, packages.Len(), sha512.Sum512(nil))
		f.repo["stable/deb/"+a.deb+"/InRelease"] = clearsignBytes(t, f.key, []byte(release))

		primaryXML.WriteString(`</metadata>`)
		gz := gzipBytes(t, primaryXML.Bytes())
		gsum := sha512.Sum512(gz)
		f.repo["stable/rpm/"+a.rpm+"/repodata/abc-primary.xml.gz"] = gz
		repomd := fmt.Sprintf(`<?xml version="1.0"?><repomd xmlns="http://linux.duke.edu/metadata/repo">`+
			`<data type="other"><checksum type="sha512">00</checksum><location href="repodata/x-other.xml.gz"/></data>`+
			`<data type="primary"><checksum type="sha512">%x</checksum><location href="repodata/abc-primary.xml.gz"/></data></repomd>`, gsum)
		f.repo["stable/rpm/"+a.rpm+"/repodata/repomd.xml"] = []byte(repomd)
		f.repo["stable/rpm/"+a.rpm+"/repodata/repomd.xml.asc"] = detachSign(t, f.key, []byte(repomd))
	}
	return f
}

func clearsignBytes(t *testing.T, e *openpgp.Entity, text []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w, err := clearsign.Encode(&b, e.PrivateKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(text); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func detachSign(t *testing.T, e *openpgp.Entity, msg []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := openpgp.ArmoredDetachSign(&b, e, bytes.NewReader(msg), nil); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// buildDeb is a deb, as dpkg-deb writes one (debian-binary, control.tar.xz,
// data.tar.xz), whose data member holds ./usr/bin/nvidia-cdi-hook.
func buildDeb(t *testing.T, arch string, hook []byte) []byte {
	t.Helper()
	return buildDebOf(t, arch, map[string][]byte{"./usr/bin/nvidia-ctk": []byte("ctk"), "./" + HookPath: hook})
}

// buildDebOf is a deb whose data member holds files.
func buildDebOf(t *testing.T, arch string, files map[string][]byte) []byte {
	t.Helper()
	control := tarXZ(t, map[string][]byte{"./control": []byte("Package: " + PackageName + "\nVersion: 1.0.0-1\nArchitecture: " + arch + "\nMaintainer: x\nDescription: x\n")})
	data := tarXZ(t, files)
	var b bytes.Buffer
	b.WriteString("!<arch>\n")
	for _, m := range []struct {
		name string
		body []byte
	}{{"debian-binary", []byte("2.0\n")}, {"control.tar.xz", control}, {"data.tar.xz", data}} {
		fmt.Fprintf(&b, "%-16s%-12d%-6d%-6d%-8s%-10d`\n", m.name, 0, 0, 0, "100644", len(m.body))
		b.Write(m.body)
		if len(m.body)%2 == 1 {
			b.WriteByte('\n')
		}
	}
	// The deb reads back as one, so the fixture is a real one.
	if d, err := deb.Load(bytes.NewReader(b.Bytes()), "fixture.deb"); err != nil {
		t.Fatalf("the fixture deb does not load: %v", err)
	} else {
		d.Close()
	}
	return b.Bytes()
}

// tarXZ is an xz-compressed tar of files, in name order.
func tarXZ(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var out bytes.Buffer
	xw, err := xz.NewWriter(&out)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(xw)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		body := files[name]
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := xw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// buildRPM is an rpm whose cpio payload, compressed with compressor, holds
// /usr/bin/nvidia-cdi-hook.
func buildRPM(t *testing.T, version, arch, compressor string, hook []byte) []byte {
	t.Helper()
	r, err := rpmpack.NewRPM(rpmpack.RPMMetaData{Name: PackageName, Version: version, Release: "1", Arch: arch, Compressor: compressor})
	if err != nil {
		t.Fatal(err)
	}
	r.AddFile(rpmpack.RPMFile{Name: "/usr/bin/nvidia-ctk", Body: []byte("ctk"), Mode: 0o100755})
	r.AddFile(rpmpack.RPMFile{Name: "/" + HookPath, Body: hook, Mode: 0o100755})
	var b bytes.Buffer
	if err := r.Write(&b); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// TestDeriveRows_EveryBuildAtOrAboveTheFloor: each release at or above the
// floor gives one row per arch and packaging, sorted, each with the digest
// its signed metadata gives and the SHA-256 of the hook it carries; a release
// below the floor and another package are left out.
func TestDeriveRows_EveryBuildAtOrAboveTheFloor(t *testing.T) {
	f := newFixture(t, "1.17.8", "1.19.1", "1.20.2", "1.20.10")
	rows, err := deriveRows(context.Background(), f.repo, "1.19.1", f.fingerprint)
	if err != nil {
		t.Fatalf("deriveRows: %v", err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.Version+"/"+r.Arch+"/"+r.Packaging)
		if want := sha256Hex(f.hooks[r.Version+"/"+r.Arch+"/"+r.Packaging]); r.BinarySHA256 != want {
			t.Errorf("%s/%s/%s: binary %s, want %s", r.Version, r.Arch, r.Packaging, r.BinarySHA256, want)
		}
		wantAlg := map[string]string{"deb": "sha256:", "rpm": "sha512:"}[r.Packaging]
		if !strings.HasPrefix(r.PackageDigest, wantAlg) || !strings.HasPrefix(r.Package, Repository+"/stable/") {
			t.Errorf("%+v: digest or package URL is not the metadata's", r)
		}
	}
	want := []string{
		"1.19.1/amd64/deb", "1.19.1/amd64/rpm", "1.19.1/arm64/deb", "1.19.1/arm64/rpm",
		"1.20.2/amd64/deb", "1.20.2/amd64/rpm", "1.20.2/arm64/deb", "1.20.2/arm64/rpm",
		"1.20.10/amd64/deb", "1.20.10/amd64/rpm", "1.20.10/arm64/deb", "1.20.10/arm64/rpm",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("rows = %v\nwant %v", got, want)
	}
}

// TestDeriveRows_RefusesWhatTheSignatureDoesNotVouchFor: a key with another
// fingerprint, metadata signed by another key, and any file whose bytes
// differ from the digest the signed metadata gives refuse the whole table.
func TestDeriveRows_RefusesWhatTheSignatureDoesNotVouchFor(t *testing.T) {
	for name, tc := range map[string]struct {
		tamper func(t *testing.T, f *fixture)
		says   string
	}{
		"another key's fingerprint": {func(t *testing.T, f *fixture) { f.fingerprint = strings.Repeat("A", 40) }, "not the pinned"},
		"InRelease signed by another key": {func(t *testing.T, f *fixture) {
			f.repo["stable/deb/amd64/InRelease"] = clearsignBytes(t, newKey(t), []byte("Origin: x\nSHA512:\n 00 1 Packages\n"))
		}, "InRelease"},
		"InRelease edited after signing": {func(t *testing.T, f *fixture) {
			f.repo["stable/deb/amd64/InRelease"] = bytes.Replace(f.repo["stable/deb/amd64/InRelease"], []byte("Codename: stable"), []byte("Codename: stabl3"), 1)
		}, "InRelease"},
		"Packages edited": {func(t *testing.T, f *fixture) {
			f.repo["stable/deb/arm64/Packages"] = append(f.repo["stable/deb/arm64/Packages"], '\n')
		}, "Packages"},
		"a deb edited": {func(t *testing.T, f *fixture) {
			k := fmt.Sprintf("stable/deb/amd64/%s_1.20.2-1_amd64.deb", PackageName)
			f.repo[k] = append(f.repo[k], 0)
		}, "the signed metadata says"},
		"repomd.xml signed by another key": {func(t *testing.T, f *fixture) {
			f.repo["stable/rpm/x86_64/repodata/repomd.xml.asc"] = detachSign(t, newKey(t), f.repo["stable/rpm/x86_64/repodata/repomd.xml"])
		}, "repomd.xml"},
		"primary.xml.gz edited": {func(t *testing.T, f *fixture) {
			f.repo["stable/rpm/aarch64/repodata/abc-primary.xml.gz"] = gzipBytes(t, []byte("<metadata/>"))
		}, "primary.xml.gz"},
		"an rpm edited": {func(t *testing.T, f *fixture) {
			k := fmt.Sprintf("stable/rpm/x86_64/%s-1.20.2-1.x86_64.rpm", PackageName)
			f.repo[k] = append(f.repo[k], 0)
		}, "the signed metadata says"},
		"a file missing":      {func(t *testing.T, f *fixture) { delete(f.repo, "stable/rpm/aarch64/repodata/repomd.xml.asc") }, "404"},
		"repomd.xml unsigned": {func(t *testing.T, f *fixture) { f.repo["stable/rpm/x86_64/repodata/repomd.xml.asc"] = []byte("") }, "repomd.xml"},
		"a deb misfiled under another arch": {func(t *testing.T, f *fixture) {
			resign(t, f, "stable/deb/arm64", func(p []byte) []byte {
				return bytes.Replace(p, []byte("Architecture: arm64"), []byte("Architecture: amd64"), 1)
			})
		}, `is for "amd64", not arm64`},
		"an rpm misfiled under another arch": {func(t *testing.T, f *fixture) {
			resignPrimary(t, f, "aarch64", func(x []byte) []byte {
				return bytes.Replace(x, []byte("<arch>aarch64</arch>"), []byte("<arch>x86_64</arch>"), 1)
			})
		}, `is for "x86_64", not aarch64`},
		"a re-release of one version": {func(t *testing.T, f *fixture) {
			resign(t, f, "stable/deb/amd64", func(p []byte) []byte {
				stanza := p[:bytes.Index(p, []byte("\n\n"))+2]
				return append(p, bytes.Replace(stanza, []byte("1.20.2-1"), []byte("1.20.2-2"), 1)...)
			})
		}, "listed twice"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, "1.20.2")
			tc.tamper(t, f)
			rows, err := deriveRows(context.Background(), f.repo, "1.19.1", f.fingerprint)
			if err == nil || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("deriveRows = %d rows, %v; want it refused, naming %q", len(rows), err, tc.says)
			}
		})
	}
}

// resign edits dir's Packages and signs a new InRelease for it.
func resign(t *testing.T, f *fixture, dir string, edit func([]byte) []byte) {
	t.Helper()
	p := edit(f.repo[dir+"/Packages"])
	f.repo[dir+"/Packages"] = p
	release := fmt.Sprintf("Origin: test\nSHA512:\n %x %d Packages\n", sha512.Sum512(p), len(p))
	f.repo[dir+"/InRelease"] = clearsignBytes(t, f.key, []byte(release))
}

// resignPrimary edits arch's primary.xml and signs a new repomd.xml for it.
func resignPrimary(t *testing.T, f *fixture, arch string, edit func([]byte) []byte) {
	t.Helper()
	dir := "stable/rpm/" + arch + "/repodata/"
	zr, err := gzip.NewReader(bytes.NewReader(f.repo[dir+"abc-primary.xml.gz"]))
	if err != nil {
		t.Fatal(err)
	}
	var x bytes.Buffer
	if _, err := x.ReadFrom(zr); err != nil {
		t.Fatal(err)
	}
	gz := gzipBytes(t, edit(x.Bytes()))
	f.repo[dir+"abc-primary.xml.gz"] = gz
	repomd := fmt.Sprintf(`<repomd><data type="primary"><checksum type="sha512">%x</checksum><location href="repodata/abc-primary.xml.gz"/></data></repomd>`, sha512.Sum512(gz))
	f.repo[dir+"repomd.xml"] = []byte(repomd)
	f.repo[dir+"repomd.xml.asc"] = detachSign(t, f.key, []byte(repomd))
}

// TestDebHook_RefusesADuplicateOrNonRegularHook: the hook is hashed only when
// it is one regular file in the package; a second entry of its path (which
// dpkg would install instead) or a link in its place is refused.
func TestDebHook_RefusesADuplicateOrNonRegularHook(t *testing.T) {
	if _, err := debHook(buildDebOf(t, "amd64", map[string][]byte{"./" + HookPath: []byte("a"), "/" + HookPath: []byte("b")})); err == nil ||
		!strings.Contains(err.Error(), "more than once") {
		t.Fatalf("a duplicate hook = %v; want it refused", err)
	}
	if got, err := debHook(buildDeb(t, "amd64", []byte("only"))); err != nil || string(got) != "only" {
		t.Fatalf("one hook = %q, %v", got, err)
	}
}

// TestDeriveRows_RefusesAPackageOutsideItsDirectory: a Filename that is not
// one file name is refused, never fetched.
func TestDeriveRows_RefusesAPackageOutsideItsDirectory(t *testing.T) {
	f := newFixture(t, "1.20.2")
	k := "stable/deb/amd64/Packages"
	f.repo[k] = bytes.Replace(f.repo[k], []byte("Filename: ./"), []byte("Filename: ../../"), 1)
	release := fmt.Sprintf("Origin: test\nSHA512:\n %x %d Packages\n", sha512.Sum512(f.repo[k]), len(f.repo[k]))
	f.repo["stable/deb/amd64/InRelease"] = clearsignBytes(t, f.key, []byte(release))
	if _, err := deriveRows(context.Background(), f.repo, "1.19.1", f.fingerprint); err == nil || !strings.Contains(err.Error(), "not one file") {
		t.Fatalf("deriveRows = %v; want the path refused", err)
	}
}

// TestDeriveRows_NothingAtOrAboveTheFloor: a repository with no release the
// floor admits is an error, never an empty table.
func TestDeriveRows_NothingAtOrAboveTheFloor(t *testing.T) {
	f := newFixture(t, "1.17.8")
	if _, err := deriveRows(context.Background(), f.repo, "1.19.1", f.fingerprint); err == nil || !strings.Contains(err.Error(), "no "+PackageName) {
		t.Fatalf("deriveRows = %v; want an error", err)
	}
}

// TestRender_TheSameRowsGiveTheSameBytes: the table is a function of its
// rows and floor, and is gofmt'd Go of package orker.
func TestRender_TheSameRowsGiveTheSameBytes(t *testing.T) {
	f := newFixture(t, "1.19.1", "1.20.2")
	rows, err := deriveRows(context.Background(), f.repo, "1.19.1", f.fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	a, err := render(rows, "1.19.1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := render(rows, "1.19.1")
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("render is not deterministic: %v", err)
	}
	for _, want := range []string{"// Code generated by", "package orker", `const cdiHookFloor = "1.19.1"`, "var cdiHookTable = []cdiHookBuild{", rows[0].BinarySHA256} {
		if !bytes.Contains(a, []byte(want)) {
			t.Errorf("the table lacks %q", want)
		}
	}
}

// TestParseVersion: MAJOR.MINOR.PATCH with a package's release suffix; a
// pre-release, a missing part or a leading zero is refused.
func TestParseVersion(t *testing.T) {
	for in, want := range map[string]string{"1.20.2": "1.20.2", "1.20.2-1": "1.20.2", "10.0.0": "10.0.0", "0.0.0": "0.0.0",
		"1.20.999999999": "1.20.999999999"} {
		if v, err := parseVersion(in); err != nil || v.String() != want {
			t.Errorf("parseVersion(%q) = %v, %v; want %s", in, v, err, want)
		}
	}
	for _, in := range []string{"1.20", "1.20.2~rc.1-1", "1.020.2", "a.b.c", "", "1.20.2.1",
		"1.+0.0-0", "+1.20.2", "1.-0.2", "1.20.-2", " 1.20.2", "1. 20.2", "1.20.2 ", "1..2", "1.20.", "0x1.2.3", "1_0.2.3", "1.\u0662.3",
		"1.20.1000000000", "1.20.99999999999999999999"} {
		if v, err := parseVersion(in); err == nil {
			t.Errorf("parseVersion(%q) = %v; want an error", in, v)
		}
	}
	if compareVersions(mustParse(t, "1.20.10"), mustParse(t, "1.20.2")) <= 0 {
		t.Error("1.20.10 does not sort after 1.20.2")
	}
}

func mustParse(t *testing.T, s string) version {
	t.Helper()
	v, err := parseVersion(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestTableIsGenerated: the committed table opens with the generator's
// header, so it is never mistaken for a hand-edited file (the nightly's
// -check compares its rows with the real repository).
func TestTableIsGenerated(t *testing.T) {
	raw, err := os.ReadFile("../../cdi_hook_table.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte("// Code generated by go run ./internal/orker/cmd/gencdihooks; DO NOT EDIT.\n")) {
		t.Fatal("cdi_hook_table.go does not open with the generator's header")
	}
}

// FuzzReleaseMetadata holds the deb822 reader, the Release digest lookup and
// the version parser, which read what NVIDIA's repository serves, to never
// panic, and a parsed version to print back the release it was read from.
func FuzzReleaseMetadata(f *testing.F) {
	f.Add([]byte("Origin: x\nSHA512:\n ab 1 Packages\n\nPackage: p\nVersion: 1.20.2-1\n"), "1.20.2-1")
	f.Add([]byte("Package: a\n continued\n"), "1.2")
	f.Add([]byte(""), "~")
	f.Fuzz(func(t *testing.T, raw []byte, v string) {
		stanzas, err := deb822(raw)
		if err == nil {
			for _, s := range stanzas {
				_, _ = releaseSum(s.Get("SHA512"), "Packages")
			}
		}
		if got, err := parseVersion(v); err == nil {
			base, _, _ := strings.Cut(v, "-")
			if got.String() != base {
				t.Fatalf("parseVersion(%q) = %s, not %s", v, got, base)
			}
		}
	})
}

// TestFetchBuild_ExtractsOnlyWhatTheSignedChainVouchesFor: one build's hook,
// and any other file of its package, come out only after the signed
// metadata and the package's digest verify; a wrong pinned key or a build the
// repository does not list refuses.
func TestFetchBuild_ExtractsOnlyWhatTheSignedChainVouchesFor(t *testing.T) {
	f := newFixture(t, "1.19.1", "1.20.2")
	files, err := fetchBuild(context.Background(), f.repo, f.fingerprint, mustParse(t, "1.20.2"), "amd64", "deb", []string{HookPath, "usr/bin/nvidia-ctk"})
	if err != nil {
		t.Fatalf("fetchBuild: %v", err)
	}
	if !bytes.Equal(files[HookPath], f.hooks["1.20.2/amd64/deb"]) || string(files["usr/bin/nvidia-ctk"]) != "ctk" {
		t.Fatalf("files = %q", files)
	}
	rpm, err := fetchBuild(context.Background(), f.repo, f.fingerprint, mustParse(t, "1.19.1"), "arm64", "rpm", []string{HookPath})
	if err != nil || !bytes.Equal(rpm[HookPath], f.hooks["1.19.1/arm64/rpm"]) {
		t.Fatalf("rpm fetchBuild = %v, %v", rpm, err)
	}
	if _, err := fetchBuild(context.Background(), f.repo, strings.Repeat("A", 40), mustParse(t, "1.20.2"), "amd64", "deb", []string{HookPath}); err == nil ||
		!strings.Contains(err.Error(), "not the pinned") {
		t.Errorf("another fingerprint: %v", err)
	}
	if _, err := fetchBuild(context.Background(), f.repo, f.fingerprint, mustParse(t, "1.20.3"), "amd64", "deb", []string{HookPath}); err == nil ||
		!strings.Contains(err.Error(), "lists no") {
		t.Errorf("an unlisted release: %v", err)
	}
	if _, err := fetchBuild(context.Background(), f.repo, f.fingerprint, mustParse(t, "1.20.2"), "amd64", "deb", []string{"usr/bin/absent"}); err == nil {
		t.Errorf("a file the package lacks was returned")
	}
	k := fmt.Sprintf("stable/deb/amd64/%s_1.20.2-1_amd64.deb", PackageName)
	f.repo[k] = append(f.repo[k], 0)
	if _, err := fetchBuild(context.Background(), f.repo, f.fingerprint, mustParse(t, "1.20.2"), "amd64", "deb", []string{HookPath}); err == nil ||
		!strings.Contains(err.Error(), "the signed metadata says") {
		t.Errorf("a tampered package: %v", err)
	}
}

// TestRepository_OnlyAnUnservingRepositoryIsANetworkError: a connection or
// timeout, a 5xx and a 429 are networkErrors, which -fetch exits 77 on (a
// missing prerequisite); a 404 or 403 for a file, a certificate nobody trusts, a
// redirect elsewhere, and a bad signature or digest, are plain failures (exit 1).
func TestRepository_OnlyAnUnservingRepositoryIsANetworkError(t *testing.T) {
	status := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	r := &repository{base: srv.URL, client: srv.Client()}
	for code, network := range map[int]bool{404: false, 403: false, 410: false, 429: true, 500: true, 502: true, 503: true} {
		status = code
		_, err := r.get(context.Background(), "x")
		if err == nil || errors.As(err, new(*networkError)) != network {
			t.Errorf("HTTP %d: %v, networkError want %v", code, err, network)
		}
	}
	srv.Close()
	if _, err := r.get(context.Background(), "x"); !errors.As(err, new(*networkError)) {
		t.Errorf("a closed server: %v, want a networkError", err)
	}
	untrusted := httptest.NewTLSServer(http.NotFoundHandler())
	defer untrusted.Close()
	r = &repository{base: untrusted.URL, client: &http.Client{}}
	if _, err := r.get(context.Background(), "x"); err == nil || errors.As(err, new(*networkError)) || !strings.Contains(err.Error(), "x509") {
		t.Errorf("an untrusted certificate: %v, want a plain failure that says so", err)
	}
	elsewhere := httptest.NewServer(http.RedirectHandler("https://elsewhere.invalid/", http.StatusFound))
	defer elsewhere.Close()
	r = &repository{base: elsewhere.URL, client: &http.Client{CheckRedirect: sameHostHTTPS}}
	if _, err := r.get(context.Background(), "x"); err == nil || errors.As(err, new(*networkError)) || !strings.Contains(err.Error(), "a redirect to") {
		t.Errorf("a redirect elsewhere: %v, want a plain failure that says so", err)
	}
	f := newFixture(t, "1.20.2")
	_, err := fetchBuild(context.Background(), f.repo, strings.Repeat("A", 40), mustParse(t, "1.20.2"), "amd64", "deb", []string{HookPath})
	if err == nil || errors.As(err, new(*networkError)) {
		t.Errorf("a wrong fingerprint: %v, want a failure that is no networkError", err)
	}
	_, err = fetchBuild(context.Background(), f.repo, f.fingerprint, mustParse(t, "1.20.3"), "amd64", "deb", []string{HookPath})
	if err == nil || errors.As(err, new(*networkError)) || !strings.Contains(err.Error(), "lists no") {
		t.Errorf("a delisted build: %v, want a plain failure", err)
	}
}

func TestRunFetch_RefusesWhatItWouldOverwrite(t *testing.T) {
	out := filepath.Join(t.TempDir(), "o")
	for _, extra := range [][]string{{"usr/bin/x", "opt/x"}, {"usr/bin/nvidia-cdi-hook"}, {"opt/nvidia-cdi-hook"}} {
		if err := runFetch("1.20.2", "amd64", "deb", out, extra); err == nil || !strings.Contains(err.Error(), "-extra") {
			t.Errorf("-extra %v: %v, want a refusal before any network", extra, err)
		}
	}
}

// TestRowsScript_ParsesWhatRenderWrites: scripts/nvidia-hooks-rows.sh, which the
// proofs' cache is verified with, reads every row the generator writes (an
// arch's rows come out as "version packaging sha256"), and refuses a table whose
// format it no longer parses rather than skipping a row.
func TestRowsScript_ParsesWhatRenderWrites(t *testing.T) {
	rows := []Row{
		{Version: "1.19.1", Arch: "amd64", Packaging: "deb", Package: Repository + "/stable/deb/amd64/a.deb", PackageDigest: "sha256:" + strings.Repeat("1", 64), BinarySHA256: strings.Repeat("a", 64)},
		{Version: "1.19.1", Arch: "arm64", Packaging: "rpm", Package: Repository + "/stable/rpm/aarch64/a.rpm", PackageDigest: "sha512:" + strings.Repeat("2", 128), BinarySHA256: strings.Repeat("b", 64)},
		{Version: "1.20.2", Arch: "amd64", Packaging: "rpm", Package: Repository + "/stable/rpm/x86_64/c.rpm", PackageDigest: "sha512:" + strings.Repeat("3", 128), BinarySHA256: strings.Repeat("c", 64)},
	}
	table, err := render(rows, "1.19.1")
	if err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs("../../../../scripts/nvidia-hooks-rows.sh")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "table.go")
	if err := os.WriteFile(file, table, 0o644); err != nil {
		t.Fatal(err)
	}
	run := func(path, arch string) (string, error) {
		out, err := exec.Command("bash", script, path, arch).Output()
		return string(out), err
	}
	got, err := run(file, "amd64")
	want := "1.19.1 deb " + strings.Repeat("a", 64) + "\n1.20.2 rpm " + strings.Repeat("c", 64) + "\n"
	if err != nil || got != want {
		t.Fatalf("amd64 rows = %q, %v; want %q", got, err, want)
	}
	if got, err := run(file, "riscv64"); err != nil || got != "" {
		t.Errorf("an arch with no row = %q, %v; want nothing and no error (the caller refuses it)", got, err)
	}
	// A row in a shape it does not parse fails the script.
	bad := strings.Replace(string(table), ", Packaging:", ",  Packaging:", 1)
	badFile := filepath.Join(t.TempDir(), "bad.go")
	if err := os.WriteFile(badFile, []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := run(badFile, "amd64"); err == nil {
		t.Error("a table with an unparsable row was read")
	}
}

// TestHooksCacheScript: scripts/nvidia-hooks-cache.sh's verdicts over a fetch
// that is a fake, since neither an unreachable repository nor a tampered build
// can be produced on demand: a verified hit and a verified fetch print the
// directory; every binary of a hit is hashed again; an unreachable repository
// is 77 and any other failure 1, with the fetch's own words last, and neither
// leaves anything that looks present.
func TestHooksCacheScript(t *testing.T) {
	binary := "the hook"
	sum := sha256.Sum256([]byte(binary))
	table, err := render([]Row{{Version: "1.20.2", Arch: runtime.GOARCH, Packaging: "deb", Package: Repository + "/stable/deb/" + runtime.GOARCH + "/a.deb",
		PackageDigest: "sha256:" + strings.Repeat("1", 64), BinarySHA256: hex.EncodeToString(sum[:])}}, "1.19.1")
	if err != nil {
		t.Fatal(err)
	}
	const unreachable, refused = "gencdihooks: dial tcp: no such host", "gencdihooks: the signature does not verify"
	for name, tc := range map[string]struct {
		hit   string // what a cached binary holds; "" for no cached directory
		fetch string // the fetch script's body
		want  int
		says  string
	}{
		"a verified hit":     {hit: binary, fetch: "exit 9", want: 0},
		"a tampered hit":     {hit: "another", fetch: "exit 9", want: 1, says: "the table's row"},
		"a verified fetch":   {fetch: `mkdir -p "$1/1.20.2-` + runtime.GOARCH + `-deb"; printf '%s' "` + binary + `" >"$1/1.20.2-` + runtime.GOARCH + `-deb/nvidia-cdi-hook"`, want: 0},
		"a fetch that lies":  {fetch: `mkdir -p "$1/1.20.2-` + runtime.GOARCH + `-deb"; echo other >"$1/1.20.2-` + runtime.GOARCH + `-deb/nvidia-cdi-hook"`, want: 1, says: "the table's row"},
		"unreachable":        {fetch: `mkdir "$1"; echo "` + unreachable + `" >&2; exit 77`, want: 77, says: unreachable},
		"refused":            {fetch: `mkdir "$1"; echo "` + refused + `" >&2; exit 1`, want: 1, says: refused},
		"a directory, empty": {hit: "-", fetch: "exit 9", want: 1, says: "absent"},
	} {
		t.Run(name, func(t *testing.T) {
			repo, cache := t.TempDir(), t.TempDir()
			write := func(path, body string, mode os.FileMode) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(body), mode); err != nil {
					t.Fatal(err)
				}
			}
			for _, script := range []string{"nvidia-hooks-cache.sh", "nvidia-hooks-rows.sh"} {
				raw, err := os.ReadFile("../../../../scripts/" + script)
				if err != nil {
					t.Fatal(err)
				}
				write(filepath.Join(repo, "scripts", script), string(raw), 0o755)
			}
			write(filepath.Join(repo, "scripts", "nvidia-hooks-fetch.sh"), "#!/usr/bin/env bash\n"+tc.fetch+"\n", 0o755)
			write(filepath.Join(repo, "internal/orker/cdi_hook_table.go"), string(table), 0o644)
			tableSum := sha256.Sum256(table)
			dir := filepath.Join(cache, hex.EncodeToString(tableSum[:]))
			switch tc.hit {
			case "":
			case "-":
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
			default:
				write(filepath.Join(dir, "1.20.2-"+runtime.GOARCH+"-deb", "nvidia-cdi-hook"), tc.hit, 0o755)
			}
			cmd := exec.Command("bash", filepath.Join(repo, "scripts", "nvidia-hooks-cache.sh"))
			cmd.Env = append(os.Environ(), "NVIDIA_HOOKS_CACHE="+cache)
			var stderr strings.Builder
			cmd.Stderr = &stderr
			out, _ := cmd.Output()
			if got := cmd.ProcessState.ExitCode(); got != tc.want {
				t.Fatalf("exit %d, want %d: %s", got, tc.want, stderr.String())
			}
			if tc.want == 0 {
				if strings.TrimSpace(string(out)) != dir {
					t.Fatalf("printed %q, want %s", out, dir)
				}
				return
			}
			lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
			if len(out) != 0 || !strings.Contains(lines[len(lines)-1], tc.says) {
				t.Fatalf("printed %q; last said %q, want %q", out, lines[len(lines)-1], tc.says)
			}
			if left, _ := os.ReadDir(cache); tc.hit == "" && strings.HasPrefix(tc.says, "gencdihooks") && len(left) != 0 {
				t.Fatalf("a failed fetch left %v", left)
			}
		})
	}
}
