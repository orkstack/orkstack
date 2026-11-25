package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/clearsign"
	rpmutils "github.com/sassoftware/go-rpmutils"
	"pault.ag/go/debian/deb"
)

// Row is one build of the hook: the release and arch it is of, how it was
// packaged, the package it came from with the digest the signed metadata
// gives of it, and the SHA-256 of the binary itself.
type Row struct {
	Version   string // "1.20.2"
	Arch      string // "amd64" or "arm64", the Orker's spelling
	Packaging string // "deb" or "rpm"
	Package   string // the package's URL
	// PackageDigest is the digest the signed metadata gives, with its
	// algorithm: "sha256:…" for a deb (Packages), and for an rpm the one
	// primary.xml names (sha512 in NVIDIA's repository).
	PackageDigest string
	BinarySHA256  string

	v version
}

// getter reads one file of the repository.
type getter interface {
	get(ctx context.Context, rel string) ([]byte, error)
}

// arches are the Orker's arches, as each packaging's repository names them.
var arches = []struct{ orker, deb, rpm string }{
	{orker: "amd64", deb: "amd64", rpm: "x86_64"},
	{orker: "arm64", deb: "arm64", rpm: "aarch64"},
}

// deriveRows is every row at or above floor, sorted by version, arch and
// packaging, from a repository whose signing key has the primary fingerprint
// given.
func deriveRows(ctx context.Context, src getter, floor, fingerprint string) ([]Row, error) {
	floorV, err := parseVersion(floor)
	if err != nil {
		return nil, fmt.Errorf("the floor %q: %w", floor, err)
	}
	keyring, err := signingKey(ctx, src, fingerprint)
	if err != nil {
		return nil, err
	}
	var rows []Row
	for _, a := range arches {
		debs, err := debPackages(ctx, src, keyring, a.deb, floorV)
		if err != nil {
			return nil, err
		}
		rpms, err := rpmPackages(ctx, src, keyring, a.rpm, floorV)
		if err != nil {
			return nil, err
		}
		for _, p := range append(debs, rpms...) {
			body, err := src.get(ctx, p.rel)
			if err != nil {
				return nil, err
			}
			if err := p.digest.check(body); err != nil {
				return nil, fmt.Errorf("%s: %w", p.rel, err)
			}
			hook, err := p.extract(body)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", p.rel, err)
			}
			sum := sha256.Sum256(hook)
			rows = append(rows, Row{Version: p.version.String(), Arch: a.orker, Packaging: p.packaging,
				Package: Repository + "/" + p.rel, PackageDigest: p.digest.String(), BinarySHA256: hex.EncodeToString(sum[:]), v: p.version})
		}
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no %s release at or above %s in the repository", PackageName, floor)
	}
	slices.SortFunc(rows, func(a, b Row) int {
		if c := compareVersions(a.v, b.v); c != 0 {
			return c
		}
		return strings.Compare(a.Arch+"/"+a.Packaging, b.Arch+"/"+b.Packaging)
	})
	for i := 1; i < len(rows); i++ {
		if rows[i].v == rows[i-1].v && rows[i].Arch == rows[i-1].Arch && rows[i].Packaging == rows[i-1].Packaging {
			return nil, fmt.Errorf("%s %s %s %s is listed twice", PackageName, rows[i].Version, rows[i].Arch, rows[i].Packaging)
		}
	}
	return rows, nil
}

// fetchBuild is the files (paths in the package, no leading slash) of one
// build of PackageName: release v for the Orker's arch, packaged as
// packaging. It trusts exactly what deriveRows does: the pinned key, the
// signed metadata chain and the package's signed digest, and no more.
func fetchBuild(ctx context.Context, src getter, fingerprint string, v version, arch, packaging string, want []string) (map[string][]byte, error) {
	keyring, err := signingKey(ctx, src, fingerprint)
	if err != nil {
		return nil, err
	}
	for _, a := range arches {
		if a.orker != arch {
			continue
		}
		var list []pkg
		switch packaging {
		case "deb":
			list, err = debPackages(ctx, src, keyring, a.deb, v)
		case "rpm":
			list, err = rpmPackages(ctx, src, keyring, a.rpm, v)
		default:
			return nil, fmt.Errorf("packaging %q is not deb or rpm", packaging)
		}
		if err != nil {
			return nil, err
		}
		for _, p := range list {
			if p.version != v {
				continue
			}
			body, err := src.get(ctx, p.rel)
			if err != nil {
				return nil, err
			}
			if err := p.digest.check(body); err != nil {
				return nil, fmt.Errorf("%s: %w", p.rel, err)
			}
			out := map[string][]byte{}
			for _, w := range want {
				if out[w], err = p.extractAt(body, w); err != nil {
					return nil, fmt.Errorf("%s: %w", p.rel, err)
				}
			}
			return out, nil
		}
		return nil, fmt.Errorf("the signed repository lists no %s %s for %s (%s)", PackageName, v, arch, packaging)
	}
	return nil, fmt.Errorf("arch %q is not amd64 or arm64", arch)
}

// signingKey is the repository's key, once its primary fingerprint is the
// pinned one.
func signingKey(ctx context.Context, src getter, fingerprint string) (openpgp.EntityList, error) {
	raw, err := src.get(ctx, "gpgkey")
	if err != nil {
		return nil, err
	}
	keyring, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("the repository's signing key: %w", err)
	}
	if len(keyring) != 1 {
		return nil, fmt.Errorf("the repository's signing key: %d keys, want one", len(keyring))
	}
	got := strings.ToUpper(hex.EncodeToString(keyring[0].PrimaryKey.Fingerprint))
	if got != fingerprint {
		return nil, fmt.Errorf("the repository's signing key has fingerprint %s, not the pinned %s", got, fingerprint)
	}
	return keyring, nil
}

// pkg is one package to fetch and the digest it must have.
type pkg struct {
	rel       string
	version   version
	packaging string
	digest    digest
	extract   func([]byte) ([]byte, error)
	// extractAt is extract for any one path of the package (no leading slash).
	extractAt func(body []byte, want string) ([]byte, error)
}

// digest is an algorithm and the hex the signed metadata gives.
type digest struct{ alg, hex string }

func (d digest) String() string { return d.alg + ":" + d.hex }

func (d digest) check(body []byte) error {
	var got string
	switch d.alg {
	case "sha256":
		sum := sha256.Sum256(body)
		got = hex.EncodeToString(sum[:])
	case "sha512":
		sum := sha512.Sum512(body)
		got = hex.EncodeToString(sum[:])
	default:
		return fmt.Errorf("digest %q is not sha256 or sha512", d.alg)
	}
	if got != strings.ToLower(d.hex) {
		return fmt.Errorf("%s is %s, the signed metadata says %s", d.alg, got, d.hex)
	}
	return nil
}

// debPackages reads arch's signed InRelease, then the Packages it vouches
// for, and lists PackageName's releases at or above min.
func debPackages(ctx context.Context, src getter, keyring openpgp.EntityList, arch string, floorV version) ([]pkg, error) {
	dir := "stable/deb/" + arch
	raw, err := src.get(ctx, dir+"/InRelease")
	if err != nil {
		return nil, err
	}
	block, rest := clearsign.Decode(raw)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("%s/InRelease is not one clearsigned message", dir)
	}
	if _, err := block.VerifySignature(keyring, nil); err != nil {
		return nil, fmt.Errorf("%s/InRelease: %w", dir, err)
	}
	// The signed bytes, as the signature covers them (trailing whitespace
	// stripped, CRLF line ends), never the display plaintext.
	release, err := deb822(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s/InRelease: %w", dir, err)
	}
	if len(release) != 1 {
		return nil, fmt.Errorf("%s/InRelease: %d paragraphs, want one", dir, len(release))
	}
	fmt.Fprintf(os.Stderr, "gencdihooks: %s/InRelease is dated %s\n", dir, release[0].Get("Date"))
	want, err := releaseSum(release[0].Get("SHA512"), "Packages")
	if err != nil {
		return nil, fmt.Errorf("%s/InRelease: %w", dir, err)
	}
	packages, err := src.get(ctx, dir+"/Packages")
	if err != nil {
		return nil, err
	}
	if err := (digest{alg: "sha512", hex: want}).check(packages); err != nil {
		return nil, fmt.Errorf("%s/Packages: %w", dir, err)
	}
	stanzas, err := deb822(packages)
	if err != nil {
		return nil, fmt.Errorf("%s/Packages: %w", dir, err)
	}
	var out []pkg
	for _, s := range stanzas {
		if s.Get("Package") != PackageName {
			continue
		}
		if got := s.Get("Architecture"); got != arch {
			return nil, fmt.Errorf("%s/Packages: %s %s is for %q, not %s", dir, PackageName, s.Get("Version"), got, arch)
		}
		v, err := parseVersion(s.Get("Version"))
		if err != nil {
			return nil, fmt.Errorf("%s/Packages: %s: %w", dir, PackageName, err)
		}
		if compareVersions(v, floorV) < 0 {
			continue
		}
		file, err := oneFile(strings.TrimPrefix(s.Get("Filename"), "./"))
		sum := strings.ToLower(s.Get("SHA256"))
		if err != nil || len(sum) != 64 {
			return nil, fmt.Errorf("%s/Packages: %s %s: Filename %q or SHA256 %q is not one file and its digest", dir, PackageName, v, s.Get("Filename"), sum)
		}
		out = append(out, pkg{rel: dir + "/" + file, version: v, packaging: "deb", digest: digest{alg: "sha256", hex: sum}, extract: debHook, extractAt: debFile})
	}
	return out, nil
}

// oneFile is name when it is one file name, with no directory and no "..".
func oneFile(name string) (string, error) {
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.Contains(clean, "/") {
		return "", fmt.Errorf("%q is not one file name", name)
	}
	return clean, nil
}

// deb822 reads paragraphs of RFC 822 style fields separated by blank lines,
// continuation lines included, as Debian's InRelease and Packages are.
func deb822(raw []byte) ([]textproto.MIMEHeader, error) {
	r := textproto.NewReader(bufio.NewReader(bytes.NewReader(raw)))
	var out []textproto.MIMEHeader
	for {
		h, err := r.ReadMIMEHeader()
		if len(h) > 0 {
			out = append(out, h)
		}
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// releaseSum is the hex digest a Release field gives for name. The field is
// "hex size name" entries, one per continuation line, which textproto
// unfolds into one line of fields.
func releaseSum(field, name string) (string, error) {
	f := strings.Fields(field)
	if len(f)%3 != 0 {
		return "", fmt.Errorf("its digest list is not hex, size and name entries")
	}
	for i := 0; i < len(f); i += 3 {
		if f[i+2] == name {
			return strings.ToLower(f[i]), nil
		}
	}
	return "", fmt.Errorf("lists no digest of %s", name)
}

// repomd is the part of repomd.xml read: where primary.xml.gz is and its
// digest.
type repomd struct {
	Data []struct {
		Type     string `xml:"type,attr"`
		Checksum struct {
			Type  string `xml:"type,attr"`
			Value string `xml:",chardata"`
		} `xml:"checksum"`
		Location struct {
			Href string `xml:"href,attr"`
		} `xml:"location"`
	} `xml:"data"`
}

// primary is the part of primary.xml read.
type primary struct {
	Packages []struct {
		Name    string `xml:"name"`
		Arch    string `xml:"arch"`
		Version struct {
			Ver string `xml:"ver,attr"`
		} `xml:"version"`
		Checksum struct {
			Type  string `xml:"type,attr"`
			Value string `xml:",chardata"`
		} `xml:"checksum"`
		Location struct {
			Href string `xml:"href,attr"`
		} `xml:"location"`
	} `xml:"package"`
}

// rpmPackages reads arch's repomd.xml once its detached signature verifies,
// then the primary.xml.gz it vouches for, and lists PackageName's releases at
// or above min.
func rpmPackages(ctx context.Context, src getter, keyring openpgp.EntityList, arch string, floorV version) ([]pkg, error) {
	dir := "stable/rpm/" + arch
	md, err := src.get(ctx, dir+"/repodata/repomd.xml")
	if err != nil {
		return nil, err
	}
	sig, err := src.get(ctx, dir+"/repodata/repomd.xml.asc")
	if err != nil {
		return nil, err
	}
	if _, err := openpgp.CheckArmoredDetachedSignature(keyring, bytes.NewReader(md), bytes.NewReader(sig), nil); err != nil {
		return nil, fmt.Errorf("%s/repodata/repomd.xml: %w", dir, err)
	}
	var index repomd
	if err := xml.Unmarshal(md, &index); err != nil {
		return nil, fmt.Errorf("%s/repodata/repomd.xml: %w", dir, err)
	}
	var primaryRel string
	var primarySum digest
	for _, d := range index.Data {
		if d.Type == "primary" {
			primaryRel, primarySum = d.Location.Href, digest{alg: d.Checksum.Type, hex: strings.TrimSpace(d.Checksum.Value)}
		}
	}
	name, err := oneFile(strings.TrimPrefix(primaryRel, "repodata/"))
	if err != nil || !strings.HasPrefix(primaryRel, "repodata/") {
		return nil, fmt.Errorf("%s/repodata/repomd.xml: no primary data under repodata/ (%q)", dir, primaryRel)
	}
	gz, err := src.get(ctx, dir+"/repodata/"+name)
	if err != nil {
		return nil, err
	}
	if err := primarySum.check(gz); err != nil {
		return nil, fmt.Errorf("%s/%s: %w", dir, primaryRel, err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, fmt.Errorf("%s/%s: %w", dir, primaryRel, err)
	}
	var list primary
	if err := xml.NewDecoder(io.LimitReader(zr, maxFile)).Decode(&list); err != nil {
		return nil, fmt.Errorf("%s/%s: %w", dir, primaryRel, err)
	}
	var out []pkg
	for _, p := range list.Packages {
		if p.Name != PackageName {
			continue
		}
		if p.Arch != arch {
			return nil, fmt.Errorf("%s: %s %s is for %q, not %s", dir, PackageName, p.Version.Ver, p.Arch, arch)
		}
		v, err := parseVersion(p.Version.Ver)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", dir, PackageName, err)
		}
		if compareVersions(v, floorV) < 0 {
			continue
		}
		file, err := oneFile(p.Location.Href)
		if err != nil {
			return nil, fmt.Errorf("%s: %s %s: location: %w", dir, PackageName, v, err)
		}
		out = append(out, pkg{rel: dir + "/" + file, version: v, packaging: "rpm",
			digest: digest{alg: p.Checksum.Type, hex: strings.TrimSpace(p.Checksum.Value)}, extract: rpmHook, extractAt: rpmFile})
	}
	return out, nil
}

// debHook is HookPath out of a deb's data member, however it is
// compressed (pault.ag/go/debian reads xz, zst and gz).
func debHook(body []byte) ([]byte, error) { return debFile(body, HookPath) }

// debFile is the file want (no leading slash) out of a deb's data member.
func debFile(body []byte, want string) ([]byte, error) {
	d, err := deb.Load(bytes.NewReader(body), "package.deb")
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return tarFile(d.Data, want)
}

// tarFile is want out of a package's data tar. The whole archive is read: a
// path that appears more than once, of any type, is refused, since dpkg would
// install the last of them, not the one hashed.
func tarFile(t *tar.Reader, want string) ([]byte, error) {
	var hook []byte
	found := false
	for {
		h, err := t.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if entryPath(h.Name) != want {
			continue
		}
		if found {
			return nil, fmt.Errorf("%s appears more than once", want)
		}
		found = true
		if h.Typeflag != tar.TypeReg {
			return nil, fmt.Errorf("%s is not a regular file", want)
		}
		if hook, err = readBounded(t, want); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, fmt.Errorf("no %s", want)
	}
	return hook, nil
}

// rpmHook is HookPath out of an rpm's cpio payload, however it is compressed
// (go-rpmutils reads xz and zstd, as NVIDIA's rpms have used both).
func rpmHook(body []byte) ([]byte, error) { return rpmFile(body, HookPath) }

// rpmFile is want out of an rpm's cpio payload, under rpmHook's rules.
func rpmFile(body []byte, want string) ([]byte, error) {
	p, err := rpmutils.ReadRpm(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	pr, err := p.PayloadReaderExtended()
	if err != nil {
		return nil, fmt.Errorf("payload: %w", err)
	}
	// The whole payload is read: a path that appears more than once, of any
	// type, is refused, since rpm would install the last of them.
	var hook []byte
	found := false
	for {
		f, err := pr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if entryPath(f.Name()) != want {
			continue
		}
		if found {
			return nil, fmt.Errorf("%s appears more than once", want)
		}
		found = true
		if f.Mode()&0o170000 != 0o100000 || pr.IsLink() {
			return nil, fmt.Errorf("%s is not a regular file", want)
		}
		if hook, err = readBounded(pr, want); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, fmt.Errorf("no %s", want)
	}
	return hook, nil
}

// entryPath is an archive entry's path from the root, however the archive
// spells it: "./usr/bin/x" (NVIDIA's packages), "/usr/bin/x" or "usr/bin/x".
func entryPath(name string) string {
	return strings.TrimPrefix(path.Clean("/"+name), "/")
}

func readBounded(r io.Reader, name string) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxFile+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxFile {
		return nil, fmt.Errorf("%s is larger than %d bytes", name, maxFile)
	}
	return b, nil
}

// version is a toolkit release, MAJOR.MINOR.PATCH.
type version [3]int

func (v version) String() string { return fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2]) }

// parseVersion reads MAJOR.MINOR.PATCH, with a package's "-1" release suffix
// allowed and ignored; a pre-release (a "~" in it) is not a release.
func parseVersion(s string) (version, error) {
	if strings.Contains(s, "~") {
		return version{}, fmt.Errorf("%q is a pre-release", s)
	}
	base, _, _ := strings.Cut(s, "-")
	parts := strings.Split(base, ".")
	if len(parts) != 3 {
		return version{}, fmt.Errorf("%q is not MAJOR.MINOR.PATCH", s)
	}
	var v version
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || (len(p) > 1 && p[0] == '0') {
			return version{}, fmt.Errorf("%q is not MAJOR.MINOR.PATCH", s)
		}
		v[i] = n
	}
	return v, nil
}

func compareVersions(a, b version) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}
