package dist

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"time"
)

// Entry is one file in a release archive. Archives are flat: there are no
// directories, because nothing in a release needs one.
type Entry struct {
	// Name is the file's name in the archive: a single path element.
	Name string
	Data []byte
	// Mode is the permission bits: 0o755 for a binary, 0o644 for the rest.
	Mode int64
}

// Asset is a packed release archive and the name it is published under.
type Asset struct {
	Name string
	Data []byte
}

const (
	modeExecutable = 0o755
	modeFile       = 0o644
)

// Pack archives a built binary together with the LICENSE for its target: a
// .zip holding orkctl.exe for Windows, a .tar.gz for everything else. mtime is
// the commit time and stamps every entry; nothing else about the clock or the
// host reaches the bytes, so packing the same binary twice gives the same
// archive.
func Pack(target Target, binary, license []byte, mtime time.Time) (Asset, error) {
	if len(binary) == 0 {
		return Asset{}, fmt.Errorf("%s: the built binary is empty", target)
	}
	if len(license) == 0 {
		return Asset{}, fmt.Errorf("%s: the LICENSE is empty or missing; every archive carries it", target)
	}
	entries := []Entry{
		{Name: target.Executable(), Data: binary, Mode: modeExecutable},
		{Name: "LICENSE", Data: license, Mode: modeFile},
	}
	pack := TarGz
	if target.isWindows() {
		pack = Zip
	}
	data, err := pack(entries, mtime)
	if err != nil {
		return Asset{}, fmt.Errorf("%s: %w", target, err)
	}
	return Asset{Name: target.Asset(), Data: data}, nil
}

// sortedEntries validates entries and returns them in byte order of their
// names, without touching the caller's slice.
func sortedEntries(entries []Entry, mtime time.Time) ([]Entry, error) {
	if mtime.IsZero() {
		return nil, errors.New("no commit time: an archive is stamped with it, never with the clock")
	}
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b Entry) int { return strings.Compare(a.Name, b.Name) })
	for i, entry := range sorted {
		if !singleElement(entry.Name) {
			return nil, fmt.Errorf("archive entry name %q is not a single file name", entry.Name)
		}
		if i > 0 && sorted[i-1].Name == entry.Name {
			return nil, fmt.Errorf("archive entry %q appears twice", entry.Name)
		}
	}
	return sorted, nil
}

// TarGz writes entries as a gzip-compressed tar stream that depends only on
// its arguments: entries sorted by name, owned by uid and gid 0 with no owner
// names, every time the commit time (whole seconds, UTC), plain USTAR headers
// with no extended records, and a gzip header with no file name, comment or
// time. The gzip level is fixed, so the same Go toolchain reproduces the bytes.
func TarGz(entries []Entry, mtime time.Time) ([]byte, error) {
	sorted, err := sortedEntries(entries, mtime)
	if err != nil {
		return nil, err
	}
	items := make([]tarItem, len(sorted))
	for i, entry := range sorted {
		items[i] = tarItem{name: entry.Name, data: entry.Data, mode: entry.Mode}
	}
	return writeTarGz(items, mtime)
}

// modeDirectory is the permission bits of every directory in a tree archive.
const modeDirectory = 0o755

// TarGzTree writes entries as the gzip-compressed tar stream of one directory
// tree, in the same deterministic form as TarGz: every entry sits under the
// single top-level directory root, an Entry's Name is its slash-separated path
// below it, and the directories are written as entries of their own
// (root-owned, mode 0755) ahead of what they hold. Entries are sorted by full
// path, so the bytes do not depend on the order they are given in.
func TarGzTree(root string, entries []Entry, mtime time.Time) ([]byte, error) {
	if mtime.IsZero() {
		return nil, errors.New("no commit time: an archive is stamped with it, never with the clock")
	}
	if !singleElement(root) {
		return nil, fmt.Errorf("archive root %q is not a single directory name", root)
	}
	if len(entries) == 0 {
		return nil, errors.New("nothing to archive")
	}
	files := map[string]bool{}
	dirs := map[string]bool{root + "/": true}
	for _, entry := range entries {
		parts := strings.Split(entry.Name, "/")
		for _, part := range parts {
			if !singleElement(part) {
				return nil, fmt.Errorf("archive entry name %q is not a clean relative path", entry.Name)
			}
		}
		full := root + "/" + entry.Name
		if files[full] {
			return nil, fmt.Errorf("archive entry %q appears twice", entry.Name)
		}
		files[full] = true
		for i := 1; i < len(parts); i++ {
			dirs[root+"/"+strings.Join(parts[:i], "/")+"/"] = true
		}
	}
	items := make([]tarItem, 0, len(files)+len(dirs))
	for dir := range dirs {
		if files[strings.TrimSuffix(dir, "/")] {
			return nil, fmt.Errorf("archive entry %q is both a file and a directory", strings.TrimSuffix(dir, "/"))
		}
		items = append(items, tarItem{name: dir, mode: modeDirectory, dir: true})
	}
	for _, entry := range entries {
		items = append(items, tarItem{name: root + "/" + entry.Name, data: entry.Data, mode: entry.Mode})
	}
	slices.SortFunc(items, func(a, b tarItem) int { return strings.Compare(a.name, b.name) })
	return writeTarGz(items, mtime)
}

// singleElement reports whether name is one path element: not empty, not "."
// or "..", and with no separator or NUL in it.
func singleElement(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}

// tarItem is one entry of a tar stream; a directory's name ends in "/".
type tarItem struct {
	name string
	data []byte
	mode int64
	dir  bool
}

// writeTarGz writes items, already validated and in order, as the stream
// TarGz documents.
func writeTarGz(items []tarItem, mtime time.Time) ([]byte, error) {
	stamp := mtime.UTC().Truncate(time.Second)
	var out bytes.Buffer
	gz, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("start gzip: %w", err)
	}
	// The zero Header is what makes the stream repeatable: no Name, no
	// Comment and a zero ModTime, which gzip writes as 0 and not as now.
	gz.Header = gzip.Header{OS: 255}
	writer := tar.NewWriter(gz)
	for _, item := range items {
		header := &tar.Header{
			Typeflag: tar.TypeReg,
			Name:     item.name,
			Mode:     item.mode,
			Size:     int64(len(item.data)),
			ModTime:  stamp,
			Format:   tar.FormatUSTAR,
		}
		if item.dir {
			header.Typeflag = tar.TypeDir
		}
		if err := writer.WriteHeader(header); err != nil {
			return nil, fmt.Errorf("write the header of %s: %w", item.name, err)
		}
		if _, err := writer.Write(item.data); err != nil {
			return nil, fmt.Errorf("write %s: %w", item.name, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("finish tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("finish gzip: %w", err)
	}
	return out.Bytes(), nil
}

// Zip writes entries as a zip archive that depends only on its arguments:
// entries sorted by name, deflated at a fixed level, every time the commit
// time in UTC, and the permission bits recorded.
func Zip(entries []Entry, mtime time.Time) ([]byte, error) {
	sorted, err := sortedEntries(entries, mtime)
	if err != nil {
		return nil, err
	}
	stamp := mtime.UTC().Truncate(time.Second)
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	writer.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(w, flate.BestCompression)
	})
	for _, entry := range sorted {
		header := &zip.FileHeader{Name: entry.Name, Method: zip.Deflate, Modified: stamp}
		header.SetMode(fsMode(entry.Mode))
		file, err := writer.CreateHeader(header)
		if err != nil {
			return nil, fmt.Errorf("write the header of %s: %w", entry.Name, err)
		}
		if _, err := file.Write(entry.Data); err != nil {
			return nil, fmt.Errorf("write %s: %w", entry.Name, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("finish zip: %w", err)
	}
	return out.Bytes(), nil
}

// fsMode is a permission mask as a file mode.
func fsMode(mode int64) fs.FileMode {
	return fs.FileMode(mode).Perm()
}
