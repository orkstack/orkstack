package dist

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// SumsFile is the name of the checksums file a release publishes.
const SumsFile = "SHA256SUMS"

// sumsName is the file names a SHA256SUMS line can carry without escaping.
// sha256sum -c reads a leading backslash as an escape, a "*" as binary mode and
// a newline as the end of a line, so a name that could be misread is refused
// rather than written in a form some checker would verify differently.
var sumsName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Sums renders the contents of SHA256SUMS for files, a map from name to bytes:
// one "<hex digest>  <name>" line per file, sorted by name in byte order, which
// is the format `sha256sum -c` verifies. Two spaces separate the digest and
// the name, as sha256sum writes in text mode.
func Sums(files map[string][]byte) ([]byte, error) {
	if len(files) == 0 {
		return nil, errors.New("no files to checksum: an empty SHA256SUMS would verify anything")
	}
	names := make([]string, 0, len(files))
	for name := range files {
		if !sumsName.MatchString(name) {
			return nil, fmt.Errorf("file name %q cannot be written to %s unambiguously", name, SumsFile)
		}
		names = append(names, name)
	}
	slices.Sort(names)
	var out strings.Builder
	for _, name := range names {
		sum := sha256.Sum256(files[name])
		out.WriteString(hex.EncodeToString(sum[:]))
		out.WriteString("  ")
		out.WriteString(name)
		out.WriteByte('\n')
	}
	return []byte(out.String()), nil
}

// WriteSums checksums the regular files directly in dir and writes SHA256SUMS
// beside them, returning the names it summed in order. SHA256SUMS itself (from
// an earlier run), directories such as bin/, and symbolic links are not
// release assets and are left out. It writes nothing when there is nothing to
// sum.
func WriteSums(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read the release directory: %w", err)
	}
	files := map[string][]byte{}
	for _, entry := range entries {
		if entry.Name() == SumsFile || !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		files[entry.Name()] = data
	}
	sums, err := Sums(files)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, SumsFile), sums, 0o644); err != nil {
		return nil, fmt.Errorf("write %s: %w", SumsFile, err)
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}
