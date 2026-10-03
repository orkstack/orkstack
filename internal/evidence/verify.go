package evidence

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// The failure reasons Verify reports, complete.
const (
	FailureDigestMismatch   = "digest mismatch"
	FailureSizeMismatch     = "size mismatch"
	FailureMissing          = "missing from bundle"
	FailureNotInManifest    = "not in manifest"
	FailureOutOfOrder       = "out of order"
	FailureUnsafePath       = "unsafe path"
	FailureNotRegular       = "not a regular file"
	FailureManifestNotFirst = "manifest not first"
	// The two below are disagreements inside the manifest itself, which the
	// engine never writes but a hand-edited or foreign file can carry: a
	// member whose state is none of the four, and a path listed twice.
	// Without them such a member would land in no list of the Report.
	FailureUnknownState    = "unknown state"
	FailureDuplicateMember = "listed twice in manifest"
)

// Report is what Verify found. Every member of the manifest is accounted
// for in exactly one of its lists: a member whose state is unknown, or the
// second listing of a path, is a Failure.
type Report struct {
	Manifest   *Manifest `json:"manifest"`
	Verified   []Member  `json:"verified"`   // present in manifest, present in file, digest and size equal
	Reclaimed  []Member  `json:"reclaimed"`  // listed reclaimed; absent from the file, as they must be
	Unrecorded []Member  `json:"unrecorded"` // listed unrecorded; absent from the file
	Rejected   []Member  `json:"rejected"`   // listed rejected; absent from the file
	Failures   []Failure `json:"failures"`   // anything else
}

// OK reports whether the bundle matched its manifest in every respect.
func (r *Report) OK() bool { return r != nil && r.Manifest != nil && len(r.Failures) == 0 }

// Failure is one way the file disagrees with its manifest.
type Failure struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
	Want   string `json:"want,omitempty"`
	Got    string `json:"got,omitempty"`
}

// Verify reads one bundle from r and checks every member against its
// manifest. It never writes to disk and never follows a path: entries are
// hashed as they stream and compared by the manifest's path string.
//
// It returns an error only when the file cannot be read as a bundle at all —
// not gzip, a torn tar, an undecodable manifest, or a schema_version this
// tool does not know. Every disagreement between a readable file and its
// manifest is a Failure in the Report.
func Verify(r io.Reader) (*Report, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("evidence: bundle is not gzip: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	first, err := tr.Next()
	if err != nil {
		return nil, fmt.Errorf("evidence: read first entry: %w", err)
	}
	report := &Report{}
	if first.Name != ManifestPath {
		report.Failures = append(report.Failures, Failure{Path: first.Name, Reason: FailureManifestNotFirst})
		return report, nil
	}
	document, err := io.ReadAll(tr)
	if err != nil {
		return nil, fmt.Errorf("evidence: read %s: %w", ManifestPath, err)
	}
	manifest, err := ParseManifest(document)
	if err != nil {
		return nil, err
	}
	report.Manifest = manifest

	type slot struct {
		index  int
		member Member
	}
	present := map[string]slot{}
	listed := map[string]bool{}
	for i, member := range manifest.Members {
		if listed[member.Path] {
			report.Failures = append(report.Failures, Failure{Path: member.Path, Reason: FailureDuplicateMember})
			continue
		}
		listed[member.Path] = true
		switch member.State {
		case StatePresent:
			present[member.Path] = slot{index: i, member: member}
		case StateReclaimed:
			report.Reclaimed = append(report.Reclaimed, member)
		case StateUnrecorded:
			report.Unrecorded = append(report.Unrecorded, member)
		case StateRejected:
			report.Rejected = append(report.Rejected, member)
		default:
			report.Failures = append(report.Failures, Failure{Path: member.Path, Reason: FailureUnknownState,
				Got: string(member.State)})
		}
	}
	seen := map[string]bool{}
	furthest := -1
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("evidence: read bundle: %w", err)
		}
		name := header.Name
		if unsafePath(name) {
			report.Failures = append(report.Failures, Failure{Path: name, Reason: FailureUnsafePath})
			continue
		}
		if header.Typeflag != tar.TypeReg {
			report.Failures = append(report.Failures, Failure{Path: name, Reason: FailureNotRegular})
			continue
		}
		entry, ok := present[name]
		if !ok || seen[name] {
			report.Failures = append(report.Failures, Failure{Path: name, Reason: FailureNotInManifest})
			continue
		}
		seen[name] = true
		hash := sha256.New()
		size, err := io.Copy(hash, tr)
		if err != nil {
			return nil, fmt.Errorf("evidence: read %s: %w", name, err)
		}
		var failures []Failure
		if entry.index < furthest {
			failures = append(failures, Failure{Path: name, Reason: FailureOutOfOrder})
		} else {
			furthest = entry.index
		}
		got := hex.EncodeToString(hash.Sum(nil))
		switch {
		case size != entry.member.SizeBytes:
			failures = append(failures, Failure{Path: name, Reason: FailureSizeMismatch,
				Want: strconv.FormatInt(entry.member.SizeBytes, 10), Got: strconv.FormatInt(size, 10)})
		case got != entry.member.SHA256:
			failures = append(failures, Failure{Path: name, Reason: FailureDigestMismatch,
				Want: entry.member.SHA256, Got: got})
		}
		if len(failures) > 0 {
			report.Failures = append(report.Failures, failures...)
			continue
		}
		report.Verified = append(report.Verified, entry.member)
	}
	for i, member := range manifest.Members {
		if entry, ok := present[member.Path]; ok && entry.index == i && !seen[member.Path] {
			report.Failures = append(report.Failures, Failure{Path: member.Path, Reason: FailureMissing})
		}
	}
	return report, nil
}

// unsafePath reports an entry name no extractor should honour: absolute, a
// ".." segment, or a NUL byte.
func unsafePath(name string) bool {
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) {
		return true
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

// ReadManifest reads the MANIFEST.json of a bundle — its first member — and
// nothing else.
func ReadManifest(r io.Reader) (*Manifest, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("evidence: bundle is not gzip: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	first, err := tr.Next()
	if err != nil {
		return nil, fmt.Errorf("evidence: read first entry: %w", err)
	}
	if first.Name != ManifestPath {
		return nil, fmt.Errorf("evidence: first entry is %q, not %s", first.Name, ManifestPath)
	}
	document, err := io.ReadAll(tr)
	if err != nil {
		return nil, fmt.Errorf("evidence: read %s: %w", ManifestPath, err)
	}
	return ParseManifest(document)
}
