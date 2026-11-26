package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"path"
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
	// The two below are an archive body of a resumed run whose event stream
	// skips a number it does not account for, or that is not an archive body.
	FailureEventSequence = "event sequence broken"
	FailureBadStream     = "not an archive body"
	// FailureDetailMismatch is a resolution whose detail member is absent, or
	// is not the detail its record's bytes name.
	FailureDetailMismatch = "detail is not the one its record names"
)

// Report is what Verify found. Every member of the manifest is accounted
// for in exactly one of its lists: a member whose state is unknown, or the
// second listing of a path, is a Failure.
type Report struct {
	Manifest   *Manifest `json:"manifest"`
	Verified   []Member  `json:"verified"`           // present in manifest, present in file, digest and size equal
	Reclaimed  []Member  `json:"reclaimed"`          // listed reclaimed; absent from the file, as they must be
	Unrecorded []Member  `json:"unrecorded"`         // listed unrecorded; absent from the file
	Rejected   []Member  `json:"rejected"`           // listed rejected; absent from the file
	Withheld   []Member  `json:"withheld,omitempty"` // listed withheld; absent from the file
	// Missing are the records listed missing: absent from the file, and a gap
	// a reader states, since the run says their tasks succeeded.
	Missing  []Member  `json:"missing,omitempty"`
	Failures []Failure `json:"failures"` // anything else
	// EventGaps are the ranges of the run's event stream its archive does
	// not hold — lost from object storage before it was archived, or never
	// published — as the manifest names them. A notice, never a failure: the
	// bundle is exactly what the archive holds.
	EventGaps []EventGap `json:"event_gaps,omitempty"`
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
	report.EventGaps = manifest.EventGaps

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
		case StateWithheld:
			report.Withheld = append(report.Withheld, member)
		case StateMissing:
			report.Missing = append(report.Missing, member)
		default:
			report.Failures = append(report.Failures, Failure{Path: member.Path, Reason: FailureUnknownState,
				Got: string(member.State)})
		}
	}
	seen := map[string]bool{}
	// resolutions holds each resolution's bytes as streamed, to hold its
	// detail member to the detail they name.
	resolutions := map[string]string{}
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
		into := io.Writer(hash)
		var resolution *bytes.Buffer
		if entry.member.Kind == KindResolution {
			resolution = &bytes.Buffer{}
			into = io.MultiWriter(hash, &capped{w: resolution, left: maxResolutionMember})
		}
		var stream *streamCheck
		if len(manifest.Endings) > 0 && (entry.member.Kind == KindRun || entry.member.Kind == KindEnding) {
			stream = newStreamCheck(manifest, name)
			into = io.MultiWriter(hash, stream)
		}
		size, err := io.Copy(into, tr)
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
		case stream != nil:
			if failure := stream.end(); failure != nil {
				failure.Path = name
				failures = append(failures, *failure)
			}
		}
		if len(failures) > 0 {
			report.Failures = append(report.Failures, failures...)
			continue
		}
		if resolution != nil {
			resolutions[name] = detailDigestOf(resolution.Bytes(), size)
		}
		report.Verified = append(report.Verified, entry.member)
	}
	report.Failures = append(report.Failures, checkDetails(manifest.Members, resolutions)...)
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

// maxResolutionMember bounds the bytes of a resolution the verifier reads for
// its detail's digest: the engine writes a record of at most 64 KiB, and a
// larger one fails its detail check rather than pass unchecked.
const maxResolutionMember = 64 << 10

// capped writes at most left bytes into w and drops the rest.
type capped struct {
	w    io.Writer
	left int64
}

func (c *capped) Write(p []byte) (int, error) {
	if c.left > 0 {
		n := min(int64(len(p)), c.left)
		if _, err := c.w.Write(p[:n]); err != nil {
			return 0, err
		}
		c.left -= n
	}
	return len(p), nil
}

// detailDigestOf is the detail digest a resolution's bytes name, by the
// record's own key spellings (protojson's camelCase or proto name), or ""
// when they are too large, do not decode, or name none.
func detailDigestOf(body []byte, size int64) string {
	if size > maxResolutionMember {
		return ""
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(body, &fields); err != nil {
		return ""
	}
	for _, key := range []string{"detailSha256", "detail_sha256"} {
		var digest string
		if raw, ok := fields[key]; ok && json.Unmarshal(raw, &digest) == nil {
			return NormalizeDigest(digest)
		}
	}
	return ""
}

// checkDetails holds each verified resolution's detail member, beside it in
// the bundle, to the digest its record's bytes name: absent, another, or a
// resolution that names none is a failure named by the detail's path.
func checkDetails(members []Member, resolutions map[string]string) []Failure {
	digests := map[string]string{}
	for _, m := range members {
		if m.Kind == KindResolutionDetail && m.State == StatePresent {
			digests[m.Path] = NormalizeDigest(m.SHA256)
		}
	}
	var out []Failure
	for _, m := range members {
		want, ok := resolutions[m.Path]
		if m.Kind != KindResolution || !ok {
			continue
		}
		detailPath := path.Join(path.Dir(m.Path), "resolution-detail.json")
		got, listed := digests[detailPath]
		if want == "" || !listed || got != want {
			out = append(out, Failure{Path: detailPath, Reason: FailureDetailMismatch, Want: want, Got: got})
		}
	}
	return out
}
