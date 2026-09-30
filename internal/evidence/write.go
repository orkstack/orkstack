package evidence

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Source is what the engine hands the writer: the manifest it built and a
// way to open each present member's bytes. The writer owns tar, gzip,
// hashing and the abort rule; it never touches a store directly.
type Source interface {
	// Open returns the bytes of one present member. For a member with a
	// recorded digest the writer verifies size and sha256 as it copies and
	// returns ErrMemberMismatch when they differ; ErrMemberVanished wraps a
	// NotFound from a member the manifest already lists as present.
	Open(ctx context.Context, m Member) (io.ReadCloser, error)
}

var (
	// ErrMemberMismatch reports bytes that do not hash, or do not measure,
	// to what the manifest already promised.
	ErrMemberMismatch = errors.New("evidence: member bytes do not match the recorded reference")
	// ErrMemberVanished reports a member the manifest lists as present whose
	// object answered NotFound when it was opened for streaming.
	ErrMemberVanished = errors.New("evidence: member disappeared between manifest and stream")
)

// MemberError names the member a Write failed on. It unwraps to
// ErrMemberMismatch, ErrMemberVanished, or the Source's own error.
type MemberError struct {
	Member Member
	Err    error
}

func (e *MemberError) Error() string {
	return fmt.Sprintf("evidence: member %s: %v", e.Member.Path, e.Err)
}

func (e *MemberError) Unwrap() error { return e.Err }

// entryMode is every tar entry's permission bits.
const entryMode = 0o644

// Write streams the bundle to w: MANIFEST.json, then every present member in
// manifest order. w is the chunking writer the handler supplies. On any error
// it returns without finishing the archive — no tar trailer, no gzip footer —
// and the caller must not send `last`: a torn bundle without `last` is not a
// bundle.
func Write(ctx context.Context, w io.Writer, m *Manifest, src Source) error {
	if m == nil {
		return errors.New("evidence: write: nil manifest")
	}
	document, err := m.MarshalJSON()
	if err != nil {
		return err
	}
	modTime := m.ExportedAt.UTC()
	gz, err := gzip.NewWriterLevel(w, gzip.DefaultCompression)
	if err != nil {
		return fmt.Errorf("evidence: gzip: %w", err)
	}
	gz.ModTime = modTime
	gz.Name = ""
	tw := tar.NewWriter(gz)
	header := func(name string, size int64) *tar.Header {
		return &tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Size:     size,
			Mode:     entryMode,
			ModTime:  modTime,
			Uid:      0,
			Gid:      0,
		}
	}
	if err := tw.WriteHeader(header(ManifestPath, int64(len(document)))); err != nil {
		return fmt.Errorf("evidence: write %s: %w", ManifestPath, err)
	}
	if _, err := tw.Write(document); err != nil {
		return fmt.Errorf("evidence: write %s: %w", ManifestPath, err)
	}
	for _, member := range m.Members {
		if member.State != StatePresent {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tw.WriteHeader(header(member.Path, member.SizeBytes)); err != nil {
			return &MemberError{Member: member, Err: err}
		}
		if err := copyMember(ctx, tw, member, src); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("evidence: finish tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("evidence: finish gzip: %w", err)
	}
	return nil
}

// copyMember copies exactly member.SizeBytes bytes of the member into tw,
// hashing as it goes, and refuses a stream that ends short, runs long, or
// hashes to anything but member.SHA256.
func copyMember(ctx context.Context, tw io.Writer, member Member, src Source) error {
	reader, err := src.Open(ctx, member)
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return &MemberError{Member: member, Err: fmt.Errorf("%w: %w", ErrMemberVanished, err)}
		}
		return &MemberError{Member: member, Err: err}
	}
	defer reader.Close()
	hash := sha256.New()
	copied, err := io.CopyN(io.MultiWriter(tw, hash), reader, member.SizeBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return &MemberError{Member: member, Err: err}
	}
	if copied != member.SizeBytes {
		return &MemberError{Member: member, Err: fmt.Errorf("%w: read %d bytes, recorded %d",
			ErrMemberMismatch, copied, member.SizeBytes)}
	}
	// One byte more than recorded is a different object.
	var extra [1]byte
	if n, _ := io.ReadFull(reader, extra[:]); n > 0 {
		return &MemberError{Member: member, Err: fmt.Errorf("%w: more than the recorded %d bytes",
			ErrMemberMismatch, member.SizeBytes)}
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != member.SHA256 {
		return &MemberError{Member: member, Err: fmt.Errorf("%w: sha256 %s, recorded %s",
			ErrMemberMismatch, got, member.SHA256)}
	}
	return nil
}
