package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// LocalLogStore keeps task log chunks on the filesystem, under exactly the S3
// key layout as directories — the same mirroring LocalRunArchive does, so the
// two backends answer identical reads and a local tree can be uploaded to a
// bucket verbatim.
//
// Everything goes through an os.Root, so a run or task id that tried to climb
// out of the prefix cannot: the ids are already encoded into single safe
// segments by LogChunkKey, and the root refuses anything that still escapes.
type LocalLogStore struct {
	dir  string
	root *os.Root
}

// compile-time contract check.
var _ LogStore = (*LocalLogStore)(nil)

// NewLocalLogStore opens (creating if needed) the log tree under dir.
func NewLocalLogStore(dir string) (*LocalLogStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("artifacts: local log store: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local log store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local log store: create %q: %w", abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local log store: open %q: %w", abs, err)
	}
	for _, subdir := range []string{localTempDir, LogPrefix} {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local log store: create %q: %w", subdir, err)
		}
	}
	return &LocalLogStore{dir: filepath.Clean(abs), root: root}, nil
}

// Dir returns the absolute root directory of the log tree.
func (s *LocalLogStore) Dir() string { return s.dir }

// Close releases the directory handle.
func (s *LocalLogStore) Close() error { return s.root.Close() }

// PutLogChunk writes one chunk through a temp file and a rename, so a reader
// tailing the directory never sees a half-written chunk. A partially readable
// chunk is worse than a late one: it would be sent, acknowledged by sequence,
// and never resent.
func (s *LocalLogStore) PutLogChunk(ctx context.Context, chunk LogChunk) error {
	key, err := LogChunkKey(chunk.LogRef, chunk.Seq, chunk.Truncated)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return fmt.Errorf("artifacts: local log chunk %q: create directory: %w", key, err)
	}
	tmpKey := path.Join(localTempDir, "log-"+randomToken())
	file, err := s.root.Create(tmpKey)
	if err != nil {
		return fmt.Errorf("artifacts: local log chunk %q: create temp: %w", key, err)
	}
	if _, err := file.Write(chunk.Data); err != nil {
		_ = file.Close()
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local log chunk %q: write: %w", key, err)
	}
	if err := file.Close(); err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local log chunk %q: close: %w", key, err)
	}
	if err := s.root.Rename(tmpKey, key); err != nil {
		_ = s.root.Remove(tmpKey)
		return fmt.Errorf("artifacts: local log chunk %q: commit: %w", key, err)
	}
	return nil
}

// ReadLogChunks returns the attempt's chunks after afterSeq, in order, bounded
// by both limit and maxBytes.
func (s *LocalLogStore) ReadLogChunks(
	ctx context.Context,
	ref LogRef,
	afterSeq int64,
	limit int,
	maxBytes int64,
) ([]LogChunk, error) {
	prefix, err := LogAttemptPrefix(ref)
	if err != nil {
		return nil, err
	}
	dir := strings.TrimSuffix(prefix, "/")
	entries, err := fs.ReadDir(s.root.FS(), dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// An attempt that wrote nothing reads empty. A task with no output
			// is ordinary, and the reader opens the stream before the first
			// chunk exists by design.
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: local log chunks %q: %w", dir, err)
	}
	found := make([]LogChunk, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		chunkRef, seq, truncated, ok := ParseLogChunkKey(dir + "/" + entry.Name())
		if !ok || chunkRef != ref || seq <= afterSeq {
			continue
		}
		found = append(found, LogChunk{LogRef: ref, Seq: seq, Truncated: truncated})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].Seq < found[j].Seq })
	if limit > 0 && limit < len(found) {
		found = found[:limit]
	}
	read := int64(0)
	for i := range found {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := LogChunkKey(found[i].LogRef, found[i].Seq, found[i].Truncated)
		if err != nil {
			return nil, err
		}
		data, err := fs.ReadFile(s.root.FS(), key)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Expired under a retention rule between the listing and the
				// read. Stop at the gap rather than skipping it: a caller
				// resumes from the last sequence it actually received.
				return found[:i], nil
			}
			return nil, fmt.Errorf("artifacts: local log chunk %q: %w", key, err)
		}
		found[i].Data = data
		// The budget is spent AFTER this chunk, never before it: the first
		// chunk is always returned whole, so a reader cannot be stalled by a
		// chunk larger than one page.
		read += int64(len(data))
		if maxBytes > 0 && read >= maxBytes {
			return found[:i+1], nil
		}
	}
	return found, nil
}

// LatestLogAttempt returns the highest attempt of a task that wrote a chunk.
func (s *LocalLogStore) LatestLogAttempt(ctx context.Context, runID, taskID string) (int32, error) {
	prefix, err := LogTaskPrefix(runID, taskID)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	dir := strings.TrimSuffix(prefix, "/")
	entries, err := fs.ReadDir(s.root.FS(), dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, fmt.Errorf("artifacts: task %q of run %q has no logs: %w",
				taskID, runID, ErrNotFound)
		}
		return 0, fmt.Errorf("artifacts: local log attempts %q: %w", dir, err)
	}
	latest := int32(0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		attempt, ok := parseFixedWidth(entry.Name(), logAttemptWidth)
		if !ok || attempt < 1 || attempt > (1<<31-1) {
			continue
		}
		// A directory is not evidence: an attempt whose chunks all expired
		// leaves one behind. The answer is the highest attempt that still has
		// something to read.
		chunks, err := s.ReadLogChunks(ctx, LogRef{
			RunID: runID, TaskID: taskID, Attempt: int32(attempt),
		}, 0, 1, 0)
		if err != nil {
			return 0, err
		}
		if len(chunks) > 0 && int32(attempt) > latest {
			latest = int32(attempt)
		}
	}
	if latest == 0 {
		return 0, fmt.Errorf("artifacts: task %q of run %q has no logs: %w", taskID, runID, ErrNotFound)
	}
	return latest, nil
}
