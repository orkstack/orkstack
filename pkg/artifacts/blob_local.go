package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// localBlobs is a local directory laid out exactly as the bucket's keys, as
// directories, so a local tree can be uploaded to a bucket verbatim and the
// two backends answer identical reads. Every operation goes through an
// os.Root, so no key can traverse out of the directory or follow a symlink
// outside it.
//
// mu serialises replace with remove, so within one process a replace can
// never resurrect a removed object, and every publication with the pruning of
// emptied directories, so a write never lands in a directory being pruned. Several processes writing one local
// directory concurrently are not a supported topology (AGENTS.md §2.2: local
// filesystem storage is for local use).
type localBlobs struct {
	dir  string
	root *os.Root
	// keep are the directories open created; pruning after a removal stops
	// at them.
	keep map[string]bool
	mu   sync.Mutex
}

var _ blobs = (*localBlobs)(nil)

// openLocalBlobs opens (creating if needed) dir and, inside it, the temp
// directory and each of subdirs. what names the store in errors.
func openLocalBlobs(what, dir string, subdirs ...string) (*localBlobs, error) {
	if dir == "" {
		return nil, fmt.Errorf("artifacts: local %s: empty directory", what)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local %s: resolve %q: %w", what, dir, err)
	}
	if err := os.MkdirAll(abs, localDirPerm); err != nil {
		return nil, fmt.Errorf("artifacts: local %s: create %q: %w", what, abs, err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local %s: open %q: %w", what, abs, err)
	}
	keep := map[string]bool{".": true}
	for _, subdir := range append([]string{localTempDir}, subdirs...) {
		if err := root.MkdirAll(subdir, localDirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("artifacts: local %s: create %q: %w", what, subdir, err)
		}
		keep[subdir] = true
	}
	return &localBlobs{dir: filepath.Clean(abs), root: root, keep: keep}, nil
}

// close releases the directory handle.
func (b *localBlobs) close() error { return b.root.Close() }

func (b *localBlobs) uri(key string) string { return key }

func (b *localBlobs) locate(key string) string {
	abs := filepath.Join(b.dir, filepath.FromSlash(key))
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}).String()
}

func (b *localBlobs) resolve(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("%q: %w: %v", uri, ErrInvalidURI, err)
	}
	if u.Scheme != "file" {
		return "", fmt.Errorf("%q is not a file uri: %w", uri, ErrInvalidURI)
	}
	rel, err := filepath.Rel(b.dir, filepath.FromSlash(u.Path))
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%q is outside the store: %w", uri, ErrInvalidURI)
	}
	return filepath.ToSlash(rel), nil
}

func (b *localBlobs) get(ctx context.Context, key string, max int64) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	file, err := b.root.Open(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, "", errBlobNotFound
		}
		return nil, "", err
	}
	defer file.Close()
	var reader io.Reader = file
	if max > 0 {
		reader = io.LimitReader(file, max)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, "", err
	}
	return body, localVersion(body), nil
}

// localVersion is the version replace compares: the bytes' SHA-256, which
// changes whenever the content does, as an S3 ETag does.
func localVersion(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (b *localBlobs) getRange(ctx context.Context, key string, offset, length int64) ([]byte, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	file, err := b.root.Open(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, errBlobNotFound
		}
		return nil, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat: %w", err)
	}
	total := info.Size()
	if offset+length > total {
		return nil, total, errBlobRange
	}
	buf := make([]byte, length)
	if _, err := file.ReadAt(buf, offset); err != nil {
		return nil, total, fmt.Errorf("read [%d, %d): %w", offset, offset+length, err)
	}
	return buf, total, nil
}

func (b *localBlobs) head(ctx context.Context, key string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	info, err := b.root.Stat(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, errBlobNotFound
		}
		return 0, err
	}
	return info.Size(), nil
}

// writeTemp writes body to a fresh exclusive file under the temp directory,
// optionally synced, and returns its key. A crash leaves only an ignored temp
// file; readers never see a partial object at its final key.
func (b *localBlobs) writeTemp(body []byte, sync bool) (string, error) {
	tmpKey := path.Join(localTempDir, "blob-"+randomToken())
	file, err := b.root.OpenFile(tmpKey, os.O_CREATE|os.O_EXCL|os.O_WRONLY, localFilePerm)
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	if _, err = file.Write(body); err == nil && sync {
		err = file.Sync()
	}
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = b.root.Remove(tmpKey)
		return "", fmt.Errorf("write: %w", err)
	}
	return tmpKey, nil
}

// syncDir makes a publication in dir durable.
func (b *localBlobs) syncDir(dir string) error {
	handle, err := b.root.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}

func (b *localBlobs) put(ctx context.Context, key, _ string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmpKey, err := b.writeTemp(body, false)
	if err != nil {
		return err
	}
	if err := b.publish(key, func() error { return b.root.Rename(tmpKey, key) }); err != nil {
		_ = b.root.Remove(tmpKey)
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// publish creates key's directory and moves an object there under mu, the
// lock removeKeys prunes empty directories under, so a directory can never be
// pruned between its creation and the object landing in it.
func (b *localBlobs) publish(key string, move func() error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.root.MkdirAll(path.Dir(key), localDirPerm); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	return move()
}

// create publishes a fully written, synced inode with link(2), which never
// replaces another writer's winner.
func (b *localBlobs) create(ctx context.Context, key, _ string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmpKey, err := b.writeTemp(body, true)
	if err != nil {
		return err
	}
	defer func() { _ = b.root.Remove(tmpKey) }()
	err = b.publish(key, func() error {
		err := b.root.Link(tmpKey, key)
		if errors.Is(err, syscall.EINTR) {
			err = b.root.Link(tmpKey, key)
		}
		return err
	})
	if errors.Is(err, fs.ErrExist) {
		return errBlobExists
	}
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	if err := b.syncDir(path.Dir(key)); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}

// replace compares and renames under mu, the lock remove holds.
func (b *localBlobs) replace(ctx context.Context, key, _ string, body []byte, version string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, err := b.root.ReadFile(key)
	if errors.Is(err, fs.ErrNotExist) {
		return errBlobChanged
	}
	if err != nil {
		return err
	}
	if localVersion(current) != version {
		return errBlobChanged
	}
	tmpKey, err := b.writeTemp(body, true)
	if err != nil {
		return err
	}
	if err := b.root.Rename(tmpKey, key); err != nil {
		_ = b.root.Remove(tmpKey)
		return fmt.Errorf("commit: %w", err)
	}
	if err := b.syncDir(path.Dir(key)); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}

func (b *localBlobs) remove(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// removeKeys removes each key, then every directory the removals left empty,
// up to the directories open created.
func (b *localBlobs) removeKeys(ctx context.Context, keys []string) (map[string]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var failed map[string]error
	dirs := map[string]bool{}
	for _, key := range keys {
		if err := b.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
			if failed == nil {
				failed = map[string]error{}
			}
			failed[key] = fmt.Errorf("delete %q: %w", key, err)
			continue
		}
		dirs[path.Dir(key)] = true
	}
	// Deepest first, so a parent is examined after its children went.
	ordered := make([]string, 0, len(dirs))
	for dir := range dirs {
		ordered = append(ordered, dir)
	}
	sort.Slice(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })
	for _, dir := range ordered {
		b.pruneEmpty(dir)
	}
	return failed, nil
}

// pruneEmpty removes dir and each parent while they are empty, stopping at a
// directory open created.
func (b *localBlobs) pruneEmpty(dir string) {
	for dir != "" && dir != "/" && !b.keep[dir] {
		entries, err := fs.ReadDir(b.root.FS(), dir)
		if err != nil || len(entries) != 0 {
			return
		}
		if err := b.root.Remove(dir); err != nil {
			return
		}
		dir = path.Dir(dir)
	}
}

// list descends the directory tree in exact key order: siblings are sorted by
// the string their keys begin with (name+"/" for a directory, name for a
// file), not by name, because '-' (0x2D) sorts below the '/' (0x2F) that
// separates segments, so sibling directories "a" and "a-b" hold keys in the
// opposite order of their names. A subtree wholly at or before the cursor is
// skipped without being read, and the descent stops once the page is full, so
// one page costs about the page, not the tree. Only regular files are keys.
func (b *localBlobs) list(ctx context.Context, in blobListInput) (blobListPage, error) {
	max := int(in.MaxKeys)
	if max <= 0 {
		max = blobRemoveBatch
	}
	after := in.StartAfter
	if in.Token > after {
		after = in.Token
	}
	walk := &localListWalk{b: b, ctx: ctx, in: in, after: after, max: max}
	base := "."
	if i := strings.LastIndexByte(in.Prefix, '/'); i >= 0 {
		base = in.Prefix[:i]
	}
	if err := walk.descend(base); err != nil {
		return blobListPage{}, err
	}
	return walk.page, nil
}

// localListWalk is one list call's descent.
type localListWalk struct {
	b     *localBlobs
	ctx   context.Context
	in    blobListInput
	after string
	max   int
	page  blobListPage
	count int
	last  string
}

var errLocalListFull = errors.New("page full")

func (w *localListWalk) emit(key string, object *blobObject) error {
	if w.count == w.max {
		w.page.Truncated = true
		w.page.Next = w.last
		return errLocalListFull
	}
	if object != nil {
		w.page.Objects = append(w.page.Objects, *object)
	} else {
		w.page.Prefixes = append(w.page.Prefixes, key)
	}
	w.count++
	w.last = key
	return nil
}

func (w *localListWalk) descend(dir string) error {
	err := w.walk(dir)
	if errors.Is(err, errLocalListFull) {
		return nil
	}
	return err
}

func (w *localListWalk) walk(dir string) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	children, err := fs.ReadDir(w.b.root.FS(), dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return nil
		}
		return err
	}
	sort.Slice(children, func(i, j int) bool { return keyOrderName(children[i]) < keyOrderName(children[j]) })
	for _, child := range children {
		key := child.Name()
		if dir != "." {
			key = dir + "/" + child.Name()
		}
		if child.IsDir() {
			subtree := key + "/"
			if !strings.HasPrefix(subtree, w.in.Prefix) && !strings.HasPrefix(w.in.Prefix, subtree) {
				continue
			}
			// Every key beneath starts with subtree: all of them are at or
			// before the cursor when it sorts after subtree without lying
			// inside it. The comparison is against subtree, never the bare
			// name: for siblings "f" and "f-g" the name "f" sorts before a
			// cursor ".../f-g/..." while every key under it, ".../f/...",
			// sorts after, and pruning on the name would skip keys never
			// visited.
			if w.after != "" && !strings.HasPrefix(w.after, subtree) && w.after > subtree {
				continue
			}
			if w.in.Delimiter == "/" && strings.HasPrefix(subtree, w.in.Prefix) &&
				!strings.Contains(subtree[len(w.in.Prefix):len(subtree)-1], "/") {
				// A page that ended on this prefix resumes past all of it, as
				// S3 does; and a common prefix exists only while some key
				// beneath it does.
				if w.in.Token == subtree {
					continue
				}
				held, err := w.holdsKeyAfter(key)
				if err != nil {
					return err
				}
				if held {
					if err := w.emit(subtree, nil); err != nil {
						return err
					}
				}
				continue
			}
			if err := w.walk(key); err != nil {
				return err
			}
			continue
		}
		if !child.Type().IsRegular() || !strings.HasPrefix(key, w.in.Prefix) || key <= w.after {
			continue
		}
		info, err := child.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		if err := w.emit(key, &blobObject{Key: key, Size: info.Size(), Modified: info.ModTime().UTC()}); err != nil {
			return err
		}
	}
	return nil
}

// holdsKeyAfter reports whether dir holds a regular file whose key sorts
// after the cursor.
func (w *localListWalk) holdsKeyAfter(dir string) (bool, error) {
	found := false
	err := fs.WalkDir(w.b.root.FS(), dir, func(key string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.Type().IsRegular() && key > w.after {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	return found, err
}

// keyOrderName renders the string a child's keys begin with, so sorting by it
// orders siblings the same way their keys sort.
func keyOrderName(d fs.DirEntry) string {
	if d.IsDir() {
		return d.Name() + "/"
	}
	return d.Name()
}
