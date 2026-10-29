package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
)

// errRenderOnly is what observedRuntime.Create returns under --render-only,
// after the input tree is sealed and snapshotted. The dispatcher reports it
// as RUNTIME_REFUSED, because that is what any Create error is; the command,
// which asked for the stop itself, reports RENDERED instead — decided by its
// own flag and by observedRuntime.stopped, never by reading the error.
var errRenderOnly = errors.New("--render-only: the input tree is sealed and no container is created")

// observedRuntime wraps the real ContainerRuntime and adds observation at two
// points of the interface and nothing else. Everything the dispatcher does to
// the container — the §9.2 constants, the resource caps, the mount set — is
// built by the dispatcher's own containerSpec and passed through untouched.
//
//   - Create is called after the input tree is sealed. It snapshots the
//     /orkstack/input mount's source into session/input/ and has the
//     transcript print config.json, then — under --render-only — stops.
//   - Remove delegates first, then (with --keep) copies the /orkstack/output
//     mount's tree into session/output/. The dispatcher defers the
//     container's Remove after the workspace's own removal is deferred, so by
//     LIFO order the workspace still exists here, the container has exited,
//     and the scan and uploads are finished. Copying inside Wait instead
//     would delay the exit the task-timeout select is racing.
//   - Start and Wait delegate and measure the container's wall-clock run.
type observedRuntime struct {
	orker.ContainerRuntime

	session    string
	keep       bool
	renderOnly bool
	// onSealed is called with the snapshot directory once Create has copied
	// the sealed input tree.
	onSealed func(snapshot string)

	mu           sync.Mutex
	stopped      bool
	createAsked  bool
	created      bool
	removed      bool
	started      time.Time
	ran          time.Duration
	waited       bool
	outputSource string
	sharedSource string
	labels       map[string]string
	keptNotes    []string
}

func (o *observedRuntime) Create(ctx context.Context, spec orker.ContainerSpec) (orker.ContainerID, error) {
	inputSource := ""
	o.mu.Lock()
	o.labels = spec.Labels
	for _, mount := range spec.Mounts {
		switch mount.Destination {
		case orker.ContainerInput:
			inputSource = mount.Source
		case orker.ContainerOutput:
			o.outputSource = mount.Source
		case orker.ContainerShared:
			o.sharedSource = mount.Source
		}
	}
	o.mu.Unlock()

	snapshot := filepath.Join(o.session, "input")
	if inputSource != "" {
		keepAll := o.keep
		_, err := copyTree(inputSource, snapshot, func(rel string) bool {
			if keepAll {
				return true
			}
			switch rel {
			case "config.json", "task.json", "run.json":
				return true
			}
			return rel == "upstream" || strings.HasPrefix(rel, "upstream/")
		})
		if err != nil {
			return "", fmt.Errorf("snapshot the sealed input tree: %w", err)
		}
	}
	if o.onSealed != nil {
		o.onSealed(snapshot)
	}
	if o.renderOnly {
		o.mu.Lock()
		o.stopped = true
		o.mu.Unlock()
		return "", errRenderOnly
	}
	o.mu.Lock()
	o.createAsked = true
	o.mu.Unlock()
	id, err := o.ContainerRuntime.Create(ctx, spec)
	if err == nil {
		o.mu.Lock()
		o.created = true
		o.mu.Unlock()
	}
	return id, err
}

func (o *observedRuntime) Start(ctx context.Context, id orker.ContainerID, stdout, stderr io.Writer) error {
	o.mu.Lock()
	o.started = time.Now()
	o.mu.Unlock()
	return o.ContainerRuntime.Start(ctx, id, stdout, stderr)
}

func (o *observedRuntime) Wait(ctx context.Context, id orker.ContainerID) (orker.ExitStatus, error) {
	status, err := o.ContainerRuntime.Wait(ctx, id)
	o.mu.Lock()
	if !o.started.IsZero() {
		o.ran = time.Since(o.started)
	}
	o.waited = err == nil
	o.mu.Unlock()
	return status, err
}

func (o *observedRuntime) Remove(ctx context.Context, id orker.ContainerID) error {
	err := o.ContainerRuntime.Remove(ctx, id)
	o.mu.Lock()
	source := o.outputSource
	o.removed = err == nil
	o.mu.Unlock()
	if o.keep && source != "" {
		skipped, copyErr := copyTree(source, filepath.Join(o.session, "output"), nil)
		o.mu.Lock()
		o.keptNotes = append(o.keptNotes, skipped...)
		if copyErr != nil {
			o.keptNotes = append(o.keptNotes, "output/ could not be kept: "+copyErr.Error())
		}
		o.mu.Unlock()
	}
	return err
}

// settled reports whether this run leaves no container of its own behind: it
// never asked the runtime for one, or the one it got was removed. A create
// that failed may still have left one, so it is not settled; the next run's
// scan asks the runtime by this run's label. A run that stopped before the
// decorator existed created nothing.
func (o *observedRuntime) settled() bool {
	if o == nil {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.createAsked || o.removed
}

// observation is a consistent read of what the decorator saw.
type observation struct {
	stopped      bool
	created      bool
	ranContainer bool
	waited       bool
	ran          time.Duration
	sharedSource string
	labels       map[string]string
	keptNotes    []string
}

func (o *observedRuntime) observed() observation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return observation{
		stopped:      o.stopped,
		created:      o.created,
		ranContainer: !o.started.IsZero(),
		waited:       o.waited,
		ran:          o.ran,
		sharedSource: o.sharedSource,
		labels:       o.labels,
		keptNotes:    append([]string(nil), o.keptNotes...),
	}
}

// copyTree copies the regular files and directories under src into dst,
// preserving the relative layout, and returns one note per entry it skipped
// (a symlink, device, socket or FIFO, or a file this user cannot read). keep,
// when non-nil, selects which relative paths are copied; a directory is
// entered whenever keep might select something below it.
//
// Copies are made with ordinary modes (0755/0644): the source tree's sealed
// or sticky modes describe the container's view, not a kept snapshot's.
func copyTree(src, dst string, keep func(rel string) bool) ([]string, error) {
	var skipped []string
	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if walkErr != nil {
			if rel == "." {
				return walkErr
			}
			skipped = append(skipped, fmt.Sprintf("%s: %v", rel, walkErr))
			if entry != nil && entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		target := filepath.Join(dst, filepath.FromSlash(rel))
		switch {
		case rel == ".":
			return os.MkdirAll(dst, 0o755)
		case entry.IsDir():
			if keep != nil && !keep(rel) && !keepsBelow(keep, rel) {
				return fs.SkipDir
			}
			return os.MkdirAll(target, 0o755)
		case entry.Type().IsRegular():
			if keep != nil && !keep(rel) {
				return nil
			}
			if err := copyFile(path, target); err != nil {
				skipped = append(skipped, fmt.Sprintf("%s: %v", rel, err))
			}
			return nil
		default:
			if keep != nil && !keep(rel) {
				return nil
			}
			skipped = append(skipped, fmt.Sprintf("%s: %s skipped; only regular files and directories are copied",
				rel, describeFileType(entry.Type())))
			return nil
		}
	})
	return skipped, err
}

// keepsBelow answers whether a directory may hold something keep selects.
// The only selective caller keeps named top-level files and one subtree, so
// a directory is entered when keep selects it or it is an ancestor of a kept
// path; for that caller, "upstream" is the only directory that qualifies.
func keepsBelow(keep func(string) bool, dir string) bool {
	return keep(dir + "/")
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// describeFileType names a non-regular file type in AGENTS.md §3.1's
// vocabulary.
func describeFileType(mode fs.FileMode) string {
	switch {
	case mode&fs.ModeSymlink != 0:
		return "a symlink"
	case mode&fs.ModeNamedPipe != 0:
		return "a FIFO"
	case mode&fs.ModeSocket != 0:
		return "a socket"
	case mode&fs.ModeDevice != 0, mode&fs.ModeCharDevice != 0:
		return "a device"
	default:
		return "not a regular file"
	}
}
