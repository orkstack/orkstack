package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/orkstack/orkstack/internal/orker"
)

// A task run's identity on the container runtime.
//
// Every `orkctl task run` labels its container ork.orker=orkctl-task-run-<hex>,
// <hex> being its own local run id's, so no run's identity names another
// run's container. While it lives, a run holds an exclusive lock on
// <root>/orkctl-task-run-<hex>.lock, which the kernel releases when the
// process exits, however it exits; the file names the host and the runtime
// endpoint the run used. A run that settled its own container — removed it,
// or never created one — deletes its lock file as it exits. One that did not
// (killed, abandoned on a second interrupt, or its removal failed) leaves the
// file, and the next run under the same root, on the same host and endpoint,
// removes that identity's containers once it can take the lock, then deletes
// the file. A live run's lock is held; a run on another host, another daemon
// or under another root is never matched; so neither is ever touched.
const (
	taskRunOrkerPrefix = "orkctl-task-run-"
	taskRunLockSuffix  = ".lock"
)

// taskRunLockName is a lock file a run may have left, named for its identity:
// the local run id's hex, and nothing else, names a label filter.
var taskRunLockName = regexp.MustCompile(`^` + taskRunOrkerPrefix + `([0-9a-f]{16})` + regexp.QuoteMeta(taskRunLockSuffix) + `$`)

// taskRunOwner is this run's claim on its identity. file is nil when the root
// cannot lock; the run then keeps its own identity and settles nobody else's.
type taskRunOwner struct {
	id       string
	path     string
	file     *os.File
	unlocked error
}

// taskRunFingerprint is what a lock file names: the host, and the endpoint the
// runtime's commands reach. A runtime that cannot say is named by its type.
func taskRunFingerprint(rt orker.ContainerRuntime) string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	endpoint := fmt.Sprintf("runtime=%T", rt)
	if named, ok := rt.(interface{ Endpoint() string }); ok {
		endpoint = named.Endpoint()
	}
	return "host=" + host + "\n" + endpoint + "\n"
}

// claimTaskRunOwner takes this run's identity under root. The lock file is
// written and locked under a name no scan reads, then renamed into place, so
// a scan never takes a claim in progress for a run that is gone. Go opens
// every file close-on-exec, so the container CLI processes a run starts never
// inherit the lock and keep a dead run looking alive. A root whose filesystem
// cannot lock is not an error: the owner comes back unlocked, and says why.
func claimTaskRunOwner(root, localID, fingerprint string) (*taskRunOwner, error) {
	id := taskRunOrkerPrefix + strings.TrimPrefix(localID, "local-")
	path := filepath.Join(root, id+taskRunLockSuffix)
	if !taskRunLockName.MatchString(filepath.Base(path)) {
		return nil, fmt.Errorf("claim the run's identity: %q is not a local run id", localID)
	}
	staging := path + ".new"
	file, err := os.OpenFile(staging, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("claim the run's identity: %w", err)
	}
	abandon := func() {
		_ = file.Close()
		_ = os.Remove(staging)
	}
	if _, err := file.WriteString(fingerprint); err != nil {
		abandon()
		return nil, fmt.Errorf("claim the run's identity: write %s: %w", staging, err)
	}
	if err := lockTaskRunOwner(file); err != nil {
		abandon()
		return &taskRunOwner{id: id, unlocked: fmt.Errorf("lock %s: %w", staging, err)}, nil
	}
	if err := os.Rename(staging, path); err != nil {
		abandon()
		return nil, fmt.Errorf("claim the run's identity: %w", err)
	}
	return &taskRunOwner{id: id, path: path, file: file}, nil
}

// release gives the claim up. A run that settled its own container deletes
// the lock file first, while it still holds the lock; one that did not leaves
// it for the next run's scan.
func (o *taskRunOwner) release(settled bool) {
	if o.file == nil {
		return
	}
	if settled {
		_ = os.Remove(o.path)
	}
	_ = o.file.Close()
	o.file = nil
}

// reapGoneTaskRuns removes the containers of every run under root that is
// gone and used this fingerprint, and returns how many it removed for each
// identity. A container that could not be removed keeps its lock file for a
// later run; the error says which.
func reapGoneTaskRuns(ctx context.Context, rt orker.ContainerRuntime, root, fingerprint string, self *taskRunOwner) (map[string]int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", root, err)
	}
	removed := map[string]int{}
	var failures []error
	for _, entry := range entries {
		match := taskRunLockName.FindStringSubmatch(entry.Name())
		if match == nil || !entry.Type().IsRegular() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if self != nil && path == self.path {
			continue
		}
		id := taskRunOrkerPrefix + match[1]
		count, err := reapGoneTaskRun(ctx, rt, path, id, fingerprint)
		if count > 0 {
			removed[id] = count
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return removed, errors.Join(failures...)
}

// reapGoneTaskRun removes one identity's containers when its lock is free and
// its file names this fingerprint, holding the lock until the file is gone.
func reapGoneTaskRun(ctx context.Context, rt orker.ContainerRuntime, path, id, fingerprint string) (int, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return 0, nil // another run settled it first
	case errors.Is(err, fs.ErrPermission):
		return 0, nil // another user's run, in a shared root
	case err != nil:
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	if err := lockTaskRunOwner(file); err != nil {
		if errors.Is(err, errTaskRunOwnerHeld) {
			return 0, nil // a live run
		}
		return 0, fmt.Errorf("lock %s: %w", path, err)
	}
	named, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	if string(named) != fingerprint {
		return 0, nil // a run on another host or daemon: its own kind settles it
	}
	count, err := rt.Reap(ctx, id)
	if err != nil {
		return count, fmt.Errorf("remove the containers labelled ork.orker=%s: %w", id, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return count, fmt.Errorf("remove %s: %w", path, err)
	}
	return count, nil
}

// reapGone removes the containers of runs under root that are gone, and says
// what it removed. A failure is a warning: it never stops this run, and the
// lock file it could not settle waits for the next one.
func (r *taskRunner) reapGone(root, fingerprint string, owner *taskRunOwner) {
	removed, err := reapGoneTaskRuns(r.ctx, r.rt, root, fingerprint, owner)
	ids := make([]string, 0, len(removed))
	for id := range removed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r.t.printf("removed %d container(s) of an earlier task run that is gone (ork.orker=%s)\n", removed[id], id)
	}
	if err != nil {
		r.t.printf("warning: an earlier task run's containers could not be settled: %v\n", err)
	}
}
