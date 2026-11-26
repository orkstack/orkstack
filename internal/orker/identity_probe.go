package orker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// identityMountPath is where a task's identity directories are bound.
const identityMountPath = "/orkstack/identity"

// identityProbeFile is the file the identity directory probe reads back.
const identityProbeFile = "probe"

const identityProbeTaskID = "ork-identity-probe"

// ProbeIdentityDir shows that a container can read a file the Orker put in
// dir, by binding a directory of it read-only where a task's identities are
// bound and reading the file back. A remote or virtualised runtime that
// cannot see the host's directory fails it. A runtime with no boot probe is
// not probed. workRoot holds the probe's own workspace, as the boot probe's.
func (d *OCIDispatcher) ProbeIdentityDir(ctx context.Context, workRoot, dir string) error {
	prober, ok := d.Runtime.(bootProber)
	if !ok {
		d.logger().Warn("identity storage: this runtime runs no boot probe, so that a container can read " + dir + " is assumed, not shown")
		return nil
	}
	sub, err := os.MkdirTemp(dir, "ork-identity-probe-*")
	if err != nil {
		return fmt.Errorf("make a probe directory under %s: %w", dir, err)
	}
	defer func() { _ = removeSealedTree(sub) }()
	nonce := probeNonce()
	if err := writeIdentityFile(filepath.Join(sub, identityProbeFile), []byte(nonce)); err != nil {
		return fmt.Errorf("write the probe's file under %s: %w", dir, err)
	}
	if err := os.Chmod(sub, 0o555); err != nil {
		return fmt.Errorf("seal the probe's directory under %s: %w", dir, err)
	}
	wsRoot, err := os.MkdirTemp(workRoot, "ork-identityprobe-*")
	if err != nil {
		return fmt.Errorf("make the probe's workspace under %s: %w", workRoot, err)
	}
	defer func() { _ = removeSealedTree(wsRoot) }()
	ws, err := NewWorkspace(wsRoot)
	if err != nil {
		return fmt.Errorf("build the probe's workspace: %w", err)
	}
	if err := ws.Seal(); err != nil {
		return fmt.Errorf("seal the probe's workspace: %w", err)
	}
	if err := ws.SealSecrets(); err != nil {
		return fmt.Errorf("seal the probe's secrets: %w", err)
	}
	spec, err := d.probeContainerSpec(ws, identityProbeTaskID, []string{"/bin/cat", identityMountPath + "/" + identityProbeFile})
	if err != nil {
		return err
	}
	spec.Mounts = append(spec.Mounts, Mount{Source: sub, Destination: identityMountPath, ReadOnly: true})
	run, err := prober.RunBootProbe(ctx, spec)
	if run.Container != "" {
		d.removeProbeContainer(ctx, run.Container)
	}
	d.warnProbeLeftovers(run.Leftovers)
	if err != nil {
		return fmt.Errorf("%w: %w", errIdentityProbeNotRun, err)
	}
	if got := strings.TrimSpace(run.Stdout); got != nonce {
		if len(got) > 200 {
			got = got[:200] + "…"
		}
		return fmt.Errorf("a container did not read back the file the Orker put in %s (it printed %q)", dir, got)
	}
	return nil
}

// bootIdentityStorage chooses, for an Orker that serves identity, where its
// identity files are made, and removes what an earlier process under this
// Orker's identity left there: the boot holds that identity's lock. root is
// the work root, which holds the probe's own workspace.
func (a *Activities) bootIdentityStorage(ctx context.Context, root string) error {
	if a.identityConfig == nil {
		return nil
	}
	d := a.dispatcher
	choice, err := chooseIdentityStorage(ctx, a.identityConfig.Dir, hostIdentityStorage(func(ctx context.Context, dir string) error {
		return d.ProbeIdentityDir(ctx, root, dir)
	}))
	if err != nil {
		return err
	}
	if choice.Warning != "" {
		d.logger().Warn("identity storage: " + choice.Warning)
	}
	if choice.Dir != "" {
		reapIdentityLeftovers(choice.Dir, d.logger())
		if _, _, err := ensureOwnIdentityDir(choice.Dir, d.OrkerID, true); err != nil {
			return fmt.Errorf("orker.identity.dir: %w", err)
		}
	}
	d.identityStorage.Store(&choice)
	d.logger().Info("identity storage", "kind", choice.Kind, "dir", choice.Dir)
	return nil
}

// identityStorageReport is what DescribeOrker says of a choice.
func identityStorageReport(choice *identityStorageChoice) orkstackv1.IdentityStorage {
	switch {
	case choice == nil:
		return orkstackv1.IdentityStorage_IDENTITY_STORAGE_UNSPECIFIED
	case choice.Kind == identityStorageMemory:
		return orkstackv1.IdentityStorage_IDENTITY_STORAGE_MEMORY
	default:
		return orkstackv1.IdentityStorage_IDENTITY_STORAGE_DISK
	}
}

// errIdentityProbeNotRun is a probe that could not run, which says nothing
// about whether a container can read the directory: the boot is refused
// rather than falling back to disk for a daemon's moment.
var errIdentityProbeNotRun = errors.New("the identity directory probe could not run")

// identityDirMarker marks a directory as one an Orker made for its identity
// files: only such a directory is ever removed.
const identityDirMarker = ".orkstack-identity"

// ensureOwnIdentityDir is <dir>/<orkerID>, this Orker's own directory of
// identity files, made with its marker when absent (made says it was). With
// reap, what an earlier process under this id left there is removed first:
// the boot holds the identity lock. A directory there without the marker is
// not one an Orker made and is refused, never removed, unless it is empty.
// The directory and its marker appear together, by one rename, so no crash
// leaves one without the other.
func ensureOwnIdentityDir(dir, orkerID string, reap bool) (own string, made bool, err error) {
	if orkerID == "" || orkerID == "." || orkerID == ".." || strings.ContainsAny(orkerID, "/\\\x00") {
		return "", false, fmt.Errorf("this Orker's id %q cannot name a directory under %s", orkerID, dir)
	}
	own = filepath.Join(dir, orkerID)
	info, err := os.Lstat(own)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return own, true, makeOwnIdentityDir(dir, own)
	case err != nil:
		return "", false, err
	case info.Mode()&os.ModeSymlink != 0 || !info.IsDir():
		return "", false, fmt.Errorf("%s is not a directory this Orker made", own)
	}
	if _, err := os.Lstat(filepath.Join(own, identityDirMarker)); err != nil {
		entries, rerr := os.ReadDir(own)
		if rerr != nil || len(entries) > 0 {
			return "", false, fmt.Errorf("%s holds no %s marker, so it is not a directory an Orker made, and it is left alone", own, identityDirMarker)
		}
		// An empty directory holds nothing to protect: it is made ours.
		if err := os.Remove(own); err != nil {
			return "", false, fmt.Errorf("replace the empty %s: %w", own, err)
		}
		return own, true, makeOwnIdentityDir(dir, own)
	}
	if !reap {
		return own, false, nil
	}
	if err := removeSealedTree(own); err != nil {
		return "", false, fmt.Errorf("remove what an earlier process of this Orker left in %s: %w", own, err)
	}
	return own, true, makeOwnIdentityDir(dir, own)
}

// makeOwnIdentityDir makes own with its marker under a temporary name in
// dir, then renames it into place.
func makeOwnIdentityDir(dir, own string) error {
	tmp, err := os.MkdirTemp(dir, ".ork-identity-new-*")
	if err != nil {
		return fmt.Errorf("make a directory under %s: %w", dir, err)
	}
	if err := writeIdentityFile(filepath.Join(tmp, identityDirMarker), nil); err != nil {
		_ = os.RemoveAll(tmp)
		return fmt.Errorf("mark %s: %w", own, err)
	}
	if err := os.Rename(tmp, own); err != nil {
		_ = os.RemoveAll(tmp)
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return fmt.Errorf("put %s in place: %w", own, err)
	}
	return nil
}

// reapIdentityLeftovers removes the temporary directories a crash between
// makeOwnIdentityDir's two steps left in dir. Only names it makes are
// matched, and the boot holds the identity lock.
func reapIdentityLeftovers(dir string, logger *slog.Logger) {
	leftovers, err := filepath.Glob(filepath.Join(dir, ".ork-identity-new-*"))
	if err != nil {
		return
	}
	for _, left := range leftovers {
		if err := removeSealedTree(left); err != nil && logger != nil {
			logger.Warn("identity storage: a directory a crash left could not be removed", "path", left, "error", err)
		}
	}
}
