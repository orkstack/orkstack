package orker

import (
	"context"
	"os"
	"strings"
)

// bootServiceStorage chooses, for an Orker that hosts services, the memory-backed
// directory their secrets and conversations are kept in: the one its identity
// files are in, or, where it serves no identity, the one they would be. An Orker
// with none boots, and is unfit to host.
func (a *Activities) bootServiceStorage(ctx context.Context, root string) {
	if !a.sourceHost.HostsSources() {
		return
	}
	d := a.dispatcher
	choice := d.identityStorage.Load()
	if choice == nil {
		picked, err := chooseIdentityStorage(ctx, DefaultIdentityDir, hostIdentityStorage(func(ctx context.Context, dir string) error {
			return d.ProbeIdentityDir(ctx, root, dir)
		}))
		if err != nil {
			d.logger().Warn("no directory for a service's secrets and conversation could be chosen; this Orker hosts no service", "error", err)
			return
		}
		if picked.Dir != "" {
			if _, _, err := ensureOwnIdentityDir(picked.Dir, d.OrkerID, true); err != nil {
				d.logger().Warn("the directory for a service's secrets and conversation is not usable; this Orker hosts no service", "dir", picked.Dir, "error", err)
				return
			}
		}
		choice = &picked
	}
	if choice.Kind != identityStorageMemory {
		d.logger().Warn("no memory-backed directory for a service's secrets and conversation (orker.identity.dir); this Orker hosts no service")
		return
	}
	a.services.setStorage(choice.Dir)
	swaps, err := hostSwaps()
	d.logger().Info("service storage", "dir", choice.Dir, "host_swaps", swaps)
	if swaps || err != nil {
		d.logger().Warn("this host swaps memory out, or could not say: a service's secrets are kept in memory and can reach its swap device; use encrypted swap or none (docs/DEPLOY.md, \"Source hosts\")",
			"error", err)
	}
}

// hostSwaps reports a host with a swap device in use.
func hostSwaps() (bool, error) {
	raw, err := os.ReadFile("/proc/swaps")
	if err != nil {
		return false, err
	}
	return swapsOn(string(raw)), nil
}

// swapsOn reads /proc/swaps: a heading, then a line for each device in use.
func swapsOn(table string) bool {
	_, devices, _ := strings.Cut(table, "\n")
	return strings.TrimSpace(devices) != ""
}

// hostIdentityStorage is the host as the storage choice reads it.
func hostIdentityStorage(readable func(ctx context.Context, dir string) error) identityStorageProbe {
	shm := "/dev/shm"
	if info, err := os.Stat(shm); err != nil || !info.IsDir() {
		shm = ""
	}
	return identityStorageProbe{uid: os.Getuid(), runtimeDir: os.Getenv("XDG_RUNTIME_DIR"), shmDir: shm, memoryBacked: identityMemoryBacked, readable: readable}
}
