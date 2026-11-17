//go:build !linux

package orker

import (
	"fmt"
	"os"
)

// cgroup2Mounted: only Linux has cgroups; elsewhere a container's usage is
// never visible to the Orker.
func cgroup2Mounted(string) (bool, error) { return false, errCgroupNotFound }

// oNoFollow: no cgroup is read off Linux; the reader still builds there
// (orkctl's task run carries the Orker's dispatcher), and os.Root already
// refuses a path that leaves its directory.
const oNoFollow = 0

// verifyCgroupFS: off Linux no directory is a cgroup's, so none can be
// verified.
func verifyCgroupFS(*os.File) error {
	return fmt.Errorf("%w: cgroups are Linux's", errCgroupUnverified)
}

// oDirectory: no cgroup is read off Linux; the identity check refuses it
// first.
const oDirectory = 0
