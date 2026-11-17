//go:build !unix

package orker

import "os"

// nodeStatOf reports no device numbers off Unix: device profiles are
// Linux-host-only, and this build only has to compile (orkctl links the
// dispatcher for `task run`).
func nodeStatOf(os.FileInfo) (nodeStat, bool) { return nodeStat{}, false }
