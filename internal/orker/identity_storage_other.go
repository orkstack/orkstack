//go:build !linux

package orker

// identityMemoryBacked: off Linux no filesystem is told memory-backed, so
// auto falls back to the work root and says so.
func identityMemoryBacked(string) (bool, error) { return false, nil }
