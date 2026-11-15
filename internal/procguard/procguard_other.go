//go:build !linux

package procguard

// supported says Harden does something on this platform: off Linux, nothing.
const supported = false

func setDumpable(bool) error { return nil }
