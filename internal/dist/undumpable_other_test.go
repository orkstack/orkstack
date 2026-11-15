//go:build !linux

package dist_test

// beUndumpableUntilStopped is Linux's alone: off Linux ork and orker do not
// harden themselves, and nothing asks for this child.
func beUndumpableUntilStopped(string) int { return 2 }
