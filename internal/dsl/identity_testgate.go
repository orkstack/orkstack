//go:build ork_test_identity

package dsl

import "log/slog"

// This build, and only this build, runs task identity before the gate opens:
// the binaries of the end-to-end tests of it, which testbin.GatedPath makes.
// No release, image or development build passes the tag (internal/dist,
// TestNoBuildOpensTheIdentityGate), and one that did would say so first in
// its log. It goes with the gate.
func init() {
	taskIdentityAvailable = true
	slog.Warn("task identity is open by build tag: a test's binary, never one to deploy")
}
