// Package failfast is a test suite for proofrun's -fail-fast: one test that
// fails once another is running, one that blocks until it is stopped, and one
// that must never start (failfast_integration_test.go). It lives under
// testdata, so `./...` never runs it.
package failfast
