// Package bucketsweep is a test suite for proofrun's bucket sweep: one test
// that passes (or fails, with BUCKETSWEEP_FAIL=1), in a binary whose TestMain
// ends the way internal/testsupport/s3bucket.Main ends a real integration
// package, minus the object store (bucketsweep_integration_test.go). It lives
// under testdata, so `./...` never runs it.
package bucketsweep
