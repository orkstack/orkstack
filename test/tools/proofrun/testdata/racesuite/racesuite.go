// Package racesuite is a test suite for proofrun's own tests: a package whose
// unit test has a deliberate data race, for the race gate to find. It lives
// under testdata, so `./...` never builds or runs it.
package racesuite
