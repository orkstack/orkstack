// Package fuzzsuite is a test suite for proofrun's own tests: native fuzz
// targets beside an ordinary test, one whose seed passes and one whose seed
// fails, so a gate that runs a Fuzz target as evidence can be told to have
// run it. It lives under testdata, so `./...` never runs it or fuzzes it.
package fuzzsuite
