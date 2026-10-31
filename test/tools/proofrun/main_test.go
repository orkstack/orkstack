package main

import (
	"os"
	"testing"
)

// TestMain lets this test binary stand in for the proofrun command in a test
// that runs a workflow step or a script: with PROOFRUN_AS_COMMAND=1 in its
// environment it runs the subcommand its arguments name, as proofrun would,
// and runs no test.
func TestMain(m *testing.M) {
	if os.Getenv("PROOFRUN_AS_COMMAND") == "1" {
		os.Exit(runCommand(os.Args[1:]))
	}
	os.Exit(m.Run())
}
