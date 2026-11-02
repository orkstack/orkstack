package main

import (
	"os"
	"testing"
)

// TestMain lets this test binary stand in for another program in a test that
// runs a workflow step or a script. With PROOFRUN_AS_COMMAND=1 in its
// environment it runs the subcommand its arguments name, as proofrun would;
// with PROOFRUN_AS_FAKE_TOOL=<program> it is that program of a test's fake
// PATH (fakeToolMain). Either way it runs no test.
func TestMain(m *testing.M) {
	if os.Getenv("PROOFRUN_AS_COMMAND") == "1" {
		os.Exit(runCommand(os.Args[1:]))
	}
	if program := os.Getenv("PROOFRUN_AS_FAKE_TOOL"); program != "" {
		os.Exit(fakeToolMain(program, os.Args[1:]))
	}
	os.Exit(m.Run())
}
