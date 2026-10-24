//go:build integration

// Package fakesuite is a test suite for proofrun's own tests: every outcome
// a ledger has to tell apart, including a binary that dies mid-test. It lives
// under testdata, so `./...` never runs it.
package fakesuite

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPass(t *testing.T) {}

func TestFail(t *testing.T) { t.Fatal("this test fails") }

func TestSkip(t *testing.T) { t.Skip("a prerequisite is absent") }

func TestAllowedSkip(t *testing.T) { t.Skip("an opt-in recorder") }

func TestSubSkip(t *testing.T) {
	t.Run("inner", func(t *testing.T) { t.Skip("a subtest skip is a skip") })
}

// TestCwd proves the job ran in the package directory.
func TestCwd(t *testing.T) {
	wd, _ := os.Getwd()
	if filepath.Base(wd) != "fakesuite" {
		t.Fatalf("cwd = %s", wd)
	}
}

// TestNamespace proves the slot's namespace reached the process.
func TestNamespace(t *testing.T) {
	if ns := os.Getenv("ORK_TEMPORAL_NAMESPACE"); !strings.HasPrefix(ns, "fake-w") {
		t.Fatalf("ORK_TEMPORAL_NAMESPACE = %q", ns)
	}
}

// TestCrash ends the whole binary while it runs.
func TestCrash(t *testing.T) {
	if os.Getenv("FAKESUITE_CRASH") == "1" {
		os.Exit(3)
	}
}

// TestAfterCrash had not started when TestCrash ended the binary.
func TestAfterCrash(t *testing.T) {}
