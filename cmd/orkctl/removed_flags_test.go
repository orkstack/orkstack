package main

import (
	"bytes"
	"strings"
	"testing"
)

// This is the one file that must spell the flags the structured list
// filters removed: it proves each is refused. scripts/check-list-filter-
// leftovers.sh scans everything else for them.

// TestRemovedFlags_AreUnknown: a removed filter flag is an unknown flag —
// cobra refuses it and the command exits non-zero — never an alias of
// --filter. OrkStack keeps no compatibility layer.
func TestRemovedFlags_AreUnknown(t *testing.T) {
	for _, args := range [][]string{
		{"trigger", "list", "--orkflow", "hello-ork"},
		{"schedule", "list", "--orkflow", "hello-ork"},
		{"orker", "list", "--pool", "gpu"},
		{"namespace", "list", "--name-contains", "pay"},
	} {
		var stdout, stderr bytes.Buffer
		code := Execute(append(args, "--server", "http://127.0.0.1:1"), &stdout, &stderr)
		if code == exitOK {
			t.Errorf("orkctl %v exited 0; a removed flag must be refused", args)
		}
		if !strings.Contains(stderr.String(), "unknown flag: "+args[2]) {
			t.Errorf("orkctl %v stderr = %q; want cobra's unknown flag error", args, stderr.String())
		}
	}
}
