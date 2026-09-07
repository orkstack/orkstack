package config

import (
	"os"
	"testing"
)

// TestDefaultsFile_MatchesRenderDefaults proves
// deploy/orkstack.defaults.yaml is exactly what "make config-defaults" (or
// `ork --defaults` / `orker --defaults`) would write today — i.e. that
// nobody hand-edited it out of sync with a Schema change since the last
// regeneration.
func TestDefaultsFile_MatchesRenderDefaults(t *testing.T) {
	data, err := os.ReadFile("../../deploy/orkstack.defaults.yaml")
	if err != nil {
		t.Fatalf("read deploy/orkstack.defaults.yaml: %v", err)
	}
	if string(data) != RenderDefaults() {
		t.Fatal("deploy/orkstack.defaults.yaml is stale: run make config-defaults")
	}
}

// TestDefaultsFile_LoadAcceptsForBothBinaries proves the generated file is
// valid input to config.Load for ork AND orker: every key either binary
// reads (or a section belonging entirely to the other one, which Load
// accepts and ignores) parses cleanly.
func TestDefaultsFile_LoadAcceptsForBothBinaries(t *testing.T) {
	for _, binary := range []Binary{BinaryOrk, BinaryOrker} {
		if _, err := Load("../../deploy/orkstack.defaults.yaml", binary); err != nil {
			t.Errorf("Load(deploy/orkstack.defaults.yaml, %s): %v", binary, err)
		}
	}
}
