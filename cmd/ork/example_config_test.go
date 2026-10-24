package main

import (
	"testing"

	cfgfile "github.com/orkstack/orkstack/internal/config"
)

// TestExampleConfigLoadsForBothBinaries proves deploy/orkstack.example.yaml
// can never silently drift from the schema internal/config actually
// understands: this is exactly what `ork --config deploy/orkstack.example.yaml
// --check` and `orker --config deploy/orkstack.example.yaml --check` do
// (docs/DEPLOY.md §1), run here as a unit test rather than only by hand.
func TestExampleConfigLoadsForBothBinaries(t *testing.T) {
	const path = "../../deploy/orkstack.example.yaml"
	if _, err := cfgfile.Load(path, cfgfile.BinaryOrk); err != nil {
		t.Errorf("Load(%s, BinaryOrk): %v", path, err)
	}
	if _, err := cfgfile.Load(path, cfgfile.BinaryOrker); err != nil {
		t.Errorf("Load(%s, BinaryOrker): %v", path, err)
	}
}
