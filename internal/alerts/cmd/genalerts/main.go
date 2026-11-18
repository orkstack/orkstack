// Command genalerts writes the observability kit's Prometheus rule files and
// their promtool tests under deploy/observability/prometheus/rules, and the
// generated alert entries of docs/RUNBOOKS.md, from internal/alerts.
// TestAlerts_AreGenerated refuses a stale file; scripts/check-alerts.sh runs
// promtool on the result.
//
//	go run ./internal/alerts/cmd/genalerts
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/orkstack/orkstack/internal/alerts"
)

func main() {
	root := flag.String("root", ".", "the repository root to write into")
	flag.Parse()
	if err := run(*root); err != nil {
		fmt.Fprintln(os.Stderr, "genalerts:", err)
		os.Exit(1)
	}
}

func run(root string) error {
	files, err := alerts.Files()
	if err != nil {
		return err
	}
	dir := filepath.Join(root, alerts.RulesDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for name, b := range files {
		if name == alerts.RunbooksFile {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			return err
		}
	}
	path := filepath.Join(root, alerts.RunbooksFile)
	doc, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	spliced, err := alerts.Splice(string(doc), string(files[alerts.RunbooksFile]))
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(spliced), 0o644)
}
