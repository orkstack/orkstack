// Command gendashboards writes the observability kit's provisioned
// dashboards, deploy/observability/grafana/dashboards/<uid>.json, from
// internal/dashboards. TestDashboards_AreGenerated refuses a stale file.
//
//	go run ./internal/dashboards/cmd/gendashboards
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/orkstack/orkstack/internal/dashboards"
)

func main() {
	dir := flag.String("dir", "deploy/observability/grafana/dashboards", "the directory to write the dashboards into")
	flag.Parse()
	if err := run(*dir); err != nil {
		fmt.Fprintln(os.Stderr, "gendashboards:", err)
		os.Exit(1)
	}
}

func run(dir string) error {
	files, err := dashboards.JSON()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for uid, b := range files {
		if err := os.WriteFile(filepath.Join(dir, uid+".json"), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}
