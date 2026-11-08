package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/orkstack/orkstack/internal/buildinfo"
)

// version is the orkctl release. Release builds override it with
// -ldflags "-X main.version=vX.Y.Z"; source builds report "dev".
var version = "dev"

// commit is the commit this binary was built from. Release builds set it with
// -ldflags "-X main.commit=<sha>", because they build with -buildvcs=false;
// a development build leaves it empty and reports what version control says
// (internal/buildinfo).
var commit string

// versionInfo is the machine-readable shape of `orkctl version --json`.
type versionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	GoVersion string `json:"goVersion"`
	Platform  string `json:"platform"`
}

func newVersionCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the orkctl version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			info := collectVersionInfo()
			out := cmd.OutOrStdout()
			if g.json {
				return writeGoJSON(out, info)
			}
			table := newTable(out)
			fmt.Fprintf(table, "orkctl:\t%s\n", info.Version)
			fmt.Fprintf(table, "commit:\t%s\n", info.Commit)
			fmt.Fprintf(table, "go:\t%s\n", info.GoVersion)
			fmt.Fprintf(table, "platform:\t%s\n", info.Platform)
			if err := table.Flush(); err != nil {
				return fmt.Errorf("write output: %w", err)
			}
			return nil
		},
	}
}

func collectVersionInfo() versionInfo {
	return versionInfo{
		Version:   version,
		Commit:    buildinfo.Commit(commit),
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}
}
