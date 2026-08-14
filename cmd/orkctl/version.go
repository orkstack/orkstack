package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// version is the orkctl release. Release builds override it with
// -ldflags "-X main.version=vX.Y.Z"; source builds report "dev".
var version = "dev"

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
				encoded, err := json.MarshalIndent(info, "", "  ")
				if err != nil {
					return fmt.Errorf("encode json: %w", err)
				}
				_, err = out.Write(append(encoded, '\n'))
				if err != nil {
					return fmt.Errorf("write output: %w", err)
				}
				return nil
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
	info := versionInfo{
		Version:   version,
		Commit:    "unknown",
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	for _, setting := range build.Settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			info.Commit = setting.Value
		}
	}
	return info
}
