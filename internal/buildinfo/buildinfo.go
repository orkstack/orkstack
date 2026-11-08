// Package buildinfo says which commit a binary was built from. The three
// commands share it so that they answer alike (AGENTS.md §12, cmd/ork,
// cmd/orker, cmd/orkctl).
package buildinfo

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"text/tabwriter"
)

// Unknown is what a binary reports when nothing says which commit built it:
// the quickstart's source build, or a build from a source archive.
const Unknown = "unknown"

// Commit resolves the commit a binary reports, most deliberate first: the
// value `-X main.commit` stamped in, which a release build passes because it
// builds with -buildvcs=false (internal/dist, Target.BuildArgs), then the
// revision a development build's version control information carries, then
// Unknown.
func Commit(stamped string) string {
	build, ok := debug.ReadBuildInfo()
	if !ok {
		build = nil
	}
	return resolve(stamped, build)
}

func resolve(stamped string, build *debug.BuildInfo) string {
	if stamped != "" {
		return stamped
	}
	if build != nil {
		for _, setting := range build.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				return setting.Value
			}
		}
	}
	return Unknown
}

// Write prints a daemon's answer to --version: its name and version, its
// commit, and what it was built with, in the columns `orkctl version` prints.
func Write(w io.Writer, name, version, stamped string) error {
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(table, "%s:\t%s\n", name, version)
	fmt.Fprintf(table, "commit:\t%s\n", Commit(stamped))
	fmt.Fprintf(table, "go:\t%s\n", runtime.Version())
	fmt.Fprintf(table, "platform:\t%s/%s\n", runtime.GOOS, runtime.GOARCH)
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
