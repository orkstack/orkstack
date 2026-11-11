package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// shorthands is orkctl's whole letter table: each letter and the long flags
// it may stand for. A letter means one flag on any one command; -f is --file
// wherever a command reads a file, and --follow on run logs, which reads none.
// Every letter is CLI surface to keep, so a new one is added here on purpose.
var shorthands = map[string][]string{
	"F": {"filter"},
	"f": {"file", "follow"},
	"g": {"group-by"},
	"h": {"help"}, // Cobra's own, on every command
	"i": {"input"},
	"j": {"json"},
	"n": {"namespace"},
	"o": {"output"},
	"s": {"since"},
	"t": {"task"},
	"w": {"watch"},
}

// TestFlags_ShorthandLettersKeepOneMeaningPerCommand walks every command with
// its inherited flags: a letter names one flag on it, that flag is one the
// table allows the letter, and a long flag the table names always carries
// its letter, so a new --task without -t fails here rather than in a doc.
func TestFlags_ShorthandLettersKeepOneMeaningPerCommand(t *testing.T) {
	root, _ := newRootCommand()
	letterOf := map[string]string{}
	for letter, longs := range shorthands {
		for _, long := range longs {
			letterOf[long] = letter
		}
	}
	var bad []string
	commands := 0
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		commands++
		meaning := map[string]string{}
		visit := func(f *pflag.Flag) {
			if f.Shorthand != "" {
				if other, ok := meaning[f.Shorthand]; ok && other != f.Name {
					bad = append(bad, fmt.Sprintf("%s: -%s is both --%s and --%s", cmd.CommandPath(), f.Shorthand, other, f.Name))
				}
				meaning[f.Shorthand] = f.Name
				if allowed, ok := shorthands[f.Shorthand]; !ok || !slices.Contains(allowed, f.Name) {
					bad = append(bad, fmt.Sprintf("%s: -%s for --%s is not in the letter table", cmd.CommandPath(), f.Shorthand, f.Name))
				}
			}
			if letter, ok := letterOf[f.Name]; ok && f.Shorthand != letter {
				bad = append(bad, fmt.Sprintf("%s: --%s has shorthand %q, want -%s", cmd.CommandPath(), f.Name, f.Shorthand, letter))
			}
		}
		cmd.LocalFlags().VisitAll(visit)
		cmd.InheritedFlags().VisitAll(visit)
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(root)
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("%d shorthand problems:\n%s", len(bad), strings.Join(bad, "\n"))
	}
	t.Logf("%d commands, %d letters", commands, len(shorthands))
}
