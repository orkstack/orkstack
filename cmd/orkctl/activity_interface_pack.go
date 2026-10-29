package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/orkstack/orkstack/internal/dsl"
)

// `orkctl activity interface pack <file>` (docs/ACTIVITY_IMAGES.md,
// "Declaring the interface").
//
// The org.orkstack.activity.interface label is the ABI the engine reads, and
// its value is one line of JSON. A person writes the same declaration as a
// kind: ActivityInterface document; this prints the label value that
// document packs into, for any toolchain's --label. It reads a local file
// and asks no engine.
func newActivityInterfaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "interface",
		Short: "Author the with: interface an activity image declares",
	}
	cmd.AddCommand(newActivityInterfacePackCommand())
	return cmd
}

func newActivityInterfacePackCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "pack <file>",
		Short: "Print the interface label value a kind: ActivityInterface document packs into",
		Long: "Read a kind: ActivityInterface document and print the value of the\n" +
			dsl.InterfaceLabel + " label it packs into: compact JSON,\n" +
			"one line, the same bytes for the same declaration. The document is checked\n" +
			"by the rules the engine reads a label by, so a value this prints is never\n" +
			"one the engine refuses.\n\n" +
			"Pass the value to any toolchain's --label, or use `orkctl activity build\n" +
			"--interface FILE`, which does so itself.",
		Example: "  orkctl activity interface pack interface.ork.yaml\n" +
			`  docker build --label "` + dsl.InterfaceLabel + `=$(orkctl activity interface pack interface.ork.yaml)" .`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			label, err := packInterfaceFile(args[0])
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), label)
			return nil
		},
	}
}

// packInterfaceFile reads one kind: ActivityInterface document and returns
// the label value it packs into. Every refusal names the file.
func packInterfaceFile(path string) (string, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	iface, err := dsl.ParseInterfaceDocument(source)
	if err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	label, err := dsl.PackInterfaceLabel(iface)
	if err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	return label, nil
}
