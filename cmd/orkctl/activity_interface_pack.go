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
// The org.orkstack.activity.interface, .icon and .docs labels are the ABI
// the engine reads, each one line. A person writes all three as one
// kind: ActivityInterface document; this prints the value of the one
// --label names, for any toolchain's --label. It reads a local file and
// asks no engine.
func newActivityInterfaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "interface",
		Short: "Author the with: interface an activity image declares",
	}
	cmd.AddCommand(newActivityInterfacePackCommand())
	return cmd
}

func newActivityInterfacePackCommand() *cobra.Command {
	var which string
	cmd := &cobra.Command{
		Use:   "pack <file>",
		Short: "Print a label value a kind: ActivityInterface document packs into",
		Long: "Read a kind: ActivityInterface document and print the value of one label it\n" +
			"packs into: one line — compact JSON, or the icon's canonical SVG — the same\n" +
			"bytes for the same declaration.\n" +
			"The document is checked by the rules the engine reads each label by, so a\n" +
			"value this prints is never one the engine refuses.\n\n" +
			"--label names the label: interface (the default) is " + dsl.InterfaceLabel + ",\n" +
			"icon is " + dsl.IconLabel + " and docs is " + dsl.DocsLabel + ",\n" +
			"which hold the document's icon: and its descriptions. A document that\n" +
			"declares no icon, or no description, prints an empty line: the empty value\n" +
			"declares none, and clears a label a base image set.\n\n" +
			"Pass the values to any toolchain's --label, or use `orkctl activity build\n" +
			"--interface FILE`, which passes all three itself.",
		Example: "  orkctl activity interface pack interface.ork.yaml\n" +
			"  orkctl activity interface pack --label icon interface.ork.yaml\n" +
			`  docker build --label "` + dsl.InterfaceLabel + `=$(orkctl activity interface pack interface.ork.yaml)" \` + "\n" +
			`    --label "` + dsl.IconLabel + `=$(orkctl activity interface pack --label icon interface.ork.yaml)" \` + "\n" +
			`    --label "` + dsl.DocsLabel + `=$(orkctl activity interface pack --label docs interface.ork.yaml)" .`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch which {
			case "interface", "icon", "docs":
			default:
				return fmt.Errorf("--label %q is not a label an activity interface packs into: interface, icon or docs", which)
			}
			labels, err := packDocumentFile(args[0])
			if err != nil {
				return err
			}
			for _, label := range labels {
				if label.short == which {
					fmt.Fprintln(cmd.OutOrStdout(), label.value)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&which, "label", "interface", "the label to print: interface, icon or docs")
	return cmd
}

// packedLabel is one label a document packs into: its short name, its key
// and its value ("" for an icon or docs the document does not declare).
type packedLabel struct {
	short, key, value string
}

// packDocumentFile reads one kind: ActivityInterface document and returns
// every label it packs into, interface first. Every refusal names the file.
func packDocumentFile(path string) ([]packedLabel, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	decl, err := dsl.ParseActivityDocument(source)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	iface, err := dsl.PackInterfaceLabel(decl.Interface)
	if err != nil {
		return nil, fmt.Errorf("%s: %s label: %w", path, dsl.InterfaceLabel, err)
	}
	icon, err := dsl.PackIconLabel(decl.Icon)
	if err != nil {
		return nil, fmt.Errorf("%s: %s label: %w", path, dsl.IconLabel, err)
	}
	docs, err := dsl.PackDocsLabel(decl.Docs, decl.Interface)
	if err != nil {
		return nil, fmt.Errorf("%s: %s label: %w", path, dsl.DocsLabel, err)
	}
	return []packedLabel{
		{"interface", dsl.InterfaceLabel, iface},
		{"icon", dsl.IconLabel, icon},
		{"docs", dsl.DocsLabel, docs},
	}, nil
}
