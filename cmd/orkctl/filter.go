package main

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

// The one filter flag every list command takes (docs/UI_CLI_PARITY.md
// "Filtering lists"). orkctl checks a filter's SYNTAX only; what a field may
// take is the server's to refuse, so the browser and the CLI share one
// validator and cannot disagree. A refusal is printed verbatim.

// addFilterFlag registers the repeatable --filter flag on cmd for the list
// request's allowlist, appends that allowlist to the command's --help, and
// makes `--filter help` print it and exit 0 without a request. Call it after
// RunE is set.
//
// The flag is a StringArray, never a StringSlice: a slice would split
// `--filter status=FAILED,CANCELED` on its comma into two flags, and a
// comma is the in operator.
func addFilterFlag(cmd *cobra.Command, request proto.Message) *[]string {
	var texts []string
	spec := listfilter.MustSpec(request)
	cmd.Flags().StringArrayVar(&texts, "filter", nil,
		"keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)")
	cmd.Long = strings.TrimRight(cmd.Long, "\n") + "\n\n" + strings.TrimRight(spec.Help(), "\n")
	run := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if slices.Contains(texts, "help") {
			_, err := io.WriteString(cmd.OutOrStdout(), spec.Help())
			return err
		}
		return run(cmd, args)
	}
	return &texts
}

// parseFilterFlags reads every --filter value in the CLI grammar. A syntax
// error is refused before any request.
func parseFilterFlags(texts []string) ([]*orkstackv1.FieldFilter, error) {
	out := make([]*orkstackv1.FieldFilter, 0, len(texts))
	for _, text := range texts {
		f, err := listfilter.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("--filter %s: %w", shellQuote(text), err)
		}
		out = append(out, f)
	}
	return out, nil
}

// filterArgs is each filter as the flag that sends it, shell-quoted, for a
// printed next-page command.
func filterArgs(filters []*orkstackv1.FieldFilter) []string {
	out := make([]string, 0, 2*len(filters))
	for _, f := range filters {
		out = append(out, "--filter", shellQuote(listfilter.Format(f)))
	}
	return out
}

// noMatchSentence is what a filtered listing prints instead of an empty
// table: `No namespaces match --filter 'name~zz'.`
func noMatchSentence(noun string, filters []*orkstackv1.FieldFilter) string {
	return fmt.Sprintf("No %s match %s.\n", noun, strings.Join(filterArgs(filters), " "))
}
