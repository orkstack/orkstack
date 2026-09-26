package main

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// diffFieldCount is the size of the full DiffRuns vocabulary: a
// fields_compared shorter than this is a narrowed diff, and says so.
const diffFieldCount = 13

// runDiffReasons maps each DiffRuns reason token to its documented sentence
// (docs/UI_CLI_PARITY.md "Run diff"; ui/src/lib/runDiff.ts carries the same
// table). A token this build has never heard of is printed as itself.
var runDiffReasons = map[string]string{
	"definition_unavailable":     "the pinned definition is no longer stored",
	"definition_digest_mismatch": "the stored definition does not hash to the digest this run pinned",
	"task_not_in_definition":     "the pinned definition has no task of this id",
	"no_attempt_store":           "this engine has no task attempt store configured",
	"no_outcome_attempt":         "the snapshot names no outcome attempt",
	"attempt_record_missing":     "no record of the outcome attempt or of a retry that adopted its commit: reclaimed, or the Orker never filed one",
	"timestamps_missing":         "the snapshot lacks a start or finish time",
	"timestamps_inverted":        "the finish time precedes the start time",
}

// reasonText is a reason token's sentence, or the token itself when unknown.
func reasonText(token string) string {
	if sentence, ok := runDiffReasons[token]; ok {
		return sentence
	}
	return token
}

// newRunDiffCommand builds `orkctl run diff`.
func newRunDiffCommand(g *globalFlags) *cobra.Command {
	var only []string
	cmd := &cobra.Command{
		Use:   "diff <from-run-id> <to-run-id>",
		Short: "Compare two runs field by field",
		Long: "diff compares two runs of one namespace: the pinned definition, the input,\n" +
			"and per task its status, attempt, pool, pin, orker, exit code, error,\n" +
			"duration, the image it was dispatched with and the platform manifest the\n" +
			"Orker ran. Only differences are listed.\n\n" +
			"A value that does not apply prints as -, and evidence that is gone (a\n" +
			"reclaimed attempt record, a deleted definition) prints as unavailable with\n" +
			"its reason under the table: the two never look alike.\n\n" +
			"Two runs of different orkflows are compared and flagged, never refused.\n" +
			"--only narrows the fields (and what the engine reads); a task only one run\n" +
			"has is always listed, whatever --only names. The comparison is computed by\n" +
			"the engine on every call and stored nowhere. Exit status is 0 whenever the\n" +
			"comparison succeeded, identical or not; scripts read --json.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.runClient().DiffRuns(ctx, connect.NewRequest(&orkstackv1.DiffRunsRequest{
				FromRunId: args[0],
				ToRunId:   args[1],
				Namespace: g.namespace,
				Only:      only,
			}))
			if err != nil {
				return rpcError("diff runs", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			return renderRunDiff(cmd.OutOrStdout(), g.namespace, response.Msg)
		},
	}
	// A StringSlice, unlike every other list flag here: field names never
	// contain a comma, and comma splitting is the UX this flag wants.
	cmd.Flags().StringSliceVar(&only, "only", nil,
		"compare only these fields (comma-separated or repeated); presence is always compared")
	return cmd
}

// renderRunDiff prints a DiffRunsResponse. Every value is the server's
// rendered string, printed verbatim; this function decides only layout.
func renderRunDiff(out io.Writer, namespace string, resp *orkstackv1.DiffRunsResponse) error {
	from, to := resp.GetFrom(), resp.GetTo()
	if namespace == "" {
		namespace = "default"
	}
	header := func(label, value string) { fmt.Fprintf(out, "%-13s%s\n", label+":", value) }

	header("Diff", from.GetId()+" -> "+to.GetId())
	header("Namespace", namespace)
	if len(resp.GetFieldsCompared()) < diffFieldCount {
		header("Compared", strings.Join(resp.GetFieldsCompared(), ", "))
	}
	if resp.GetSameOrkflow() {
		header("Orkflow", from.GetOrkflowName()+" (same orkflow)")
	} else {
		header("Orkflow", from.GetOrkflowName()+" -> "+to.GetOrkflowName()+" (DIFFERENT ORKFLOWS)")
	}
	header("Status", statusLabel(from.GetStatus())+" -> "+statusLabel(to.GetStatus()))
	header("Finished", formatTime(from.GetFinishedAt())+" -> "+formatTime(to.GetFinishedAt()))
	header("Definition", definitionLine(resp))
	header("Input", inputLine(resp))
	if diff := resp.GetInputUnifiedDiff(); diff != "" {
		fmt.Fprintln(out)
		fmt.Fprint(out, diff)
	}
	fmt.Fprintln(out)

	var taskRows []*orkstackv1.RunFieldDiff
	for _, row := range resp.GetDifferences() {
		if row.GetTaskId() != "" {
			taskRows = append(taskRows, row)
		}
	}
	if len(taskRows) == 0 {
		fmt.Fprintln(out, "No task differences.")
	} else {
		table := newTable(out)
		fmt.Fprintln(table, "TASK\tFIELD\tFROM\tTO")
		for _, row := range taskRows {
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", row.GetTaskId(), row.GetField(),
				diffCell(row.GetFromValue(), row.GetFromAvailable()),
				diffCell(row.GetToValue(), row.GetToAvailable()))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, diffSummary(resp))
	for _, line := range unavailableLines(resp) {
		fmt.Fprintln(out, line)
	}
	return nil
}

// definitionLine reads the run-level definition row against the digest flag.
func definitionLine(resp *orkstackv1.DiffRunsResponse) string {
	if !slices.Contains(resp.GetFieldsCompared(), "definition") {
		return "not compared"
	}
	for _, row := range resp.GetDifferences() {
		if row.GetTaskId() == "" && row.GetField() == "definition" {
			if resp.GetSameDefinitionDigest() {
				return "same digest, version moved: " + row.GetFromValue() + " -> " + row.GetToValue()
			}
			return "differs: " + row.GetFromValue() + " -> " + row.GetToValue()
		}
	}
	return "identical (" + orDash(resp.GetFrom().GetOrkflowDefinitionDigest()) + ")"
}

// inputLine summarizes the input diff; the text itself follows verbatim.
func inputLine(resp *orkstackv1.DiffRunsResponse) string {
	if !slices.Contains(resp.GetFieldsCompared(), "input") {
		return "not compared"
	}
	diff := resp.GetInputUnifiedDiff()
	if diff == "" {
		return "identical"
	}
	changed := 0
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "--- ") || strings.HasPrefix(line, "+++ ") {
			continue
		}
		if strings.HasPrefix(line, "+") || strings.HasPrefix(line, "-") {
			changed++
		}
	}
	return fmt.Sprintf("differs (%d lines)", changed)
}

// diffCell renders one side: - for "does not apply", unavailable for
// evidence that is gone.
func diffCell(value string, available bool) string {
	if !available {
		return "unavailable"
	}
	return orDash(value)
}

// diffSummary is the one summary sentence both clients print.
func diffSummary(resp *orkstackv1.DiffRunsResponse) string {
	compared, identical := resp.GetTasksCompared(), resp.GetTasksIdentical()
	line := fmt.Sprintf("%d of %d tasks differ; %d identical", compared-identical, compared, identical)
	switch unavailable := resp.GetEvidenceUnavailable(); unavailable {
	case 0:
	case 1:
		line += "; 1 field unavailable"
	default:
		line += fmt.Sprintf("; %d fields unavailable", unavailable)
	}
	return line + "."
}

// unavailableLines is one footnote per unavailable side, in row order.
func unavailableLines(resp *orkstackv1.DiffRunsResponse) []string {
	var lines []string
	footnote := func(row *orkstackv1.RunFieldDiff, runID, reason string) {
		subject := row.GetTaskId()
		if subject == "" {
			subject = "run"
		}
		line := fmt.Sprintf("%s %s in %s: %s", subject, row.GetField(), runID, reason)
		if sentence := reasonText(reason); sentence != reason {
			line += " (" + sentence + ")"
		}
		lines = append(lines, line)
	}
	for _, row := range resp.GetDifferences() {
		if !row.GetFromAvailable() {
			footnote(row, resp.GetFrom().GetId(), row.GetFromReason())
		}
		if !row.GetToAvailable() {
			footnote(row, resp.GetTo().GetId(), row.GetToReason())
		}
	}
	return lines
}
