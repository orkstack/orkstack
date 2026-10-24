package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// nowFunc is the clock the await table's REMAINING column is measured
// against. A variable so a golden test can hold it still.
var nowFunc = time.Now

func newRunResolveCommand(g *globalFlags) *cobra.Command {
	var (
		taskID     string
		values     string
		valuesFile string
		reject     bool
		reason     string
	)
	cmd := &cobra.Command{
		Use:   "resolve <run-id> --task TASK [--values JSON | --values-file PATH | --reject --reason TEXT]",
		Short: "Complete or reject an awaited task of a running run",
		Long: "resolve delivers the outcome of an awaited task from outside its run.\n\n" +
			"A completion's values become the task's values.json, byte for byte:\n" +
			"--values-file sends the file's bytes verbatim (- reads stdin), and\n" +
			"neither --values nor --values-file means {}. --reject fails the task\n" +
			"with AWAIT_REJECTED and needs a --reason.\n\n" +
			"The resolution is stored and delivered to the run before this returns;\n" +
			"the run applies it next. Follow the run to see the outcome: a resolution\n" +
			"that loses a race is recorded on the run, never applied twice.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if taskID == "" {
				return errors.New("--task is required: name the awaited task to resolve")
			}
			valuesSet := cmd.Flags().Changed("values")
			fileSet := cmd.Flags().Changed("values-file")
			if valuesSet && fileSet {
				return errors.New("--values and --values-file are exclusive")
			}
			if reject && (valuesSet || fileSet) {
				return errors.New("values_json and reject are exclusive: a rejection carries a --reason, not values")
			}
			if !reject && cmd.Flags().Changed("reason") {
				return errors.New("reason is only for a rejection: add --reject")
			}
			if reject && reason == "" {
				return errors.New("reason is required with a rejection: say why the await is rejected")
			}
			body := values
			if fileSet {
				raw, err := readValuesFile(cmd.InOrStdin(), valuesFile)
				if err != nil {
					return err
				}
				body = raw
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.runClient().ResolveAwait(ctx, connect.NewRequest(&orkstackv1.ResolveAwaitRequest{
				RunId:      args[0],
				Namespace:  g.namespace,
				TaskId:     taskID,
				ValuesJson: body,
				Reject:     reject,
				Reason:     reason,
			}))
			if err != nil {
				return rpcError("resolve await", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderResolved(cmd.OutOrStdout(), g.namespace, resp.Msg, reason)
		},
	}
	cmd.Flags().StringVar(&taskID, "task", "", "the awaited task to resolve (required)")
	cmd.Flags().StringVar(&values, "values", "", "the completion's values as one JSON object (default {})")
	cmd.Flags().StringVar(&valuesFile, "values-file", "", "read the completion's values from a file, verbatim (- for stdin)")
	cmd.Flags().BoolVar(&reject, "reject", false, "reject the await instead of completing it")
	cmd.Flags().StringVar(&reason, "reason", "", "why the await is rejected (with --reject)")
	return cmd
}

// readValuesFile returns a file's bytes exactly: no trimming, no
// re-encoding, so line endings, indentation and number spelling survive to
// the stored values.json.
func readValuesFile(stdin io.Reader, path string) (string, error) {
	if path == "-" {
		raw, err := io.ReadAll(stdin)
		if err != nil {
			return "", fmt.Errorf("read values from stdin: %w", err)
		}
		return string(raw), nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read --values-file: %w", err)
	}
	return string(raw), nil
}

// renderResolved prints what was delivered — never that it was applied.
func renderResolved(out io.Writer, namespace string, resp *orkstackv1.ResolveAwaitResponse, reason string) error {
	var b strings.Builder
	switch {
	case resp.GetRepeat() && resp.GetReject():
		fmt.Fprintf(&b, "Await %s of run %s was already rejected with this reason; nothing was sent.\n",
			resp.GetTaskId(), resp.GetRunId())
	case resp.GetRepeat():
		fmt.Fprintf(&b, "Await %s of run %s was already resolved with these values; nothing was sent.\n",
			resp.GetTaskId(), resp.GetRunId())
	case resp.GetReject():
		fmt.Fprintf(&b, "Rejection delivered to await %s of run %s\n", resp.GetTaskId(), resp.GetRunId())
		fmt.Fprintf(&b, "  reason  %s\n", reason)
		fmt.Fprintf(&b, "The run applies it next; follow it with: %s\n",
			orkctlCommand(namespace, "run", "watch", resp.GetRunId()))
	default:
		fmt.Fprintf(&b, "Resolution delivered to await %s of run %s\n", resp.GetTaskId(), resp.GetRunId())
		fmt.Fprintf(&b, "  values  sha256:%s   (stored before delivery)\n", resp.GetValuesSha256())
		fmt.Fprintf(&b, "The run applies it next; follow it with: %s\n",
			orkctlCommand(namespace, "run", "watch", resp.GetRunId()))
	}
	if _, err := io.WriteString(out, b.String()); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// awaitCurlExample renders the callback for one await against the engine
// address this command used, with the real token, every value shell-quoted.
func awaitCurlExample(baseURL, path, token string) string {
	return fmt.Sprintf("  curl -X POST %s \\\n"+
		"    -H %s \\\n"+
		"    -H 'Content-Type: application/json' \\\n"+
		"    -d '{}'\n"+
		"  reject: POST the same URL + /reject with -d '{\"reason\":\"…\"}'",
		shellQuote(strings.TrimRight(baseURL, "/")+path),
		shellQuote("Authorization: Bearer "+token))
}

// renderAwaitCallbacks prints each callback the submission disclosed, once.
func renderAwaitCallbacks(out io.Writer, baseURL string, callbacks []*orkstackv1.AwaitCallback) {
	for _, callback := range callbacks {
		fmt.Fprintln(out)
		fmt.Fprintf(out, "Callback for await %s — shown once; only a digest of the token is kept:\n", callback.GetTaskId())
		fmt.Fprintln(out, awaitCurlExample(baseURL, callback.GetPath(), callback.GetToken()))
	}
}

// awaitRemaining is REMAINING: deadline − now for an AWAITING task,
// "expired" when it has passed, and "-" otherwise. ui/src/lib/awaits.ts
// awaitRemaining renders the same duration.
func awaitRemaining(taskRun *orkstackv1.TaskRun, now time.Time) string {
	deadline := taskRun.GetAwait().GetDeadline()
	if taskRun.GetStatus() != orkstackv1.Status_STATUS_AWAITING || deadline == nil {
		return dash
	}
	left := deadline.AsTime().Sub(now)
	if left < 0 {
		return "expired"
	}
	return formatRemaining(left)
}

// formatRemaining is a coarse countdown: hours and minutes, minutes, or
// seconds under a minute.
func formatRemaining(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%dm", int(d/time.Hour), int((d%time.Hour)/time.Minute))
	case d >= time.Minute:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	default:
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
}

// awaitViaLabel is RESOLVED VIA: the door, or "-" when nothing was applied.
func awaitViaLabel(via orkstackv1.AwaitResolvedVia) string {
	switch via {
	case orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API:
		return "api"
	case orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK:
		return "callback"
	default:
		return dash
	}
}

// renderAwaitTable prints the await table under `run get`'s task table, only
// when the run has an awaited task.
func renderAwaitTable(out io.Writer, run *orkstackv1.Run, now time.Time) error {
	hasAwait := false
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetAwait() != nil {
			hasAwait = true
		}
	}
	if !hasAwait {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "AWAIT\tSTATUS\tDEADLINE\tREMAINING\tRESOLVED VIA\tBY\tCALLBACK")
	for _, taskRun := range run.GetTaskRuns() {
		await := taskRun.GetAwait()
		if await == nil {
			continue
		}
		callback := "no"
		if await.GetCallback() {
			callback = "yes"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			taskRun.GetTaskId(), taskStatusLabel(taskRun), formatTime(await.GetDeadline()),
			awaitRemaining(taskRun, now), awaitViaLabel(await.GetResolvedVia()),
			orDash(await.GetResolvedBy()), callback)
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
