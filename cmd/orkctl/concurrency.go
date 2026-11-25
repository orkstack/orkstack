package main

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// A namespace's concurrency limits on the command line (docs/DSL.md
// "Concurrency limits"): `concurrency list` and `concurrency get`, the
// ConcurrencyService's two reads.

func (g *globalFlags) concurrencyClient() orkstackv1connect.ConcurrencyServiceClient {
	return orkstackv1connect.NewConcurrencyServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func newConcurrencyCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "concurrency",
		Short: "Read the namespace's concurrency limits: who holds each key and who waits",
		Long: "A concurrency limit (concurrency: in an Orkflow) bounds how many runs hold one\n" +
			"key at once. These commands read each key's limit as it stands: the runs\n" +
			"holding its slots and the runs waiting in line, in the order they start.\n\n" +
			"A key is readable by anyone who may read the namespace's runs, and is stored\n" +
			"unencrypted: never render a secret or personal data into one.",
	}
	cmd.AddCommand(newConcurrencyListCommand(g), newConcurrencyGetCommand(g))
	return cmd
}

func newConcurrencyListCommand(g *globalFlags) *cobra.Command {
	var filterTexts *[]string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the keys with runs holding or waiting",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			out := &orkstackv1.ListConcurrencyLimitsResponse{}
			token := ""
			for {
				// Each page is one request with its own deadline.
				ctx, cancel := g.unaryContext(cmd)
				resp, err := g.concurrencyClient().ListConcurrencyLimits(ctx, connect.NewRequest(&orkstackv1.ListConcurrencyLimitsRequest{
					Namespace: g.namespace, Filters: filters, PageToken: token}))
				cancel()
				if err != nil {
					return rpcError("list concurrency limits", err)
				}
				out.Limits = append(out.Limits, resp.Msg.GetLimits()...)
				if token = resp.Msg.GetNextPageToken(); token == "" {
					break
				}
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			if len(filters) > 0 && len(out.GetLimits()) == 0 {
				fmt.Fprint(cmd.OutOrStdout(), noMatchSentence("concurrency limits", filters))
				return nil
			}
			return renderConcurrencyList(cmd.OutOrStdout(), out.GetLimits())
		},
	}
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListConcurrencyLimitsRequest{})
	return cmd
}

func newConcurrencyGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get KEY",
		Short: "Show one key's holders and line",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.concurrencyClient().GetConcurrencyLimit(ctx, connect.NewRequest(&orkstackv1.GetConcurrencyLimitRequest{
				Namespace: g.namespace, Key: args[0]}))
			if err != nil {
				return rpcError("get concurrency limit", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderConcurrencyLimit(cmd.OutOrStdout(), resp.Msg.GetLimit())
		},
	}
}

// concurrencyState is a limit's STATE cell.
func concurrencyState(l *orkstackv1.ConcurrencyLimit) string {
	if !l.GetAnswering() {
		return "not answering"
	}
	switch l.GetRebuild() {
	case orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_READING, orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_SETTLING:
		return "reading its holders"
	case orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_FAILING:
		return "reading its holders, failing since " + formatTime(l.GetRebuildFailingSince())
	}
	return "ok"
}

func renderConcurrencyList(out io.Writer, limits []*orkstackv1.ConcurrencyLimit) error {
	if len(limits) == 0 {
		fmt.Fprintln(out, "No key has runs holding or waiting.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "KEY\tHOLDING\tWAITING\tSTATE")
	for _, l := range limits {
		if !l.GetAnswering() {
			fmt.Fprintf(table, "%s\t%s\t%s\tnot answering: %s\n", orDash(l.GetKey()), dash, dash, l.GetNotAnsweringReason())
			continue
		}
		fmt.Fprintf(table, "%s\t%d\t%d\t%s\n", l.GetKey(), l.GetHoldersCount(), l.GetQueuedCount(), concurrencyState(l))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func renderConcurrencyLimit(out io.Writer, l *orkstackv1.ConcurrencyLimit) error {
	header := newTable(out)
	fmt.Fprintf(header, "Key:\t%s\n", l.GetKey())
	fmt.Fprintf(header, "Namespace:\t%s\n", orDash(l.GetNamespace()))
	fmt.Fprintf(header, "State:\t%s\n", concurrencyState(l))
	if !l.GetAnswering() {
		fmt.Fprintf(header, "Why:\t%s\n", l.GetNotAnsweringReason())
		return flushTable(header)
	}
	fmt.Fprintf(header, "Epoch:\t%d\n", l.GetEpoch())
	if err := flushTable(header); err != nil {
		return err
	}
	fmt.Fprintf(out, "\nHolding (%d):\n", l.GetHoldersCount())
	if len(l.GetHolders()) == 0 {
		fmt.Fprintln(out, "  -")
	} else {
		table := newTable(out)
		fmt.Fprintln(table, "  RUN\tLEVEL\tSINCE\tLIMIT\tON LIMIT\tNOTE")
		for _, h := range l.GetHolders() {
			note := dash
			if h.GetStalled() {
				note = "not making progress; an operator must end it"
			}
			fmt.Fprintf(table, "  %s\t%s\t%s\t%d\t%s\t%s\n", h.GetRunId(), concurrencyLevel(h.GetTaskLevel()),
				formatTime(h.GetGrantedAt()), h.GetLimit(), strategyName(h.GetStrategy()), note)
		}
		if err := flushTable(table); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "\nWaiting (%d):\n", l.GetQueuedCount())
	if len(l.GetLine()) == 0 {
		fmt.Fprintln(out, "  -")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "  #\tRUN\tLEVEL\tSINCE\tTIMES OUT\tLIMIT")
	for i, e := range l.GetLine() {
		fmt.Fprintf(table, "  %d\t%s\t%s\t%s\t%s\t%d\n", i+1, e.GetRunId(), concurrencyLevel(e.GetTaskLevel()),
			formatTime(e.GetArrivedAt()), formatTime(e.GetTimesOutAt()), e.GetLimit())
	}
	return flushTable(table)
}

func concurrencyLevel(task bool) string {
	if task {
		return "task"
	}
	return "run"
}

func strategyName(s orkstackv1.ConcurrencyStrategy) string {
	switch s {
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE:
		return "queue"
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE:
		return "refuse"
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP:
		return "skip"
	}
	return dash
}

func flushTable(t interface{ Flush() error }) error {
	if err := t.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
