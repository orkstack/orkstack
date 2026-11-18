package main

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// maxRestorePages bounds how many pages one reconcile walks: 1000 pages of
// 1000 cleanup records each.
const maxRestorePages = 1000

func newAdminRestoreCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Reconcile the deployment after a restore of Temporal or the bucket",
		Long: "restore holds the steps of a restore that only OrkStack can take\n" +
			"(docs/RUNBOOKS.md, \"Backup and disaster recovery\").",
	}
	cmd.AddCommand(newAdminRestoreReconcileCommand(g))
	return cmd
}

func newAdminRestoreReconcileCommand(g *globalFlags) *cobra.Command {
	var (
		restoredAt string
		dryRun     bool
		limit      int32
		yes        bool
	)
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "Find runs a restore of Temporal brought back after cleanup retired them",
		Long: "After Temporal was restored to the instant --temporal-restored-at, list the\n" +
			"runs retention cleanup retired after that instant whose executions the restore\n" +
			"brought back, and, without --dry-run, delete those executions where the bucket\n" +
			"holds the run's archive, which then answers for the run. A running execution\n" +
			"is never deleted: cancel it, or let it finish. It also lists the runs Temporal\n" +
			"shows open whose finished archive the bucket holds: they will run their tail,\n" +
			"and send its events and webhooks, again. Nothing acts on them.\n\n" +
			"It deletes no object. Without --dry-run the engine refuses while its retention\n" +
			"cleanup is on: turn cleanup off on every engine first (ORK_CLEANUP_ENABLED=false).\n" +
			"Without --dry-run it asks for confirmation; it refuses when standard input is\n" +
			"not a terminal unless --yes is given.",
		Example: "  orkctl admin restore reconcile --temporal-restored-at 2026-11-17T03:00:00Z --dry-run\n" +
			"  orkctl admin restore reconcile --temporal-restored-at 2026-11-17T03:00:00Z",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if restoredAt == "" {
				return fmt.Errorf("--temporal-restored-at is required: the instant Temporal was restored to, in RFC 3339")
			}
			at, err := time.Parse(time.RFC3339, restoredAt)
			if err != nil {
				return fmt.Errorf("--temporal-restored-at %q is not an RFC 3339 instant: %w", restoredAt, err)
			}
			if limit < 0 || limit > 1000 {
				return fmt.Errorf("--limit must be 1 to 1000")
			}
			if !dryRun {
				prompt := fmt.Sprintf("Deleting the Temporal executions of every run retention cleanup retired after %s\n"+
					"whose archive the bucket holds, and that is not running. Its archive then answers for the run.",
					at.UTC().Format(time.RFC3339))
				if err := confirmDestructive(cmd, g, "reconcile the restore", prompt, yes); err != nil {
					return err
				}
			}
			all, err := reconcileAllPages(cmd, g, &orkstackv1.ReconcileRestoreRequest{
				TemporalRestoredAt: timestamppb.New(at), DryRun: dryRun, Limit: limit,
			})
			if err != nil {
				return err
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), all)
			}
			return renderRestoreReconcile(cmd.OutOrStdout(), all, dryRun)
		},
	}
	cmd.Flags().StringVar(&restoredAt, "temporal-restored-at", "", "the instant Temporal was restored to, in RFC 3339 (required)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "list what it would delete, and delete nothing")
	cmd.Flags().Int32Var(&limit, "limit", 0, "cleanup records each request reads, 1 to 1000 (default 1000)")
	addYesFlag(cmd, &yes)
	return cmd
}

// reconcileAllPages walks every page and folds them into one answer.
func reconcileAllPages(cmd *cobra.Command, g *globalFlags, req *orkstackv1.ReconcileRestoreRequest) (*orkstackv1.ReconcileRestoreResponse, error) {
	all := &orkstackv1.ReconcileRestoreResponse{}
	for page := 0; ; page++ {
		if page == maxRestorePages {
			return nil, fmt.Errorf("admin restore reconcile: more than %d pages; rerun it to continue", maxRestorePages)
		}
		ctx, cancel := g.unaryContext(cmd)
		resp, err := g.systemClient().ReconcileRestore(ctx, connect.NewRequest(req))
		cancel()
		if err != nil {
			return nil, rpcError("admin restore reconcile", err)
		}
		m := resp.Msg
		all.Retired = append(all.Retired, m.GetRetired()...)
		if page == 0 {
			all.FinishedRunIds, all.FinishedTruncated = m.GetFinishedRunIds(), m.GetFinishedTruncated()
		}
		all.Listed += m.GetListed()
		if m.GetNextCursor() == "" {
			return all, nil
		}
		req.Cursor = m.GetNextCursor()
	}
}

func renderRestoreReconcile(out io.Writer, res *orkstackv1.ReconcileRestoreResponse, dryRun bool) error {
	fmt.Fprintf(out, "cleanup records read: %d\n", res.GetListed())
	if len(res.GetRetired()) == 0 {
		fmt.Fprintln(out, "retired after the restore, with an execution: none")
	} else {
		fmt.Fprintln(out)
		w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "RUN\tRETIRED AT\tARCHIVED\tACTION")
		for _, r := range res.GetRetired() {
			fmt.Fprintf(w, "%s\t%s\t%t\t%s\n", r.GetRunId(), r.GetRetiredAt().AsTime().UTC().Format(time.RFC3339), r.GetArchived(), restoreAction(r, dryRun))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	fmt.Fprintln(out)
	if len(res.GetFinishedRunIds()) == 0 && !res.GetFinishedTruncated() {
		fmt.Fprintln(out, "open runs that already finished in the archive: none")
		return nil
	}
	fmt.Fprintln(out, "open runs that already finished in the archive (they run their tail again; nothing acts on them):")
	for _, id := range res.GetFinishedRunIds() {
		fmt.Fprintf(out, "  %s\n", id)
	}
	if res.GetFinishedTruncated() {
		fmt.Fprintln(out, "  (more runs are open than one answer lists; the list is incomplete)")
	}
	return nil
}

// restoreAction is what happened, or would, to one retired run's executions.
func restoreAction(r *orkstackv1.RestoreRetiredRun, dryRun bool) string {
	switch {
	case r.GetOpen():
		return "kept: running (cancel it, or let it finish)"
	case !r.GetArchived():
		return "kept: no archive answers for it"
	case r.GetDeleted():
		return "deleted"
	case dryRun:
		return "would delete"
	default:
		return "kept"
	}
}
