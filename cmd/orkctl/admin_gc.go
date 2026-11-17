package main

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newAdminGCCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Inspect artifact reclaim: what the artifact GC deletes",
		Long: "gc inspects the artifact GC (reclaim, as the API calls it), which deletes the\n" +
			"artifact bytes of retired runs during retention cleanup (docs/STORAGE.md,\n" +
			"\"Reclaiming space\").",
	}
	cmd.AddCommand(newAdminGCPreviewCommand(g))
	return cmd
}

func newAdminGCPreviewCommand(g *globalFlags) *cobra.Command {
	var (
		namespace string
		limit     int32
		samples   int32
		cursor    string
	)
	cmd := &cobra.Command{
		Use:   "preview",
		Short: "Show what the artifact GC would delete now, and what it keeps and why",
		Long: "Show what a pass of the artifact GC would do now, for at most --limit retired\n" +
			"runs (1000 at most) after --cursor, within 30 seconds. It writes nothing. Any\n" +
			"replica answers, one preview at a time.\n\n" +
			"It names the run holding the low-water mark: nothing produced before that run\n" +
			"was created is deleted while it is open or unarchived. --samples names objects,\n" +
			"and needs --namespace. Run it before setting cleanup.artifact_gc to delete.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if samples > 0 && namespace == "" {
				return fmt.Errorf("--samples needs --namespace")
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.systemClient().PreviewArtifactReclaim(ctx, connect.NewRequest(&orkstackv1.PreviewArtifactReclaimRequest{
				Namespace: namespace, Limit: limit, Samples: samples, Cursor: cursor,
			}))
			if err != nil {
				return rpcError("admin gc preview", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderReclaimPreview(cmd.OutOrStdout(), resp.Msg)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "limit the preview to one namespace's runs (needed for --samples)")
	cmd.Flags().Int32Var(&limit, "limit", 0, "runs to cover, 1 to 1000 (default 1000)")
	cmd.Flags().Int32Var(&samples, "samples", 0, "objects to name, at most 100")
	cmd.Flags().StringVar(&cursor, "cursor", "", "continue from a previous preview's next cursor")
	return cmd
}

func renderReclaimPreview(out io.Writer, res *orkstackv1.PreviewArtifactReclaimResponse) error {
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "mode\t%s\n", res.GetMode())
	fmt.Fprintf(w, "cleanup enabled\t%t\n", res.GetCleanupEnabled())
	if res.GetHeld() {
		fmt.Fprintf(w, "held\t%s (a pass deletes nothing now)\n", res.GetHeldReason())
	} else {
		fmt.Fprintf(w, "held\tno\n")
	}
	if res.GetStoreVersioned() {
		fmt.Fprintf(w, "store\tversioned: a delete only adds a delete marker\n")
	}
	switch {
	case !res.GetLowWaterKnown():
		fmt.Fprintf(w, "low-water mark\tunknown (%s)\n", res.GetLowWaterUnknownCause())
	case res.GetLowWaterRunId() == "":
		fmt.Fprintf(w, "low-water mark\tnone: no run is open or unarchived\n")
	default:
		fmt.Fprintf(w, "low-water mark\t%s, held by run %s\n", res.GetLowWaterCreatedAt().AsTime().UTC().Format(time.RFC3339), res.GetLowWaterRunId())
	}
	fmt.Fprintf(w, "runs examined\t%d (%d waiting)\n", res.GetRunsExamined(), res.GetWaitingRuns())
	if err := w.Flush(); err != nil {
		return err
	}
	if len(res.GetNamespaces()) > 0 {
		fmt.Fprintln(out)
		t := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(t, "NAMESPACE\tWOULD DELETE\tBYTES\tKEPT")
		for _, ns := range res.GetNamespaces() {
			kept := ""
			for i, k := range ns.GetKept() {
				if i > 0 {
					kept += ", "
				}
				kept += fmt.Sprintf("%s %d", k.GetReason(), k.GetObjects())
			}
			if kept == "" {
				kept = "-"
			}
			fmt.Fprintf(t, "%s\t%d\t%d\t%s\n", ns.GetNamespace(), ns.GetWouldDeleteObjects(), ns.GetWouldDeleteBytes(), kept)
		}
		if err := t.Flush(); err != nil {
			return err
		}
	}
	if len(res.GetSamples()) > 0 {
		fmt.Fprintln(out)
		t := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(t, "RUN\tTASK\tARTIFACT\tBYTES\tOUTCOME\tREASON")
		for _, s := range res.GetSamples() {
			reason := s.GetReason()
			if at := s.GetWaitsForRunsCreatedBefore(); at != nil {
				reason += " (waits for runs created before " + at.AsTime().UTC().Format(time.RFC3339) + ")"
			}
			fmt.Fprintf(t, "%s\t%s\t%s\t%d\t%s\t%s\n", s.GetRunId(), s.GetTaskId(), s.GetArtifactName(), s.GetBytes(), s.GetOutcome(), reason)
		}
		if err := t.Flush(); err != nil {
			return err
		}
	}
	if res.GetNextCursor() != "" {
		fmt.Fprintf(out, "\nmore runs follow: --cursor %s\n", res.GetNextCursor())
	}
	return nil
}
