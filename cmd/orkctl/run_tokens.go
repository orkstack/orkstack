package main

import (
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// newRunTokensCommand lists what a run's attempts were issued: each
// token's claims, never a token (docs/TASK_IDENTITY.md, "What was issued").
func newRunTokensCommand(g *globalFlags) *cobra.Command {
	var task string
	var item int32
	cmd := &cobra.Command{
		Use:   "tokens <run-id>",
		Short: "List the identity tokens a run's attempts were issued",
		Long: "tokens lists one line a token each mint of the run issued, in the order they\n" +
			"were issued: the task, item and attempt, the identity, its audience and\n" +
			"subject, the token's id and when it expires. No token is ever shown or kept.",
		Example: "  orkctl run tokens ork-01jx... --task apply",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			req := &orkstackv1.ListIssuedTokensRequest{Namespace: g.namespace, RunId: args[0], TaskId: task}
			if cmd.Flags().Changed("item") {
				if task == "" {
					return fmt.Errorf("--item needs --task")
				}
				req.Item = &item
			}
			var records []*orkstackv1.IssuedTokenRecord
			for {
				resp, err := g.identityClient().ListIssuedTokens(ctx, connect.NewRequest(req))
				if err != nil {
					return rpcError("run tokens", err)
				}
				records = append(records, resp.Msg.GetRecords()...)
				if resp.Msg.GetNextPageToken() == "" {
					break
				}
				req.PageToken = resp.Msg.GetNextPageToken()
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), &orkstackv1.ListIssuedTokensResponse{Records: records})
			}
			return renderIssuedTokens(cmd.OutOrStdout(), records)
		},
	}
	cmd.Flags().StringVarP(&task, "task", "t", "", "only this task's")
	cmd.Flags().Int32Var(&item, "item", 0, "only this item of the --task fan-out")
	return cmd
}

func renderIssuedTokens(out io.Writer, records []*orkstackv1.IssuedTokenRecord) error {
	if len(records) == 0 {
		_, err := fmt.Fprintln(out, "No token was issued to this run.")
		return err
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TASK\tITEM\tATTEMPT\tIDENTITY\tAUDIENCE\tSUBJECT\tJTI\tORKER\tEXPIRES")
	for _, r := range records {
		itemCell := "-"
		if r.Item != nil {
			itemCell = strconv.Itoa(int(r.GetItem()))
		}
		for _, t := range r.GetTokens() {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", r.GetTaskId(), itemCell, r.GetAttempt(), t.GetName(), t.GetAudience(),
				t.GetSubject(), t.GetJti(), r.GetOrkerId(), formatTime(t.GetExpiresAt()))
		}
	}
	return w.Flush()
}
