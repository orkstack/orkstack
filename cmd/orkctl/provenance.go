package main

import (
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The fixed wordings of a provenance answer. The UI's disclosure renders the
// same three reference states in the same words, so a screenshot and a
// golden file describe one answer.
const (
	provenanceBytesPresent  = "present in the object store"
	provenanceBytesMissing  = "MISSING from the object store — a lifecycle rule or an operator removed them; the references below still stand"
	provenanceHeld          = "held — this run's archive holds its marker on the object"
	provenanceReleased      = "released — cleanup released this run's object claims at %s"
	provenanceAbsent        = "absent — no marker and no cleanup record for this run (not archived yet, or archived before the object index existed)"
	provenanceNoReferences  = "No archived run of this namespace references this object."
	provenanceUnreadableOne = "1 marker could not be resolved: its run body is not in the archive"
	provenanceUnreadableN   = "%d markers could not be resolved: their run bodies are not in the archive"
)

func newRunArtifactProvenanceCommand(g *globalFlags) *cobra.Command {
	var task, kindText, name, expect string
	var item itemFlag
	cmd := &cobra.Command{
		Use:   "provenance <run-id>",
		Short: "Show whether a recorded artifact's bytes remain and which archived runs recorded the same object",
		Long: "provenance reads the object index the run archive keeps for cleanup: whether the\n" +
			"artifact's bytes are still in the object store, whether this run's reference is held,\n" +
			"released by cleanup, or absent, and every archived run of this namespace that recorded\n" +
			"the same object. Missing bytes are a finding, not an error: the command exits 0.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if task == "" || kindText == "" || name == "" {
				return fmt.Errorf("--task, --kind and --name are required")
			}
			kind, err := parseArtifactKind(kindText)
			if err != nil {
				return err
			}
			selected, err := item.resolve(cmd, task)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			request := &orkstackv1.GetArtifactProvenanceRequest{
				RunId: args[0], Namespace: g.namespace, TaskId: task, Item: selected, Kind: kind, Name: name, ExpectSha256: expect,
			}
			response, err := g.runClient().GetArtifactProvenance(ctx, connect.NewRequest(request))
			if err != nil {
				return rpcError("get artifact provenance", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			return renderArtifactProvenance(cmd.OutOrStdout(), response.Msg, request)
		},
	}
	cmd.Flags().StringVar(&task, "task", "", "task that recorded the artifact")
	cmd.Flags().StringVar(&kindText, "kind", "", "artifact kind: output, values, stdout, stderr")
	cmd.Flags().StringVar(&name, "name", "", "exact artifact name returned by run artifacts")
	cmd.Flags().StringVar(&expect, "expect-sha256", "", "fail unless the recorded digest is this lowercase hex sha256")
	item.register(cmd, "the item of the fan-out --task names (required for a fan-out)")
	return cmd
}

// renderArtifactProvenance prints one provenance answer: the artifact, its
// bytes, the addressed run's own reference, and the in-namespace runs that
// recorded the same object, in the order the server returned them. The
// request supplies the artifact's kind, which the response does not echo.
func renderArtifactProvenance(out io.Writer, res *orkstackv1.GetArtifactProvenanceResponse, req *orkstackv1.GetArtifactProvenanceRequest) error {
	artifact := res.GetArtifact()
	head := newTable(out)
	digest := strings.TrimPrefix(artifact.GetSha256(), "sha256:")
	if digest == "" {
		digest = dash
	} else {
		digest = "sha256:" + digest
	}
	fmt.Fprintf(head, "ARTIFACT\t%s  %s  %d bytes  %s\n", artifact.GetName(), strings.TrimPrefix(req.GetKind().String(), "ARTIFACT_KIND_"), artifact.GetSizeBytes(), digest)
	bytesLine := provenanceBytesPresent
	if !res.GetBytesPresent() {
		bytesLine = provenanceBytesMissing
	}
	fmt.Fprintf(head, "BYTES\t%s\n", bytesLine)
	fmt.Fprintf(head, "REFERENCE\t%s\n", referenceLine(res))
	if err := head.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(out)

	if len(res.GetReferences()) == 0 {
		fmt.Fprintln(out, provenanceNoReferences)
	} else {
		table := newTable(out)
		fmt.Fprintln(table, "REFERENCED BY\tORKFLOW\tVERSION\tTASK\tKIND\tSTATUS\tCREATED")
		for _, ref := range res.GetReferences() {
			created := dash
			if ref.GetRunCreatedAt() != nil {
				created = ref.GetRunCreatedAt().AsTime().UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				ref.GetRunId(), orDash(ref.GetOrkflowName()), orDash(ref.GetOrkflowVersion()), itemLabel(ref.GetTaskId(), ref.Item),
				strings.TrimPrefix(ref.GetKind().String(), "ARTIFACT_KIND_"), statusLabel(ref.GetRunStatus()), created)
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	switch n := res.GetReferencesUnreadable(); {
	case n == 1:
		fmt.Fprintln(out, provenanceUnreadableOne)
	case n > 1:
		fmt.Fprintf(out, provenanceUnreadableN+"\n", n)
	}
	return nil
}

func referenceLine(res *orkstackv1.GetArtifactProvenanceResponse) string {
	switch res.GetOwnReference() {
	case orkstackv1.ReferenceState_REFERENCE_STATE_HELD:
		return provenanceHeld
	case orkstackv1.ReferenceState_REFERENCE_STATE_RELEASED:
		at := dash
		if res.GetReleasedAt() != nil {
			at = res.GetReleasedAt().AsTime().UTC().Format(time.RFC3339)
		}
		return fmt.Sprintf(provenanceReleased, at)
	default:
		return provenanceAbsent
	}
}
