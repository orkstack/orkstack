package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/spf13/cobra"
)

func newRunArtifactsCommand(g *globalFlags) *cobra.Command {
	var task string
	cmd := &cobra.Command{Use: "artifacts <run-id>", Short: "List artifacts recorded for a run", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := g.unaryContext(cmd)
		defer cancel()
		response, err := g.runClient().ListRunArtifacts(ctx, connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{RunId: args[0], Namespace: g.namespace, TaskId: task}))
		if err != nil {
			return rpcError("list run artifacts", err)
		}
		if g.json {
			return writeJSON(cmd.OutOrStdout(), response.Msg)
		}
		return renderArtifactList(cmd.OutOrStdout(), response.Msg.GetArtifacts())
	}}
	cmd.Flags().StringVar(&task, "task", "", "only artifacts for this task")
	return cmd
}

func newRunArtifactCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "artifact", Short: "Read one recorded artifact"}
	cmd.AddCommand(newRunArtifactDownloadCommand(g))
	return cmd
}

func newRunArtifactDownloadCommand(g *globalFlags) *cobra.Command {
	var task, kindText, name, output string
	var overwrite bool
	cmd := &cobra.Command{Use: "download <run-id>", Short: "Download one recorded run artifact", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		kind, err := parseArtifactKind(kindText)
		if err != nil {
			return err
		}
		if task == "" || name == "" || output == "" {
			return fmt.Errorf("--task, --kind, --name and --output are required")
		}
		if err := safeOutputPath(output, overwrite); err != nil {
			return err
		}
		ctx, cancel := g.streamContext(cmd)
		defer cancel()
		stream, err := g.runClient().ReadArtifact(ctx, connect.NewRequest(&orkstackv1.ReadArtifactRequest{RunId: args[0], Namespace: g.namespace, TaskId: task, Kind: kind, Name: name}))
		if err != nil {
			return rpcError("read artifact", err)
		}
		defer stream.Close()
		dir := filepath.Dir(output)
		tmp, err := os.CreateTemp(dir, ".orkctl-artifact-*")
		if err != nil {
			return fmt.Errorf("create temporary download: %w", err)
		}
		tmpName := tmp.Name()
		complete := false
		defer func() {
			if !complete {
				_ = os.Remove(tmpName)
			}
		}()
		last := false
		for stream.Receive() {
			chunk := stream.Msg()
			if len(chunk.GetData()) > 0 {
				if _, err := tmp.Write(chunk.GetData()); err != nil {
					_ = tmp.Close()
					return fmt.Errorf("write artifact: %w", err)
				}
			}
			if chunk.GetLast() {
				last = true
			}
		}
		if err := stream.Err(); err != nil {
			_ = tmp.Close()
			return rpcError("read artifact", err)
		}
		if !last {
			_ = tmp.Close()
			return fmt.Errorf("artifact download ended without integrity confirmation")
		}
		if err := tmp.Close(); err != nil {
			return fmt.Errorf("close artifact: %w", err)
		}
		if overwrite {
			if err := os.Rename(tmpName, output); err != nil {
				return fmt.Errorf("commit artifact download: %w", err)
			}
		} else {
			// Link is an atomic no-clobber commit when the temporary file is a
			// sibling of output. A concurrent creator wins without us deleting
			// their file or replacing it after the initial Lstat check.
			if err := os.Link(tmpName, output); err != nil {
				if os.IsExist(err) {
					return fmt.Errorf("output %q already exists (pass --overwrite to replace it)", output)
				}
				return fmt.Errorf("commit artifact download: %w", err)
			}
			if err := os.Remove(tmpName); err != nil {
				return fmt.Errorf("remove temporary download: %w", err)
			}
		}
		complete = true
		return nil
	}}
	cmd.Flags().StringVar(&task, "task", "", "task that recorded the artifact")
	cmd.Flags().StringVar(&kindText, "kind", "", "artifact kind: output, values, stdout, stderr")
	cmd.Flags().StringVar(&name, "name", "", "exact artifact name returned by run artifacts")
	cmd.Flags().StringVar(&output, "output", "", "destination file")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "replace an existing destination file")
	return cmd
}
func safeOutputPath(output string, overwrite bool) error {
	if output == "" || filepath.Base(output) == "." || filepath.Base(output) == string(filepath.Separator) {
		return fmt.Errorf("--output must name a file")
	}
	return nil
}
func parseArtifactKind(value string) (orkstackv1.ArtifactKind, error) {
	switch strings.ToLower(value) {
	case "output":
		return orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, nil
	case "values":
		return orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES, nil
	case "stdout":
		return orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT, nil
	case "stderr":
		return orkstackv1.ArtifactKind_ARTIFACT_KIND_STDERR, nil
	default:
		return orkstackv1.ArtifactKind_ARTIFACT_KIND_UNSPECIFIED, fmt.Errorf("--kind must be output, values, stdout, or stderr")
	}
}
func renderArtifactList(out io.Writer, items []*orkstackv1.RunArtifact) error {
	if len(items) == 0 {
		_, err := fmt.Fprintln(out, "No artifacts.")
		return err
	}
	table := newTable(out)
	fmt.Fprintln(table, "TASK\tATTEMPT\tKIND\tNAME\tSIZE\tSHA256")
	for _, item := range items {
		fmt.Fprintf(table, "%s\t%d\t%s\t%s\t%d\t%s\n", item.GetTaskId(), item.GetAttempt(), strings.TrimPrefix(item.GetKind().String(), "ARTIFACT_KIND_"), item.GetArtifact().GetName(), item.GetArtifact().GetSizeBytes(), item.GetArtifact().GetSha256())
	}
	return table.Flush()
}
