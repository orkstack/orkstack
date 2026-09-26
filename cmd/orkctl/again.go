package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"
)

// newRunAgainCommand reuses the source's pinned coordinates and inputs, but
// deliberately never its idempotency key: this is a new external execution.
func newRunAgainCommand(g *globalFlags) *cobra.Command {
	var version, key, inputJSON, originRef string
	var inputs []string
	var dryRun, allowEmptyPool bool
	cmd := &cobra.Command{Use: "again <run-id>", Short: "Submit a new run from a prior run's pinned definition and inputs", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := g.unaryContext(cmd)
		defer cancel()
		source, err := g.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: args[0], Namespace: g.namespace}))
		if err != nil {
			return rpcError("get source run", err)
		}
		if source.Msg.GetRun() == nil || source.Msg.GetRun().GetOrkflowName() == "" || source.Msg.GetRun().GetOrkflowVersion() == "" {
			return fmt.Errorf("source run %q has no pinned orkflow coordinates and cannot be run again", args[0])
		}
		chosen := source.Msg.GetRun().GetOrkflowVersion()
		if version != "" {
			chosen = version
		}
		input, err := againInput(source.Msg.GetRun().GetInput(), inputs, inputJSON)
		if err != nil {
			return err
		}
		// SourceRunId is what makes the new run's origin run-again <source>: a
		// claim the server checks exists in this namespace. It is set before
		// --dry-run so the printed submission is exactly what would be sent.
		request := &orkstackv1.SubmitRunRequest{OrkflowName: source.Msg.GetRun().GetOrkflowName(), OrkflowVersion: chosen, Input: input, IdempotencyKey: key, Namespace: g.namespace, AllowEmptyPool: allowEmptyPool, SourceRunId: args[0], OriginReference: originRef}
		if dryRun {
			return writeJSON(cmd.OutOrStdout(), request)
		}
		if !g.json {
			fmt.Fprintln(cmd.OutOrStdout(), "This starts a new execution and may repeat external side effects.")
		}
		response, err := g.runClient().SubmitRun(ctx, connect.NewRequest(request))
		if err != nil {
			return rpcError("run again", err)
		}
		if g.json {
			return writeJSON(cmd.OutOrStdout(), response.Msg)
		}
		return renderSubmitted(cmd.OutOrStdout(), response.Msg.GetRun())
	}}
	cmd.Flags().StringVar(&version, "version", "", "override the source run's pinned orkflow version")
	cmd.Flags().StringArrayVar(&inputs, "input", nil, "override source input as key=value (repeatable)")
	cmd.Flags().StringVar(&inputJSON, "input-json", "", "JSON object merged into source inputs before --input")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "fresh dedupe key for the new run")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact new submission without starting it")
	cmd.Flags().BoolVar(&allowEmptyPool, "allow-empty-pool", false, "submit even when the selected pool has no live Orkers")
	cmd.Flags().StringVar(&originRef, "origin-ref", "", originRefHelp)
	return cmd
}

func againInput(source *orkstackv1.Payload, pairs []string, rawJSON string) (*orkstackv1.Payload, error) {
	values := map[string]any{}
	if source.GetData() != nil {
		for key, value := range source.GetData().AsMap() {
			values[key] = value
		}
	}
	if rawJSON != "" {
		var overrides map[string]any
		if err := json.Unmarshal([]byte(rawJSON), &overrides); err != nil {
			return nil, fmt.Errorf("invalid --input-json: %w", err)
		}
		for key, value := range overrides {
			values[key] = value
		}
	}
	for _, pair := range pairs {
		key, raw, ok := strings.Cut(pair, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid --input %q: expected key=value", pair)
		}
		values[key] = parseInputValue(raw)
	}
	if len(values) == 0 {
		return nil, nil
	}
	data, err := structpb.NewStruct(values)
	if err != nil {
		return nil, fmt.Errorf("build run input: %w", err)
	}
	return &orkstackv1.Payload{Data: data}, nil
}
