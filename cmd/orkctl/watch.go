package main

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newRunWatchCommand(g *globalFlags) *cobra.Command {
	var fromSequence int64
	cmd := &cobra.Command{
		Use:   "watch <run-id>",
		Short: "Stream a run's events until it reaches a terminal state",
		Long: "watch consumes RunService.StreamEvents and prints each event as it arrives,\n" +
			"exiting when the run reaches a terminal state. The exit code reflects the\n" +
			"final status: 0 succeeded, 2 failed, 3 canceled.\n\n" +
			"The run is addressed inside the namespace this command acts in; a run of\n" +
			"another namespace is not found here.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return watchRun(cmd, g, args[0], fromSequence)
		},
	}
	cmd.Flags().Int64Var(&fromSequence, "from-sequence", 0,
		"resume after this event sequence (0 = from the start of the run)")
	return cmd
}

// watchRun streams a run's events live, then reports the run's final state.
// The stream can end without a terminal event (for example when resuming past
// one), so the final status always comes from GetRun.
func watchRun(cmd *cobra.Command, g *globalFlags, runID string, fromSequence int64) error {
	out := cmd.OutOrStdout()
	client := g.runClient()

	var sink eventSink = &humanEventSink{out: out}
	if g.json {
		sink = &jsonEventSink{out: out}
	}

	streamCtx, cancelStream := g.streamContext(cmd)
	defer cancelStream()

	stream, err := client.StreamEvents(streamCtx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
		RunId:        runID,
		FromSequence: fromSequence,
		Namespace:    g.namespace,
	}))
	if err != nil {
		return rpcError("watch run", err)
	}
	defer func() { _ = stream.Close() }()

	if err := sink.start(); err != nil {
		return err
	}
	// closed guards the JSON document: it must be terminated even when the
	// stream or the final lookup fails, so stdout stays parseable.
	closed := false
	defer func() {
		if !closed {
			_ = sink.finish(nil)
		}
	}()

	for stream.Receive() {
		if err := sink.event(stream.Msg()); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		return rpcError("watch run", err)
	}
	cancelStream()

	runCtx, cancelRun := g.unaryContext(cmd)
	defer cancelRun()
	resp, err := client.GetRun(runCtx, connect.NewRequest(
		&orkstackv1.GetRunRequest{RunId: runID, Namespace: g.namespace}))
	if err != nil {
		return rpcError("get run", err)
	}

	run := resp.Msg.GetRun()
	closed = true
	if err := sink.finish(run); err != nil {
		return err
	}
	return runExitError(run)
}

// runExitError maps a terminal run status onto the process exit code.
func runExitError(run *orkstackv1.Run) error {
	switch run.GetStatus() {
	case orkstackv1.Status_STATUS_FAILED:
		return &exitCodeError{
			code: exitRunFailed,
			err:  fmt.Errorf("run %s failed: %s", run.GetId(), errorLine(run.GetError())),
		}
	case orkstackv1.Status_STATUS_CANCELED:
		return &exitCodeError{
			code: exitRunCanceled,
			err:  fmt.Errorf("run %s was canceled", run.GetId()),
		}
	default:
		return nil
	}
}

// eventSink renders a watched run incrementally: events are written as they
// arrive, and finish reports the run's final state.
type eventSink interface {
	start() error
	event(*orkstackv1.Event) error
	finish(*orkstackv1.Run) error
}

// humanEventSink writes one aligned line per event as it arrives.
type humanEventSink struct {
	out io.Writer
}

func (s *humanEventSink) start() error {
	_, err := fmt.Fprintf(s.out, "%4s  %-20s  %-16s  %-12s  %s\n",
		"SEQ", "TIMESTAMP", "EVENT", "TASK", "MESSAGE")
	if err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func (s *humanEventSink) event(event *orkstackv1.Event) error {
	_, err := fmt.Fprintf(s.out, "%4d  %-20s  %-16s  %-12s  %s\n",
		event.GetSequence(), formatTime(event.GetTimestamp()), eventLabel(event.GetType()),
		orDash(event.GetTaskId()), orDash(event.GetMessage()))
	if err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func (s *humanEventSink) finish(run *orkstackv1.Run) error {
	if run == nil {
		return nil
	}
	if _, err := fmt.Fprintf(s.out, "\nRun %s finished with status %s\n",
		run.GetId(), statusLabel(run.GetStatus())); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if run.GetError() != nil {
		if _, err := fmt.Fprintf(s.out, "Error: %s\n", errorLine(run.GetError())); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	return nil
}

// jsonEventSink streams a single JSON document: events land as they arrive and
// the final run is appended, so the whole of stdout parses as one object.
type jsonEventSink struct {
	out   io.Writer
	count int
}

func (s *jsonEventSink) start() error {
	return s.write("{\n  \"events\": [")
}

func (s *jsonEventSink) event(event *orkstackv1.Event) error {
	encoded, err := marshalJSON(event)
	if err != nil {
		return err
	}
	separator := "\n    "
	if s.count > 0 {
		separator = ",\n    "
	}
	s.count++
	return s.write(separator + indentJSONBlock(string(encoded), "    "))
}

func (s *jsonEventSink) finish(run *orkstackv1.Run) error {
	closing := "\n  ]"
	if s.count == 0 {
		closing = "]"
	}
	if run == nil {
		return s.write(closing + ",\n  \"run\": null\n}\n")
	}
	encoded, err := marshalJSON(run)
	if err != nil {
		return err
	}
	return s.write(closing + ",\n  \"run\": " + indentJSONBlock(string(encoded), "  ") + "\n}\n")
}

func (s *jsonEventSink) write(text string) error {
	if _, err := io.WriteString(s.out, text); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
