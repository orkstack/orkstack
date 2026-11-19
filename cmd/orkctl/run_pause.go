package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/runstatus"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// defaultPauseWaitTimeout is how long --wait waits for the run to apply the
// request, unless --wait-timeout says otherwise.
const defaultPauseWaitTimeout = 5 * time.Minute

const pauseExitHelp = "With --wait the command streams the run's events from the moment the\n" +
	"request was checked and decides by the request's own id. Exit codes:\n" +
	"  0  the run applied the request\n" +
	"  1  the request was refused or failed\n" +
	"  4  the run ended first; the request had no effect\n" +
	"  5  --wait-timeout passed before the run applied it\n" +
	"%s" +
	"Codes 2 and 3 are not used: they mean failed and canceled to run watch.\n" +
	"Without --wait the command exits 0 once the run has recorded the request.\n" +
	"Warnings print to stderr."

func newRunPauseCommand(g *globalFlags) *cobra.Command {
	var (
		reason      string
		timeout     time.Duration
		wait        bool
		dryRun      bool
		waitTimeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "pause <run-id>",
		Short: "Pause a run: running work finishes, nothing new starts",
		Long: "pause stops a running run starting anything new: no task, fan-out item,\n" +
			"child run, pin, await, sleep or failure handler, except what an\n" +
			"already-open pin needs to finish. Work it started finishes under its\n" +
			"own rules, and its running child runs are paused too. Every clock keeps\n" +
			"running: a timeout, an await's deadline or a sleep's wake-up is not\n" +
			"stretched. Unpause it with `orkctl run unpause`.\n\n" +
			"A pause lasts at most --timeout (the deployment's engine.run_pause_timeout,\n" +
			"7 days by default, when unset; at most engine.run_pause_timeout_max): a run\n" +
			"that is not failing then ends FAILED with RUN_PAUSE_TIMEOUT. A pause cannot\n" +
			"be changed: unpause and pause again.\n\n" +
			"The request records who asked, as cancel does: verified under auth.mode\n" +
			"observe or enforce, asserted otherwise.\n\n" +
			"--dry-run makes every check and prints the warnings, and sends nothing.\n\n" +
			fmt.Sprintf(pauseExitHelp, "  6  the run became paused by another request, or by its parent, first\n"),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if dryRun && wait {
				return fmt.Errorf("pause run: --dry-run sends nothing, so there is nothing to --wait for")
			}
			request := &orkstackv1.PauseRunRequest{RunId: args[0], Namespace: g.namespace, Reason: reason, DryRun: dryRun}
			if cmd.Flags().Changed("timeout") {
				request.Timeout = durationpb.New(timeout)
			}
			ctx, cancel := g.unaryContext(cmd)
			resp, err := g.runClient().PauseRun(ctx, connect.NewRequest(request))
			cancel()
			if err != nil {
				return rpcError("pause run", err)
			}
			if dryRun {
				if g.json {
					return writeJSON(cmd.OutOrStdout(), resp.Msg)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Run %s can be paused; nothing was sent\n", args[0])
				for _, warning := range resp.Msg.GetWarnings() {
					fmt.Fprintln(cmd.OutOrStdout(), "warning: "+warning)
				}
				return nil
			}
			for _, warning := range resp.Msg.GetWarnings() {
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: "+warning)
			}
			waiter := &pauseWaiter{runID: args[0], requestID: resp.Msg.GetRequestId(), pause: true}
			return finishPauseRequest(cmd, g, waiter, resp.Msg, resp.Msg.GetAfterSequence(), wait, waitTimeout)
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why the run is paused, recorded on the pause and its event")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "how long the pause lasts unless unpaused, in whole seconds (default: the deployment's engine.run_pause_timeout)")
	cmd.Flags().BoolVar(&wait, "wait", false, "wait until the run applies the request")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "check the request and print its warnings; send nothing")
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", defaultPauseWaitTimeout, "how long --wait waits")
	return cmd
}

func newRunUnpauseCommand(g *globalFlags) *cobra.Command {
	var (
		reason      string
		wait        bool
		waitTimeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "unpause <run-id>",
		Short: "Unpause a run paused with run pause",
		Long: "unpause lifts a run's own pause, and the pauses of its child runs that it\n" +
			"caused; a child paused on its own stays paused. The run then starts what\n" +
			"it held, in the order it would have with no pause. A run paused because\n" +
			"its parent run is paused is refused (RUN_PAUSE_INHERITED): unpause the\n" +
			"parent.\n\n" +
			fmt.Sprintf(pauseExitHelp, "  6  the run is still paused: its parent run's pause holds it too\n"),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			resp, err := g.runClient().UnpauseRun(ctx, connect.NewRequest(
				&orkstackv1.UnpauseRunRequest{RunId: args[0], Namespace: g.namespace, Reason: reason}))
			cancel()
			if err != nil {
				return rpcError("unpause run", err)
			}
			waiter := &pauseWaiter{runID: args[0], requestID: resp.Msg.GetRequestId()}
			return finishPauseRequest(cmd, g, waiter, resp.Msg, resp.Msg.GetAfterSequence(), wait, waitTimeout)
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why the run is unpaused, recorded on the audit record")
	cmd.Flags().BoolVar(&wait, "wait", false, "wait until the run applies the request")
	cmd.Flags().DurationVar(&waitTimeout, "wait-timeout", defaultPauseWaitTimeout, "how long --wait waits")
	return cmd
}

// writePauseWaitJSON prints --json's document with --wait:
// {"response": …, "outcome": …, "event": …|null}, each message in the API's
// JSON dialect.
func writePauseWaitJSON(out io.Writer, response proto.Message, outcome pauseOutcome, event *orkstackv1.Event) error {
	r, err := marshalCompactJSON(response)
	if err != nil {
		return err
	}
	e := []byte("null")
	if event != nil {
		if e, err = marshalCompactJSON(event); err != nil {
			return err
		}
	}
	compact := fmt.Appendf(nil, `{"response":%s,"outcome":%q,"event":%s}`, r, string(outcome), e)
	var indented bytes.Buffer
	if err := stablejson.Indent(&indented, compact, "  "); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	if _, err := out.Write(append(jsonTerminalSafe(indented.Bytes()), '\n')); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// finishPauseRequest prints a recorded request and, with --wait, waits for
// the run to apply it and exits by its outcome.
func finishPauseRequest(
	cmd *cobra.Command, g *globalFlags, w *pauseWaiter, response proto.Message, after int64, wait bool, waitTimeout time.Duration,
) error {
	out := cmd.OutOrStdout()
	if !wait {
		if g.json {
			return writeJSON(out, response)
		}
		verb := "Unpause"
		if w.pause {
			verb = "Pause"
		}
		fmt.Fprintf(out, "%s of run %s recorded (request %s)\n", verb, w.runID, w.requestID)
		return nil
	}
	outcome, run, err := waitForPause(cmd, g, w, after, waitTimeout)
	if err != nil {
		return err
	}
	if g.json {
		if err := writePauseWaitJSON(out, response, outcome, w.event); err != nil {
			return err
		}
	} else {
		children := false
		for _, task := range run.GetTaskRuns() {
			if task.GetChild().GetRunId() != "" && task.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
				children = true
			}
		}
		fmt.Fprintln(out, w.summary(outcome, formatTime(run.GetPause().GetExpiresAt()), children))
		if outcome == pauseApplied {
			printPauseWarnings(cmd.ErrOrStderr(), run)
		}
	}
	if code := outcome.exitCode(); code != 0 {
		return &exitCodeError{code: code, err: errors.New(w.summary(outcome, "", false))}
	}
	return nil
}

// errStreamEndedUndecided is a stream that ended cleanly before the request
// was decided; waitForPause resumes it after a wait.
var errStreamEndedUndecided = errors.New("the event stream ended before the run applied the request")

// waitForPause streams the run's events after after until the request's
// outcome is decided or waitTimeout passes, resuming an interrupted stream
// from the last event read. After a STREAM_GAP, or a stream that ended
// without deciding, the run as GetRun reads it decides.
func waitForPause(cmd *cobra.Command, g *globalFlags, w *pauseWaiter, after int64, waitTimeout time.Duration) (pauseOutcome, *orkstackv1.Run, error) {
	streamCtx, cancelStream := g.streamContext(cmd)
	defer cancelStream()
	ctx, cancel := context.WithTimeout(streamCtx, waitTimeout)
	defer cancel()
	client := g.runClient()
	last, resumes := after, 0
	var outcome pauseOutcome
	decided := false
	for !decided {
		stream, err := client.StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{
			RunId: w.runID, FromSequence: last, Namespace: g.namespace,
		}))
		checkSnapshot, gap := false, false
		if err == nil {
			for !decided && stream.Receive() {
				event := stream.Msg()
				if event.GetSequence() <= last {
					continue
				}
				last, resumes = event.GetSequence(), 0
				if event.GetType() == orkstackv1.EventType_EVENT_TYPE_STREAM_GAP {
					checkSnapshot, gap = true, true
					break
				}
				outcome, decided = w.observe(event)
			}
			err = stream.Err()
			_ = stream.Close()
			if err == nil && !decided {
				checkSnapshot = true
			}
		}
		if decided {
			break
		}
		if ctx.Err() != nil && streamCtx.Err() == nil {
			return pauseTimeout, nil, nil
		}
		if checkSnapshot {
			run, gerr := finalRun(cmd, g, client, w.runID)
			switch {
			case gerr != nil:
				err = gerr
			default:
				if outcome, decided = w.fromSnapshot(run); decided {
					return w.settle(cmd, g, outcome, run)
				}
				if gap {
					// The gap moved past numbers the run will never fill: read on
					// from after it at once.
					continue
				}
				// A stream that ended cleanly with nothing decided (a replica
				// stopping) is resumed after a wait and counted, as run watch
				// does, never asked again back to back.
				err = errStreamEndedUndecided
			}
		}
		if err != nil && err != errStreamEndedUndecided && !engineInterrupted(ctx, err) {
			if ctx.Err() != nil && streamCtx.Err() == nil {
				return pauseTimeout, nil, nil
			}
			return "", nil, rpcError("wait for the run", err)
		}
		if resumes >= watchResumeLimit {
			return "", nil, rpcError("wait for the run", err)
		}
		delay := min(watchResumeDelay<<resumes, watchResumeMaxDelay)
		resumes++
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			if streamCtx.Err() == nil {
				return pauseTimeout, nil, nil
			}
			return "", nil, fmt.Errorf("wait for the run: %w", ctx.Err())
		case <-timer.C:
		}
	}
	run, err := finalRun(cmd, g, client, w.runID)
	if err != nil {
		return "", nil, rpcError("get run", err)
	}
	return w.settle(cmd, g, outcome, run)
}

// settle records what the outcome needs from the run read after it: an
// applied request on a run that has since ended says so.
func (w *pauseWaiter) settle(_ *cobra.Command, _ *globalFlags, outcome pauseOutcome, run *orkstackv1.Run) (pauseOutcome, *orkstackv1.Run, error) {
	if outcome == pauseApplied && runstatus.IsTerminal(run.GetStatus()) {
		w.ended = &orkstackv1.Event{Status: run.GetStatus()}
	}
	if outcome == pauseRunEnded {
		w.endedCode = run.GetError().GetCode()
		if w.event == nil {
			w.event = &orkstackv1.Event{Status: run.GetStatus()}
		}
	}
	return outcome, run, nil
}
