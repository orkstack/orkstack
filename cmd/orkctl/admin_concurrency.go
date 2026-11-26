package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// `admin concurrency reset`: an operator's remedy for a concurrency limit
// whose own record is wrong or does not answer (docs/RUNBOOKS.md).

// The exit codes of admin concurrency reset, beside exitOK and exitError (it
// ended otherwise than completed).
const (
	exitResetTimedOut = 4
	exitResetRefused  = 5
)

// resetPollInterval is how often a reset is asked about until it ends;
// resetCallTimeout bounds each of those requests.
var resetPollInterval = 2 * time.Second

const resetCallTimeout = 30 * time.Second

func (g *globalFlags) concurrencyAdminClient() orkstackv1connect.ConcurrencyAdminServiceClient {
	return orkstackv1connect.NewConcurrencyAdminServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func newAdminConcurrencyCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "concurrency",
		Short: "Repair a concurrency limit's own record",
	}
	cmd.AddCommand(newAdminConcurrencyResetCommand(g))
	return cmd
}

func newAdminConcurrencyResetCommand(g *globalFlags) *cobra.Command {
	var (
		dryRun    bool
		force     bool
		epoch     int64
		requestID string
		issuedAt  string
		timeout   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "reset KEY",
		Short: "Rebuild a limit's record from the runs that hold or wait for it",
		Long: "Rebuild the limit's record from the runs that hold or wait for it, and wait until\n" +
			"it is rebuilt. A limit that answers keeps every run that is still running: no run\n" +
			"is stopped, restarted or moved in line. --dry-run says what a reset would do and\n" +
			"changes nothing.\n\n" +
			"A limit that does not answer is reset only with --force-unanswered, which accepts\n" +
			"that more runs than the limit may hold the key until every run that holds it is\n" +
			"found.\n\n" +
			"A reset is named by its request id and the time it was issued, and acts once. To\n" +
			"ask again about one that timed out, pass the --request-id and --issued-at it\n" +
			"printed.\n\n" +
			"Exit status: 0 completed or a dry run, 4 still running at --timeout, 5 refused,\n" +
			"1 failed, interrupted, never applied, or the command itself failed.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &orkstackv1.ResetConcurrencyLimitRequest{Namespace: g.namespace, Key: args[0], DryRun: dryRun, ForceUnanswered: force}
			if cmd.Flags().Changed("expected-epoch") {
				req.ExpectedEpoch = &epoch
			}
			if !dryRun {
				if err := nameReset(req, requestID, issuedAt, time.Now()); err != nil {
					return err
				}
			}
			return resetConcurrency(cmd.Context(), g, cmd.OutOrStdout(), cmd.ErrOrStderr(), req, timeout)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "say what a reset would do, and change nothing")
	cmd.Flags().BoolVar(&force, "force-unanswered", false, "reset a limit that does not answer")
	cmd.Flags().Int64Var(&epoch, "expected-epoch", 0, "refuse when the limit was rebuilt since this epoch was read")
	cmd.Flags().StringVar(&requestID, "request-id", "", "the request's id, to ask about one already made (with --issued-at)")
	cmd.Flags().StringVar(&issuedAt, "issued-at", "", "when that request was issued, as it printed it (RFC 3339)")
	// A LOCAL --timeout shadows the root's persistent one on this command
	// only: here it bounds the wait.
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute, "how long to wait for the reset to finish")
	return cmd
}

// nameReset gives req the two values that name it: the ones it is asked again
// with, or new ones. One without the other names no request.
func nameReset(req *orkstackv1.ResetConcurrencyLimitRequest, id, issuedAt string, now time.Time) error {
	if (id == "") != (issuedAt == "") {
		return errors.New("--request-id and --issued-at name a request together: pass both, or neither for a new reset")
	}
	at := now.UTC()
	if id == "" {
		id = "rst-" + strings.ToLower(rand.Text())
	} else {
		parsed, err := time.Parse(time.RFC3339Nano, issuedAt)
		if err != nil {
			return fmt.Errorf("--issued-at %q is not an RFC 3339 time: %w", issuedAt, err)
		}
		at = parsed.UTC()
	}
	req.RequestId, req.IssuedAt = id, timestamppb.New(at)
	return nil
}

// resetConcurrency asks for the reset and then, by the same request, how it
// stands, until it ends or timeout passes.
func resetConcurrency(ctx context.Context, g *globalFlags, out, errOut io.Writer, req *orkstackv1.ResetConcurrencyLimitRequest, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	again := fmt.Sprintf("--request-id %s --issued-at %s", req.GetRequestId(), req.GetIssuedAt().AsTime().Format(time.RFC3339Nano))
	shown := false
	var last error
	for {
		cctx, cancel := context.WithTimeout(ctx, resetCallTimeout)
		resp, err := g.concurrencyAdminClient().ResetConcurrencyLimit(cctx, connect.NewRequest(req))
		cancel()
		switch {
		case err == nil:
			last = nil
			msg := resp.Msg
			if !g.json && !shown && msg.GetConsequence() != "" {
				fmt.Fprintln(out, msg.GetConsequence())
				shown = true
			}
			if msg.GetState() != orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_RUNNING {
				return finishReset(g, out, msg)
			}
			if msg.GetNote() != "" {
				last = errors.New(msg.GetNote())
			}
		case ctx.Err() != nil:
			return ctx.Err()
		case connect.CodeOf(err) == connect.CodeFailedPrecondition:
			return &exitCodeError{code: exitResetRefused, err: rpcError("admin concurrency reset", err)}
		case connect.CodeOf(err) != connect.CodeUnavailable && connect.CodeOf(err) != connect.CodeDeadlineExceeded, req.GetDryRun():
			return rpcError("admin concurrency reset", err)
		default:
			last = rpcError("admin concurrency reset", err)
		}
		if !time.Now().Add(resetPollInterval).Before(deadline) {
			fmt.Fprintf(errOut, "to ask about this reset again: orkctl admin concurrency reset %s %s\n", shellArg(req.GetKey()), again)
			if last != nil {
				return &exitCodeError{code: exitResetTimedOut, err: fmt.Errorf("the reset did not finish within %s: %w", timeout, last)}
			}
			return &exitCodeError{code: exitResetTimedOut, err: fmt.Errorf("the reset did not finish within %s", timeout)}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(resetPollInterval):
		}
	}
}

// finishReset renders how a reset ended, as its exit status.
func finishReset(g *globalFlags, out io.Writer, msg *orkstackv1.ResetConcurrencyLimitResponse) error {
	if g.json {
		if err := writeJSON(out, msg); err != nil {
			return err
		}
	}
	switch msg.GetState() {
	case orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_DRY_RUN:
		if !g.json {
			fmt.Fprintln(out, "Dry run: nothing was changed.")
		}
		return nil
	case orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_COMPLETED:
		if !g.json {
			fmt.Fprintln(out, "The limit's record is rebuilt.")
		}
		return nil
	}
	return &exitCodeError{code: exitError, err: fmt.Errorf("%s: %s", orDash(msg.GetReasonCode()), orDash(msg.GetReason()))}
}
