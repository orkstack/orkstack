package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The exit codes of `orkctl run cancel --force` beside exitOK and exitError;
// 2 and 3 are not used, as they mean failed and canceled to run watch.
const (
	exitForceCancelTimeout = 4
	exitForceCancelRefused = 5
	exitCascadeRunning     = 6
)

// forceCancelListed is how many run ids text output lists.
const forceCancelListed = 50

// forceCancelPoll is how often --wait asks.
var forceCancelPoll = 2 * time.Second

// forceCancelAppears is how long a force cancel just started may take to be
// there to read.
const forceCancelAppears = 30 * time.Second

const forceCancelHelp = "\n\n--force ends an open run that an ordinary cancel does not end. The run is\n" +
	"asked to cancel, and is ended only once it has made no progress for --grace,\n" +
	"or has still not ended at the deployment's ceiling. Nothing of a run ended\n" +
	"that way runs afterwards: no failure handler, no output, and its tasks read\n" +
	"as of its last event. It ends CANCELED with RUN_FORCE_CANCELED. Its child\n" +
	"runs are only asked to cancel; --cascade also force-cancels every open run\n" +
	"below it, each with its own grace, and needs the permission at the namespace.\n" +
	"A child run's own cancellation graces do not lengthen --grace: where they are\n" +
	"long, give one. Repeating the command joins, or finishes, what an earlier one\n" +
	"left.\n\n" +
	"Exit codes with --force:\n" +
	"  0  accepted; with --wait, the run has ended, and with --cascade nothing\n" +
	"     below it is open\n" +
	"  1  it failed, or the cascade left something open or unknown\n" +
	"  4  --wait-timeout passed first\n" +
	"  5  refused\n" +
	"  6  the cascade was accepted and still runs (without --wait)"

// forceCancelFlags are `run cancel`'s flags that need --force.
type forceCancelFlags struct {
	force, cascade, dryRun, stop, wait bool
	grace, waitTimeout                 time.Duration
	requestID                          string
}

func (f *forceCancelFlags) register(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&f.force, "force", false, "end the run even if an ordinary cancel does not (needs --reason)")
	cmd.Flags().DurationVar(&f.grace, "grace", 0, "with --force: how long the run may make no progress before it is ended (default: the least allowed)")
	cmd.Flags().BoolVar(&f.cascade, "cascade", false, "with --force: also force-cancel every open run below it")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "with --cascade: list the open runs below it and do nothing")
	cmd.Flags().BoolVar(&f.stop, "stop", false, "with --force: stop the run's newest cascade; those it started go on")
	cmd.Flags().BoolVar(&f.wait, "wait", false, "with --force: wait until it is over")
	cmd.Flags().DurationVar(&f.waitTimeout, "wait-timeout", time.Hour, "with --force: how long --wait waits")
	cmd.Flags().StringVar(&f.requestID, "request-id", "", "with --force: names this request to whoever repeats it, so the same id is the same request")
}

// check refuses flags that say nothing together.
func (f *forceCancelFlags) check(cmd *cobra.Command) error {
	if !f.force {
		for _, name := range []string{"grace", "cascade", "dry-run", "stop", "wait", "wait-timeout", "request-id"} {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("cancel run: --%s needs --force", name)
			}
		}
		return nil
	}
	switch {
	case f.dryRun && !f.cascade:
		return errors.New("cancel run: --dry-run needs --cascade")
	case f.stop && (f.cascade || f.dryRun || f.wait || cmd.Flags().Changed("grace") || f.requestID != ""):
		return errors.New("cancel run: --stop takes no other flag but --reason")
	case f.dryRun && f.wait:
		return errors.New("cancel run: --dry-run does nothing, so there is nothing to --wait for")
	}
	return nil
}

// runForceCancel is `run cancel --force`.
func runForceCancel(cmd *cobra.Command, g *globalFlags, runID, reason string, f *forceCancelFlags) error {
	req := &orkstackv1.ForceCancelRunRequest{RunId: runID, Namespace: g.namespace, Reason: reason, ClientRequestId: f.requestID, Cascade: f.cascade, Preview: f.dryRun, Stop: f.stop}
	if f.dryRun {
		req.Cascade = false // a preview acts on nothing
	}
	if cmd.Flags().Changed("grace") {
		req.Grace = durationpb.New(f.grace)
	}
	ctx, cancel := g.unaryContext(cmd)
	resp, err := g.runClient().ForceCancelRun(ctx, connect.NewRequest(req))
	cancel()
	if err != nil {
		return forceCancelRPCError(err)
	}
	out := resp.Msg
	ended := out.GetState() == orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_ALREADY_ENDED
	if f.cascade && !f.dryRun && !out.GetCascades() && (out.GetAttached() || !ended) {
		// The same --request-id asked for no cascade before.
		return &exitCodeError{code: exitError, err: fmt.Errorf("request %s of run %s does not cascade, so no cascade was started; ask again without --request-id, or under another", out.GetRequestId(), runID)}
	}
	if f.wait && ended && out.GetRequestId() != "" && out.GetCascades() {
		return waitForceCancel(cmd, g, runID, out.GetRequestId(), f) // what its cascade did
	}
	if !f.wait || ended {
		if g.json {
			if err := writeJSON(cmd.OutOrStdout(), out); err != nil {
				return err
			}
		} else if err := renderForceCancel(cmd.OutOrStdout(), g, runID, f, out); err != nil {
			return err
		}
		if out.GetCascades() && !ended {
			return &exitCodeError{code: exitCascadeRunning, err: fmt.Errorf("the cascade is still running; follow it with `%s`",
				orkctlCommand(g.namespace, "run", "cancel", runID, "--force", "--cascade", "--wait", "--reason", "..."))}
		}
		return nil
	}
	if !g.json {
		if err := renderForceCancel(cmd.OutOrStdout(), g, runID, f, out); err != nil {
			return err
		}
	}
	return waitForceCancel(cmd, g, runID, out.GetRequestId(), f)
}

// forceCancelRPCError is a refusal, which a script tells from a failure.
func forceCancelRPCError(err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeInvalidArgument, connect.CodePermissionDenied, connect.CodeUnauthenticated, connect.CodeFailedPrecondition, connect.CodeNotFound, connect.CodeAborted:
		return &exitCodeError{code: exitForceCancelRefused, err: rpcError("force cancel run", err)}
	}
	return rpcError("force cancel run", err)
}

// renderForceCancel prints what became of a request.
func renderForceCancel(w io.Writer, g *globalFlags, runID string, f *forceCancelFlags, out *orkstackv1.ForceCancelRunResponse) error {
	by := func() string {
		if out.GetPrincipal() == "" {
			return ""
		}
		mark := principalCaption
		if out.GetPrincipalVerified() {
			mark = "(verified)"
		}
		return " by " + assertedName(out.GetPrincipal()) + " " + mark
	}
	switch {
	case f.stop:
		fmt.Fprintf(w, "The cascade of run %s is asked to stop (request %s); the force cancels it started go on\n", runID, out.GetRequestId())
		return nil
	case f.dryRun:
		return renderBelow(w, runID, out.GetOpenDescendants())
	case out.GetState() == orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_ALREADY_ENDED:
		fmt.Fprintf(w, "Run %s had already ended\n", runID)
		return nil
	case out.GetAttached():
		fmt.Fprintf(w, "A force cancel of run %s%s is already in progress; its reason and grace apply\n", runID, by())
	case out.GetDirect():
		fmt.Fprintf(w, "Run %s was ended: two force cancels had left it open\n", runID)
	default:
		fmt.Fprintf(w, "Force cancel requested for run %s\n", runID)
	}
	table := newTable(w)
	fmt.Fprintf(table, "Request:\t%s\n", out.GetRequestId())
	if out.GetReason() != "" {
		fmt.Fprintf(table, "Reason:\t%s\n", assertedName(out.GetReason()))
	}
	if out.GetGrace() != nil {
		fmt.Fprintf(table, "Grace:\t%s\n", out.GetGrace().AsDuration())
	}
	if n := out.GetAttachUnverified(); n > 0 {
		fmt.Fprintf(table, "Note:\t%d that claimed to be force cancels of this run were nobody's, and were ended\n", n)
	}
	if out.GetAttachUnknown() {
		fmt.Fprintf(table, "Note:\twhether another is in progress could not be told; this one may run beside it\n")
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// renderBelow prints the open runs below one.
func renderBelow(w io.Writer, runID string, below *orkstackv1.ForceCancelSubtree) error {
	fmt.Fprintf(w, "%d open runs below run %s; nothing was done\n", len(below.GetRuns()), runID)
	table := newTable(w)
	if len(below.GetRuns()) > 0 {
		fmt.Fprintln(table, "RUN\tORKFLOW\tDEPTH\tPARENT")
	}
	for _, r := range below.GetRuns() {
		if r.GetRedacted() {
			fmt.Fprintln(table, "-\ta run of another Orkflow\t-\t-")
			continue
		}
		fmt.Fprintf(table, "%s\t%s\t%d\t%s\n", assertedName(r.GetRunId()), assertedName(r.GetOrkflow()), r.GetDepth(), assertedName(r.GetParentRunId()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if by := below.GetTruncatedBy(); by != "" {
		fmt.Fprintf(w, "The search was cut short (%s); there may be more\n", by)
	}
	leftOut(w, below.GetExclusions())
	return nil
}

// leftOut prints the runs a search below left out, and why.
func leftOut(w io.Writer, exclusions []*orkstackv1.ForceCancelExclusion) {
	for _, e := range exclusions {
		fmt.Fprintf(w, "Left out: %s (%s)\n", orDash(e.GetRunId()), e.GetCause())
	}
}

// waitForceCancel asks until the force cancel, and its cascade, is over.
func waitForceCancel(cmd *cobra.Command, g *globalFlags, runID, requestID string, f *forceCancelFlags) error {
	deadline := time.NewTimer(f.waitTimeout)
	defer deadline.Stop()
	seen := map[string]bool{}
	asked := time.Now()
	var last *orkstackv1.GetForceCancelResponse
	for {
		ctx, cancel := g.unaryContext(cmd)
		resp, err := g.runClient().GetForceCancel(ctx, connect.NewRequest(&orkstackv1.GetForceCancelRequest{RunId: runID, Namespace: g.namespace, RequestId: requestID}))
		cancel()
		switch {
		case err == nil:
			last = resp.Msg
			if !g.json {
				progressForceCancel(cmd.OutOrStdout(), last, seen)
			}
			if forceCancelOver(last) {
				return finishForceCancel(cmd, g, runID, last)
			}
		case cmd.Context().Err() != nil:
			return cmd.Context().Err()
		case connect.CodeOf(err) == connect.CodeNotFound && last == nil && time.Since(asked) < forceCancelAppears, engineInterruptedOrBusy(cmd.Context(), err):
			// Started a moment ago, or an engine restarting: asked again.
		default:
			return rpcError("read force cancel", err)
		}
		select {
		case <-cmd.Context().Done():
			return cmd.Context().Err()
		case <-deadline.C:
			if g.json && last != nil {
				if err := writeJSON(cmd.OutOrStdout(), last); err != nil {
					return err
				}
			}
			return &exitCodeError{code: exitForceCancelTimeout, err: fmt.Errorf("force cancel of run %s is not over after %s; it goes on", runID, f.waitTimeout)}
		case <-time.After(forceCancelPoll):
		}
	}
}

// engineInterruptedOrBusy reports an answer worth asking again for.
func engineInterruptedOrBusy(ctx context.Context, err error) bool {
	code := connect.CodeOf(err)
	return engineInterrupted(ctx, err) || code == connect.CodeUnavailable || code == connect.CodeResourceExhausted || code == connect.CodeDeadlineExceeded
}

// forceCancelOver reports whether nothing more will change in r.
func forceCancelOver(r *orkstackv1.GetForceCancelResponse) bool {
	return r.GetState() != orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_REQUESTED &&
		r.GetCascade().GetState() != orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_RUNNING
}

func forceCancelStateLabel(s orkstackv1.ForceCancelState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "FORCE_CANCEL_STATE_"))
}

func cascadeStateLabel(s orkstackv1.ForceCancelCascadeState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "FORCE_CANCEL_CASCADE_STATE_"))
}

// progressForceCancel prints each run below once it needs nothing more.
func progressForceCancel(w io.Writer, r *orkstackv1.GetForceCancelResponse, seen map[string]bool) {
	for _, b := range r.GetCascade().GetRuns() {
		if b.GetRedacted() || seen[b.GetRunId()] || !forceCancelRowOver(b.GetState()) {
			continue
		}
		seen[b.GetRunId()] = true
		line := fmt.Sprintf("%s  %s  %s", assertedName(b.GetRunId()), assertedName(b.GetOrkflow()), forceCancelStateLabel(b.GetState()))
		if b.GetCause() != "" {
			line += " (" + assertedName(b.GetCause()) + ")"
		}
		fmt.Fprintln(w, line)
	}
}

func forceCancelRowOver(s orkstackv1.ForceCancelState) bool {
	return s != orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_REQUESTED && s != orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_WAITING
}

// finishForceCancel prints the outcome, and is the command's exit: 0 only when
// the run has ended, what that needs is done, and nothing below it is left.
func finishForceCancel(cmd *cobra.Command, g *globalFlags, runID string, r *orkstackv1.GetForceCancelResponse) error {
	if g.json {
		if err := writeJSON(cmd.OutOrStdout(), r); err != nil {
			return err
		}
	} else if err := renderForceCancelOutcome(cmd.OutOrStdout(), g, runID, r); err != nil {
		return err
	}
	switch {
	case r.GetState() == orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_UNSPECIFIED:
		return &exitCodeError{code: exitError, err: fmt.Errorf("what became of the force cancel of run %s is not known to this orkctl", runID)}
	case r.GetState() == orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_FAILED:
		return &exitCodeError{code: exitError, err: fmt.Errorf("force cancel of run %s failed (%s); the run may still be open", runID, orDash(r.GetCause()))}
	case r.GetIncomplete():
		return &exitCodeError{code: exitError, err: fmt.Errorf("force cancel of run %s is incomplete (%s); run the same command again to finish it", runID, orDash(r.GetCause()))}
	case r.GetCascade() != nil && r.GetCascade().GetState() != orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_COMPLETE:
		return &exitCodeError{code: exitError, err: fmt.Errorf("some runs under %s may still be open (the cascade is %s)", runID, cascadeStateLabel(r.GetCascade().GetState()))}
	}
	return nil
}

func renderForceCancelOutcome(w io.Writer, g *globalFlags, runID string, r *orkstackv1.GetForceCancelResponse) error {
	fmt.Fprintf(w, "Run %s: %s\n", runID, forceCancelStateLabel(r.GetState()))
	if cause := r.GetCause(); cause != "" {
		fmt.Fprintf(w, "Cause: %s\n", assertedName(cause))
	}
	if r.GetUnaudited() {
		fmt.Fprintln(w, "Note: a record of it could not be written to the audit trail")
	}
	again := func(id string) {
		fmt.Fprintf(w, "  %s\n", orkctlCommand(g.namespace, "run", "cancel", id, "--force", "--reason", "..."))
	}
	if by := r.GetOpenDescendants().GetTruncatedBy(); by != "" {
		fmt.Fprintf(w, "Not every open run below it could be listed (%s)\n", by)
	}
	leftOut(w, r.GetOpenDescendants().GetExclusions())
	if below := r.GetOpenDescendants(); len(below.GetRuns()) > 0 {
		fmt.Fprintf(w, "%d open runs below it were only asked to cancel:\n", len(below.GetRuns()))
		for i, b := range below.GetRuns() {
			if i == forceCancelListed {
				fmt.Fprintf(w, "  and %d more\n", len(below.GetRuns())-i)
				break
			}
			if b.GetRedacted() {
				fmt.Fprintln(w, "  a run of another Orkflow")
				continue
			}
			again(b.GetRunId())
		}
	}
	c := r.GetCascade()
	if c == nil {
		return nil
	}
	if c.GetState() != orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_COMPLETE {
		fmt.Fprintf(w, "Some runs under %s may still be open.\n", runID)
	}
	if c.GetUnread() {
		fmt.Fprintln(w, "Where its cascade stands could not be asked")
	}
	if cause := c.GetCause(); cause != "" {
		fmt.Fprintf(w, "The runs below could not be looked for (%s)\n", cause)
	}
	for i, id := range c.GetOpenAfter().GetRunIds() {
		if i == forceCancelListed {
			break
		}
		again(id)
	}
	if more := int(c.GetOpenAfter().GetCount()) - min(len(c.GetOpenAfter().GetRunIds()), forceCancelListed); more > 0 {
		fmt.Fprintf(w, "  and %d more: run the same command again with --cascade\n", more)
	}
	if cause := c.GetOpenAfter().GetUnconfirmedCause(); cause != "" {
		fmt.Fprintf(w, "Some runs couldn't be checked (%s); run the same command again with --cascade\n", cause)
	}
	if n := c.GetLeftToParents(); n > 0 {
		fmt.Fprintf(w, "%d deeper runs were not force-canceled; their parents' cancel reaches them, and any that stay open need --cascade again\n", n)
	}
	if n := c.GetNotStarted(); n > 0 {
		fmt.Fprintf(w, "%d runs were not force-canceled, and no parent's cancel reaches them; run the same command again with --cascade\n", n)
	}
	leftOut(w, c.GetExclusions())
	if c.GetRerunSuggested() && c.GetState() != orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_COMPLETE {
		fmt.Fprintln(w, "Some descendants may remain open; run the same command again with --cascade")
	}
	return nil
}

// forceCancelLine says what a force-canceled run's newest force cancel lacks,
// "" when nothing.
func forceCancelLine(r *orkstackv1.GetForceCancelResponse) string {
	switch {
	case r.GetIncomplete():
		return "incomplete; `orkctl run cancel --force` again finishes it"
	case r.GetCascade().GetState() == orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_RUNNING:
		return "its cascade is in progress"
	case r.GetCascade().GetState() == orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_PARTIAL:
		return "its cascade left runs below open, or could not tell"
	case r.GetCascade().GetState() == orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_STOPPED:
		return "its cascade was stopped before its end"
	case r.GetUnaudited():
		return "a record of it could not be written to the audit trail"
	}
	return ""
}

// appendForceCancel prints that line under a force-canceled run. A run archived
// and gone from the cluster has none, and one that cannot be read is said to be,
// and is no reason to fail the rest.
func appendForceCancel(cmd *cobra.Command, g *globalFlags, run *orkstackv1.Run) {
	if run.GetError().GetCode() != "RUN_FORCE_CANCELED" {
		return
	}
	ctx, cancel := g.unaryContext(cmd)
	defer cancel()
	resp, err := g.runClient().GetForceCancel(ctx, connect.NewRequest(&orkstackv1.GetForceCancelRequest{RunId: run.GetId(), Namespace: g.namespace}))
	switch connect.CodeOf(err) {
	case connect.CodeNotFound, connect.CodeFailedPrecondition, connect.CodeUnimplemented:
		return // an engine that has none
	}
	if err != nil {
		fmt.Fprintf(cmd.OutOrStdout(), "\nForce cancel: could not be read (%s)\n", connect.CodeOf(err))
		return
	}
	if line := forceCancelLine(resp.Msg); line != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "\nForce cancel: %s\n", line)
	}
}
