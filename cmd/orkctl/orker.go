package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// exitDrainTimedOut is `orkctl orker drain`'s answer when the Orker still
// holds work at --timeout: distinct from exitError so a lifecycle hook can
// tell "still running, decide" from "the command failed".
const exitDrainTimedOut = 4

// drainPollInterval is how often drain asks the one Orker how much work it
// still holds; drainPollTimeout bounds each of those requests (the root's
// --timeout is shadowed on drain, where it means the drain bound); and
// drainUnreachableGrace is how long drain keeps asking an Orker that stopped
// answering before it says so and exits.
const (
	drainPollInterval     = 5 * time.Second
	drainPollTimeout      = 15 * time.Second
	drainUnreachableGrace = 30 * time.Second
)

// defaultDrainReason is the cordon reason drain gives when none is passed and
// the Orker is not already cordoned.
const defaultDrainReason = "orkctl orker drain"

// drainClock is drain's clock, injectable so a test runs the loop in
// milliseconds with stable timestamps.
type drainClockSource interface {
	Now() time.Time
	// Sleep waits d or until ctx is done.
	Sleep(ctx context.Context, d time.Duration) error
}

type realDrainClock struct{}

func (realDrainClock) Now() time.Time { return time.Now() }

func (realDrainClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// drainClock and drainInterval are package variables only so a test can
// replace them; production never does.
var (
	drainClock    drainClockSource = realDrainClock{}
	drainInterval                  = drainPollInterval
)

func newOrkerCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "orker",
		Short: "List the deployment's Orkers, cordon one, or wait for one to drain",
		Long: "An Orker is one execution process. This command lists the deployment's\n" +
			"Orkers, cordons one so it takes no new task and opens no new pin, and waits\n" +
			"for one to drain. Orkers are deployment infrastructure, not namespace\n" +
			"objects: these commands ignore --namespace.\n\n" +
			"An Orker also cordons itself while object storage does not answer it, and\n" +
			"resumes by itself; the list says which cause holds.\n\n" +
			"A cordon is a flag in the Orker process and nowhere else: it touches nothing\n" +
			"already running, and a restarted Orker comes back ready.",
	}
	cmd.AddCommand(
		newOrkerListCommand(g),
		newOrkerGetCommand(g),
		newOrkerCordonCommand(g),
		newOrkerUncordonCommand(g),
		newOrkerDrainCommand(g),
	)
	return cmd
}

func newOrkerListCommand(g *globalFlags) *cobra.Command {
	var filterTexts *[]string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List every live Orker with its pool, load and cordon state",
		Long: "List every live Orker of the deployment, narrowed by --filter\n" +
			"(--filter pool=build keeps the Orkers that report serving build).\n\n" +
			"The list comes from the cluster (who is polling) and from each Orker's own\n" +
			"answer, so any replica answers the same. An Orker that could not be asked is\n" +
			"listed as `unknown (unreachable)` with the reason under the table: its load\n" +
			"is unknown, never zero, and a filter on pool, build_id or cordoned keeps it.\n" +
			"An Orker that stopped reads this way for up to 90 seconds, because the cluster\n" +
			"still remembers it polling.\n" +
			"STATE names every cause that holds a cordon, an operator's first:\n" +
			"cordoned (operator; profile drifted). PROFILES is each profile the Orker\n" +
			"offers with its kind and the cause on it, a100 (gpu; drifted), and each\n" +
			"profile cause is a line under the table with what was found and how it ends.\n" +
			"PINS is open/max for a pin host and - otherwise. CAPACITY is what an Orker\n" +
			"declaring one admits tasks within, and RESERVED what stands reserved against\n" +
			"it, pinned members included and each outstanding poll counted at the per-task\n" +
			"cap, so an idle Orker shows its pollers' share; both are - for an Orker that\n" +
			"declares none.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.activityClient().ListOrkers(ctx, connect.NewRequest(&orkstackv1.ListOrkersRequest{Filters: filters}))
			if err != nil {
				return rpcError("list orkers", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if len(filters) > 0 && len(resp.Msg.GetOrkers()) == 0 {
				fmt.Fprint(cmd.OutOrStdout(), noMatchSentence("Orkers", filters))
				return nil
			}
			if err := renderOrkerList(cmd.OutOrStdout(), resp.Msg.GetOrkers()); err != nil {
				return err
			}
			if len(resp.Msg.GetOrkers()) > 0 {
				renderReadAt(cmd.OutOrStdout(), resp.Msg.GetReadAt())
			}
			return nil
		},
	}
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListOrkersRequest{})
	return cmd
}

// oneOrker is the ListOrkers request that asks exactly one Orker: an
// orker_id equals filter, which the engine answers from that Orker's own
// channel alone.
func oneOrker(id string) *orkstackv1.ListOrkersRequest {
	return &orkstackv1.ListOrkersRequest{Filters: []*orkstackv1.FieldFilter{equalsFilter("orker_id", id)}}
}

// equalsFilter is field = value.
func equalsFilter(field, value string) *orkstackv1.FieldFilter {
	return &orkstackv1.FieldFilter{Field: field, Op: orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{value}}
}

func newOrkerGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get <orker-id>",
		Short: "Show one Orker: its state, profiles, load and open pins",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.activityClient().ListOrkers(ctx, connect.NewRequest(oneOrker(args[0])))
			if err != nil {
				return rpcError("get orker", err)
			}
			if g.json {
				if err := writeJSON(cmd.OutOrStdout(), resp.Msg); err != nil {
					return err
				}
			}
			if len(resp.Msg.GetOrkers()) == 0 {
				return errNotPolling(args[0])
			}
			if g.json {
				return nil
			}
			return renderOrker(cmd.OutOrStdout(), resp.Msg.GetOrkers()[0])
		},
	}
}

func errNotPolling(id string) error {
	return fmt.Errorf("no Orker %q is polling; orkctl orker list shows who is", id)
}

func newOrkerCordonCommand(g *globalFlags) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "cordon <orker-id> --reason TEXT",
		Short: "Stop one Orker taking new tasks and opening new pins",
		Long: "Cordon one Orker: it takes no new task and opens no new pin, and everything\n" +
			"already running on it — containers and open pins — finishes where it is.\n" +
			"The reason is required; it is what every reader of the fleet sees beside\n" +
			"the state. The cordon is lost if the Orker restarts.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Refused before any request leaves the process.
			if strings.TrimSpace(reason) == "" {
				return fmt.Errorf("--reason must not be blank: it is what every reader of the fleet sees beside the state")
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.activityClient().CordonOrker(ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{
				OrkerId: args[0], Cordoned: true, Reason: reason}))
			if err != nil {
				return rpcError("cordon orker", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			row := safeOrker(resp.Msg.GetOrker())
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "cordoned %s: %s\n", row.GetOrkerId(), strconv.Quote(row.GetCordonReason()))
			fmt.Fprintf(out, "it is accepting no new tasks and will open no new pins; %s %s still running on it.\n",
				loadPhraseAnd(row.GetRunningTasks(), row.GetOpenPins()), isAre(row.GetRunningTasks()+row.GetOpenPins()))
			return nil
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why this Orker is cordoned (required)")
	_ = cmd.MarkFlagRequired("reason")
	return cmd
}

func newOrkerUncordonCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "uncordon <orker-id>",
		Short: "Clear an operator's cordon so the Orker can take work again",
		Long: "Clear the operator's cordon on one Orker. An Orker that has cordoned itself\n" +
			"because object storage does not answer stays cordoned until the store answers\n" +
			"again, then resumes by itself; this command cannot override that.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.activityClient().CordonOrker(ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{
				OrkerId: args[0], Cordoned: false}))
			if err != nil {
				return rpcError("uncordon orker", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			row := safeOrker(resp.Msg.GetOrker())
			if row.GetStopping() != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "cleared the operator's cordon on %s; it stays cordoned because it is stopping, and takes no new work before it exits.\n",
					row.GetOrkerId())
				return nil
			}
			if causes := profileCauses(row); len(causes) > 0 {
				also := ""
				if row.GetStoreOutage() != nil {
					also = "; object storage does not answer it either, which ends by itself once it does"
				}
				fmt.Fprintf(cmd.OutOrStdout(), "cleared the operator's cordon on %s; it stays cordoned while %s%s.\n",
					row.GetOrkerId(), profileHoldsText(causes[0]), also)
				return nil
			}
			if row.GetStoreOutage() != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "cleared the operator's cordon on %s; it stays cordoned while %s, and resumes by itself once object storage answers.\n",
					row.GetOrkerId(), row.GetStoreOutage().GetReason())
				return nil
			}
			fmt.Fprintf(cmd.OutOrStdout(), "uncordoned %s; it is accepting work again.\n", row.GetOrkerId())
			return nil
		},
	}
}

func newOrkerDrainCommand(g *globalFlags) *cobra.Command {
	var (
		reason  string
		timeout time.Duration
		watch   bool
	)
	cmd := &cobra.Command{
		Use:   "drain <orker-id> [--reason TEXT] [--timeout 30m] [--watch]",
		Short: "Cordon one Orker and wait until it runs no task and hosts no pin",
		Long: "Cordon one Orker, then ask it every few seconds how much work it still holds,\n" +
			"until it reports no running task and no open pin.\n\n" +
			"Exit 0 means the Orker measured zero: it is safe to stop the process.\n" +
			"Exit 4 means --timeout passed first; what is still running is printed, and\n" +
			"stopping the process then fails any open pin's run with PIN_HOST_LOST.\n" +
			"An Orker cordoned only because object storage does not answer gets an\n" +
			"operator's cordon too, so it does not resume in the middle of the drain.\n\n" +
			"Exit 1 means the Orker stopped answering, or was uncordoned or restarted\n" +
			"(a restart clears the cordon, so it is taking work again).\n\n" +
			"Nothing server-side holds drain state: this is a loop in orkctl.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := &drainer{
				g: g, cmd: cmd, id: args[0], reason: reason,
				reasonGiven: cmd.Flags().Changed("reason"), timeout: timeout, watch: watch,
				out: cmd.OutOrStdout(),
			}
			return d.run()
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "cordon reason; default keeps an existing one, else \""+defaultDrainReason+"\"")
	// A LOCAL --timeout shadows the root's persistent one on this command only:
	// here it is the drain bound, and each poll is bounded by drainPollTimeout.
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute,
		"how long to wait for the Orker to finish its tasks and pins (on this command, not the per-request timeout)")
	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "print a line each time the running tasks or open pins change")
	return cmd
}

// drainer is one drain: cordon, then poll one Orker until it measures zero.
type drainer struct {
	g           *globalFlags
	cmd         *cobra.Command
	id          string
	reason      string
	reasonGiven bool
	timeout     time.Duration
	watch       bool
	out         io.Writer

	last *orkstackv1.ListOrkersResponse
}

// poll asks the one Orker, bounded by the constant poll timeout.
func (d *drainer) poll() (*orkstackv1.ListOrkersResponse, error) {
	ctx, cancel := context.WithTimeout(d.cmd.Context(), drainPollTimeout)
	defer cancel()
	resp, err := d.g.activityClient().ListOrkers(ctx, connect.NewRequest(oneOrker(d.id)))
	if err != nil {
		return nil, err
	}
	d.last = resp.Msg
	return resp.Msg, nil
}

// finish ends the drain with code and err, printing the final response once
// under --json whichever way the loop ended.
func (d *drainer) finish(code int, err error) error {
	if d.g.json {
		last := d.last
		if last == nil {
			last = &orkstackv1.ListOrkersResponse{}
		}
		if werr := writeJSON(d.out, last); werr != nil {
			return werr
		}
	}
	if err == nil {
		return nil
	}
	return &exitCodeError{code: code, err: err}
}

func (d *drainer) say(format string, args ...any) {
	if !d.g.json {
		fmt.Fprintf(d.out, format, args...)
	}
}

func (d *drainer) run() error {
	start := drainClock.Now()
	deadline := start.Add(d.timeout)

	first, err := d.poll()
	if err != nil {
		return d.finish(exitError, rpcError("drain orker", err))
	}
	if len(first.GetOrkers()) == 0 {
		return d.finish(exitError, errNotPolling(d.id))
	}
	row := safeOrker(first.GetOrkers()[0])
	if operatorCordoned(row) && !d.reasonGiven {
		d.say("cordoned %s: %s\n", d.id, strconv.Quote(row.GetCordonReason()))
	} else {
		reason := d.reason
		if !d.reasonGiven {
			reason = defaultDrainReason
		}
		ctx, cancel := context.WithTimeout(d.cmd.Context(), drainPollTimeout)
		resp, err := d.g.activityClient().CordonOrker(ctx, connect.NewRequest(&orkstackv1.CordonOrkerRequest{
			OrkerId: d.id, Cordoned: true, Reason: reason}))
		cancel()
		if err != nil {
			return d.finish(exitError, rpcError("cordon orker", err))
		}
		row = safeOrker(resp.Msg.GetOrker())
		d.last = &orkstackv1.ListOrkersResponse{Orkers: []*orkstackv1.Orker{row}}
		d.say("cordoned %s: %s\n", d.id, strconv.Quote(row.GetCordonReason()))
	}
	if row.GetUnavailableReason() == "" && (row.GetRunningTasks() > 0 || row.GetOpenPins() > 0) {
		d.say("waiting for %s to finish…\n", loadPhraseAnd(row.GetRunningTasks(), row.GetOpenPins()))
	}

	printed := false
	var lastTasks, lastPins int32
	var unreachableSince time.Time
	reportedUnreachable := false
	for {
		now := drainClock.Now()
		if !now.Before(deadline) {
			return d.timedOut(now)
		}
		resp, err := d.poll()
		now = drainClock.Now()
		var cur *orkstackv1.Orker
		if err == nil && len(resp.GetOrkers()) > 0 {
			cur = safeOrker(resp.GetOrkers()[0])
		}
		switch {
		case err != nil || cur == nil || cur.GetUnavailableReason() != "":
			// Not measured is never zero: keep asking, and give up only after
			// the grace, without ever saying "drained".
			if unreachableSince.IsZero() {
				unreachableSince = now
			}
			if !reportedUnreachable {
				why := "is not polling"
				switch {
				case err != nil:
					why = "could not be asked: " + rpcError("list orkers", err).Error()
				case cur != nil:
					why = "could not be asked: " + cur.GetUnavailableReason()
				}
				d.say("  %s  %s %s\n", now.UTC().Format("15:04:05"), d.id, why)
				reportedUnreachable = true
			}
			if now.Sub(unreachableSince) >= drainUnreachableGrace {
				return d.finish(exitError, fmt.Errorf("%s stopped answering; if the process is gone its pins are "+
					"already lost (PIN_HOST_LOST), if it restarted it is uncordoned and accepting work", d.id))
			}
		case !operatorCordoned(cur) && cur.GetCordoned() && len(cur.GetProfileCordons()) > 0:
			// A profile's cause holds it now, not the drain's cordon, so the drain
			// cannot call it safe to stop: an unavailable profile ends by itself.
			return d.finish(exitError, fmt.Errorf("%s was uncordoned by an operator; it stays cordoned while %s",
				d.id, profileHoldsText(profileCauses(cur)[0])))
		case !operatorCordoned(cur) && cur.GetCordoned():
			// A store outage alone holds it now, and that ends by itself, so this
			// Orker would take work again with the drain reporting it empty.
			return d.finish(exitError, fmt.Errorf("%s was uncordoned by an operator; it is cordoned only while object "+
				"storage does not answer, and takes work again once the store answers", d.id))
		case !cur.GetCordoned():
			unreachableSince, reportedUnreachable = time.Time{}, false
			return d.finish(exitError, fmt.Errorf("%s is no longer cordoned: it restarted or was uncordoned, "+
				"and is accepting work again", d.id))
		default:
			unreachableSince, reportedUnreachable = time.Time{}, false
			tasks, pins := cur.GetRunningTasks(), cur.GetOpenPins()
			if d.watch && (!printed || tasks != lastTasks || pins != lastPins) {
				d.progress(now, cur)
				printed, lastTasks, lastPins = true, tasks, pins
			}
			if tasks == 0 && pins == 0 {
				d.say("%s is drained; it is safe to stop this process.\n", d.id)
				return d.finish(exitOK, nil)
			}
		}
		if err := drainClock.Sleep(d.cmd.Context(), drainInterval); err != nil {
			return d.finish(exitError, err)
		}
	}
}

// progress prints one watch line: the counts, then one line per open pin.
func (d *drainer) progress(now time.Time, row *orkstackv1.Orker) {
	head := fmt.Sprintf("  %s  %s", now.UTC().Format("15:04:05"), countsPhrase(row.GetRunningTasks(), row.GetOpenPins()))
	pins := row.GetPins()
	if len(pins) == 0 {
		d.say("%s\n", head)
		return
	}
	d.say("%-30s %s\n", head, pinLine(pins[0], now))
	for _, pin := range pins[1:] {
		d.say("%-30s %s\n", "", pinLine(pin, now))
	}
}

func (d *drainer) timedOut(now time.Time) error {
	row := &orkstackv1.Orker{}
	if d.last != nil && len(d.last.GetOrkers()) > 0 {
		row = d.last.GetOrkers()[0]
	}
	d.say("%s is NOT drained after %s: %s still running\n", d.id, d.timeout, countsPhrase(row.GetRunningTasks(), row.GetOpenPins()))
	for _, pin := range row.GetPins() {
		d.say("  %s\n", pinLine(pin, now))
	}
	switch {
	case row.GetOpenPins() == 1:
		d.say("stopping the process now fails that run with PIN_HOST_LOST.\n")
	case row.GetOpenPins() > 1:
		d.say("stopping the process now fails those runs with PIN_HOST_LOST.\n")
	case row.GetRunningTasks() > 0:
		d.say("stopping the process now interrupts %s; each is retried elsewhere by its own retry policy.\n",
			orkerPlural(int(row.GetRunningTasks()), "task", "tasks"))
	}
	return d.finish(exitDrainTimedOut, fmt.Errorf("%s did not drain within %s", d.id, d.timeout))
}

// pinLine renders one open pin: `pin "build" of run ork-…  expires in 2h11m`.
func pinLine(pin *orkstackv1.OpenPin, now time.Time) string {
	name := dash
	if pin.GetPin() != "" {
		name = strconv.Quote(pin.GetPin())
	}
	return fmt.Sprintf("pin %s of run %s  expires in %s", name, pin.GetRunId(), expiresIn(pin.GetExpiresAt(), now))
}

// expiresIn renders the time to a lease as 2h11m, or - when unset.
func expiresIn(at *timestamppb.Timestamp, now time.Time) string {
	if at == nil || !at.IsValid() {
		return dash
	}
	left := at.AsTime().Sub(now)
	if left < 0 {
		left = 0
	}
	minutes := int(left / time.Minute)
	return fmt.Sprintf("%dh%02dm", minutes/60, minutes%60)
}

func orkerPlural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// countsPhrase is "2 tasks, 1 pin".
func countsPhrase(tasks, pins int32) string {
	return orkerPlural(int(tasks), "task", "tasks") + ", " + orkerPlural(int(pins), "pin", "pins")
}

// loadPhraseAnd is "2 tasks and 1 pin".
func loadPhraseAnd(tasks, pins int32) string {
	return orkerPlural(int(tasks), "task", "tasks") + " and " + orkerPlural(int(pins), "pin", "pins")
}

func isAre(n int32) string {
	if n == 1 {
		return "is"
	}
	return "are"
}

// operatorCordoned says an operator's cordon holds: it always carries a reason.
// An Orker of a release with no store check, no stop grace and no profiles
// only ever reports that cause, so a row with none reads exactly as it always
// has.
func operatorCordoned(row *orkstackv1.Orker) bool {
	if !row.GetCordoned() {
		return false
	}
	return row.GetCordonReason() != "" ||
		row.GetStoreOutage() == nil && row.GetStopping() == nil && len(row.GetProfileCordons()) == 0
}

// orkerState is the STATE column. A stopping Orker never takes work again, so
// it wins; otherwise a cordon names every cause that holds it, in the order
// Admin → Pools ranks them: an operator's, a profile's, a store outage.
func orkerState(row *orkstackv1.Orker) string {
	switch {
	case row.GetUnavailableReason() != "":
		return "unknown (unreachable)"
	case row.GetStopping() != nil && operatorCordoned(row):
		return "stopping (cordoned by an operator)"
	case row.GetStopping() != nil:
		return "stopping"
	case !row.GetCordoned() && row.GetStoreOutage() == nil && len(row.GetProfileCordons()) == 0:
		// A cause is read from its own field whatever cordoned says, as the
		// UI reads it, so a row from a skewed or newer release still names it.
		return "ready"
	}
	// A drifted or unreadable profile ranks before a store outage, an
	// unavailable one after it, as orkerState in ui/src/lib/orkers.ts does.
	var causes []string
	profile := profileStateLabel(row)
	if profile != "" && profile != "profile unavailable" {
		causes = append(causes, profile)
	}
	if row.GetStoreOutage() != nil {
		causes = append(causes, "object storage unreachable")
	}
	if profile == "profile unavailable" {
		causes = append(causes, profile)
	}
	if len(causes) == 0 {
		return "cordoned"
	}
	if operatorCordoned(row) {
		causes = append([]string{"operator"}, causes...)
	}
	return "cordoned (" + strings.Join(causes, "; ") + ")"
}

// orkerPins is PINS: open/max for a pin host, - otherwise or when unmeasured.
func orkerPins(row *orkstackv1.Orker) string {
	if row.GetUnavailableReason() != "" || row.GetMaxConcurrentPins() == 0 {
		return dash
	}
	return fmt.Sprintf("%d/%d", row.GetOpenPins(), row.GetMaxConcurrentPins())
}

// formatResources renders a cpu and memory amount in the spelling an operator
// configures one in ("16 cpu, 64Gi"), with a dimension at 0 left out and -
// for none at all.
func formatResources(q *orkstackv1.ResourceQuantity) string {
	if q == nil {
		return dash
	}
	var parts []string
	if q.GetCpu() != 0 {
		parts = append(parts, strconv.FormatFloat(q.GetCpu(), 'f', -1, 64)+" cpu")
	}
	if bytes := q.GetMemoryBytes(); bytes != 0 {
		parts = append(parts, formatMemory(bytes))
	}
	if len(parts) == 0 {
		return "0"
	}
	return strings.Join(parts, ", ")
}

// formatMemory is a byte count in the Kubernetes binary spelling when it is a
// whole number of one, 0 for none, and in bytes otherwise.
func formatMemory(bytes int64) string {
	if bytes == 0 {
		return "0"
	}
	for _, unit := range []struct {
		suffix string
		size   int64
	}{{"Ti", 1 << 40}, {"Gi", 1 << 30}, {"Mi", 1 << 20}, {"Ki", 1 << 10}} {
		if bytes%unit.size == 0 {
			return strconv.FormatInt(bytes/unit.size, 10) + unit.suffix
		}
	}
	return strconv.FormatInt(bytes, 10)
}

func orkerTasks(row *orkstackv1.Orker) string {
	if row.GetUnavailableReason() != "" {
		return dash
	}
	return strconv.Itoa(int(row.GetRunningTasks()))
}

// renderReadAt is the list's "as of" line: when the engine finished reading
// the fleet, on its own clock; nothing when the response carries no time.
func renderReadAt(out io.Writer, readAt *timestamppb.Timestamp) {
	if readAt == nil || !readAt.IsValid() {
		return
	}
	fmt.Fprintf(out, "\nAs of %s, the engine's clock.\n", formatTime(readAt))
}

func orkerCount(n int) string { return orkerPlural(n, "orker", "orkers") }

// renderOrkerList prints the fleet as a table, then the cordoned and the
// unreachable Orkers with their reasons, one line each.
func renderOrkerList(out io.Writer, rows []*orkstackv1.Orker) error {
	rows = safeOrkers(rows)
	if len(rows) == 0 {
		fmt.Fprintln(out, "No orkers are polling.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "ORKER\tPOOL\tBUILD\tSTATE\tTASKS\tPINS\tPROFILES\tCAPACITY\tRESERVED\tLAST SEEN")
	var cordoned, outages, profiled, stopping, unreachable, exposed []*orkstackv1.Orker
	for _, row := range rows {
		if row.GetUnavailableReason() != "" {
			unreachable = append(unreachable, row)
		} else {
			if operatorCordoned(row) {
				cordoned = append(cordoned, row)
			}
			if row.GetStoreOutage() != nil {
				outages = append(outages, row)
			}
			if len(row.GetProfileCordons()) > 0 {
				profiled = append(profiled, row)
			}
			if row.GetStopping() != nil {
				stopping = append(stopping, row)
			}
			if len(metadataConcerns(row)) > 0 {
				exposed = append(exposed, row)
			}
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.GetOrkerId(), orDash(row.GetPool()), orDash(row.GetBuildId()), orkerState(row),
			orkerTasks(row), orkerPins(row), orkerProfiles(row), formatResources(row.GetCapacity()), formatResources(row.GetReserved()),
			formatTime(row.GetLastSeen()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if len(cordoned) > 0 {
		fmt.Fprintf(out, "\n%s %s cordoned and %s accepting no new work:\n",
			orkerCount(len(cordoned)), isAre(int32(len(cordoned))), isAre(int32(len(cordoned))))
		for _, row := range cordoned {
			fmt.Fprintf(out, "  %s: %s, since %s\n", row.GetOrkerId(), strconv.Quote(row.GetCordonReason()),
				formatTime(row.GetCordonedAt()))
		}
	}
	if len(outages) > 0 {
		fmt.Fprintf(out, "\n%s cordoned %s because object storage does not answer; tasks wait, and %s by %s once it does:\n",
			orkerCount(len(outages)), itselfThemselves(len(outages)), resumeVerb(len(outages)), itselfThemselves(len(outages)))
		for _, row := range outages {
			fmt.Fprintf(out, "  %s: %s, since %s\n", row.GetOrkerId(), row.GetStoreOutage().GetReason(),
				formatTime(row.GetStoreOutage().GetSince()))
		}
	}
	if len(profiled) > 0 {
		fmt.Fprintf(out, "\n%s cordoned by a profile; what holds %s, and how it ends:\n",
			orkerCount(len(profiled)), pick(len(profiled), "it", "each"))
		for _, row := range profiled {
			for _, cause := range profileCauses(row) {
				fmt.Fprintf(out, "  %s: %s\n", row.GetOrkerId(), profileCauseNote(cause))
			}
		}
	}
	if len(stopping) > 0 {
		fmt.Fprintf(out, "\n%s stopping: %s no new work and %s what %s already runs, then %s:\n",
			orkerCount(len(stopping)), takeTakes(len(stopping)), finishFinishes(len(stopping)),
			itThey(len(stopping)), exitExits(len(stopping)))
		for _, row := range stopping {
			fmt.Fprintf(out, "  %s: %s%s\n", row.GetOrkerId(), orkerTasks(row), stoppingTimes(row.GetStopping()))
		}
	}
	if len(exposed) > 0 {
		fmt.Fprintf(out, "\n%s found at boot that a task could reach the host's instance metadata service, or could not tell:\n",
			orkerCount(len(exposed)))
		for _, row := range exposed {
			fmt.Fprintf(out, "  %s: %s\n", row.GetOrkerId(), strings.Join(metadataConcerns(row), "; "))
		}
	}
	if len(unreachable) > 0 {
		fmt.Fprintf(out, "\n%s could not be asked, so %s load is unknown rather than zero:\n",
			orkerCount(len(unreachable)), itsTheir(len(unreachable)))
		for _, row := range unreachable {
			fmt.Fprintf(out, "  %s: %s\n", row.GetOrkerId(), row.GetUnavailableReason())
		}
		fmt.Fprintln(out, stoppedOrkerNote)
	}
	return nil
}

// stoppedOrkerNote says the one reading of an unreachable row a reader cannot
// work out from the table: the cluster remembers a stopped Orker's polls, so
// for the freshness window (engine.DefaultPollerFreshness) it is a row here and
// a count in `orkctl pool list`. An Orker that comes back under another id, as
// one on a new host does by default, is therefore two Orkers until then.
const stoppedOrkerNote = "A stopped Orker stays listed, and counted by `orkctl pool list`, for up to 90 seconds."

func itselfThemselves(n int) string {
	if n == 1 {
		return "itself"
	}
	return "themselves"
}

func resumeVerb(n int) string {
	if n == 1 {
		return "it resumes"
	}
	return "they resume"
}

func itsTheir(n int) string {
	if n == 1 {
		return "its"
	}
	return "their"
}

// renderOrker prints one Orker as key/value lines, then its open pins.
func renderOrker(out io.Writer, row *orkstackv1.Orker) error {
	row = safeOrker(row)
	table := newTable(out)
	reason, since := dash, dash
	if operatorCordoned(row) {
		reason, since = strconv.Quote(row.GetCordonReason()), formatTime(row.GetCordonedAt())
	}
	fmt.Fprintf(table, "orker\t%s\n", row.GetOrkerId())
	fmt.Fprintf(table, "pool\t%s\n", orDash(row.GetPool()))
	fmt.Fprintf(table, "build\t%s\n", orDash(row.GetBuildId()))
	fmt.Fprintf(table, "state\t%s\n", orkerState(row))
	if row.GetUnavailableReason() != "" {
		fmt.Fprintf(table, "unavailable\t%s\n", row.GetUnavailableReason())
	}
	fmt.Fprintf(table, "reason\t%s\n", reason)
	fmt.Fprintf(table, "since\t%s\n", since)
	if outage := row.GetStoreOutage(); outage != nil {
		fmt.Fprintf(table, "store outage\t%s\n", outage.GetReason())
		fmt.Fprintf(table, "outage since\t%s\n", formatTime(outage.GetSince()))
		fmt.Fprintf(table, "resumes\tby itself once object storage answers\n")
	}
	if stop := row.GetStopping(); stop != nil {
		fmt.Fprintf(table, "stopping\ttakes no new work; finishes what it runs, then exits\n")
		if stop.GetSince() != nil {
			fmt.Fprintf(table, "stopping since\t%s\n", formatTime(stop.GetSince()))
		}
		if stop.GetGraceEndsAt() != nil {
			fmt.Fprintf(table, "grace ends\t%s\n", formatTime(stop.GetGraceEndsAt()))
		}
	}
	if row.GetUnavailableReason() == "" {
		for _, p := range row.GetProfiles() {
			fmt.Fprintf(table, "profile %s\t%s\n", p.GetName(), profileLine(row, p))
		}
		for _, cause := range profileCauses(row) {
			fmt.Fprintf(table, "profile cordon\t%s\n", profileCauseNote(cause))
		}
	}
	fmt.Fprintf(table, "tasks\t%s\n", orkerTasks(row))
	fmt.Fprintf(table, "pins\t%s\n", orkerPins(row))
	fmt.Fprintf(table, "last seen\t%s\n", formatTime(row.GetLastSeen()))
	for _, probe := range row.GetMetadataProbes() {
		fmt.Fprintf(table, "metadata %s\t%s\n", probe.GetAddress(), metadataProbeText(probe))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	for _, pin := range row.GetPins() {
		name := dash
		if pin.GetPin() != "" {
			name = strconv.Quote(pin.GetPin())
		}
		fmt.Fprintf(out, "pin %s of run %s since %s expires %s\n",
			name, pin.GetRunId(), formatTime(pin.GetSince()), formatTime(pin.GetExpiresAt()))
	}
	return nil
}

// stoppingTimes is a stopping row's ", since T, grace ends T" in the list's
// footer; empty for an Orker of a release that reports neither.
func stoppingTimes(stop *orkstackv1.OrkerStopping) string {
	text := ""
	if stop.GetSince() != nil {
		text += ", since " + formatTime(stop.GetSince())
	}
	if stop.GetGraceEndsAt() != nil {
		text += ", grace ends " + formatTime(stop.GetGraceEndsAt())
	}
	return text
}

// The verb forms of the stopping footer, for one Orker or several.
func takeTakes(n int) string      { return pick(n, "it takes", "they take") }
func finishFinishes(n int) string { return pick(n, "finishes", "finish") }
func itThey(n int) string         { return pick(n, "it", "they") }
func exitExits(n int) string      { return pick(n, "exits", "exit") }

func pick(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// metadataProbeText is one address's outcome, with its detail: "answered
// (HTTP 200 to GET /)", "no answer", "probe failed (why)".
func metadataProbeText(p *orkstackv1.MetadataProbe) string {
	var outcome string
	switch p.GetOutcome() {
	case orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_ANSWERED:
		outcome = "answered"
	case orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_NO_ANSWER:
		outcome = "no answer"
	case orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_PROBE_FAILED:
		outcome = "probe failed"
	default:
		outcome = "unknown"
	}
	if p.GetDetail() == "" {
		return outcome
	}
	return outcome + " (" + p.GetDetail() + ")"
}

// metadataConcerns are the addresses of an Orker's boot metadata probe that
// answered or could not be told about, each with its outcome: what a reader
// of the list must see. An address with no answer is none.
func metadataConcerns(row *orkstackv1.Orker) []string {
	var concerns []string
	for _, p := range row.GetMetadataProbes() {
		if p.GetOutcome() == orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_NO_ANSWER {
			continue
		}
		concerns = append(concerns, p.GetAddress()+" "+metadataProbeText(p))
	}
	return concerns
}
