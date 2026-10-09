package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// `orkctl release` (docs/DEPLOY.md §3): which build of OrkStack takes new
// runs, the one write that changes it, and the client-side wait for the
// release before it to drain. Nothing server-side holds a rollout: `status
// --watch` and `wait` are loops here.

// The loops' cadence and bounds. A poll is bounded by releasePollTimeout (the
// root's --timeout is shadowed on wait, where it means the wait bound), and a
// wait that stops hearing from the engine gives up after
// releaseUnreachableGrace of consecutive errors.
const (
	releaseWatchInterval    = 5 * time.Second
	releaseWaitInterval     = 10 * time.Second
	releasePollTimeout      = 30 * time.Second
	releaseUnreachableGrace = 60 * time.Second
	releaseRunsShown        = 10
)

// releaseClock, releaseWatchEvery, releaseWaitEvery and releaseWatchLimit are
// package variables only so a test can drive the loops in milliseconds;
// production never changes them. releaseWatchLimit 0 means the watch runs
// until interrupted.
var (
	releaseClock      drainClockSource = realDrainClock{}
	releaseWatchEvery                  = releaseWatchInterval
	releaseWaitEvery                   = releaseWaitInterval
	releaseWatchLimit                  = 0
)

func newReleaseCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "release",
		Short: "Show which release takes new runs, change it, and wait for one to drain",
		Long: "A release is one build of OrkStack: ork and orker built and deployed together,\n" +
			"named by ORK_BUILD_ID. These commands show which release takes new runs,\n" +
			"change it, and wait for the release before it to finish its runs. Releases\n" +
			"are deployment-wide, not namespace objects: these commands ignore --namespace.\n\n" +
			"ramp and promote are refused, with nothing changed, when the release could not\n" +
			"carry the runs it would take: no engine of it is running, a pool in use has no\n" +
			"uncordoned Orker of it, a pool that hosts pins has no pin host of it, or runs\n" +
			"started before any release was current are still executing.",
	}
	cmd.AddCommand(
		newReleaseStatusCommand(g),
		newReleaseRampCommand(g),
		newReleasePromoteCommand(g, false),
		newReleasePromoteCommand(g, true),
		newReleaseWaitCommand(g),
	)
	return cmd
}

// --- status -------------------------------------------------------------------

func newReleaseStatusCommand(g *globalFlags) *cobra.Command {
	var watch bool
	cmd := &cobra.Command{
		Use:   "status [--watch]",
		Short: "List every release, its role, its engines and Orkers, its runs, and what blocks it",
		Long: "List every release the cluster knows: its role (current, ramping N%, previous,\n" +
			"candidate), its engines and Orkers, the pools it serves, its runs in flight,\n" +
			"whether it is drained, and every reason a release that does not take new runs\n" +
			"may not start to. A count that could not be read is `unknown`, never 0.\n\n" +
			"--watch reprints the view every few seconds when it changes, until interrupted.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if watch && g.json {
				return errors.New(`--watch is a view for a person; scripts use "release status --json" or "release wait --json"`)
			}
			if watch {
				return watchReleases(g, cmd)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.systemClient().ListReleases(ctx, connect.NewRequest(&orkstackv1.ListReleasesRequest{}))
			if err != nil {
				return rpcError("release status", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			renderReleases(cmd.OutOrStdout(), resp.Msg, releaseClock.Now())
			return nil
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "reprint the view whenever it changes, until interrupted")
	return cmd
}

// watchReleases polls and reprints the whole view only when it changed.
func watchReleases(g *globalFlags, cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	var last *orkstackv1.ListReleasesResponse
	lastErr := ""
	for polls := 0; releaseWatchLimit == 0 || polls < releaseWatchLimit; polls++ {
		if polls > 0 {
			if err := releaseClock.Sleep(cmd.Context(), releaseWatchEvery); err != nil {
				return nil
			}
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), releasePollTimeout)
		resp, err := g.systemClient().ListReleases(ctx, connect.NewRequest(&orkstackv1.ListReleasesRequest{}))
		cancel()
		now := releaseClock.Now()
		if err != nil {
			if text := rpcError("release status", err).Error(); text != lastErr {
				fmt.Fprintf(out, "— %s — %s\n", now.UTC().Format("15:04:05"), text)
				lastErr = text
			}
			continue
		}
		lastErr = ""
		view := proto.Clone(resp.Msg).(*orkstackv1.ListReleasesResponse)
		view.ObservedAt = nil
		if last != nil && proto.Equal(last, view) {
			continue
		}
		last = view
		fmt.Fprintf(out, "— %s —\n", now.UTC().Format("15:04:05"))
		renderReleases(out, resp.Msg, now)
		fmt.Fprintln(out)
	}
	return nil
}

func roleWord(r *orkstackv1.Release) string {
	switch r.GetRole() {
	case orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT:
		return "current"
	case orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING:
		return "ramping"
	case orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS:
		return "previous"
	case orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE:
		return "candidate"
	default:
		return "unknown"
	}
}

func releaseRoleLabel(r *orkstackv1.Release) string {
	if r.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING {
		return fmt.Sprintf("ramping %s%%", percentText(r.GetRampPercent()))
	}
	return roleWord(r)
}

func percentText(p float32) string {
	return strconv.FormatFloat(float64(p), 'g', -1, 32)
}

func blockerCount(n int) string { return orkerPlural(n, "blocker", "blockers") }

// releaseState is the STATE column.
func releaseState(r *orkstackv1.Release) string {
	n := len(r.GetBlockers())
	switch r.GetRole() {
	case orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT:
		return "serving"
	case orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING:
		if n == 0 {
			return "serving"
		}
		return "at risk (" + blockerCount(n) + ")"
	case orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE:
		if n == 0 {
			return "ready"
		}
		return "not ready (" + blockerCount(n) + ")"
	default:
		return previousState(r, formatTime)
	}
}

// previousState renders a PREVIOUS release's drain, with times formatted by
// the caller (RFC 3339 in the table, HH:MM:SS in wait's lines).
func previousState(r *orkstackv1.Release, at func(*timestamppb.Timestamp) string) string {
	switch {
	case r.GetDrained():
		return "drained"
	case r.GetDrainState() == orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED:
		// The cluster's verdict is in, and something else still holds it: say
		// which, so "drained" is never read into a state it is not.
		switch {
		case releaseOpenPins(r) > 0:
			return "drained by the cluster, pins still open"
		case r.GetRunsUnavailableReason() == "" && r.GetRunsInFlight() > 0:
			return "drained by the cluster, runs still counted"
		default:
			return "drained by the cluster, Orker list incomplete"
		}
	case r.GetDrainState() == orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINING:
		if r.GetDrainCheckedAt() == nil {
			return "draining"
		}
		return "draining (checked " + at(r.GetDrainCheckedAt()) + ")"
	default:
		return "not yet checked"
	}
}

func releaseRuns(r *orkstackv1.Release) string {
	if r.GetRunsUnavailableReason() != "" {
		return "unknown"
	}
	return strconv.Itoa(int(r.GetRunsInFlight()))
}

func releaseOpenPins(r *orkstackv1.Release) int {
	n := 0
	for _, row := range r.GetOrkers() {
		n += len(row.GetPins())
	}
	return n
}

// releasePinLine is `pin "build" of run ork-… on orker-7f3a, expires in 0h41m`.
func releasePinLine(pin *orkstackv1.OpenPin, orker string, now time.Time) string {
	name := dash
	if pin.GetPin() != "" {
		name = strconv.Quote(pin.GetPin())
	}
	expires := "expires -"
	if left := expiresIn(pin.GetExpiresAt(), now); left != dash {
		expires = "expires in " + left
	}
	return fmt.Sprintf("pin %s of run %s on %s, %s", name, pin.GetRunId(), orker, expires)
}

func releasePinLines(r *orkstackv1.Release, now time.Time) []string {
	var lines []string
	for _, row := range r.GetOrkers() {
		for _, pin := range row.GetPins() {
			lines = append(lines, releasePinLine(pin, row.GetOrkerId(), now))
		}
	}
	return lines
}

func runLabel(run *orkstackv1.Run) string {
	ns := run.GetNamespace()
	if ns == "" {
		ns = "default"
	}
	return fmt.Sprintf("%s/%s@%s", ns, orDash(run.GetOrkflowName()), orDash(run.GetOrkflowVersion()))
}

// renderReleases prints the status view (docs/DEPLOY.md §3).
func renderReleases(out io.Writer, resp *orkstackv1.ListReleasesResponse, now time.Time) {
	fmt.Fprintf(out, "%-12s %s\n", "deployment:", resp.GetDeploymentName())
	if !resp.GetVersioned() {
		fmt.Fprintln(out, "No release of this deployment has registered with the cluster, so versioning is off:")
		fmt.Fprintln(out, "every engine and Orker runs without ORK_BUILD_ID. Set ORK_BUILD_ID on both binaries")
		fmt.Fprintln(out, "to start one (docs/DEPLOY.md §1).")
		return
	}
	var current, ramping *orkstackv1.Release
	for _, r := range resp.GetReleases() {
		switch r.GetRole() {
		case orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT:
			current = r
		case orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING:
			ramping = r
		}
	}
	if current != nil {
		fmt.Fprintf(out, "%-12s %s since %s\n", "current:", current.GetBuildId(), formatTime(current.GetRoleSince()))
	} else {
		fmt.Fprintf(out, "%-12s %s\n", "current:", noCurrentSentence(resp.GetUnversionedEngineReplicas()))
	}
	if ramping != nil {
		fmt.Fprintf(out, "%-12s %s at %s%% since %s\n", "ramping:", ramping.GetBuildId(),
			percentText(ramping.GetRampPercent()), formatTime(ramping.GetRoleSince()))
	} else {
		fmt.Fprintf(out, "%-12s %s\n", "ramping:", dash)
	}
	fmt.Fprintln(out)

	if len(resp.GetReleases()) > 0 {
		table := newTable(out)
		fmt.Fprintln(table, "RELEASE\tROLE\tENGINE\tORKERS\tPOOLS SERVED\tRUNS\tSTATE")
		for _, r := range resp.GetReleases() {
			fmt.Fprintf(table, "%s\t%s\t%d\t%d\t%s\t%s\t%s\n", r.GetBuildId(), releaseRoleLabel(r),
				r.GetEngineReplicas(), len(r.GetOrkers()), joinOrDash(r.GetPoolsServed()), releaseRuns(r), releaseState(r))
		}
		_ = table.Flush()
	}

	// Blockers, per release that does not take every new run.
	for _, r := range resp.GetReleases() {
		if len(r.GetBlockers()) == 0 || r.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT {
			continue
		}
		if r.GetDrained() && r.GetEngineReplicas() == 0 && len(r.GetOrkers()) == 0 {
			// A drained release with nothing running is history, not a
			// candidate for anything; its blockers stay in --json.
			continue
		}
		fmt.Fprintln(out)
		if r.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING {
			fmt.Fprintf(out, "%s is ramping but cannot take more new runs — the runs it takes now are affected too:\n", r.GetBuildId())
		} else {
			fmt.Fprintf(out, "%s cannot take new runs yet:\n", r.GetBuildId())
		}
		for _, b := range r.GetBlockers() {
			fmt.Fprintf(out, "  - %s\n", b.GetMessage())
		}
	}

	// Holds: what keeps a release that takes no new runs from being stopped.
	var held []string
	for _, r := range resp.GetReleases() {
		role := r.GetRole()
		if role != orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS && role != orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE {
			continue
		}
		pins := releasePinLines(r, now)
		if r.GetDrained() || (len(r.GetRuns()) == 0 && r.GetRunsInFlight() == 0 && len(pins) == 0) {
			continue
		}
		held = append(held, r.GetBuildId())
		fmt.Fprintln(out)
		fmt.Fprintf(out, "%s is still finishing work:\n", r.GetBuildId())
		if r.GetRunsUnavailableReason() == "" && r.GetRunsInFlight() > 0 {
			label := "  " + orkerPlural(int(r.GetRunsInFlight()), "run", "runs") + ":  "
			pad := strings.Repeat(" ", len(label))
			for i, run := range r.GetRuns() {
				prefix := pad
				if i == 0 {
					prefix = label
				}
				fmt.Fprintf(out, "%s%s  %s\n", prefix, run.GetId(), runLabel(run))
			}
			if more := int(r.GetRunsInFlight()) - len(r.GetRuns()); more > 0 {
				prefix := pad
				if len(r.GetRuns()) == 0 {
					prefix = label
				}
				fmt.Fprintf(out, "%sand %d more\n", prefix, more)
			}
		}
		if len(pins) > 0 {
			label := "  " + orkerPlural(len(pins), "open pin", "open pins") + ":  "
			pad := strings.Repeat(" ", len(label))
			for i, line := range pins {
				prefix := pad
				if i == 0 {
					prefix = label
				}
				fmt.Fprintf(out, "%s%s\n", prefix, line)
			}
		}
	}
	if len(held) > 0 {
		fmt.Fprintln(out, "Keep one of its engines and one of its Orkers in every pool it serves running until it is")
		fmt.Fprintln(out, "drained, and stop no Orker that hosts an open pin (\"orkctl orker drain <orker>\" waits for one).")
		for _, build := range held {
			fmt.Fprintf(out, "\"orkctl release wait %s\" exits when it is drained.\n", build)
		}
	}

	var notes []string
	for _, r := range resp.GetReleases() {
		if reason := r.GetRunsUnavailableReason(); reason != "" {
			notes = append(notes, fmt.Sprintf("runs in flight could not be counted for %s: %s", r.GetBuildId(), reason))
		}
	}
	if rows := resp.GetUnattributedOrkers(); len(rows) > 0 {
		notes = append(notes, orkerPlural(len(rows), "Orker belongs", "Orkers belong")+" to no release:")
		for _, row := range rows {
			switch {
			case row.GetUnavailableReason() != "":
				notes = append(notes, fmt.Sprintf("  %s: did not answer (%s)", row.GetOrkerId(), row.GetUnavailableReason()))
			case row.GetBuildId() == "":
				notes = append(notes, fmt.Sprintf("  %s: pool %s, started without ORK_BUILD_ID", row.GetOrkerId(), orDash(row.GetPool())))
			default:
				notes = append(notes, fmt.Sprintf("  %s: pool %s, release %s is no longer known to the cluster",
					row.GetOrkerId(), orDash(row.GetPool()), row.GetBuildId()))
			}
		}
	}
	if reason := resp.GetOrkersUnavailableReason(); reason != "" {
		notes = append(notes, reason)
	}
	if current != nil && current.GetEngineReplicas() == 0 {
		notes = append(notes, "the current release has no running engine: new runs will not start")
	}
	if len(notes) > 0 {
		fmt.Fprintln(out)
		for _, note := range notes {
			fmt.Fprintln(out, note)
		}
	}
}

func noCurrentSentence(engines int32) string {
	return fmt.Sprintf("none — new runs go to the %s started without ORK_BUILD_ID; a release takes nothing "+
		"until it is promoted (docs/DEPLOY.md §3 step 0)", orkerPlural(int(engines), "engine", "engines"))
}

// --- promote / rollback ---------------------------------------------------------

// readReleases is the read a write is judged against, bounded on its own so
// the write that follows it gets a whole request budget of its own.
func (g *globalFlags) readReleases(cmd *cobra.Command) (*orkstackv1.ListReleasesResponse, error) {
	ctx, cancel := g.unaryContext(cmd)
	defer cancel()
	read, err := g.systemClient().ListReleases(ctx, connect.NewRequest(&orkstackv1.ListReleasesRequest{}))
	if err != nil {
		return nil, err
	}
	return read.Msg, nil
}

func findRelease(resp *orkstackv1.ListReleasesResponse, build string) *orkstackv1.Release {
	for _, r := range resp.GetReleases() {
		if r.GetBuildId() == build {
			return r
		}
	}
	return nil
}

func newReleasePromoteCommand(g *globalFlags, rollback bool) *cobra.Command {
	use, short, long, op := "promote <build-id>", "Make one release take every new run",
		"Make one release take every new run. Runs already executing on the release before it\n"+
			"finish there and are never moved. Refused, with nothing changed, when the release\n"+
			"could not carry the runs it would take; the refusal names every reason and what to\n"+
			"start. There is no override: the cluster's own commands (docs/DEPLOY.md) remain.",
		"release promote"
	if rollback {
		use, short, long, op = "rollback <build-id>", "Return new runs to a release that was current before",
			"Return new runs to a release that was current before: promote, aimed only at a\n"+
				"previous release. Runs executing on the release rolled back from finish there.",
			"release rollback"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			build := args[0]
			read, err := g.readReleases(cmd)
			if err != nil {
				return rpcError(op, err)
			}
			target := findRelease(read, build)
			if rollback {
				if target == nil {
					return fmt.Errorf("no release %s is known to the cluster", build)
				}
				if target.GetRole() != orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS {
					return fmt.Errorf("rollback returns to a release that was current before; %s is %s — use \"orkctl release promote %s\"",
						build, roleWord(target), build)
				}
			}
			// The write is its own request with its own bound: the
			// server may spend up to 10s confirming routing after the change,
			// and the read above must not have spent that budget already.
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.systemClient().PromoteRelease(ctx, connect.NewRequest(&orkstackv1.PromoteReleaseRequest{
				BuildId: build, ExpectedRevision: read.GetRevision()}))
			if err != nil {
				return rpcError(op, err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			renderPromote(cmd.OutOrStdout(), read, target, resp.Msg, rollback)
			return nil
		},
	}
}

func renderPromote(out io.Writer, read *orkstackv1.ListReleasesResponse, target *orkstackv1.Release,
	resp *orkstackv1.PromoteReleaseResponse, rollback bool) {
	build := target.GetBuildId()
	if target == nil {
		build = resp.GetPreviousBuildId()
	}
	if !resp.GetChanged() {
		fmt.Fprintf(out, "release %s is already current; nothing changed.\n", build)
		return
	}
	verb := "current"
	if rollback {
		verb = "current again"
	}
	previous := resp.GetPreviousBuildId()
	ramp := ""
	if target.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING {
		ramp = fmt.Sprintf(" (its %s%% ramp ends)", percentText(target.GetRampPercent()))
	}
	if previous == "" {
		fmt.Fprintf(out, "release %s is %s%s.\n", build, verb, ramp)
		fmt.Fprintf(out, "New runs start on %s.\n", build)
	} else {
		fmt.Fprintf(out, "release %s is %s (was %s)%s.\n", build, verb, previous, ramp)
		fmt.Fprintf(out, "New runs start on %s; %s.\n", build, runsFinishPhrase(findRelease(read, previous), previous))
		fmt.Fprintf(out, "Keep %s running until \"orkctl release wait %s\" exits 0.\n", previous, previous)
	}
	if !resp.GetRoutingConfirmed() {
		fmt.Fprintln(out, "The change is recorded, but the cluster had not applied it to new runs after 10s; "+
			"\"orkctl release status\" shows when it has.")
	}
}

// runsFinishPhrase is "the 14 runs executing on X finish there".
func runsFinishPhrase(r *orkstackv1.Release, build string) string {
	if r == nil || r.GetRunsUnavailableReason() != "" {
		return "the runs executing on " + build + " finish there"
	}
	if r.GetRunsInFlight() == 1 {
		return "the 1 run executing on " + build + " finishes there"
	}
	return fmt.Sprintf("the %d runs executing on %s finish there", r.GetRunsInFlight(), build)
}

// --- ramp -------------------------------------------------------------------------

func newReleaseRampCommand(g *globalFlags) *cobra.Command {
	var percent int
	cmd := &cobra.Command{
		Use:   "ramp <build-id> --percent N",
		Short: "Send a share of new runs to a release that is not current",
		Long: "Send N% of new runs to one release that is not current, and the rest to the\n" +
			"current release. --percent 0 stops the ramp of the release that is ramping.\n" +
			"A ramp above 0 is refused, with nothing changed, when the release could not\n" +
			"carry the runs it would take. Runs already executing are never moved.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			build := args[0]
			if percent < 0 || percent > 100 {
				return fmt.Errorf("--percent is 1–100 to ramp a release, or 0 to stop its ramp; %d is neither", percent)
			}
			read, err := g.readReleases(cmd)
			if err != nil {
				return rpcError("release ramp", err)
			}
			// Its own request, its own bound: see promote.
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.systemClient().RampRelease(ctx, connect.NewRequest(&orkstackv1.RampReleaseRequest{
				BuildId: build, Percent: int32(percent), ExpectedRevision: read.GetRevision()}))
			if err != nil {
				return rpcError("release ramp", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			renderRamp(cmd.OutOrStdout(), read, build, int32(percent), resp.Msg)
			return nil
		},
	}
	cmd.Flags().IntVar(&percent, "percent", 0, "share of new runs, 1–100; 0 stops the ramp (required)")
	_ = cmd.MarkFlagRequired("percent")
	return cmd
}

func renderRamp(out io.Writer, read *orkstackv1.ListReleasesResponse, build string, percent int32,
	resp *orkstackv1.RampReleaseResponse) {
	if !resp.GetChanged() {
		fmt.Fprintf(out, "release %s already takes %d%% of new runs; nothing changed.\n", build, percent)
		return
	}
	if percent == 0 {
		runs := "the runs it started during the ramp finish on it"
		if r := findRelease(read, build); r != nil && r.GetRunsUnavailableReason() == "" {
			if r.GetRunsInFlight() == 1 {
				runs = "the 1 run it started during the ramp finishes on it"
			} else {
				runs = fmt.Sprintf("the %d runs it started during the ramp finish on it", r.GetRunsInFlight())
			}
		}
		fmt.Fprintf(out, "release %s no longer takes new runs; %s.\n", build, runs)
	} else {
		rest := "the engines started without ORK_BUILD_ID take the rest"
		for _, r := range read.GetReleases() {
			if r.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT {
				rest = r.GetBuildId() + " (current) takes the rest"
			}
		}
		replaced := ""
		if prev := resp.GetPreviousBuildId(); prev != "" && prev != build {
			replaced = fmt.Sprintf(" (%s was ramping at %s%%; its runs finish on it)", prev, percentText(resp.GetPreviousPercent()))
		}
		fmt.Fprintf(out, "release %s now takes %d%% of new runs; %s%s.\n", build, percent, rest, replaced)
	}
	if !resp.GetRoutingConfirmed() {
		fmt.Fprintln(out, "The change is recorded, but the cluster had not applied it to new runs after 10s; "+
			"\"orkctl release status\" shows when it has.")
	}
}

// --- wait -------------------------------------------------------------------------

func newReleaseWaitCommand(g *globalFlags) *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "wait <build-id> [--timeout 1h]",
		Short: "Wait until a release that takes no new runs is drained and may be stopped",
		Long: "Ask every few seconds until the release is drained: the cluster reports none of\n" +
			"its runs executing AND no Orker of it hosts an open pin. Each change in what still\n" +
			"holds it — its runs, and each open pin with the Orker it lives on — is printed.\n\n" +
			"Exit 0 means it is drained (or the cluster has already forgotten it): its engines\n" +
			"and Orkers may be stopped. Exit 4 means --timeout passed first; what remains is\n" +
			"printed. Exit 1 means it takes new runs, is unknown, or the engine stopped\n" +
			"answering. The cluster refreshes its drain verdict on a delay of minutes.\n\n" +
			"Nothing server-side holds wait state: this is a loop in orkctl.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			w := &releaseWaiter{g: g, cmd: cmd, build: args[0], timeout: timeout, out: cmd.OutOrStdout()}
			return w.run()
		},
	}
	// A LOCAL --timeout shadows the root's persistent one on this command only:
	// here it is the wait bound, and each poll is bounded by releasePollTimeout.
	cmd.Flags().DurationVar(&timeout, "timeout", time.Hour,
		"how long to wait for the release to drain (on this command, not the per-request timeout)")
	return cmd
}

type releaseWaiter struct {
	g       *globalFlags
	cmd     *cobra.Command
	build   string
	timeout time.Duration
	out     io.Writer
	last    *orkstackv1.ListReleasesResponse
}

func (w *releaseWaiter) poll() (*orkstackv1.Release, error) {
	ctx, cancel := context.WithTimeout(w.cmd.Context(), releasePollTimeout)
	defer cancel()
	resp, err := w.g.systemClient().ListReleases(ctx, connect.NewRequest(&orkstackv1.ListReleasesRequest{BuildId: w.build}))
	if err != nil {
		return nil, err
	}
	w.last = resp.Msg
	return findRelease(resp.Msg, w.build), nil
}

func (w *releaseWaiter) say(format string, args ...any) {
	if !w.g.json {
		fmt.Fprintf(w.out, format, args...)
	}
}

// finish ends the wait with code and err, printing the final response once
// under --json whichever way the loop ended.
func (w *releaseWaiter) finish(code int, err error) error {
	if w.g.json {
		last := w.last
		if last == nil {
			last = &orkstackv1.ListReleasesResponse{}
		}
		if werr := writeJSON(w.out, last); werr != nil {
			return werr
		}
	}
	if err == nil {
		return nil
	}
	return &exitCodeError{code: code, err: err}
}

func takesNewRuns(r *orkstackv1.Release) bool {
	return r.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT || r.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING
}

func clockTime(ts *timestamppb.Timestamp) string {
	if ts == nil || !ts.IsValid() {
		return dash
	}
	return ts.AsTime().UTC().Format("15:04:05")
}

// waitKey is what a progress line is printed for a change of.
func waitKey(r *orkstackv1.Release) string {
	return fmt.Sprintf("%s|%d|%s", releaseRuns(r), releaseOpenPins(r), previousState(r, clockTime))
}

func (w *releaseWaiter) progress(now time.Time, r *orkstackv1.Release) {
	runs := releaseRuns(r)
	counts := fmt.Sprintf("%s %s, %s", runs, pluralWord(runs, "run", "runs"),
		orkerPlural(releaseOpenPins(r), "pin", "pins"))
	w.say("  %s  %-15s %s\n", now.UTC().Format("15:04:05"), counts, previousState(r, clockTime))
	for _, line := range releasePinLines(r, now) {
		w.say("            %s\n", line)
	}
}

func pluralWord(count, one, many string) string {
	if count == "1" {
		return one
	}
	return many
}

// incompleteInventory names why drained cannot be reported, or "".
func incompleteInventory(resp *orkstackv1.ListReleasesResponse) string {
	if reason := resp.GetOrkersUnavailableReason(); reason != "" {
		return reason
	}
	for _, row := range resp.GetUnattributedOrkers() {
		if row.GetUnavailableReason() != "" {
			return row.GetOrkerId() + " did not answer: " + row.GetUnavailableReason()
		}
	}
	return ""
}

func (w *releaseWaiter) drainedLine(now time.Time, r *orkstackv1.Release) error {
	w.progress(now, r)
	w.say("release %s is drained; its engines and Orkers may be stopped.\n", w.build)
	return w.finish(exitOK, nil)
}

func (w *releaseWaiter) run() error {
	start := releaseClock.Now()
	deadline := start.Add(w.timeout)

	r, err := w.poll()
	if err != nil {
		return w.finish(exitError, rpcError("release wait", err))
	}
	if r == nil {
		return w.finish(exitError, fmt.Errorf("no release %s is known to the cluster; \"orkctl release status\" lists the ones that are", w.build))
	}
	if takesNewRuns(r) {
		return w.finish(exitError, fmt.Errorf("release %s takes new runs (%s); it cannot drain until another release is promoted",
			w.build, releaseRoleLabel(r)))
	}
	w.say("waiting for release %s to drain…\n", w.build)
	if r.GetDrained() {
		return w.drainedLine(releaseClock.Now(), r)
	}
	w.progress(releaseClock.Now(), r)
	lastKey := waitKey(r)
	reportedIncomplete := false
	if why := incompleteInventory(w.last); why != "" {
		w.say("the Orker list is incomplete (%s); drained is not reported until every Orker answers\n", why)
		reportedIncomplete = true
	}

	var failingSince time.Time
	reportedError := false
	for {
		now := releaseClock.Now()
		if !now.Before(deadline) {
			return w.timedOut(now)
		}
		wait := releaseWaitEvery
		if left := deadline.Sub(now); left < wait {
			wait = left
		}
		if err := releaseClock.Sleep(w.cmd.Context(), wait); err != nil {
			return w.finish(exitError, err)
		}
		now = releaseClock.Now()
		if !now.Before(deadline) {
			return w.timedOut(now)
		}
		r, err := w.poll()
		now = releaseClock.Now()
		if err != nil {
			if failingSince.IsZero() {
				failingSince = now
			}
			if !reportedError {
				w.say("  %s  %s\n", now.UTC().Format("15:04:05"), rpcError("release wait", err))
				reportedError = true
			}
			if now.Sub(failingSince) >= releaseUnreachableGrace {
				return w.finish(exitError, fmt.Errorf("the engine stopped answering for %s; release %s may not be drained",
					releaseUnreachableGrace, w.build))
			}
			continue
		}
		failingSince, reportedError = time.Time{}, false
		switch {
		case r == nil:
			w.say("release %s is no longer known to the cluster: it drained and none of its processes is polling, so the cluster forgot it.\n", w.build)
			return w.finish(exitOK, nil)
		case takesNewRuns(r):
			return w.finish(exitError, fmt.Errorf("release %s takes new runs again (it was promoted or ramped); stop waiting", w.build))
		case r.GetDrained():
			return w.drainedLine(now, r)
		}
		if key := waitKey(r); key != lastKey {
			w.progress(now, r)
			lastKey = key
		}
		if why := incompleteInventory(w.last); why != "" && !reportedIncomplete {
			w.say("the Orker list is incomplete (%s); drained is not reported until every Orker answers\n", why)
			reportedIncomplete = true
		}
	}
}

func (w *releaseWaiter) timedOut(now time.Time) error {
	r := findRelease(w.last, w.build)
	if r == nil {
		r = &orkstackv1.Release{BuildId: w.build}
	}
	runs := releaseRuns(r)
	pins := releaseOpenPins(r)
	w.say("release %s is NOT drained after %s: %s %s, %s still on it\n", w.build, w.timeout, runs,
		pluralWord(runs, "run", "runs"), orkerPlural(pins, "pin", "pins"))
	for _, run := range r.GetRuns() {
		w.say("  run %s (%s)\n", run.GetId(), runLabel(run))
	}
	if more := int(r.GetRunsInFlight()) - len(r.GetRuns()); more > 0 {
		w.say("  and %d more\n", more)
	}
	hosts := map[string]bool{}
	for _, row := range r.GetOrkers() {
		for _, pin := range row.GetPins() {
			w.say("  %s\n", releasePinLine(pin, row.GetOrkerId(), now))
			hosts[row.GetOrkerId()] = true
		}
	}
	if runs != "0" {
		that := "those runs"
		if runs == "1" {
			that = "that run"
		}
		w.say("stopping its last engine, or its last Orker of a pool, now leaves %s unable to finish;\n", that)
	}
	if len(hosts) > 0 {
		names := make([]string, 0, len(hosts))
		for host := range hosts {
			names = append(names, host)
		}
		sort.Strings(names)
		it := "it"
		if pins > 1 {
			it = "them"
		}
		w.say("stopping %s fails %s with PIN_HOST_LOST.\n", strings.Join(names, ", "), it)
	}
	return w.finish(exitDrainTimedOut, fmt.Errorf("release %s did not drain within %s", w.build, w.timeout))
}
