package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

const overlapPolicyPrefix = "OVERLAP_POLICY_"

// overlapPolicySlug is the user-facing spelling of an overlap policy: the
// value's name after OVERLAP_POLICY_, lowercased, "_" as "-" — the rule
// origin kinds use. UNSPECIFIED and undeclared numbers have none.
func overlapPolicySlug(p orkstackv1.OverlapPolicy) (string, bool) {
	if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		return "", false
	}
	name, ok := orkstackv1.OverlapPolicy_name[int32(p)]
	if !ok {
		return "", false
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(name, overlapPolicyPrefix)), "_", "-"), true
}

// overlapPolicySlugs is every slug, in declaration order.
func overlapPolicySlugs() []string {
	values := orkstackv1.File_orkstack_v1_schedule_proto.Enums().ByName("OverlapPolicy").Values()
	var slugs []string
	for i := 0; i < values.Len(); i++ {
		if slug, ok := overlapPolicySlug(orkstackv1.OverlapPolicy(values.Get(i).Number())); ok {
			slugs = append(slugs, slug)
		}
	}
	return slugs
}

// parseOverlapPolicy reads an --overlap slug. "unspecified" is not a slug:
// leaving the flag out is how a caller says "no policy given".
func parseOverlapPolicy(slug string) (orkstackv1.OverlapPolicy, error) {
	for number := range orkstackv1.OverlapPolicy_name {
		p := orkstackv1.OverlapPolicy(number)
		if candidate, ok := overlapPolicySlug(p); ok && candidate == slug {
			return p, nil
		}
	}
	return orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED,
		fmt.Errorf("invalid --overlap %q: want one of %s", slug, strings.Join(overlapPolicySlugs(), ", "))
}

// overlapLabel renders a recorded overlap; a read never carries UNSPECIFIED,
// but an old or hand-edited record reads as the cluster's default.
func overlapLabel(p orkstackv1.OverlapPolicy) string {
	if slug, ok := overlapPolicySlug(p); ok {
		return slug
	}
	return "skip"
}

// scheduleClient reaches ScheduleService.
func (g *globalFlags) scheduleClient() orkstackv1connect.ScheduleServiceClient {
	return orkstackv1connect.NewScheduleServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func newScheduleCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Start runs of an orkflow on a schedule",
		Long: "A schedule starts a run of one orkflow at the times its cron\n" +
			"expressions or interval name, in its time zone. The cluster keeps the\n" +
			"schedule: overlap, catch-up, pause and backfill are its own, and\n" +
			"OrkStack stores nothing else about a schedule.\n\n" +
			"Every run a schedule starts carries origin schedule:<id>, so\n" +
			"'orkctl run list --filter origin.kind=SCHEDULE --filter origin.id=<id>'\n" +
			"is its audit trail.",
	}
	cmd.AddCommand(
		newScheduleCreateCommand(g),
		newScheduleListCommand(g),
		newScheduleGetCommand(g),
		newScheduleUpdateCommand(g),
		newSchedulePauseCommand(g),
		newScheduleResumeCommand(g),
		newScheduleFireCommand(g),
		newScheduleBackfillCommand(g),
		newScheduleDeleteCommand(g),
	)
	return cmd
}

// scheduleSpecFlags are the flags that author a ScheduleSpec, shared by
// create and update.
type scheduleSpecFlags struct {
	cron           []string
	interval       string
	timezone       string
	jitter         string
	startAt        string
	endAt          string
	overlap        string
	catchupWindow  string
	pauseOnFailure bool
}

func (f *scheduleSpecFlags) register(cmd *cobra.Command, pauseOnFailureHelp string) {
	cmd.Flags().StringArrayVar(&f.cron, "cron", nil,
		"cron expression (5-7 fields, @daily, \"@every 15m\"); repeatable, fires at the union")
	cmd.Flags().StringVar(&f.interval, "interval", "", "fire every DURATION, aligned to the Unix epoch (e.g. 1h, 15m)")
	cmd.Flags().StringVar(&f.timezone, "timezone", "", "IANA time zone the cron expressions are evaluated in (default UTC)")
	cmd.Flags().StringVar(&f.jitter, "jitter", "", "delay each fire by a random amount up to DURATION")
	cmd.Flags().StringVar(&f.startAt, "start-at", "", "no fire before this RFC3339 time")
	cmd.Flags().StringVar(&f.endAt, "end-at", "", "no fire after this RFC3339 time")
	cmd.Flags().StringVar(&f.overlap, "overlap", "",
		"when a fire is due while the previous run still executes: "+strings.Join(overlapPolicySlugs(), "|")+" (default skip)")
	cmd.Flags().StringVar(&f.catchupWindow, "catchup-window", "",
		"how late a fire missed while the cluster was down may still be taken (at least 10s)")
	cmd.Flags().BoolVar(&f.pauseOnFailure, "pause-on-failure", false, pauseOnFailureHelp)
}

// apply writes every flag the user set onto spec. On create every flag is
// "set" from an empty spec; on update only changed flags replace a field.
func (f *scheduleSpecFlags) apply(cmd *cobra.Command, spec *orkstackv1.ScheduleSpec) error {
	changed := cmd.Flags().Changed
	if changed("cron") {
		spec.Cron = append([]string(nil), f.cron...)
	}
	if changed("interval") {
		d, err := parseScheduleDuration("--interval", f.interval)
		if err != nil {
			return err
		}
		spec.Interval = d
	}
	if changed("timezone") {
		spec.Timezone = f.timezone
	}
	if changed("jitter") {
		d, err := parseScheduleDuration("--jitter", f.jitter)
		if err != nil {
			return err
		}
		spec.Jitter = d
	}
	if changed("start-at") {
		ts, err := parseRFC3339Bound("--start-at", f.startAt)
		if err != nil {
			return err
		}
		spec.StartAt = ts
	}
	if changed("end-at") {
		ts, err := parseRFC3339Bound("--end-at", f.endAt)
		if err != nil {
			return err
		}
		spec.EndAt = ts
	}
	if changed("overlap") {
		p, err := parseOverlapPolicy(f.overlap)
		if err != nil {
			return err
		}
		spec.Overlap = p
	}
	if changed("catchup-window") {
		d, err := parseScheduleDuration("--catchup-window", f.catchupWindow)
		if err != nil {
			return err
		}
		spec.CatchupWindow = d
	}
	if changed("pause-on-failure") {
		spec.PauseOnFailure = f.pauseOnFailure
	}
	return nil
}

// parseScheduleDuration reads a Go duration flag; empty clears the field.
func parseScheduleDuration(flag, value string) (*durationpb.Duration, error) {
	if value == "" {
		return nil, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return nil, fmt.Errorf("%s must be a duration such as 30s, 15m or 1h: %w", flag, err)
	}
	return durationpb.New(d), nil
}

func newScheduleCreateCommand(g *globalFlags) *cobra.Command {
	var (
		spec        scheduleSpecFlags
		orkflow     string
		version     string
		inputs      []string
		inputJSON   string
		description string
		paused      bool
	)
	cmd := &cobra.Command{
		Use:   "create NAME --orkflow ORKFLOW (--cron EXPR ... | --interval DUR)",
		Short: "Create a schedule that starts one orkflow",
		Long: "create binds a new schedule, named NAME in this namespace, to one orkflow.\n\n" +
			"--version pins a version; omitting it follows the orkflow's current\n" +
			"pointer, resolved at each fire. --overlap decides what happens when a\n" +
			"fire is due while the previous run still executes; the default is skip.\n" +
			"--paused creates it paused, so 'orkctl schedule get' shows the next fire\n" +
			"times before it ever fires.",
		Example: "  orkctl schedule create nightly --orkflow reconcile --cron '0 2 * * *' \\\n" +
			"    --timezone Europe/Lisbon --overlap skip --paused",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(orkflow) == "" {
				return fmt.Errorf("--orkflow is required: a schedule starts exactly one orkflow")
			}
			if len(spec.cron) == 0 && strings.TrimSpace(spec.interval) == "" {
				return fmt.Errorf("spec: at least one cron expression or an interval is required (--cron or --interval)")
			}
			request, err := buildScheduleCreateRequest(cmd, g.namespace, args[0], orkflow, version,
				&spec, inputs, inputJSON, description, paused)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().CreateSchedule(ctx, connect.NewRequest(request))
			if err != nil {
				return rpcError("create schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Created schedule %s\n\n", args[0])
			return renderSchedule(cmd.OutOrStdout(), response.Msg.GetSchedule())
		},
	}
	spec.register(cmd, "pause the schedule when a fire fails or is refused")
	cmd.Flags().StringVar(&orkflow, "orkflow", "", "orkflow every fire starts (required)")
	cmd.Flags().StringVar(&version, "version", "", "orkflow version to pin (default: the current one at each fire)")
	cmd.Flags().StringArrayVar(&inputs, "input", nil, "input value as key=value, submitted at every fire (repeatable)")
	cmd.Flags().StringVar(&inputJSON, "input-json", "", "input as one JSON object, submitted at every fire")
	cmd.Flags().StringVar(&description, "description", "", "operator label for this schedule")
	cmd.Flags().BoolVar(&paused, "paused", false, "create the schedule paused")
	return cmd
}

// buildScheduleCreateRequest assembles a create from the flags. An absent
// --overlap sends UNSPECIFIED, which the server records as skip.
func buildScheduleCreateRequest(
	cmd *cobra.Command,
	namespace, name, orkflow, version string,
	spec *scheduleSpecFlags,
	inputs []string, inputJSON, description string,
	paused bool,
) (*orkstackv1.CreateScheduleRequest, error) {
	built := &orkstackv1.ScheduleSpec{}
	if err := spec.apply(cmd, built); err != nil {
		return nil, err
	}
	input, err := buildInputPayload(inputs, inputJSON)
	if err != nil {
		return nil, err
	}
	return &orkstackv1.CreateScheduleRequest{
		Namespace:      namespace,
		Name:           name,
		OrkflowName:    orkflow,
		OrkflowVersion: version,
		Input:          input,
		Description:    description,
		Spec:           built,
		Paused:         paused,
	}, nil
}

func newScheduleListCommand(g *globalFlags) *cobra.Command {
	var filterTexts *[]string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the schedules of this namespace",
		Long: "list shows the schedules of the namespace this command acts in, each\n" +
			"described on the cluster for this call, narrowed by --filter: a filter on\n" +
			"id, name or orkflow_name is decided before a schedule is described. A row\n" +
			"the cluster could not describe reads 'unavailable', and is kept under a\n" +
			"filter on a field it could not report.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().ListSchedules(ctx, connect.NewRequest(&orkstackv1.ListSchedulesRequest{
				Namespace: g.namespace, Filters: filters,
			}))
			if err != nil {
				return rpcError("list schedules", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			if len(filters) > 0 && len(response.Msg.GetSchedules()) == 0 {
				fmt.Fprint(cmd.OutOrStdout(), noMatchSentence("schedules", filters))
				return nil
			}
			return renderScheduleList(cmd.OutOrStdout(), response.Msg.GetSchedules())
		},
	}
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListSchedulesRequest{})
	return cmd
}

func newScheduleGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get NAME",
		Short: "Show one schedule, its next fire times and recent fires",
		Long: "get reads one schedule from the cluster: its spec, state, next fire\n" +
			"times, and recent fires with the run each one started or why it\n" +
			"started none.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().GetSchedule(ctx, connect.NewRequest(&orkstackv1.GetScheduleRequest{
				Name: args[0], Namespace: g.namespace,
			}))
			if err != nil {
				return rpcError("get schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			return renderSchedule(cmd.OutOrStdout(), response.Msg.GetSchedule())
		},
	}
}

func newScheduleUpdateCommand(g *globalFlags) *cobra.Command {
	var (
		spec          scheduleSpecFlags
		version       string
		followCurrent bool
		inputs        []string
		inputJSON     string
		clearInput    bool
		description   string
	)
	cmd := &cobra.Command{
		Use:   "update NAME [flags]",
		Short: "Change a schedule's version pin, input, description or spec",
		Long: "update reads the schedule, replaces only the fields whose flags you set,\n" +
			"and writes the whole schedule back. --cron given replaces every cron\n" +
			"expression. --follow-current clears a version pin; --clear-input clears\n" +
			"the input. The orkflow a schedule starts cannot change: delete and\n" +
			"create instead.\n\n" +
			"Last writer wins: two concurrent updates are not detected.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("version") && followCurrent {
				return fmt.Errorf("--version and --follow-current are mutually exclusive")
			}
			if clearInput && (len(inputs) > 0 || inputJSON != "") {
				return fmt.Errorf("--clear-input cannot be combined with --input or --input-json")
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			current, err := g.scheduleClient().GetSchedule(ctx, connect.NewRequest(&orkstackv1.GetScheduleRequest{
				Name: args[0], Namespace: g.namespace,
			}))
			if err != nil {
				return rpcError("get schedule", err)
			}
			request, err := buildScheduleUpdateRequest(cmd, g.namespace, current.Msg.GetSchedule(), &spec,
				version, followCurrent, inputs, inputJSON, clearInput, description)
			if err != nil {
				return err
			}
			response, err := g.scheduleClient().UpdateSchedule(ctx, connect.NewRequest(request))
			if err != nil {
				return rpcError("update schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Updated schedule %s\n\n", args[0])
			return renderSchedule(cmd.OutOrStdout(), response.Msg.GetSchedule())
		},
	}
	spec.register(cmd, "pause the schedule when a fire fails or is refused (--pause-on-failure=false turns it off)")
	cmd.Flags().StringVar(&version, "version", "", "pin this orkflow version")
	cmd.Flags().BoolVar(&followCurrent, "follow-current", false, "clear the version pin: follow the current pointer at each fire")
	cmd.Flags().StringArrayVar(&inputs, "input", nil, "replace the input with key=value pairs (repeatable)")
	cmd.Flags().StringVar(&inputJSON, "input-json", "", "replace the input with one JSON object")
	cmd.Flags().BoolVar(&clearInput, "clear-input", false, "clear the input")
	cmd.Flags().StringVar(&description, "description", "", "replace the description")
	return cmd
}

// buildScheduleUpdateRequest is the read-modify-write: the schedule as read,
// with only the flags the user set replacing fields. A read-back overlap is
// sent back as read.
func buildScheduleUpdateRequest(
	cmd *cobra.Command,
	namespace string,
	current *orkstackv1.Schedule,
	spec *scheduleSpecFlags,
	version string,
	followCurrent bool,
	inputs []string, inputJSON string, clearInput bool,
	description string,
) (*orkstackv1.UpdateScheduleRequest, error) {
	if current.GetUnavailableReason() != "" {
		return nil, fmt.Errorf("schedule %s cannot be read back to update: %s", current.GetName(), current.GetUnavailableReason())
	}
	request := &orkstackv1.UpdateScheduleRequest{
		Name:           current.GetName(),
		Namespace:      namespace,
		OrkflowVersion: current.GetOrkflowVersion(),
		Input:          current.GetInput(),
		Description:    current.GetDescription(),
		Spec:           proto.Clone(current.GetSpec()).(*orkstackv1.ScheduleSpec),
	}
	if request.Spec == nil {
		request.Spec = &orkstackv1.ScheduleSpec{}
	}
	if err := spec.apply(cmd, request.Spec); err != nil {
		return nil, err
	}
	switch {
	case followCurrent:
		request.OrkflowVersion = ""
	case cmd.Flags().Changed("version"):
		request.OrkflowVersion = version
	}
	switch {
	case clearInput:
		request.Input = nil
	case len(inputs) > 0 || inputJSON != "":
		input, err := buildInputPayload(inputs, inputJSON)
		if err != nil {
			return nil, err
		}
		request.Input = input
	}
	if cmd.Flags().Changed("description") {
		request.Description = description
	}
	return request, nil
}

func newSchedulePauseCommand(g *globalFlags) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "pause NAME",
		Short: "Stop a schedule's fires without deleting anything",
		Long: "pause stops fires until 'orkctl schedule resume'. The reason is kept as\n" +
			"the schedule's note and shown on every read. 'schedule fire' still works\n" +
			"on a paused schedule.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().PauseSchedule(ctx, connect.NewRequest(&orkstackv1.PauseScheduleRequest{
				Name: args[0], Namespace: g.namespace, Reason: reason,
			}))
			if err != nil {
				return rpcError("pause schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Paused schedule %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why the schedule is paused (at most 256 bytes)")
	return cmd
}

func newScheduleResumeCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "resume NAME",
		Short: "Let a paused schedule fire again",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().ResumeSchedule(ctx, connect.NewRequest(&orkstackv1.ResumeScheduleRequest{
				Name: args[0], Namespace: g.namespace,
			}))
			if err != nil {
				return rpcError("resume schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Resumed schedule %s\n", args[0])
			return nil
		},
	}
}

// fireOverride reads an optional --overlap for fire and backfill: absent is
// UNSPECIFIED, which the server hands on as "no override".
func fireOverride(cmd *cobra.Command, slug string) (orkstackv1.OverlapPolicy, error) {
	if !cmd.Flags().Changed("overlap") {
		return orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, nil
	}
	return parseOverlapPolicy(slug)
}

const overrideHelp = "override the schedule's overlap policy for this request: %s " +
	"(default: none, the schedule's own policy applies)"

func newScheduleFireCommand(g *globalFlags) *cobra.Command {
	var overlap string
	cmd := &cobra.Command{
		Use:   "fire NAME",
		Short: "Fire a schedule once, now",
		Long: "fire starts one fire now, as if its time had come, even while the\n" +
			"schedule is paused. Without --overlap the schedule's own overlap policy\n" +
			"applies. The run is not known when this returns: it appears in\n" +
			"'orkctl schedule get NAME' once prepared.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			policy, err := fireOverride(cmd, overlap)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().FireSchedule(ctx, connect.NewRequest(&orkstackv1.FireScheduleRequest{
				Name: args[0], Namespace: g.namespace, Overlap: policy,
			}))
			if err != nil {
				return rpcError("fire schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Fire requested for schedule %s. Its run appears in 'orkctl schedule get %s' "+
				"once prepared and in 'orkctl run list --filter origin.kind=SCHEDULE --filter origin.id=%s'.\n",
				args[0], args[0], orDash(response.Msg.GetSchedule().GetId()))
			return nil
		},
	}
	cmd.Flags().StringVar(&overlap, "overlap", "", fmt.Sprintf(overrideHelp, strings.Join(overlapPolicySlugs(), "|")))
	return cmd
}

func newScheduleBackfillCommand(g *globalFlags) *cobra.Command {
	var start, end, overlap string
	cmd := &cobra.Command{
		Use:   "backfill NAME --start RFC3339 --end RFC3339",
		Short: "Fire once for every time the schedule names in a past window",
		Long: "backfill fires once for every nominal time the schedule's spec names in\n" +
			"[--start, --end], as if that time passed now. Each run's origin reference\n" +
			"is its nominal time. Without --overlap the schedule's own overlap policy\n" +
			"applies. The window is at most 366 days.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			request, err := buildScheduleBackfillRequest(cmd, g.namespace, args[0], start, end, overlap)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().BackfillSchedule(ctx, connect.NewRequest(request))
			if err != nil {
				return rpcError("backfill schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Backfill requested for schedule %s from %s to %s. Its runs appear in "+
				"'orkctl schedule get %s' once prepared and in 'orkctl run list --filter origin.kind=SCHEDULE --filter origin.id=%s'.\n",
				args[0], formatTime(request.GetStart()), formatTime(request.GetEnd()),
				args[0], orDash(response.Msg.GetSchedule().GetId()))
			return nil
		},
	}
	cmd.Flags().StringVar(&start, "start", "", "first nominal time to fire for, RFC3339 (required)")
	cmd.Flags().StringVar(&end, "end", "", "last nominal time to fire for, RFC3339 (required)")
	cmd.Flags().StringVar(&overlap, "overlap", "", fmt.Sprintf(overrideHelp, strings.Join(overlapPolicySlugs(), "|")))
	return cmd
}

// buildScheduleBackfillRequest parses the window and the optional override.
func buildScheduleBackfillRequest(cmd *cobra.Command, namespace, name, start, end, overlap string) (*orkstackv1.BackfillScheduleRequest, error) {
	if start == "" || end == "" {
		return nil, fmt.Errorf("--start and --end are both required")
	}
	startTS, err := parseRFC3339Bound("--start", start)
	if err != nil {
		return nil, err
	}
	endTS, err := parseRFC3339Bound("--end", end)
	if err != nil {
		return nil, err
	}
	policy, err := fireOverride(cmd, overlap)
	if err != nil {
		return nil, err
	}
	return &orkstackv1.BackfillScheduleRequest{
		Name: name, Namespace: namespace, Start: startTS, End: endTS, Overlap: policy,
	}, nil
}

func newScheduleDeleteCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME",
		Short: "Delete a schedule",
		Long: "delete removes the schedule from the cluster. Runs it already started\n" +
			"continue; they keep their origin.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			response, err := g.scheduleClient().DeleteSchedule(ctx, connect.NewRequest(&orkstackv1.DeleteScheduleRequest{
				Name: args[0], Namespace: g.namespace,
			}))
			if err != nil {
				return rpcError("delete schedule", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), response.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted schedule %s\n", args[0])
			return nil
		},
	}
}

// ---- rendering ----

// shortDuration spells a duration the way an operator types one: 1h, 15m,
// 1h30m, 30s — never 1h0m0s.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// whenLabel is a spec's WHEN cell: cron expressions joined with " | ", then
// "every <interval>", then the zone in parentheses.
func whenLabel(spec *orkstackv1.ScheduleSpec, sep string) string {
	if spec == nil {
		return dash
	}
	parts := append([]string(nil), spec.GetCron()...)
	if iv := spec.GetInterval(); iv != nil {
		parts = append(parts, "every "+shortDuration(iv.AsDuration()))
	}
	if len(parts) == 0 {
		return dash
	}
	label := strings.Join(parts, " | ")
	if tz := spec.GetTimezone(); tz != "" && len(spec.GetCron()) > 0 {
		label += sep + "(" + tz + ")"
	}
	return label
}

// scheduleState is the STATE cell.
func scheduleState(s *orkstackv1.Schedule) string {
	switch {
	case s.GetUnavailableReason() != "":
		return "unavailable"
	case s.GetPaused() && s.GetNote() != "":
		return "paused: " + s.GetNote()
	case s.GetPaused():
		return "paused"
	default:
		return "active"
	}
}

// lastRunLabel is the LAST RUN cell: the newest fire's run, "refused", or "-".
func lastRunLabel(s *orkstackv1.Schedule) string {
	fires := s.GetRecentFires()
	if len(fires) == 0 {
		return dash
	}
	newest := fires[len(fires)-1]
	switch {
	case newest.GetRunId() != "":
		return newest.GetRunId()
	case newest.GetRefusal() != "":
		return "refused"
	default:
		return dash
	}
}

func renderScheduleList(out io.Writer, list []*orkstackv1.Schedule) error {
	if len(list) == 0 {
		fmt.Fprintln(out, "No schedules.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tORKFLOW\tVERSION\tWHEN\tSTATE\tNEXT\tLAST RUN\tID")
	for _, s := range list {
		version, when, next := dash, dash, dash
		if s.GetUnavailableReason() == "" {
			version = triggerVersionLabel(s.GetOrkflowVersion())
			when = whenLabel(s.GetSpec(), " ")
			if times := s.GetNextFireTimes(); len(times) > 0 {
				next = formatTime(times[0])
			}
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			s.GetName(), orDash(s.GetOrkflowName()), version, when, scheduleState(s), next, lastRunLabel(s), orDash(s.GetId()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// inputLabel renders a schedule's input as one compact JSON object.
func inputLabel(input *orkstackv1.Payload) string {
	if input.GetData() == nil || len(input.GetData().GetFields()) == 0 {
		return dash
	}
	encoded, err := json.Marshal(input.GetData().AsMap())
	if err != nil {
		return dash
	}
	return string(encoded)
}

// localFireTime is a nominal fire time in the schedule's own zone.
func localFireTime(ts *timestamppb.Timestamp, zone string) string {
	if zone == "" {
		return ""
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return ""
	}
	return "  (" + ts.AsTime().In(loc).Format("2006-01-02 15:04") + " " + zone + ")"
}

func renderSchedule(out io.Writer, s *orkstackv1.Schedule) error {
	line := func(label, value string) { fmt.Fprintf(out, "%-14s%s\n", label+":", value) }
	line("Schedule", fmt.Sprintf("%s (%s)", s.GetName(), orDash(s.GetId())))
	line("Namespace", orDash(s.GetNamespace()))
	if reason := s.GetUnavailableReason(); reason != "" {
		line("Runs", orDash(s.GetOrkflowName()))
		line("State", "unavailable — "+reason)
		return nil
	}
	spec := s.GetSpec()
	line("Runs", s.GetOrkflowName()+"@"+triggerVersionLabel(s.GetOrkflowVersion()))
	line("Description", orDash(s.GetDescription()))
	line("When", whenLabel(spec, "  "))
	line("Overlap", overlapLabel(spec.GetOverlap()))
	if cw := spec.GetCatchupWindow(); cw != nil {
		line("Catch-up", shortDuration(cw.AsDuration()))
	}
	if spec.GetPauseOnFailure() {
		line("On failure", "pause")
	}
	if j := spec.GetJitter(); j != nil && j.AsDuration() > 0 {
		line("Jitter", shortDuration(j.AsDuration()))
	}
	if spec.GetStartAt() != nil || spec.GetEndAt() != nil {
		line("Window", formatTime(spec.GetStartAt())+" – "+formatTime(spec.GetEndAt()))
	}
	state := "active"
	if s.GetPaused() {
		state = "paused"
		if note := s.GetNote(); note != "" {
			state += " — " + note
		}
	}
	line("State", state)
	line("Fires", fmt.Sprintf("%d taken, %d skipped (overlap), %d missed (catch-up window)",
		s.GetFiresTaken(), s.GetFiresSkippedOverlap(), s.GetFiresMissedCatchup()))
	line("Created", formatTime(s.GetCreatedAt())+"    Updated: "+formatTime(s.GetUpdatedAt()))
	line("Input", inputLabel(s.GetInput()))

	fmt.Fprintln(out, "\nNext fires:")
	if len(s.GetNextFireTimes()) == 0 {
		fmt.Fprintln(out, "  -")
	}
	for _, ts := range s.GetNextFireTimes() {
		fmt.Fprintf(out, "  %s%s\n", formatTime(ts), localFireTime(ts, spec.GetTimezone()))
	}

	fmt.Fprintln(out, "\nRecent fires:")
	if len(s.GetRecentFires()) == 0 {
		fmt.Fprintln(out, "  -")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "  SCHEDULED\tSTARTED\tRUN\tREFUSAL")
	for _, fire := range s.GetRecentFires() {
		run := fire.GetRunId()
		switch {
		case !fire.GetDetailsAvailable():
			run = "unavailable"
		case run == "" && fire.GetRefusal() == "":
			run = "preparing"
		}
		fmt.Fprintf(table, "  %s\t%s\t%s\t%s\n",
			formatTime(fire.GetScheduledAt()), formatTime(fire.GetActualAt()), orDash(run), orDash(fire.GetRefusal()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
