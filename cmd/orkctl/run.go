package main

import (
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

func newRunCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Submit and observe orkflow runs",
	}
	cmd.AddCommand(
		newRunSubmitCommand(g),
		newRunPlanCommand(g),
		newRunGetCommand(g),
		newRunListCommand(g),
		newRunWatchCommand(g),
		newRunLogsCommand(g),
		newRunArtifactsCommand(g),
		newRunArtifactCommand(g),
		newRunAttemptsCommand(g),
		newRunEvidenceCommand(g),
		newRunInspectCommand(g),
		newRunDiffCommand(g),
		newRunTimingCommand(g),
		newRunAgainCommand(g),
		newRunCancelCommand(g),
		newRunResolveCommand(g),
	)
	return cmd
}

func parseRFC3339Bound(flag, value string) (*timestamppb.Timestamp, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339: %w", flag, err)
	}
	return timestamppb.New(parsed), nil
}

func newRunSubmitCommand(g *globalFlags) *cobra.Command {
	var (
		inputs         []string
		inputJSON      string
		version        string
		idempotencyKey string
		watch          bool
		allowEmptyPool bool
		noCache        bool
		originRef      string
	)
	cmd := &cobra.Command{
		Use:   "submit <orkflow>",
		Short: "Submit a run of a registered orkflow",
		Long: "submit starts a run of a registered orkflow.\n\n" +
			"Each --input value is parsed as JSON when it is valid JSON (so 3, true,\n" +
			"[1,2] and {\"a\":1} keep their types) and kept as a plain string otherwise.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			payload, err := buildInputPayload(inputs, inputJSON)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			client := g.runClient()
			resp, err := client.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName:    args[0],
				OrkflowVersion: version,
				Input:          payload,
				IdempotencyKey: idempotencyKey,
				Namespace:      g.namespace,
				AllowEmptyPool: allowEmptyPool,
				// Sent whatever the Orkflow declares: the server ignores it
				// when nothing is cached.
				NoCache: noCache,
				// Validation is the server's: the CLI sends what it was given.
				OriginReference: originRef,
			}))
			if err != nil {
				return rpcError("submit run", err)
			}
			cancel()

			run := resp.Msg.GetRun()
			if !watch {
				if g.json {
					return writeJSON(cmd.OutOrStdout(), resp.Msg)
				}
				return renderSubmitted(cmd.OutOrStdout(), g.baseURL(), resp.Msg)
			}
			if !g.json {
				if err := renderSubmitted(cmd.OutOrStdout(), g.baseURL(), resp.Msg); err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout())
			}
			return watchRun(cmd, g, run.GetId(), 0)
		},
	}
	cmd.Flags().StringArrayVar(&inputs, "input", nil,
		"run input as key=value (repeatable); values that parse as JSON keep their JSON type")
	cmd.Flags().StringVar(&inputJSON, "input-json", "",
		"run input as a JSON object; merged under --input values")
	cmd.Flags().StringVar(&version, "version", "", "orkflow version (default: the current one)")
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "",
		"dedupe key for repeated submissions, scoped to this namespace")
	cmd.Flags().BoolVar(&watch, "watch", false, "stream events until the run reaches a terminal state")
	cmd.Flags().BoolVar(&allowEmptyPool, "allow-empty-pool", false, "submit even when the selected pool has no live Orkers")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, noCacheHelp)
	cmd.Flags().StringVar(&originRef, "origin-ref", "", originRefHelp)
	return cmd
}

// noCacheHelp is the --no-cache help, shared by `run submit` and `run again`.
const noCacheHelp = "ignore every memo for this run: each cached task runs its container and refreshes its memo"

// originRefHelp is the --origin-ref help, shared by `run submit` and `run again`.
const originRefHelp = "correlation reference recorded on the run, e.g. a CI job URL or a ticket; at most 256 bytes"

func newRunGetCommand(g *globalFlags) *cobra.Command {
	var withAttempts, withTiming, tree bool
	var treeDepth int
	var expand []string
	cmd := &cobra.Command{
		Use:   "get <run-id>",
		Short: "Show one run",
		Long: "get shows one run of the namespace this command acts in. A run id\n" +
			"addresses a run inside a namespace, so a run of another one is not found\n" +
			"here even when its id is known.\n\n" +
			"The task table shows each task's FINAL outcome, which is all Temporal\n" +
			"keeps: a try that failed and was retried away is not in it. --attempts\n" +
			"adds the per-attempt timeline underneath, the same table\n" +
			"`orkctl run attempts` prints. --timing adds the wait/run/total table and\n" +
			"each attempt's phases, the same output `orkctl run timing` prints.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if withAttempts && g.json {
				// Two responses cannot share one JSON document, and printing
				// them one after another would hand a script two concatenated
				// objects. The machine-readable timeline has its own command.
				return fmt.Errorf(
					"--attempts is a second response and cannot be added to --json output; "+
						"read the timeline with: %s",
					orkctlCommand(g.namespace, "run", "attempts", args[0], "--json"))
			}
			if withTiming && g.json {
				return fmt.Errorf(
					"--timing is a second response and cannot be added to --json output; "+
						"read the timing with: %s",
					orkctlCommand(g.namespace, "run", "timing", args[0], "--json"))
			}
			if len(expand) > 0 && g.json {
				return fmt.Errorf("--expand only shapes the table; the JSON already carries every item " +
					"under taskRuns[].items")
			}
			if tree && g.json {
				return fmt.Errorf("--tree reads one run per child and cannot be one JSON document; " +
					"read each child with orkctl run get <id> --json (their ids are taskRuns[].child.runId)")
			}
			if cmd.Flags().Changed("depth") {
				if !tree {
					return fmt.Errorf("--depth limits --tree; give --tree with it")
				}
				if treeDepth < 1 {
					return fmt.Errorf("--depth must be at least 1, got %d", treeDepth)
				}
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().GetRun(ctx, connect.NewRequest(
				&orkstackv1.GetRunRequest{RunId: args[0], Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("get run", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			for _, taskID := range expand {
				if fanOutRow(resp.Msg.GetRun(), taskID) == nil {
					return fmt.Errorf("task %q is not a fan-out", taskID)
				}
			}
			if err := renderRunWith(cmd.OutOrStdout(), resp.Msg.GetRun(), g.namespace); err != nil {
				return err
			}
			if tree {
				read := func(runID, namespace string) (*orkstackv1.Run, error) {
					ctx, cancel := g.unaryContext(cmd)
					defer cancel()
					child, err := g.runClient().GetRun(ctx, connect.NewRequest(
						&orkstackv1.GetRunRequest{RunId: runID, Namespace: namespace}))
					if err != nil {
						return nil, err
					}
					return child.Msg.GetRun(), nil
				}
				if err := renderRunTree(cmd.OutOrStdout(), resp.Msg.GetRun(), read, 1, treeDepth); err != nil {
					return err
				}
			}
			for _, taskID := range expand {
				if err := renderFanOutItems(cmd.OutOrStdout(), fanOutRow(resp.Msg.GetRun(), taskID)); err != nil {
					return err
				}
			}
			if withAttempts {
				if err := appendRunAttempts(cmd, g, args[0]); err != nil {
					return err
				}
			}
			if withTiming {
				return appendRunTiming(cmd, g, args[0])
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&withAttempts, "attempts", false,
		"also print every attempt of every task, including the ones that were retried away")
	cmd.Flags().BoolVar(&withTiming, "timing", false,
		"also print each task's wait, run and total time and every attempt's phases")
	cmd.Flags().StringArrayVar(&expand, "expand", nil,
		"print every item of this fan-out task (repeatable)")
	cmd.Flags().BoolVar(&tree, "tree", false,
		"also print every child run a composed task started, and theirs, depth first")
	cmd.Flags().IntVar(&treeDepth, "depth", 0,
		"with --tree, stop descending below this level of child runs (default: the whole tree)")
	return cmd
}

// appendRunAttempts prints the attempt timeline under a rendered run.
//
// It is a second RPC, made only when the flag asks for it: `run get` is the
// command an operator types constantly, and the retry history is a read of the
// object store that most of those invocations do not need.
func appendRunAttempts(cmd *cobra.Command, g *globalFlags, runID string) error {
	ctx, cancel := g.unaryContext(cmd)
	defer cancel()

	resp, err := g.runClient().ListTaskAttempts(ctx, connect.NewRequest(
		&orkstackv1.ListTaskAttemptsRequest{RunId: runID, Namespace: g.namespace},
	))
	if err != nil {
		return attemptsError(err)
	}
	fmt.Fprintln(cmd.OutOrStdout())
	return renderAttemptList(cmd.OutOrStdout(), resp.Msg.GetAttempts())
}

func newRunListCommand(g *globalFlags) *cobra.Command {
	var (
		limit       int32
		pageToken   string
		filterTexts *[]string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List runs, newest first",
		Long: "list shows runs of the namespace this command acts in, live and finished,\n" +
			"newest first, narrowed by --filter. When more runs match than one page\n" +
			"holds, the command prints the next page's command.\n\n" +
			"--filter origin.kind=KIND keeps runs by what started them (SUBMIT, TRIGGER,\n" +
			"SCHEDULE, PARENT_RUN or RUN_AGAIN), and --filter origin.id=ID beside it\n" +
			"narrows that to one originating object (a trigger or schedule id, a source\n" +
			"or parent run id). A run started before origins were recorded matches no\n" +
			"origin.kind filter.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
				PageSize:  limit,
				PageToken: pageToken,
				Namespace: g.namespace,
				Filters:   filters,
			}))
			if err != nil {
				return rpcError("list runs", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if len(filters) > 0 && len(resp.Msg.GetRuns()) == 0 {
				fmt.Fprint(cmd.OutOrStdout(), noMatchSentence("runs", filters))
				return nil
			}
			if err := renderRunList(cmd.OutOrStdout(), resp.Msg.GetRuns()); err != nil {
				return err
			}
			if next := resp.Msg.GetNextPageToken(); next != "" {
				// The token is bound to the filters it was minted under, so
				// the next-page command repeats every one of them.
				args := append([]string{"run", "list"}, filterArgs(filters)...)
				if limit != 0 {
					args = append(args, "--limit", fmt.Sprint(limit))
				}
				args = append(args, "--page-token", next)
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "\nNext page: %s\n", orkctlCommand(g.namespace, args...))
				return err
			}
			return nil
		},
	}
	cmd.Flags().Int32Var(&limit, "limit", 0, "maximum number of runs to return (0 = server default)")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token from a previous list")
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListRunsRequest{})
	return cmd
}

func newRunCancelCommand(g *globalFlags) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "cancel <run-id>",
		Short: "Request cancellation of a run",
		Long: "cancel requests cancellation of a run of the namespace this command acts\n" +
			"in. A run of another namespace is not found here: cancelling is a write,\n" +
			"and holding an id is not authority over somebody else's tenant.\n\n" +
			"A failing run's failure handlers are stopped too; the run still ends\n" +
			"FAILED with its original reason.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().CancelRun(ctx, connect.NewRequest(
				&orkstackv1.CancelRunRequest{RunId: args[0], Reason: reason, Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("cancel run", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Cancellation requested for run %s\n", resp.Msg.GetRun().GetId())
			return nil
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "human readable cancellation reason")
	return cmd
}

// renderSubmitted prints the freshly created run, id first, and then each
// await callback the submission disclosed — once, because only a digest of
// each token is kept.
func renderSubmitted(out io.Writer, baseURL string, resp *orkstackv1.SubmitRunResponse) error {
	run := resp.GetRun()
	fmt.Fprintf(out, "Submitted run %s\n", run.GetId())
	table := newTable(out)
	fmt.Fprintf(table, "Orkflow:\t%s\n", run.GetOrkflowName())
	fmt.Fprintf(table, "Version:\t%s\n", run.GetOrkflowVersion())
	fmt.Fprintf(table, "Status:\t%s\n", statusLabel(run.GetStatus()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	renderAwaitCallbacks(out, baseURL, resp.GetAwaitCallbacks())
	return nil
}

// renderRun prints a run detail view with its task runs.
func renderRun(out io.Writer, run *orkstackv1.Run) error {
	return renderRunWith(out, run, "")
}

// renderRunWith is renderRun with the namespace the fan-out hint's command
// names.
func renderRunWith(out io.Writer, run *orkstackv1.Run, namespace string) error {
	header := newTable(out)
	fmt.Fprintf(header, "Run:\t%s\n", run.GetId())
	fmt.Fprintf(header, "Orkflow:\t%s\n", run.GetOrkflowName())
	fmt.Fprintf(header, "Version:\t%s\n", run.GetOrkflowVersion())
	fmt.Fprintf(header, "Status:\t%s\n", runStatusLabel(run))
	fmt.Fprintf(header, "Created:\t%s\n", formatTime(run.GetCreatedAt()))
	fmt.Fprintf(header, "Started:\t%s\n", formatTime(run.GetStartedAt()))
	fmt.Fprintf(header, "Finished:\t%s\n", formatTime(run.GetFinishedAt()))
	writeOriginLines(header, run.GetOrigin())
	fmt.Fprintf(header, "Input:\t%s\n", payloadSummary(run.GetInput()))
	fmt.Fprintf(header, "Output:\t%s\n", payloadSummary(run.GetOutput()))
	if run.GetError() != nil {
		fmt.Fprintf(header, "Error:\t%s\n", errorLine(run.GetError()))
	}
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)

	if len(run.GetTaskRuns()) == 0 {
		fmt.Fprintln(out, "No task runs yet.")
		return nil
	}
	if err := renderTaskRunTable(out, run); err != nil {
		return err
	}
	// One line per expanded fan-out, naming the command that expands it.
	first := true
	for _, taskRun := range run.GetTaskRuns() {
		if !taskRun.GetEach().GetExpanded() {
			continue
		}
		if first {
			fmt.Fprintln(out)
			first = false
		}
		fmt.Fprintln(out, fanOutSummaryLine(namespace, run.GetId(), taskRun))
	}
	// One line per composed task, naming its child run and the command that
	// opens it. A run with no composed task prints nothing new.
	first = true
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetChild() == nil {
			continue
		}
		if first {
			fmt.Fprintln(out)
			first = false
		}
		fmt.Fprintln(out, composedLine(run.GetNamespace(), taskRun))
	}
	if err := renderAwaitTable(out, run, nowFunc()); err != nil {
		return err
	}
	// One line per cached task, after every other footer. A run with no
	// cache on any row prints nothing new.
	renderCacheLines(out, run)
	// The JOIN table, after the cache lines. A run with no join prints
	// nothing new.
	return renderRunJoinTable(out, run)
}

// runStatusLabel is the Status line: RUNNING (failing) for a run whose
// outcome is decided FAILED while its failure handlers still run.
func runStatusLabel(run *orkstackv1.Run) string {
	if run.GetStatus() == orkstackv1.Status_STATUS_RUNNING && run.GetError() != nil {
		return statusLabel(run.GetStatus()) + " (failing)"
	}
	return statusLabel(run.GetStatus())
}

// renderRunJoinTable is one row per task whose TaskRun.join is set, in run
// order.
func renderRunJoinTable(out io.Writer, run *orkstackv1.Run) error {
	var rows []*orkstackv1.TaskRun
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
			rows = append(rows, taskRun)
		}
	}
	if len(rows) == 0 {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "JOIN\tPOLICY")
	for _, taskRun := range rows {
		fmt.Fprintf(table, "%s\t%s\n", taskRun.GetTaskId(), dsl.JoinWord(taskRun.GetJoin()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func renderRunList(out io.Writer, runs []*orkstackv1.Run) error {
	if len(runs) == 0 {
		fmt.Fprintln(out, "No runs.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "ID\tORKFLOW\tVERSION\tORIGIN\tSTATUS\tCREATED\tFINISHED")
	for _, run := range runs {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			run.GetId(), run.GetOrkflowName(), run.GetOrkflowVersion(), originColumn(run.GetOrigin()),
			statusLabel(run.GetStatus()), formatTime(run.GetCreatedAt()), formatTime(run.GetFinishedAt()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// renderTaskRunTable prints a run's task table: the table `run get` shows,
// and the one `run get --tree` indents under each child run.
func renderTaskRunTable(out io.Writer, run *orkstackv1.Run) error {
	table := newTable(out)
	// POOL/PIN/ORKER sit between the identity and the outcome: WHERE a task ran
	// is the question asked right after WHAT it is and HOW it ended. PIN and
	// ORKER together are the client-visible proof a pin held — same pin, same
	// orker. ORKER is filled for every task that reached an Orker, pinned or
	// not; a dash therefore means "never got there", not "not pinned".
	//
	// SCHEDULED is when the engine dispatched the task; STARTED is when the
	// Orker began the attempt that decided it. The gap between them is the
	// wait `orkctl run timing` breaks down.
	fmt.Fprintln(table, "TASK\tSTATUS\tPOOL\tPIN\tORKER\tATTEMPT\tEXIT\tSCHEDULED\tSTARTED\tFINISHED\tERROR")
	for _, taskRun := range run.GetTaskRuns() {
		status := taskStatusLabel(taskRun)
		attempt := fmt.Sprintf("%d", taskRun.GetAttempt())
		exit := fmt.Sprintf("%d", taskRun.GetExitCode())
		if taskRun.GetEach() != nil {
			// A fan-out's own row: its status and settled fraction; the
			// attempt, exit and orker belong to its items.
			status, attempt, exit = fanOutStatusLabel(taskRun), dash, dash
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			taskRun.GetTaskId(), status,
			orDash(taskRun.GetPool()), orDash(taskRun.GetPin()), orDash(taskRun.GetOrkerId()),
			attempt, exit, formatTime(taskRun.GetScheduledAt()),
			formatTime(taskRun.GetStartedAt()),
			formatTime(taskRun.GetFinishedAt()), errorLine(taskRun.GetError()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
