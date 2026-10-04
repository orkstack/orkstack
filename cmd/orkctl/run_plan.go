package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// errPlanVersionWithFile is the sentence the server uses for the same refusal,
// repeated client-side so the command refuses before any RPC.
var errPlanVersionWithFile = errors.New(
	"orkflow_version cannot be set with yaml: a document has the version it pins or derives (drop --version or --file)")

func newRunPlanCommand(g *globalFlags) *cobra.Command {
	var (
		file          string
		version       string
		inputs        []string
		inputJSON     string
		outputs       []string
		outputsFile   string
		assertRuns    []string
		assertSkipped []string
	)
	cmd := &cobra.Command{
		Use:   "plan [<orkflow>]",
		Short: "Show which tasks a run would execute, without starting anything",
		Long: "plan answers, for one input, which tasks of an orkflow would run, which\n" +
			"would be skipped and exactly why, and in which dispatch wave each would\n" +
			"start. The engine evaluates every condition with the interpreter's own\n" +
			"evaluator; nothing is started, stored, or recorded.\n\n" +
			"Plan a registered orkflow by name (--version pins one; the default is the\n" +
			"current one), or an unapplied document with --file.\n\n" +
			"A condition that reads an upstream task's output sees an empty output\n" +
			"unless one is supplied with --output TASK=JSON or --outputs-file PATH.\n\n" +
			"--assert-runs and --assert-skipped make the plan a CI check: a plan that\n" +
			"contradicts them exits 4, distinct from 1 for a plan that could not be\n" +
			"computed. A plan proves wiring, never behaviour, and is not an admission\n" +
			"check: pools, holds, and limits are asked by run submit.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := ""
			if len(args) == 1 {
				name = args[0]
			}
			if (name == "") == (file == "") {
				return errors.New("give exactly one of <orkflow> and --file")
			}
			if file != "" && version != "" {
				return errPlanVersionWithFile
			}
			payload, err := buildInputPayload(inputs, inputJSON)
			if err != nil {
				return err
			}
			assumed, err := buildAssumedOutputs(outputs, outputsFile)
			if err != nil {
				return err
			}
			req := &orkstackv1.PlanRunRequest{
				OrkflowName:    name,
				OrkflowVersion: version,
				Namespace:      g.namespace,
				Input:          payload,
				AssumedOutputs: assumed,
			}
			if file != "" {
				source, err := os.ReadFile(file)
				if err != nil {
					return fmt.Errorf("read orkflow file: %w", err)
				}
				req.Yaml = string(source)
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.orkflowClient().PlanRun(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("plan run", err)
			}
			plan := resp.Msg

			if g.json {
				if err := writeJSON(cmd.OutOrStdout(), plan); err != nil {
					return err
				}
			} else if err := renderPlan(cmd.OutOrStdout(), plan, file); err != nil {
				return err
			}
			return checkPlanAssertions(cmd.ErrOrStderr(), plan, assertRuns, assertSkipped)
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "plan an unapplied orkflow document instead of a registered one")
	cmd.Flags().StringVar(&version, "version", "", "orkflow version (default: the current one)")
	cmd.Flags().StringArrayVar(&inputs, "input", nil,
		"run input as key=value (repeatable); values that parse as JSON keep their JSON type")
	cmd.Flags().StringVar(&inputJSON, "input-json", "",
		"run input as a JSON object; merged under --input values")
	cmd.Flags().StringArrayVar(&outputs, "output", nil,
		"assume a task's values.json as TASK=JSON-object (repeatable); wins over --outputs-file")
	cmd.Flags().StringVar(&outputsFile, "outputs-file", "",
		"a JSON object keyed by task id whose values are the assumed values.json objects")
	cmd.Flags().StringArrayVar(&assertRuns, "assert-runs", nil,
		"exit 4 unless this task is planned to run (repeatable)")
	cmd.Flags().StringArrayVar(&assertSkipped, "assert-skipped", nil,
		"exit 4 unless this task is planned to be skipped (repeatable)")
	return cmd
}

// buildAssumedOutputs turns --outputs-file and repeated --output TASK=JSON
// pairs into PlanRunRequest.assumed_outputs. --output entries are applied
// after the file, so an explicit pair wins over the same key in the file —
// the precedence --input has over --input-json. Every value must be a JSON
// object, as a values.json is.
func buildAssumedOutputs(pairs []string, file string) (map[string]*orkstackv1.Payload, error) {
	values := map[string]map[string]any{}
	if file != "" {
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read --outputs-file %s: %w", file, err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil || decoded == nil {
			if err == nil {
				err = errors.New("not a JSON object")
			}
			return nil, fmt.Errorf("invalid --outputs-file %s: want a JSON object keyed by task id: %w", file, err)
		}
		keys := make([]string, 0, len(decoded))
		for k := range decoded {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, id := range keys {
			object, ok := decoded[id].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid --outputs-file %s: value for task %q must be a JSON object", file, id)
			}
			values[id] = object
		}
	}
	for _, pair := range pairs {
		id, raw, found := strings.Cut(pair, "=")
		if !found || id == "" {
			return nil, fmt.Errorf("invalid --output %q: expected TASK=JSON", pair)
		}
		var object map[string]any
		if err := json.Unmarshal([]byte(raw), &object); err != nil || object == nil {
			return nil, fmt.Errorf("invalid --output %q: value must be a JSON object", pair)
		}
		values[id] = object
	}
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]*orkstackv1.Payload, len(values))
	for id, object := range values {
		data, err := structpb.NewStruct(object)
		if err != nil {
			return nil, fmt.Errorf("assumed output for task %q: %w", id, err)
		}
		out[id] = &orkstackv1.Payload{Data: data}
	}
	return out, nil
}

// planOutcomeWord is the lower-case OUTCOME column word.
func planOutcomeWord(outcome orkstackv1.PlanOutcome) string {
	return strings.ToLower(planOutcomeLabel(outcome))
}

// planOutcomeLabel is the short upper-case name the JSON dialect uses.
func planOutcomeLabel(outcome orkstackv1.PlanOutcome) string {
	return strings.TrimPrefix(outcome.String(), "PLAN_OUTCOME_")
}

// planFailing returns the FAILS task, nil when the plan has none.
func planFailing(plan *orkstackv1.PlanRunResponse) *orkstackv1.PlanTask {
	for _, task := range plan.GetTasks() {
		if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS {
			return task
		}
	}
	return nil
}

// planRows orders tasks by wave then declaration order, with the wave-0
// (FAILS and UNREACHED) rows last. ui/src/lib/plan.ts planRows mirrors it.
func planRows(plan *orkstackv1.PlanRunResponse) []*orkstackv1.PlanTask {
	rows := append([]*orkstackv1.PlanTask(nil), plan.GetTasks()...)
	sort.SliceStable(rows, func(i, j int) bool {
		wi, wj := rows[i].GetWave(), rows[j].GetWave()
		if (wi == 0) != (wj == 0) {
			return wj == 0
		}
		return wi < wj
	})
	return rows
}

// planDetail is the DETAIL cell for one task.
func planDetail(task *orkstackv1.PlanTask, failing string) string {
	switch task.GetOutcome() {
	case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
		return task.GetSkipReason()
	case orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS:
		if task.GetEach() != nil {
			// A fan-out is one PlanTask (spec each-fan-out §5.12).
			return eachDetail(task)
		}
		if task.GetPin() != "" {
			return "pin " + task.GetPin()
		}
	case orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS:
		return awaitPlanDetail(task.GetAwait())
	case orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS:
		return task.GetError().GetMessage()
	case orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED:
		return "after " + failing + " fails"
	}
	return ""
}

// planSummary is the one-line summary. ui/src/lib/plan.ts planSummary
// renders the same sentence.
//
// The await count is inserted after the run count only when the plan has an
// await, so every plan without one reads exactly as it always did.
func planSummary(plan *orkstackv1.PlanRunResponse) string {
	awaits := ""
	if plan.GetAwaits() > 0 {
		awaits = plural(int64(plan.GetAwaits()), "await") + ", "
	}
	return fmt.Sprintf("%d run, %s%d skipped, %s. Nothing was started.",
		plan.GetRuns(), awaits, plan.GetSkipped(), plural(int64(plan.GetWaveCount()), "wave"))
}

// awaitPlanDetail is the DETAIL cell of an AWAITS task: its deadline and
// whether it takes a callback. ui/src/lib/plan.ts renders the same words.
func awaitPlanDetail(spec *orkstackv1.AwaitSpec) string {
	detail := "await ≤ " + (time.Duration(spec.GetTimeoutSeconds()) * time.Second).String()
	if spec.GetCallback() {
		detail += ", callback"
	}
	return detail
}

// planFailureSentence is the FAILS sentence, empty when nothing fails.
func planFailureSentence(plan *orkstackv1.PlanRunResponse) string {
	failing := planFailing(plan)
	if failing == nil {
		return ""
	}
	var unreached int64
	for _, task := range plan.GetTasks() {
		if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED {
			unreached++
		}
	}
	return fmt.Sprintf("The run would FAIL deciding %s: %s. %s unreached; "+
		"tasks planned to run in the same or a later wave may be cancelled in flight.",
		failing.GetTaskId(), failing.GetError().GetMessage(), plural(unreached, "task"))
}

// planAbsentSentence names the tasks whose condition read an upstream output
// the request did not supply, empty when there are none.
func planAbsentSentence(plan *orkstackv1.PlanRunResponse) string {
	var ids []string
	for _, task := range plan.GetTasks() {
		if len(task.GetAssumedAbsent()) > 0 {
			ids = append(ids, task.GetTaskId())
		}
	}
	if len(ids) == 0 {
		return ""
	}
	verb := "conditions read"
	if len(ids) == 1 {
		verb = "condition reads"
	}
	return fmt.Sprintf("%d %s upstream outputs that were not supplied (planned as {}): %s.",
		len(ids), verb, strings.Join(ids, ", "))
}

// planUnusedSentence names assumed outputs whose task did not run.
func planUnusedSentence(plan *orkstackv1.PlanRunResponse) string {
	if len(plan.GetUnusedAssumedOutputs()) == 0 {
		return ""
	}
	return fmt.Sprintf("Assumed outputs for tasks that do not run were ignored: %s.",
		strings.Join(plan.GetUnusedAssumedOutputs(), ", "))
}

// renderPlan prints the human plan (docs: run-plan spec §5.2).
func renderPlan(out io.Writer, plan *orkstackv1.PlanRunResponse, file string) error {
	var buf bytes.Buffer
	header := fmt.Sprintf("PLAN  %s@%s  namespace %s", plan.GetOrkflowName(), plan.GetOrkflowVersion(), plan.GetNamespace())
	if plan.GetFromYaml() {
		header += fmt.Sprintf("  (from %s, not applied)", file)
	}
	input, err := dsl.CanonicalInputLine(plan.GetInput().GetData())
	if err != nil {
		return fmt.Errorf("render plan input: %w", err)
	}
	fmt.Fprintln(&buf, header)
	fmt.Fprintf(&buf, "definition  %s\n", plan.GetDefinitionDigest())
	fmt.Fprintf(&buf, "input       %s\n", input)
	fmt.Fprintln(&buf)

	failing := ""
	if task := planFailing(plan); task != nil {
		failing = task.GetTaskId()
	}
	table := newTable(&buf)
	fmt.Fprintln(table, "WAVE\tTASK\tOUTCOME\tDETAIL")
	for _, task := range planRows(plan) {
		wave := dash
		if task.GetWave() > 0 {
			wave = fmt.Sprint(task.GetWave())
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", wave, task.GetTaskId(),
			planOutcomeWord(task.GetOutcome()), planDetail(task, failing))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, planSummary(plan))
	if sentence := planFailureSentence(plan); sentence != "" {
		fmt.Fprintln(&buf, sentence)
	}
	if sentence := planAbsentSentence(plan); sentence != "" {
		fmt.Fprintln(&buf, sentence)
		fmt.Fprintln(&buf, `  Supply them with --output TASK='{"…"}' or --outputs-file PATH.`)
	}
	if sentence := planUnusedSentence(plan); sentence != "" {
		fmt.Fprintln(&buf, sentence)
	}

	// The table's last column is left-aligned free text; an empty DETAIL
	// would otherwise leave padding at the end of its line.
	lines := strings.Split(buf.String(), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " ")
	}
	if _, err := io.WriteString(out, strings.Join(lines, "\n")); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// checkPlanAssertions checks --assert-runs/--assert-skipped against a plan.
// A task the plan does not contain is an error (exit 1); an outcome that
// contradicts an assertion is one stderr line each and exit 4.
func checkPlanAssertions(errOut io.Writer, plan *orkstackv1.PlanRunResponse, runs, skipped []string) error {
	if len(runs) == 0 && len(skipped) == 0 {
		return nil
	}
	byID := map[string]*orkstackv1.PlanTask{}
	for _, task := range plan.GetTasks() {
		byID[task.GetTaskId()] = task
	}
	failing := ""
	if task := planFailing(plan); task != nil {
		failing = task.GetTaskId()
	}
	var failures []string
	check := func(flag string, ids []string, want orkstackv1.PlanOutcome) error {
		for _, id := range ids {
			task, ok := byID[id]
			if !ok {
				return fmt.Errorf("%s: task %q is not in this orkflow", flag, id)
			}
			if task.GetOutcome() == want {
				continue
			}
			// The interpreter starts an awaited task as surely as a
			// container one, so --assert-runs holds for AWAITS too.
			if want == orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS &&
				task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS {
				continue
			}
			line := fmt.Sprintf("%s %s: planned %s", flag, id, planOutcomeLabel(task.GetOutcome()))
			if detail := planDetail(task, failing); detail != "" {
				line += " (" + detail + ")"
			}
			failures = append(failures, line)
		}
		return nil
	}
	if err := check("assert-runs", runs, orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS); err != nil {
		return err
	}
	if err := check("assert-skipped", skipped, orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED); err != nil {
		return err
	}
	if len(failures) == 0 {
		return nil
	}
	for _, line := range failures {
		fmt.Fprintln(errOut, line)
	}
	return &exitCodeError{
		code: exitPlanAssertionFailed,
		err:  fmt.Errorf("%s contradicted by the plan", plural(int64(len(failures)), "assertion")),
	}
}
