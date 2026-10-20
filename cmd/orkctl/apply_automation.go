package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/dsl"
)

// applyOptions are apply's flags beyond the global ones.
type applyOptions struct {
	dryRun     bool
	check      bool
	prune      bool
	expectPlan string
	secretsOut string
}

// documentKind is what a file given to apply turned out to be.
type documentKind int

const (
	orkflowDocument documentKind = iota
	automationDocument
)

// applyInput is one file apply read.
type applyInput struct {
	path   string
	source []byte
	kind   documentKind
	// namespace is the document's own namespace: — resolved ("default"
	// spelled out) for an automation document, as written (maybe empty) for
	// an orkflow.
	namespace string
}

// classifyDocument peeks a file's top-level keys with a non-strict decode
// and reads its kind: explicitly: kind: Orkflow is an orkflow document,
// kind: Automation an automation document, and a file with no kind, or any
// other, is refused here, before anything is sent. The engine checks each
// kind's apiVersion; YAML that does not parse is the engine's to explain,
// with a line, through ValidateOrkflow.
func classifyDocument(path string, source []byte) (applyInput, error) {
	var peek struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Namespace  string `yaml:"namespace"`
	}
	in := applyInput{path: path, source: source, kind: orkflowDocument}
	if err := yaml.Unmarshal(source, &peek); err != nil {
		return in, nil
	}
	switch {
	case peek.Kind == dsl.KindOrkflow:
		in.namespace = peek.Namespace
		return in, nil
	case peek.Kind == dsl.KindAutomation:
		in.kind = automationDocument
		in.namespace = peek.Namespace
		if in.namespace == "" {
			in.namespace = "default"
		}
		return in, nil
	case peek.Kind == "" && peek.APIVersion == "":
		return in, fmt.Errorf("%s: no apiVersion or kind: an OrkStack document begins with %q and its kind — "+
			"%q for an Orkflow, %q for an automation document; add the two lines at the top, after any leading comments",
			path, "apiVersion: "+dsl.APIVersion, "kind: "+dsl.KindOrkflow, "kind: "+dsl.KindAutomation)
	case peek.Kind == "":
		return in, fmt.Errorf("%s: kind is required: add %q (or %q) below %q",
			path, "kind: "+dsl.KindOrkflow, "kind: "+dsl.KindAutomation, "apiVersion: "+dsl.APIVersion)
	default:
		return in, fmt.Errorf("%s: unknown document kind %q: want kind: %s or kind: %s",
			path, peek.Kind, dsl.KindOrkflow, dsl.KindAutomation)
	}
}

// expandApplyArgs reads every argument: a file as is, a directory as its
// regular *.ork.yaml and *.ork.yml files, non-recursively, in lexical order.
func expandApplyArgs(args []string) ([]applyInput, error) {
	var inputs []applyInput
	for _, arg := range args {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", arg, err)
		}
		paths := []string{arg}
		if info.IsDir() {
			entries, err := os.ReadDir(arg)
			if err != nil {
				return nil, fmt.Errorf("read directory %s: %w", arg, err)
			}
			paths = nil
			for _, entry := range entries {
				name := entry.Name()
				if !entry.Type().IsRegular() || !(strings.HasSuffix(name, ".ork.yaml") || strings.HasSuffix(name, ".ork.yml")) {
					continue
				}
				paths = append(paths, filepath.Join(arg, name))
			}
			sort.Strings(paths)
			if len(paths) == 0 {
				return nil, fmt.Errorf("%s: no *.ork.yaml or *.ork.yml files in this directory", arg)
			}
		}
		for _, path := range paths {
			source, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", path, err)
			}
			in, err := classifyDocument(path, source)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, in)
		}
	}
	return inputs, nil
}

// applyRun is one invocation's state, in the shape `--json` prints.
type applyRun struct {
	g      *globalFlags
	cmd    *cobra.Command
	opts   applyOptions
	report *orkstackv1.ApplyReport
	// sources are the orkflow documents' bytes by file, sent unchanged.
	sources map[string][]byte
	// refusals are why nothing may be written; notes are dry-run remarks.
	refusals []string
	notes    map[int][]string // automation document index → extra lines
	// secretsFile is the --secrets-out file, created (exclusive, 0600)
	// before the first write so an uncreatable path refuses the whole
	// apply; it is filled once every step has run.
	secretsFile *os.File
	// secretsWritten is true once the disclosed secrets are in secretsFile;
	// only then are they withheld from the output.
	secretsWritten bool
}

// runApplyDocuments is apply for anything but the one shape that existed
// before automation documents: Orkflow documents first, then each
// namespace's plan, nothing written until every document has been read,
// validated and planned.
func runApplyDocuments(cmd *cobra.Command, g *globalFlags, args []string, opts applyOptions) error {
	if opts.check {
		opts.dryRun = true
	}
	if opts.secretsOut != "" && opts.dryRun {
		return fmt.Errorf("--secrets-out writes the secrets an apply discloses; a dry run discloses none")
	}
	inputs, err := expandApplyArgs(args)
	if err != nil {
		return err
	}
	seen := map[string]string{}
	for _, in := range inputs {
		if in.kind != automationDocument {
			continue
		}
		if other, dup := seen[in.namespace]; dup {
			return fmt.Errorf("%s and %s both describe namespace %q: one automation document per namespace; nothing was written",
				other, in.path, in.namespace)
		}
		seen[in.namespace] = in.path
	}

	run := &applyRun{g: g, cmd: cmd, opts: opts, report: &orkstackv1.ApplyReport{DryRun: opts.dryRun},
		notes: map[int][]string{}, sources: map[string][]byte{}}
	if err := run.validateOrkflows(inputs); err != nil {
		return err
	}
	if err := run.planAutomation(inputs); err != nil {
		return err
	}
	run.checkRefusals()
	if len(run.refusals) == 0 && !opts.dryRun && opts.secretsOut != "" {
		// Claim the file before anything is written: a path that cannot be
		// created must refuse the apply, not lose the secrets after it.
		f, err := os.OpenFile(opts.secretsOut, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			run.refusals = append(run.refusals, fmt.Sprintf("--secrets-out %s cannot be created: %v", opts.secretsOut, err))
		} else {
			run.secretsFile = f
			defer func() { _ = f.Close() }()
		}
	}

	out := cmd.OutOrStdout()
	if len(run.refusals) > 0 {
		if g.json {
			if err := writeJSON(out, run.report); err != nil {
				return err
			}
		} else {
			run.renderPlan(out)
			for _, refusal := range run.refusals {
				fmt.Fprintln(out, refusal)
			}
			fmt.Fprintln(out, "Nothing was written.")
		}
		return &exitCodeError{code: exitError, err: errors.New("apply refused: nothing was written")}
	}

	if opts.dryRun {
		if g.json {
			if err := writeJSON(out, run.report); err != nil {
				return err
			}
		} else {
			run.renderPlan(out)
			fmt.Fprintln(out, "Dry run: nothing was written.")
		}
		if opts.check && run.hasChanges() {
			return &exitCodeError{code: exitApplyCheckChanges, err: errors.New("apply --check: the documents differ from what is stored")}
		}
		return nil
	}

	if !g.json {
		run.renderPlan(out)
	}
	failed := run.applyOrkflows(out)
	var disclosed []*orkstackv1.DisclosedSecret
	if !failed {
		disclosed, failed = run.executeSteps()
	} else {
		run.markAllNotRun()
	}
	var writeErr error
	if run.secretsFile != nil {
		writeErr = writeDisclosedSecrets(run.secretsFile, disclosed)
		if writeErr != nil {
			// The secrets are printed below instead; a partial file must not
			// look like them.
			_ = run.secretsFile.Close()
			_ = os.Remove(opts.secretsOut)
		} else {
			run.secretsWritten = true
			run.redactSecrets()
		}
	}
	if g.json {
		if err := writeJSON(out, run.report); err != nil {
			return err
		}
	} else {
		run.renderResults(out, disclosed)
	}
	if writeErr != nil {
		return fmt.Errorf("write --secrets-out %s: %w (the secrets printed above are the only copy)", opts.secretsOut, writeErr)
	}
	if failed {
		return &exitCodeError{code: exitError, err: errors.New("apply failed part-way; run apply again: it plans from what now exists")}
	}
	return nil
}

// validateOrkflows validates every orkflow document and reads what the store
// holds for its compiled name and version.
func (r *applyRun) validateOrkflows(inputs []applyInput) error {
	for _, in := range inputs {
		if in.kind != orkflowDocument {
			continue
		}
		r.sources[in.path] = in.source
		ctx, cancel := r.g.unaryContext(r.cmd)
		validated, err := r.g.orkflowClient().ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
			Yaml: string(in.source), Namespace: r.g.namespace,
		}))
		cancel()
		if err != nil {
			if connect.CodeOf(err) == connect.CodeNotFound {
				if creator := automationFor(inputs, orkflowTarget(in, r.g.namespace)); creator != "" {
					return fmt.Errorf("%s: namespace %q does not exist yet, and an orkflow is validated inside an existing "+
						"namespace; apply %s first (it creates the namespace), then apply the orkflows; nothing was written",
						in.path, orkflowTarget(in, r.g.namespace), creator)
				}
			}
			return rpcError("validate "+in.path, err)
		}
		doc := &orkstackv1.OrkflowDocumentReport{File: in.path, Validated: validated.Msg}
		r.report.Orkflows = append(r.report.Orkflows, doc)
		if automation.HasErrors(validated.Msg.GetIssues()) {
			continue
		}
		wf := validated.Msg.GetOrkflow()
		ctx, cancel = r.g.unaryContext(r.cmd)
		stored, err := r.g.orkflowClient().GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{
			Name: wf.GetName(), Version: wf.GetVersion(), Namespace: orkflowNamespace(wf, r.g.namespace),
		}))
		cancel()
		switch {
		case connect.CodeOf(err) == connect.CodeNotFound:
			doc.Stored = orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_NEW
		case err != nil:
			return rpcError("read "+in.path, err)
		case bytes.Equal([]byte(stored.Msg.GetOrkflow().GetSourceYaml()), in.source):
			// The store's own source rule (pkg/artifacts.SameStoredOrkflow):
			// GetOrkflow returns the stored source verbatim, so this says
			// identical exactly when the apply would be a no-op on the source.
			doc.Stored = orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_IDENTICAL
		default:
			doc.Stored = orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_CONFLICTING
		}
	}
	return nil
}

// orkflowTarget is the namespace an orkflow document is applied into: its
// own namespace:, else the request's, else the default.
func orkflowTarget(in applyInput, requested string) string {
	for _, ns := range []string{in.namespace, requested} {
		if ns != "" {
			return ns
		}
	}
	return "default"
}

// automationFor names the automation document of this invocation that
// describes namespace ns, or "".
func automationFor(inputs []applyInput, ns string) string {
	for _, in := range inputs {
		if in.kind == automationDocument && in.namespace == ns {
			return in.path
		}
	}
	return ""
}

// orkflowNamespace is where an orkflow document lands: its own namespace:,
// or the request's, or the default.
func orkflowNamespace(wf *orkstackv1.Orkflow, requested string) string {
	if ns := wf.GetNamespace(); ns != "" {
		return ns
	}
	return requested
}

// planAutomation plans every automation document, telling each plan which
// orkflow versions this invocation stores first.
func (r *applyRun) planAutomation(inputs []applyInput) error {
	var pending []*orkstackv1.PendingOrkflow
	for _, doc := range r.report.GetOrkflows() {
		switch doc.GetStored() {
		case orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_NEW, orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_IDENTICAL:
			wf := doc.GetValidated().GetOrkflow()
			pending = append(pending, &orkstackv1.PendingOrkflow{
				Namespace: orkflowNamespace(wf, r.g.namespace), Name: wf.GetName(), Version: wf.GetVersion(),
			})
		}
	}
	// The document's namespace: is authoritative; only an -n typed on this
	// command line is sent, so a saved default never refuses another
	// namespace's document in a directory.
	explicit := ""
	if r.cmd.Flags().Changed("namespace") {
		explicit = r.g.namespace
	}
	for _, in := range inputs {
		if in.kind != automationDocument {
			continue
		}
		ctx, cancel := r.g.unaryContext(r.cmd)
		planned, err := r.g.automationClient().PlanAutomation(ctx, connect.NewRequest(&orkstackv1.PlanAutomationRequest{
			Yaml: string(in.source), Namespace: explicit, Prune: r.opts.prune, PendingOrkflows: pending,
		}))
		cancel()
		if connect.CodeOf(err) == connect.CodeUnimplemented {
			return fmt.Errorf("%s: this engine does not serve AutomationService; upgrade the engine", in.path)
		}
		if err != nil {
			return rpcError("plan "+in.path, err)
		}
		r.report.Automation = append(r.report.Automation, &orkstackv1.AutomationDocumentReport{File: in.path, Plan: planned.Msg})
	}
	return nil
}

// checkRefusals collects every reason the invocation must write nothing.
func (r *applyRun) checkRefusals() {
	for _, doc := range r.report.GetOrkflows() {
		if automation.HasErrors(doc.GetValidated().GetIssues()) {
			r.refusals = append(r.refusals, fmt.Sprintf("%s: the orkflow document has errors.", doc.GetFile()))
		}
		if doc.GetStored() == orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_CONFLICTING {
			wf := doc.GetValidated().GetOrkflow()
			r.refusals = append(r.refusals, fmt.Sprintf("%s: version %s of %s is stored with different source; "+
				"ApplyOrkflow would refuse it — bump version: or drop it", doc.GetFile(), wf.GetVersion(), wf.GetName()))
		}
	}
	for _, doc := range r.report.GetAutomation() {
		if automation.HasErrors(doc.GetPlan().GetIssues()) {
			r.refusals = append(r.refusals, fmt.Sprintf("%s: the plan has errors.", doc.GetFile()))
		}
	}
	if r.opts.expectPlan != "" {
		switch documents := r.report.GetAutomation(); {
		case len(documents) != 1:
			r.refusals = append(r.refusals, fmt.Sprintf(
				"--expect-plan pins the plan of exactly one automation document; this invocation has %d", len(documents)))
		case documents[0].GetPlan().GetPlanDigest() == "" || documents[0].GetPlan().GetPlanDigest() != r.opts.expectPlan:
			r.refusals = append(r.refusals, fmt.Sprintf(
				"the plan differs from the one reviewed (%s ≠ %s); nothing was written",
				orDash(documents[0].GetPlan().GetPlanDigest()), r.opts.expectPlan))
		}
	}
	for i, doc := range r.report.GetAutomation() {
		for _, step := range doc.GetPlan().GetSteps() {
			env := step.GetSecretEnv()
			if env == "" || step.GetAction() != orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE || os.Getenv(env) != "" {
				continue
			}
			message := fmt.Sprintf("webhooks.%s.secret_env: $%s is not set; apply needs it to create the subscription", step.GetName(), env)
			if r.opts.dryRun {
				r.notes[i] = append(r.notes[i], "note: "+message)
			} else {
				r.refusals = append(r.refusals, message)
			}
		}
	}
	if r.opts.secretsOut != "" {
		if _, err := os.Lstat(r.opts.secretsOut); err == nil {
			r.refusals = append(r.refusals, fmt.Sprintf("--secrets-out %s already exists; it is never overwritten", r.opts.secretsOut))
		}
	}
}

// hasChanges reports whether applying would write anything: an actionable
// step, or an orkflow version not stored yet.
func (r *applyRun) hasChanges() bool {
	for _, doc := range r.report.GetOrkflows() {
		if doc.GetStored() == orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_NEW {
			return true
		}
	}
	for _, doc := range r.report.GetAutomation() {
		for _, step := range doc.GetPlan().GetSteps() {
			if automation.Actionable(step.GetAction()) {
				return true
			}
		}
	}
	return false
}

// applyOrkflows stores each NEW orkflow version exactly as `apply FILE`
// always has, stopping at the first failure. IDENTICAL ones are skipped: the
// engine would write nothing anyway.
func (r *applyRun) applyOrkflows(out io.Writer) bool {
	for _, doc := range r.report.GetOrkflows() {
		if doc.GetStored() != orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_NEW {
			continue
		}
		source := string(r.sources[doc.GetFile()])
		ctx, cancel := r.g.unaryContext(r.cmd)
		applied, err := r.g.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: source, Namespace: r.g.namespace,
		}))
		cancel()
		if err != nil {
			fmt.Fprintf(r.cmd.ErrOrStderr(), "%v\n", rpcError("apply "+doc.GetFile(), err))
			return true
		}
		doc.Applied = applied.Msg
		if !r.g.json {
			wf := applied.Msg.GetOrkflow()
			fmt.Fprintf(out, "Applied orkflow %s (version %s)\n", wf.GetName(), wf.GetVersion())
		}
	}
	return false
}

// markAllNotRun records every actionable step as not run.
func (r *applyRun) markAllNotRun() {
	for _, doc := range r.report.GetAutomation() {
		for i, step := range doc.GetPlan().GetSteps() {
			if automation.Actionable(step.GetAction()) {
				doc.Results = append(doc.Results, &orkstackv1.AutomationStepResult{
					Step: int32(i), Outcome: orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_NOT_RUN,
				})
			}
		}
	}
}

// executeSteps sends each actionable step's carried request to the RPC it
// names, in plan order, document after document. A DELETE whose object is
// already gone is ALREADY_GONE; any other failure stops everything after it.
func (r *applyRun) executeSteps() ([]*orkstackv1.DisclosedSecret, bool) {
	var disclosed []*orkstackv1.DisclosedSecret
	failed := false
	for _, doc := range r.report.GetAutomation() {
		plan := doc.GetPlan()
		for i, step := range plan.GetSteps() {
			if !automation.Actionable(step.GetAction()) {
				continue
			}
			result := &orkstackv1.AutomationStepResult{Step: int32(i)}
			doc.Results = append(doc.Results, result)
			if failed {
				result.Outcome = orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_NOT_RUN
				continue
			}
			ctx, cancel := r.g.unaryContext(r.cmd)
			id, secret, err := executeStep(ctx, r.g, step)
			cancel()
			switch {
			case err == nil:
				result.Outcome = orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_APPLIED
				result.Id = id
				if result.Id == "" {
					result.Id = step.GetId()
				}
				if secret != "" {
					entry := &orkstackv1.DisclosedSecret{
						Namespace: plan.GetNamespace(), Kind: step.GetKind(), Name: step.GetName(), Id: id, Secret: secret,
					}
					if step.GetKind() == orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER {
						entry.InvokeUrl = strings.TrimRight(r.g.baseURL(), "/") + triggerInvokePath + id
					}
					disclosed = append(disclosed, entry)
					result.Secret = secret
				}
			case step.GetAction() == orkstackv1.AutomationAction_AUTOMATION_ACTION_DELETE && connect.CodeOf(err) == connect.CodeNotFound:
				result.Outcome = orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_ALREADY_GONE
				result.Id = step.GetId()
			default:
				result.Outcome = orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_FAILED
				result.Error = stepError(err)
				failed = true
			}
		}
	}
	return disclosed, failed
}

// stepError is "<connect code>: <message>".
func stepError(err error) string {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return fmt.Sprintf("%s: %s", connectErr.Code(), connectErr.Message())
	}
	return err.Error()
}

// stepExecutor performs one carried request and returns the object's id and,
// for a create that disclosed one, its one-time secret.
type stepExecutor func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (id, secret string, err error)

// stepExecutors has one entry per variant of AutomationStep's request oneof,
// keyed by the oneof field's name. TestApply_ExecutorHandlesEveryStepVariant
// walks the descriptor so a variant added to the proto without an executor
// fails a test rather than an apply.
var stepExecutors = map[protoreflect.Name]stepExecutor{
	"create_namespace": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.namespaceClient().CreateNamespace(ctx, connect.NewRequest(step.GetCreateNamespace()))
		return "", "", err
	},
	"update_namespace": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.namespaceClient().UpdateNamespace(ctx, connect.NewRequest(step.GetUpdateNamespace()))
		return "", "", err
	},
	"create_schedule": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		resp, err := g.scheduleClient().CreateSchedule(ctx, connect.NewRequest(step.GetCreateSchedule()))
		if err != nil {
			return "", "", err
		}
		return resp.Msg.GetSchedule().GetId(), "", nil
	},
	"update_schedule": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.scheduleClient().UpdateSchedule(ctx, connect.NewRequest(step.GetUpdateSchedule()))
		return "", "", err
	},
	"delete_schedule": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.scheduleClient().DeleteSchedule(ctx, connect.NewRequest(step.GetDeleteSchedule()))
		return "", "", err
	},
	"create_trigger": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		resp, err := g.triggerClient().CreateTrigger(ctx, connect.NewRequest(step.GetCreateTrigger()))
		if err != nil {
			return "", "", err
		}
		return resp.Msg.GetTrigger().GetId(), resp.Msg.GetSecret(), nil
	},
	"update_trigger": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.triggerClient().UpdateTrigger(ctx, connect.NewRequest(step.GetUpdateTrigger()))
		return "", "", err
	},
	"delete_trigger": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.triggerClient().DeleteTrigger(ctx, connect.NewRequest(step.GetDeleteTrigger()))
		return "", "", err
	},
	"register_webhook": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		req := proto.Clone(step.GetRegisterWebhook()).(*orkstackv1.RegisterWebhookRequest)
		if env := step.GetSecretEnv(); env != "" {
			// The pre-shared secret enters the engine through this one
			// request and nowhere else: never the document, never the plan.
			req.Secret = os.Getenv(env)
		}
		resp, err := g.webhookClient().RegisterWebhook(ctx, connect.NewRequest(req))
		if err != nil {
			return "", "", err
		}
		if step.GetSecretEnv() != "" {
			// The operator already holds this secret; it is not disclosed.
			return resp.Msg.GetWebhook().GetId(), "", nil
		}
		return resp.Msg.GetWebhook().GetId(), resp.Msg.GetSecret(), nil
	},
	"update_webhook": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.webhookClient().UpdateWebhook(ctx, connect.NewRequest(step.GetUpdateWebhook()))
		return "", "", err
	},
	"delete_webhook": func(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
		_, err := g.webhookClient().DeleteWebhook(ctx, connect.NewRequest(step.GetDeleteWebhook()))
		return "", "", err
	},
}

// executeStep runs the one executor the step's request names.
func executeStep(ctx context.Context, g *globalFlags, step *orkstackv1.AutomationStep) (string, string, error) {
	message := step.ProtoReflect()
	field := message.WhichOneof(message.Descriptor().Oneofs().ByName("request"))
	if field == nil {
		return "", "", fmt.Errorf("step %s %s %q carries no request", step.GetAction(), step.GetKind(), step.GetName())
	}
	executor, ok := stepExecutors[field.Name()]
	if !ok {
		return "", "", fmt.Errorf("this orkctl cannot execute a %s step; upgrade orkctl", field.Name())
	}
	return executor(ctx, g, step)
}

// secretsFileWrite writes the encoded secrets; a variable so a test can
// make the late write fail.
var secretsFileWrite = func(f *os.File, encoded []byte) error {
	_, err := f.Write(encoded)
	return err
}

// writeDisclosedSecrets fills the --secrets-out file runApplyDocuments
// created (mode 0600, exclusive) before the first write, and closes it.
func writeDisclosedSecrets(f *os.File, secrets []*orkstackv1.DisclosedSecret) error {
	encoded, err := marshalJSON(&orkstackv1.DisclosedSecrets{Secrets: secrets})
	if err != nil {
		return err
	}
	if err := secretsFileWrite(f, append(encoded, '\n')); err != nil {
		return err
	}
	return f.Close()
}

// redactSecrets clears each result's one-time secret once it is safely in
// the --secrets-out file, so the report does not repeat it.
func (r *applyRun) redactSecrets() {
	for _, doc := range r.report.GetAutomation() {
		for _, result := range doc.GetResults() {
			result.Secret = ""
		}
	}
}

// ---------------------------------------------------------------------------
// human output
// ---------------------------------------------------------------------------

// renderPlan prints every document: orkflows first, then each automation
// document's issues and steps, then the counts and the digests.
func (r *applyRun) renderPlan(out io.Writer) {
	for _, doc := range r.report.GetOrkflows() {
		fmt.Fprintln(out, doc.GetFile())
		for _, issue := range doc.GetValidated().GetIssues() {
			fmt.Fprintf(out, "%s %s: %s\n", severityLabel(issue.GetSeverity()), orDash(issue.GetField()), issue.GetMessage())
		}
		wf := doc.GetValidated().GetOrkflow()
		symbol, state := " ", ""
		switch doc.GetStored() {
		case orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_NEW:
			symbol, state = "+", "new version"
		case orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_IDENTICAL:
			state = "unchanged"
		case orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_CONFLICTING:
			symbol, state = "!", "stored with different source"
		default:
			state = "not validated"
		}
		if wf != nil {
			fmt.Fprintf(out, "%s %-10s %-18s %s  %s\n", symbol, "orkflow", wf.GetName(), wf.GetVersion(), state)
		}
	}
	for i, doc := range r.report.GetAutomation() {
		if i > 0 || len(r.report.GetOrkflows()) > 0 {
			fmt.Fprintln(out)
		}
		renderAutomationPlan(out, doc.GetFile(), doc.GetPlan())
		for _, note := range r.notes[i] {
			fmt.Fprintln(out, note)
		}
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, r.countsLine())
	for _, doc := range r.report.GetAutomation() {
		if digest := doc.GetPlan().GetPlanDigest(); digest != "" {
			if len(r.report.GetAutomation()) > 1 {
				fmt.Fprintf(out, "Plan digest: %s  (%s)\n", digest, doc.GetFile())
			} else {
				fmt.Fprintf(out, "Plan digest: %s\n", digest)
			}
		}
	}
}

func (r *applyRun) countsLine() string {
	created, updated, deleted, stored := 0, 0, 0, 0
	for _, doc := range r.report.GetOrkflows() {
		if doc.GetStored() == orkstackv1.OrkflowStoredState_ORKFLOW_STORED_STATE_NEW {
			stored++
		}
	}
	for _, doc := range r.report.GetAutomation() {
		for _, step := range doc.GetPlan().GetSteps() {
			switch step.GetAction() {
			case orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE:
				created++
			case orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE:
				updated++
			case orkstackv1.AutomationAction_AUTOMATION_ACTION_DELETE:
				deleted++
			}
		}
	}
	line := "Plan: "
	if len(r.report.GetOrkflows()) > 0 {
		noun := "orkflow versions"
		if stored == 1 {
			noun = "orkflow version"
		}
		line += fmt.Sprintf("%d %s to store; ", stored, noun)
	}
	return line + fmt.Sprintf("%d to create, %d to update, %d to delete.", created, updated, deleted)
}

// renderAutomationPlan prints one document's plan: its issues, its steps with
// their changes and notes, and a line per section it does not manage.
func renderAutomationPlan(out io.Writer, file string, plan *orkstackv1.PlanAutomationResponse) {
	if plan.GetNamespace() != "" {
		fmt.Fprintf(out, "%s  namespace %s\n", file, plan.GetNamespace())
	} else {
		fmt.Fprintln(out, file)
	}
	for _, issue := range plan.GetIssues() {
		fmt.Fprintf(out, "%s %s: %s\n", severityLabel(issue.GetSeverity()), orDash(issue.GetField()), issue.GetMessage())
	}
	for _, step := range plan.GetSteps() {
		fmt.Fprintln(out, stepLine(step))
		for _, change := range step.GetChanges() {
			if step.GetAction() == orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE {
				fmt.Fprintf(out, "      %s: %s\n", change.GetField(), change.GetAfter())
			} else {
				fmt.Fprintf(out, "      %s: %s → %s\n", change.GetField(), change.GetBefore(), change.GetAfter())
			}
		}
		if step.GetAction() == orkstackv1.AutomationAction_AUTOMATION_ACTION_UNMANAGED {
			continue
		}
		for _, note := range step.GetNotes() {
			fmt.Fprintf(out, "      note: %s\n", note)
		}
	}
	if len(plan.GetSteps()) > 0 || plan.GetPlanDigest() != "" {
		managed := map[orkstackv1.AutomationObjectKind]bool{}
		for _, kind := range plan.GetManaged() {
			managed[kind] = true
		}
		for _, kind := range []orkstackv1.AutomationObjectKind{
			orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_NAMESPACE,
			orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_SCHEDULE,
			orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER,
			orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_WEBHOOK,
		} {
			if !managed[kind] {
				fmt.Fprintf(out, "(%s: not managed by this document)\n", automation.SectionKey(kind))
			}
		}
	}
}

// stepLine is one step's line: symbol, kind, name, summary, id, and what
// happens to an object the apply leaves alone.
func stepLine(step *orkstackv1.AutomationStep) string {
	parts := []string{}
	if summary := step.GetSummary(); summary != "" {
		parts = append(parts, summary)
	}
	if id := step.GetId(); id != "" {
		parts = append(parts, id)
	}
	switch step.GetAction() {
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED:
		parts = append(parts, "unchanged")
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_UNDECLARED:
		parts = append(parts, "not in this document; kept (--prune deletes it)")
	case orkstackv1.AutomationAction_AUTOMATION_ACTION_UNMANAGED:
		parts = append(parts, step.GetNotes()...)
	}
	line := fmt.Sprintf("%s %-10s %-18s", automation.ActionSymbol(step.GetAction()),
		automation.KindSlug(step.GetKind()), orDash(step.GetName()))
	if len(parts) > 0 {
		line += " " + strings.Join(parts, "  ")
	}
	return strings.TrimRight(line, " ")
}

func severityLabel(severity orkstackv1.ValidationSeverity) string {
	if severity == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING {
		return "WARNING"
	}
	return "ERROR"
}

// renderResults prints what an apply did, step by step, then the one-time
// secrets it disclosed.
func (r *applyRun) renderResults(out io.Writer, disclosed []*orkstackv1.DisclosedSecret) {
	failed := 0
	any := false
	for _, doc := range r.report.GetAutomation() {
		plan := doc.GetPlan()
		results := doc.GetResults()
		if len(results) == 0 {
			continue
		}
		any = true
		applied := 0
		for _, result := range results {
			switch result.GetOutcome() {
			case orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_APPLIED,
				orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_ALREADY_GONE:
				applied++
			case orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_FAILED:
				failed++
			}
		}
		fmt.Fprintf(out, "\nApplied %d of %d steps in namespace %s.\n", applied, len(results), plan.GetNamespace())
		for _, result := range results {
			step := plan.GetSteps()[result.GetStep()]
			head := fmt.Sprintf("  %-9s %s %-10s %-18s", outcomeLabel(result.GetOutcome()),
				automation.ActionSymbol(step.GetAction()), automation.KindSlug(step.GetKind()), orDash(step.GetName()))
			tail := ""
			switch {
			case result.GetOutcome() == orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_FAILED:
				tail = result.GetError()
			case step.GetAction() == orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE &&
				result.GetOutcome() == orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_APPLIED:
				tail = result.GetId()
			}
			fmt.Fprintln(out, strings.TrimRight(head+" "+tail, " "))
		}
	}
	if !any && !r.anyOrkflowApplied() {
		fmt.Fprintln(out, "\nNo changes: nothing was written.")
	}
	for _, secret := range disclosed {
		fmt.Fprintln(out)
		switch secret.GetKind() {
		case orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER:
			if r.secretsWritten {
				fmt.Fprintf(out, "Trigger %s (%s) was created. Its token: secret written to %s\n",
					secret.GetName(), secret.GetId(), r.opts.secretsOut)
				continue
			}
			fmt.Fprintf(out, "Trigger %s (%s) was created. Its token is shown once:\n  %s\nInvoke it with:\n%s\n",
				secret.GetName(), secret.GetId(), secret.GetSecret(),
				triggerCurlExample(r.g.baseURL(), secret.GetId(), secret.GetSecret()))
		default:
			if r.secretsWritten {
				fmt.Fprintf(out, "Webhook %s (%s) was registered with a generated signing secret: secret written to %s\n",
					secret.GetName(), secret.GetId(), r.opts.secretsOut)
				continue
			}
			fmt.Fprintf(out, "Webhook %s (%s) was registered with a generated signing secret. It is shown once:\n  %s\n",
				secret.GetName(), secret.GetId(), secret.GetSecret())
		}
	}
	if failed > 0 {
		noun := "step"
		if failed > 1 {
			noun = "steps"
		}
		fmt.Fprintf(out, "\n%d %s failed. Fix the cause and run apply again: it plans from what now exists.\n", failed, noun)
	}
}

func (r *applyRun) anyOrkflowApplied() bool {
	for _, doc := range r.report.GetOrkflows() {
		if doc.GetApplied() != nil {
			return true
		}
	}
	return false
}

func outcomeLabel(outcome orkstackv1.AutomationStepOutcome) string {
	switch outcome {
	case orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_APPLIED:
		return "applied"
	case orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_ALREADY_GONE:
		return "gone"
	case orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_FAILED:
		return "failed"
	case orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_NOT_RUN:
		return "not run"
	}
	return "-"
}
