package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// `orkctl activity test <image>` — the §10.3 contract test suite.
//
// It runs any image against the ABI with NO Orkflow and NO Temporal, which is
// the property AGENTS.md §10 asks for: a developer must be able to test an OCI
// activity without running a complete Orkflow.
//
// It drives the REAL internal/orker.OCIDispatcher rather than reimplementing
// the workspace. That is the whole point of the suite: a second implementation
// would test the second implementation. An image that passes here has been
// through the same workspace construction, the same output scan, the same
// readability check and the same commit protocol that an Orker will put it
// through in production — against local artifact and commit stores in a
// temporary directory instead of S3.

// assertion is one §10.3 property, and its outcome.
type assertion struct {
	name   string
	detail string
	err    error
	// skipped records a property this host cannot express, which is not a
	// pass and must never be reported as one.
	skipped bool
}

func (a assertion) status() string {
	switch {
	case a.skipped:
		return "SKIP"
	case a.err != nil:
		return "FAIL"
	default:
		return "PASS"
	}
}

// activityTestReport is the machine-readable form behind --json.
type activityTestReport struct {
	Image      string                `json:"image"`
	Passed     int                   `json:"passed"`
	Failed     int                   `json:"failed"`
	Skipped    int                   `json:"skipped"`
	OK         bool                  `json:"ok"`
	Assertions []activityTestOutcome `json:"assertions"`
}

type activityTestOutcome struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
	Error  string `json:"error,omitempty"`
}

func newActivityTestCommand(g *globalFlags) *cobra.Command {
	var (
		gracePeriod time.Duration
		timeout     time.Duration
		keep        bool
	)
	cmd := &cobra.Command{
		Use:   "test <image>",
		Short: "Check an image against the OCI Activity Contract",
		Long: "Run an image against the activity ABI, with no Orkflow and no Temporal.\n\n" +
			"The image is executed through the same dispatcher an Orker uses, so a pass\n" +
			"here means the same workspace construction, output scan, readability check\n" +
			"and commit protocol that production will apply. Nothing is reimplemented for\n" +
			"the test.\n\n" +
			"The image reference is used verbatim: this command does not resolve it\n" +
			"through the catalog, because the point is to test an image you just built.\n\n" +
			"Requires a working container runtime. Exits non-zero if any assertion fails.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			suite := &activitySuite{
				image:       args[0],
				grace:       gracePeriod,
				taskTimeout: timeout,
				keep:        keep,
				out:         cmd.OutOrStdout(),
			}
			report, err := suite.run(cmd.Context())
			if err != nil {
				return err
			}
			if g.json {
				if err := writeGoJSON(cmd.OutOrStdout(), report); err != nil {
					return err
				}
			} else {
				renderActivityTestReport(cmd.OutOrStdout(), report)
			}
			if !report.OK {
				// A failed contract is a failed command. Cobra would print
				// the error again, which adds nothing to a rendered table.
				cmd.SilenceUsage = true
				cmd.SilenceErrors = true
				return fmt.Errorf("%s does not satisfy the activity contract", report.Image)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&gracePeriod, "grace-period", 10*time.Second,
		"SIGTERM-to-SIGKILL window used by the cancellation assertion")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute,
		"per-assertion activity timeout")
	cmd.Flags().BoolVar(&keep, "keep", false,
		"keep the temporary workspaces for inspection")
	return cmd
}

func renderActivityTestReport(out io.Writer, report *activityTestReport) {
	fmt.Fprintf(out, "Activity contract: %s\n\n", report.Image)
	table := newTable(out)
	fmt.Fprintln(table, "RESULT\tASSERTION\tDETAIL")
	for _, a := range report.Assertions {
		detail := a.Detail
		if a.Error != "" {
			detail = a.Error
		}
		fmt.Fprintf(table, "%s\t%s\t%s\n", a.Status, a.Name, detail)
	}
	_ = table.Flush()
	fmt.Fprintf(out, "\n%d passed, %d failed, %d skipped\n",
		report.Passed, report.Failed, report.Skipped)
	if report.OK {
		fmt.Fprintf(out, "\n%s satisfies the OCI Activity Contract.\n", report.Image)
	}
}

// activitySuite holds the six assertions and the scaffolding they share.
type activitySuite struct {
	image       string
	grace       time.Duration
	taskTimeout time.Duration
	keep        bool
	out         io.Writer

	root       string
	dispatcher *orker.OCIDispatcher
	pinned     *orkstackv1.ImageRef
}

func (s *activitySuite) run(ctx context.Context) (*activityTestReport, error) {
	root, err := os.MkdirTemp(suiteRootParent(), "ork-activity-test-")
	if err != nil {
		return nil, fmt.Errorf("create the suite workspace: %w", err)
	}
	s.root = root
	if !s.keep {
		defer os.RemoveAll(root)
	} else {
		fmt.Fprintf(s.out, "workspaces kept under %s\n\n", root)
	}

	if err := s.build(ctx); err != nil {
		return nil, err
	}

	// PREFLIGHT. Every assertion below drives the image through the `op: exec`
	// + `command` convention, because probing the ABI means running a command
	// of the suite's choosing inside the image and reading what came back.
	//
	// An image with a different config contract — activity-http takes `url`,
	// activity-file takes `src` — cannot be driven that way, and reporting six
	// contract FAILURES for it would be a false accusation about a correct
	// image. So the inability to drive it is diagnosed ONCE, as its own error,
	// and clearly separated from "this image violates the contract".
	if err := s.preflight(ctx); err != nil {
		return nil, err
	}

	assertions := []assertion{
		s.assertStartsAndWritesValues(ctx),
		s.assertInputIsReadOnly(ctx),
		s.assertTaskJSONIsValid(ctx),
		s.assertArtifactNamesAreEnforced(ctx),
		s.assertExitCodesAreNotInterpreted(ctx),
		s.assertCancellationIsHonoured(ctx),
	}
	return buildActivityTestReport(s.image, assertions), nil
}

// buildActivityTestReport folds the six outcomes into the reported form.
//
// It is a pure function, separated from run() so the reporting layer can be
// tested without a container runtime. That matters more than it looks: the
// FAIL half of this command is the half an acceptance criterion names, and it
// is the half that is hardest to reach through Docker.
//
// A SKIP is not a pass and is never counted as one, but it does not make the
// report fail either — the host could not express the property, which is not
// the image's fault. A caller that needs "all six were actually measured"
// must check Passed, which is why proof gate 4 asserts on it rather than on
// the exit code alone.
func buildActivityTestReport(image string, assertions []assertion) *activityTestReport {
	report := &activityTestReport{Image: image, OK: true}
	for _, a := range assertions {
		outcome := activityTestOutcome{Name: a.name, Status: a.status(), Detail: a.detail}
		if a.err != nil {
			outcome.Error = a.err.Error()
		}
		switch {
		case a.skipped:
			report.Skipped++
		case a.err != nil:
			report.Failed++
			report.OK = false
		default:
			report.Passed++
		}
		report.Assertions = append(report.Assertions, outcome)
	}
	return report
}

// build wires a dispatcher against local stores in the suite's temp root.
func (s *activitySuite) build(ctx context.Context) error {
	store, err := artifacts.NewLocalStore(filepath.Join(s.root, "artifacts"))
	if err != nil {
		return fmt.Errorf("artifact store: %w", err)
	}
	commits, err := artifacts.NewLocalCommitStore(filepath.Join(s.root, "commits"))
	if err != nil {
		return fmt.Errorf("commit store: %w", err)
	}
	work := filepath.Join(s.root, "work")
	if err := os.MkdirAll(work, 0o700); err != nil {
		return fmt.Errorf("work root: %w", err)
	}

	runtime := orker.NewDockerRuntime()
	// Resolved ONCE, for the same reason the Orker pins: a tag that moved
	// between two assertions would make the suite report on two different
	// images and call it one.
	pinned, err := s.resolve(ctx)
	if err != nil {
		return err
	}
	s.pinned = pinned

	policy := orker.SecurityPolicyFromEnv()
	s.dispatcher = &orker.OCIDispatcher{
		Runtime:  runtime,
		Store:    store,
		Commits:  commits,
		Refs:     store,
		Policy:   policy,
		WorkRoot: work,
		OrkerID:  "orkctl-activity-test",
	}
	return nil
}

// resolve pins the image through §4.3's resolver — the SAME one apply uses.
//
// The suite cannot skip this and hand the dispatcher a tag: the dispatcher
// refuses an unpinned reference outright, because a running execution must
// never silently switch images because a tag moved (AGENTS.md §5). Testing
// against a tag would therefore be testing a different contract than the one
// production enforces.
//
// An image that has never been pushed has no registry manifest digest — a
// local image ID is not a manifest digest — so it genuinely cannot be pinned,
// and the error says so rather than inventing a local-only pin that would not
// transfer to a second Orker (§4.6).
func (s *activitySuite) resolve(ctx context.Context) (*orkstackv1.ImageRef, error) {
	image, err := dsl.NewResolver(os.Getenv("ORK_REGISTRY_AUTH_FILE")).
		Resolve(ctx, s.image, s.image)
	if err == nil {
		return image, nil
	}
	return nil, fmt.Errorf("%w\n\n"+
		"`activity test` runs the image the way an Orker will, and an Orker only ever\n"+
		"runs a digest. Push the image first — `orkctl activity build <path> --tag %s`,\n"+
		"or `make -C images push` for a first-party image — and the tag resolves to an\n"+
		"index digest. A locally built, never-pushed image has no manifest digest to pin",
		err, s.image)
}

// input builds one ExecuteTaskInput for an assertion.
func (s *activitySuite) input(name string, with map[string]any) (*orkstackv1.ExecuteTaskInput, error) {
	config, err := structpb.NewStruct(with)
	if err != nil {
		return nil, fmt.Errorf("with: %w", err)
	}
	runID := "ork-activity-test-" + name
	return &orkstackv1.ExecuteTaskInput{
		RunId:                          runID,
		OrkflowName:                    "activity-contract",
		OrkflowVersion:                 "test",
		Namespace:                      "default",
		Pool:                           "main",
		TaskId:                         name,
		IdempotencyKey:                 "default/" + runID + "/" + name,
		Image:                          s.pinned,
		With:                           config,
		TimeoutSeconds:                 int32(s.taskTimeout / time.Second),
		CancellationGracePeriodSeconds: int32(s.grace / time.Second),
		Limits: &orkstackv1.WorkspaceLimits{
			ValuesMaxBytes:   262144,
			ArtifactMaxCount: 256,
			ArtifactMaxBytes: 5 << 30,
			LogMaxBytes:      16 << 20,
			CommitMaxBytes:   4 << 20,
		},
	}, nil
}

// execute runs one attempt and returns its result or its failure.
func (s *activitySuite) execute(
	ctx context.Context, name string, with map[string]any,
) (*orkstackv1.ExecuteTaskResult, error) {
	in, err := s.input(name, with)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.taskTimeout+30*time.Second)
	defer cancel()
	return s.dispatcher.Execute(ctx, in)
}

// --- the six assertions -------------------------------------------------

//  1. The container starts, the workload's stdout reaches the stream, and
//     values.json is written and is an object.
//
// stdout is asserted rather than only values.json because the `eval`
// command-substitution defect was invisible to any values-only check:
// values.json was right while the durable stdout.log was empty for every eval
// task (§14.18), silently voiding §3.6's regression gate.
func (s *activitySuite) assertStartsAndWritesValues(ctx context.Context) assertion {
	const marker = "orkstack-contract-stdout-marker"
	a := assertion{name: "starts, writes values.json, and its stdout reaches the stream"}
	result, err := s.execute(ctx, "startup", map[string]any{
		"op":      "exec",
		"command": []any{"sh", "-c", "echo " + marker},
	})
	if err != nil {
		a.err = fmt.Errorf("the image did not complete a trivial exec: %w", err)
		return a
	}
	if result.GetExitCode() != 0 {
		a.err = fmt.Errorf("a trivial exec exited %d", result.GetExitCode())
		return a
	}
	if result.GetValuesRef() == nil && len(result.GetValuesJson()) == 0 {
		a.err = errors.New("the attempt succeeded but committed no values.json")
		return a
	}
	stdout, err := s.systemArtifact(ctx, result, "stdout.log")
	if err != nil {
		a.err = fmt.Errorf("read the durable stdout.log: %w", err)
		return a
	}
	if !strings.Contains(stdout, marker) {
		a.err = fmt.Errorf("the workload printed %q but stdout.log does not carry it; "+
			"a values-only check would have missed this (§14.18)", marker)
		return a
	}
	a.detail = "values.json committed; stdout.log carries the workload's output"
	return a
}

// 2. /orkstack/input is mounted read-only.
//
// Asserted from INSIDE the container, because a mount option the host believes
// in is not the same fact as a write the workload cannot perform.
func (s *activitySuite) assertInputIsReadOnly(ctx context.Context) assertion {
	a := assertion{name: "/orkstack/input is read-only to the workload"}
	result, err := s.execute(ctx, "readonly", map[string]any{
		"op": "eval",
		"command": []any{"sh", "-c",
			"if touch /orkstack/input/breach 2>/dev/null; then echo WRITABLE; else echo READONLY; fi"},
	})
	if err != nil {
		a.err = fmt.Errorf("the read-only probe did not complete: %w", err)
		return a
	}
	stdout, err := s.systemArtifact(ctx, result, "stdout.log")
	if err != nil {
		a.err = fmt.Errorf("read the durable stdout.log: %w", err)
		return a
	}
	if !strings.Contains(stdout, "READONLY") {
		a.err = errors.New("the workload wrote into /orkstack/input; it must be a read-only mount (§3.1)")
		return a
	}
	a.detail = "a write into /orkstack/input was refused inside the container"
	return a
}

// 3. /orkstack/input/task.json exists and is complete against the §3.2 table.
//
// The fields are read BY THE WORKLOAD, with jq, so the assertion is about what
// the container can actually see rather than about what the host believed it
// wrote. That distinction is the whole reason the file is the ABI.
func (s *activitySuite) assertTaskJSONIsValid(ctx context.Context) assertion {
	a := assertion{name: "task.json is present and complete against the §3.2 table"}
	// One jq filter per §3.2 row, emitted as name=value so a missing field
	// names itself in the failure rather than showing up as a column count.
	const probe = `jq -r '
	  "schemaVersion=\(.schemaVersion)",
	  "idempotencyKey=\(.idempotencyKey)",
	  "run.id=\(.run.id)",
	  "run.namespace=\(.run.namespace)",
	  "run.orkflow.name=\(.run.orkflow.name)",
	  "run.orkflow.version=\(.run.orkflow.version)",
	  "task.id=\(.task.id)",
	  "task.attempt=\(.task.attempt)",
	  "task.pool=\(.task.pool)",
	  "image.pinned=\(.image.pinned)",
	  "image.indexDigest=\(.image.indexDigest)",
	  "limits.valuesMaxBytes=\(.limits.valuesMaxBytes)",
	  "limits.artifactMaxCount=\(.limits.artifactMaxCount)",
	  "limits.cancellationGracePeriodSeconds=\(.limits.cancellationGracePeriodSeconds)",
	  "network=\(.network)",
	  "paths.input=\(.paths.input)",
	  "paths.output=\(.paths.output)",
	  "paths.values=\(.paths.values)",
	  "paths.artifacts=\(.paths.artifacts)",
	  "paths.tmp=\(.paths.tmp)"
	' /orkstack/input/task.json`

	result, err := s.execute(ctx, "taskjson", map[string]any{
		"op":      "eval",
		"command": []any{"sh", "-c", probe},
	})
	if err != nil {
		a.err = fmt.Errorf("the task.json probe did not complete: %w", err)
		return a
	}
	stdout, err := s.systemArtifact(ctx, result, "stdout.log")
	if err != nil {
		a.err = fmt.Errorf("read the durable stdout.log: %w", err)
		return a
	}

	seen := map[string]string{}
	for _, line := range strings.Split(stdout, "\n") {
		if name, value, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			seen[name] = value
		}
	}
	if len(seen) == 0 {
		a.err = fmt.Errorf("the workload could not read task.json at all; stdout was %q", stdout)
		return a
	}
	required := []string{
		"schemaVersion", "idempotencyKey",
		"run.id", "run.namespace", "run.orkflow.name", "run.orkflow.version",
		"task.id", "task.attempt", "task.pool",
		"image.pinned", "image.indexDigest",
		"limits.valuesMaxBytes", "limits.artifactMaxCount",
		"limits.cancellationGracePeriodSeconds",
		"network",
		"paths.input", "paths.output", "paths.values", "paths.artifacts", "paths.tmp",
	}
	for _, name := range required {
		value, ok := seen[name]
		if !ok {
			a.err = fmt.Errorf("task.json is missing %s (§3.2)", name)
			return a
		}
		if value == "" || value == "null" {
			a.err = fmt.Errorf("task.json field %s is %q (§3.2 requires it)", name, value)
			return a
		}
	}
	// The pin the container sees must be the pin the suite resolved, or the
	// image cannot know what it is running as.
	if got := seen["image.pinned"]; got != s.pinned.GetPinned() {
		a.err = fmt.Errorf("task.json reports image.pinned=%q, but the attempt was dispatched with %q",
			got, s.pinned.GetPinned())
		return a
	}
	a.detail = fmt.Sprintf("all %d §3.2 fields present, and image.pinned matches the dispatched digest",
		len(required))
	return a
}

// 4. Artifact names round-trip, and an unsafe name is rejected at the SCAN.
//
// §14.16: without this the failure surfaces only after upload.
func (s *activitySuite) assertArtifactNamesAreEnforced(ctx context.Context) assertion {
	a := assertion{name: "artifact names round-trip, and a directory is refused at the scan"}
	result, err := s.execute(ctx, "artifacts", map[string]any{
		"op": "exec",
		"command": []any{"sh", "-c",
			`printf 'payload' > /orkstack/output/artifacts/report.txt; ` +
				`printf 'x' > /orkstack/output/artifacts/a-name.with.dots`},
	})
	if err != nil {
		a.err = fmt.Errorf("writing two ordinary artifacts failed: %w", err)
		return a
	}
	got := map[string]bool{}
	for _, ref := range result.GetArtifacts() {
		got[ref.GetName()] = true
	}
	for _, want := range []string{"report.txt", "a-name.with.dots"} {
		if !got[want] {
			a.err = fmt.Errorf("artifact %q did not round-trip; got %v", want, keysOf(got))
			return a
		}
	}

	// A directory in output/artifacts/ must fail the scan: §3.5 has no
	// directory semantics, and an artifact is a regular file.
	_, dirErr := s.execute(ctx, "artifactdir", map[string]any{
		"op":      "exec",
		"command": []any{"sh", "-c", `mkdir -p /orkstack/output/artifacts/a-directory`},
	})
	if dirErr == nil {
		a.err = errors.New("a DIRECTORY in output/artifacts/ was accepted; §3.5 admits regular files only")
		return a
	}
	if !strings.Contains(dirErr.Error(), "ARTIFACT") {
		a.err = fmt.Errorf("a directory artifact failed, but not with an ARTIFACT_* reason: %v", dirErr)
		return a
	}
	a.detail = "names round-trip; a directory is refused at the scan, before any upload"
	return a
}

// 5. Exit codes are reported, never interpreted.
//
// §14.13: 125, 126 and 127 carry no runtime guarantee, so they must be
// ordinary failures rather than magic values. There are no magic exit codes.
func (s *activitySuite) assertExitCodesAreNotInterpreted(ctx context.Context) assertion {
	a := assertion{name: "every non-zero exit fails the activity, and none is magic"}
	for _, code := range []int{1, 42, 125, 126, 127} {
		_, err := s.execute(ctx, fmt.Sprintf("exit%d", code), map[string]any{
			"op":      "exec",
			"command": []any{"sh", "-c", fmt.Sprintf("exit %d", code)},
		})
		if err == nil {
			a.err = fmt.Errorf("exit %d was reported as SUCCESS; every non-zero exit is a failure "+
				"(AGENTS.md §3.2, §14.13)", code)
			return a
		}
	}
	// And zero still succeeds, or the assertion above proves nothing.
	if _, err := s.execute(ctx, "exit0", map[string]any{
		"op":      "exec",
		"command": []any{"sh", "-c", "exit 0"},
	}); err != nil {
		a.err = fmt.Errorf("exit 0 did not succeed: %w", err)
		return a
	}
	a.detail = "0 succeeds; 1, 42, 125, 126 and 127 all fail, none specially"
	return a
}

// 6. SIGTERM is honoured within the grace period, and the workload observes it.
//
// BOTH halves are asserted, because they fail INDEPENDENTLY (§14.14):
// `exec sh -c` delivers the signal but loses values.json, while the naive
// shape loses both and exits 143. A test that only measured how long
// cancellation took would pass on an image that never received the signal at
// all and was SIGKILLed promptly.
//
// The workload prints a marker from its own TERM trap. That line reaching the
// log stream is direct evidence the signal crossed the entry script into the
// workload — which is exactly what §10.2 rule 2a exists to guarantee and what
// the previous draft of that script got wrong.
func (s *activitySuite) assertCancellationIsHonoured(ctx context.Context) assertion {
	const (
		ready      = "orkstack-contract-ready"
		terminated = "orkstack-contract-caught-sigterm"
	)
	a := assertion{name: "SIGTERM reaches the workload within the grace period"}

	// Observe the live stream so the cancellation happens once the workload
	// is genuinely running. A sleep here would be a race dressed as a delay
	// (AGENTS.md §11.1: no sleeps for synchronization).
	running := make(chan struct{})
	caught := make(chan struct{})
	var once, onceCaught bool
	s.dispatcher.LogSinkObserver = func(_ *orkstackv1.ExecuteTaskInput, line string) {
		if !once && strings.Contains(line, ready) {
			once = true
			close(running)
		}
		if !onceCaught && strings.Contains(line, terminated) {
			onceCaught = true
			close(caught)
		}
	}
	defer func() { s.dispatcher.LogSinkObserver = nil }()

	run, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		in, err := s.input("cancel", map[string]any{
			"op": "exec",
			"command": []any{"sh", "-c",
				"trap 'echo " + terminated + "; exit 0' TERM; echo " + ready +
					"; while true; do sleep 0.2; done"},
		})
		if err != nil {
			done <- err
			return
		}
		_, execErr := s.dispatcher.Execute(run, in)
		done <- execErr
	}()

	select {
	case <-running:
	case err := <-done:
		a.err = fmt.Errorf("the workload exited before it was cancellable: %v", err)
		return a
	case <-time.After(s.taskTimeout):
		a.err = errors.New("the workload never printed its ready marker; " +
			"either it did not start or its stdout does not reach the stream")
		return a
	}

	cancelledAt := time.Now()
	cancel()

	select {
	case <-caught:
	case <-time.After(s.grace):
		a.err = fmt.Errorf("the workload did not observe SIGTERM within the %s grace period; "+
			"the entry script is not forwarding it (§10.2 rule 2a, §14.14)", s.grace)
		return a
	}
	elapsed := time.Since(cancelledAt)

	select {
	case <-done:
	case <-time.After(s.grace + 60*time.Second):
		a.err = errors.New("the attempt did not return after cancellation")
		return a
	}
	a.detail = fmt.Sprintf("the workload's own TERM trap fired %s after cancellation, "+
		"within the %s grace period", elapsed.Truncate(time.Millisecond), s.grace)
	return a
}

// systemArtifact reads one of the attempt's durable system artifacts.
func (s *activitySuite) systemArtifact(
	ctx context.Context, result *orkstackv1.ExecuteTaskResult, name string,
) (string, error) {
	for _, ref := range result.GetSystemArtifacts() {
		if ref.GetName() != name {
			continue
		}
		reader, err := s.dispatcher.Store.Get(ctx, ref.GetUri())
		if err != nil {
			return "", err
		}
		defer reader.Close()
		body, err := io.ReadAll(reader)
		if err != nil {
			return "", err
		}
		return string(body), nil
	}
	return "", fmt.Errorf("the attempt committed no %s; §3.6 requires stdout and stderr "+
		"to be captured independently and retained", name)
}

func keysOf(in map[string]bool) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	return out
}

// writeGoJSON renders a plain Go value as --json output.
//
// writeJSON takes a proto.Message, because almost every --json payload in this
// CLI is one. This report is not: it is a local result type with no wire
// contract, and inventing a proto message for a command that talks to a
// container runtime rather than to the engine would put a cross-process type
// in the way of a purely local check.
func writeGoJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// suiteRootParent picks a directory the container daemon can actually
// bind-mount.
//
// The OS temp directory is the obvious choice and is wrong on the most common
// developer machine there is: on macOS with a VM-hosted daemon, $HOME is a
// shared path and /var/folders — which is exactly what os.TempDir() returns —
// is not. A suite rooted there fails every assertion with
// WORKSPACE_NOT_BINDABLE, which looks like the IMAGE is broken.
//
// So the order is: the Orker's own configured workspace root, because an
// operator who set it has already solved this for this host; then $HOME,
// which is shared by default; then the OS temp directory, which is correct on
// Linux where the daemon shares the host's filesystem.
func suiteRootParent() string {
	if configured := os.Getenv(orker.WorkRootVar); configured != "" {
		if err := os.MkdirAll(configured, 0o700); err == nil {
			return configured
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidate := filepath.Join(home, ".orkstack", "activity-test")
		if err := os.MkdirAll(candidate, 0o700); err == nil {
			return candidate
		}
	}
	return ""
}

// preflight establishes that the suite can drive this image at all.
//
// It distinguishes two failures that look alike from the outside and mean
// opposite things:
//
//   - The image REFUSED the suite's config. activity-http exits 2 on an
//     unknown op, activity-file's fail() exits 1. That is correct behaviour
//     for an image with its own config contract, and reporting six contract
//     FAILURES for it would be a false accusation.
//
//   - The image ACCEPTED the config, exited 0, and still broke the ABI floor.
//     §3.4 is unconditional: exit 0 means the computation succeeded, and the
//     Orker then validates and commits outputs. An image that claims success
//     and writes no values.json is not speaking a different config contract,
//     it is violating the one contract every image shares.
//
// The second case must not be swallowed as "cannot be driven": that would make
// assertion 1 unreachable for EXACTLY the failure mode it exists to catch —
// `orkctl activity test` on an image that exits 0 writing nothing would return
// a bare error with no report and no FAIL row, leaving the fail half of the
// command unprovable. The exit code decides, because it is the workload's own
// statement about whether it did the job.
func (s *activitySuite) preflight(ctx context.Context) error {
	_, err := s.execute(ctx, "preflight", map[string]any{
		"op":      "exec",
		"command": []any{"sh", "-c", "exit 0"},
	})
	if err == nil {
		return nil
	}
	if preflightIsContractViolation(err) {
		// Let the assertions run and NAME the violation, rather than
		// reporting it as an image the suite cannot drive.
		return nil
	}
	return fmt.Errorf("this image cannot be driven by the contract suite: %w\n\n"+
		"Every assertion runs a command of the suite's choosing inside the image, through\n"+
		"the `op: exec` + `command` convention that activity-shell implements and the Bash\n"+
		"fixture uses. An image with its own config contract — activity-http takes a `url`,\n"+
		"activity-file takes a `src` — refuses that input, which is correct behaviour and\n"+
		"NOT a contract violation.\n\n"+
		"Test such an image with a fixture that speaks its own config contract; see\n"+
		"test/fixtures/ and the image's README", err)
}

// preflightIsContractViolation answers whether a failed preflight attempt is
// the image breaking the ABI floor rather than refusing the suite's config.
//
// Pure, and separated from preflight, so the decision that governs whether a
// violation is REPORTED can be tested without a container runtime.
//
// The test is: the container exited 0 — it claimed the work succeeded — and
// the failure is a §8.1 ContractViolation. A non-zero exit is the image
// refusing the input, which preflight exists to forgive. A failure that is
// not a ContractViolation (an image pull failure, an unbindable workdir) is
// the host's problem and is not the image's contract either way.
//
// OCIDispatcher.Execute returns failure.TemporalError(), so the error on this
// path is a *temporal.ApplicationError whose TYPE is the category and whose
// detail carries the reason code and the exit code — the same shape and the
// same decode the engine performs in internal/engine/workflow.go. The raw
// *orker.Failure is accepted too, because Validate and the pre-runtime
// refusals return one directly.
func preflightIsContractViolation(err error) bool {
	if err == nil {
		return false
	}
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		if applicationErr.Type() != orker.CategoryContractViolation {
			return false
		}
		var detail *orkstackv1.ActivityFailureDetail
		if decodeErr := applicationErr.Details(&detail); decodeErr != nil {
			// No detail means no exit code to judge by. The suite does not
			// guess: an image is not accused on an error it cannot read.
			return false
		}
		return detail.GetExitCode() == 0
	}
	var failure *orker.Failure
	if errors.As(err, &failure) {
		return failure.Category() == orker.CategoryContractViolation &&
			failure.Detail.GetExitCode() == 0
	}
	return false
}
