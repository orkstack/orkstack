//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// The shipped-examples suite's stack (examples/README.md): one real `ork` built from this tree on S3 with a
// private Temporal namespace and channel, one real Orker that is a pin host,
// an OrkStack namespace of the suite's own — so every example is applied
// under its SHIPPED name and composition by name keeps working — and every
// command run through the BUILT orkctl with the repository root as its
// working directory, so `examples/<file>` is the path a reader types.

type examplesOptions struct {
	git bool // start the TLS git server and give the Orker its token and CA
}

type examplesStack struct {
	*timingStack
	ns         string
	configPath string
	sessions   string // where `orkctl task run` keeps its local workspaces
	services   *exampleServices
	git        *tlsGitServer
}

func newExamplesStack(ctx context.Context, t *testing.T, opts examplesOptions) *examplesStack {
	t.Helper()
	requireLabelledImages(t)
	services := newExampleServices(t)
	requireContainerCanReach(ctx, t, services.base+"/healthz")
	extra := []string{"ORK_ORKER_MAX_PINS=1"}
	var git *tlsGitServer
	if opts.git {
		git = startTLSGitServer(t)
		for _, url := range git.HealthURLs() {
			requireContainerCanReachTLS(ctx, t, url)
		}
		extra = append(extra, "GIT_TOKEN="+git.Token(), "EXAMPLES_GIT_CA="+git.CA())
	}
	s := newTimingStack(ctx, t, 1)
	startOrkerWithEnv(ctx, t, s.temporal, s.orkerPath, s.address, s.namespace, s.queue, extra...)
	x := &examplesStack{
		timingStack: s,
		ns:          fmt.Sprintf("ex-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000),
		configPath:  filepath.Join(t.TempDir(), "orkctl.json"),
		services:    services,
		git:         git,
		sessions:    filepath.Join(bindableOrkerWorkRoot(t), "task-run-sessions"),
	}
	if err := os.MkdirAll(x.sessions, 0o755); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&x.transcript, "# shipped examples (%s)\n# OrkStack namespace %s, Temporal namespace %s, services %s\n\n",
		time.Now().UTC().Format(time.RFC3339), x.ns, s.namespace, services.base)
	x.mustRun(0, "namespace", "create", x.ns)
	return x
}

// run executes the built orkctl against the engine, from the repository
// root, with no inherited server, namespace or saved configuration, and
// records the exchange. -n is the caller's to pass.
func (x *examplesStack) run(args ...string) (string, string, int) {
	x.t.Helper()
	full := append(append([]string(nil), args...), "--server", "http://"+x.replicas[0].addr)
	cmd := exec.CommandContext(x.ctx, x.orkctlPath, full...)
	cmd.Dir = x.root
	cmd.Env = append(os.Environ(), serverEnvVar+"=", namespaceEnvVar+"=", configEnvVar+"="+x.configPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	} else if err != nil {
		x.t.Fatalf("run orkctl: %v", err)
	}
	fmt.Fprintf(&x.transcript, "$ orkctl %s\n%s", strings.Join(quoteArgs(args), " "), stdout.String())
	if stderr.Len() > 0 {
		fmt.Fprintf(&x.transcript, "(stderr) %s", stderr.String())
	}
	fmt.Fprintf(&x.transcript, "(exit %d)\n\n", code)
	return stdout.String(), stderr.String(), code
}

// mustRun requires exit code want and returns stdout.
func (x *examplesStack) mustRun(want int, args ...string) string {
	x.t.Helper()
	stdout, stderr, code := x.run(args...)
	if code != want {
		x.t.Fatalf("orkctl %s exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), code, want, stdout, stderr)
	}
	return stdout
}

// in is mustRun(0, …) in the stack's namespace.
func (x *examplesStack) in(args ...string) string {
	x.t.Helper()
	return x.mustRun(0, append(args, "-n", x.ns)...)
}

func (x *examplesStack) note(format string, args ...any) {
	fmt.Fprintf(&x.transcript, "# "+format+"\n\n", args...)
}

// finish writes the transcript as examples-<scenario>.txt, after checking it
// carries no form of the git token.
func (x *examplesStack) finish(t *testing.T, scenario string) {
	t.Helper()
	body := x.transcript.String()
	if x.git != nil {
		for _, form := range tokenForms(x.git.originalToken()) {
			if strings.Contains(body, form) {
				t.Errorf("the %s transcript carries the git token", scenario)
				body = strings.ReplaceAll(body, form, "<TOKEN REDACTED BY THE TEST>")
			}
		}
	}
	proof.Write(t, "orkctl", "examples-"+scenario+".txt", []byte(body))
	x.transcript.Reset()
}

// apply applies one shipped example (a path under the repository root) or a
// variant file into the stack's namespace.
func (x *examplesStack) apply(path string) string {
	x.t.Helper()
	return x.in("apply", path)
}

// variantFile writes a variant document and returns its absolute path.
func (x *examplesStack) variantFile(name string, body []byte) string {
	x.t.Helper()
	path := filepath.Join(x.t.TempDir(), name)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		x.t.Fatalf("write %s: %v", name, err)
	}
	fmt.Fprintf(&x.transcript, "# variant %s:\n%s\n", name, body)
	return path
}

// submit starts a run of name in the stack's namespace and returns its id.
func (x *examplesStack) submit(name string, extra ...string) string {
	x.t.Helper()
	return parseRunID(x.t, x.in(append([]string{"run", "submit", name}, extra...)...))
}

// getRun reads the run from the engine's read model.
func (x *examplesStack) getRun(runID string) (*orkstackv1.Run, error) {
	got, err := x.replicas[0].runs.GetRun(x.ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
		RunId: runID, Namespace: x.ns,
	}))
	if err != nil {
		return nil, err
	}
	return got.Msg.GetRun(), nil
}

// await polls the run until it is terminal, or until done says so.
func (x *examplesStack) await(runID string, done func(*orkstackv1.Run) bool) *orkstackv1.Run {
	x.t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	var last *orkstackv1.Run
	for time.Now().Before(deadline) {
		if run, err := x.getRun(runID); err == nil {
			last = run
			if done(run) {
				return run
			}
		}
		select {
		case <-x.ctx.Done():
			x.t.Fatalf("run %s: %v", runID, x.ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	x.t.Fatalf("run %s never reached the awaited state (last %v)", runID, last.GetStatus())
	return nil
}

func terminal(run *orkstackv1.Run) bool {
	switch run.GetStatus() {
	case orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED:
		return true
	}
	return false
}

// runToEnd submits and waits for a terminal status, and records each task's
// outcome in the transcript.
func (x *examplesStack) runToEnd(name string, extra ...string) *orkstackv1.Run {
	x.t.Helper()
	run := x.await(x.submit(name, extra...), terminal)
	x.note("run %s: %s", run.GetId(), summarizeRun(run))
	return run
}

func summarizeRun(run *orkstackv1.Run) string {
	var parts []string
	for _, tr := range run.GetTaskRuns() {
		part := tr.GetTaskId() + "=" + strings.TrimPrefix(tr.GetStatus().String(), "STATUS_")
		if c := tr.GetCache(); c != nil {
			part += "(cache " + strings.TrimPrefix(c.GetOutcome().String(), "CACHE_OUTCOME_") + ")"
		}
		parts = append(parts, part)
	}
	return strings.TrimPrefix(run.GetStatus().String(), "STATUS_") + ": " + strings.Join(parts, " ")
}

// download fetches one output artifact (of one item, when item >= 0).
func (x *examplesStack) download(runID, task, name string, item int) []byte {
	x.t.Helper()
	path := filepath.Join(x.t.TempDir(), name)
	args := []string{"run", "artifact", "download", runID, "--task", task, "--kind", "output", "--name", name, "--output", path}
	if item >= 0 {
		args = append(args, "--item", fmt.Sprint(item))
	}
	x.in(args...)
	body, err := os.ReadFile(path)
	if err != nil {
		x.t.Fatalf("read downloaded %s: %v", name, err)
	}
	return body
}

// plan runs `orkctl --json run plan` and decodes it.
func (x *examplesStack) plan(args ...string) *orkstackv1.PlanRunResponse {
	x.t.Helper()
	out := x.in(append([]string{"--json", "run", "plan"}, args...)...)
	var plan orkstackv1.PlanRunResponse
	if err := api.UnmarshalJSON([]byte(out), &plan); err != nil {
		x.t.Fatalf("decode run plan --json: %v\n%s", err, out)
	}
	return &plan
}

// requirePlanMatchesRun holds a plan to the run it describes: every task the
// plan RUNS succeeded and every task it SKIPS was skipped.
func requirePlanMatchesRun(t *testing.T, plan *orkstackv1.PlanRunResponse, run *orkstackv1.Run) {
	t.Helper()
	for _, task := range plan.GetTasks() {
		got := taskRunOf(t, run, task.GetTaskId()).GetStatus()
		switch task.GetOutcome() {
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS:
			if got != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Errorf("plan RUNS %s, the run recorded %s", task.GetTaskId(), got)
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
			if got != orkstackv1.Status_STATUS_SKIPPED {
				t.Errorf("plan SKIPS %s, the run recorded %s", task.GetTaskId(), got)
			}
		default:
			t.Errorf("plan %s for %s, want RUNS or SKIPPED", task.GetOutcome(), task.GetTaskId())
		}
	}
}

// taskRun runs `orkctl task run` with a work root a container can bind.
func (x *examplesStack) taskRun(args ...string) (string, string, int) {
	x.t.Helper()
	full := append(append([]string(nil), args...), "-n", x.ns)
	return runTaskRunBinary(x.ctx, x.t, x.orkctlPath, x.root, x.sessions, "http://"+x.replicas[0].addr, &x.transcript, full...)
}

// taskRunValues decodes `orkctl --json task run`'s values.
func taskRunValues(t *testing.T, stdout string) map[string]any {
	t.Helper()
	var report struct {
		Outcome string          `json:"outcome"`
		Values  json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("decode task run --json: %v\n%s", err, stdout)
	}
	var values map[string]any
	if err := json.Unmarshal(report.Values, &values); err != nil {
		t.Fatalf("decode task run values (outcome %s): %v\n%s", report.Outcome, err, stdout)
	}
	return values
}

// ============================================================ the services

// exampleServices is the HTTP API every HTTP example talks to: an order
// service, a notification sink, an export with a readiness endpoint, and an
// echo for http-post. Containers address it as host.docker.internal.
type exampleServices struct {
	server *httptest.Server
	base   string

	mu       sync.Mutex
	requests []serviceRequest
	statuses map[string]int
	notReady int
	polled   int
}

type serviceRequest struct {
	Method, Path string
	Body         []byte
}

const exampleExport = `{"generated_at":"2026-01-02T03:04:05Z","rows":[{"amount":10},{"amount":2.5}]}`

var orderPath = regexp.MustCompile(`^/orders/[^/]+/(reservation|payment|shipment)$`)

func newExampleServices(t *testing.T) *exampleServices {
	t.Helper()
	s := &exampleServices{statuses: map[string]int{}}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(s.serve))
	s.server.Listener = listener
	s.server.Start()
	t.Cleanup(s.server.Close)
	s.base = fmt.Sprintf("http://host.docker.internal:%d", listener.Addr().(*net.TCPAddr).Port)
	return s
}

func (s *exampleServices) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	if r.URL.Path != "/healthz" {
		s.requests = append(s.requests, serviceRequest{Method: r.Method, Path: r.URL.Path, Body: body})
	}
	status, forced := s.statuses[r.URL.Path]
	s.mu.Unlock()
	switch {
	case r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusOK)
	case orderPath.MatchString(r.URL.Path) && r.Method == http.MethodPut:
		if !forced {
			status = http.StatusCreated
		}
		w.WriteHeader(status)
	case orderPath.MatchString(r.URL.Path) && r.Method == http.MethodDelete && !strings.HasSuffix(r.URL.Path, "/shipment"):
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/notify" && r.Method == http.MethodPost:
		w.WriteHeader(http.StatusOK)
	case r.URL.Path == "/exports/status" && r.Method == http.MethodGet:
		s.mu.Lock()
		s.polled++
		ready := s.polled > s.notReady
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ready": %t}`, ready)
	case r.URL.Path == "/exports/latest.json" && r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, exampleExport)
	case r.URL.Path == "/post" && r.Method == http.MethodPost:
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"json": %s}`, jsonOrString(body))
	default:
		http.NotFound(w, r)
	}
}

func jsonOrString(body []byte) string {
	if json.Valid(body) {
		return string(body)
	}
	encoded, _ := json.Marshal(string(body))
	return string(encoded)
}

// SetStatus makes every PUT to path answer code.
func (s *exampleServices) SetStatus(path string, code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[path] = code
}

// SetNotReady makes the next n status polls answer {"ready": false}.
func (s *exampleServices) SetNotReady(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notReady, s.polled = n, 0
}

// Requests returns every recorded request whose path starts with prefix.
func (s *exampleServices) Requests(prefix string) []serviceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []serviceRequest
	for _, r := range s.requests {
		if strings.HasPrefix(r.Path, prefix) {
			out = append(out, r)
		}
	}
	return out
}

// notification is one decoded POST /notify body.
type notification struct {
	Subject string `json:"subject"`
	Text    string `json:"text"`
}

// Notifications returns every notification posted since mark (a count from
// an earlier call of len(Notifications(0))).
func (s *exampleServices) Notifications(t *testing.T, mark int) []notification {
	t.Helper()
	var out []notification
	for i, r := range s.Requests("/notify") {
		if i < mark {
			continue
		}
		var n notification
		if err := json.Unmarshal(r.Body, &n); err != nil {
			t.Fatalf("a notification body is not JSON: %v\n%s", err, r.Body)
		}
		out = append(out, n)
	}
	return out
}

// ======================================================== the git server

// examplesRepoFiles is what E8 and E9 seed private/examples.git with: what
// examples/config-from-git.ork.yaml and examples/pinned-checkout.ork.yaml say
// the repository must hold.
var examplesRepoFiles = map[string]string{
	"VERSION":              "1.4.2\n",
	"deploy/app.conf.tmpl": "# {{ .name }}, rendered from commit {{ .commit }}\nservice {{ .name }} {\n  listen   {{ .port }};\n  replicas {{ .replicas }};\n  env      {{ .environment }};\n}\n",
	"deploy/services.json": `{"services":[{"name":"api","port":8080,"replicas":2},{"name":"web","port":8081,"replicas":1}]}`,
	"config/base.json":     `{"log":{"level":"info","format":"json"},"replicas":1}`,
	"config/staging.json":  `{"log":{"level":"debug"}}`,
	"config/prod.json":     `{"replicas":3,"log":{"format":"text"}}`,
}

// ========================================================== the variants

// The only documents this suite runs that are not byte-for-byte the shipped
// file. Each comes with a check that it differs by exactly what it says.

// withExampleGitCA adds, to every `uses: git` task, `caFile: secrets/git-ca`
// under with: and a secret git-ca from env:EXAMPLES_GIT_CA: the test git
// server's certificate is signed by a CA generated for the test, which no
// image trusts. Nothing else changes.
func withExampleGitCA(t *testing.T, src []byte) []byte {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(src, &doc); err != nil {
		t.Fatalf("parse: %v", err)
	}
	tasks := mappingValue(doc.Content[0], "tasks")
	if tasks == nil {
		t.Fatal("the document has no tasks")
	}
	for _, task := range tasks.Content {
		if uses := mappingValue(task, "uses"); uses == nil || uses.Value != "git" {
			continue
		}
		with := mappingValue(task, "with")
		with.Content = append(with.Content, scalar("caFile"), scalar("secrets/git-ca"))
		secrets := mappingValue(task, "secrets")
		if secrets == nil {
			secrets = &yaml.Node{Kind: yaml.SequenceNode}
			task.Content = append(task.Content, scalar("secrets"), secrets)
		}
		secrets.Content = append(secrets.Content, &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
			scalar("name"), scalar("git-ca"), scalar("source"), scalar("env:EXAMPLES_GIT_CA"),
		}})
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return out
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func scalar(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

// requireOnlyGitCAAdded compiles both documents, deletes exactly the two
// additions from the variant, and requires the two definitions be equal.
func requireOnlyGitCAAdded(t *testing.T, shipped, variant []byte) {
	t.Helper()
	want, err := dsl.Compile(shipped)
	if err != nil {
		t.Fatalf("compile the shipped document: %v", err)
	}
	got, err := dsl.Compile(variant)
	if err != nil {
		t.Fatalf("compile the variant: %v", err)
	}
	added := 0
	for _, task := range got.GetTasks() {
		if task.GetUses() != "git" {
			continue
		}
		fields := task.GetWith().GetFields()
		if fields["caFile"].GetStringValue() != "secrets/git-ca" {
			t.Fatalf("variant task %s has no caFile", task.GetId())
		}
		delete(fields, "caFile")
		var kept []*orkstackv1.SecretRef
		for _, secret := range task.GetSecrets() {
			if secret.GetName() == "git-ca" {
				added++
				continue
			}
			kept = append(kept, secret)
		}
		task.Secrets = kept
	}
	if added == 0 {
		t.Fatal("the variant added no CA")
	}
	want.SourceYaml, got.SourceYaml = "", ""
	if !proto.Equal(want, got) {
		t.Fatalf("the CA variant differs from the shipped document by more than the CA:\nshipped %v\nvariant %v", want, got)
	}
}

// examplesRegistryPrefix is the registry prefix the shipped custom-image
// example names.
const examplesRegistryPrefix = "localhost:5001/orkstack-examples/"

// withRegistry points the custom-image example at the suite's registry when
// it is not localhost:5001; undoing the substitution must give back the
// shipped bytes.
func withRegistry(t *testing.T, src []byte, registry string) []byte {
	t.Helper()
	if registry+"/orkstack-examples/" == examplesRegistryPrefix {
		return src
	}
	prefix := registry + "/orkstack-examples/"
	out := bytes.ReplaceAll(src, []byte(examplesRegistryPrefix), []byte(prefix))
	if back := bytes.ReplaceAll(out, []byte(prefix), []byte(examplesRegistryPrefix)); !bytes.Equal(back, src) {
		t.Fatal("the registry variant does not undo to the shipped document")
	}
	return out
}

// automationVariant points the shipped automation document at the stack's
// namespace and the suite's export service. Both parse with no issue, and
// undoing exactly those substitutions gives back the shipped bytes.
func automationVariant(t *testing.T, src []byte, ns, base string) []byte {
	t.Helper()
	const shippedNamespace, shippedExports = "namespace: examples\n", "https://exports.example.com"
	if bytes.Count(src, []byte(shippedNamespace)) != 1 || !bytes.Contains(src, []byte(shippedExports)) {
		t.Fatal("the shipped automation document no longer has the namespace line and export URLs this variant replaces")
	}
	exports := base + "/exports"
	out := bytes.Replace(src, []byte(shippedNamespace), []byte("namespace: "+ns+"\n"), 1)
	out = bytes.ReplaceAll(out, []byte(shippedExports), []byte(exports))
	back := bytes.Replace(out, []byte("namespace: "+ns+"\n"), []byte(shippedNamespace), 1)
	back = bytes.ReplaceAll(back, []byte(exports), []byte(shippedExports))
	if !bytes.Equal(back, src) {
		t.Fatal("the automation variant does not undo to the shipped document")
	}
	for name, doc := range map[string][]byte{"shipped": src, "variant": out} {
		if _, issues := automation.Parse(doc); len(issues) != 0 {
			t.Fatalf("the %s automation document has issues: %v", name, issues)
		}
	}
	return out
}

// ===================================================== the custom images

var (
	exampleImagesOnce sync.Once
	exampleImagesErr  error
	exampleImagesRefs map[string]string
	exampleImagesLog  string
)

// exampleImageTag is the tag this process pushes the example images under:
// the proof's image tag (or `dev`), this process and a stamp. It is unique to
// the process because `orkctl activity build` — the command under test —
// pushes, and two concurrent processes pushing one tag would move it under
// each other (AGENTS.md §11.1).
var exampleImageTag = sync.OnceValue(func() string {
	return fmt.Sprintf("%s-p%d-%d", localcatalog.Tag(), os.Getpid(), time.Now().UnixNano()%1_000_000_000)
})

// ensureExampleImages builds and pushes both custom images with the built
// orkctl's `activity build`, once per process, and returns each tag's pinned
// reference.
func ensureExampleImages(ctx context.Context, t *testing.T, orkctlPath, root string) map[string]string {
	t.Helper()
	requireRegistry(t, localcatalog.Registry())
	exampleImagesOnce.Do(func() {
		exampleImagesRefs = map[string]string{}
		pinned := regexp.MustCompile(`(?m)^  uses: (\S+@sha256:[0-9a-f]{64})$`)
		var log strings.Builder
		for _, dir := range []string{"custom-image", "custom-image-shell"} {
			tag := fmt.Sprintf("%s/orkstack-examples/%s:%s", localcatalog.Registry(), dir, exampleImageTag())
			cmd := exec.CommandContext(ctx, orkctlPath, "activity", "build", "examples/"+dir, "--tag", tag)
			cmd.Dir = root
			out, err := cmd.CombinedOutput()
			fmt.Fprintf(&log, "$ orkctl activity build examples/%s --tag %s\n%s\n", dir, tag, out)
			if err != nil {
				exampleImagesErr = fmt.Errorf("orkctl activity build examples/%s: %w", dir, err)
				break
			}
			match := pinned.FindSubmatch(out)
			if match == nil {
				exampleImagesErr = fmt.Errorf("orkctl activity build examples/%s printed no pinned reference", dir)
				break
			}
			exampleImagesRefs[tag] = string(match[1])
		}
		exampleImagesLog = log.String()
	})
	if exampleImagesErr != nil {
		t.Fatalf("%v\n%s", exampleImagesErr, exampleImagesLog)
	}
	return exampleImagesRefs
}

// customImageDocument is examples/custom-image.ork.yaml, pointed at the
// suite's registry when it is not the shipped one.
func customImageDocument(t *testing.T, root string) []byte {
	t.Helper()
	src, err := os.ReadFile(filepath.Join(root, "examples", "custom-image.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return withExampleTag(t, withRegistry(t, src, localcatalog.Registry()), localcatalog.Registry())
}

// withExampleTag points the example's `:dev` image references at the tag
// this process pushed (exampleImageTag); undoing it gives back its input.
func withExampleTag(t *testing.T, src []byte, registry string) []byte {
	t.Helper()
	ref := regexp.MustCompile(regexp.QuoteMeta(registry+"/orkstack-examples/") + `([a-z0-9-]+):dev\b`)
	tag := exampleImageTag()
	out := ref.ReplaceAll(src, []byte(registry+"/orkstack-examples/${1}:"+tag))
	back := regexp.MustCompile(regexp.QuoteMeta(registry+"/orkstack-examples/")+`([a-z0-9-]+):`+regexp.QuoteMeta(tag)).
		ReplaceAll(out, []byte(registry+"/orkstack-examples/${1}:dev"))
	if !bytes.Equal(back, src) || bytes.Equal(out, src) {
		t.Fatal("the example-tag variant does not undo to the shipped document, or changed nothing")
	}
	return out
}

// exampleFiles is every examples/*.ork.yaml, sorted, relative to the root.
func exampleFiles(t *testing.T, root string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, "examples", "*.ork.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob examples: %v (%d found)", err, len(paths))
	}
	var files []string
	for _, p := range paths {
		files = append(files, "examples/"+filepath.Base(p))
	}
	return files
}
