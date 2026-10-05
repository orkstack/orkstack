package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/orker"
)

// Exit codes. Anything non-zero means the command did not succeed; the
// distinct run codes let scripts tell a failed run from a failed command.
const (
	exitOK          = 0
	exitError       = 1
	exitRunFailed   = 2
	exitRunCanceled = 3
	// exitDrainTimedOut (4, orker.go) is `orkctl orker drain` timing out
	// with work still on the Orker.
	//
	// exitPlanAssertionFailed is `orkctl run plan` whose plan contradicts an
	// --assert-runs/--assert-skipped: distinct from exitError so a CI step
	// can tell "the plan contradicts the policy" from "the engine was
	// unreachable". It shares the number with drain's timeout because the
	// two belong to different commands and neither can return the other.
	exitPlanAssertionFailed = 4
	// exitApplyCheckChanges is `orkctl apply --check` finding that applying
	// would change something: the documents and what is stored differ. It
	// is that command's own negative verdict, on the terms above.
	exitApplyCheckChanges = 4
)

// defaultServer is used when neither --server nor ORK_SERVER is set.
const defaultServer = "http://localhost:8080"

// serverEnvVar is the environment fallback for --server.
const serverEnvVar = "ORK_SERVER"

// exitCodeError carries a process exit code alongside the underlying error so
// command code never calls os.Exit itself.
type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string { return e.err.Error() }

func (e *exitCodeError) Unwrap() error { return e.err }

// exitCodeFor maps a command error to the process exit code.
func exitCodeFor(err error) int {
	if err == nil {
		return exitOK
	}
	var coded *exitCodeError
	if errors.As(err, &coded) {
		return coded.code
	}
	return exitError
}

// Execute runs orkctl with the given arguments and streams, returning the
// process exit code. main is the only caller that turns it into os.Exit.
func Execute(args []string, stdout, stderr io.Writer) int {
	root, flags := newRootCommand()
	// The CLI is one shot: release the engine connection when the command ends.
	defer flags.closeIdleConnections()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return exitCodeFor(err)
	}
	return exitOK
}

// globalFlags holds the persistent flags and the derived API clients.
type globalFlags struct {
	server         string
	namespace      string
	json           bool
	timeout        time.Duration
	timeoutChanged bool

	client *http.Client

	// containerRuntime is the test seam `orkctl task run` drives its
	// dispatcher through. Nil means the Orker's own default runtime
	// (orker.NewDockerRuntime); no flag sets it.
	containerRuntime orker.ContainerRuntime
}

// newRootCommand builds the command tree and the shared flag state.
func newRootCommand() (*cobra.Command, *globalFlags) {
	g := &globalFlags{}
	root := &cobra.Command{
		Use:   "orkctl",
		Short: "Control and observe OrkStack orkflows",
		Long: "orkctl drives the OrkStack engine: apply orkflow DSL files, submit runs,\n" +
			"watch their events live, and inspect runs, orkflows, and activities.\n\n" +
			"Everything an orkflow or a run belongs to is scoped by a namespace.\n" +
			"--namespace picks one for a single command; `orkctl namespace use`\n" +
			"remembers one so it does not have to be typed again.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("server") {
				if fromEnv := os.Getenv(serverEnvVar); fromEnv != "" {
					g.server = fromEnv
				}
			}
			if err := g.resolveNamespace(cmd); err != nil {
				return err
			}
			g.timeoutChanged = cmd.Flags().Changed("timeout")
			return g.validate()
		},
	}
	root.PersistentFlags().StringVar(&g.server, "server", defaultServer,
		"OrkStack engine base URL (env "+serverEnvVar+")")
	// No backticks in this usage string: Cobra reads a backticked span as the
	// flag's value placeholder, so "`orkctl namespace use`" would render as
	// `-n, --namespace orkctl namespace use`.
	root.PersistentFlags().StringVarP(&g.namespace, "namespace", "n", "",
		"namespace to act in (env "+namespaceEnvVar+
			", then the one saved by 'orkctl namespace use'; default \"default\")")
	root.PersistentFlags().BoolVar(&g.json, "json", false,
		"emit machine-readable JSON instead of human tables")
	root.PersistentFlags().DurationVar(&g.timeout, "timeout", 30*time.Second,
		"timeout for each API request (0 disables it)")

	root.AddCommand(
		newAdminCommand(g),
		newApplyCommand(g),
		newCacheCommand(g),
		newExportCommand(g),
		newNamespaceCommand(g),
		newOrkflowCommand(g),
		newOrkerCommand(g),
		newPoolCommand(g),
		newReleaseCommand(g),
		newRunCommand(g),
		newActivityCommand(g),
		newTaskCommand(g),
		newScheduleCommand(g),
		newStatsCommand(g),
		newTriggerCommand(g),
		newWebhookCommand(g),
		newVersionCommand(g),
	)
	return root, g
}

// resolveNamespace settles which namespace this invocation acts in.
//
// Precedence, most specific first: the --namespace flag, ORK_NAMESPACE, the
// persisted default from `orkctl namespace use`, then the server's own default
// namespace (sent as an empty string, so the CLI never has to know its name).
// The order is the ordinary one — what you typed beats what your shell says
// beats what you saved — and each step is only consulted when the one above it
// said nothing.
//
// An unreadable config file is an error rather than a shrug: running against
// `default` when the user believes they selected `payments` is the mistake
// this whole flag exists to prevent.
func (g *globalFlags) resolveNamespace(cmd *cobra.Command) error {
	if cmd.Flags().Changed("namespace") {
		return nil
	}
	if fromEnv := os.Getenv(namespaceEnvVar); fromEnv != "" {
		g.namespace = fromEnv
		return nil
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	g.namespace = cfg.Namespace
	return nil
}

// closeIdleConnections releases the transport's sockets once the command ends.
func (g *globalFlags) closeIdleConnections() {
	if g.client == nil {
		return
	}
	if transport, ok := g.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

// validate rejects a server address the Connect clients could never use.
func (g *globalFlags) validate() error {
	if strings.TrimSpace(g.server) == "" {
		return fmt.Errorf("--server must not be empty")
	}
	parsed, err := url.Parse(g.server)
	if err != nil {
		return fmt.Errorf("invalid --server %q: %w", g.server, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("invalid --server %q: scheme must be http or https", g.server)
	}
	if parsed.Host == "" {
		return fmt.Errorf("invalid --server %q: missing host", g.server)
	}
	return nil
}

// httpClient returns a client that speaks the engine's transport. The engine
// serves Connect over h2c, so plain http:// URLs use unencrypted HTTP/2;
// https:// URLs keep the standard HTTP/1.1 + HTTP/2 negotiation.
func (g *globalFlags) httpClient() *http.Client {
	if g.client != nil {
		return g.client
	}
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	if strings.HasPrefix(g.server, "https://") {
		transport.Protocols.SetHTTP1(true)
		transport.Protocols.SetHTTP2(true)
	} else {
		transport.Protocols.SetUnencryptedHTTP2(true)
	}
	g.client = &http.Client{Transport: transport}
	return g.client
}

func (g *globalFlags) orkflowClient() orkstackv1connect.OrkflowServiceClient {
	return orkstackv1connect.NewOrkflowServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func (g *globalFlags) runClient() orkstackv1connect.RunServiceClient {
	return orkstackv1connect.NewRunServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// activityClient reaches ActivityService: an activity is an image, and which
// images exist is engine configuration rather than something to ask an Orker
// (§4.2, §6.5).
func (g *globalFlags) activityClient() orkstackv1connect.ActivityServiceClient {
	return orkstackv1connect.NewActivityServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func (g *globalFlags) namespaceClient() orkstackv1connect.NamespaceServiceClient {
	return orkstackv1connect.NewNamespaceServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func (g *globalFlags) systemClient() orkstackv1connect.SystemServiceClient {
	return orkstackv1connect.NewSystemServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func (g *globalFlags) statsClient() orkstackv1connect.StatsServiceClient {
	return orkstackv1connect.NewStatsServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func (g *globalFlags) webhookClient() orkstackv1connect.WebhookServiceClient {
	return orkstackv1connect.NewWebhookServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// triggerClient reaches TriggerService, the inbound mirror of WebhookService:
// a trigger is what an external caller invokes to start a run.
func (g *globalFlags) triggerClient() orkstackv1connect.TriggerServiceClient {
	return orkstackv1connect.NewTriggerServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// automationClient reaches AutomationService: the plan and the export of a
// namespace's automation document. It writes nothing; apply sends the plan's
// requests to the other clients.
func (g *globalFlags) automationClient() orkstackv1connect.AutomationServiceClient {
	return orkstackv1connect.NewAutomationServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// auditClient reaches AuditService, the read-back of the control-plane audit
// trail.
func (g *globalFlags) auditClient() orkstackv1connect.AuditServiceClient {
	return orkstackv1connect.NewAuditServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// principalEnvVar names the identity this CLI asserts to the engine. It is an
// ASSERTION, not authentication: the engine records it verbatim in the audit
// trail and verifies nothing.
const principalEnvVar = "ORK_PRINCIPAL"

// principalHeader is the request header the assertion travels in; the engine
// reads the same name (internal/api.AuditPrincipalHeader).
const principalHeader = "Ork-Principal"

// clientOptions is what every Connect client this CLI builds shares. With
// ORK_PRINCIPAL set, every request — unary or streaming — carries
// Ork-Principal: $ORK_PRINCIPAL; unset, nothing is added.
func (g *globalFlags) clientOptions() []connect.ClientOption {
	principal := os.Getenv(principalEnvVar)
	if principal == "" {
		return nil
	}
	return []connect.ClientOption{connect.WithInterceptors(principalInterceptor{principal: principal})}
}

// principalInterceptor sets the Ork-Principal header on every outgoing
// request. It is a full interceptor rather than a unary one so a streaming
// read carries the same assertion as a write.
type principalInterceptor struct{ principal string }

func (p principalInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set(principalHeader, p.principal)
		return next(ctx, req)
	}
}

func (p principalInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set(principalHeader, p.principal)
		return conn
	}
}

func (principalInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func (g *globalFlags) baseURL() string {
	return strings.TrimRight(g.server, "/")
}

// unaryContext bounds a single request with --timeout.
func (g *globalFlags) unaryContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	if g.timeout <= 0 {
		return context.WithCancel(cmd.Context())
	}
	return context.WithTimeout(cmd.Context(), g.timeout)
}

// streamContext bounds an event stream. The default per-request timeout would
// cut a legitimate long run short, so it applies only when the user asked for
// it explicitly.
func (g *globalFlags) streamContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	if g.timeoutChanged && g.timeout > 0 {
		return context.WithTimeout(cmd.Context(), g.timeout)
	}
	return context.WithCancel(cmd.Context())
}

// rpcError renders a Connect failure as a short, actionable CLI error.
func rpcError(operation string, err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return fmt.Errorf("%s: %s: %s", operation, connectErr.Code(), connectErr.Message())
	}
	return fmt.Errorf("%s: %w", operation, err)
}
