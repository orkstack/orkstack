package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
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
	// exitApplyCheckChanges is `orkctl orkflow create --check` or
	// `orkctl automation apply --check` finding that running it would change
	// something: the documents and what is stored differ. It is those
	// commands' own negative verdict, on the terms above.
	exitApplyCheckChanges = 4
	// exitInterrupted is SIGINT or SIGTERM ending the command: the shell's
	// own 128+SIGINT, so a script can tell "stopped" from "failed".
	exitInterrupted = 130
)

// defaultServer is used when neither --server, ORK_SERVER nor the config
// file's server is set.
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
	flags.stderr = stderr
	// The CLI is one shot: release the engine connection when the command ends.
	defer flags.closeIdleConnections()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	// The first SIGINT or SIGTERM cancels the context every command derives
	// from, so the command unwinds through its defers — a --json document is
	// closed, a partial download's temporary file removed — and exits 130.
	// The notifier is stopped on that first signal, so a second one kills
	// orkctl at once.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()
	if err := root.ExecuteContext(ctx); err != nil {
		return reportError(ctx, stderr, err)
	}
	return exitOK
}

// reportError prints a command's error and returns its exit code. A command
// a signal ended reports itself interrupted (130) — unless it chose its own
// code for that case (`task run` exits 3 once it has cancelled its
// container).
func reportError(ctx context.Context, stderr io.Writer, err error) int {
	var coded *exitCodeError
	if ctx.Err() != nil && !errors.As(err, &coded) {
		fmt.Fprintln(stderr, "Error: interrupted")
		return exitInterrupted
	}
	fmt.Fprintf(stderr, "Error: %v\n", err)
	return exitCodeFor(err)
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

	// terminal is the test seam for "is standard input a terminal a person
	// can answer a confirmation from" (confirm.go). Nil means the real
	// check; no flag sets it.
	terminal func() bool

	// goos is the test seam for the host operating system the local-run
	// commands (`task run`, `activity test`) check before they start
	// (local_platform.go). Empty means runtime.GOOS; no flag sets it.
	goos string

	// creds are how requests authenticate and which roots they trust.
	creds cliCredentials

	// stderr is where a warning goes (Execute's); os.Stderr when unset.
	stderr io.Writer
	// principalChecked asks the engine, once a run of orkctl, whether it
	// records ORK_PRINCIPAL at all.
	principalChecked sync.Once
}

// newRootCommand builds the command tree and the shared flag state.
func newRootCommand() (*cobra.Command, *globalFlags) {
	g := &globalFlags{}
	root := &cobra.Command{
		Use:   "orkctl",
		Short: "Control and observe OrkStack orkflows",
		Long: "orkctl drives the OrkStack engine: create orkflows from DSL files, submit runs,\n" +
			"watch their events live, and inspect runs, orkflows, and activities.\n\n" +
			"Everything an orkflow or a run belongs to is scoped by a namespace.\n" +
			"--namespace picks one for a single command; `orkctl namespace use`\n" +
			"remembers one so it does not have to be typed again.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// The config file is read at most once, and only when a flag
			// or the environment left something for it to answer.
			var cfg *cliConfig
			config := func() (cliConfig, error) {
				if cfg == nil {
					loaded, err := loadConfig()
					if err != nil {
						return cliConfig{}, err
					}
					cfg = &loaded
				}
				return *cfg, nil
			}
			if err := g.resolveServer(cmd, config); err != nil {
				return err
			}
			if err := g.resolveNamespace(cmd, config); err != nil {
				return err
			}
			g.timeoutChanged = cmd.Flags().Changed("timeout")
			if err := g.validate(); err != nil {
				return err
			}
			return g.resolveCredentials()
		},
	}
	root.PersistentFlags().StringVar(&g.server, "server", defaultServer,
		"OrkStack engine base URL (env "+serverEnvVar+", then the config file's server)")
	// No backticks in this usage string: Cobra reads a backticked span as the
	// flag's value placeholder, so "`orkctl namespace use`" would render as
	// `-n, --namespace orkctl namespace use`.
	root.PersistentFlags().StringVarP(&g.namespace, "namespace", "n", "",
		"namespace to act in (env "+namespaceEnvVar+
			", then the one saved by 'orkctl namespace use'; default \"default\")")
	root.PersistentFlags().BoolVarP(&g.json, "json", "j", false,
		"emit machine-readable JSON instead of human tables")
	root.PersistentFlags().DurationVar(&g.timeout, "timeout", 30*time.Second,
		"timeout for each API request (0 disables it)")
	root.PersistentFlags().StringVar(&g.creds.tokenFile, "token-file", "",
		"a file holding the bearer token to present, read at every request (env "+tokenFileEnvVar+")")
	root.PersistentFlags().StringVar(&g.creds.caFile, "ca-file", "",
		"PEM roots to trust for an https server, in place of the system's")
	root.PersistentFlags().StringVar(&g.creds.clientCert, "client-cert", "",
		"a PEM client certificate to present to an https server (with --client-key)")
	root.PersistentFlags().StringVar(&g.creds.clientKey, "client-key", "",
		"the PEM private key of --client-cert")
	root.PersistentFlags().StringVar(&g.creds.serviceAccount, "service-account", "",
		"sign each request as this service account (with --service-account-key; env "+serviceAccountEnvVar+")")
	root.PersistentFlags().StringVar(&g.creds.serviceAccountKey, "service-account-key", "",
		"the PEM private key --service-account signs with (env "+serviceAccountKeyEnvVar+")")
	root.PersistentFlags().StringVar(&g.creds.reason, "break-glass-reason", "",
		"why a break-glass key is used, sent with each request (required with --service-account break-glass; env "+breakGlassReasonEnvVar+")")

	root.AddCommand(
		newAdminCommand(g),
		newAuthCommand(g),
		newAutomationCommand(g),
		newCacheCommand(g),
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
		newEventSourceCommand(g),
		newWebhookCommand(g),
		newVersionCommand(g),
		newWhoamiCommand(g),
		newLoginCommand(g),
		newLogoutCommand(g),
	)
	for _, child := range root.Commands() {
		refuseUnknownSubcommands(child)
	}
	return root, g
}

// refuseUnknownSubcommands makes every group at or below cmd — a command
// whose only job is to hold subcommands — a command of its own: bare, it
// prints its help and exits 0; with an argument that names no subcommand, it
// fails with `unknown command "x" for "orkctl run"` (and cobra's nearest
// suggestions) and exits 1.
//
// Cobra alone answers a non-runnable group's stray argument with its help and
// a nil error before it ever validates the arguments, so `orkctl run lsit`
// would read as success to a script. A group that already runs (stats) keeps
// its own RunE and argument check. The root is left to cobra, whose own
// handling of an unknown top-level command already fails.
func refuseUnknownSubcommands(cmd *cobra.Command) {
	if cmd.HasSubCommands() && !cmd.Runnable() {
		cmd.Args = unknownSubcommand
		cmd.RunE = func(c *cobra.Command, _ []string) error { return c.Help() }
	}
	for _, child := range cmd.Commands() {
		refuseUnknownSubcommands(child)
	}
}

// unknownSubcommand is the argument check refuseUnknownSubcommands installs:
// no positional argument is valid where only a subcommand may follow.
func unknownSubcommand(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	message := fmt.Sprintf("unknown command %q for %q", args[0], cmd.CommandPath())
	// Cobra applies its default distance only on the root's own path.
	if cmd.SuggestionsMinimumDistance <= 0 {
		cmd.SuggestionsMinimumDistance = 2
	}
	if suggestions := cmd.SuggestionsFor(args[0]); len(suggestions) > 0 {
		message += "\n\nDid you mean this?\n\t" + strings.Join(suggestions, "\n\t")
	}
	return errors.New(message)
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
func (g *globalFlags) resolveNamespace(cmd *cobra.Command, config func() (cliConfig, error)) error {
	if cmd.Flags().Changed("namespace") {
		return nil
	}
	if fromEnv := os.Getenv(namespaceEnvVar); fromEnv != "" {
		g.namespace = fromEnv
		return nil
	}
	cfg, err := config()
	if err != nil {
		return err
	}
	g.namespace = cfg.Namespace
	return nil
}

// resolveServer settles which engine this invocation talks to, by the same
// precedence as the namespace: the --server flag, ORK_SERVER, the config
// file's server, then defaultServer.
func (g *globalFlags) resolveServer(cmd *cobra.Command, config func() (cliConfig, error)) error {
	if cmd.Flags().Changed("server") {
		return nil
	}
	if fromEnv := os.Getenv(serverEnvVar); fromEnv != "" {
		g.server = fromEnv
		return nil
	}
	cfg, err := config()
	if err != nil {
		return err
	}
	if cfg.Server != "" {
		g.server = cfg.Server
	}
	return nil
}

// closeIdleConnections releases the transport's sockets once the command ends.
func (g *globalFlags) closeIdleConnections() {
	if g.client == nil {
		return
	}
	g.client.CloseIdleConnections()
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
		transport.TLSClientConfig = g.creds.tls
	} else {
		transport.Protocols.SetUnencryptedHTTP2(true)
	}
	var rt http.RoundTripper = connectionEndTransport{Transport: transport}
	if c := g.creds; c.signer != nil && c.clientCert == "" && c.tokenFile == "" {
		rt = &authn.AssertionTransport{Base: rt, Signer: c.signer, Account: c.serviceAccount,
			Audience: strings.TrimSuffix(g.server, "/"), Reason: c.reason}
	}
	g.client = &http.Client{Transport: rt}
	return g.client
}

// connectionEndTransport marks a response body whose read failed because the
// connection under it ended (connectionEndedError). It reads the HTTP layer
// because connect-go cannot say: a connection that ends and an HTTP stream
// that ends cleanly partway through a message both reach a caller as INTERNAL
// or UNKNOWN wrapping io.ErrUnexpectedEOF, some with the same text, and only
// the first is an engine going away.
type connectionEndTransport struct {
	*http.Transport
}

func (t connectionEndTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.Transport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	response.Body = connectionEndBody{ReadCloser: response.Body}
	return response, nil
}

type connectionEndBody struct {
	io.ReadCloser
}

func (b connectionEndBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if err != nil && err != io.EOF && connectionEnded(err) {
		err = &connectionEndedError{err: err}
	}
	return n, err
}

// connectionEndedError is a response body's read that failed because the
// connection to the engine ended. Its text is the cause's, unchanged.
type connectionEndedError struct {
	err error
}

func (e *connectionEndedError) Error() string { return e.err.Error() }
func (e *connectionEndedError) Unwrap() error { return e.err }

// goAwayEnded opens the error Go's HTTP/2 client gives a stream whose
// connection closed after a GOAWAY. Its type is internal to net/http, so its
// text is all there is to read; TestRunWatch_ResumesAfterAGoAwayAndAClose
// holds a real one to this.
const goAwayEnded = "http2: server sent GOAWAY and closed the connection"

// connectionEnded reports whether err, a response body's read error, is the
// connection's end and not the stream's: the connection closed under the read
// (the HTTP/2 client's io.ErrUnexpectedEOF for a FIN, HTTP/1.1's for a cut
// body), closed after a GOAWAY, or failed a network read. A peer's RST_STREAM
// is none of them, and neither is the clean end of an HTTP stream, io.EOF.
func connectionEnded(err error) bool {
	if errors.Is(err, io.ErrUnexpectedEOF) || strings.HasPrefix(err.Error(), goAwayEnded) {
		return true
	}
	var network *net.OpError
	return errors.As(err, &network) && network.Op == "read"
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
	var interceptors []connect.Interceptor
	switch c := g.creds; {
	case c.clientCert != "", c.signer != nil && c.tokenFile == "":
		// A certificate, or an assertion the transport signs.
	case c.tokenFile != "":
		interceptors = append(interceptors, authn.BearerFromFile{Path: c.tokenFile, Label: "token file"})
	case strings.HasPrefix(g.server, "https://") || loopbackServer(g.server):
		// The stored login, if there is one: last in precedence (authn-authz.md §9).
		var warn io.Writer = os.Stderr
		if g.stderr != nil {
			warn = g.stderr
		}
		interceptors = append(interceptors, loginBearer{server: g.server, warn: warn})
	}
	if principal := os.Getenv(principalEnvVar); principal != "" {
		interceptors = append(interceptors, principalInterceptor{principal: principal, g: g})
	}
	if len(interceptors) == 0 {
		return nil
	}
	return []connect.ClientOption{connect.WithInterceptors(interceptors...)}
}

// principalCheckTimeout bounds the one question of the engine's mode: an
// engine that does not answer in time is not warned about.
const principalCheckTimeout = 3 * time.Second

// checkPrincipal warns, once, when the engine does not record ORK_PRINCIPAL:
// one that authenticates its callers (auth mode observe or enforce, or one
// that refuses an unauthenticated caller) reads the header nowhere
// (authn-authz.md §1). An engine that predates auth modes, or that does not
// answer, is not warned about.
func (g *globalFlags) checkPrincipal(ctx context.Context, principal string) {
	g.principalChecked.Do(func() {
		ctx, cancel := context.WithTimeout(ctx, principalCheckTimeout)
		defer cancel()
		client := orkstackv1connect.NewSystemServiceClient(g.httpClient(), g.baseURL(),
			connect.WithInterceptors(principalInterceptor{principal: principal}))
		resp, err := client.GetSystemInfo(ctx, connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
		mode := ""
		switch {
		case connect.CodeOf(err) == connect.CodeUnauthenticated:
			mode = "observe or enforce"
		case err == nil && (resp.Msg.GetAuthMode() == "observe" || resp.Msg.GetAuthMode() == "enforce"):
			mode = resp.Msg.GetAuthMode()
		}
		if mode == "" {
			return
		}
		w := g.stderr
		if w == nil {
			w = os.Stderr
		}
		fmt.Fprintf(w, "orkctl: warning: ORK_PRINCIPAL is set, but this engine authenticates its callers (auth mode %s): "+
			"it records the caller it verified and never reads ORK_PRINCIPAL\n", mode)
	})
}

// principalInterceptor sets the Ork-Principal header on every outgoing
// request. It is a full interceptor rather than a unary one so a streaming
// read carries the same assertion as a write. With g set, the first
// request first checks, once, that the engine records the header at all.
type principalInterceptor struct {
	principal string
	g         *globalFlags
}

func (p principalInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if p.g != nil {
			p.g.checkPrincipal(ctx, p.principal)
		}
		req.Header().Set(principalHeader, p.principal)
		return next(ctx, req)
	}
}

func (p principalInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		if p.g != nil {
			p.g.checkPrincipal(ctx, p.principal)
		}
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

// engineInterrupted reports whether err ended a call because the engine, or
// the connection to it, went away under it — what a watch rides out and
// resumes: an UNAVAILABLE answer, a CANCELED that ctx, the watch's own
// context, did not cause, or a connection that ended under a response body.
//
// From connect-go v1.21.1 a connection the engine reset reaches a client as
// CANCELED when the client's read is the first to meet the reset (error.go,
// wrapIfContextError's isClientDisconnected). A connection the engine closes
// (a FIN, what a replica that exits sends), or one whose reset the client's own
// write met first — an HTTP/2 acknowledgement or flow-control update, whose
// error Go's HTTP/2 client drops — is UNAVAILABLE before any response, and
// INTERNAL between messages or UNKNOWN inside one (envelope.go,
// envelopeReader.Read); connectionEndedError says which of those it was.
//
// Nothing the engine said is resumed: its own error arrives in the stream's
// end message, with whatever code it chose, and never as a failed read. Nor is
// an HTTP stream the engine ended cleanly partway through a message on a
// connection that stays up, or a stream it reset (a handler's panic). A Ctrl-C
// ends ctx, so it ends the watch whichever way the call failed.
func engineInterrupted(ctx context.Context, err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable:
		return true
	case connect.CodeCanceled:
		return ctx.Err() == nil
	default:
		var ended *connectionEndedError
		return errors.As(err, &ended) && ctx.Err() == nil
	}
}
