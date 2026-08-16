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
)

// Exit codes. Anything non-zero means the command did not succeed; the
// distinct run codes let scripts tell a failed run from a failed command.
const (
	exitOK          = 0
	exitError       = 1
	exitRunFailed   = 2
	exitRunCanceled = 3
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
	json           bool
	timeout        time.Duration
	timeoutChanged bool

	client *http.Client
}

// newRootCommand builds the command tree and the shared flag state.
func newRootCommand() (*cobra.Command, *globalFlags) {
	g := &globalFlags{}
	root := &cobra.Command{
		Use:   "orkctl",
		Short: "Control and observe OrkStack orkflows",
		Long: "orkctl drives the OrkStack engine: apply orkflow DSL files, submit runs,\n" +
			"watch their events live, and inspect runs, orkflows, and plugins.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("server") {
				if fromEnv := os.Getenv(serverEnvVar); fromEnv != "" {
					g.server = fromEnv
				}
			}
			g.timeoutChanged = cmd.Flags().Changed("timeout")
			return g.validate()
		},
	}
	root.PersistentFlags().StringVar(&g.server, "server", defaultServer,
		"OrkStack engine base URL (env "+serverEnvVar+")")
	root.PersistentFlags().BoolVar(&g.json, "json", false,
		"emit machine-readable JSON instead of human tables")
	root.PersistentFlags().DurationVar(&g.timeout, "timeout", 30*time.Second,
		"timeout for each API request (0 disables it)")

	root.AddCommand(
		newApplyCommand(g),
		newOrkflowCommand(g),
		newRunCommand(g),
		newPluginCommand(g),
		newWebhookCommand(g),
		newVersionCommand(g),
	)
	return root, g
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
	return orkstackv1connect.NewOrkflowServiceClient(g.httpClient(), g.baseURL())
}

func (g *globalFlags) runClient() orkstackv1connect.RunServiceClient {
	return orkstackv1connect.NewRunServiceClient(g.httpClient(), g.baseURL())
}

func (g *globalFlags) pluginClient() orkstackv1connect.PluginServiceClient {
	return orkstackv1connect.NewPluginServiceClient(g.httpClient(), g.baseURL())
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
