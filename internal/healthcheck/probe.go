// Package healthcheck is `ork --healthcheck` and `orker --healthcheck`: a
// process asking its own running copy, over the admin listener, whether it is
// ready.
//
// A distroless container has no shell, no curl and no wget, so a container
// health check cannot be written as a command that fetches /readyz. The two
// daemons carry the fetch themselves: the same binary, started with
// --healthcheck, reads the same configuration the daemon read, GETs
// /readyz on the admin address, prints one line, and exits 0 (ready) or 1
// (anything else). Nothing here serves, holds state, or opens a second
// listener; it asks the one the process already has (AGENTS.md §11, operational
// endpoints live on ORK_ADMIN_ADDR).
package healthcheck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/telemetry"
)

// Timeout bounds one check. A health check that waits as long as the listener
// does answers nothing, and an orchestrator's own probe timeout is longer than
// this.
const Timeout = 3 * time.Second

// maxBody bounds how much of an answer is read to explain a refusal.
const maxBody = 256

// AddrError is a problem with the configured admin address itself: not set,
// unreadable, or a port nobody can ask. It is not a verdict on the process.
type AddrError struct {
	msg string
}

func (e *AddrError) Error() string { return e.msg }

func addrError(format string, args ...any) *AddrError {
	return &AddrError{msg: fmt.Sprintf(format, args...)}
}

// ReadyURL is the URL that answers whether the process whose admin listener is
// configured as adminAddr (ORK_ADMIN_ADDR) is ready.
//
// A listener bound to every interface (":9464", "0.0.0.0:9464", "[::]:9464")
// is asked on loopback, "127.0.0.1" or "::1": a wildcard address says where to
// listen, and is no place to connect to. Any other host is used as written,
// because a listener bound to 10.1.2.3 does not answer on loopback.
func ReadyURL(adminAddr string) (string, error) {
	addr := strings.TrimSpace(adminAddr)
	if addr == "" {
		return "", addrError("%s is not set, so this process opens no admin listener to ask; "+
			"set it, for example %s=127.0.0.1:9464", telemetry.AdminAddrVar, telemetry.AdminAddrVar)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", addrError("%s=%q is not host:port (%v)", telemetry.AdminAddrVar, addr, trimAddrError(err))
	}
	if strings.ContainsAny(host, " \t\r\n/\\?#@%") {
		return "", addrError("%s=%q has a host that is not a name or an address", telemetry.AdminAddrVar, addr)
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", addrError("%s=%q has a port that is not a number from 1 to 65535", telemetry.AdminAddrVar, addr)
	}
	if number == 0 {
		return "", addrError("%s=%q asks for a random port, which a health check cannot know; set a fixed port",
			telemetry.AdminAddrVar, addr)
	}
	if host == "" {
		host = "127.0.0.1"
	} else if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		if ip.To4() != nil {
			host = "127.0.0.1"
		} else {
			host = "::1"
		}
	}
	return "http://" + net.JoinHostPort(host, port) + telemetry.ReadyzPath, nil
}

// trimAddrError drops the "address X: " prefix net.SplitHostPort puts on its
// reason, which would repeat the address already quoted.
func trimAddrError(err error) string {
	msg := err.Error()
	if i := strings.Index(msg, ": "); i >= 0 && strings.HasPrefix(msg, "address ") {
		return msg[i+2:]
	}
	return msg
}

// Probe GETs /readyz on the admin listener configured as adminAddr and returns
// nil only when it answers 200. Any other status, a refused or reset
// connection, an answer that takes longer than Timeout, and an address that
// cannot be asked are each an error whose text is one line and says what
// happened.
func Probe(ctx context.Context, adminAddr string) error {
	target, err := ReadyURL(adminAddr)
	if err != nil {
		return err
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("GET %s: %w", target, err)
	}
	// No proxy, no redirects and no connection reuse: this asks one local
	// listener one question, and a 3xx or a proxy's answer is not its answer.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		switch {
		case errors.Is(parent.Err(), context.DeadlineExceeded):
			return fmt.Errorf("GET %s: no answer before the caller's deadline", target)
		case errors.Is(parent.Err(), context.Canceled):
			return fmt.Errorf("GET %s: canceled before it answered", target)
		case errors.Is(err, context.DeadlineExceeded):
			return fmt.Errorf("GET %s: no answer within %s", target, Timeout)
		}
		return fmt.Errorf("GET %s: %w", target, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusOK {
		return nil
	}
	detail := ""
	if body, readErr := io.ReadAll(io.LimitReader(response.Body, maxBody)); readErr == nil {
		line, _, _ := strings.Cut(strings.TrimSpace(string(body)), "\n")
		if line = strings.TrimSpace(line); line != "" && printable(line) {
			detail = ": " + line
		}
	}
	return fmt.Errorf("GET %s: %s%s", target, response.Status, detail)
}

// printable reports whether s is fit to put in a one-line message.
func printable(s string) bool {
	for _, r := range s {
		if r < ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

// Run is the whole of --healthcheck for the process called name ("ork" or
// "orker"): it reads the admin address from the environment the daemon reads
// (the configuration file is already applied to it by then), probes it, and
// returns the exit status. It prints one line, to stdout when ready and to
// stderr with the reason when not: 0 ready, 1 anything else.
func Run(ctx context.Context, name string, getenv func(string) string, stdout, stderr io.Writer) int {
	addr := strings.TrimSpace(getenv(telemetry.AdminAddrVar))
	if err := Probe(ctx, addr); err != nil {
		var problem *AddrError
		if errors.As(err, &problem) {
			fmt.Fprintf(stderr, "%s: cannot check readiness: %v\n", name, err)
		} else {
			fmt.Fprintf(stderr, "%s: not ready: %v\n", name, err)
		}
		return 1
	}
	target, _ := ReadyURL(addr)
	fmt.Fprintf(stdout, "%s: ready (%s)\n", name, target)
	return 0
}
