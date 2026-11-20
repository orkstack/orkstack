// Package dockerport publishes a test's own container on a loopback port no
// other process holds, and reads back which.
//
// A test that finds a port free (listen on :0, close, pass the port on) and
// hands it to `docker run -p` races every other process on the host: another
// test can bind that port in between, and a readiness probe then reaches the
// wrong listener, or the container cannot start. Docker picks the port
// itself when the host port is left out (127.0.0.1::<port>), and `docker
// port` names the one it bound. Once `docker run` has returned, Docker holds
// that port for the container (its userland proxy, the default, or rootless
// Docker's port driver binds it; TestIntegration_AddrReadsThePortDockerHolds
// checks it on the host it runs on), so a probe of it reaches only this
// test's container. A daemon with the userland proxy turned off binds
// nothing, and gives neither guarantee.
//
// A container that must name its own address before it starts (an OIDC
// issuer's URL is in its configuration), or keep it across a stop and start
// (Docker gives a restarted container a new port), cannot leave the port to
// Docker. For
// it, RunPinned picks a free port and runs again on another only when Docker
// itself refuses the bind; a run Docker accepted owns its port.
//
// testcontainers-go does the same with MappedPort; it was weighed and not
// used, because every suite here drives the docker CLI directly, and these
// few functions over that CLI are all it needs.
package dockerport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
)

// Publish is a `docker run -p` value that publishes containerPort on
// loopback, at a host port Docker picks.
func Publish(containerPort string) string { return "127.0.0.1::" + containerPort }

// Addr is the loopback address Docker published container's containerPort
// on: `docker port`'s answer, after `docker run -d -p Publish(...)`.
func Addr(ctx context.Context, container, containerPort string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "port", container, containerPort+"/tcp").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("dockerport: docker port %s %s: %v: %s", container, containerPort, err, bytes.TrimSpace(out))
	}
	addr, err := parse(string(out))
	if err != nil {
		return "", fmt.Errorf("dockerport: %s %s: %w", container, containerPort, err)
	}
	return addr, nil
}

// Port is Addr's port.
func Port(ctx context.Context, container, containerPort string) (int, error) {
	addr, err := Addr(ctx, container, containerPort)
	if err != nil {
		return 0, err
	}
	_, port, _ := net.SplitHostPort(addr)
	return strconv.Atoi(port)
}

// parse is the IPv4 loopback mapping of `docker port`'s output.
func parse(out string) (string, error) {
	for _, line := range strings.Split(out, "\n") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(line))
		if err != nil || host != "127.0.0.1" {
			continue
		}
		if n, err := strconv.Atoi(port); err == nil && n > 0 {
			return net.JoinHostPort(host, port), nil
		}
	}
	return "", fmt.Errorf("no loopback port published: %q", strings.TrimSpace(out))
}

// AddressInUse reports whether a `docker run`'s output is Docker refusing the
// host port because another process holds it.
func AddressInUse(out []byte) bool {
	s := string(out)
	return strings.Contains(s, "port is already allocated") || strings.Contains(s, "address already in use")
}

// RunPinned runs run on a free loopback address, and again on another when
// Docker refused the bind, at most tries times. run is a `docker run` that
// publishes addr and returns its combined output. It returns the address the
// run that started holds.
func RunPinned(ctx context.Context, tries int, run func(addr string) ([]byte, error)) (string, []byte, error) {
	var (
		out []byte
		err error
	)
	for range max(tries, 1) {
		if ctx.Err() != nil {
			return "", nil, ctx.Err()
		}
		addr, ferr := free()
		if ferr != nil {
			return "", nil, ferr
		}
		out, err = run(addr)
		if err == nil {
			return addr, out, nil
		}
		if !AddressInUse(out) {
			return "", out, err
		}
	}
	return "", out, fmt.Errorf("dockerport: every try lost its port to another process: %w: %s", err, bytes.TrimSpace(out))
}

// free is a loopback address nothing held a moment ago.
func free() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("dockerport: pick a port: %w", err)
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}
