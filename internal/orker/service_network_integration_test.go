//go:build integration

package orker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// A service's network (docs/SECURITY_MODEL.md, "Service containers"): a
// container on it reaches what lies outside it, as a source reaches its
// broker, and reaches no container on another service's network or on the
// runtime's default one. The broker stands where a broker on the same host
// would: a container on an ordinary network of its own, publishing its port
// on the host's address. A service reaches it through the host's address
// translation at that port, as it reaches any address the host routes to,
// and never at the broker container's own address. A network with no route
// out (an internal one) reaches neither, which is what this test exists to
// catch. (A listener of the host itself is no stand-in: a host firewall may
// admit containers' traffic to the host from the default bridge only, and
// what a host exposes at a service network's gateway is the reach probe's
// to measure.)
// The positive control is a container on the default network, which reaches
// the default network's listener, so a closed answer is the network's and
// not a broken nc.
func TestIntegration_ServiceNetwork_ReachesOutNotSideways(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	rt := orker.NewDockerRuntime()
	_, err := rt.Pull(ctx, orker.BindProbeImage)
	must.OKf(t, err, "pull %s", orker.BindProbeImage)
	owner := proof.OrkerID("svcnet")
	labels := map[string]string{"ork.orker": owner}

	hostIP := hostAddress(t)
	cli := containerCLI(t)
	docker := func(args ...string) string {
		out, err := exec.CommandContext(ctx, cli, args...).CombinedOutput()
		must.OKf(t, err, "%s %s: %s", cli, strings.Join(args, " "), out)
		return strings.TrimSpace(string(out))
	}
	brokerNet := fmt.Sprintf("%s-broker-%d", owner, time.Now().UnixNano()%1e6)
	docker("network", "create", "--label", "ork.orker="+owner, brokerNet)
	t.Cleanup(func() { _ = exec.Command(cli, "network", "rm", brokerNet).Run() })
	brokerID := docker("run", "-d", "--label", "ork.orker="+owner, "--network", brokerNet, "-p", "0.0.0.0::8080",
		orker.BindProbeImage, "nc", "-lk", "-p", "8080", "-e", "true")
	t.Cleanup(func() { _ = exec.Command(cli, "rm", "-f", brokerID).Run() })
	_, brokerPort, err := net.SplitHostPort(strings.Fields(docker("port", brokerID, "8080/tcp"))[0])
	must.OKf(t, err, "the broker's published port")
	brokerContainer := docker("inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", brokerID)

	// A runtime that cannot keep a service's network apart refuses to make
	// one, by name: the refusal is what such a runtime must show, and making
	// one anyway would be the failure.
	refusal := rt.ServiceNetworksRefused(ctx)
	probeName := fmt.Sprintf("%s-refusal-%d", owner, time.Now().UnixNano()%1e6)
	id, err := rt.CreateServiceNetwork(ctx, probeName, labels)
	if refusal != nil {
		if !errors.Is(err, orker.ErrServiceNetworkUnsupported) {
			if id != "" {
				_ = rt.RemoveServiceNetwork(ctx, id)
			}
			t.Fatalf("this runtime cannot keep a service network apart (%v), yet creating one answered %v", refusal, err)
		}
		t.Logf("refused, as this runtime must: %v", err)
		return
	}
	must.OKf(t, err, "create a service network")
	must.OK(t, rt.RemoveServiceNetwork(ctx, id))
	network := func(name string) string {
		id, err := rt.CreateServiceNetwork(ctx, fmt.Sprintf("%s-%s-%d", owner, name, time.Now().UnixNano()%1e6), labels)
		must.OKf(t, err, "create service network %s", name)
		t.Cleanup(func() { _ = rt.RemoveServiceNetwork(context.WithoutCancel(ctx), id) })
		return id
	}
	mine, theirs := network("mine"), network("theirs")

	run := func(name, netMode, netID, script string) (orker.ContainerID, *bytes.Buffer) {
		spec := orker.ContainerSpec{
			Image: orker.BindProbeImage, Command: []string{"sh", "-c", script}, WorkDir: "/",
			Network: netMode, ServiceNetworkID: netID, CPUs: "0.5", Memory: "64Mi", PidsLimit: 64,
			TmpfsPath: "/tmp", TmpfsBytes: 1 << 20,
			Labels:         map[string]string{"ork.orker": owner, "ork.task": "svcnet-" + name},
			ReadOnlyRootfs: true, DropAllCapabilities: true, NoNewPrivileges: true, SeccompProfile: orker.SeccompBuiltin, Init: true,
		}
		id, err := rt.Create(ctx, spec)
		if id != "" {
			t.Cleanup(func() { _ = rt.Remove(context.WithoutCancel(ctx), id) })
		}
		must.OKf(t, err, "create %s", name)
		var out bytes.Buffer
		must.OKf(t, rt.Start(ctx, id, &out, &out), "start %s", name)
		return id, &out
	}
	listener := `exec nc -lk -p 8080 -e true`
	theirListener, _ := run("theirs", orker.NetworkService, theirs, listener)
	defaultListener, _ := run("default", orker.NetworkDefault, "", listener)
	theirAddr, err := rt.ContainerAddress(ctx, theirListener, theirs)
	must.OKf(t, err, "their listener's address")
	defaultAddr, err := rt.ContainerAddress(ctx, defaultListener, "")
	must.OKf(t, err, "the default listener's address")

	probe := func(host, port string) string {
		return fmt.Sprintf(`if nc -z -w 3 %s %s; then echo "%s:%s open"; else echo "%s:%s closed"; fi; `,
			host, port, host, port, host, port)
	}
	// A listener's loop may still be starting: the control asks until the
	// default network's listener answers, before it reports.
	ready := fmt.Sprintf(`for i in $(seq 1 50); do nc -z -w 1 %s 8080 && break; sleep 0.2; done; `, defaultAddr)
	ask := func(name, netMode, netID, script string) string {
		id, out := run(name, netMode, netID, script)
		status, err := rt.Wait(ctx, id)
		must.OKf(t, err, "wait %s", name)
		if status.Code != 0 {
			t.Fatalf("%s exited %d:\n%s", name, status.Code, out.String())
		}
		return out.String()
	}
	control := ask("control", orker.NetworkDefault, "", ready+probe(defaultAddr, "8080"))
	if !strings.Contains(control, defaultAddr+":8080 open") {
		t.Fatalf("positive control: the default network does not reach its own listener:\n%s", control)
	}
	seen := ask("service", orker.NetworkService, mine,
		probe(hostIP, brokerPort)+probe(brokerContainer, "8080")+probe(theirAddr, "8080")+probe(defaultAddr, "8080"))
	for _, want := range []string{hostIP + ":" + brokerPort + " open", brokerContainer + ":8080 closed",
		theirAddr + ":8080 closed", defaultAddr + ":8080 closed"} {
		if !strings.Contains(seen, want) {
			t.Errorf("from a service network: want %q, saw:\n%s", want, seen)
		}
	}
}

// hostAddress is the host's own address on its default route, which a
// container reaches through its network's gateway. No packet is sent: a
// UDP socket only chooses its source address.
func hostAddress(t *testing.T) string {
	t.Helper()
	c, err := net.Dial("udp4", "192.0.2.1:9")
	if err != nil {
		infraAbsent(t, "the host has no IPv4 default route: %v", err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}
