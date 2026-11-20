//go:build integration

package orker_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// The reach probe on a real runtime (docs/DEPLOY.md, "Source hosts"): an
// orker image built from this tree, its containers on networks made as a
// service's are, and the Orker's judgement. Its positive control is an
// address the operator lists that a container on the host does answer (a
// published port): the probe must report it reached, so a clean answer
// elsewhere is the network's and not a probe that dials nothing.
func TestIntegration_ReachProbe_RunsAndJudges(t *testing.T) {
	requireDocker(t)
	registry := localcatalog.Registry()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cli := containerCLI(t)
	run := func(args ...string) string {
		out, err := exec.CommandContext(ctx, containerCLI(t), args...).CombinedOutput()
		must.OKf(t, err, "%s %s: %s", cli, strings.Join(args, " "), out)
		return strings.TrimSpace(string(out))
	}
	owner := proof.OrkerID("reachprobe")
	image := buildProbeImage(ctx, t, cli, registry, owner)

	listenerNet := owner + "-open"
	run("network", "create", "--label", "ork.orker="+owner, listenerNet)
	t.Cleanup(func() { _ = exec.Command(containerCLI(t), "network", "rm", listenerNet).Run() })
	open := run("run", "-d", "--label", "ork.orker="+owner, "--network", listenerNet, "-p", "0.0.0.0::8080", image,
		"/usr/local/bin/orker", "probe", "reach", "listen", "8080")
	t.Cleanup(func() { _ = exec.Command(containerCLI(t), "rm", "-f", open).Run() })
	_, port, _ := strings.Cut(strings.Fields(run("port", open, "8080/tcp"))[0], ":")
	planted := hostAddress(t) + ":" + port
	// A listener on the host's own address too: a network with a route out
	// reaches at least one of the two (a host firewall may close this one,
	// a runtime's strict isolation the published port).
	hostListener, err := net.Listen("tcp", "0.0.0.0:0")
	must.OKf(t, err, "listen on the host")
	t.Cleanup(func() { _ = hostListener.Close() })
	go func() {
		for {
			c, err := hostListener.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	plantedHost := net.JoinHostPort(hostAddress(t), fmt.Sprint(hostListener.Addr().(*net.TCPAddr).Port))

	d := &orker.OCIDispatcher{Runtime: orker.NewDockerRuntime(), OrkerID: owner}
	got := d.RunReachProbe(ctx, orker.ReachProbeConfig{Image: image, OperatorAddresses: []string{planted, plantedHost}, Cluster: "127.0.0.1:1"})
	r := got.Result
	if !r.Ran {
		t.Fatalf("the probe did not run: %+v", r)
	}
	if !got.AnswerFS {
		t.Error("a service's answer filesystem could not be made and opened")
	}
	joined := strings.Join(r.Fitness.Failures, "\n")
	if !strings.Contains(joined, "question 1: tcp reached "+planted) && !strings.Contains(joined, "question 1: tcp reached "+plantedHost) {
		t.Fatalf("positive control: the probe reported neither planted address (%s, %s) reached:\n%s", planted, plantedHost, joined)
	}
	for _, f := range r.Fitness.Failures {
		if strings.HasPrefix(f, "question 4:") || strings.Contains(f, "not asked") {
			t.Errorf("a canary was reached, or a question went unasked: %s", f)
		}
	}
	t.Logf("what this host answers to a service network:\n%s", joined)
	out := run("ps", "-a", "--filter", "label=ork.orker="+owner, "--format", "{{.ID}} {{.Label \"ork.task\"}}")
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, " reachprobe") {
			t.Errorf("a probe container was left: %s", line)
		}
	}
	if nets := run("network", "ls", "--filter", "label=ork.orker="+owner, "--format", "{{.Name}}"); strings.Contains(nets, "ork-reach-") {
		t.Errorf("a probe network was left: %s", nets)
	}
}

// buildProbeImage builds this tree's orker, whose `orker probe reach` the
// reach probe runs, into an image of that one binary, pushes it under a tag
// of this process's own and returns it by digest.
func buildProbeImage(ctx context.Context, t *testing.T, cli, registry, owner string) string {
	t.Helper()
	dir := t.TempDir()
	bin, err := os.ReadFile(testbin.StaticPath(t, "./cmd/orker"))
	must.OKf(t, err, "read the orker binary")
	must.OK(t, os.WriteFile(filepath.Join(dir, "orker"), bin, 0o755))
	dockerfile := "FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99\n" +
		"COPY orker /usr/local/bin/orker\n"
	must.OK(t, os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o644))
	tag := fmt.Sprintf("%s/orkstack-test/reach-probe:%s", registry, owner)
	for _, args := range [][]string{{"build", "-q", "-t", tag, dir}, {"push", "-q", tag}} {
		if out, err := exec.CommandContext(ctx, containerCLI(t), args...).CombinedOutput(); err != nil {
			t.Fatalf("%s %s: %v\n%s", cli, strings.Join(args, " "), err, out)
		}
	}
	t.Cleanup(func() { _ = exec.Command(containerCLI(t), "rmi", "-f", tag).Run() })
	out, err := exec.CommandContext(ctx, containerCLI(t), "inspect", "--format", "{{index .RepoDigests 0}}", tag).CombinedOutput()
	must.OKf(t, err, "the probe image's digest: %s", out)
	return strings.TrimSpace(string(out))
}
