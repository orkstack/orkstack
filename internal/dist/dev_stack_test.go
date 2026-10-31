package dist_test

import (
	"regexp"
	"testing"
)

// TestDevCompose_PublishesOnlyOnLoopback: the development stack's services
// are a contributor's own. Temporal's frontend, the object store with its
// default key, an unauthenticated registry and Grafana with anonymous Admin
// are reachable from this host and from nowhere else, so every port the
// stack publishes, in every profile, is bound to 127.0.0.1. Nothing needs
// more: the host's processes, its Docker daemon pulling from the registry and
// the observability gate reach them on loopback, an activity container
// reaches only a test's own server on the host (host.docker.internal), and
// the observability profile's services find each other on the compose network
// (CONTRIBUTING.md, "Linux hosts").
func TestDevCompose_PublishesOnlyOnLoopback(t *testing.T) {
	t.Parallel()
	doc := parseCompose(t, devComposePath, readRepoFile(t, devComposePath))
	hostPort := regexp.MustCompile(`^[0-9]+$`)
	containerPort := regexp.MustCompile(`^[0-9]+(/(tcp|udp))?$`)
	published := 0
	for _, name := range doc.serviceNames() {
		for _, port := range doc.Services[name].Ports {
			parts := splitColon(port)
			if len(parts) != 3 || parts[0] != "127.0.0.1" || !hostPort.MatchString(parts[1]) || !containerPort.MatchString(parts[2]) {
				t.Errorf("service %s publishes %q; a published port is 127.0.0.1:<host port>:<container port>", name, port)
				continue
			}
			published++
		}
		if _, ok := doc.Services[name].Other["network_mode"]; ok {
			t.Errorf("service %s sets network_mode; a service on the host's network publishes every port it listens on", name)
		}
	}
	if published == 0 {
		t.Error("the development stack publishes no port; the rule above checked nothing")
	}
}
