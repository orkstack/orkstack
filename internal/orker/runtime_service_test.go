package orker

import (
	"errors"
	"strings"
	"testing"
)

// TestSpec_NoSwapNoCoreNoNewPrivsNoHostPath: a service container's spec
// renders its own network, the bounded answer filesystem owned by the
// Orker, no swap and no core dumps, beside the mandatory profile.
func TestSpec_NoSwapNoCoreNoNewPrivsNoHostPath(t *testing.T) {
	spec := validSpec()
	spec.Network = NetworkService
	spec.ServiceNetworkID = strings.Repeat("a", 64)
	spec.ServiceTmpfs = &ServiceTmpfs{Path: "/orkstack/service/answer", Bytes: 8 << 20, Inodes: 128, UID: 1000, GID: 1000}
	spec.NoSwap, spec.NoCoreDumps, spec.RunAsOrker = true, true, true
	flags, err := dockerContainerFlags(spec, "ork-svc", dockerDialect)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.Join(flags, " ")
	for _, want := range []string{
		"--network " + strings.Repeat("a", 64),
		"--tmpfs /orkstack/service/answer:rw,nosuid,nodev,noexec,size=8388608,nr_inodes=128,mode=0700,uid=1000,gid=1000",
		"--memory-swap " + dockerMemory(spec.Memory),
		"--ulimit core=0",
		"--security-opt no-new-privileges",
		"--cap-drop=ALL",
		"--read-only",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("flags lack %q:\n%s", want, line)
		}
	}
	for _, refused := range []string{"--privileged", "--pid=host", "--network host", "docker.sock"} {
		if strings.Contains(line, refused) {
			t.Errorf("flags hold %q", refused)
		}
	}
}

func TestSpec_ServiceNetworkRules(t *testing.T) {
	for name, edit := range map[string]func(*ContainerSpec){
		"service without an id":    func(s *ContainerSpec) { s.Network = NetworkService },
		"service with a name":      func(s *ContainerSpec) { s.Network, s.ServiceNetworkID = NetworkService, "bridge" },
		"an id on the default net": func(s *ContainerSpec) { s.ServiceNetworkID = strings.Repeat("a", 64) },
		"host network":             func(s *ContainerSpec) { s.Network = "host" },
		"answer fs with no size":   func(s *ContainerSpec) { s.ServiceTmpfs = &ServiceTmpfs{Path: "/a", Inodes: 1} },
		"answer fs relative":       func(s *ContainerSpec) { s.ServiceTmpfs = &ServiceTmpfs{Path: "a", Bytes: 1, Inodes: 1} },
		"answer fs with options":   func(s *ContainerSpec) { s.ServiceTmpfs = &ServiceTmpfs{Path: "/a,exec", Bytes: 1, Inodes: 1} },
	} {
		spec := validSpec()
		edit(&spec)
		if _, err := dockerContainerFlags(spec, "ork-svc", dockerDialect); err == nil {
			t.Errorf("%s: rendered", name)
		}
	}
}

func TestSpec_AnswerFSPathCannotAddOptions(t *testing.T) {
	for _, path := range []string{"/a,exec", "/a:rw,exec", "/a,size=0"} {
		spec := validSpec()
		spec.ServiceTmpfs = &ServiceTmpfs{Path: path, Bytes: 1 << 20, Inodes: 64}
		if _, err := dockerContainerFlags(spec, "ork-svc", dockerDialect); err == nil {
			t.Errorf("path %q rendered", path)
		}
	}
}

// TestServiceNetworkArgs: a service's network routes out through the
// host's address translation (14 §7: it reaches its broker), is never
// internal and never the host's, and is cut off from every other network:
// Docker separates its bridges by default and is told to keep containers
// on one network apart too; Podman separates nothing unless told, so it is
// told strictly, which also keeps it from its default network.
func TestServiceNetworkArgs(t *testing.T) {
	labels := map[string]string{"ork.orker": "o-1", "ork.source": "evs-1"}
	tests := []struct {
		name    string
		x       dialect
		want    []string
		refused []string
	}{
		{name: "docker", x: dockerDialect,
			want:    []string{"network create", "--driver bridge", "-o com.docker.network.bridge.enable_icc=false", "--label ork.service=1", "--label ork.orker=o-1", "--label ork.source=evs-1"},
			refused: []string{"--internal", "isolated", "host", "isolate="}},
		{name: "podman", x: dialect{cli: CLIPodman, engine: CLIPodman},
			want:    []string{"network create", "--driver bridge", "-o isolate=strict", "--label ork.service=1"},
			refused: []string{"--internal", "enable_icc", "host"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, err := serviceNetworkArgs(tt.x, "ork-svc-1", labels)
			if err != nil {
				t.Fatal(err)
			}
			line := strings.Join(args, " ")
			if !strings.HasSuffix(line, " ork-svc-1") {
				t.Errorf("the name is not last: %s", line)
			}
			for _, w := range tt.want {
				if !strings.Contains(line, w) {
					t.Errorf("lacks %q: %s", w, line)
				}
			}
			for _, r := range tt.refused {
				if strings.Contains(line, r) {
					t.Errorf("holds %q: %s", r, line)
				}
			}
		})
	}
	for _, bad := range []string{"", "-x", "a b", "a=b,c"} {
		if _, err := serviceNetworkArgs(dockerDialect, bad, nil); err == nil {
			t.Errorf("name %q rendered", bad)
		}
	}
	if _, err := serviceNetworkArgs(dockerDialect, "n", map[string]string{"k": "a\nb"}); err == nil {
		t.Error("a label value with a newline rendered")
	}
}

// TestParseContainerAddress: a container's IPv4 address on one network,
// from the runtime's `.NetworkSettings.Networks`, by the network's id (a
// service network) or name (the default bridge); nothing else is guessed.
func TestParseContainerAddress(t *testing.T) {
	id := strings.Repeat("b", 64)
	raw := `{"bridge":{"NetworkID":"` + strings.Repeat("c", 64) + `","IPAddress":"172.17.0.3"},` +
		`"ork-svc-1":{"NetworkID":"` + id + `","IPAddress":"172.20.0.2"},"empty":{"NetworkID":"x","IPAddress":""}}`
	tests := []struct {
		network, want string
		ok            bool
	}{
		{network: id, want: "172.20.0.2", ok: true},
		{network: "bridge", want: "172.17.0.3", ok: true},
		{network: strings.Repeat("d", 64)},
		{network: "empty"},
	}
	for _, tt := range tests {
		got, err := parseContainerAddress(raw, tt.network)
		if (err == nil) != tt.ok || got != tt.want {
			t.Errorf("network %s: %q, %v; want %q", tt.network, got, err, tt.want)
		}
	}
	for _, bad := range []string{"", "null", "[]", `{"n":{"NetworkID":"` + id + `","IPAddress":"not-an-ip"}}`} {
		if got, err := parseContainerAddress(bad, id); err == nil {
			t.Errorf("%q read as %q", bad, got)
		}
	}
}

func TestParseNetworkGateway(t *testing.T) {
	for raw, want := range map[string]string{
		"172.20.0.1 ":         "172.20.0.1",
		"fd00::1 172.21.0.1 ": "172.21.0.1",
		"":                    "",
		"not-an-address ":     "",
		"fd00::1 ":            "",
	} {
		got, err := parseNetworkGateway(raw)
		if (err == nil) != (want != "") || got != want {
			t.Errorf("%q: %q, %v; want %q", raw, got, err, want)
		}
	}
}

// TestVerifyServiceNetwork: an engine that ignored an option rather than
// refusing it, or an engine taken for another, leaves a network that is not
// cut off; the Orker reads the network back and refuses it.
func TestVerifyServiceNetwork(t *testing.T) {
	podman := dialect{cli: CLIPodman, engine: CLIPodman}
	tests := []struct {
		name string
		x    dialect
		raw  string
		ok   bool
	}{
		{"docker as asked", dockerDialect, `{"com.docker.network.bridge.enable_icc":"false","com.docker.network.bridge.name":"orksvc012345678"}|false|false`, true},
		{"docker ignored icc", dockerDialect, `{}|false|false`, false},
		{"docker made it internal", dockerDialect, `{"com.docker.network.bridge.enable_icc":"false","com.docker.network.bridge.name":"orksvc012345678"}|true|false`, false},
		{"docker turned IPv6 on", dockerDialect, `{"com.docker.network.bridge.enable_icc":"false","com.docker.network.bridge.name":"orksvc012345678"}|false|true`, false},
		{"podman as asked", podman, `{"isolate":"strict"}|false|false`, true},
		{"podman isolated loosely", podman, `{"isolate":"true"}|false|false`, false},
		{"podman with no options", podman, `null|false|false`, false},
		{"garbage", dockerDialect, `nonsense`, false},
		{"three fields missing", dockerDialect, `{"com.docker.network.bridge.enable_icc":"false","com.docker.network.bridge.name":"orksvc012345678"}|false`, false},
	}
	for _, tt := range tests {
		if err := verifyServiceNetwork(tt.x, tt.raw); (err == nil) != tt.ok {
			t.Errorf("%s: %v", tt.name, err)
		}
	}
	if !strings.Contains(strings.Join(serviceNetworkInspectArgs(podman, "n"), " "), ".IPv6Enabled") ||
		!strings.Contains(strings.Join(serviceNetworkInspectArgs(dockerDialect, "n"), " "), ".EnableIPv6") {
		t.Error("the inspect format does not follow the CLI")
	}
	args, _ := serviceNetworkArgs(dockerDialect, "n", nil)
	if !strings.Contains(strings.Join(args, " "), "--ipv6=false") {
		t.Error("IPv6 is not turned off")
	}
}

func TestParseAddress_RefusesUnspecifiedAndLoopback(t *testing.T) {
	id := strings.Repeat("b", 64)
	for _, ip := range []string{"0.0.0.0", "127.0.0.1"} {
		if got, err := parseContainerAddress(`{"n":{"NetworkID":"`+id+`","IPAddress":"`+ip+`"}}`, id); err == nil {
			t.Errorf("address %s read as %q", ip, got)
		}
		if got, err := parseNetworkGateway(ip + " "); err == nil {
			t.Errorf("gateway %s read as %q", ip, got)
		}
	}
}

// TestServiceBridgeName: every service network's bridge is named with one
// prefix, at most the kernel's 15 bytes, the same for the same network, so
// one host firewall rule on the prefix closes the host's own listeners to
// service networks and to nothing else (docs/DEPLOY.md, "Source hosts").
func TestServiceBridgeName(t *testing.T) {
	a, b := serviceBridgeName("ork-svc-1"), serviceBridgeName("ork-svc-2")
	if !strings.HasPrefix(a, ServiceBridgePrefix) || len(a) > 15 || a == b || a != serviceBridgeName("ork-svc-1") {
		t.Fatalf("bridge names %q, %q", a, b)
	}
	docker, _ := serviceNetworkArgs(dockerDialect, "ork-svc-1", nil)
	if !strings.Contains(strings.Join(docker, " "), "-o com.docker.network.bridge.name="+a) {
		t.Errorf("docker does not name its bridge: %q", docker)
	}
	podman, _ := serviceNetworkArgs(dialect{cli: CLIPodman, engine: CLIPodman}, "ork-svc-1", nil)
	if !strings.Contains(strings.Join(podman, " "), "--interface-name "+a) {
		t.Errorf("podman does not name its bridge: %q", podman)
	}
	if err := verifyServiceNetwork(dockerDialect, `{"com.docker.network.bridge.enable_icc":"false","com.docker.network.bridge.name":"br-1"}|false|false`); err == nil {
		t.Error("a docker bridge named outside the prefix was accepted")
	}
}

func TestContainerAddress_TheOnlyNetwork(t *testing.T) {
	one := `{"podman":{"NetworkID":"` + strings.Repeat("e", 64) + `","IPAddress":"10.88.0.5"}}`
	if got, err := parseContainerAddress(one, ""); err != nil || got != "10.88.0.5" {
		t.Fatalf("the only network: %q, %v", got, err)
	}
	two := `{"a":{"NetworkID":"x","IPAddress":"10.0.0.1"},"b":{"NetworkID":"y","IPAddress":"10.0.0.2"}}`
	if got, err := parseContainerAddress(two, ""); err == nil {
		t.Fatalf("two networks read as one: %q", got)
	}
	if !strings.Contains(networkGatewayFormat(dialect{cli: CLIPodman, engine: CLIPodman}), ".Subnets") ||
		!strings.Contains(networkGatewayFormat(dockerDialect), ".IPAM.Config") {
		t.Fatal("the gateway format does not follow the CLI")
	}
}

// TestServiceNetworkArgs_DockerCLIAgainstPodmanIsRefused: the Podman engine's
// Docker-compatible API does not apply Podman's isolate option, so a network
// it made would not be cut off; such a runtime hosts no service, and says so
// by name rather than failing its probe without a reason.
func TestServiceNetworkArgs_DockerCLIAgainstPodmanIsRefused(t *testing.T) {
	if _, err := serviceNetworkArgs(dialect{cli: CLIDocker, engine: CLIPodman}, "n", nil); !errors.Is(err, ErrServiceNetworkUnsupported) {
		t.Fatalf("the docker CLI against Podman: %v", err)
	}
	for _, x := range []dialect{dockerDialect, {cli: CLIPodman, engine: CLIPodman}, {cli: CLIPodman, engine: CLIPodman, rootless: true}} {
		if _, err := serviceNetworkArgs(x, "n", nil); err != nil {
			t.Errorf("%+v: %v", x, err)
		}
	}
}

// The podman CLI names a network it made by its name; its id is read back.
func TestServiceNetworkIDArgs(t *testing.T) {
	if got := strings.Join(serviceNetworkIDArgs("n"), " "); got != "network inspect --format {{.Id}} n" {
		t.Fatalf("%q", got)
	}
}

// TestSpec_AnswerFSOnPodman: the podman CLI takes no inode count and no
// uid or gid on a tmpfs (it refuses them as unknown options); the mount is
// given to the container's user with U, and its size, mode and the noexec,
// nosuid and nodev flags are spelled as on Docker.
func TestSpec_AnswerFSOnPodman(t *testing.T) {
	spec := validSpec()
	spec.Network, spec.ServiceNetworkID = NetworkService, strings.Repeat("a", 64)
	spec.ServiceTmpfs = &ServiceTmpfs{Path: "/orkstack/service/answer", Bytes: 8 << 20, Inodes: 128, UID: 1000, GID: 1000}
	flags, err := dockerContainerFlags(spec, "ork-svc", dialect{cli: CLIPodman, engine: CLIPodman})
	if err != nil {
		t.Fatal(err)
	}
	line := strings.Join(flags, " ")
	if !strings.Contains(line, "--tmpfs /orkstack/service/answer:rw,nosuid,nodev,noexec,size=8388608,mode=0700,U") {
		t.Errorf("podman's answer filesystem: %s", line)
	}
	for _, refused := range []string{"nr_inodes", "uid=", "gid="} {
		if strings.Contains(line, refused) {
			t.Errorf("podman's flags hold %q, which its CLI refuses: %s", refused, line)
		}
	}
}

func TestContainerAddress_NoAddressOnTheDefaultNetwork(t *testing.T) {
	for _, raw := range []string{"null", "{}", `{"pasta":{"NetworkID":"pasta","IPAddress":""}}`} {
		if _, err := parseContainerAddress(raw, ""); !errors.Is(err, ErrNoContainerAddress) {
			t.Errorf("%s: %v", raw, err)
		}
	}
}
