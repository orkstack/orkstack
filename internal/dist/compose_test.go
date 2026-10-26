package dist_test

import (
	"bytes"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/orkstack/orkstack/internal/config"
	"github.com/orkstack/orkstack/internal/dist"
	"github.com/orkstack/orkstack/internal/orker"
)

// deploy/quickstart/compose.yaml is the file a user runs. These tests hold it
// to the quickstart contract (docs/DEPLOY.md, "Running the published
// images"; docs/SECURITY_MODEL.md): what it pins, what it publishes, who sees
// the Docker socket, and how its services find each other. They read the
// committed file, so an edit that breaks the contract fails in the unit gate.
// The smoke (scripts/release-smoke.sh) is the proof that it also runs.

const (
	composePath    = "deploy/quickstart/compose.yaml"
	devComposePath = "deploy/docker-compose.yml"
	envExamplePath = "deploy/quickstart/.env.example"
)

type composeFile struct {
	Name       string                    `yaml:"name"`
	Services   map[string]composeService `yaml:"services"`
	Volumes    map[string]any            `yaml:"volumes"`
	Extensions map[string]any            `yaml:",inline"`
	// source names the file in a failure; it is not part of the document.
	source string
}

type composeService struct {
	Image           string              `yaml:"image"`
	Command         []string            `yaml:"command"`
	Environment     map[string]string   `yaml:"environment"`
	Ports           []string            `yaml:"ports"`
	Volumes         []string            `yaml:"volumes"`
	Healthcheck     *composeHealthcheck `yaml:"healthcheck"`
	Profiles        []string            `yaml:"profiles"`
	Init            bool                `yaml:"init"`
	StopGracePeriod string              `yaml:"stop_grace_period"`
	WorkingDir      string              `yaml:"working_dir"`
	DependsOn       composeDependsOn    `yaml:"depends_on"`
	// Other holds every key not named above, so a setting this file never
	// meant to carry (privileged, network_mode) is seen whatever it is called.
	Other map[string]any `yaml:",inline"`
}

type composeHealthcheck struct {
	Test     []string `yaml:"test"`
	Interval string   `yaml:"interval"`
	Timeout  string   `yaml:"timeout"`
	Retries  int      `yaml:"retries"`
}

type composeDependency struct {
	Condition string `yaml:"condition"`
}

// composeDependsOn is depends_on in either of its forms: a list of names
// (each waits for the service to start) or a map with a condition.
type composeDependsOn map[string]composeDependency

func (d *composeDependsOn) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.SequenceNode {
		var names []string
		if err := node.Decode(&names); err != nil {
			return err
		}
		*d = composeDependsOn{}
		for _, name := range names {
			(*d)[name] = composeDependency{Condition: "service_started"}
		}
		return nil
	}
	var m map[string]composeDependency
	if err := node.Decode(&m); err != nil {
		return err
	}
	*d = m
	return nil
}

func parseCompose(t *testing.T, name string, data []byte) composeFile {
	t.Helper()
	var doc composeFile
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(doc.Services) == 0 {
		t.Fatalf("%s declares no services", name)
	}
	doc.source = name
	return doc
}

func loadCompose(t *testing.T) composeFile {
	t.Helper()
	return parseCompose(t, composePath, readRepoFile(t, composePath))
}

func (c composeFile) service(t *testing.T, name string) composeService {
	t.Helper()
	svc, ok := c.Services[name]
	if !ok {
		t.Fatalf("%s has no service %q", c.source, name)
	}
	return svc
}

func (c composeFile) serviceNames() []string {
	names := make([]string, 0, len(c.Services))
	for name := range c.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

var (
	requiredImageVariable = regexp.MustCompile(`^\$\{ORKSTACK_[A-Z]+_IMAGE:\?[^}]+\}$`)
	digestPinnedImage     = regexp.MustCompile(`^[^\s@$]+@sha256:[0-9a-f]{64}$`)
	taggedAndDigested     = regexp.MustCompile(`^[^\s@$]+:[A-Za-z0-9_][A-Za-z0-9._-]{0,127}@sha256:[0-9a-f]{64}$`)
	exactVersionImage     = regexp.MustCompile(`^[^\s@$:]+:[0-9]+\.[0-9]+\.[0-9]+$`)
)

// imageProblem says why image is not pinned to something that cannot move, or
// returns "". An image is acceptable when a release must name it (a required
// variable), when it names its digest, or when it names an exact version: the
// development stack pins Temporal by version and the quickstart runs the same
// one.
func imageProblem(image string) string {
	switch {
	case requiredImageVariable.MatchString(image):
		return ""
	case taggedAndDigested.MatchString(image), digestPinnedImage.MatchString(image):
		return ""
	case exactVersionImage.MatchString(image):
		return ""
	}
	return "is neither a required ${ORKSTACK_*_IMAGE:?...} variable, nor pinned by digest, nor an exact x.y.z version"
}

func TestQuickstartCompose_ImagesArePinnedOrRequiredVariables(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	required := map[string]string{
		"ork": "ORKSTACK_ORK_IMAGE", "orker": "ORKSTACK_ORKER_IMAGE", "ui": "ORKSTACK_UI_IMAGE", "orkctl": "ORKSTACK_ORKCTL_IMAGE",
	}
	for _, name := range doc.serviceNames() {
		svc := doc.Services[name]
		if problem := imageProblem(svc.Image); problem != "" {
			t.Errorf("service %s: image %q %s", name, svc.Image, problem)
		}
		if variable, ok := required[name]; ok && !strings.HasPrefix(svc.Image, "${"+variable+":?") {
			t.Errorf("service %s: image %q must be the required variable ${%s:?...}, because a release writes it", name, svc.Image, variable)
		}
	}
	// The check is not vacuous.
	for _, floating := range []string{
		"temporalio/temporal", "temporalio/temporal:latest", "temporalio/temporal:1.8", "temporalio/temporal:stable",
		"chrislusf/seaweedfs:latest", "${ORKSTACK_ORK_IMAGE}", "${ORKSTACK_ORK_IMAGE:-ghcr.io/orkstack/ork:latest}",
		"registry:3@sha256:" + strings.Repeat("a", 63),
	} {
		if imageProblem(floating) == "" {
			t.Errorf("the pin check accepts %q", floating)
		}
	}
}

func TestQuickstartCompose_InfraPinsMatchTheDevStack(t *testing.T) {
	t.Parallel()
	quickstart := loadCompose(t)
	dev := parseCompose(t, devComposePath, readRepoFile(t, devComposePath))
	for _, name := range []string{"temporal", "seaweedfs"} {
		got, want := quickstart.service(t, name).Image, dev.service(t, name).Image
		if got != want {
			t.Errorf("service %s runs %q, but deploy/docker-compose.yml, whose pin the proof tests, runs %q", name, got, want)
		}
	}
	// The object store is the same server with the same bucket, and the
	// bucket is the one the engine and the Orker are told to use.
	flag := regexp.MustCompile(`^-bucket=(.+)$`)
	for _, arg := range quickstart.service(t, "seaweedfs").Command {
		if m := flag.FindStringSubmatch(arg); m != nil {
			if bucket := quickstart.service(t, "ork").Environment["ORK_S3_BUCKET"]; m[1] != bucket {
				t.Errorf("seaweedfs creates the bucket %q, but the engine uses %q", m[1], bucket)
			}
			return
		}
	}
	t.Error("the seaweedfs command creates no bucket (-bucket=...)")
}

func TestQuickstartCompose_PublishesOnlyOnLoopback(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	hostPort := regexp.MustCompile(`^([0-9]+|\$\{ORKSTACK_[A-Z_]+-[0-9]+\})$`)
	published := map[string][]string{}
	for _, name := range doc.serviceNames() {
		for _, port := range doc.Services[name].Ports {
			parts := splitColon(port)
			if len(parts) != 3 || parts[0] != "127.0.0.1" || !hostPort.MatchString(parts[1]) {
				t.Errorf("service %s publishes %q; a published port is 127.0.0.1:<port or ${ORKSTACK_*-default}>:<container port>", name, port)
				continue
			}
			published[name] = append(published[name], parts[2])
		}
		if _, ok := doc.Services[name].Other["expose"]; ok {
			t.Errorf("service %s declares expose; nothing but the three published ports is reachable", name)
		}
	}
	// Exactly the Temporal UI, the API and the UI: the admin listeners, the
	// object store and the Temporal frontend stay inside the network.
	want := map[string][]string{"temporal": {"8233"}, "ork": {"8080"}, "ui": {"5173"}}
	if len(published) != len(want) {
		t.Errorf("published ports %v, want %v", published, want)
	}
	for name, ports := range want {
		if !slices.Equal(published[name], ports) {
			t.Errorf("service %s publishes %v, want %v", name, published[name], ports)
		}
	}
}

// hostPathSource reports whether a volume's source is a path on the host, not
// a named volume.
func hostPathSource(source string) bool {
	return strings.HasPrefix(source, "/") || strings.HasPrefix(source, ".") ||
		strings.HasPrefix(source, "~") || strings.HasPrefix(source, "$")
}

func TestQuickstartCompose_OnlyTheOrkerSeesTheDockerSocket(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	touchesSocket := func(volume string) bool {
		parts := splitColon(volume)
		return strings.Contains(parts[0], "docker.sock") || strings.Contains(parts[0], "DOCKER_SOCKET") ||
			(len(parts) > 1 && strings.HasSuffix(parts[1], "docker.sock"))
	}
	for _, name := range doc.serviceNames() {
		svc := doc.Services[name]
		sockets := 0
		for _, volume := range svc.Volumes {
			if touchesSocket(volume) {
				sockets++
			}
		}
		if _, set := svc.Environment["DOCKER_HOST"]; set {
			t.Errorf("service %s sets DOCKER_HOST; only the Orker talks to a daemon, through the socket it mounts", name)
		}
		switch {
		case name == "orker" && sockets != 1:
			t.Errorf("the orker mounts the Docker socket %d times, want once", sockets)
		case name != "orker" && sockets != 0:
			t.Errorf("service %s mounts the Docker socket; whoever has it controls the host, so only the orker may", name)
		}
	}
	if socket := doc.service(t, "orker").Volumes; len(socket) == 0 ||
		socket[0] != "${ORKSTACK_DOCKER_SOCKET-/var/run/docker.sock}:/var/run/docker.sock" {
		t.Errorf("the orker's first volume is %v; want ${ORKSTACK_DOCKER_SOCKET-/var/run/docker.sock}:/var/run/docker.sock", socket)
	}
	// No other service sees a host path at all, but the CLI's read-only
	// bundle; the Orker sees the socket and its work directory and nothing else.
	for _, name := range doc.serviceNames() {
		for _, volume := range doc.Services[name].Volumes {
			parts := splitColon(volume)
			if !hostPathSource(parts[0]) {
				continue
			}
			switch {
			case name == "orker":
			case name == "orkctl" && volume == "./:/work:ro":
			default:
				t.Errorf("service %s mounts the host path %q", name, volume)
			}
		}
	}
	if n := len(doc.service(t, "orker").Volumes); n != 2 {
		t.Errorf("the orker has %d volumes (%v), want its socket and its work directory", n, doc.service(t, "orker").Volumes)
	}
}

func TestQuickstartCompose_TheOrkerWorkdirIsTheSamePathInsideAndOut(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	orker := doc.service(t, "orker")
	var mounts []string
	for _, volume := range orker.Volumes {
		if strings.Contains(volume, "ORKSTACK_WORKDIR") {
			mounts = append(mounts, volume)
		}
	}
	if len(mounts) != 1 {
		t.Fatalf("the orker mounts its work directory %d times (%v), want once", len(mounts), orker.Volumes)
	}
	parts := splitColon(mounts[0])
	if len(parts) != 2 || parts[0] != parts[1] {
		t.Errorf("the orker mounts %q; the work directory must be the same path inside and out, because the Docker "+
			"daemon bind-mounts each task's workspace from the host path the Orker names", mounts[0])
	}
	if got := orker.Environment["ORK_ORKER_WORKDIR"]; got != parts[0] {
		t.Errorf("ORK_ORKER_WORKDIR is %q, but the mount is %q: the Orker would name a path the daemon cannot find", got, parts[0])
	}
	_, optional := composeVariables(mounts[0])
	def, ok := optional["ORKSTACK_WORKDIR"]
	// Under the user's home, never a shared directory such as /tmp: another
	// user who created it first would own it, and could swap a task's
	// directories for links the daemon then mounts.
	if !ok || !strings.HasPrefix(def, "${HOME}/") || !path.IsAbs(strings.TrimPrefix(def, "${HOME}")) ||
		strings.ContainsAny(def, " \t") {
		t.Errorf("ORKSTACK_WORKDIR defaults to %q; want a path under ${HOME} with no space", def)
	}
	if example := parseEnvFile(t, envExamplePath, readRepoFile(t, envExamplePath))["ORKSTACK_WORKDIR"]; example != def {
		t.Errorf(".env.example says ORKSTACK_WORKDIR defaults to %q, the compose file to %q", example, def)
	}
	// The Orker's identity names its lock and its containers' label; a fixed
	// default keeps both stable when the container is recreated.
	if id := orker.Environment["ORK_ORKER_ID"]; !strings.HasPrefix(id, "${ORKSTACK_ORKER_ID-") {
		t.Errorf("ORK_ORKER_ID is %q; want ${ORKSTACK_ORKER_ID-<default>}", id)
	}
}

func TestQuickstartCompose_EngineAndOrkerShareOneStore(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	ork, orker := doc.service(t, "ork"), doc.service(t, "orker")
	// Every setting that decides which Temporal and which store a process
	// talks to is the same on both, absent included.
	for key := range ork.Environment {
		if strings.HasPrefix(key, "ORK_S3_") || strings.HasPrefix(key, "ORK_TEMPORAL_") {
			if _, ok := orker.Environment[key]; !ok {
				t.Errorf("the engine sets %s but the orker does not", key)
			}
		}
	}
	for _, key := range sortedKeys(orker.Environment) {
		if strings.HasPrefix(key, "ORK_S3_") || strings.HasPrefix(key, "ORK_TEMPORAL_") {
			if ork.Environment[key] != orker.Environment[key] {
				t.Errorf("%s is %q for the engine and %q for the orker", key, ork.Environment[key], orker.Environment[key])
			}
		}
	}
	for _, key := range []string{"ORK_DEPLOYMENT_NAME", "ORK_BUILD_ID", "ORK_ORKER_POOL"} {
		if ork.Environment[key] != orker.Environment[key] {
			t.Errorf("%s differs between the engine and the orker: %q, %q", key, ork.Environment[key], orker.Environment[key])
		}
	}
	// And it is a shared one: object storage the other containers can reach,
	// on the port and with the bucket the store really serves, not a local
	// directory each container would have to itself.
	if backend := ork.Environment["ORK_ARTIFACT_BACKEND"]; backend != "s3" {
		t.Errorf("ORK_ARTIFACT_BACKEND is %q, want s3: a filesystem store would not be shared", backend)
	}
	if got := ork.Environment["ORK_S3_ENDPOINT"]; got != "http://seaweedfs:8333" {
		t.Errorf("ORK_S3_ENDPOINT is %q, want the seaweedfs service on the S3 port its health check probes (8333)", got)
	}
	if got := ork.Environment["ORK_TEMPORAL_ADDRESS"]; got != "temporal:7233" {
		t.Errorf("ORK_TEMPORAL_ADDRESS is %q, want the temporal service on its frontend port (7233)", got)
	}
	if hc := doc.service(t, "seaweedfs").Healthcheck; hc == nil || !strings.Contains(strings.Join(hc.Test, " "), "localhost:8333") {
		t.Errorf("the seaweedfs health check does not probe the S3 port 8333 the engine is told to use")
	}
	if len(doc.Services["seaweedfs"].Ports) != 0 {
		t.Errorf("the object store publishes %v; it is reachable from the compose network only", doc.Services["seaweedfs"].Ports)
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// isSet reports whether a decoded YAML value turns anything on: not false,
// not empty, not an empty list.
func isSet(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case []any:
		return len(v) != 0
	}
	return true
}

// forbiddenSettings lists what a published service may never carry (AGENTS.md
// §6): privilege, the host's namespaces, capabilities, devices, or a profile
// that turns a default protection off.
func forbiddenSettings(svc composeService) []string {
	var found []string
	for _, key := range []string{"privileged", "cap_add", "devices", "userns_mode", "cgroup", "cgroup_parent"} {
		if value, ok := svc.Other[key]; ok && isSet(value) {
			found = append(found, key)
		}
	}
	for _, key := range []string{"network_mode", "pid", "ipc", "uts"} {
		if value, ok := svc.Other[key]; ok && value == "host" {
			found = append(found, key+": host")
		}
	}
	if options, ok := svc.Other["security_opt"].([]any); ok {
		for _, option := range options {
			if text, _ := option.(string); strings.Contains(text, "unconfined") {
				found = append(found, "security_opt: "+text)
			}
		}
	}
	for _, volume := range svc.Volumes {
		switch source := splitColon(volume)[0]; source {
		case "/", "/proc", "/sys", "/dev", "/etc", "/var/run", "/run", "/var/lib/docker":
			found = append(found, "volume "+volume)
		}
	}
	return found
}

func TestQuickstartCompose_NoPrivilegedHostNetworkOrHostPID(t *testing.T) {
	t.Parallel()
	// smoke.yaml's forwarder uses network_mode: service:ork; that file is
	// the smoke's override and never published. This one is.
	doc := loadCompose(t)
	for _, name := range doc.serviceNames() {
		if found := forbiddenSettings(doc.Services[name]); len(found) != 0 {
			t.Errorf("service %s carries %v, which AGENTS.md section 6 forbids", name, found)
		}
	}
	// The check is not vacuous.
	bad := parseCompose(t, "bad", []byte(`
services:
  a: {image: x, privileged: true}
  b: {image: x, network_mode: host}
  c: {image: x, pid: host}
  d: {image: x, ipc: host}
  e: {image: x, cap_add: [SYS_ADMIN]}
  f: {image: x, devices: ["/dev/kvm"]}
  g: {image: x, security_opt: ["seccomp=unconfined"]}
  h: {image: x, volumes: ["/:/host"]}
  i: {image: x, userns_mode: host}
  ok: {image: x, network_mode: "service:other", privileged: false}
`))
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		if len(forbiddenSettings(bad.Services[name])) == 0 {
			t.Errorf("forbiddenSettings missed service %s: %+v", name, bad.Services[name])
		}
	}
	if found := forbiddenSettings(bad.Services["ok"]); len(found) != 0 {
		t.Errorf("forbiddenSettings flagged a service sharing a service's network: %v", found)
	}
}

func TestQuickstartCompose_EveryServiceHasAHealthcheck(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	var withoutOne []string
	for _, name := range doc.serviceNames() {
		svc := doc.Services[name]
		if svc.Healthcheck == nil {
			withoutOne = append(withoutOne, name)
			continue
		}
		hc := svc.Healthcheck
		if len(hc.Test) < 2 || (hc.Test[0] != "CMD" && hc.Test[0] != "CMD-SHELL") {
			t.Errorf("service %s: health check test %v is neither CMD nor CMD-SHELL", name, hc.Test)
		}
		for what, value := range map[string]string{"interval": hc.Interval, "timeout": hc.Timeout} {
			if d, err := time.ParseDuration(value); err != nil || d <= 0 {
				t.Errorf("service %s: health check %s %q is not a positive duration", name, what, value)
			}
		}
		if hc.Retries <= 0 {
			t.Errorf("service %s: health check retries %d; `up --wait` would never see it fail", name, hc.Retries)
		}
	}
	// Only the CLI, which is a command to run and not a service to wait for,
	// has none, and it is behind a profile so `up --wait` never starts it.
	if !slices.Equal(withoutOne, []string{"orkctl"}) {
		t.Errorf("services without a health check: %v, want only orkctl", withoutOne)
	}
	if got := doc.service(t, "orkctl").Profiles; !slices.Equal(got, []string{"cli"}) {
		t.Errorf("orkctl is in profiles %v, want [cli]", got)
	}
	for _, name := range doc.serviceNames() {
		if name != "orkctl" && len(doc.Services[name].Profiles) != 0 {
			t.Errorf("service %s is behind a profile, so `up --wait` would not start it", name)
		}
	}
}

func TestQuickstartCompose_HealthchecksUseTheImagesOwnProbes(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	stages := dockerfileStages(t)
	// ork and orker are distroless: no shell, so the probe is their own
	// binary, at the path their Dockerfile target installs it.
	for _, name := range []string{"ork", "orker"} {
		entry := entrypointOf(t, stageNamed(t, stages, name))
		want := []string{"CMD", entry[0], "--healthcheck"}
		if got := doc.service(t, name).Healthcheck.Test; !slices.Equal(got, want) {
			t.Errorf("service %s health check is %v, want %v: the image has no shell and no other tool", name, got, want)
		}
	}
	// The ui probes the port its image listens on.
	ui := stageNamed(t, stages, "ui")
	var port string
	for _, env := range ui.all("ENV") {
		if m := regexp.MustCompile(`\bPORT=(\d+)`).FindStringSubmatch(env.args); m != nil {
			port = m[1]
		}
	}
	probe := strings.Join(doc.service(t, "ui").Healthcheck.Test, " ")
	if port == "" || !strings.Contains(probe, "127.0.0.1:"+port+"/") {
		t.Errorf("the ui health check %q does not probe the port %q its image listens on", probe, port)
	}
	if !slices.Equal(doc.service(t, "ui").Ports, []string{"127.0.0.1:${ORKSTACK_UI_PORT-5173}:" + port}) {
		t.Errorf("the ui publishes %v, but its image listens on %s", doc.service(t, "ui").Ports, port)
	}
	// And the engine's API is on the port its image exposes.
	expose := stageNamed(t, stages, "ork").all("EXPOSE")
	if len(expose) != 1 {
		t.Fatalf("the ork target has %d EXPOSE instructions, want 1", len(expose))
	}
	if addr, want := doc.service(t, "ork").Environment["ORK_HTTP_ADDR"], "0.0.0.0:"+expose[0].args; addr != want {
		t.Errorf("ORK_HTTP_ADDR is %q, but the image exposes %s (want %q)", addr, expose[0].args, want)
	}
	if !slices.Equal(doc.service(t, "ork").Ports, []string{"127.0.0.1:${ORKSTACK_API_PORT-8080}:" + expose[0].args}) {
		t.Errorf("the ork service publishes %v, but its image exposes %s", doc.service(t, "ork").Ports, expose[0].args)
	}
}

// TestQuickstartCompose_TheBrowserReachesTheEngineWhereItIsPublished: the UI
// runs in the user's browser, which reaches the engine at the port the engine
// is published on, and the engine accepts the WebSocket only from the origin
// the UI is published on. Both follow the variable that moves the port.
func TestQuickstartCompose_TheBrowserReachesTheEngineWhereItIsPublished(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	origins := strings.Split(doc.service(t, "ork").Environment["ORK_WS_ALLOWED_ORIGINS"], ",")
	for _, host := range []string{"localhost", "127.0.0.1"} {
		if want := host + ":${ORKSTACK_UI_PORT-5173}"; !slices.Contains(origins, want) {
			t.Errorf("ORK_WS_ALLOWED_ORIGINS is %q, missing %s: the UI published on ORKSTACK_UI_PORT would be refused", origins, want)
		}
	}
	if len(origins) != 2 {
		t.Errorf("ORK_WS_ALLOWED_ORIGINS allows %v; only the UI's own two spellings of this machine", origins)
	}
	if got, want := doc.service(t, "ui").Environment["PUBLIC_ENGINE_WS_URL"], "ws://localhost:${ORKSTACK_API_PORT-8080}"; got != want {
		t.Errorf("PUBLIC_ENGINE_WS_URL is %q, want %q: the browser connects where the engine is published", got, want)
	}
	if got, want := doc.service(t, "ui").Environment["ENGINE_API_URL"], "http://ork:8080"; got != want {
		t.Errorf("ENGINE_API_URL is %q, want %q: the UI's server reaches the engine over the compose network", got, want)
	}
}

func TestQuickstartCompose_StartupFollowsHealth(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	for _, name := range doc.serviceNames() {
		for dependency, condition := range doc.Services[name].DependsOn {
			if condition.Condition != "service_healthy" {
				t.Errorf("service %s waits for %s with condition %q, want service_healthy", name, dependency, condition.Condition)
			}
			if _, ok := doc.Services[dependency]; !ok {
				t.Errorf("service %s depends on %s, which the file does not declare", name, dependency)
			}
		}
	}
	for name, wants := range map[string][]string{
		"ork": {"seaweedfs", "temporal"}, "orker": {"seaweedfs", "temporal"}, "ui": {"ork"}, "orkctl": {"ork"},
	} {
		var got []string
		for dependency := range doc.service(t, name).DependsOn {
			got = append(got, dependency)
		}
		if sort.Strings(got); !slices.Equal(got, wants) {
			t.Errorf("service %s depends on %v, want %v", name, got, wants)
		}
	}
}

func TestQuickstartCompose_TheOrkerOutlastsItsContainersGrace(t *testing.T) {
	t.Parallel()
	orkerService := loadCompose(t).service(t, "orker")
	grace, err := time.ParseDuration(orkerService.StopGracePeriod)
	if err != nil {
		t.Fatalf("stop_grace_period %q: %v", orkerService.StopGracePeriod, err)
	}
	// The Orker's own SIGTERM waits this long for each task container before
	// it kills it (AGENTS.md section 3.3); Docker kills the Orker after
	// stop_grace_period, so it must be longer than a container's grace.
	if grace <= orker.DefaultCancellationGrace || grace <= 30*time.Second {
		t.Errorf("stop_grace_period is %v, want more than a task container's %v cancellation grace", grace, orker.DefaultCancellationGrace)
	}
	if !orkerService.Init {
		t.Error("the orker has no init: true; a task container it kills would leave a zombie in a process that is not PID 1-aware")
	}
}

func TestQuickstartCompose_OrkctlIsProfileGatedReadOnly(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	cli := doc.service(t, "orkctl")
	if !slices.Equal(cli.Profiles, []string{"cli"}) {
		t.Errorf("profiles %v, want [cli]: `up` must not start a command-line tool", cli.Profiles)
	}
	if !slices.Equal(cli.Volumes, []string{"./:/work:ro"}) {
		t.Errorf("volumes %v, want only the bundle, read-only, at /work", cli.Volumes)
	}
	if len(cli.Ports) != 0 {
		t.Errorf("orkctl publishes %v", cli.Ports)
	}
	// It runs where the Dockerfile's target starts it, against the engine by
	// its service name.
	if want := stageNamed(t, dockerfileStages(t), "orkctl").all("WORKDIR"); len(want) != 1 || cli.WorkingDir != want[0].args {
		t.Errorf("working_dir is %q, but the orkctl image's WORKDIR is %v", cli.WorkingDir, want)
	}
	if cli.WorkingDir != "/work" {
		t.Errorf("working_dir is %q, want /work, where the bundle is mounted", cli.WorkingDir)
	}
	if server := cli.Environment["ORK_SERVER"]; server != "http://ork:8080" {
		t.Errorf("ORK_SERVER is %q, want http://ork:8080 (the engine service)", server)
	}
	if !strings.HasPrefix(cli.Image, "${ORKSTACK_ORKCTL_IMAGE:?") {
		t.Errorf("image %q", cli.Image)
	}
}

// TestQuickstartCompose_EveryOrkSettingIsARealOne: a misspelled ORK_* name is
// silently ignored by the process, so the file's environment is checked
// against the schema the binaries read.
func TestQuickstartCompose_EveryOrkSettingIsARealOne(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	reads := map[string]config.Binary{"ork": config.BinaryOrk, "orker": config.BinaryOrker}
	for _, name := range []string{"ork", "orker"} {
		for _, key := range sortedKeys(doc.Services[name].Environment) {
			if !strings.HasPrefix(key, "ORK_") {
				continue
			}
			setting, ok := config.Lookup(key)
			if !ok {
				t.Errorf("service %s sets %s, which is no setting of ork or orker", name, key)
				continue
			}
			if binary := reads[name]; !slices.Contains(setting.Binaries, binary) {
				t.Errorf("service %s sets %s, which the %s binary does not read (it is read by %v)", name, key, binary, setting.Binaries)
			}
		}
	}
	// orkctl reads ORK_SERVER, which is the CLI's and not in the daemons'
	// schema; the file may set it only on the orkctl service.
	if _, ok := config.Lookup("ORK_SERVER"); ok {
		t.Error("ORK_SERVER is now a daemon setting; update the orkctl service of this test")
	}
	for _, name := range doc.serviceNames() {
		for key := range doc.Services[name].Environment {
			if strings.HasPrefix(key, "ORK_") && name != "ork" && name != "orker" && !(name == "orkctl" && key == "ORK_SERVER") {
				t.Errorf("service %s sets %s, which no process of it reads", name, key)
			}
		}
	}
	// The health check reads the admin listener, which must be there to read.
	for _, name := range []string{"ork", "orker"} {
		if addr := doc.service(t, name).Environment["ORK_ADMIN_ADDR"]; !strings.HasPrefix(addr, "127.0.0.1:") {
			t.Errorf("service %s: ORK_ADMIN_ADDR is %q; the admin listener serves the health check inside the container and is never published, so it binds loopback", name, addr)
		}
	}
}

func TestQuickstartEnvExample_ListsEveryRequiredVariable(t *testing.T) {
	t.Parallel()
	required, _ := composeVariables(string(readRepoFile(t, composePath)))
	if len(required) == 0 {
		t.Fatal("compose.yaml requires no variable; a release has nothing to write")
	}
	example := parseEnvFile(t, envExamplePath, readRepoFile(t, envExamplePath))
	for _, variable := range required {
		if _, ok := example[variable]; !ok {
			t.Errorf("compose.yaml requires %s, but .env.example does not list it", variable)
		}
	}
}

func TestQuickstartEnvExample_MatchesTheComposeFile(t *testing.T) {
	t.Parallel()
	required, optional := composeVariables(string(readRepoFile(t, composePath)))
	example := parseEnvFile(t, envExamplePath, readRepoFile(t, envExamplePath))
	known := map[string]bool{}
	for _, variable := range required {
		known[variable] = true
	}
	for variable, def := range optional {
		known[variable] = true
		if got, ok := example[variable]; !ok || got != def {
			t.Errorf(".env.example sets %s to %q (listed: %v); compose.yaml defaults it to %q", variable, got, ok, def)
		}
	}
	for variable := range example {
		if !known[variable] {
			t.Errorf(".env.example lists %s, which compose.yaml never reads", variable)
		}
	}
}

// TestQuickstartEnvExample_ShowsTheShapeTheReleaseWrites: the placeholders in
// .env.example are the shape of the real lines, so the file does not teach a
// form the release does not write.
func TestQuickstartEnvExample_ShowsTheShapeTheReleaseWrites(t *testing.T) {
	t.Parallel()
	example := parseEnvFile(t, envExamplePath, readRepoFile(t, envExamplePath))
	var images []dist.Image
	for _, name := range dist.Images() {
		images = append(images, dist.Image{Name: name, Digest: "sha256:" + strings.Repeat("0", 64)})
	}
	version, err := dist.ParseVersion("v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := dist.QuickstartEnv(version, "ghcr.io", "X.Y.Z", images)
	if err != nil {
		t.Fatal(err)
	}
	written := parseEnvFile(t, "the rendered .env", rendered)
	digest := regexp.MustCompile(`sha256:0{64}`)
	for _, key := range []string{"ORKSTACK_ORK_IMAGE", "ORKSTACK_ORKER_IMAGE", "ORKSTACK_UI_IMAGE", "ORKSTACK_ORKCTL_IMAGE"} {
		if got := digest.ReplaceAllString(written[key], "sha256:<digest>"); got != example[key] {
			t.Errorf("%s: the release writes %q, .env.example shows %q", key, got, example[key])
		}
	}
	// The catalog line shows one full entry and elides the rest.
	if prefix := `{activities: {shell: "ghcr.io/orkstack/activity-shell:X.Y.Z@sha256:<digest>", `; !strings.HasPrefix(example["ORKSTACK_ACTIVITY_CATALOG"], prefix) ||
		!strings.HasPrefix(digest.ReplaceAllString(written["ORKSTACK_ACTIVITY_CATALOG"], "sha256:<digest>"), prefix) {
		t.Errorf("the catalog line differs in shape:\n  example %s\n  written %s", example["ORKSTACK_ACTIVITY_CATALOG"], written["ORKSTACK_ACTIVITY_CATALOG"])
	}
}
