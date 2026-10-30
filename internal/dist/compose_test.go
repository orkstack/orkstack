package dist_test

import (
	"bytes"
	"maps"
	"os"
	"path"
	"path/filepath"
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

// compose.yaml, at the repository root, is the quickstart a user runs from a
// clone (docs/GETTING_STARTED.md). These tests hold it to the quickstart
// contract (docs/DEPLOY.md, "Running the published images";
// docs/SECURITY_MODEL.md): what it builds and pins, what it publishes, who
// sees the Docker socket, and how its services find each other. They read the
// committed file, so an edit that breaks the contract fails in the unit gate.
// scripts/quickstart-smoke.sh is the proof that it also runs: as the README
// says, from a fresh clone, and on a release's own images before the release
// publishes anything.

const (
	composePath    = "compose.yaml"
	devComposePath = "deploy/docker-compose.yml"
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
	Build           *composeBuild       `yaml:"build"`
	Scale           *int                `yaml:"scale"`
	Entrypoint      []string            `yaml:"entrypoint"`
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

// composeBuild is build in either of its forms: a context directory, or a
// mapping.
type composeBuild struct {
	Context    string `yaml:"context"`
	Dockerfile string `yaml:"dockerfile"`
	Target     string `yaml:"target"`
	// Other holds every other key (args, additional_contexts, ...), so one
	// this file never meant to carry is seen.
	Other map[string]any `yaml:",inline"`
}

func (b *composeBuild) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		b.Context = node.Value
		return nil
	}
	type plain composeBuild
	return node.Decode((*plain)(b))
}

type composeHealthcheck struct {
	Test     []string `yaml:"test"`
	Interval string   `yaml:"interval"`
	Timeout  string   `yaml:"timeout"`
	Retries  int      `yaml:"retries"`
}

type composeDependency struct {
	Condition string `yaml:"condition"`
	Restart   bool   `yaml:"restart"`
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
	digestPinnedImage = regexp.MustCompile(`^[^\s@$]+@sha256:[0-9a-f]{64}$`)
	taggedAndDigested = regexp.MustCompile(`^[^\s@$]+:[A-Za-z0-9_][A-Za-z0-9._-]{0,127}@sha256:[0-9a-f]{64}$`)
	exactVersionImage = regexp.MustCompile(`^[^\s@$:]+:[0-9]+\.[0-9]+\.[0-9]+$`)
)

// imageProblem says why an image the quickstart pulls is not pinned to
// something that cannot move, or returns "". An image is acceptable when it
// names its digest, or an exact version: the development stack pins Temporal
// by version and the quickstart runs the same one.
func imageProblem(image string) string {
	switch {
	case taggedAndDigested.MatchString(image), digestPinnedImage.MatchString(image):
		return ""
	case exactVersionImage.MatchString(image):
		return ""
	}
	return "is neither pinned by digest nor an exact x.y.z version"
}

// sourceTargets are the platform's services, each built from
// deploy/source/Dockerfile's target of its own name.
var sourceTargets = []string{"ork", "orker", "orkctl"}

// activityImage is the name the stack builds a first-party activity as, and
// pushes and pins it by: an image of its own registry, on the host port that
// moves with ORKSTACK_REGISTRY_PORT.
func activityImage(name string) string {
	return registryHost + "/orkstack/" + dist.ActivityPrefix + name + ":local"
}

// registryHost is the registry's address as the host's Docker daemon and the
// engine both reach it: loopback, on the one port that can be moved.
const registryHost = "127.0.0.1:${ORKSTACK_REGISTRY_PORT-5002}"

// TestQuickstartCompose_BuildsFromTheCheckoutAndPinsTheRest: a clone is all
// the quickstart needs, so the platform and the first-party activities are
// built from it, and every image it pulls is pinned.
func TestQuickstartCompose_BuildsFromTheCheckoutAndPinsTheRest(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	targets := map[string]bool{}
	for _, s := range sourceStages(t) {
		targets[s.name] = true
	}
	built := map[string]bool{}
	for _, name := range sourceTargets {
		built[name] = true
		svc := doc.service(t, name)
		if b := svc.Build; b == nil || b.Context != "." || b.Dockerfile != sourceDockerfilePath || b.Target != name || len(b.Other) != 0 {
			t.Errorf("service %s builds %+v, want context ., dockerfile %s, target %s and nothing else", name, b, sourceDockerfilePath, name)
		}
		if !targets[name] {
			t.Errorf("service %s builds the target %s, which %s does not have", name, name, sourceDockerfilePath)
		}
		if svc.Image != "" {
			t.Errorf("service %s names its image %q; compose names it after the project, and nothing is published", name, svc.Image)
		}
	}
	for _, name := range dist.ActivityNames() {
		service := dist.ActivityPrefix + name
		built[service] = true
		svc := doc.service(t, service)
		if b := svc.Build; b == nil || b.Context != "images/"+service || b.Dockerfile != "" || b.Target != "" || len(b.Other) != 0 {
			t.Errorf("service %s builds %+v, want the context images/%s and nothing else", service, b, service)
		}
		if svc.Image != activityImage(name) {
			t.Errorf("service %s is built as %q, want %q", service, svc.Image, activityImage(name))
		}
		readRepoFile(t, "images/"+service+"/Dockerfile")
	}
	// Every activity under images/ is one the stack builds: one added there
	// and not here would be missing from the quickstart's catalog.
	entries, err := os.ReadDir(filepath.Join(repoRoot(t), "images"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), dist.ActivityPrefix) && !built[e.Name()] {
			t.Errorf("images/%s is a first-party activity the quickstart does not build", e.Name())
		}
	}
	for _, name := range doc.serviceNames() {
		svc := doc.Services[name]
		switch {
		case built[name]:
		case svc.Build != nil:
			t.Errorf("service %s builds %+v; only the platform and the first-party activities are built here", name, svc.Build)
		default:
			if problem := imageProblem(svc.Image); problem != "" {
				t.Errorf("service %s: image %q %s", name, svc.Image, problem)
			}
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

// TestQuickstartCompose_TheEnginePinsTheActivitiesThePushPushes: an Orkflow
// pins each task's image by its registry digest at apply, so the activities
// are pushed to the stack's own registry before the engine starts, and the
// engine's catalog names exactly what was pushed, at an address both the
// host's daemon and the engine reach.
func TestQuickstartCompose_TheEnginePinsTheActivitiesThePushPushes(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	var catalog struct {
		Activities map[string]string `yaml:"activities"`
	}
	decoder := yaml.NewDecoder(strings.NewReader(doc.service(t, "ork").Environment["ORK_ACTIVITY_CATALOG"]))
	decoder.KnownFields(true)
	if err := decoder.Decode(&catalog); err != nil {
		t.Fatalf("ORK_ACTIVITY_CATALOG: %v", err)
	}
	if got, want := sorted(slices.Collect(maps.Keys(catalog.Activities))), sorted(dist.ActivityNames()); !slices.Equal(got, want) {
		t.Errorf("the catalog names %v, want every first-party activity %v", got, want)
	}
	for name, ref := range catalog.Activities {
		if ref != activityImage(name) {
			t.Errorf("the catalog resolves %s to %q, but the stack builds and pushes it as %q", name, ref, activityImage(name))
		}
	}

	push := doc.service(t, "activities")
	if len(push.Command) != 1 {
		t.Fatalf("the activities step runs %d commands, want one script", len(push.Command))
	}
	loop := regexp.MustCompile(`for name in ([a-z ]+); do`).FindStringSubmatch(push.Command[0])
	if loop == nil || !slices.Equal(strings.Fields(loop[1]), dist.ActivityNames()) {
		t.Errorf("the activities step pushes %v, want %v", loop, dist.ActivityNames())
	}
	if want := `docker push --quiet "` + activityImage("$$name") + `"`; !strings.Contains(push.Command[0], want) {
		t.Errorf("the activities step does not push %s", want)
	}
	if !slices.Equal(push.Entrypoint, []string{"sh", "-ec"}) {
		t.Errorf("the activities step runs %v; want sh -ec, so a failed push fails it and the engine never starts", push.Entrypoint)
	}
	if dep := doc.service(t, "ork").DependsOn["activities"]; dep.Condition != "service_completed_successfully" {
		t.Errorf("the engine waits for the activities step with %q, want service_completed_successfully", dep.Condition)
	}

	// The registry is published on the host's loopback for the daemon, and a
	// forwarder in the engine's own network namespace serves the same port
	// on its loopback, so one reference reaches the registry from both.
	registry := doc.service(t, "registry")
	if !slices.Equal(registry.Ports, []string{registryHost + ":5000"}) {
		t.Errorf("the registry publishes %v, want %s:5000", registry.Ports, registryHost)
	}
	if hc := registry.Healthcheck; hc == nil || !strings.Contains(strings.Join(hc.Test, " "), "127.0.0.1:5000/v2/") {
		t.Errorf("the registry's health check does not ask its API on 5000: the push would start before it answers")
	}
	forward := doc.service(t, "registry-forward")
	if mode := forward.Other["network_mode"]; mode != "service:ork" {
		t.Errorf("the forwarder runs in network %v, want service:ork: the engine resolves the catalog on its own loopback", mode)
	}
	if want := []string{"TCP4-LISTEN:${ORKSTACK_REGISTRY_PORT-5002},bind=127.0.0.1,fork,reuseaddr", "TCP:registry:5000"}; !slices.Equal(forward.Command, want) {
		t.Errorf("the forwarder runs %v, want %v", forward.Command, want)
	}
}

func TestQuickstartCompose_InfraPinsMatchTheDevStack(t *testing.T) {
	t.Parallel()
	quickstart := loadCompose(t)
	dev := parseCompose(t, devComposePath, readRepoFile(t, devComposePath))
	for _, name := range []string{"temporal", "seaweedfs", "registry"} {
		got, want := quickstart.service(t, name).Image, dev.service(t, name).Image
		if got != want {
			t.Errorf("service %s runs %q, but deploy/docker-compose.yml, whose pin the proof tests, runs %q", name, got, want)
		}
	}
	// The push runs the docker CLI the Orker's image carries, by one pin.
	for _, stages := range [][]dfStage{dockerfileStages(t), sourceStages(t)} {
		if got, want := quickstart.service(t, "activities").Image, stageNamed(t, stages, "docker-cli").image; got != want {
			t.Errorf("the activities step runs %q, but the orker's docker CLI is %q", got, want)
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
			t.Errorf("service %s declares expose; nothing but the published ports is reachable", name)
		}
	}
	// Exactly the Temporal UI, the API (which serves the web UI) and the
	// registry the host's daemon pulls the activities from: the admin
	// listeners, the object store and the Temporal frontend stay inside the
	// network.
	want := map[string][]string{"temporal": {"8233"}, "ork": {"8080"}, "registry": {"5000"}}
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

func TestQuickstartCompose_OnlyTheOrkerAndThePushSeeTheDockerSocket(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	touchesSocket := func(volume string) bool {
		parts := splitColon(volume)
		return strings.Contains(parts[0], "docker.sock") || strings.Contains(parts[0], "DOCKER_SOCKET") ||
			(len(parts) > 1 && strings.HasSuffix(parts[1], "docker.sock"))
	}
	const socket = "${ORKSTACK_DOCKER_SOCKET-/var/run/docker.sock}:/var/run/docker.sock"
	for _, name := range doc.serviceNames() {
		svc := doc.Services[name]
		sockets := 0
		for _, volume := range svc.Volumes {
			if touchesSocket(volume) {
				sockets++
			}
		}
		if _, set := svc.Environment["DOCKER_HOST"]; set {
			t.Errorf("service %s sets DOCKER_HOST; only the Orker and the push talk to a daemon, through the socket they mount", name)
		}
		switch {
		case (name == "orker" || name == "activities") && sockets != 1:
			t.Errorf("service %s mounts the Docker socket %d times, want once", name, sockets)
		case name != "orker" && name != "activities" && sockets != 0:
			t.Errorf("service %s mounts the Docker socket; whoever has it controls the host, so only the orker and the push may", name)
		}
	}
	if volumes := doc.service(t, "orker").Volumes; len(volumes) == 0 || volumes[0] != socket {
		t.Errorf("the orker's first volume is %v; want %s", volumes, socket)
	}
	if volumes := doc.service(t, "activities").Volumes; !slices.Equal(volumes, []string{socket}) {
		t.Errorf("the activities step mounts %v; want the socket alone", volumes)
	}
	// No other service sees a host path at all, but the CLI's read-only
	// checkout; the Orker sees the socket and its work directory and nothing
	// else.
	for _, name := range doc.serviceNames() {
		for _, volume := range doc.Services[name].Volumes {
			parts := splitColon(volume)
			if !hostPathSource(parts[0]) {
				continue
			}
			switch {
			case name == "orker":
			case name == "activities" && volume == socket:
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
	// The registry's forwarder shares the engine's network (service:ork),
	// which is not the host's.
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

func TestQuickstartCompose_EveryLongRunningServiceHasAHealthcheck(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	var withoutOne, notStarted []string
	for _, name := range doc.serviceNames() {
		svc := doc.Services[name]
		if svc.Scale != nil {
			if *svc.Scale != 0 {
				t.Errorf("service %s has scale %d; a service runs once, or is built and never started (0)", name, *svc.Scale)
			}
			notStarted = append(notStarted, name)
		}
		if len(svc.Profiles) != 0 {
			t.Errorf("service %s is behind profiles %v, so `up` would neither build nor start it", name, svc.Profiles)
		}
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
	// What `up` builds and never starts (scale 0): the CLI, a command to run
	// and not a service to wait for, and the activities, which are pushed.
	wantNotStarted := []string{"orkctl"}
	for _, name := range dist.ActivityNames() {
		wantNotStarted = append(wantNotStarted, dist.ActivityPrefix+name)
	}
	if !slices.Equal(notStarted, sorted(wantNotStarted)) {
		t.Errorf("services built and never started: %v, want %v", notStarted, sorted(wantNotStarted))
	}
	// Without a health check: those, the push, which exits once it has
	// pushed (the engine waits for it to succeed), and the forwarder, a bare
	// socat that `up --wait` sees running.
	wantWithout := append([]string{"activities", "registry-forward"}, wantNotStarted...)
	if !slices.Equal(withoutOne, sorted(wantWithout)) {
		t.Errorf("services without a health check: %v, want %v", withoutOne, sorted(wantWithout))
	}
}

func TestQuickstartCompose_HealthchecksUseTheImagesOwnProbes(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	stages := sourceStages(t)
	// ork and orker are distroless: no shell, so the probe is their own
	// binary, at the path their Dockerfile target installs it.
	for _, name := range []string{"ork", "orker"} {
		entry := entrypointOf(t, stageNamed(t, stages, name))
		want := []string{"CMD", entry[0], "--healthcheck"}
		if got := doc.service(t, name).Healthcheck.Test; !slices.Equal(got, want) {
			t.Errorf("service %s health check is %v, want %v: the image has no shell and no other tool", name, got, want)
		}
	}
	// And the engine's API, and the web UI it serves, are on the port its
	// image exposes.
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

// TestQuickstartCompose_TheBrowserHasOneOrigin: the engine serves the web UI
// on its own published port, so the page, the API it calls and the
// WebSockets it opens share one origin, which the bridge admits by default.
// No service serves the UI apart from the engine, and nothing configures the
// browser's way to it: no allowed origins, no second port, no engine URL.
func TestQuickstartCompose_TheBrowserHasOneOrigin(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	if _, ok := doc.Services["ui"]; ok {
		t.Error("compose.yaml declares a ui service; the engine serves the UI, built into ork")
	}
	if origins, ok := doc.service(t, "ork").Environment["ORK_WS_ALLOWED_ORIGINS"]; ok {
		t.Errorf("the engine sets ORK_WS_ALLOWED_ORIGINS=%q; the UI is on the engine's own origin, "+
			"which the bridge's default admits, and any other origin it named would be one more the bridge trusts", origins)
	}
	if _, ok := doc.service(t, "ork").Environment["ORK_UI_DIR"]; ok {
		t.Error("the engine sets ORK_UI_DIR; the quickstart serves the UI built into ork")
	}
	text := string(readRepoFile(t, composePath))
	for _, gone := range []string{"ORKSTACK_UI_PORT", "ENGINE_API_URL", "PUBLIC_ENGINE_WS_URL"} {
		if strings.Contains(text, gone) {
			t.Errorf("compose.yaml names %s, which belonged to the UI's own server; the engine serves the UI now", gone)
		}
	}
}

func TestQuickstartCompose_StartupFollowsHealth(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	const healthy, started, done = "service_healthy", "service_started", "service_completed_successfully"
	want := map[string]map[string]string{
		"activities": {"registry": healthy},
		"ork":        {"temporal": healthy, "seaweedfs": healthy, "activities": done},
		// The forwarder joins the engine's network, which exists once the
		// engine's container has started.
		"registry-forward": {"registry": healthy, "ork": started},
		"orker":            {"temporal": healthy, "seaweedfs": healthy},
		// orkctl depends on nothing: `docker compose run` would start what
		// it depends on, the activities step included, on every command.
	}
	for _, name := range doc.serviceNames() {
		got := map[string]string{}
		for dependency, condition := range doc.Services[name].DependsOn {
			got[dependency] = condition.Condition
			if _, ok := doc.Services[dependency]; !ok {
				t.Errorf("service %s depends on %s, which the file does not declare", name, dependency)
			}
		}
		if wants := want[name]; !maps.Equal(got, wants) {
			t.Errorf("service %s depends on %v, want %v", name, got, wants)
		}
	}
	// A restarted engine has a new network namespace; the forwarder in the
	// old one would serve nothing, so it restarts with the engine.
	if !doc.service(t, "registry-forward").DependsOn["ork"].Restart {
		t.Error("registry-forward does not restart with ork (depends_on: ork: restart: true)")
	}
	for name, svc := range doc.Services {
		for dependency, condition := range svc.DependsOn {
			if condition.Restart && (name != "registry-forward" || dependency != "ork") {
				t.Errorf("service %s restarts with %s; only the forwarder shares a service's network", name, dependency)
			}
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

func TestQuickstartCompose_OrkctlIsBuiltNotStartedReadOnly(t *testing.T) {
	t.Parallel()
	doc := loadCompose(t)
	cli := doc.service(t, "orkctl")
	// `up` builds it, so an update rebuilds it with the rest, and starts no
	// container of it: `docker compose run --rm orkctl` does.
	if cli.Scale == nil || *cli.Scale != 0 {
		t.Errorf("scale %v, want 0: `up` must not start a command-line tool", cli.Scale)
	}
	if !slices.Equal(cli.Volumes, []string{"./:/work:ro"}) {
		t.Errorf("volumes %v, want only the checkout, read-only, at /work", cli.Volumes)
	}
	if len(cli.Ports) != 0 {
		t.Errorf("orkctl publishes %v", cli.Ports)
	}
	// It runs where the Dockerfile's target starts it, against the engine by
	// its service name.
	if want := stageNamed(t, sourceStages(t), "orkctl").all("WORKDIR"); len(want) != 1 || cli.WorkingDir != want[0].args {
		t.Errorf("working_dir is %q, but the orkctl image's WORKDIR is %v", cli.WorkingDir, want)
	}
	if cli.WorkingDir != "/work" {
		t.Errorf("working_dir is %q, want /work, where the checkout is mounted", cli.WorkingDir)
	}
	if server := cli.Environment["ORK_SERVER"]; server != "http://ork:8080" {
		t.Errorf("ORK_SERVER is %q, want http://ork:8080 (the engine service)", server)
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

// mappingValue is the value of key in a YAML mapping node, or nil.
func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

// smokeVariable is the variable deploy/release-smoke.yaml names a release
// image by, as scripts/quickstart-smoke.sh exports it: SMOKE_<NAME>_IMAGE.
func smokeVariable(image string) string {
	return "SMOKE_" + strings.ToUpper(strings.ReplaceAll(image, "-", "_")) + "_IMAGE"
}

// smokeOrkerForward is the one service the release smoke adds to compose.yaml:
// the registry on the Orker's 127.0.0.1, because the Orker verifies a
// signature with its own registry client and the smoke runs an Orkflow under
// require_signed_by.
const smokeOrkerForward = "registry-forward-orker"

// checkSmokeOrkerForward holds that service to compose.yaml's own
// registry-forward: the same command and pinned image (which the smoke reads
// from compose.yaml), in the Orker's network namespace instead of the
// engine's, and restarted with it.
func checkSmokeOrkerForward(t *testing.T, doc composeFile, path string, node *yaml.Node) {
	t.Helper()
	if node == nil {
		t.Errorf("%s does not forward the registry into the Orker's network namespace (%s), so the Orker could not verify a signature",
			path, smokeOrkerForward)
		return
	}
	var got composeService
	if err := node.Decode(&got); err != nil {
		t.Fatalf("%s: %s: %v", path, smokeOrkerForward, err)
	}
	engine := doc.service(t, "registry-forward")
	if got.Image != "${SMOKE_FORWARD_IMAGE:?}" {
		t.Errorf("%s: %s runs %q, want ${SMOKE_FORWARD_IMAGE:?}, the forwarder compose.yaml pins", path, smokeOrkerForward, got.Image)
	}
	if !slices.Equal(got.Command, engine.Command) {
		t.Errorf("%s: %s runs %v, want registry-forward's own %v", path, smokeOrkerForward, got.Command, engine.Command)
	}
	if mode, _ := got.Other["network_mode"].(string); mode != "service:orker" {
		t.Errorf("%s: %s has network_mode %q, want service:orker", path, smokeOrkerForward, mode)
	}
	if delete(got.Other, "network_mode"); len(got.Other) != 0 || len(got.Ports) != 0 || len(got.Volumes) != 0 || len(got.Environment) != 0 {
		t.Errorf("%s: %s carries more than an image, a command, a network mode and its dependencies: %+v", path, smokeOrkerForward, got)
	}
	if !got.DependsOn["orker"].Restart || got.DependsOn["registry"].Condition != "service_healthy" {
		t.Errorf("%s: %s does not wait for a healthy registry and restart with orker", path, smokeOrkerForward)
	}
	pin := regexp.MustCompile(`alpine/socat:[^ @]+@sha256:[0-9a-f]{64}`).FindString(engine.Image)
	smoke := string(readRepoFile(t, "scripts/quickstart-smoke.sh"))
	if pin == "" || !strings.Contains(smoke, `SMOKE_FORWARD_IMAGE="$(grep -m1 -oE 'alpine/socat:[^ @]+@sha256:[0-9a-f]{64}' "$REPO/compose.yaml")"`) {
		t.Errorf("scripts/quickstart-smoke.sh does not take SMOKE_FORWARD_IMAGE from compose.yaml's pinned forwarder (%q)", engine.Image)
	}
}

// TestReleaseSmokeOverride_RunsOnlyTheReleasesImages: the release smoke's
// override turns every service compose.yaml builds into the release image the
// smoke pushed, so `up --no-build` runs the release's own bytes, the engine
// pins the release's activities, and nothing of the checkout is built.
func TestReleaseSmokeOverride_RunsOnlyTheReleasesImages(t *testing.T) {
	t.Parallel()
	const path = "deploy/release-smoke.yaml"
	doc := loadCompose(t)
	var file yaml.Node
	if err := yaml.Unmarshal(readRepoFile(t, path), &file); err != nil || len(file.Content) != 1 {
		t.Fatalf("%s: %v", path, err)
	}
	services := mappingValue(file.Content[0], "services")
	if services == nil {
		t.Fatalf("%s has no services", path)
	}
	named := map[string]bool{}
	for i := 0; i+1 < len(services.Content); i += 2 {
		name := services.Content[i].Value
		if _, ok := doc.Services[name]; !ok && name != smokeOrkerForward {
			t.Errorf("%s overrides %s, which compose.yaml does not declare", path, name)
		}
	}
	checkSmokeOrkerForward(t, doc, path, mappingValue(services, smokeOrkerForward))
	for _, name := range doc.serviceNames() {
		if doc.Services[name].Build == nil {
			continue
		}
		override := mappingValue(services, name)
		if build := mappingValue(override, "build"); build == nil || build.Tag != "!reset" {
			t.Errorf("%s does not reset %s's build (build: !reset null), so the smoke would run what the checkout builds", path, name)
		}
		image := mappingValue(override, "image")
		want := "${" + smokeVariable(name) + ":?"
		if image == nil || !strings.HasPrefix(image.Value, want) || !strings.HasSuffix(image.Value, "}") {
			t.Errorf("%s runs %s as %v, want the required variable %s...}", path, name, image, want)
			continue
		}
		named[name] = true
	}
	// Every image the release publishes is run, and the smoke exports exactly
	// those variables.
	if got, want := sorted(slices.Collect(maps.Keys(named))), dist.Images(); !slices.Equal(got, want) {
		t.Errorf("%s runs %v, want every image a release publishes %v", path, got, want)
	}
	smoke := string(readRepoFile(t, "scripts/quickstart-smoke.sh"))
	if !strings.Contains(smoke, `variable="SMOKE_$(tr 'a-z-' 'A-Z_' <<<"$name")_IMAGE"`) {
		t.Error("scripts/quickstart-smoke.sh does not export SMOKE_<NAME>_IMAGE for each image, as the override reads them")
	}
	// The engine pins the release's activities, and the push has nothing to do.
	catalog := mappingValue(mappingValue(mappingValue(services, "ork"), "environment"), "ORK_ACTIVITY_CATALOG")
	if catalog == nil {
		t.Fatalf("%s does not set the engine's catalog", path)
	}
	for _, name := range dist.ActivityNames() {
		if want := name + ": ${" + smokeVariable(dist.ActivityPrefix+name) + ":?}"; !strings.Contains(catalog.Value, want) {
			t.Errorf("the smoke's catalog does not say %q", want)
		}
	}
	if command := mappingValue(mappingValue(services, "activities"), "command"); command == nil ||
		len(command.Content) != 1 || command.Content[0].Value != "true" {
		t.Errorf("%s leaves the activities step pushing what the checkout built", path)
	}
}
