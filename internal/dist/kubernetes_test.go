package dist_test

import (
	"bytes"
	"errors"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// deploy/kubernetes/ holds the two supported shapes of an Orker on
// Kubernetes, each with a Docker engine of its own in a sidecar
// (docs/DEPLOY.md, "Orkers on Kubernetes"). These tests hold the committed
// manifests to what that section promises: the work root and the engine's
// socket are one volume at one path in both containers, the engine is a
// native sidecar that outlives the Orker, every image is pinned, the engine's
// limits cover the Orker's capacity, and the two shapes differ in privilege
// and nothing else; and they hold the check (deploy/kubernetes/check) to
// what it says it does to a cluster. scripts/k8s-orker-check.sh, which
// scripts/kind-orker.sh runs on kind, is the proof that they also run.

var kubernetesShapes = []string{"orker-userns", "orker-privileged"}

type k8sDeployment struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec k8sPodSpec `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type k8sPodSpec struct {
	HostUsers                     *bool          `yaml:"hostUsers"`
	RuntimeClassName              string         `yaml:"runtimeClassName"`
	TerminationGracePeriodSeconds int            `yaml:"terminationGracePeriodSeconds"`
	InitContainers                []k8sContainer `yaml:"initContainers"`
	Containers                    []k8sContainer `yaml:"containers"`
	Volumes                       []k8sVolume    `yaml:"volumes"`
}

type k8sContainer struct {
	Name            string   `yaml:"name"`
	Image           string   `yaml:"image"`
	RestartPolicy   string   `yaml:"restartPolicy"`
	Command         []string `yaml:"command"`
	Args            []string `yaml:"args"`
	Env             []k8sEnv `yaml:"env"`
	StartupProbe    any      `yaml:"startupProbe"`
	SecurityContext struct {
		Privileged *bool          `yaml:"privileged"`
		Extra      map[string]any `yaml:",inline"`
	} `yaml:"securityContext"`
	Resources struct {
		Limits   map[string]string `yaml:"limits"`
		Requests map[string]string `yaml:"requests"`
	} `yaml:"resources"`
	VolumeMounts []struct {
		Name      string `yaml:"name"`
		MountPath string `yaml:"mountPath"`
	} `yaml:"volumeMounts"`
}

type k8sEnv struct {
	Name      string `yaml:"name"`
	Value     string `yaml:"value"`
	ValueFrom *struct {
		FieldRef *struct {
			FieldPath string `yaml:"fieldPath"`
		} `yaml:"fieldRef"`
	} `yaml:"valueFrom"`
}

type k8sVolume struct {
	Name     string         `yaml:"name"`
	EmptyDir map[string]any `yaml:"emptyDir"`
	Extra    map[string]any `yaml:",inline"`
}

// orkerDeployment reads one shape's orker.yaml: its pod, its engine sidecar
// and its Orker container.
func orkerDeployment(t *testing.T, shape string) (k8sPodSpec, k8sContainer, k8sContainer) {
	t.Helper()
	rel := "deploy/kubernetes/" + shape + "/orker.yaml"
	var d k8sDeployment
	if err := yaml.Unmarshal(readRepoFile(t, rel), &d); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	if d.Kind != "Deployment" || d.Metadata.Name != "orker" {
		t.Fatalf("%s is a %s named %q, want the Deployment orker", rel, d.Kind, d.Metadata.Name)
	}
	pod := d.Spec.Template.Spec
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Name != "engine" {
		t.Fatalf("%s: want exactly one init container, the engine, got %d", rel, len(pod.InitContainers))
	}
	if len(pod.Containers) != 1 || pod.Containers[0].Name != "orker" {
		t.Fatalf("%s: want exactly one container, the orker, got %d", rel, len(pod.Containers))
	}
	return pod, pod.InitContainers[0], pod.Containers[0]
}

func envOf(c k8sContainer, name string) (k8sEnv, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e, true
		}
	}
	return k8sEnv{}, false
}

func mountOf(c k8sContainer, volume string) (string, bool) {
	for _, m := range c.VolumeMounts {
		if m.Name == volume {
			return m.MountPath, true
		}
	}
	return "", false
}

func volumeOf(pod k8sPodSpec, name string) (k8sVolume, bool) {
	for _, v := range pod.Volumes {
		if v.Name == name {
			return v, true
		}
	}
	return k8sVolume{}, false
}

// engineScript is the engine's start-up: the last argument of `sh -c`.
func engineScript(t *testing.T, shape string, engine k8sContainer) string {
	t.Helper()
	if len(engine.Command) != 3 || engine.Command[0] != "sh" || engine.Command[1] != "-c" || len(engine.Args) != 0 {
		t.Fatalf("%s: the engine starts with %q %q, want sh -c <start-up> and no args", shape, engine.Command, engine.Args)
	}
	return engine.Command[2]
}

// TestKubernetesOrker_EngineAndOrkerShareTheWorkRootAndTheSocket: the engine
// bind-mounts each task's workspace from the path the Orker names, so the
// work root is one emptyDir at one path in both containers; the Orker reaches
// the engine on a socket in a volume they share, and the engine listens on
// nothing else.
func TestKubernetesOrker_EngineAndOrkerShareTheWorkRootAndTheSocket(t *testing.T) {
	t.Parallel()
	for _, shape := range kubernetesShapes {
		pod, engine, orker := orkerDeployment(t, shape)
		workdir, ok := envOf(orker, "ORK_ORKER_WORKDIR")
		if !ok || workdir.Value == "" {
			t.Fatalf("%s: the orker sets no ORK_ORKER_WORKDIR", shape)
		}
		for _, c := range []k8sContainer{engine, orker} {
			if got, ok := mountOf(c, "work"); !ok || got != workdir.Value {
				t.Errorf("%s: the %s mounts the work volume at %q, want ORK_ORKER_WORKDIR's %q", shape, c.Name, got, workdir.Value)
			}
		}
		if v, ok := volumeOf(pod, "work"); !ok || v.EmptyDir == nil {
			t.Errorf("%s: the work volume is not an emptyDir", shape)
		}

		host, ok := envOf(orker, "DOCKER_HOST")
		socket, isUnix := strings.CutPrefix(host.Value, "unix://")
		if !ok || !isUnix || !strings.Contains(socket, "/") {
			t.Fatalf("%s: the orker's DOCKER_HOST is %q, want a unix socket", shape, host.Value)
		}
		dir := socket[:strings.LastIndex(socket, "/")]
		for _, c := range []k8sContainer{engine, orker} {
			if got, ok := mountOf(c, "engine-socket"); !ok || got != dir {
				t.Errorf("%s: the %s mounts the engine-socket volume at %q, want %q, where DOCKER_HOST's socket is", shape, c.Name, got, dir)
			}
		}
		if v, ok := volumeOf(pod, "engine-socket"); !ok || v.EmptyDir == nil {
			t.Errorf("%s: the engine-socket volume is not an emptyDir", shape)
		}
		script := engineScript(t, shape, engine)
		hosts := regexp.MustCompile(`--host=(\S+)`).FindAllStringSubmatch(script, -1)
		if len(hosts) != 1 || hosts[0][1] != host.Value {
			t.Errorf("%s: the engine listens on %q, want only the orker's DOCKER_HOST %q", shape, hosts, host.Value)
		}
		if strings.Contains(script, "tcp://") {
			t.Errorf("%s: the engine's start-up names a TCP listener; its API is the socket only", shape)
		}

		if got, ok := mountOf(engine, "engine-data"); !ok || got != "/var/lib/docker" {
			t.Errorf("%s: the engine's data root is %q, want the engine-data volume at /var/lib/docker", shape, got)
		}
		if v, ok := volumeOf(pod, "engine-data"); !ok || v.EmptyDir == nil {
			t.Errorf("%s: the engine's data is not an emptyDir, a disposable cache (AGENTS.md §5)", shape)
		}
		for _, v := range pod.Volumes {
			if v.EmptyDir == nil {
				t.Errorf("%s: volume %s is not an emptyDir; no host path, and nothing of the node, reaches this pod", shape, v.Name)
			}
		}
	}
}

// TestKubernetesOrker_TheEngineIsANativeSidecarThatOutlivesTheOrker: the
// engine starts, and answers, before the Orker's boot probe runs, and stops
// after the Orker has cancelled its tasks; the pod's grace is longer than a
// task's default one, and the Orker's id is unique to its pod.
func TestKubernetesOrker_TheEngineIsANativeSidecarThatOutlivesTheOrker(t *testing.T) {
	t.Parallel()
	for _, shape := range kubernetesShapes {
		pod, engine, orker := orkerDeployment(t, shape)
		if engine.RestartPolicy != "Always" {
			t.Errorf("%s: the engine's restartPolicy is %q, want Always (a native sidecar)", shape, engine.RestartPolicy)
		}
		if engine.StartupProbe == nil || orker.StartupProbe == nil {
			t.Errorf("%s: the engine and the orker each need a startup probe", shape)
		}
		if pod.TerminationGracePeriodSeconds <= 30 {
			t.Errorf("%s: terminationGracePeriodSeconds is %d, want longer than a task's default 30 s grace", shape, pod.TerminationGracePeriodSeconds)
		}
		if !strings.Contains(engineScript(t, shape, engine), "exec docker-init -- dockerd ") {
			t.Errorf("%s: the engine does not exec dockerd under docker-init", shape)
		}
		if p := orker.SecurityContext.Privileged; p != nil && *p {
			t.Errorf("%s: the orker container is privileged; only the engine of the privileged shape may be", shape)
		}
		id, _ := envOf(orker, "ORK_ORKER_ID")
		name, _ := envOf(orker, "POD_NAME")
		namespace, _ := envOf(orker, "POD_NAMESPACE")
		if id.Value != "$(POD_NAME).$(POD_NAMESPACE)" ||
			name.ValueFrom == nil || name.ValueFrom.FieldRef == nil || name.ValueFrom.FieldRef.FieldPath != "metadata.name" ||
			namespace.ValueFrom == nil || namespace.ValueFrom.FieldRef == nil || namespace.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
			t.Errorf("%s: ORK_ORKER_ID is %q, want $(POD_NAME).$(POD_NAMESPACE) from the pod's own name and namespace", shape, id.Value)
		}
	}
}

// TestKubernetesOrker_TheShapesDifferInPrivilegeAndNothingElse: one engine
// start-up, which keeps every task in the engine container's own cgroup; the
// user-namespaced shape asks for the orkstack-engine RuntimeClass and
// privileges nothing, and the fallback privileges the engine alone.
func TestKubernetesOrker_TheShapesDifferInPrivilegeAndNothingElse(t *testing.T) {
	t.Parallel()
	userns, usernsEngine, _ := orkerDeployment(t, "orker-userns")
	privileged, privilegedEngine, _ := orkerDeployment(t, "orker-privileged")
	script := engineScript(t, "orker-userns", usernsEngine)
	if got := engineScript(t, "orker-privileged", privilegedEngine); got != script {
		t.Errorf("the two shapes start their engine differently:\n%s\n---\n%s", script, got)
	}
	if !strings.Contains(script, `--cgroup-parent="$cg/tasks"`) || !strings.Contains(script, `/proc/self/cgroup`) {
		t.Errorf("the engine's tasks are not placed under the engine container's own cgroup:\n%s", script)
	}
	if usernsEngine.Image != privilegedEngine.Image {
		t.Errorf("the shapes run different engines: %q and %q", usernsEngine.Image, privilegedEngine.Image)
	}

	if userns.HostUsers == nil || *userns.HostUsers {
		t.Error("orker-userns: hostUsers is not false")
	}
	var class struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Handler string `yaml:"handler"`
	}
	if err := yaml.Unmarshal(readRepoFile(t, "deploy/kubernetes/orker-userns/runtimeclass.yaml"), &class); err != nil {
		t.Fatal(err)
	}
	if class.Kind != "RuntimeClass" || userns.RuntimeClassName != class.Metadata.Name {
		t.Errorf("orker-userns runs in RuntimeClass %q, but runtimeclass.yaml declares the %s %q", userns.RuntimeClassName, class.Kind, class.Metadata.Name)
	}
	if !strings.Contains(string(readRepoFile(t, "deploy/kubernetes/kind/kind.yaml")), "runtimes."+class.Handler+"]") {
		t.Errorf("the kind cluster declares no containerd handler %q", class.Handler)
	}
	if p := usernsEngine.SecurityContext.Privileged; p != nil && *p {
		t.Error("orker-userns: the engine is privileged")
	}

	if privileged.HostUsers != nil || privileged.RuntimeClassName != "" {
		t.Error("orker-privileged: asks for a user namespace or a RuntimeClass, which the fallback is for clusters without")
	}
	if p := privilegedEngine.SecurityContext.Privileged; p == nil || !*p {
		t.Error("orker-privileged: the engine is not privileged")
	}
	if len(privilegedEngine.SecurityContext.Extra) != 0 {
		t.Errorf("orker-privileged: the engine's securityContext sets more than privileged: %v", privilegedEngine.SecurityContext.Extra)
	}
}

// TestKubernetesOrker_ImagesArePinned: the engine is the release of Docker
// whose CLI the orker image carries, by digest; the orker image is the name
// an overlay sets to a release by digest; every image the proof runs is
// pinned, or built by the proof from this tree, and its infrastructure is
// the development stack's.
func TestKubernetesOrker_ImagesArePinned(t *testing.T) {
	t.Parallel()
	cli := stageNamed(t, dockerfileStages(t), "docker-cli").image
	version, _, _ := strings.Cut(strings.TrimPrefix(cli, "docker:"), "-cli@")
	for _, shape := range kubernetesShapes {
		_, engine, orker := orkerDeployment(t, shape)
		if !regexp.MustCompile(`^docker:` + regexp.QuoteMeta(version) + `-dind@sha256:[0-9a-f]{64}$`).MatchString(engine.Image) {
			t.Errorf("%s: the engine is %q, want docker:%s-dind by digest, the release of the orker's CLI (%s)", shape, engine.Image, version, cli)
		}
		if orker.Image != "orker" {
			t.Errorf("%s: the orker image is %q, want the name orker, which the kustomization says to set by digest", shape, orker.Image)
		}
		kustomization := string(readRepoFile(t, "deploy/kubernetes/"+shape+"/kustomization.yaml"))
		if !strings.Contains(kustomization, "- name: orker") || !strings.Contains(kustomization, "digest: sha256:") {
			t.Errorf("%s/kustomization.yaml does not say how to set the orker image by digest", shape)
		}
	}

	dev := parseCompose(t, devComposePath, readRepoFile(t, devComposePath))
	pinned := regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
	built := regexp.MustCompile(`^localhost/orkstack-k8s/[a-z-]+:proof$`)
	images := map[string]bool{}
	for _, rel := range []string{
		"deploy/kubernetes/kind/infra/infra.yaml",
		"deploy/kubernetes/kind/rootless-dind/rootless-dind.yaml",
	} {
		for _, image := range imagesIn(t, rel) {
			images[image] = true
			if !pinned.MatchString(image) && !built.MatchString(image) {
				t.Errorf("%s runs %q, neither pinned by digest nor built by the proof", rel, image)
			}
		}
	}
	for _, service := range []string{"temporal", "seaweedfs", "registry"} {
		if want := dev.service(t, service).Image; !images[want] {
			t.Errorf("the proof's infrastructure does not run %s as deploy/docker-compose.yml pins it (%s)", service, want)
		}
	}
}

// imagesIn lists every `image:` of the Kubernetes documents in rel.
func imagesIn(t *testing.T, rel string) []string {
	t.Helper()
	var images []string
	var walk func(node any)
	walk = func(node any) {
		switch v := node.(type) {
		case map[string]any:
			for key, value := range v {
				if s, ok := value.(string); ok && key == "image" {
					images = append(images, s)
				}
				walk(value)
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		}
	}
	decoder := yaml.NewDecoder(bytes.NewReader(readRepoFile(t, rel)))
	for {
		var doc any
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		walk(doc)
	}
	if len(images) == 0 {
		t.Fatalf("%s names no image", rel)
	}
	return images
}

// TestKubernetesOrker_TheEnginesLimitsCoverTheOrkersCapacity: every task runs
// inside the engine container's cgroup, so its limits bound them all
// together. The Orker declares a capacity in both dimensions, admits no task
// that would take what it has reserved above it, and reserves each task's
// limit, so the engine's limits hold that capacity with room for the daemon;
// the capacity holds one task at the per-task cap, or the Orker refuses to
// boot; and the requests equal the limits, so the pod is given what it may
// use. The check's component resizes the pod, and keeps the same rule.
func TestKubernetesOrker_TheEnginesLimitsCoverTheOrkersCapacity(t *testing.T) {
	t.Parallel()
	for _, shape := range kubernetesShapes {
		_, engine, orker := orkerDeployment(t, shape)
		checkSizing(t, shape, engine, orker)
		engine, orker = withCheckComponent(t, engine, orker)
		checkSizing(t, shape+" under deploy/kubernetes/check/common", engine, orker)
	}
}

func checkSizing(t *testing.T, what string, engine, orker k8sContainer) {
	t.Helper()
	value := func(name string) float64 {
		e, ok := envOf(orker, name)
		if !ok {
			t.Fatalf("%s: the orker sets no %s", what, name)
		}
		return k8sQuantity(t, e.Value)
	}
	for _, resource := range []string{"cpu", "memory", "ephemeral-storage"} {
		if engine.Resources.Requests[resource] != engine.Resources.Limits[resource] || engine.Resources.Limits[resource] == "" {
			t.Errorf("%s: the engine requests %s %q and is limited to %q, want both set and equal", what, resource,
				engine.Resources.Requests[resource], engine.Resources.Limits[resource])
		}
	}
	capCPU, capMemory := value("ORK_ORKER_CAPACITY_CPU"), value("ORK_ORKER_CAPACITY_MEMORY")
	if maxCPU := value("ORK_ORKER_MAX_CPU"); capCPU < maxCPU {
		t.Errorf("%s: ORK_ORKER_CAPACITY_CPU %g is less than one task at ORK_ORKER_MAX_CPU %g", what, capCPU, maxCPU)
	}
	if maxMemory := value("ORK_ORKER_MAX_MEMORY"); capMemory < maxMemory {
		t.Errorf("%s: ORK_ORKER_CAPACITY_MEMORY %.0f bytes is less than one task at ORK_ORKER_MAX_MEMORY %.0f", what, capMemory, maxMemory)
	}
	if cpu := k8sQuantity(t, engine.Resources.Limits["cpu"]); cpu < capCPU {
		t.Errorf("%s: the engine's cpu %s is less than the Orker's capacity, %g", what, engine.Resources.Limits["cpu"], capCPU)
	}
	if memory := k8sQuantity(t, engine.Resources.Limits["memory"]); memory <= capMemory {
		t.Errorf("%s: the engine's memory %s holds no more than the Orker's capacity, %.0f bytes, with nothing left for the daemon",
			what, engine.Resources.Limits["memory"], capMemory)
	}
}

// withCheckComponent applies deploy/kubernetes/check/common's patch of the
// Deployment to one base's engine and Orker, as kustomize merges it: the
// engine's resources replaced, and the Orker's env merged by name.
func withCheckComponent(t *testing.T, engine, orker k8sContainer) (k8sContainer, k8sContainer) {
	t.Helper()
	const rel = "deploy/kubernetes/check/common/kustomization.yaml"
	var component struct {
		Patches []struct {
			Target struct {
				Kind string `yaml:"kind"`
			} `yaml:"target"`
			Patch string `yaml:"patch"`
		} `yaml:"patches"`
	}
	if err := yaml.Unmarshal(readRepoFile(t, rel), &component); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	for _, p := range component.Patches {
		if p.Target.Kind != "Deployment" {
			continue
		}
		var d k8sDeployment
		if err := yaml.Unmarshal([]byte(p.Patch), &d); err != nil {
			t.Fatalf("%s: the Deployment patch: %v", rel, err)
		}
		for _, c := range d.Spec.Template.Spec.InitContainers {
			if c.Name == "engine" && c.Resources.Limits != nil {
				engine.Resources = c.Resources
			}
		}
		for _, c := range d.Spec.Template.Spec.Containers {
			if c.Name != "orker" {
				continue
			}
			env := append([]k8sEnv(nil), orker.Env...)
		patched:
			for _, e := range c.Env {
				for i := range env {
					if env[i].Name == e.Name {
						env[i] = e
						continue patched
					}
				}
				env = append(env, e)
			}
			orker.Env = env
		}
		return engine, orker
	}
	t.Fatalf("%s patches no Deployment", rel)
	return engine, orker
}

// k8sQuantity reads the Kubernetes quantity forms these manifests use: a
// plain or decimal number, millis, and the binary suffixes.
func k8sQuantity(t *testing.T, raw string) float64 {
	t.Helper()
	scale, number := 1.0, raw
	for _, unit := range []struct {
		suffix string
		factor float64
	}{{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"m", 1e-3}} {
		if trimmed, ok := strings.CutSuffix(raw, unit.suffix); ok {
			scale, number = unit.factor, trimmed
			break
		}
	}
	value, err := strconv.ParseFloat(number, 64)
	if err != nil || value < 0 || math.IsInf(value, 0) {
		t.Fatalf("cannot read the quantity %q", raw)
	}
	return value * scale
}

// TestKubernetesOrkerCheck_CreatesNothingClusterWideAndKeepsToItsPool: the
// check deploys the bases into a cluster that is not its own, so it creates
// nothing cluster-wide (the RuntimeClass of the user-namespaced base is
// required, never applied), and its Orkers join a pool of their own that
// every one of its tasks names, so they take no other run's tasks and its
// tasks land on no other Orker.
func TestKubernetesOrkerCheck_CreatesNothingClusterWideAndKeepsToItsPool(t *testing.T) {
	t.Parallel()
	common := string(readRepoFile(t, "deploy/kubernetes/check/common/kustomization.yaml"))
	if !regexp.MustCompile(`(?m)^\s+\$patch: delete\n\s+apiVersion: node\.k8s\.io/v1\n\s+kind: RuntimeClass\n\s+metadata:\n\s+name: orkstack-engine$`).MatchString(common) {
		t.Error("deploy/kubernetes/check/common does not delete the base's RuntimeClass, so the check would create it cluster-wide")
	}
	pool := regexp.MustCompile(`(?m)^\s+- name: ORK_ORKER_POOL\n\s+value: (\S+)$`).FindStringSubmatch(common)
	if pool == nil {
		t.Fatal("deploy/kubernetes/check/common does not put the check's Orkers in a pool of their own")
	}
	if pool[1] == "main" || pool[1] == "" {
		t.Errorf("the check's Orkers join the pool %q, which real Orkers serve", pool[1])
	}
	flows, err := filepath.Glob(filepath.Join(repoRoot(t), "deploy", "kubernetes", "check", "orkflows", "*.ork.yaml"))
	if err != nil || len(flows) == 0 {
		t.Fatalf("deploy/kubernetes/check/orkflows holds no Orkflow (%v)", err)
	}
	for _, path := range flows {
		rel := "deploy/kubernetes/check/orkflows/" + filepath.Base(path)
		var doc struct {
			Tasks []struct {
				ID   string `yaml:"id"`
				Pool string `yaml:"pool"`
			} `yaml:"tasks"`
		}
		if err := yaml.Unmarshal(readRepoFile(t, rel), &doc); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if len(doc.Tasks) == 0 {
			t.Fatalf("%s has no tasks", rel)
		}
		for _, task := range doc.Tasks {
			if task.Pool != pool[1] {
				t.Errorf("%s: task %s runs on pool %q, not the check's own %q", rel, task.ID, task.Pool, pool[1])
			}
		}
	}
}
