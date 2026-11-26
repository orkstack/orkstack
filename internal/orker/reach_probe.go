package orker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/servicehost/reachprobe"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The reach probe (docs/DEPLOY.md, "Source hosts"; docs/SECURITY_MODEL.md,
// "Service containers"): before an Orker that hosts services reports itself
// fit, and every hour, containers of the release's probe image ask, from a
// network made exactly as a service's is, the four questions of
// servicehost.ProbePlan, and the Orker judges the answer
// (servicehost.JudgeAnswered). Its containers carry this Orker's label, so a
// boot reaps one a dead process left, and run under the sandbox with no
// secret of any kind.

// serviceNetworker is what the probe needs of a runtime beyond running
// containers: networks of a service's own and the addresses on them.
type serviceNetworker interface {
	CreateServiceNetwork(ctx context.Context, name string, labels map[string]string) (string, error)
	RemoveServiceNetwork(ctx context.Context, id string) error
	ContainerAddress(ctx context.Context, id ContainerID, network string) (string, error)
	NetworkGateway(ctx context.Context, network string) (string, error)
	ContainerPid(ctx context.Context, id ContainerID) (int, error)
}

// ReachProbeConfig is one run's settings.
type ReachProbeConfig struct {
	// Image is the probe image, by digest.
	Image string
	// Allow are the questions the operator accepted.
	Allow servicehost.Allowances
	// OperatorAddresses are orker.services.probe_addresses.
	OperatorAddresses []string
	// AdminPort is this Orker's admin listener's port, 0 for none.
	AdminPort int
	// Cluster is the address this Orker reaches the cluster's frontend at.
	Cluster string
}

// ReachProbeRun is one run's result: the verdict, and whether the runtime
// made, and the Orker could open, a service's bounded answer filesystem.
type ReachProbeRun struct {
	Result   servicehost.ProbeResult
	AnswerFS bool
}

const (
	reachProbePort    = "8080"
	reachProbeTimeout = 4 * time.Minute
)

// RunReachProbe runs the probe once.
func (d *OCIDispatcher) RunReachProbe(ctx context.Context, cfg ReachProbeConfig) ReachProbeRun {
	ctx, cancel := context.WithTimeout(ctx, reachProbeTimeout)
	defer cancel()
	sn, ok := d.Runtime.(serviceNetworker)
	if !ok {
		return ReachProbeRun{Result: servicehost.ProbeResult{Err: "this Orker's runtime cannot make a service's network"}}
	}
	if cfg.Image == "" {
		return ReachProbeRun{Result: servicehost.ProbeResult{ImageRefused: true, Err: "none is named"}}
	}
	if _, err := d.Runtime.Pull(ctx, cfg.Image); err != nil {
		return ReachProbeRun{Result: servicehost.ProbeResult{ImageRefused: true,
			Err: fmt.Sprintf("%s could not be pulled: %s", cfg.Image, terminalSafeShort(err.Error()))}}
	}
	run := &reachRun{d: d, sn: sn, cfg: cfg, token: randomToken()}
	defer run.cleanup()
	return run.ask(ctx)
}

// reachRun is one probe's containers and networks, removed when it ends.
type reachRun struct {
	d          *OCIDispatcher
	sn         serviceNetworker
	cfg        ReachProbeConfig
	token      string
	networks   []string
	containers []ContainerID
}

func (r *reachRun) ask(ctx context.Context) ReachProbeRun {
	out := ReachProbeRun{}
	notRun := func(what string, err error) ReachProbeRun {
		out.Result = servicehost.ProbeResult{Err: what + ": " + terminalSafeShort(err.Error())}
		return out
	}
	probeNet, err := r.network(ctx, "p")
	if err != nil {
		return notRun("its network", err)
	}
	canaryNet, err := r.network(ctx, "c")
	if err != nil {
		return notRun("the canary's network", err)
	}
	// The loopback canary, which no container may open.
	loop, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return notRun("the loopback canary", err)
	}
	defer func() { _ = loop.Close() }()
	go acceptAndClose(loop)

	// The probe's own listener carries a service's answer filesystem, so one
	// container shows the Orker can make and open it.
	listener, err := r.start(ctx, "listen", NetworkService, probeNet, true, probeCommand("listen", reachProbePort))
	if err != nil {
		return notRun("its listener", err)
	}
	out.AnswerFS = r.openAnswer(ctx, listener)
	serviceCanary, err := r.start(ctx, "canary-s", NetworkService, canaryNet, false, probeCommand("listen", reachProbePort))
	if err != nil {
		return notRun("a canary", err)
	}
	defaultCanary, err := r.start(ctx, "canary-d", NetworkDefault, "", false, probeCommand("listen", reachProbePort))
	if err != nil {
		return notRun("a canary", err)
	}
	listenerAddr, err1 := r.sn.ContainerAddress(ctx, listener, probeNet)
	serviceAddr, err2 := r.sn.ContainerAddress(ctx, serviceCanary, canaryNet)
	defaultAddr, err3 := r.sn.ContainerAddress(ctx, defaultCanary, "")
	if errors.Is(err3, ErrNoContainerAddress) {
		// The default network gives a container no address another can dial:
		// there is no canary there to ask, only the way back from it.
		defaultAddr, err3 = "", nil
	}
	gateway, err4 := r.sn.NetworkGateway(ctx, probeNet)
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return notRun("an address", err)
	}
	// The positive control: every listener answers the Orker itself, before
	// the probe asks and after it, so a canary that never listened, or one
	// that died, is never read as a path that is closed.
	listeners := []string{net.JoinHostPort(listenerAddr, reachProbePort), net.JoinHostPort(serviceAddr, reachProbePort)}
	if defaultAddr != "" {
		listeners = append(listeners, net.JoinHostPort(defaultAddr, reachProbePort))
	}
	inside := false
	if rr, ok := r.d.Runtime.(interface {
		ReachControlFromInside(context.Context) (bool, error)
	}); ok {
		if inside, err = rr.ReachControlFromInside(ctx); err != nil {
			return notRun("where the positive control asks from", err)
		}
	}
	control := func(when string) *ReachProbeRun {
		var err error
		if inside {
			// The runtime leaves the host no route to a container's address
			// (ReachControlFromInside): each listener is asked from its own
			// network.
			runs := []dialRun{
				{"control-p", NetworkService, probeNet, []string{"4,address,tcp," + listeners[0]}},
				{"control-s", NetworkService, canaryNet, []string{"4,address,tcp," + listeners[1]}},
			}
			if len(listeners) > 2 {
				runs = append(runs, dialRun{"control-d", NetworkDefault, "", []string{"4,address,tcp," + listeners[2]}})
			}
			err = r.controlInside(ctx, runs)
		} else {
			err = awaitListening(ctx, listeners)
		}
		if err != nil {
			run := notRun("the positive control"+when, err)
			return &run
		}
		return nil
	}
	if failed := control(""); failed != nil {
		return *failed
	}
	hosts, err := hostAddresses()
	if err != nil {
		return notRun("the host's addresses", err)
	}
	if ha, ok := r.d.Runtime.(interface {
		HostAlias(context.Context) (string, error)
	}); ok {
		// The runtime's own name for the host, which may lead to its loopback:
		// asked as every other way a container reaches its host.
		alias, err := ha.HostAlias(ctx)
		if err != nil {
			return notRun("the runtime's name for the host", err)
		}
		if alias != "" {
			hosts = append(hosts, alias)
		}
	}
	own, err := ownAddresses()
	if err != nil {
		return notRun("the host's addresses", err)
	}
	clusterAddrs, err := clusterAddresses(ctx, r.cfg.Cluster, own, net.DefaultResolver.LookupIPAddr)
	if err != nil {
		return notRun("the cluster's name", err)
	}
	plan := servicehost.ProbePlan{
		OperatorAddresses:  r.cfg.OperatorAddresses,
		AdminPort:          r.cfg.AdminPort,
		LoopbackCanaryPort: loop.Addr().(*net.TCPAddr).Port,
		HostAddresses:      append([]string{gateway}, hosts...),
		Cluster:            r.cfg.Cluster,
		ClusterAddresses:   clusterAddrs,
		Canaries:           listeners[1:],
	}
	back := "4,address,tcp," + net.JoinHostPort(listenerAddr, reachProbePort)
	var dials []dialRun
	// The probe takes at most reachprobe.MaxTargets at once; a host with many
	// addresses is asked in several runs from the same network.
	targets := plan.Targets()
	for i := 0; i < len(targets); i += reachprobe.MaxTargets {
		dials = append(dials, dialRun{fmt.Sprintf("dial-p%d", i/reachprobe.MaxTargets), NetworkService, probeNet,
			targets[i:min(i+reachprobe.MaxTargets, len(targets))]})
	}
	// Both ways: neither canary's network reaches the probe's listener.
	dials = append(dials, dialRun{"dial-s", NetworkService, canaryNet, []string{back}}, dialRun{"dial-d", NetworkDefault, "", []string{back}})
	var obs []servicehost.Observation
	for _, dial := range dials {
		got, err := r.dial(ctx, dial.name, dial.mode, dial.network, dial.targets)
		if err != nil {
			return notRun(dial.name, err)
		}
		obs = append(obs, got...)
	}
	if failed := control(", after the probe"); failed != nil {
		return *failed
	}
	out.Result = servicehost.JudgeAnswered(obs, plan, r.cfg.Allow)
	return out
}

// dialRun is one dialling container: its name, network and targets.
type dialRun struct {
	name, mode, network string
	targets             []string
}

// controlInside asks each listener from its own network, a few times while
// it may still be starting, and needs every one to answer.
func (r *reachRun) controlInside(ctx context.Context, runs []dialRun) error {
	for _, c := range runs {
		var last error
		for attempt := range 10 {
			obs, err := r.dial(ctx, fmt.Sprintf("%s-%d", c.name, attempt), c.mode, c.network, c.targets)
			if err == nil && len(obs) == 1 && obs[0].Connected {
				last = nil
				break
			}
			last = fmt.Errorf("%s did not answer from its own network", c.targets[0])
			if err != nil {
				last = err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
		if last != nil {
			return last
		}
	}
	return nil
}

// awaitListening waits, within a bound, until every address accepts a
// connection from the Orker.
func awaitListening(ctx context.Context, addrs []string) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, a := range addrs {
		for {
			c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", a)
			if err == nil {
				_ = c.Close()
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("%s did not answer the Orker: %w", a, err)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}

func (r *reachRun) network(ctx context.Context, role string) (string, error) {
	id, err := r.sn.CreateServiceNetwork(ctx, fmt.Sprintf("ork-reach-%s-%s", r.token, role), r.labels(""))
	if err != nil {
		return "", err
	}
	r.networks = append(r.networks, id)
	return id, nil
}

func (r *reachRun) labels(task string) map[string]string {
	labels := map[string]string{"ork.task": "reachprobe"}
	if task != "" {
		labels["ork.task"] = "reachprobe-" + task
	}
	if r.d.OrkerID != "" {
		labels["ork.orker"] = r.d.OrkerID
	}
	return labels
}

// spec is a probe container: the task sandbox, no secret, no mount, a small
// fixed allowance, on the network asked for.
func (r *reachRun) spec(name, mode, network string, answer bool, cmd []string) ContainerSpec {
	spec := ContainerSpec{
		Image: r.cfg.Image, Command: cmd, WorkDir: "/", Network: mode, ServiceNetworkID: network,
		CPUs: "0.25", Memory: "64Mi", PidsLimit: 64, TmpfsPath: ContainerScratchTmp, TmpfsBytes: 1 << 20,
		Labels: r.labels(name), ReadOnlyRootfs: true, DropAllCapabilities: true, NoNewPrivileges: true,
		SeccompProfile: SeccompBuiltin, Init: true, NoCoreDumps: true, NoSwap: true, RunAsOrker: true,
	}
	if answer {
		spec.ServiceTmpfs = &ServiceTmpfs{Path: servicehost.PathAnswer, Bytes: 1 << 20, Inodes: 64, UID: os.Getuid(), GID: os.Getgid()}
	}
	return spec
}

func (r *reachRun) start(ctx context.Context, name, mode, network string, answer bool, cmd []string) (ContainerID, error) {
	id, err := r.d.Runtime.Create(ctx, r.spec(name, mode, network, answer, cmd))
	if id != "" {
		r.containers = append(r.containers, id)
	}
	if err != nil {
		return "", err
	}
	if err := r.d.Runtime.Start(ctx, id, &boundedBuffer{limit: 4096}, &boundedBuffer{limit: 4096}); err != nil {
		return "", err
	}
	return id, nil
}

// openAnswer opens the listener's answer filesystem as the Orker opens a
// service's (servicehost.OpenContainerAnswer).
func (r *reachRun) openAnswer(ctx context.Context, id ContainerID) bool {
	pid, err := r.sn.ContainerPid(ctx, id)
	if err != nil {
		r.d.logger().Warn("reach probe: the listener's process could not be found", "error", err)
		return false
	}
	dir, err := servicehost.OpenContainerAnswer(pid, servicehost.PathAnswer)
	if err != nil {
		r.d.logger().Warn("reach probe: a service's answer filesystem could not be opened", "error", err)
		return false
	}
	_ = dir.Close()
	return true
}

// dial runs one dialling container and reads its answer.
func (r *reachRun) dial(ctx context.Context, name, mode, network string, targets []string) ([]servicehost.Observation, error) {
	id, err := r.d.Runtime.Create(ctx, r.spec(name, mode, network, false, probeCommand(append([]string{"dial"}, targets...)...)))
	if id != "" {
		r.containers = append(r.containers, id)
	}
	if err != nil {
		return nil, err
	}
	stdout := &boundedBuffer{limit: servicehost.MaxProbeAnswerBytes + 1}
	if err := r.d.Runtime.Start(ctx, id, stdout, &boundedBuffer{limit: 4096}); err != nil {
		return nil, err
	}
	status, err := r.d.Runtime.Wait(ctx, id)
	if err != nil {
		return nil, err
	}
	if status.Code != 0 {
		return nil, fmt.Errorf("the probe exited %d", status.Code)
	}
	obs, err := servicehost.ParseProbeAnswer([]byte(stdout.String()))
	if err != nil {
		return nil, err
	}
	// One observation per target, each the target asked for: a probe that
	// answered fewer has not asked them all.
	if len(obs) != len(targets) {
		return nil, fmt.Errorf("the probe answered %d of %d targets", len(obs), len(targets))
	}
	for i, o := range obs {
		if got := o.Asked(); got != targets[i] {
			return nil, fmt.Errorf("the probe answered %q for target %q", got, targets[i])
		}
	}
	return obs, nil
}

func (r *reachRun) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, id := range r.containers {
		if err := r.d.Runtime.Remove(ctx, id); err != nil {
			r.d.logger().Warn("reach probe: a container could not be removed; the next boot reaps it", "container", id, "error", err)
		}
	}
	for _, id := range r.networks {
		if err := r.sn.RemoveServiceNetwork(ctx, id); err != nil {
			r.d.logger().Warn("reach probe: a network could not be removed; the next boot removes it", "network", id, "error", err)
		}
	}
}

// hostAddresses are the host's own IPv4 addresses a container may reach it
// by; loopback and link-local are left out.
func hostAddresses() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.To4() == nil || ipnet.IP.IsLoopback() || ipnet.IP.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, ipnet.IP.String())
	}
	return out, nil
}

// ownAddresses is every address of this host's interfaces.
func ownAddresses() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			out = append(out, ipnet.IP.String())
		}
	}
	return out, nil
}

// The bounds of resolving the cluster's name: one lookup's time, and the
// addresses one probe asks about.
const (
	clusterLookupTimeout = 10 * time.Second
	maxClusterAddresses  = 16
)

// clusterAddresses is what the Orker itself resolves the name in cluster
// (host:port) to, none for an address: what it reaches by a name its own
// hosts file or network knows, a container may reach by address. An address
// of this host's own (own) is its loopback: a container reaches the host at
// the addresses the plan lists. A name that resolves to nothing, or to more
// than the probe asks, is an error.
func clusterAddresses(ctx context.Context, cluster string, own []string, lookup func(context.Context, string) ([]net.IPAddr, error)) ([]string, error) {
	const loopback = "127.0.0.1"
	host, _, err := net.SplitHostPort(cluster)
	if err != nil || host == "" {
		return nil, nil // question 3 is not asked
	}
	if ip := net.ParseIP(host); ip != nil {
		// The plan lists the host's IPv4 addresses itself.
		if ip.To4() == nil && slices.Contains(own, ip.String()) {
			return []string{loopback}, nil
		}
		return nil, nil
	}
	if !servicehost.ProbeName(host) {
		return nil, fmt.Errorf("the probe cannot ask under the name %q", host)
	}
	ctx, cancel := context.WithTimeout(ctx, clusterLookupTimeout)
	defer cancel()
	ips, err := lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 || len(ips) > maxClusterAddresses {
		return nil, fmt.Errorf("%s resolves to %d addresses; the probe asks 1 to %d", host, len(ips), maxClusterAddresses)
	}
	var out []string
	for _, ip := range ips {
		addr := ip.IP.String()
		if slices.Contains(own, addr) {
			addr = loopback
		}
		out = append(out, addr)
	}
	return out, nil
}

func acceptAndClose(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_ = c.Close()
	}
}

func randomToken() string {
	var b [5]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// probeCommand is the reach probe's command in the orker image:
// `orker probe reach …`, an internal subcommand operators never run.
func probeCommand(args ...string) []string {
	return append([]string{"/usr/local/bin/orker", "probe", "reach"}, args...)
}

// addressPort is the port of an address (host:port), 0 when there is none.
func addressPort(addr string) int {
	_, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(port)
	return n
}

// hostGatewayOpen is what a probe found of the host's own listeners open to
// a service network: question 2's and 3's findings.
func hostGatewayOpen(f servicehost.Fitness) []string {
	var open []string
	for _, s := range f.Failures {
		if strings.HasPrefix(s, "question 2:") || strings.HasPrefix(s, "question 3:") {
			open = append(open, s)
		}
	}
	return open
}

// newReachImageSource is the reach probe's image: pinned, used as given and
// its build label checked, or, when pinned is empty, this release's own
// orker image (docs/DEPLOY.md, "Source hosts").
func newReachImageSource(pinned string, release *ReleaseProbe, logger *slog.Logger) *probeImageSource {
	src := newProbeImageSource(pinned, release, servicehost.ReachRetry)
	src.logger = logger
	return src
}

// resolveReachImage is the image this run of the reach probe uses, or why it has
// none: a fault of configuration, reported as the image's own reason.
func (a *Activities) resolveReachImage(ctx context.Context) (string, string, error) {
	rt, ok := a.dispatcher.Runtime.(hostViewRuntime)
	if !ok {
		return "", "this Orker's runtime cannot check a probe image", nil
	}
	if a.reachImage.pinned == "" && a.reachImage.release == nil {
		return "", "none is named: an Orker built from source sets " + ServicesProbeImageVar +
			" to the orker image of its own build, by digest", nil
	}
	a.askPolicy(ctx)
	// Judged before its registry is asked or anything of it is pulled: the one an
	// operator names, or this release's own, which is run by the digest its tag
	// resolves to.
	named, byDigest := a.reachImage.pinned, false
	if named == "" {
		named, byDigest = a.reachImage.release.Ref, true
	}
	if why := a.services.refusedByPolicy(named, byDigest); why != "" {
		return "", why, nil
	}
	image, reason, detail, err := checkProbeImage(ctx, rt, a.reachImage, a.build)
	if err != nil {
		return "", "", err
	}
	if reason != 0 {
		return "", detail, nil
	}
	// And as what it resolved to, which is what is run.
	if why := a.services.refusedByPolicy(image, false); why != "" {
		return "", why, nil
	}
	return image, "", nil
}

// askPolicy asks the engine what its image policy is now. No answer, and one that
// does not say, leave what was known.
func (a *Activities) askPolicy(ctx context.Context) {
	if a.engine == nil {
		return
	}
	ask, cancel := context.WithTimeout(ctx, identitySupportAsk)
	defer cancel()
	who, err := a.engine.Whoami(ask)
	if err != nil {
		a.dispatcher.logger().Warn("reach probe: the engine did not say what the deployment's image policy is; the probe's image is judged by what was known", "error", err)
		return
	}
	a.services.setPolicy(who.GetImagePolicy())
}

// setPolicy keeps the image policy as the engine last gave it; nil is no answer.
func (s *serviceHostState) setPolicy(p *orkstackv1.ImagePolicySelection) {
	if p == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policy = p
}

// refusedByPolicy is why the deployment's image policy does not permit the reach
// probe's image, "" where it does or nobody has said what it is. A namespace
// only adds a signer, which a probe is not held to. byDigest says that the image is
// run by a digest whatever it is named by.
func (s *serviceHostState) refusedByPolicy(image string, byDigest bool) string {
	s.mu.Lock()
	policy := s.policy
	s.mu.Unlock()
	if policy == nil {
		return ""
	}
	ref, err := dsl.ParseReference(image)
	if err != nil {
		return fmt.Sprintf("%s is no image reference", image)
	}
	if policy.GetRequireDigest() && ref.Digest == "" && !byDigest {
		return fmt.Sprintf("the deployment's image policy permits no image that is not named by its digest (require_digest), and %s is not", image)
	}
	if !task.RegistryAllowed(policy.GetAllowedRegistries(), ref.Registry, ref.Repository) {
		return fmt.Sprintf("the deployment's image policy (allowed_registries) does not permit %s", image)
	}
	return ""
}

// probed takes one run of the reach probe, and evicts what this Orker hosts where it
// is found unfit to.
func (a *Activities) probed(r servicehost.ProbeResult, at time.Time) {
	a.services.setProbe(r, at)
	verdict, log := a.services.reachVerdict(at), a.dispatcher.logger()
	switch {
	case verdict.Evict:
		why := strings.Join(verdict.Reasons, "; ")
		evicted := a.evictInstances(eventsources.ReasonServiceHostUnfit, why)
		log.Error("reach probe: this Orker is unfit to host services and evicts what it hosts (SERVICE_HOST_UNFIT)", "reasons", why, "instances_evicted", evicted)
	case !verdict.MayHost:
		log.Warn("reach probe: this Orker takes no new service", "reasons", strings.Join(verdict.Reasons, "; "))
	}
}

// RunReachProbes runs the reach probe for an Orker that hosts services, at
// once and then as servicehost.ReachWatch paces it, at an offset this Orker
// draws for itself, until ctx ends. adminAddr is this Orker's admin
// listener (host:port, "" for none) and cluster the cluster frontend's
// address, which the probe asks about.
func (a *Activities) RunReachProbes(ctx context.Context, adminAddr, cluster string) {
	if !a.sourceHost.HostsSources() {
		return
	}
	adminPort := addressPort(adminAddr)
	if adminAddr != "" && adminPort == 0 {
		a.dispatcher.logger().Error("reach probe: the admin address has no port the probe can ask; this Orker hosts no service",
			"admin", adminAddr)
		a.services.setProbe(servicehost.ProbeResult{Err: "the admin address " + adminAddr + " has no port to ask"}, time.Now())
		return
	}
	cfg := ReachProbeConfig{
		OperatorAddresses: a.sourceHost.ProbeAddresses, AdminPort: adminPort, Cluster: cluster,
		Allow: servicehost.Allowances{MetadataReach: a.sourceHost.AllowMetadataReach, OpenCluster: a.sourceHost.AllowOpenCluster,
			ContainerReach: a.sourceHost.AllowContainerReach},
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	offset := time.Duration(binary.BigEndian.Uint64(b[:]) % uint64(servicehost.ReachEvery))
	a.services.startReach(offset)
	log := a.dispatcher.logger()
	first := true
	for {
		var run ReachProbeRun
		image, refused, err := a.resolveReachImage(ctx)
		switch {
		case err != nil:
			return
		case refused != "":
			run.Result = servicehost.ProbeResult{ImageRefused: true, Err: refused}
		default:
			cfg.Image = image
			run = a.dispatcher.RunReachProbe(ctx, cfg)
		}
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		a.services.setAnswerFS(run.AnswerFS)
		a.probed(run.Result, now)
		next := a.services.reachNext()
		if first {
			first = false
			if open := hostGatewayOpen(run.Result.Fitness); len(open) > 0 {
				log.Warn("reach probe: this host's own listeners answer a service network; close them with the host firewall rule of docs/DEPLOY.md, \"Source hosts\"",
					"found", strings.Join(open, "; "))
			}
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
