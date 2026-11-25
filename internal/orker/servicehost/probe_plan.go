package servicehost

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
)

// MetadataAddresses are question 1's closed list (14 §7): the metadata
// probe's addresses (internal/orker/metadata_probe.go, docs/DEPLOY.md's
// table), the other clouds' this contract adds, and the names such endpoints
// go by. A name is dialled only where it resolves to one of these or to a
// link-local address (reachprobe).
var MetadataAddresses = []string{
	"169.254.169.254", "fd00:ec2::254", "169.254.170.2", "169.254.170.23", "168.63.129.16",
	"100.100.100.200", "192.0.0.192", "metadata.google.internal",
}

// The cluster's other ports a probe asks at the frontend's addresses: the
// history, matching and worker services' gRPC ports, and the membership
// ports beside each service (14 §7, question 3).
var (
	clusterServicePorts    = []int{7234, 7235, 7239}
	clusterMembershipPorts = []int{6933, 6934, 6935, 6939}
)

// ProbePlan is what an Orker knows that the reach probe asks about.
type ProbePlan struct {
	// OperatorAddresses are orker.services.probe_addresses (host:port).
	OperatorAddresses []string
	// AdminPort is the Orker's admin listener's port, 0 when it has none;
	// LoopbackCanaryPort the canary it binds on loopback for the probe.
	AdminPort, LoopbackCanaryPort int
	// HostAddresses are the addresses by which a container on a service
	// network reaches its host: the network's gateway and the host's
	// interface addresses.
	HostAddresses []string
	// Cluster is the address (host:port) the Orker reaches the cluster's
	// frontend at; "" asks nothing of question 3 but its absence.
	Cluster string
	// ClusterAddresses are the addresses the Orker resolves a cluster named
	// by name to.
	ClusterAddresses []string
	// Canaries are the listening canaries' addresses (host:port).
	Canaries []string
}

// Targets is the probe's argument list: every target of the four
// questions, each once, none at a loopback address (it means nothing from
// a container).
func (p ProbePlan) Targets() []string {
	var out []string
	seen := map[string]bool{}
	add := func(q int, kind, via, addr, name string) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil || isOwn(host) {
			return
		}
		t := probeTarget(q, kind, via, addr, name)
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	for _, a := range MetadataAddresses {
		add(QuestionMetadata, TargetAddress, ViaTCP, net.JoinHostPort(a, "80"), "")
	}
	for _, a := range p.OperatorAddresses {
		add(QuestionMetadata, TargetAddress, ViaTCP, a, "")
	}
	for _, h := range p.HostAddresses {
		for _, port := range []int{p.AdminPort, p.LoopbackCanaryPort} {
			if port > 0 {
				add(QuestionLoopback, TargetAddress, ViaTCP, net.JoinHostPort(h, strconv.Itoa(port)), "")
			}
		}
	}
	if host, port, err := net.SplitHostPort(p.Cluster); err == nil {
		hosts := append([]string{host}, p.reachedAt(host)...)
		for _, a := range p.ClusterAddresses {
			hosts = append(hosts, p.reachedAt(a)...)
		}
		frontend, _ := strconv.Atoi(port)
		for _, h := range hosts {
			for _, sp := range append([]int{frontend}, clusterServicePorts...) {
				a := net.JoinHostPort(h, strconv.Itoa(sp))
				for _, name := range p.under(h) {
					add(QuestionCluster, TargetService, ViaPlaintext, a, name)
					add(QuestionCluster, TargetService, ViaTLS, a, name)
				}
			}
			for _, mp := range clusterMembershipPorts {
				add(QuestionCluster, TargetMembership, ViaTCP, net.JoinHostPort(h, strconv.Itoa(mp)), "")
			}
		}
	}
	for _, c := range p.Canaries {
		add(QuestionCanary, TargetAddress, ViaTCP, c, "")
	}
	return out
}

// under is the names a cluster's service port is asked under at host: none
// ("") always, and the cluster's own name at any other address. A container
// may show a server either, and one that judges by the name may serve one
// and not the other.
func (p ProbePlan) under(host string) []string {
	cluster, _, err := net.SplitHostPort(p.Cluster)
	if err != nil || host == cluster || net.ParseIP(cluster) != nil || len(p.ClusterAddresses) == 0 {
		return []string{""}
	}
	return []string{"", cluster}
}

// reachedAt is where a container may reach what the Orker reaches at addr:
// at addr itself, and, when that is the host's, at every address by which a
// container reaches the host. A loopback or unspecified address is the
// host's to the Orker and the container's own to the container.
func (p ProbePlan) reachedAt(addr string) []string {
	switch {
	case isOwn(addr):
		return slices.Clone(p.HostAddresses)
	case contains(p.HostAddresses, addr):
		return append([]string{addr}, p.HostAddresses...)
	}
	return []string{addr}
}

// askedByAddress reports whether o, a target at the cluster's own name, was
// asked the same way at the same port of everywhere the addresses the Orker
// resolves that name to lead.
func (p ProbePlan) askedByAddress(o Observation, obs []Observation) bool {
	cluster, _, err := net.SplitHostPort(p.Cluster)
	host, port, herr := net.SplitHostPort(o.Target)
	if err != nil || herr != nil || o.Question != QuestionCluster || host != cluster || len(p.ClusterAddresses) == 0 {
		return false
	}
	for _, a := range p.ClusterAddresses {
		at := p.reachedAt(a)
		if len(at) == 0 {
			return false
		}
		for _, h := range at {
			names := []string{""}
			if o.Kind == TargetService {
				names = p.under(h)
			}
			for _, name := range names {
				want := probeTarget(o.Question, o.Kind, o.Via, net.JoinHostPort(h, port), name)
				if !slices.ContainsFunc(obs, func(b Observation) bool { return b.Asked() == want && !b.Unresolved }) {
					return false
				}
			}
		}
	}
	return true
}

// isOwn reports whether host names the machine it is asked from: its
// loopback, or the unspecified address.
func isOwn(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// Allowances are the questions an operator accepted, each by its own
// setting (orker.services.allow_*). Question 2 has none.
type Allowances struct {
	MetadataReach  bool // question 1
	OpenCluster    bool // question 3's service ports, never its membership ports
	ContainerReach bool // question 4
}

func (a Allowances) accepts(o Observation) bool {
	switch o.Question {
	case QuestionMetadata:
		return a.MetadataReach
	case QuestionCluster:
		// A cluster that asks its clients for no credential is what is
		// accepted. Its membership ports authenticate nobody and are never.
		return a.OpenCluster && o.Kind == TargetService
	case QuestionCanary:
		return a.ContainerReach
	}
	return false
}

// JudgeAllowed judges a probe that must ask all four questions, with the
// questions an operator accepted not held against the host. A question that
// was not asked is never clean, accepted or not: a probe cannot pass by
// saying nothing.
func JudgeAllowed(obs []Observation, allow Allowances) Fitness {
	var kept []Observation
	for _, o := range obs {
		if allow.accepts(o) {
			// Asked, and accepted whatever it found: kept as asked only.
			o.Connected, o.Status, o.TLS = false, "", ""
		}
		kept = append(kept, o)
	}
	return JudgeProbe(kept, QuestionMetadata, QuestionLoopback, QuestionCluster, QuestionCanary)
}

// JudgeAnswered is the result of a probe of plan from what it answered. A
// name that gave it no address to ask is a target it never asked, and not
// asked is never clean: the probe did not run, and the result names the
// target. Three names are not held to that: one of a question the operator
// accepted, whose answer changes nothing; a metadata endpoint's own name,
// which resolves only inside its cloud and whose addresses are asked by
// themselves; and the cluster's, once every address the Orker resolves it to
// was asked instead. Something the probe did reach stands whatever else went
// unasked.
func JudgeAnswered(obs []Observation, plan ProbePlan, allow Allowances) ProbeResult {
	r := ProbeResult{Ran: true, Fitness: JudgeAllowed(obs, allow)}
	if !r.Fitness.Fit {
		return r
	}
	for _, o := range obs {
		host, _, err := net.SplitHostPort(o.Target)
		if !o.Unresolved || allow.accepts(o) || plan.askedByAddress(o, obs) || (err == nil && o.Question == QuestionMetadata && contains(MetadataAddresses, host)) {
			continue
		}
		return ProbeResult{Err: fmt.Sprintf("question %d could not be asked of %s: the name leads to no address the probe may ask on a service network", o.Question, o.Target)}
	}
	return r
}
