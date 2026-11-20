package servicehost

import (
	"fmt"
	"net"
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
	// Canaries are the listening canaries' addresses (host:port).
	Canaries []string
}

// Targets is the probe's argument list: every target of the four
// questions, each once, none at a loopback address (it means nothing from
// a container).
func (p ProbePlan) Targets() []string {
	var out []string
	seen := map[string]bool{}
	add := func(q int, kind, via, addr string) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil || isLoopback(host) {
			return
		}
		t := fmt.Sprintf("%d,%s,%s,%s", q, kind, via, addr)
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	for _, a := range MetadataAddresses {
		add(QuestionMetadata, TargetAddress, ViaTCP, net.JoinHostPort(a, "80"))
	}
	for _, a := range p.OperatorAddresses {
		add(QuestionMetadata, TargetAddress, ViaTCP, a)
	}
	for _, h := range p.HostAddresses {
		for _, port := range []int{p.AdminPort, p.LoopbackCanaryPort} {
			if port > 0 {
				add(QuestionLoopback, TargetAddress, ViaTCP, net.JoinHostPort(h, strconv.Itoa(port)))
			}
		}
	}
	if host, port, err := net.SplitHostPort(p.Cluster); err == nil {
		hosts := []string{host}
		if isLoopback(host) || contains(p.HostAddresses, host) {
			hosts = append(hosts, p.HostAddresses...)
		}
		frontend, _ := strconv.Atoi(port)
		for _, h := range hosts {
			for _, sp := range append([]int{frontend}, clusterServicePorts...) {
				a := net.JoinHostPort(h, strconv.Itoa(sp))
				add(QuestionCluster, TargetService, ViaPlaintext, a)
				add(QuestionCluster, TargetService, ViaTLS, a)
			}
			for _, mp := range clusterMembershipPorts {
				add(QuestionCluster, TargetMembership, ViaTCP, net.JoinHostPort(h, strconv.Itoa(mp)))
			}
		}
	}
	for _, c := range p.Canaries {
		add(QuestionCanary, TargetAddress, ViaTCP, c)
	}
	return out
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
	OpenCluster    bool // question 3
	ContainerReach bool // question 4
}

func (a Allowances) accepts(q int) bool {
	switch q {
	case QuestionMetadata:
		return a.MetadataReach
	case QuestionCluster:
		return a.OpenCluster
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
		if allow.accepts(o.Question) {
			// Asked, and accepted whatever it found: kept as asked only.
			o.Connected, o.Status, o.TLS = false, "", ""
		}
		kept = append(kept, o)
	}
	return JudgeProbe(kept, QuestionMetadata, QuestionLoopback, QuestionCluster, QuestionCanary)
}
