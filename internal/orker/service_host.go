package orker

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The reasons an Orker is not fit to host services (14 §9), one fixed
// sentence each, reported on DescribeOrker; the reach probe's own say what it
// found (servicehost.ReachWatch).
const (
	unfitNoAnswerFS = "it cannot make a service's bounded answer filesystem on this runtime"
	unfitStuck      = "a service container it stopped is not gone (stuck service)"
	unfitNoEngine   = "it has no engine to call (orker.engine.url)"
)

// serviceHostState is what an Orker knows of its own fitness to host
// services: whether it can make the answer filesystem, its reach probe's
// history, and how many instances it runs.
type serviceHostState struct {
	mu        sync.Mutex
	answerFS  bool
	reach     *servicehost.ReachWatch
	probedAt  time.Time
	instances int32
}

// setAnswerFS records whether this Orker's runtime makes the bounded
// answer filesystem.
func (s *serviceHostState) setAnswerFS(ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.answerFS = ok
}

// setProbe records one run of the reach probe.
func (s *serviceHostState) setProbe(r servicehost.ProbeResult, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reach == nil {
		s.reach = servicehost.NewReachWatch(0)
	}
	s.reach.Record(r, at)
	s.probedAt = at
}

// startReach begins the probe's history, with its hourly offset.
func (s *serviceHostState) startReach(offset time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reach = servicehost.NewReachWatch(offset)
}

// reachNext is when the probe runs next.
func (s *serviceHostState) reachNext() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reach == nil {
		return time.Now()
	}
	return s.reach.Next()
}

// reachVerdict is the reach probe's verdict at now.
func (s *serviceHostState) reachVerdict(now time.Time) servicehost.ReachVerdict {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reach == nil {
		return servicehost.ReachVerdict{Reasons: []string{"no reach probe has run"}}
	}
	return s.reach.Verdict(now)
}

// describeServiceHost reports hosting services, for an Orker configured
// to host event sources: the versions it hosts, and fit only when every
// condition of 14 §9 holds, each that does not named.
func (a *Activities) describeServiceHost(result *orkstackv1.DescribeOrkerResult) {
	if !a.sourceHost.HostsSources() {
		return
	}
	a.services.mu.Lock()
	answerFS, probedAt, instances := a.services.answerFS, a.services.probedAt, a.services.instances
	a.services.mu.Unlock()
	reach := a.services.reachVerdict(time.Now())
	host := &orkstackv1.ServiceHost{
		ContractVersions: append([]int32{}, servicehost.HostContracts...),
		Profiles:         []*orkstackv1.ServiceProfileVersions{{Name: eventsources.ProfileEventSource, Versions: append([]int32{}, servicehost.HostProfiles...)}},
		MaxInstances:     int32(a.sourceHost.MaxInstances),
		Instances:        instances,
		MaxRequest:       &orkstackv1.ResourceRequest{Cpu: a.sourceHost.MaxRequestCPU, Memory: a.sourceHost.MaxRequestMemory},
	}
	if !probedAt.IsZero() {
		host.ProbedAt = timestamppb.New(probedAt)
	}
	if !answerFS {
		host.Unfit = append(host.Unfit, unfitNoAnswerFS)
	}
	if !reach.MayHost {
		host.Unfit = append(host.Unfit, reach.Reasons...)
	}
	if a.cordon != nil && a.cordon.ServiceStuck() != nil {
		host.Unfit = append(host.Unfit, unfitStuck)
		// The cause holds this Orker's cordon, so the answer says cordoned:
		// with no operator's reason, store outage, stop or profile cause
		// beside it, the engine's census reads the pool as not live from
		// this Orker, which polls for nothing while it holds.
		result.Cordoned = true
	}
	if a.sourceHost.EngineURL == "" {
		host.Unfit = append(host.Unfit, unfitNoEngine)
	}
	host.Fit = len(host.Unfit) == 0
	result.ServiceHost = host
}

// serviceNetworkReaper is a runtime that can list and remove the service
// networks it made.
type serviceNetworkReaper interface {
	ListServiceNetworks(ctx context.Context, label string) ([]string, error)
	RemoveServiceNetwork(ctx context.Context, id string) error
}

// reapServiceNetworks removes, at boot, every service network an earlier
// process of this Orker left (a crash mid-probe or mid-service leaves two a
// probe), after the containers on them are gone. Each is tried; one that
// cannot be removed is logged and tried again at the next boot.
func (a *Activities) reapServiceNetworks(ctx context.Context) {
	rt, ok := a.dispatcher.Runtime.(serviceNetworkReaper)
	if !ok || a.dispatcher.OrkerID == "" {
		return
	}
	log := a.dispatcher.logger()
	ids, err := rt.ListServiceNetworks(ctx, "ork.orker="+a.dispatcher.OrkerID)
	if err != nil {
		log.Warn("service networks could not be listed; any this Orker left stay", "error", err)
		return
	}
	removed := 0
	for _, id := range ids {
		if err := rt.RemoveServiceNetwork(ctx, id); err != nil {
			log.Warn("a service network this Orker left could not be removed", "network", id, "error", err)
			continue
		}
		removed++
	}
	if removed > 0 {
		log.Info("service networks reaped", "removed", removed)
	}
}
