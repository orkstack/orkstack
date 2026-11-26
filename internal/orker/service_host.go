package orker

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The reasons an Orker is not fit to host services (14 §9), one fixed
// sentence each, reported on DescribeOrker; the reach probe's own say what it
// found (servicehost.ReachWatch).
const (
	unfitNoAnswerFS = "it cannot make a service's bounded answer filesystem on this runtime"
	unfitStuck      = "a service container it stopped is not gone (stuck service)"
	unfitNoEngine   = "it has no engine to call (orker.engine.url)"
	unfitNoStorage  = "it has no memory-backed directory for a service's secrets and conversation (orker.identity.dir)"
)

// serviceHostState is what an Orker knows of its own fitness to host
// services: whether it can make the answer filesystem, its reach probe's
// history, and how many instances it runs.
type serviceHostState struct {
	mu       sync.Mutex
	answerFS bool
	reach    *servicehost.ReachWatch
	probedAt time.Time
	// policy is the deployment's image policy as the engine last gave it, nil
	// before it did.
	policy *orkstackv1.ImagePolicySelection
	// denied counts the attempts in a row the engine refused this Orker a
	// permission for, and lacks is what its self-check last found.
	denied int
	lacks  string
	// hosted are the attempts of instances this Orker has taken.
	hosted map[*hostedInstance]bool
	// parse is the Orker's share of the bodies its instances read at once.
	parse *sourcehost.ParseShare
	// storage is the memory-backed directory services' files are kept in.
	storage string
	// stuck are the services' containers that could not be stopped, each with
	// what discards its files once it is gone.
	stuck map[ContainerID]func()
}

// setStorage records where services' files are kept; "" is nowhere.
func (s *serviceHostState) setStorage(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storage = dir
}

// storageDir is where services' files are kept.
func (s *serviceHostState) storageDir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storage
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
	probedAt := a.services.probedAt
	a.services.mu.Unlock()
	host := &orkstackv1.ServiceHost{
		ContractVersions: append([]int32{}, servicehost.HostContracts...),
		Profiles:         []*orkstackv1.ServiceProfileVersions{{Name: eventsources.ProfileEventSource, Versions: append([]int32{}, servicehost.HostProfiles...)}},
		MaxInstances:     int32(a.sourceHost.MaxInstances),
		Instances:        a.services.hostedNow(),
		MaxRequest:       &orkstackv1.ResourceRequest{Cpu: a.sourceHost.MaxRequestCPU, Memory: a.sourceHost.MaxRequestMemory},
	}
	if !probedAt.IsZero() {
		host.ProbedAt = timestamppb.New(probedAt)
	}
	host.Unfit = a.unfitToHost()
	if a.cordon != nil && a.cordon.ServiceStuck() != nil {
		// The cause holds this Orker's cordon, so the answer says cordoned:
		// with no operator's reason, store outage, stop or profile cause
		// beside it, the engine's census reads the pool as not live from
		// this Orker, which polls for nothing while it holds.
		result.Cordoned = true
	}
	host.Fit = len(host.Unfit) == 0
	result.ServiceHost = host
}

// unfitToHost is why this Orker is not fit to host services, none where it is.
func (a *Activities) unfitToHost() (unfit []string) {
	a.services.mu.Lock()
	answerFS := a.services.answerFS
	a.services.mu.Unlock()
	if !answerFS {
		unfit = append(unfit, unfitNoAnswerFS)
	}
	if a.services.storageDir() == "" {
		unfit = append(unfit, unfitNoStorage)
	}
	if reach := a.services.reachVerdict(time.Now()); !reach.MayHost {
		unfit = append(unfit, reach.Reasons...)
	}
	if a.cordon != nil && a.cordon.ServiceStuck() != nil {
		unfit = append(unfit, unfitStuck)
	}
	if a.sourceHost.EngineURL == "" {
		unfit = append(unfit, unfitNoEngine)
	}
	if lacks, denied := a.services.notPermitted(); denied {
		unfit = append(unfit, lacks)
	}
	return unfit
}

// keepStuckService has the Orker take no further service until a container that
// could not be stopped is gone, which its removal retry sees to.
func (a *Activities) keepStuckService(id ContainerID, cleanup func()) {
	a.services.mu.Lock()
	if a.services.stuck == nil {
		a.services.stuck = map[ContainerID]func(){}
	}
	a.services.stuck[id] = cleanup
	// Under the lock the last one's end is decided under.
	a.cordon.SetServiceStuck(fmt.Sprintf("%s: container %s is not gone", eventsources.ReasonServiceStopStuck, id), time.Now())
	a.services.mu.Unlock()
	a.dispatcher.logger().Error("a service's container could not be stopped; this Orker takes no further service until it is gone", "container", string(id))
}

// keepStuckAtBoot is keepStuckService for the services' containers of this Orker's
// previous life that its boot could not remove, whose cordon ended with it. One
// nobody could name may be one.
func (a *Activities) keepStuckAtBoot(own []OwnContainer, unlisted bool, stayed []ContainerID) {
	if !a.sourceHost.HostsSources() {
		return
	}
	for _, id := range stayed {
		if unlisted || slices.ContainsFunc(own, func(c OwnContainer) bool { return ContainerID(c.ID) == id && c.Source != "" }) {
			a.keepStuckService(id, func() {
				ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
				defer cancel()
				a.reapServiceNetworks(ctx)
			})
		}
	}
}

// settleStuckServices discards what the containers that are gone now had, and
// ends the cordon once none is left.
func (a *Activities) settleStuckServices() {
	a.services.mu.Lock()
	var gone []func()
	for id, cleanup := range a.services.stuck {
		if a.dispatcher.unremoved.owes(id) || a.capacity != nil && a.capacity.owes(id) {
			continue
		}
		delete(a.services.stuck, id)
		gone = append(gone, cleanup)
	}
	none := len(gone) > 0 && len(a.services.stuck) == 0
	if none {
		a.cordon.ClearServiceStuck()
	}
	a.services.mu.Unlock()
	for _, cleanup := range gone {
		cleanup()
	}
	if none {
		a.dispatcher.logger().Info("the services' containers that could not be stopped are gone; this Orker takes services again")
	}
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
