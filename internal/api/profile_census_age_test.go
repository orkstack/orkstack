package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

const sensorDigest = "5e5e000000000000000000000000000000000000000000000000000000000000"

// profiledYAML names a device profile with its contract pinned in the
// document (sensor@1), so create reads no image for it.
const profiledYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: profiled
tasks:
  - id: read
    uses: busybox:latest
    pool: gpu
    devices: [sensor@1]
`

// slowFleet is a stub fleet whose inventory read takes `takes` on the
// replica's clock: the census is read at the clock's time, and the mint that
// follows sees it that much later.
type slowFleet struct {
	*stubFleet
	clock *stepClock
	takes time.Duration
}

func (f *slowFleet) ObserveOrkers(ctx context.Context) ([]engine.OrkerPresence, error) {
	f.clock.advance(f.takes)
	return f.stubFleet.ObserveOrkers(ctx)
}

// stepClock is a replica clock a test moves by hand.
type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *stepClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestSubmit_ProfiledCensusAgeGuard: a submit that pinned profile contracts
// is refused Unavailable, minting and claiming nothing, when the census its
// plan was judged on is more than engine.ProfileCensusMaxAge old at the mint,
// keyed or not; within the bound it starts.
func TestSubmit_ProfiledCensusAgeGuard(t *testing.T) {
	orker := currentOrker("gpu-1", "gpu", 0)
	orker.ProfileKindsSupported = []orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}
	orker.Profiles = []*orkstackv1.OrkerProfile{{
		Name: "sensor", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, IdentityDigest: sensorDigest,
		DefaultContract: "1", Contracts: []*orkstackv1.OrkerProfileContract{{Label: "1", IdentityDigest: sensorDigest}},
	}}
	for _, tc := range []struct {
		name  string
		takes time.Duration
		key   string
		want  connect.Code
	}{
		{"fresh, unkeyed", engine.ProfileCensusMaxAge - time.Second, "", 0},
		{"stale, unkeyed", engine.ProfileCensusMaxAge + time.Second, "", connect.CodeUnavailable},
		{"fresh, keyed", engine.ProfileCensusMaxAge - time.Second, "k-fresh", 0},
		{"stale, keyed", engine.ProfileCensusMaxAge + time.Second, "k-stale", connect.CodeUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &stepClock{now: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)}
			starter := &fakeStarter{}
			claimer := newSharedClaimer()
			minted := 0
			svc := api.NewService(api.Config{
				Starter: starter, Pools: livePools{poolQueue(t, "gpu"): 1},
				Fleet:       &slowFleet{stubFleet: fleetOf(orker), clock: clock, takes: tc.takes},
				Idempotency: claimer,
				Now:         clock.Now,
				NewRunID:    func() string { minted++; return fmt.Sprintf("run-%d", minted) },
			})
			applyYAML(t, svc, profiledYAML)
			_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName: "profiled", IdempotencyKey: tc.key}))
			if tc.want == 0 {
				if err != nil {
					t.Fatalf("submit = %v, want a start", err)
				}
				if len(starter.startedRuns()) != 1 {
					t.Fatalf("started %v, want one run", starter.startedRuns())
				}
				return
			}
			if connect.CodeOf(err) != tc.want || !strings.Contains(err.Error(), "submit again") {
				t.Fatalf("submit = %v, want %v asking to submit again", err, tc.want)
			}
			if len(starter.startedRuns()) != 0 || minted != 0 {
				t.Fatalf("a refused submission started %v / minted %d ids", starter.startedRuns(), minted)
			}
			if proposed, _ := claimer.counts(); tc.key != "" && proposed != 0 {
				t.Fatalf("a refused keyed submission claimed its key (%d claims)", proposed)
			}
		})
	}
}

// TestPrepareScheduledRun_ProfiledCensusAgeGuard: a schedule's fire is held
// to the same freshness as a submit: a census older than
// engine.ProfileCensusMaxAge at the fire's mint is Unavailable, which the
// fire's local activity retries, never a refusal that would record the fire
// refused; within the bound the fire is prepared.
func TestPrepareScheduledRun_ProfiledCensusAgeGuard(t *testing.T) {
	orker := currentOrker("gpu-1", "gpu", 0)
	orker.ProfileKindsSupported = []orkstackv1.ProfileKind{orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}
	orker.Profiles = []*orkstackv1.OrkerProfile{{
		Name: "sensor", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES, IdentityDigest: sensorDigest,
		DefaultContract: "1", Contracts: []*orkstackv1.OrkerProfileContract{{Label: "1", IdentityDigest: sensorDigest}},
	}}
	rec := &orkstackv1.ScheduleRecord{ScheduleId: "sch-4a1c0e2b7d3f8156", Name: "nightly", Namespace: "default",
		OrkflowName: "profiled", Spec: dailySpec()}
	for _, tc := range []struct {
		name  string
		takes time.Duration
		stale bool
	}{
		{"fresh", engine.ProfileCensusMaxAge - time.Second, false},
		{"stale", engine.ProfileCensusMaxAge + time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &stepClock{now: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)}
			svc := api.NewService(api.Config{
				Starter: &fakeStarter{}, Pools: livePools{poolQueue(t, "gpu"): 1},
				Fleet: &slowFleet{stubFleet: fleetOf(orker), clock: clock, takes: tc.takes},
				Now:   clock.Now,
			})
			applyYAML(t, svc, profiledYAML)
			prep, err := api.NewScheduleActivities(svc, fixedIndexes{}).PrepareScheduledRun(context.Background(), rec, clock.Now())
			if !tc.stale {
				if err != nil || prep.GetRun().GetId() == "" {
					t.Fatalf("fire = %v, %v; want it prepared", prep, err)
				}
				return
			}
			var refused *temporal.ApplicationError
			if connect.CodeOf(err) != connect.CodeUnavailable || (errors.As(err, &refused) && refused.Type() == engine.ScheduleFireRefusedType) {
				t.Fatalf("fire = %v, want Unavailable, retried and never refused", err)
			}
		})
	}
}
