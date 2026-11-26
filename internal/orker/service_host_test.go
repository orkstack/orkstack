package orker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
)

func TestServiceHost_ReportedOnlyForASourceHost(t *testing.T) {
	a := NewActivitiesWithOptions(nil, Options{Runtime: &reapRecorder{}, WorkRoot: t.TempDir()})
	got, err := a.DescribeOrker(context.Background())
	if err != nil || got.GetServiceHost() != nil {
		t.Fatalf("an Orker that hosts no sources reported %v, %v", got.GetServiceHost(), err)
	}
}

func TestServiceHost_FitOnlyWhenEveryConditionHolds(t *testing.T) {
	a := NewActivitiesWithOptions(nil, Options{Runtime: &reapRecorder{}, WorkRoot: t.TempDir(),
		SourceHost: SourceHostSettings{MaxInstances: 4, MaxRequestCPU: "500m", MaxRequestMemory: "256Mi", EngineURL: "https://ork:8080"}})
	got, _ := a.DescribeOrker(context.Background())
	sh := got.GetServiceHost()
	if sh == nil || sh.GetFit() || len(sh.GetUnfit()) != 3 || sh.GetMaxInstances() != 4 || sh.GetContractVersions()[0] != 1 ||
		sh.GetProfiles()[0].GetName() != "event-source" || sh.GetMaxRequest().GetMemory() != "256Mi" {
		t.Fatalf("before any probe: %v", sh)
	}
	a.services.setAnswerFS(true)
	a.services.setProbe(servicehost.ProbeResult{Ran: true, Fitness: servicehost.Fitness{Fit: true}}, time.Now())
	got, _ = a.DescribeOrker(context.Background())
	if sh := got.GetServiceHost(); sh.GetFit() || len(sh.GetUnfit()) != 1 || sh.GetUnfit()[0] != unfitNoStorage {
		t.Fatalf("nowhere in memory for a service's files: %v", sh)
	}
	a.services.setStorage("/run/user/1/orkstack")
	got, _ = a.DescribeOrker(context.Background())
	if sh := got.GetServiceHost(); !sh.GetFit() || len(sh.GetUnfit()) != 0 || sh.GetProbedAt() == nil {
		t.Fatalf("every condition holds: %v", sh)
	}
	a.cordon.SetServiceStuck("SERVICE_STOP_STUCK: a container is not gone", time.Now())
	got, _ = a.DescribeOrker(context.Background())
	if sh := got.GetServiceHost(); sh.GetFit() || sh.GetUnfit()[0] != unfitStuck {
		t.Fatalf("a stuck service: %v", sh)
	}
	// The Orker takes no work while it holds, so its answer must say it is
	// cordoned: the engine's census would otherwise count it as serving its
	// pool. No operator's reason and no other cause, so the pool is not live.
	if !got.GetCordoned() || got.GetCordonReason() != "" || got.GetStoreOutage() != nil || got.GetStopping() != nil {
		t.Fatalf("a stuck service is reported cordoned by its own cause alone: cordoned=%v reason=%q", got.GetCordoned(), got.GetCordonReason())
	}
	if cordoned, reason, _ := a.cordon.State(); !cordoned || reason == "" {
		t.Fatal("a stuck service cordons the Orker")
	}
	a.cordon.ClearServiceStuck()
	if cordoned, _, _ := a.cordon.State(); cordoned {
		t.Fatal("clearing the stuck service left the Orker cordoned")
	}
	// An unclean answer while hosting is asked again; unclean twice, the
	// Orker is unfit and says which question.
	unclean := servicehost.ProbeResult{Ran: true, Fitness: servicehost.Fitness{Failures: []string{"question 3: tls reached 10.0.0.5:7233 (service, answered OK)"}}}
	a.services.setProbe(unclean, time.Now())
	got, _ = a.DescribeOrker(context.Background())
	if sh := got.GetServiceHost(); !sh.GetFit() {
		t.Fatalf("one unclean probe while hosting: %v", sh)
	}
	a.services.setProbe(unclean, time.Now())
	got, _ = a.DescribeOrker(context.Background())
	if sh := got.GetServiceHost(); sh.GetFit() || len(sh.GetUnfit()) != 1 || !strings.Contains(sh.GetUnfit()[0], "question 3") {
		t.Fatalf("an unclean probe, twice: %v", sh)
	}
}

// networkRuntime is a runtime that holds service networks, for the boot
// reap of what an earlier process of the same Orker left.
type networkRuntime struct {
	reapRecorder
	label   string
	listed  []string
	removed []string
	failOn  string
}

func (n *networkRuntime) ListServiceNetworks(_ context.Context, label string) ([]string, error) {
	n.label = label
	return n.listed, nil
}

func (n *networkRuntime) RemoveServiceNetwork(_ context.Context, id string) error {
	if id == n.failOn {
		return errors.New("network has active endpoints")
	}
	n.removed = append(n.removed, id)
	return nil
}

// TestBoot_ReapsItsOwnServiceNetworks: the networks an earlier process of
// this Orker made are removed at boot, each tried, a failure logged and not
// fatal, so a crash mid-probe does not use the runtime's address pools up.
func TestBoot_ReapsItsOwnServiceNetworks(t *testing.T) {
	rt := &networkRuntime{listed: []string{"n1", "n2", "n3"}, failOn: "n2"}
	a := NewActivitiesWithOptions(nil, Options{Runtime: rt, WorkRoot: t.TempDir(), OrkerID: "orker-7"})
	a.reapServiceNetworks(context.Background())
	if rt.label != "ork.orker=orker-7" || strings.Join(rt.removed, ",") != "n1,n3" {
		t.Fatalf("listed by %q, removed %q", rt.label, rt.removed)
	}
}
