package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// handWorld is a walk with sources on an older release.
type handWorld struct {
	*passWorld
	h        *SourceHandover
	releases []ReleaseObservation
	relErr   error
	fleet    []FleetOrker
	fleetErr error
	indexed  []SourceIndexed
	indexErr error
	spec     []byte
	specErr  error
	withheld []string
	asked    []string
}

func (w *handWorld) ObserveReleases(context.Context) (ReleaseObservation, error) {
	w.asked = append(w.asked, "releases")
	obs := w.releases[0]
	if len(w.releases) > 1 {
		w.releases = w.releases[1:]
	}
	return obs, w.relErr
}

func (w *handWorld) ObserveOrkers(context.Context) ([]OrkerPresence, error) {
	w.asked = append(w.asked, "orkers")
	var out []OrkerPresence
	for _, o := range w.fleet {
		out = append(out, o.Presence)
	}
	return out, w.fleetErr
}

func (w *handWorld) DescribeOrkerByID(_ context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	for _, o := range w.fleet {
		if o.Presence.Identity == id {
			return o.Described, o.Err
		}
	}
	return nil, errors.New("no such Orker")
}

func routed(current, ramping string, percent float32) ReleaseObservation {
	return ReleaseObservation{Routing: Routing{Current: current, Ramping: ramping, RampPercent: percent}, EngineReplicas: map[string]int{"v1": 1, "v2": 1}}
}

func handHost(id, build string, slots, used int32) FleetOrker {
	return FleetOrker{Presence: OrkerPresence{Identity: id}, Described: &orkstackv1.DescribeOrkerResult{OrkerId: id, BuildId: build, Pool: "gpu",
		ServiceHost: &orkstackv1.ServiceHost{Fit: true, MaxInstances: slots, Instances: used, ContractVersions: []int32{1},
			Profiles: []*orkstackv1.ServiceProfileVersions{{Name: "event-source", Versions: []int32{1}}}}}}
}

// newHandWorld is a deployment whose current release is v2, with n sources of
// one instance on v1, and slots on v2's host.
func newHandWorld(t *testing.T, n int, slots int32) *handWorld {
	t.Helper()
	w := &handWorld{passWorld: newPassWorld(t, func(s *orkstackv1.EventSource) { s.Instances = 1 }), releases: []ReleaseObservation{routed("v2", "", 0)},
		fleet: []FleetOrker{handHost("new", "v2", slots, 0), handHost("old", "v1", 4, 4)}}
	specSrc := startSource()
	specSrc.Image = "ghcr.io/acme/s@sha256:" + strings.Repeat("a", 64)
	_, body, _, err := eventsources.BuildServiceSpec(specSrc, eventsources.Image{Contracts: []int32{1}, Profiles: []int32{1}}, eventsources.Limits{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.spec = body
	w.h = &SourceHandover{BuildID: "v2", Releases: w, Fleet: w, MaxSources: 2,
		Index: func(context.Context, time.Time) ([]SourceIndexed, error) {
			w.asked = append(w.asked, "index")
			return w.indexed, w.indexErr
		},
		Spec:     func(context.Context, string, string) ([]byte, bool, error) { return w.spec, w.spec != nil, w.specErr },
		Withheld: func(cause string, ids []string) { w.withheld = append(w.withheld, cause+" "+strings.Join(ids, ",")) }}
	w.Handover = w.h
	w.c.by, w.c.pendingOf, w.c.tries = map[string]*workflowpb.WorkflowExecutionInfo{}, map[string][]*workflowpb.PendingActivityInfo{}, map[string]int32{}
	for i := range n {
		id, name := fmt.Sprintf("evs-%016x", i+1), fmt.Sprintf("s%d", i)
		src := startSource()
		src.Id, src.Name, src.Uses, src.Instances, src.ServiceRevision, src.ResumeSeq, src.ServiceDigest = id, name, "ghcr.io/acme/s:1", 1, 3, 1, strings.Repeat("a", 64)
		if _, err := w.docs.Create(t.Context(), src); err != nil {
			t.Fatal(err)
		}
		w.on(t, id, name, "v1", 1, nil)
	}
	// The one that newPassWorld made is on the current release, with nothing.
	w.on(t, workSource, "orders", "v2", 0, func(c *eventsource.Carry) { c.Loaded.Paused = true })
	w.edit(t, "orders", func(s *orkstackv1.EventSource) { s.Pause = &orkstackv1.EventSourcePause{} })
	return w
}

func (w *handWorld) edit(t *testing.T, name string, f func(*orkstackv1.EventSource)) {
	t.Helper()
	src, err := w.docs.Get(t.Context(), "team", name)
	if err != nil {
		t.Fatal(err)
	}
	f(src)
	if _, err := w.docs.Replace(t.Context(), src, src.GetEtag()); err != nil {
		t.Fatal(err)
	}
}

// on is an open workflow on a release, with n instances on that release's host.
func (w *handWorld) on(t *testing.T, id, name, release string, n int32, edit func(*eventsource.Carry)) {
	t.Helper()
	c := eventsource.FirstCarry(sourcePinned())
	c.Loaded = &eventsource.LoadedFacts{Revision: 3, Instances: 1, ResumeSeq: 1}
	c.Good = &eventsource.Spec{Revision: 3, Digest: strings.Repeat("a", 64)}
	if edit != nil {
		edit(&c)
	}
	now := eventsource.Now{Load: "ok", Wanted: n, Source: eventsource.Source{Namespace: "team", Name: name, Pool: "gpu", Instances: n}}
	cb, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	nb, err := now.Encode()
	if err != nil {
		t.Fatal(err)
	}
	w.c.by[id] = &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "r-" + name}, Status: running,
		Type: &commonpb.WorkflowType{Name: SourceWorkflowType}, Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			sourceMemoCarry: jsonPayload(string(cb)), sourceMemoNow: jsonPayload(string(nb))}},
		VersioningInfo: &workflowpb.WorkflowExecutionVersioningInfo{DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "ork", BuildId: release}}}
	for range n {
		host := map[string]string{"v1": "old", "v2": "new"}[release]
		w.c.pendingOf[id] = append(w.c.pendingOf[id], &workflowpb.PendingActivityInfo{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, LastWorkerIdentity: host, Attempt: 1})
	}
}

// told is one walk's hand-overs, in the order they were told.
func (w *handWorld) told(t *testing.T) []string {
	t.Helper()
	var out []string
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		if err := w.walk(ctx); err != nil && w.relErr == nil && w.fleetErr == nil {
			t.Error(err)
		}
		w.c.mu.Lock()
		for _, s := range w.c.signals {
			id, rest, _ := strings.Cut(s, " ")
			run, rest, _ := strings.Cut(rest, " ")
			if strings.HasPrefix(rest, SignalSourceHandover) {
				out = append(out, strings.TrimLeft(strings.TrimPrefix(id, "evs-"), "0")+" "+run)
			}
		}
		w.c.mu.Unlock()
		cancel()
		w.watchers.Wait()
	})
	return out
}

// TestSourceHandover_TellsWhatFits: each source is told on the execution that
// was described, as the current release's hosts have room, in the order of
// their ids; what is read last is the releases and the Orkers.
func TestSourceHandover_TellsWhatFits(t *testing.T) {
	w := newHandWorld(t, 3, 2)
	if got, want := w.told(t), []string{"1 r-s0", "2 r-s1"}; !slices.Equal(got, want) {
		t.Errorf("told %v; want %v (%v)", got, want, w.did)
	}
	if want := []string{"releases", "index", "releases", "orkers"}; !slices.Equal(w.asked, want) {
		t.Errorf("read %v; want %v", w.asked, want)
	}
	for name, tc := range map[string]struct {
		edit     func(*testing.T, *handWorld)
		told     int
		withheld string
	}{
		"no ramp, no release but its own": {edit: func(t *testing.T, w *handWorld) {
			for _, info := range w.c.by {
				info.VersioningInfo.DeploymentVersion.BuildId = "v2"
			}
		}},
		"a ramp":                  {edit: func(_ *testing.T, w *handWorld) { w.releases = []ReleaseObservation{routed("v2", "v3", 5)} }},
		"a ramp at zero":          {edit: func(_ *testing.T, w *handWorld) { w.releases = []ReleaseObservation{routed("v2", "v3", 0)} }, told: 3},
		"another current release": {edit: func(_ *testing.T, w *handWorld) { w.releases = []ReleaseObservation{routed("v3", "", 0)} }},
		"a rollback while it walked": {edit: func(_ *testing.T, w *handWorld) {
			w.releases = []ReleaseObservation{routed("v2", "", 0), routed("v1", "", 0)}
		}},
		"a ramp begun while it walked": {edit: func(_ *testing.T, w *handWorld) {
			w.releases = []ReleaseObservation{routed("v2", "", 0), routed("v2", "v3", 1)}
		}},
		"the releases not read":          {edit: func(_ *testing.T, w *handWorld) { w.relErr = errWorkStore }},
		"a cluster with no releases":     {edit: func(_ *testing.T, w *handWorld) { w.relErr = ErrNotVersioned }},
		"the Orkers not read":            {edit: func(_ *testing.T, w *handWorld) { w.fleetErr = errWorkStore }},
		"a deployment that runs none":    {edit: func(_ *testing.T, w *handWorld) { w.Starter.Runs = false }},
		"a host that does not answer":    {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Err = errWorkStore }},
		"a host that is cordoned":        {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Described.Cordoned = true }},
		"a host that is not fit":         {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Described.ServiceHost.Fit = false }},
		"a host of another pool":         {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Described.Pool = "arm" }},
		"a host that hosts none":         {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Described.ServiceHost = nil }},
		"a host without its contract":    {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Described.ServiceHost.ContractVersions = []int32{2} }},
		"a host without its profile":     {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Described.ServiceHost.Profiles[0].Versions = []int32{2} }},
		"a host of another profile":      {edit: func(_ *testing.T, w *handWorld) { w.fleet[0].Described.ServiceHost.Profiles[0].Name = "grant" }},
		"a spec that is not there":       {edit: func(_ *testing.T, w *handWorld) { w.spec = nil }},
		"a spec of a later release":      {edit: func(_ *testing.T, w *handWorld) { w.spec = []byte(`{"format":9}`) }},
		"a spec that is corrupt":         {edit: func(_ *testing.T, w *handWorld) { w.specErr = eventsources.ErrSpecCorrupt }},
		"a spec that cannot be read now": {edit: func(_ *testing.T, w *handWorld) { w.specErr = errWorkStore }},
		"a carry of a later release": {edit: func(_ *testing.T, w *handWorld) {
			w.c.by["evs-0000000000000001"].Memo.Fields[sourceMemoCarry] = jsonPayload(`{"format":2}`)
		}, told: 2},
		"a task pending ends the turn":     {edit: func(_ *testing.T, w *handWorld) { w.c.tries["evs-0000000000000002"] = 1 }, told: 1},
		"a blocked one is passed over":     {edit: func(_ *testing.T, w *handWorld) { w.c.tries["evs-0000000000000002"] = 3 }, told: 2},
		"no engine of their release polls": {edit: func(_ *testing.T, w *handWorld) { w.releases[0].EngineReplicas = map[string]int{"v2": 1} }},
		"a paused one asks for nothing": {edit: func(t *testing.T, w *handWorld) {
			w.fleet[0].Described.ServiceHost.MaxInstances = 2
			w.edit(t, "s0", func(s *orkstackv1.EventSource) { s.Pause = &orkstackv1.EventSourcePause{} })
		}, told: 3},
		"a marked one needs no host": {edit: func(t *testing.T, w *handWorld) {
			w.fleet = w.fleet[1:]
			w.edit(t, "s0", func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.New(workNow) })
		}, told: 1},
		"a held namespace's": {edit: func(_ *testing.T, w *handWorld) {
			w.fleet[0].Described.ServiceHost.MaxInstances = 0
			w.c.ns.Hold = &orkstackv1.NamespaceHold{}
		}},
		"one not described": {edit: func(_ *testing.T, w *handWorld) { w.c.by["evs-0000000000000002"].Type.Name = RunWorkflowType }},

		// What no document accounts for.
		"one with no document takes room": {edit: func(t *testing.T, w *handWorld) {
			w.fleet[0].Described.ServiceHost.MaxInstances = 4
			w.on(t, "evs-00000000000000ff", "gone", "v1", 2, nil)
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000ff", Open: true}, {ID: workSource, Open: true}}
		}, told: 2},
		"one that closed with a tombstone": {edit: func(_ *testing.T, w *handWorld) {
			w.c.tombs["evs-00000000000000ff"] = true
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000ff"}}
		}, told: 3},
		"one the index shows closed, and open now": {edit: func(t *testing.T, w *handWorld) {
			w.on(t, "evs-00000000000000ff", "gone", "v1", 8, nil)
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000ff"}}
		}},
		"one that closed with none": {edit: func(t *testing.T, w *handWorld) {
			w.on(t, "evs-00000000000000ff", "gone", "v1", 8, nil)
			w.c.by["evs-00000000000000ff"].Status = completed
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000ff"}}
		}, told: 3},
		"an id that is no source's": {edit: func(_ *testing.T, w *handWorld) {
			w.indexed = []SourceIndexed{{ID: "../x"}, {ID: "ork-1", Open: true}, {ID: strings.Repeat("x", 4000), Open: true}}
		}, told: 3},
		"a memo of fewer than none": {edit: func(t *testing.T, w *handWorld) {
			w.fleet[0].Described.ServiceHost.MaxInstances = 1
			w.on(t, "evs-00000000000000ff", "gone", "v1", 0, nil)
			w.c.by["evs-00000000000000ff"].Memo.Fields[sourceMemoNow] = jsonPayload(`{"source":{"namespace":"team","name":"gone","pool":"gpu","instances":-9}}`)
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000ff", Open: true}}
		}, told: 1},
		"the index not read": {edit: func(_ *testing.T, w *handWorld) { w.indexErr = errWorkStore }, withheld: "index "},
		"a tombstone not read": {edit: func(_ *testing.T, w *handWorld) {
			w.c.tombErr = errWorkStore
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000ff"}}
		}, withheld: "describe evs-00000000000000ff"},
		"one that cannot be described": {edit: func(t *testing.T, w *handWorld) {
			w.on(t, "evs-00000000000000ff", "gone", "v1", 1, nil)
			w.c.by["evs-00000000000000ff"].Type.Name = RunWorkflowType
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000ff", Open: true}}
		}, withheld: "describe evs-00000000000000ff"},
		"more than are described": {edit: func(_ *testing.T, w *handWorld) {
			w.indexed = []SourceIndexed{{ID: "evs-00000000000000fd", Open: true}, {ID: "evs-00000000000000fe", Open: true}, {ID: "evs-00000000000000ff", Open: true}}
		}, withheld: "count 1 past 2"},
		"a document not read now": {edit: func(_ *testing.T, w *handWorld) { w.Documents = &unreadDocument{PassDocuments: w.docs, name: "s1"} }, withheld: "describe team/s1"},
		"a namespace not read":    {edit: func(_ *testing.T, w *handWorld) { w.c.nsErr = errWorkStore }},
	} {
		t.Run(name, func(t *testing.T) {
			w := newHandWorld(t, 3, 8)
			tc.edit(t, w)
			told := w.told(t)
			if len(told) != tc.told {
				t.Errorf("told %v; want %d (did %v)", told, tc.told, w.did)
			}
			switch {
			case tc.withheld == "" && len(w.withheld) == 0:
			case len(w.withheld) == 1 && (w.withheld[0] == tc.withheld || (strings.HasSuffix(tc.withheld, "*") && strings.HasPrefix(w.withheld[0], strings.TrimSuffix(tc.withheld, "*")))):
				if !slices.Contains(w.did, PassHandoverWithheld) {
					t.Errorf("did %v", w.did)
				}
			default:
				t.Errorf("withheld %q; want %q", w.withheld, tc.withheld)
			}
		})
	}
}

// TestSourceHandover_StartsWhatItTold: a source that was told is started the
// moment it has closed, and looked at no longer than a stop can take.
func TestSourceHandover_StartsWhatItTold(t *testing.T) {
	const id = "evs-0000000000000001"
	w := newHandWorld(t, 1, 4)
	synctest.Test(t, func(t *testing.T) {
		if err := w.walk(t.Context()); err != nil {
			t.Fatal(err)
		}
		if w.watching.Load() != 1 {
			t.Fatalf("%d watched", w.watching.Load())
		}
		time.Sleep(40 * time.Second)
		w.c.mu.Lock()
		carry := string(w.c.by[id].Memo.Fields[sourceMemoCarry].Data)
		w.c.by[id].Status = completed
		w.c.mu.Unlock()
		closed := time.Now()
		w.watchers.Wait()
		if waited := time.Since(closed); waited > time.Second || len(w.c.started) != 1 || w.c.inputs[0].ID != id || string(w.c.inputs[0].Carry) != carry {
			t.Errorf("after %s: %d started, %+v", waited, len(w.c.started), w.c.inputs)
		}
	})
	// One that does not close.
	w = newHandWorld(t, 1, 4)
	synctest.Test(t, func(t *testing.T) {
		if err := w.walk(t.Context()); err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		w.watchers.Wait()
		if took := time.Since(began); took < 150*time.Second || took > 152*time.Second || len(w.c.started) != 0 || w.watching.Load() != 0 {
			t.Errorf("after %s: %d started", took, len(w.c.started))
		}
	})
	// No more than ten are watched at once.
	w = newHandWorld(t, 14, 40)
	synctest.Test(t, func(t *testing.T) {
		for range 2 {
			if err := w.walk(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if n := w.watching.Load(); n != handoverMost {
			t.Errorf("%d watched", n)
		}
		w.watchers.Wait()
	})
}

// The index is read to its end, for what is open or closed a moment ago.
func TestSourceIndex(t *testing.T) {
	c := &indexPages{}
	got, err := SourceIndex(c, "ork-main")(t.Context(), time.Unix(1_800_000_000, 0))
	want := []SourceIndexed{{ID: "evs-1", Open: true}, {ID: "evs-2"}, {ID: "evs-3", Open: true}}
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("%v, %v; want %v", got, err, want)
	}
	if q := c.queries[0]; q != "WorkflowType = 'EventSourceWorkflow' AND (ExecutionStatus = 'Running' OR CloseTime >= '2027-01-15T08:00:00Z')" || len(c.queries) != 2 {
		t.Errorf("asked %q", c.queries)
	}
	c.err = serviceerror.NewUnavailable("x")
	if got, err := SourceIndex(c, "ork-main")(t.Context(), time.Now()); err == nil || got != nil {
		t.Errorf("%v, %v", got, err)
	}
}

type indexPages struct {
	queries []string
	err     error
}

func (p *indexPages) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	p.queries = append(p.queries, req.GetQuery())
	row := func(id string, status enumspb.WorkflowExecutionStatus) *workflowpb.WorkflowExecutionInfo {
		return &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}, Status: status}
	}
	if len(req.GetNextPageToken()) == 0 {
		return &workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{row("evs-1", running), row("evs-2", completed)}, NextPageToken: []byte("2")}, p.err
	}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{row("evs-3", running)}}, p.err
}

// unreadDocument fails the read of one document.
type unreadDocument struct {
	PassDocuments
	name string
}

func (u *unreadDocument) Get(ctx context.Context, ns, name string) (*orkstackv1.EventSource, error) {
	if name == u.name {
		return nil, errWorkStore
	}
	return u.PassDocuments.Get(ctx, ns, name)
}
