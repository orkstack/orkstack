package orker

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// scriptedDoor is the engine's host service with answers a real one gives only
// in states no test can put it in on demand (a guard off on every replica, a read
// that fails, an access document that changes under a call). What a real one
// answers otherwise is the integration suites'.
type scriptedDoor struct {
	mu      sync.Mutex
	spec    *orkstackv1.ServiceSpec
	judging *orkstackv1.JudgingSpec
	policy  *orkstackv1.ImagePolicySelection
	// confirm answers the n-th confirmation, from 1; nil accepts it.
	confirm   func(n int) (*orkstackv1.DoorAnswer, error)
	offer     func(n int) (*orkstackv1.DoorAnswer, error)
	report    func(n int) (*orkstackv1.DoorAnswer, error)
	confirms  []*orkstackv1.ConfirmInstanceRequest
	at        []time.Time
	offers    int
	starts    int
	reports   []*orkstackv1.ReportInstanceRequest
	pauses    []string
	lastStart *orkstackv1.StartMessageRunRequest
	// probe, where it is set, is called for each probe and answers none before it
	// returns.
	probe func()
}

var ok = &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_OK}

func door(d orkstackv1.Disposition, reason string) *orkstackv1.DoorAnswer {
	return &orkstackv1.DoorAnswer{Disposition: d, Reason: reason}
}

func (e *scriptedDoor) ConfirmInstance(_ context.Context, r *connect.Request[orkstackv1.ConfirmInstanceRequest]) (*connect.Response[orkstackv1.ConfirmInstanceResponse], error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.confirms, e.at = append(e.confirms, r.Msg), append(e.at, time.Now())
	out := &orkstackv1.ConfirmInstanceResponse{Answer: ok, ImagePolicy: e.policy}
	if e.confirm != nil {
		a, err := e.confirm(len(e.confirms))
		if err != nil {
			return nil, err
		}
		if a != nil {
			out.Answer = a
		}
	}
	if r.Msg.GetWantServiceSpec() {
		out.Service = e.spec
	}
	_, src, err := eventsources.ReadJudgingSpec(e.judging.GetSpec())
	if err != nil {
		return nil, err
	}
	if r.Msg.GetInstance().GetJudgingRevision() != src.GetJudgingRevision() {
		out.Judging = e.judging
	}
	return connect.NewResponse(out), nil
}

func (e *scriptedDoor) ReportInstance(_ context.Context, r *connect.Request[orkstackv1.ReportInstanceRequest]) (*connect.Response[orkstackv1.ReportInstanceResponse], error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reports = append(e.reports, r.Msg)
	a := ok
	if e.report != nil {
		got, err := e.report(len(e.reports))
		if err != nil {
			return nil, err
		}
		a = got
	}
	return connect.NewResponse(&orkstackv1.ReportInstanceResponse{Answer: a}), nil
}

func (e *scriptedDoor) OfferMessage(context.Context, *connect.Request[orkstackv1.OfferMessageRequest]) (*connect.Response[orkstackv1.OfferMessageResponse], error) {
	e.mu.Lock()
	e.offers++
	n, offer := e.offers, e.offer
	e.mu.Unlock()
	a := &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_PROCEED,
		Parts: []*orkstackv1.PartToUpload{{Input: "body", Part: orkstackv1.MessagePart_MESSAGE_PART_BODY}}}
	if offer != nil {
		// Without the lock: one may take its time.
		got, err := offer(n)
		if err != nil {
			return nil, err
		}
		a = got
	}
	return connect.NewResponse(&orkstackv1.OfferMessageResponse{Answer: a}), nil
}

func (e *scriptedDoor) StartMessageRun(_ context.Context, r *connect.Request[orkstackv1.StartMessageRunRequest]) (*connect.Response[orkstackv1.StartMessageRunResponse], error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.starts++
	e.lastStart = r.Msg
	return connect.NewResponse(&orkstackv1.StartMessageRunResponse{Answer: &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_ACCEPTED, RunId: "ork-1"}}), nil
}

func (e *scriptedDoor) ProbeEventSource(context.Context, *connect.Request[orkstackv1.ProbeEventSourceRequest]) (*connect.Response[orkstackv1.ProbeEventSourceResponse], error) {
	if e.probe != nil {
		e.probe()
	}
	return connect.NewResponse(&orkstackv1.ProbeEventSourceResponse{Answer: ok}), nil
}

func (e *scriptedDoor) RequestEventSourcePause(_ context.Context, r *connect.Request[orkstackv1.RequestEventSourcePauseRequest]) (*connect.Response[orkstackv1.RequestEventSourcePauseResponse], error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pauses = append(e.pauses, r.Msg.GetCode())
	return connect.NewResponse(&orkstackv1.RequestEventSourcePauseResponse{Answer: door(orkstackv1.Disposition_DISPOSITION_STOP, "EVENT_SOURCE_PAUSED")}), nil
}

var _ orkstackv1connect.EventSourceHostServiceClient = (*scriptedDoor)(nil)

// sourceRuntime is a fault runtime whose container converses: it plays the
// container's side of the file contract, which the fixtures under test/fixtures
// do with real processes.
type sourceRuntime struct {
	*faultServiceRuntime
	answer string
	mu2    sync.Mutex
	status string
	queue  []string
	done   chan struct{}
	polled int
	once   sync.Once
	// misanswers, its container's settles name a verdict nobody gave.
	misanswers bool
	labels     map[string]string
	// pullTakes is how long its first pull takes.
	pullTakes time.Duration
	// starting is called as its container is started.
	starting func()
}

func (r *sourceRuntime) Pull(context.Context, string) (PullInfo, error) {
	// The image is there once pulled.
	time.Sleep(r.pullTakes)
	r.pullTakes = 0
	return PullInfo{}, r.failed("pull")
}

func (r *sourceRuntime) ImageLabel(_ context.Context, _, key string) (string, bool, error) {
	if err := r.failed("label"); err != nil {
		return "", false, err
	}
	value, ok := r.labels[key]
	return value, ok, nil
}

func (r *sourceRuntime) say(status string) {
	r.mu2.Lock()
	defer r.mu2.Unlock()
	r.status = status
}

func (r *sourceRuntime) Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error {
	if r.starting != nil {
		r.starting()
	}
	if err := r.faultServiceRuntime.Start(ctx, id, stdout, stderr); err != nil {
		return err
	}
	ask := ""
	for _, m := range r.spec.Mounts {
		if m.Destination == servicehost.PathAsk {
			ask = m.Source
		}
	}
	go r.converse(ask)
	return nil
}

func (r *sourceRuntime) place(name, body string) {
	tmp := filepath.Join(r.answer, "tmp", filepath.Base(name))
	_ = os.WriteFile(tmp, []byte(body), 0o644)
	_ = os.Rename(tmp, filepath.Join(r.answer, name))
}

func (r *sourceRuntime) converse(ask string) {
	var poll, verdict int64
	var last time.Time
	for {
		select {
		case <-r.done:
			return
		case <-time.After(100 * time.Millisecond):
		}
		r.mu2.Lock()
		if r.status != "" && time.Since(last) > 10*time.Second || r.status != "" && last.IsZero() {
			last = time.Now()
			r.place(servicehost.StatusName, r.status)
		}
		var p struct {
			Seq       int64  `json:"seq"`
			VerdictID string `json:"verdictId"`
		}
		if raw, err := os.ReadFile(filepath.Join(ask, "lanes/0/poll.json")); err == nil && json.Unmarshal(raw, &p) == nil && p.Seq > poll {
			poll = p.Seq
			r.polled++
			if len(r.queue) == 0 {
				r.place("lanes/0/empty.json", fmt.Sprintf(`{"schemaVersion":1,"seq":%d}`, p.Seq))
			} else {
				body := r.queue[0]
				r.queue = r.queue[1:]
				r.place("lanes/0/body", body)
				r.place("lanes/0/message.json", fmt.Sprintf(`{"schemaVersion":1,"seq":%d,"id":"m-%d","bodyBytes":%d,"deliveryCount":1}`, p.Seq, p.Seq, len(body)))
			}
		}
		if raw, err := os.ReadFile(filepath.Join(ask, "lanes/0/verdict.json")); err == nil && json.Unmarshal(raw, &p) == nil && p.Seq > verdict {
			verdict = p.Seq
			if r.misanswers {
				p.VerdictID = "another"
			}
			r.place("lanes/0/settled.json", fmt.Sprintf(`{"schemaVersion":1,"seq":%d,"verdictId":%q,"ok":true}`, p.Seq, p.VerdictID))
		}
		r.mu2.Unlock()
	}
}

const sourceReady = `{"schemaVersion":1,"ready":true,"check":{"ok":true},"broker":{"object":"dir:q","maxDeliveries":5,"messageLimitBytes":1048576}}`

// hostRig is an Orker that hosts sources, an engine and a container.
type hostRig struct {
	t      *testing.T
	a      *Activities
	rt     *sourceRuntime
	engine *scriptedDoor
	in     *orkstackv1.HostEventSourceInput
	stores string
	own    string
	beats  []eventsources.HeartbeatDetails
	mu     sync.Mutex
	// deaf, no heartbeat is answered.
	deaf     bool
	attempt  int32
	earlier  eventsources.HeartbeatDetails
	rotateAt time.Time
}

func hostedSource() *orkstackv1.EventSource {
	return &orkstackv1.EventSource{Id: "evs-0123456789abcdef", Namespace: "payments", Name: "orders", OrkflowName: "handle-order", Pool: "main",
		Image: "ghcr.io/acme/source@sha256:" + strings.Repeat("b", 64), Resources: &orkstackv1.ResourceRequest{Cpu: "500m", Memory: "256Mi"},
		Concurrency: 1, Rate: 100, JudgingRevision: 7}
}

// newHostRig is made inside the bubble its test runs in.
func newHostRig(t *testing.T) *hostRig {
	t.Helper()
	stores, memory, answer := t.TempDir(), t.TempDir(), t.TempDir()
	for _, d := range []string{"tmp", "lanes/0"} {
		must.OK(t, os.MkdirAll(filepath.Join(answer, d), 0o755))
	}
	r := &hostRig{t: t, stores: stores, own: filepath.Join(memory, "orker-1"), attempt: 1,
		rt: &sourceRuntime{faultServiceRuntime: newFaultServiceRuntime(), answer: answer, status: sourceReady,
			labels: map[string]string{eventsources.ContractLabel: "1", eventsources.ProfileLabel: "event-source/1"}, done: make(chan struct{})},
		engine: &scriptedDoor{policy: &orkstackv1.ImagePolicySelection{}},
		in: &orkstackv1.HostEventSourceInput{Namespace: "payments", Name: "orders", EventSourceId: "evs-0123456789abcdef", Index: 1, Instances: 2, Epoch: 12,
			Resources: &orkstackv1.ResourceRequest{Cpu: "500m", Memory: "256Mi"}, ConfirmUnavailableMaxSeconds: 3600}}
	r.serve(hostedSource())
	r.judge(hostedSource())
	logs, err := artifacts.NewLocalLogStore(stores)
	must.OK(t, err)
	records, err := artifacts.NewLocalEventSourceRecordStore(stores)
	must.OK(t, err)
	attempts, err := artifacts.NewLocalAttemptStore(stores)
	must.OK(t, err)
	policy := DefaultSecurityPolicy()
	r.a = NewActivitiesWithOptions(nil, Options{Runtime: r.rt, WorkRoot: t.TempDir(), OrkerID: "orker-1", BuildID: "b-1", Pool: "main", Policy: &policy,
		SourceHost: SourceHostSettings{MaxInstances: 4, MaxRequestCPU: "500m", MaxRequestMemory: "256Mi", SpoolBytes: DefaultSourceHostSpoolBytes,
			ConfirmWaitMax: 15 * time.Minute, ParseBytes: 1 << 20, LogBytesPerHour: 1 << 20, EngineURL: "https://ork:8080"}})
	r.a.SetHosting(SourceHosting{Records: records, Logs: logs, Attempts: attempts, Deployment: "prod",
		Conn: sourcehost.NewHostConn(func() (orkstackv1connect.EventSourceHostServiceClient, func()) { return r.engine, func() {} })})
	r.a.answers = func(int, string) (*servicehost.AnswerDir, error) { return servicehost.OpenAnswerDir(answer) }
	r.a.services.setAnswerFS(true)
	r.a.services.setStorage(memory)
	r.fit()
	return r
}

// serves limits the rig's Orker to namespaces.
func (r *hostRig) serves(names ...string) {
	r.t.Helper()
	ns, err := artifacts.ParseNamespaces(names)
	must.OK(r.t, err)
	r.a.dispatcher.Namespaces = ns
}

// fit is a clean answer of the reach probe.
func (r *hostRig) fit() {
	r.a.services.setProbe(servicehost.ProbeResult{Ran: true, Fitness: servicehost.Fitness{Fit: true}}, time.Now())
}

// serve has the engine serve src's service spec.
func (r *hostRig) serve(src *orkstackv1.EventSource) {
	r.t.Helper()
	_, body, digest, err := eventsources.BuildServiceSpec(src, eventsources.Image{Contracts: []int32{1}, Profiles: []int32{1}}, eventsources.Limits{}, 0)
	must.OK(r.t, err)
	r.engine.spec, r.in.ServiceDigest = &orkstackv1.ServiceSpec{Spec: body, Digest: digest}, digest
}

func (r *hostRig) judge(src *orkstackv1.EventSource) {
	r.t.Helper()
	_, body, digest, err := eventsources.BuildJudgingSpec(src, eventsources.Limits{})
	must.OK(r.t, err)
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	r.engine.judging = &orkstackv1.JudgingSpec{Spec: body, Digest: digest}
}

// hosting is one attempt that runs.
type hosting struct {
	r      *hostRig
	h      *instanceHost
	cancel context.CancelCauseFunc
	done   chan instanceEnding
}

// start runs an attempt on the rig.
func (r *hostRig) start() *hosting {
	ctx, cancel := context.WithCancelCause(context.Background())
	g := &hosting{r: r, cancel: cancel, done: make(chan instanceEnding, 1), h: r.a.newInstanceHost(r.in, r.attempt, r.earlier)}
	g.h.rotateAt = r.rotateAt
	beat := &Heartbeater{send: func(context.Context, HeartbeatProgress) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.deaf {
			return errors.New("no answer")
		}
		d, read := eventsources.ReadHeartbeatDetails(g.h.details())
		if !read {
			r.t.Error("a beat nobody can read")
		}
		r.beats = append(r.beats, d)
		return nil
	}}
	done := g.done
	go func() { done <- g.h.host(ctx, beat) }()
	synctest.Wait()
	return g
}

// ends waits for the attempt's end, and that nothing of it is left.
func (g *hosting) ends() instanceEnding {
	g.r.t.Helper()
	select {
	case e := <-g.done:
		if e.Reason != "SERVICE_STOP_STUCK" {
			leftNothing(g.r.t, g.r.rt.faultServiceRuntime, g.r.own)
		}
		if n := g.r.a.services.hostedNow(); n != 0 {
			g.r.t.Errorf("%d instances hosted", n)
		}
		g.r.rt.once.Do(func() { close(g.r.rt.done) })
		return e
	case <-time.After(90 * time.Minute):
		g.r.t.Fatal("it does not end")
	}
	return instanceEnding{}
}

// stopped cancels the attempt as the cluster does and waits for its end.
func (g *hosting) stopped() instanceEnding {
	g.r.t.Helper()
	g.cancel(context.Canceled)
	return g.ends()
}

func (g *hosting) wantEnd(reason string) instanceEnding {
	g.r.t.Helper()
	e := g.ends()
	if e.Reason != reason {
		g.r.t.Errorf("ended %+v, want %s", e, reason)
	}
	return e
}

func (r *hostRig) lastBeat() eventsources.HeartbeatDetails {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.beats) == 0 {
		return eventsources.HeartbeatDetails{}
	}
	return r.beats[len(r.beats)-1]
}

// stored is the store's objects under prefix.
func (r *hostRig) stored(prefix string) (keys []string, bodies [][]byte) {
	r.t.Helper()
	_ = filepath.WalkDir(r.stores, func(p string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		key, _ := filepath.Rel(r.stores, p)
		if strings.HasPrefix(key, prefix) {
			raw, err := os.ReadFile(p)
			must.OK(r.t, err)
			keys, bodies = append(keys, key), append(bodies, raw)
		}
		return nil
	})
	return keys, bodies
}

func (r *hostRig) record() eventsources.AttemptRecord {
	r.t.Helper()
	keys, bodies := r.stored("metadata/attempts/")
	if len(keys) != 1 {
		r.t.Fatalf("records %v", keys)
	}
	rec, read := eventsources.ReadAttemptRecord(bodies[0])
	if !read {
		r.t.Fatal("a record nobody can read")
	}
	return rec
}

func (e *scriptedDoor) asked() (confirms, offers int, reports []*orkstackv1.ReportInstanceRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.confirms), e.offers, slices.Clone(e.reports)
}

// TestHostEventSource_RunsAnInstance: an attempt is confirmed before anything of it
// exists, starts its container with what the engine gave, converses, reports once
// what it reports, beats what it is, is confirmed every five minutes, and leaves
// its record and its log and nothing else.
func TestHostEventSource_RunsAnInstance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.queue = []string{"one", "two"}
		r.rt.printed = "hello\n"
		began := time.Now()
		g := r.start()
		time.Sleep(12 * time.Second)
		if d := r.lastBeat(); d.State != "active" || d.Phase != "run" || d.Epoch != 12 || d.Judging != 7 || d.ConfirmedAt.IsZero() || d.Broker == nil ||
			!strings.HasPrefix(d.Image, "ghcr.io/acme/source@") || d.LastAcceptedAt.IsZero() || !d.HeldSince.IsZero() {
			t.Errorf("beat %+v", d)
		}
		if n := r.a.services.hostedNow(); n != 1 {
			t.Errorf("%d hosted", n)
		}
		r.engine.mu.Lock()
		first := r.engine.confirms[0]
		if !first.GetWantServiceSpec() || first.GetInstance().GetEpoch() != 12 || first.GetInstance().GetAttempt() != 1 || first.GetInstance().GetIndex() != 1 ||
			first.GetInstance().GetServiceDigest() != r.in.GetServiceDigest() || first.GetInstance().GetJudgingRevision() != 0 || len(r.engine.confirms) != 1 {
			t.Errorf("%d confirmed, first %v", len(r.engine.confirms), first)
		}
		if r.engine.starts != 2 || r.engine.offers != 2 {
			t.Errorf("%d offers, %d starts", r.engine.offers, r.engine.starts)
		}
		r.engine.mu.Unlock()
		if s := r.rt.spec; s.Image != hostedSource().GetImage() || s.Labels["ork.source"] != r.in.GetEventSourceId() {
			t.Errorf("%+v", s)
		}
		time.Sleep(4*time.Minute + 40*time.Second)
		if n, _, _ := r.engine.asked(); n != 1 {
			t.Errorf("%d confirmations before five minutes", n)
		}
		time.Sleep(6 * time.Minute)
		confirms, _, reports := r.engine.asked()
		if confirms != 3 {
			t.Errorf("%d confirmations in eleven minutes", confirms)
		}
		if len(reports) != 2 || reports[0].GetWhat() != orkstackv1.InstanceReport_INSTANCE_REPORT_READY || reports[1].GetWhat() != orkstackv1.InstanceReport_INSTANCE_REPORT_HEALTHY {
			t.Errorf("reported %v", reports)
		}
		e := g.stopped()
		if !e.Cancelled || e.Reason != "" || e.ReadyFor < 10*time.Minute {
			t.Errorf("ended %+v", e)
		}
		if d := r.lastBeat(); d.Phase != "run" && d.Phase != "stop" {
			t.Errorf("last beat %+v", d)
		}
		rec := r.record()
		if rec.ID != r.in.GetEventSourceId() || rec.Index != 1 || rec.Epoch != 12 || rec.Attempt != 1 || rec.OrkerID != "orker-1" || rec.BuildID != "b-1" || rec.Pool != "main" ||
			rec.Service != r.in.GetServiceDigest() || rec.Judging != 7 || rec.Phase != "run" || rec.Reason != "" || !rec.TakenAt.Equal(began) || rec.StartedAt.Before(began) ||
			!rec.ReadyAt.After(began) || !rec.EndedAt.After(rec.ReadyAt) || rec.LogBytes != 6 || rec.ExitCode == nil || *rec.ExitCode != 143 {
			t.Errorf("record %+v", rec)
		}
		if keys, bodies := r.stored("metadata/logs/"); len(keys) != 1 || string(bodies[0]) != "hello\n" {
			t.Errorf("logs %v %q", keys, bodies)
		}
		if keys, _ := r.stored("metadata/receipts/"); len(keys) != 2 {
			t.Errorf("receipts %v", keys)
		}
	})
}

// TestHostEventSource_ItsLastWords: what a container printed is stored when its
// attempt ends, however soon.
func TestHostEventSource_ItsLastWords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.printed = "bye\n"
		r.rt.exit <- ExitStatus{Code: 2}
		r.start().wantEnd("SERVICE_FAILED")
		if keys, bodies := r.stored("metadata/logs/"); len(keys) != 1 || string(bodies[0]) != "bye\n" {
			t.Errorf("logs %v %q", keys, bodies)
		}
		if rec := r.record(); rec.LogBytes != 4 || rec.ExitCode == nil || *rec.ExitCode != 2 || !rec.ReadyAt.IsZero() {
			t.Errorf("%+v", rec)
		}
	})
}

// TestHostEventSource_AnImageThatRunsAsRoot: one that declares root and ends before
// its first status is named for that; one whose user cannot be read is not, and is
// started all the same.
func TestHostEventSource_AnImageThatRunsAsRoot(t *testing.T) {
	for reason, change := range map[string]func(*hostRig){
		"SERVICE_RUNS_AS_ROOT": func(r *hostRig) { r.rt.root = true },
		"SERVICE_FAILED":       func(r *hostRig) { r.rt.fail["root"] = errors.New("daemon") },
	} {
		synctest.Test(t, func(t *testing.T) {
			r := newHostRig(t)
			r.rt.status = ""
			change(r)
			g := r.start()
			time.Sleep(time.Second)
			r.rt.exit <- ExitStatus{Code: 1}
			g.wantEnd(reason)
		})
	}
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.root = true
		g := r.start()
		time.Sleep(3 * time.Second)
		r.rt.exit <- ExitStatus{Code: 1}
		g.wantEnd("SERVICE_FAILED")
	})
}

// TestHostEventSource_HandedBack: a host that is stopping, cordoned or unfit takes
// no instance: nothing is asked, beaten, started or recorded.
func TestHostEventSource_HandedBack(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*hostRig)
		reason string
	}{
		"stopping":       {func(r *hostRig) { r.a.cordon.SetStopping(time.Now(), time.Now().Add(time.Hour)) }, "ORKER_STOPPED"},
		"cordoned":       {func(r *hostRig) { r.a.cordon.Set("work on the rack", time.Now()) }, "ORKER_CORDONED"},
		"a store outage": {func(r *hostRig) { r.a.cordon.SetStoreOutage("store", time.Now()) }, "ORKER_CORDONED"},
		"stuck":          {func(r *hostRig) { r.a.cordon.SetServiceStuck("SERVICE_STOP_STUCK", time.Now()) }, "SERVICE_HOST_UNFIT"},
		"no storage":     {func(r *hostRig) { r.a.services.setStorage("") }, "SERVICE_HOST_UNFIT"},
		"no answers":     {func(r *hostRig) { r.a.services.setAnswerFS(false) }, "SERVICE_HOST_UNFIT"},
		"no clean probe": {func(r *hostRig) {
			for range 2 {
				r.a.services.setProbe(servicehost.ProbeResult{Ran: true, Fitness: servicehost.Fitness{Failures: []string{"question 3"}}}, time.Now())
			}
		}, "SERVICE_HOST_UNFIT"},
		"another namespace":           {func(r *hostRig) { r.serves("billing") }, "NAMESPACE_NOT_SERVED"},
		"another namespace, cordoned": {func(r *hostRig) { r.serves("billing"); r.a.cordon.Set("rack", time.Now()) }, "NAMESPACE_NOT_SERVED"},
		"its own, cordoned":           {func(r *hostRig) { r.serves("billing", "payments"); r.a.cordon.Set("rack", time.Now()) }, "ORKER_CORDONED"},
		"no engine":                   {func(r *hostRig) { r.a.sourceHost.EngineURL = "" }, "SERVICE_HOST_UNFIT"},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newHostRig(t)
				tc.change(r)
				r.start().wantEnd(tc.reason)
				keys, _ := r.stored("metadata/")
				if n, _, _ := r.engine.asked(); n != 0 || len(r.beats) != 0 || len(keys) != 0 || r.rt.spec.Image != "" {
					t.Errorf("%d confirmations, %d beats, stored %v, created %q", n, len(r.beats), keys, r.rt.spec.Image)
				}
			})
		})
	}
}

// TestHostEventSource_NoBeatAnswered: nothing is asked of the engine before the
// cluster has answered one heartbeat, and an attempt whose cluster answers none
// ends.
func TestHostEventSource_NoBeatAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.deaf = true
		g := r.start()
		g.wantEnd("SERVICE_HOST_STALLED")
		if n, _, _ := r.engine.asked(); n != 0 || r.rt.spec.Image != "" {
			t.Errorf("%d confirmations, created %q", n, r.rt.spec.Image)
		}
	})
}

// TestHostEventSource_TheFirstConfirmation: no container exists before one accepts;
// what each answer, and none, ends the attempt with, and when.
func TestHostEventSource_TheFirstConfirmation(t *testing.T) {
	wait := func(reason string) func(int) (*orkstackv1.DoorAnswer, error) {
		return func(int) (*orkstackv1.DoorAnswer, error) {
			return door(orkstackv1.Disposition_DISPOSITION_WAIT, reason), nil
		}
	}
	denied := connect.NewError(connect.CodePermissionDenied, errors.New("SOURCE_HOST_NOT_PERMITTED: eventsource.host at pool main"))
	for name, tc := range map[string]struct {
		confirm  func(int) (*orkstackv1.DoorAnswer, error)
		reason   string
		unread   string
		at, most time.Duration
	}{
		"stopped": {confirm: func(int) (*orkstackv1.DoorAnswer, error) {
			return door(orkstackv1.Disposition_DISPOSITION_STOP, "EVENT_SOURCE_PAUSED"), nil
		}, reason: "EVENT_SOURCE_PAUSED", most: time.Second},
		"not permitted":       {confirm: func(int) (*orkstackv1.DoorAnswer, error) { return nil, denied }, reason: "SOURCE_HOST_NOT_PERMITTED", most: time.Second},
		"nobody answers":      {confirm: func(int) (*orkstackv1.DoorAnswer, error) { return nil, errors.New("dial") }, reason: "ENGINE_UNAVAILABLE", at: 15 * time.Minute, most: 16 * time.Minute},
		"a wait":              {confirm: wait("held=record-missing"), reason: "IDENTITY_UNAVAILABLE", at: 15 * time.Minute, most: 16 * time.Minute},
		"its record not read": {confirm: wait("unread=record"), reason: "IDENTITY_UNAVAILABLE", unread: "record", at: 15 * time.Minute, most: 16 * time.Minute},
		"the policy not read": {confirm: wait("unread=policy"), reason: "IDENTITY_UNAVAILABLE", at: 15 * time.Minute, most: 16 * time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newHostRig(t)
				r.engine.confirm = tc.confirm
				began := time.Now()
				e := r.start().wantEnd(tc.reason)
				if took := time.Since(began); took < tc.at || took > tc.most || e.Unread != tc.unread {
					t.Errorf("after %s: %+v", took, e)
				}
				if rec := r.record(); rec.Phase != "confirm" || rec.Reason != tc.reason || !rec.StartedAt.IsZero() || rec.ExitCode != nil || r.rt.spec.Image != "" {
					t.Errorf("record %+v, created %q", rec, r.rt.spec.Image)
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.engine.confirm = func(n int) (*orkstackv1.DoorAnswer, error) {
			if n < 4 {
				return door(orkstackv1.Disposition_DISPOSITION_WAIT, "unread=spec"), nil
			}
			return nil, nil
		}
		g := r.start()
		time.Sleep(6 * time.Second)
		if d := r.lastBeat(); d.State != "starting" || d.Phase != "confirm" {
			t.Errorf("%+v", d)
		}
		time.Sleep(20 * time.Second)
		if d := r.lastBeat(); d.Phase != "run" || d.Held != "" || !d.HeldSince.IsZero() {
			t.Errorf("accepted at last: %+v", d)
		}
		g.stopped()
	})
}

// TestHostEventSource_WhatAnEarlierAttemptWaitedOn: the activity's wait for a
// judgment is counted over its attempts, and only its own.
func TestHostEventSource_WhatAnEarlierAttemptWaitedOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.engine.confirm = func(int) (*orkstackv1.DoorAnswer, error) {
			return door(orkstackv1.Disposition_DISPOSITION_WAIT, "unread=spec"), nil
		}
		since := time.Now().Add(-time.Hour)
		r.earlier = eventsources.HeartbeatDetails{Epoch: 12, Held: "unread=spec", HeldSince: since}
		g := r.start()
		time.Sleep(15 * time.Second)
		if d := r.lastBeat(); d.Held != "unread=spec" || !d.HeldSince.Equal(since) {
			t.Errorf("%+v", d)
		}
		g.stopped()
		if rec := r.record(); rec.Held != "unread=spec" || !rec.HeldSince.Equal(since) {
			t.Errorf("%+v", rec)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.engine.confirm = func(int) (*orkstackv1.DoorAnswer, error) { return nil, errors.New("dial") }
		since := time.Now().Add(-time.Hour)
		r.earlier = eventsources.HeartbeatDetails{Epoch: 12, HeldSince: since}
		g := r.start()
		time.Sleep(15 * time.Second)
		if d := r.lastBeat(); !d.HeldSince.Equal(since) {
			t.Errorf("not judged yet: %+v", d)
		}
		g.stopped()
		r.earlier.Epoch = 11
		if h := r.a.newInstanceHost(r.in, 2, r.earlier); !h.judgment.HeldSince.IsZero() {
			t.Error("another activity's wait is this one's")
		}
	})
}

// TestHostEventSource_AStartThatFails: each phase's failure is its own, leaves
// nothing, and is in the record with its phase.
func TestHostEventSource_AStartThatFails(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*hostRig)
		reason string
		phase  string
		cause  string
	}{
		"a policy that forbids its image": {func(r *hostRig) { r.engine.policy.AllowedRegistries = []string{"registry.example/"} }, "IMAGE_NOT_PERMITTED", "policy", ""},
		"more than this pool's instances": {func(r *hostRig) { r.a.sourceHost.MaxRequestCPU = "100m" }, "RESOURCES_UNSATISFIABLE", "policy", ""},
		"other resources than reserved":   {func(r *hostRig) { r.in.Resources.Memory = "128Mi" }, "ENGINE_UNAVAILABLE", "policy", "RESOURCE_ADMISSION_MISMATCH"},
		"other cpu than reserved":         {func(r *hostRig) { r.in.Resources.Cpu = "100m" }, "ENGINE_UNAVAILABLE", "policy", "RESOURCE_ADMISSION_MISMATCH"},
		"an image that cannot be pulled":  {func(r *hostRig) { r.rt.fail["pull"] = errors.New("boom") }, "IMAGE_PULL_FAILED", "image", ""},
		"labels nobody can read":          {func(r *hostRig) { r.rt.fail["label"] = errors.New("boom") }, "SERVICE_HOST_STALLED", "image", ""},
		"no contract":                     {func(r *hostRig) { delete(r.rt.labels, eventsources.ContractLabel) }, "SERVICE_CONTRACT_UNSUPPORTED", "image", ""},
		"a contract of another release":   {func(r *hostRig) { r.rt.labels[eventsources.ContractLabel] = "9" }, "SERVICE_CONTRACT_UNSUPPORTED", "image", ""},
		"another profile":                 {func(r *hostRig) { r.rt.labels[eventsources.ProfileLabel] = "secret-provider/1" }, "SERVICE_PROFILE_UNSUPPORTED", "image", ""},
		"an index that is not":            {func(r *hostRig) { r.in.Index = 5 }, "SERVICE_HOST_STALLED", "workspace", ""},
		"a container that does not start": {func(r *hostRig) { r.rt.fail["start"] = errors.New("boom") }, "SERVICE_FAILED", "workspace", ""},
		"a container that is not removed": {func(r *hostRig) { r.rt.fail["start"], r.rt.fail["remove"] = errors.New("boom"), errors.New("busy") }, "SERVICE_STOP_STUCK", "workspace", ""},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newHostRig(t)
				tc.change(r)
				r.start().wantEnd(tc.reason)
				if rec := r.record(); rec.Phase != tc.phase || rec.Reason != tc.reason || rec.Cause != tc.cause || !rec.StartedAt.IsZero() {
					t.Errorf("record %+v", rec)
				}
				if r.rt.fail["remove"] != nil {
					delete(r.rt.fail, "remove")
					r.a.retryRemovals(t.Context())
				}
			})
		})
	}
}

// TestHostEventSource_APullLongerThanAConfirmationHolds: what was confirmed before
// it is confirmed again after it, whatever it took, and is what the container is
// started with; the five minutes are counted from the last confirmation.
func TestHostEventSource_APullLongerThanAConfirmationHolds(t *testing.T) {
	for took, confirms := range map[time.Duration]int{20 * time.Minute: 2, 4 * time.Minute: 1} {
		synctest.Test(t, func(t *testing.T) {
			r := newHostRig(t)
			r.rt.pullTakes = took
			g := r.start()
			time.Sleep(took + 10*time.Second)
			if n, _, _ := r.engine.asked(); n != confirms {
				t.Errorf("%d confirmations after a pull of %s", n, took)
			}
			if d := r.lastBeat(); d.Phase != "run" || d.State != "active" {
				t.Errorf("%+v", d)
			}
			if took < 5*time.Minute {
				time.Sleep(time.Minute)
				if n, _, _ := r.engine.asked(); n != 2 {
					t.Errorf("%d confirmations five minutes after the first", n)
				}
			}
			g.stopped()
		})
	}
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.pullTakes = 6 * time.Minute
		r.engine.confirm = func(n int) (*orkstackv1.DoorAnswer, error) {
			if n == 1 {
				return nil, nil
			}
			return door(orkstackv1.Disposition_DISPOSITION_STOP, "EVENT_SOURCE_PAUSED"), nil
		}
		r.start().wantEnd("EVENT_SOURCE_PAUSED")
		if r.rt.spec.Image != "" {
			t.Error("a container was started on a confirmation that held no more")
		}
	})
	// However long it took, the wait that follows is a first one's.
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		const took = 3 * time.Hour
		r.rt.pullTakes = took
		r.engine.confirm = func(n int) (*orkstackv1.DoorAnswer, error) {
			if n == 2 {
				return nil, errors.New("dial")
			}
			return nil, nil
		}
		g := r.start()
		time.Sleep(took + 10*time.Second)
		if d := r.lastBeat(); d.Phase != "run" || d.State != "active" {
			t.Errorf("after one that got no answer: %+v", d)
		}
		g.stopped()
	})
}

// TestHostEventSource_WhatItIsStoppedWith: what ends an instance as it is stopped is
// said only where trying again cannot change it; otherwise it ended for what stopped
// it.
func TestHostEventSource_WhatItIsStoppedWith(t *testing.T) {
	for name, tc := range map[string]struct {
		during func(*hostRig)
		reason string
	}{
		"a broken contract": {func(r *hostRig) { r.rt.misanswers = true }, "SERVICE_CONTRACT_VIOLATION"},
		"an exit":           {func(r *hostRig) { r.rt.exit <- ExitStatus{Code: 3} }, "ORKER_CORDONED"},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newHostRig(t)
				r.engine.offer = func(int) (*orkstackv1.DoorAnswer, error) {
					time.Sleep(3 * time.Second)
					return door(orkstackv1.Disposition_DISPOSITION_REJECT, "MESSAGE_FIELD_MISSING"), nil
				}
				g := r.start()
				time.Sleep(3 * time.Second)
				r.rt.hand("m")
				time.Sleep(time.Second)
				r.rt.mu2.Lock()
				tc.during(r)
				r.rt.mu2.Unlock()
				r.a.evictInstances("ORKER_CORDONED", "cordoned")
				g.wantEnd(tc.reason)
			})
		})
	}
}

// TestHostEventSource_EndedAsItStarts: what ends the attempt while it starts is what
// it ends with, whatever its start then failed with, unless a container is left.
func TestHostEventSource_EndedAsItStarts(t *testing.T) {
	for reason, left := range map[string]bool{"ORKER_CORDONED": false, "SERVICE_STOP_STUCK": true} {
		synctest.Test(t, func(t *testing.T) {
			r := newHostRig(t)
			r.rt.fail["start"] = errors.New("killed")
			if left {
				r.rt.fail["remove"] = errors.New("busy")
			}
			r.rt.starting = func() { r.a.evictInstances("ORKER_CORDONED", "cordoned") }
			r.start().wantEnd(reason)
			delete(r.rt.fail, "remove")
			r.a.retryRemovals(t.Context())
		})
	}
}

// TestHostEventSource_WhatEndsARunningInstance: what each ends it with, its
// container stopped and gone.
func TestHostEventSource_WhatEndsARunningInstance(t *testing.T) {
	answer := func(d orkstackv1.Disposition, reason string, after int) func(int) (*orkstackv1.DoorAnswer, error) {
		return func(n int) (*orkstackv1.DoorAnswer, error) {
			if n <= after {
				return nil, nil
			}
			return door(d, reason), nil
		}
	}
	for name, tc := range map[string]struct {
		before func(*hostRig)
		during func(*hostRig, *hosting)
		reason string
		unread string
		within time.Duration
	}{
		"evicted by a cordon":   {during: func(r *hostRig, _ *hosting) { r.a.evictInstances("ORKER_CORDONED", "cordoned") }, reason: "ORKER_CORDONED", within: 25 * time.Second},
		"the worker's stop":     {during: func(_ *hostRig, g *hosting) { g.cancel(worker.ErrWorkerShutdown) }, reason: "ORKER_STOPPED", within: 25 * time.Second},
		"the cluster's silence": {during: func(r *hostRig, _ *hosting) { r.mu.Lock(); r.deaf = true; r.mu.Unlock() }, reason: "SERVICE_HOST_STALLED", within: time.Minute},
		"a confirmation's stop": {before: func(r *hostRig) {
			r.engine.confirm = answer(orkstackv1.Disposition_DISPOSITION_STOP, "EVENT_SOURCE_DELETING", 1)
		}, reason: "EVENT_SOURCE_DELETING", within: 6 * time.Minute},
		"an offer's stop": {before: func(r *hostRig) {
			r.engine.offer = answer(orkstackv1.Disposition_DISPOSITION_STOP, "EVENT_SOURCE_PAUSED", 0)
		}, during: func(r *hostRig, _ *hosting) { r.rt.hand("m") }, reason: "EVENT_SOURCE_PAUSED", within: 25 * time.Second},
		"a report's stop": {before: func(r *hostRig) {
			r.engine.report = func(int) (*orkstackv1.DoorAnswer, error) {
				return door(orkstackv1.Disposition_DISPOSITION_STOP, ""), nil
			}
			r.engine.confirm = answer(orkstackv1.Disposition_DISPOSITION_STOP, "EVENT_SOURCE_NOT_HELD", 1)
		}, reason: "EVENT_SOURCE_NOT_HELD", within: 25 * time.Second},
		"an offer not permitted": {before: func(r *hostRig) {
			r.engine.offer = func(int) (*orkstackv1.DoorAnswer, error) {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("SOURCE_HOST_NOT_PERMITTED: eventsource.accept"))
			}
		}, during: func(r *hostRig, _ *hosting) { r.rt.hand("m") }, reason: "SOURCE_HOST_NOT_PERMITTED", within: 25 * time.Second},
		"an engine that answers nothing": {before: func(r *hostRig) {
			r.engine.confirm = func(n int) (*orkstackv1.DoorAnswer, error) {
				if n == 1 {
					return nil, nil
				}
				return nil, errors.New("dial")
			}
		}, reason: "ENGINE_UNAVAILABLE", within: 16 * time.Minute},
		"a judgment nobody can make": {before: func(r *hostRig) {
			r.engine.confirm = answer(orkstackv1.Disposition_DISPOSITION_WAIT, "unread=spec", 1)
		}, reason: "IDENTITY_UNAVAILABLE", unread: "spec", within: 61 * time.Minute},
		"an image no longer permitted": {during: func(r *hostRig, _ *hosting) {
			r.engine.mu.Lock()
			r.engine.policy = &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"registry.example/"}}
			r.engine.mu.Unlock()
		}, reason: "IMAGE_NOT_PERMITTED", within: 6 * time.Minute},
		"its container's end":    {during: func(r *hostRig, _ *hosting) { r.rt.exit <- ExitStatus{Code: 3} }, reason: "SERVICE_FAILED", within: 5 * time.Second},
		"its container's memory": {during: func(r *hostRig, _ *hosting) { r.rt.exit <- ExitStatus{Code: 137, OOMKilled: true} }, reason: "SERVICE_OOM_KILLED", within: 5 * time.Second},
		"a fatal status": {during: func(r *hostRig, _ *hosting) {
			r.rt.say(`{"schemaVersion":1,"ready":false,"fatal":true,"check":{"ok":false,"reason":"CONFIG_INVALID","field":"stream"}}`)
		}, reason: "SERVICE_CONFIG_REJECTED", within: 25 * time.Second},
		"a broken contract":      {during: func(r *hostRig, _ *hosting) { r.rt.say(`{"schemaVersion":2}`) }, reason: "SERVICE_CONTRACT_VIOLATION", within: 25 * time.Second},
		"a status not refreshed": {during: func(r *hostRig, _ *hosting) { r.rt.say("") }, reason: "SERVICE_UNRESPONSIVE", within: 4 * time.Minute},
		"one that does not stop": {during: func(r *hostRig, _ *hosting) {
			r.rt.mu.Lock()
			r.rt.termExits = false
			r.rt.fail["remove"] = errors.New("busy")
			r.rt.mu.Unlock()
			r.a.evictInstances("ORKER_CORDONED", "cordoned")
		}, reason: "SERVICE_STOP_STUCK", within: 2 * time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newHostRig(t)
				if tc.before != nil {
					tc.before(r)
				}
				g := r.start()
				time.Sleep(3 * time.Second)
				began := time.Now()
				if tc.during != nil {
					tc.during(r, g)
				}
				e := g.wantEnd(tc.reason)
				if took := time.Since(began); took > tc.within || e.Unread != tc.unread {
					t.Errorf("after %s: %+v", took, e)
				}
				if _, held := eventsources.LookupInstanceEnd(e.Reason); !held {
					t.Errorf("%s is no reason of the table", e.Reason)
				}
				if rec := r.record(); rec.Phase != "run" || rec.Reason != tc.reason || rec.StartedAt.IsZero() {
					t.Errorf("record %+v", rec)
				}
				if r.rt.fail["remove"] != nil {
					delete(r.rt.fail, "remove")
					r.a.retryRemovals(t.Context())
					leftNothing(t, r.rt.faultServiceRuntime, r.own)
				}
			})
		})
	}
}

func (r *sourceRuntime) hand(body string) {
	r.mu2.Lock()
	defer r.mu2.Unlock()
	r.queue = append(r.queue, body)
}

// TestHostEventSource_AReplacementOnTrial: one that says what it is and never
// becomes ready ends within its time; one that becomes ready is on trial no more,
// and one that says nothing ends as any that says nothing.
func TestHostEventSource_AReplacementOnTrial(t *testing.T) {
	const notReady = `{"schemaVersion":1,"ready":false,"check":{"ok":true},"broker":{"maxDeliveries":5,"messageLimitBytes":1}}`
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.in.ReadyWithinSeconds = 120
		r.rt.status = notReady
		began := time.Now()
		r.start().wantEnd("SERVICE_NOT_READY_IN_TIME")
		if took := time.Since(began); took < 2*time.Minute || took > 2*time.Minute+30*time.Second {
			t.Errorf("after %s", took)
		}
		if _, _, reports := r.engine.asked(); len(reports) != 0 {
			t.Errorf("reported %v", reports)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.in.ReadyWithinSeconds = 120
		g := r.start()
		time.Sleep(time.Minute)
		r.rt.say(notReady)
		time.Sleep(2 * time.Minute)
		if e := g.stopped(); !e.Cancelled {
			t.Errorf("%+v", e)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.in.ReadyWithinSeconds = 30
		r.rt.status = ""
		r.start().wantEnd("SERVICE_NOT_READY")
	})
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.status = notReady
		g := r.start()
		time.Sleep(10 * time.Minute)
		if e := g.stopped(); !e.Cancelled || e.ReadyFor != 0 {
			t.Errorf("one that is no replacement: %+v", e)
		}
	})
}

// TestHostEventSource_Invalid: what its first check says is wrong is reported once,
// with the check's reason, and it stays up.
func TestHostEventSource_Invalid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.status = `{"schemaVersion":1,"ready":false,"check":{"ok":false,"reason":"OBJECT_MISSING"}}`
		failed := 0
		r.engine.report = func(n int) (*orkstackv1.DoorAnswer, error) {
			if n < 3 {
				failed++
				return nil, errors.New("dial")
			}
			return ok, nil
		}
		g := r.start()
		time.Sleep(time.Minute)
		_, _, reports := r.engine.asked()
		if len(reports) != 3 || failed != 2 || reports[2].GetWhat() != orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID || reports[2].GetReason() != "OBJECT_MISSING" {
			t.Errorf("reported %v", reports)
		}
		if d := r.lastBeat(); d.State != "invalid" || d.Reason != "OBJECT_MISSING" {
			t.Errorf("%+v", d)
		}
		r.rt.say(sourceReady)
		time.Sleep(time.Minute)
		r.rt.say(`{"schemaVersion":1,"ready":false,"check":{"ok":false,"reason":"OBJECT_MISSING"}}`)
		time.Sleep(time.Minute)
		if _, _, reports := r.engine.asked(); len(reports) != 4 || reports[3].GetWhat() != orkstackv1.InstanceReport_INSTANCE_REPORT_READY {
			t.Errorf("reported %v", reports)
		}
		g.stopped()
	})
}

// TestHostEventSource_HeldBack: while the store is out, or the engine has not
// judged the instance for ten minutes, it writes no poll, its container stays up,
// and it goes on when that ends.
func TestHostEventSource_HeldBack(t *testing.T) {
	polls := func(r *hostRig) int {
		time.Sleep(25 * time.Second)
		r.rt.mu2.Lock()
		before := r.rt.polled
		r.rt.mu2.Unlock()
		time.Sleep(time.Minute)
		r.rt.mu2.Lock()
		defer r.rt.mu2.Unlock()
		return r.rt.polled - before
	}
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		g := r.start()
		if polls(r) == 0 {
			t.Error("no poll")
		}
		r.a.cordon.SetStoreOutage("store", time.Now())
		if n := polls(r); n != 0 {
			t.Errorf("%d polls in a store outage", n)
		}
		if d := r.lastBeat(); d.State != "waiting" || d.Reason != "outage" {
			t.Errorf("%+v", d)
		}
		r.a.cordon.ClearStoreOutage()
		if polls(r) == 0 {
			t.Error("no poll after it")
		}
		g.stopped()
	})
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.engine.confirm = func(n int) (*orkstackv1.DoorAnswer, error) {
			if n == 1 || time.Since(r.engine.at[0]) > 20*time.Minute {
				return nil, nil
			}
			return door(orkstackv1.Disposition_DISPOSITION_WAIT, "unread=access"), nil
		}
		g := r.start()
		time.Sleep(8 * time.Minute)
		if polls(r) == 0 {
			t.Error("no poll before ten minutes")
		}
		time.Sleep(time.Minute)
		if n := polls(r); n != 0 {
			t.Errorf("%d polls unjudged", n)
		}
		if d := r.lastBeat(); d.State != "waiting" || d.Reason != "outage" || d.Held != "unread=access" || d.HeldSince.IsZero() {
			t.Errorf("%+v", d)
		}
		time.Sleep(10 * time.Minute)
		if polls(r) == 0 {
			t.Error("no poll once judged again")
		}
		g.stopped()
	})
}

// TestHostEventSource_JudgedByTheSpecAsItIs: a confirmation's newer judging spec is
// taken without a restart, and an older one, a lagging replica's, is not.
func TestHostEventSource_JudgedByTheSpecAsItIs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		g := r.start()
		time.Sleep(time.Minute)
		src := hostedSource()
		src.JudgingRevision = 9
		r.judge(src)
		time.Sleep(5 * time.Minute)
		if d := r.lastBeat(); d.Judging != 9 {
			t.Errorf("%+v", d)
		}
		src = proto.CloneOf(src)
		src.JudgingRevision = 8
		r.judge(src)
		time.Sleep(5 * time.Minute)
		if d := r.lastBeat(); d.Judging != 9 {
			t.Errorf("%+v", d)
		}
		g.stopped()
	})
}

// TestHostEventSource_Rotates: at its activity's bound it stops and completes.
func TestHostEventSource_Rotates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rotateAt = time.Now().Add(time.Minute)
		began := time.Now()
		if e := r.start().ends(); !e.Rotated || e.Reason != "" || time.Since(began) < time.Minute || time.Since(began) > 90*time.Second {
			t.Errorf("after %s: %+v", time.Since(began), e)
		}
	})
	if e := (&instanceHost{}).endedBy(ended(t, context.DeadlineExceeded)); !e.Rotated {
		t.Errorf("%+v", e)
	}
}

func ended(t *testing.T, cause error) context.Context {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	return ctx
}

// TestHostEventSource_BeatsWhileItStops: a stop is heartbeated for as long as it
// takes, as stopping, after the attempt's own context ended.
func TestHostEventSource_BeatsWhileItStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		r.rt.termExits = false
		g := r.start()
		time.Sleep(5 * time.Second)
		g.cancel(context.Canceled)
		time.Sleep(15 * time.Second)
		if d := r.lastBeat(); d.Phase != "stop" || d.State != "stopping" {
			t.Errorf("%+v", d)
		}
		if e := g.ends(); !e.Cancelled {
			t.Errorf("%+v", e)
		}
	})
}

// TestHostEventSource_ItsOwnLoopThatHangs: an instance whose loop hangs, in a call
// that does not end when its context does, is ended as its host's failure though it
// once looked at its lanes, and its container is stopped without it. What hangs keeps
// the conversation's handles, and nothing else of the attempt's.
func TestHostEventSource_ItsOwnLoopThatHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newHostRig(t)
		hangs, over := make(chan struct{}), make(chan struct{})
		r.engine.probe = func() {
			close(hangs)
			<-over
		}
		r.engine.offer = func(int) (*orkstackv1.DoorAnswer, error) {
			return &orkstackv1.DoorAnswer{Disposition: orkstackv1.Disposition_DISPOSITION_WAIT, Cause: "pool"}, nil
		}
		r.rt.queue = []string{"m"}
		g := r.start()
		<-hangs
		began := time.Now()
		g.wantEnd("SERVICE_HOST_STALLED")
		if took := time.Since(began); took < loopStalledAfter || took > loopStalledAfter+instanceEnds+5*time.Second {
			t.Errorf("after %s", took)
		}
		instance := g.h.running()
		select {
		case <-instance.Finished():
			t.Error("finished while its loop hangs")
		default:
		}
		close(over)
		<-instance.Finished()
	})
}

// TestHostEventSource_AtMostOnce: an at-most-once source's rejected message is
// dropped, and as it stops it is asked for what it buffered.
func TestHostEventSource_AtMostOnce(t *testing.T) {
	for delivery, want := range map[orkstackv1.DeliveryClass]struct {
		outcome string
		offers  int
	}{
		orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE:  {"-dropped-", 2},
		orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE: {"-rejected-", 1},
	} {
		synctest.Test(t, func(t *testing.T) {
			r := newHostRig(t)
			src := hostedSource()
			src.Delivery = delivery
			r.serve(src)
			r.engine.offer = func(n int) (*orkstackv1.DoorAnswer, error) {
				if n == 1 {
					time.Sleep(2 * time.Second)
				}
				return door(orkstackv1.Disposition_DISPOSITION_REJECT, "MESSAGE_FIELD_MISSING"), nil
			}
			r.rt.queue = []string{"one", "buffered"}
			g := r.start()
			time.Sleep(time.Second)
			g.stopped()
			if _, offers, _ := r.engine.asked(); offers != want.offers {
				t.Errorf("%v: %d offers", delivery, offers)
			}
			if keys, _ := r.stored("metadata/receipts/"); len(keys) != want.offers || !strings.Contains(keys[0], want.outcome) {
				t.Errorf("%v: receipts %v", delivery, keys)
			}
		})
	}
}

// TestHostEventSource_ATrial: the breaker an activity is on trial for is its first
// attempt's alone.
func TestHostEventSource_ATrial(t *testing.T) {
	for attempt, want := range map[int32]string{1: sourcehost.BreakerAllRejected, 2: ""} {
		synctest.Test(t, func(t *testing.T) {
			r := newHostRig(t)
			r.attempt, r.in.Trial = attempt, sourcehost.BreakerAllRejected
			g := r.start()
			time.Sleep(3 * time.Second)
			if got := g.h.running().Config.Trial; got != want {
				t.Errorf("attempt %d is on trial for %q", attempt, got)
			}
			g.stopped()
		})
	}
}

// TestHostEventSource_ALanesFailureIsSaidOnceAMinute: one of each kind is.
func TestHostEventSource_ALanesFailureIsSaidOnceAMinute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHostRig(t).a.newInstanceHost(&orkstackv1.HostEventSourceInput{}, 1, eventsources.HeartbeatDetails{})
		if !h.due("store") || h.due("store") || !h.due("engine") {
			t.Error("not one of each kind")
		}
		time.Sleep(time.Minute - time.Second)
		if h.due("store") {
			t.Error("again within the minute")
		}
		time.Sleep(time.Second)
		if !h.due("store") {
			t.Error("not again after it")
		}
	})
}
