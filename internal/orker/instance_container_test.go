package orker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// faultServiceRuntime is a runtime that fails one step as a real engine does not
// on demand, and keeps what is left of what it made.
type faultServiceRuntime struct {
	phaseCapturingRuntime
	mu       sync.Mutex
	fail     map[string]error
	networks map[string]bool
	live     map[ContainerID]bool
	spec     ContainerSpec
	signals  []Signal
	removes  int
	// pids counts how often its process was asked for; reused, it is another
	// the second time.
	pids   int
	reused bool
	// root, its images declare the root user.
	root bool
	exit chan ExitStatus
	// termExits, the container exits when it is sent SIGTERM.
	termExits bool
	printed   string
}

func newFaultServiceRuntime() *faultServiceRuntime {
	return &faultServiceRuntime{fail: map[string]error{}, networks: map[string]bool{}, live: map[ContainerID]bool{}, exit: make(chan ExitStatus, 1), termExits: true}
}

func (r *faultServiceRuntime) failed(step string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail[step]
}

func (r *faultServiceRuntime) CreateServiceNetwork(_ context.Context, name string, _ map[string]string) (string, error) {
	if err := r.failed("network"); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.networks["net-"+name] = true
	return "net-" + name, nil
}

func (r *faultServiceRuntime) RemoveServiceNetwork(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.networks, id)
	return nil
}

func (r *faultServiceRuntime) ContainerPid(context.Context, ContainerID) (int, error) {
	if err := r.failed("pid"); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pids++
	if r.pids > 1 {
		if err := r.fail["pid again"]; err != nil {
			return 0, err
		}
		if r.reused {
			return 4243, nil
		}
	}
	return 4242, nil
}

func (r *faultServiceRuntime) ImageRunsAsRoot(context.Context, string) (bool, error) {
	return r.root, r.failed("root")
}

func (r *faultServiceRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spec = spec
	if err := r.fail["create"]; err != nil {
		return "c1", err
	}
	r.live["c1"] = true
	return "c1", nil
}

func (r *faultServiceRuntime) Start(_ context.Context, _ ContainerID, stdout, _ io.Writer) error {
	if err := r.failed("start"); err != nil {
		return err
	}
	_, err := io.WriteString(stdout, r.printed)
	return err
}

func (r *faultServiceRuntime) Signal(_ context.Context, _ ContainerID, sig Signal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.signals = append(r.signals, sig)
	if sig == SIGKILL || r.termExits {
		select {
		case r.exit <- ExitStatus{Code: 143}:
		default:
		}
	}
	return r.fail["signal"]
}

func (r *faultServiceRuntime) Wait(ctx context.Context, _ ContainerID) (ExitStatus, error) {
	if err := r.failed("wait"); err != nil {
		return ExitStatus{}, err
	}
	select {
	case s := <-r.exit:
		// One that has ended is answered so each time it is asked.
		r.exit <- s
		return s, nil
	case <-ctx.Done():
		return ExitStatus{}, ctx.Err()
	}
}

func (r *faultServiceRuntime) Remove(_ context.Context, id ContainerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.fail["remove"]; err != nil {
		return err
	}
	r.removes++
	delete(r.live, id)
	return nil
}

// left is what is still there.
func (r *faultServiceRuntime) left() (networks, containers int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.networks), len(r.live)
}

// serviceRig is an Orker that can start one service over a fault runtime.
func serviceRig(t *testing.T) (*Activities, *faultServiceRuntime, serviceStart, string) {
	t.Helper()
	t.Setenv("PAY_KEY", "s3cret-value")
	a, spec, own := workspaceRig(t)
	rt := newFaultServiceRuntime()
	a.dispatcher.Runtime = rt
	a.cordon = NewCordon()
	spec.Secrets = []eventsources.SpecSecret{{Name: "token", Source: "env:PAY_KEY"}}
	spec.Env = map[string]string{"MODE": "fast"}
	spec.Limits.StopSeconds = 20
	store, err := artifacts.NewLocalLogStore(t.TempDir())
	must.OK(t, err)
	log := newInstanceLog(t.Context(), store, artifacts.InstanceAttempt{SourceID: spec.Source.ID, Index: 1, Epoch: 7, Attempt: 2}, LogOptions{}, 1<<20, nil)
	t.Cleanup(log.Close)
	answer := t.TempDir()
	return a, rt, serviceStart{name: "one", spec: spec, context: contextOf(t, spec), image: spec.Image, log: log,
		openAnswer: func(int, string) (*servicehost.AnswerDir, error) {
			if err := rt.failed("answer"); err != nil {
				return nil, err
			}
			return servicehost.OpenAnswerDir(answer)
		}}, own
}

func leftNothing(t *testing.T, rt *faultServiceRuntime, own string) {
	t.Helper()
	networks, containers := rt.left()
	entries, _ := os.ReadDir(own)
	for _, e := range entries {
		if e.Name() != identityDirMarker {
			t.Errorf("left %s", e.Name())
		}
	}
	if networks != 0 || containers != 0 {
		t.Errorf("left %d networks and %d containers", networks, containers)
	}
}

// TestServiceContainer_IsSandboxed: a service's container has a task's sandbox and
// what the contract adds: its own network, its answers in a bounded memory
// filesystem, no swap, no core dump, the Orker's own user, and its files only to
// read.
func TestServiceContainer_IsSandboxed(t *testing.T) {
	a, rt, start, own := serviceRig(t)
	c, f := a.startService(t.Context(), start)
	if f != nil {
		t.Fatal(f)
	}
	s := rt.spec
	if !s.ReadOnlyRootfs || !s.DropAllCapabilities || !s.NoNewPrivileges || !s.Init || s.SeccompProfile != SeccompBuiltin || !s.NoSwap || !s.NoCoreDumps || !s.RunAsOrker {
		t.Errorf("%+v", s)
	}
	if s.Network != NetworkService || s.ServiceNetworkID != "net-ork-svc-one" || len(s.Devices) != 0 || len(s.CDIDevices) != 0 || len(s.Command) != 0 {
		t.Errorf("%+v", s)
	}
	if s.CPUs != "500m" || s.Memory != "256Mi" || s.PidsLimit != a.dispatcher.Policy.PidsLimit || s.StopTimeoutSeconds != 20 || s.Env["MODE"] != "fast" || len(s.Env) != 1 {
		t.Errorf("%+v", s)
	}
	if tmp := s.ServiceTmpfs; tmp == nil || tmp.Path != servicehost.PathAnswer || tmp.Bytes != servicehost.AnswerBytes(4, 1<<20) || tmp.Inodes != 128 || tmp.UID != os.Getuid() {
		t.Errorf("%+v", s.ServiceTmpfs)
	}
	if s.Labels["ork.orker"] != "orker-1" || s.Labels["ork.source"] != start.spec.Source.ID || s.Image != start.image || !strings.Contains(s.Image, "@sha256:") {
		t.Errorf("%v %s", s.Labels, s.Image)
	}
	for _, m := range s.Mounts {
		if !m.ReadOnly || !strings.HasPrefix(m.Source, own+"/one/") {
			t.Errorf("%+v", m)
		}
	}
	if _, err := os.Stat(filepath.Join(own, "one", "ask", "alive.json")); err != nil {
		t.Errorf("no beat when it starts: %v", err)
	}
	if f := c.stop(t.Context(), 20); f != nil {
		t.Fatal(f)
	}
	if len(rt.signals) != 1 || rt.signals[0] != SIGTERM {
		t.Errorf("signals %v", rt.signals)
	}
	leftNothing(t, rt, own)
}

// TestServiceContainer_AStartThatFails: each step's failure is whose it is, and
// leaves no container, network, file or reservation.
func TestServiceContainer_AStartThatFails(t *testing.T) {
	boom := errors.New("boom")
	for name, tc := range map[string]struct {
		fail   map[string]error
		change func(*Activities, *serviceStart)
		reason string
	}{
		"its network":                     {fail: map[string]error{"network": boom}, reason: "SERVICE_HOST_STALLED"},
		"its create":                      {fail: map[string]error{"create": boom}, reason: "SERVICE_HOST_STALLED"},
		"its start":                       {fail: map[string]error{"start": boom}, reason: "SERVICE_FAILED"},
		"whether it was created":          {fail: map[string]error{"create": ErrCreateUnanswered}, reason: "SERVICE_HOST_STALLED"},
		"whether it started":              {fail: map[string]error{"start": ErrRuntimeStateUnknown}, reason: "SERVICE_HOST_STALLED"},
		"its process":                     {fail: map[string]error{"pid": boom}, reason: "SERVICE_HOST_STALLED"},
		"its process, gone since":         {fail: map[string]error{"pid again": boom}, reason: "SERVICE_HOST_STALLED"},
		"another's process":               {change: func(a *Activities, _ *serviceStart) { a.dispatcher.Runtime.(*faultServiceRuntime).reused = true }, reason: "SERVICE_HOST_STALLED"},
		"its answers":                     {fail: map[string]error{"answer": boom}, reason: "SERVICE_HOST_STALLED"},
		"its files":                       {change: func(a *Activities, _ *serviceStart) { a.services.setStorage("") }, reason: "SERVICE_HOST_UNFIT"},
		"a runtime that hosts no service": {change: func(a *Activities, _ *serviceStart) { a.dispatcher.Runtime = &phaseCapturingRuntime{} }, reason: "SERVICE_HOST_UNFIT"},
		"secrets no mask holds": {change: func(_ *Activities, s *serviceStart) {
			s.spec.Secrets = append(s.spec.Secrets, eventsources.SpecSecret{Name: "big", Source: "env:PAY_BIG"})
		}, reason: "MASK_CAPACITY_EXCEEDED"},
	} {
		t.Run(name, func(t *testing.T) {
			a, rt, start, own := serviceRig(t)
			var big strings.Builder
			for i := range 2000 {
				fmt.Fprintf(&big, "secret-line-%06d-%s\n", i, strings.Repeat("x", 40))
			}
			t.Setenv("PAY_BIG", big.String())
			rt.fail = tc.fail
			if rt.fail == nil {
				rt.fail = map[string]error{}
			}
			if tc.change != nil {
				tc.change(a, &start)
			}
			a.capacity = newSourceCapacity(1)
			a.capacity.markUsed(mustPoll(t, a.capacity, a.capacity.source))
			admission, err := a.capacity.admit(a.capacity.source, amount{milli: 500, bytes: 256 << 20})
			must.OK(t, err)
			c, f := a.startService(withAdmission(t.Context(), admission), start)
			admission.Done()
			if c != nil || f == nil || f.Reason != tc.reason {
				t.Fatalf("%v, %v; want %s", c, f, tc.reason)
			}
			if _, ok := eventsources.LookupInstanceEnd(f.Reason); !ok || strings.Contains(f.Error(), "s3cret-value") {
				t.Errorf("%v", f)
			}
			leftNothing(t, rt, own)
			wantReserved(t, a.capacity, name, amount{})
		})
	}
}

// TestServiceContainer_OneThatEndsAsItStarts: a container that is gone before it
// was found is asked how it ended.
func TestServiceContainer_OneThatEndsAsItStarts(t *testing.T) {
	a, rt, start, own := serviceRig(t)
	rt.fail["pid"] = errors.New("no running process")
	rt.exit <- ExitStatus{Code: 3}
	if _, f := a.startService(t.Context(), start); f == nil || f.Reason != "SERVICE_FAILED" || !strings.Contains(f.Detail, "exited 3") {
		t.Fatalf("%v", f)
	}
	leftNothing(t, rt, own)
}

// TestServiceContainer_HowItEnded: what the runtime saw is what the attempt says,
// and a runtime that cannot say is the host's failure.
func TestServiceContainer_HowItEnded(t *testing.T) {
	for name, tc := range map[string]struct {
		status  ExitStatus
		waitErr error
		asked   bool
		want    string
	}{
		"by itself":              {status: ExitStatus{}, want: "SERVICE_EXITED"},
		"with a code":            {status: ExitStatus{Code: 2}, want: "SERVICE_FAILED"},
		"for memory":             {status: ExitStatus{Code: 137, OOMKilled: true}, want: "SERVICE_OOM_KILLED"},
		"stopped from outside":   {status: ExitStatus{Code: 143, StoppedFromOutside: true}, want: "CONTAINER_STOPPED_BY_RUNTIME"},
		"asked to":               {status: ExitStatus{Code: 143}, asked: true, want: ""},
		"a runtime that is gone": {waitErr: errors.New("daemon"), want: "SERVICE_HOST_STALLED"},
	} {
		c := &serviceContainer{status: tc.status, waitErr: tc.waitErr}
		if e := c.ending(tc.asked, false); e.Reason != tc.want {
			t.Errorf("%s: %+v, want %s", name, e, tc.want)
		}
	}
}

// TestServiceContainer_ItsLogIsMasked: what it prints of a secret it was given is
// masked, and nothing of it is in the stored log.
func TestServiceContainer_ItsLogIsMasked(t *testing.T) {
	a, rt, start, _ := serviceRig(t)
	dir := t.TempDir()
	store, err := artifacts.NewLocalLogStore(dir)
	must.OK(t, err)
	start.log = newInstanceLog(t.Context(), store, artifacts.InstanceAttempt{SourceID: start.spec.Source.ID, Index: 1, Epoch: 7, Attempt: 2}, LogOptions{}, 1<<20, nil)
	rt.printed = "token is s3cret-value\nhalf: s3cret-"
	c, f := a.startService(t.Context(), start)
	if f != nil {
		t.Fatal(f)
	}
	if f := c.stop(t.Context(), 20); f != nil {
		t.Fatal(f)
	}
	start.log.Close()
	var all string
	must.OK(t, filepath.WalkDir(dir, func(p string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, err := os.ReadFile(p)
		all += string(raw)
		return err
	}))
	if !strings.Contains(all, "token is ***") || !strings.Contains(all, "half: ***") || strings.Contains(all, "s3cret") {
		t.Fatalf("stored %q", all)
	}
}

// TestServiceContainer_OneThatCannotBeStopped: the attempt goes on, its
// reservation and its files stay, and the Orker takes no service until the
// container is gone, when all of it goes.
func TestServiceContainer_OneThatCannotBeStopped(t *testing.T) {
	for name, declared := range map[string]bool{"a declared capacity": true, "none": false} {
		t.Run(name, func(t *testing.T) {
			a, rt, start, own := serviceRig(t)
			ctx, held := t.Context(), amount{milli: 500, bytes: 256 << 20}
			if declared {
				a.capacity = newSourceCapacity(1)
				a.capacity.markUsed(mustPoll(t, a.capacity, a.capacity.source))
				admission, err := a.capacity.admit(a.capacity.source, held)
				must.OK(t, err)
				defer admission.Done()
				ctx = withAdmission(ctx, admission)
			}
			c, f := a.startService(ctx, start)
			if f != nil {
				t.Fatal(f)
			}
			rt.mu.Lock()
			rt.fail["remove"] = errors.New("removal in progress")
			rt.mu.Unlock()
			if f := c.stop(ctx, 20); f == nil || f.Reason != "SERVICE_STOP_STUCK" {
				t.Fatalf("%v", f)
			}
			if a.cordon.ServiceStuck() == nil {
				t.Fatal("the Orker takes services")
			}
			if _, err := os.Stat(filepath.Join(own, "one")); err != nil {
				t.Fatalf("its files: %v", err)
			}
			if declared {
				wantReserved(t, a.capacity, "stuck", held)
			}
			a.retryRemovals(t.Context())
			if a.cordon.ServiceStuck() == nil {
				t.Fatal("the cordon ended while it was there")
			}
			rt.mu.Lock()
			delete(rt.fail, "remove")
			rt.mu.Unlock()
			a.retryRemovals(t.Context())
			if a.cordon.ServiceStuck() != nil {
				t.Fatal("the cordon stands though it is gone")
			}
			if declared {
				wantReserved(t, a.capacity, "gone", amount{})
			}
			leftNothing(t, rt, own)
		})
	}
}

// TestServiceContainer_OneStuckWhileAnotherGoes: the cordon stands for as long as
// any container is stuck, whenever it became so.
func TestServiceContainer_OneStuckWhileAnotherGoes(t *testing.T) {
	a, _, _, _ := serviceRig(t)
	a.dispatcher.unremoved.owe("c2", time.Time{})
	a.keepStuckService("c1", func() { a.keepStuckService("c2", func() {}) })
	a.settleStuckServices()
	if a.cordon.ServiceStuck() == nil {
		t.Fatal("the Orker takes services while c2 is stuck")
	}
	a.dispatcher.unremoved = unremovedContainers{}
	a.settleStuckServices()
	if a.cordon.ServiceStuck() != nil {
		t.Fatal("the cordon stands though both are gone")
	}
}

// TestServiceContainer_ACreateThatFails: what a create may have made is removed;
// where that fails its reservation stays with it, and where the runtime did not
// answer it is owed a removal and nothing is asked of it now.
func TestServiceContainer_ACreateThatFails(t *testing.T) {
	a, rt, start, own := serviceRig(t)
	rt.fail["create"], rt.fail["remove"] = errors.New("refused"), errors.New("removal in progress")
	a.capacity = newSourceCapacity(1)
	a.capacity.markUsed(mustPoll(t, a.capacity, a.capacity.source))
	held := amount{milli: 500, bytes: 256 << 20}
	admission, err := a.capacity.admit(a.capacity.source, held)
	must.OK(t, err)
	if _, f := a.startService(withAdmission(t.Context(), admission), start); f == nil || f.Reason != "SERVICE_STOP_STUCK" {
		t.Fatalf("%v", f)
	}
	admission.Done()
	wantReserved(t, a.capacity, "not removed", held)
	rt.mu.Lock()
	delete(rt.fail, "remove")
	rt.mu.Unlock()
	a.retryRemovals(t.Context())
	wantReserved(t, a.capacity, "removed", amount{})
	leftNothing(t, rt, own)

	rt.fail["create"] = ErrCreateUnanswered
	removed := rt.removes
	if _, f := a.startService(t.Context(), start); f == nil || f.Reason != "SERVICE_HOST_STALLED" {
		t.Fatalf("%v", f)
	}
	if rt.removes != removed || a.dispatcher.unremoved.pending() != 1 {
		t.Fatalf("%d removals, %d owed", rt.removes-removed, a.dispatcher.unremoved.pending())
	}
	leftNothing(t, rt, own)
}

// TestServiceContainer_WaitsForItself: a wait answers only of a container known to
// have ended, and asks again where the first answer was lost.
func TestServiceContainer_WaitsForItself(t *testing.T) {
	a, rt, start, _ := serviceRig(t)
	rt.fail["wait"] = errors.New("daemon")
	c, f := a.startService(t.Context(), start)
	if f != nil {
		t.Fatal(f)
	}
	<-c.exited
	if err := c.Wait(t.Context()); err == nil {
		t.Error("a runtime that cannot say is read as an exit")
	}
	rt.mu.Lock()
	delete(rt.fail, "wait")
	rt.mu.Unlock()
	rt.exit <- ExitStatus{}
	must.OK(t, c.Wait(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := (&serviceContainer{exited: make(chan struct{})}).Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a container that runs: %v", err)
	}
	if err := c.Signal(t.Context(), 1); err == nil {
		t.Error("a signal nobody sends was sent")
	}
	if f := c.stop(t.Context(), 20); f != nil {
		t.Fatal(f)
	}
}
