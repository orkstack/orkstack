package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/pkg/eventsources"
	"github.com/orkstack/orkstack/pkg/logstream"
)

// serviceRuntime is a runtime that can host a service: a network of its own, and
// the container's first process, whose answer filesystem the Orker opens.
type serviceRuntime interface {
	ContainerRuntime
	CreateServiceNetwork(ctx context.Context, name string, labels map[string]string) (string, error)
	RemoveServiceNetwork(ctx context.Context, id string) error
	ContainerPid(ctx context.Context, id ContainerID) (int, error)
	ImageRunsAsRoot(ctx context.Context, ref string) (bool, error)
}

// serviceStart is one start of one instance's container.
type serviceStart struct {
	name    string
	spec    *eventsources.ServiceSpec
	context servicehost.Context
	image   string // by its digest
	log     *instanceLog
	// openAnswer opens the container's answer filesystem.
	openAnswer func(pid int, path string) (*servicehost.AnswerDir, error)
}

// serviceContainer is a service's container as its Orker holds it: its files, its
// network, its log, and how it ended.
type serviceContainer struct {
	a         *Activities
	rt        serviceRuntime
	admission *Admission

	id      ContainerID
	network string
	ws      *serviceWorkspace
	ask     *servicehost.AskDir
	answer  *servicehost.AnswerDir
	mask    *attemptMask
	lines   []*logstream.LineWriter

	started time.Time
	// imageRoot: its image declares the root user, or none.
	imageRoot bool
	// users is closed once nothing reads or writes the container's files; nil is
	// now.
	users <-chan struct{}
	// exited is closed once the runtime answered how the container ended, or
	// that it cannot say.
	exited  chan struct{}
	status  ExitStatus
	waitErr error
}

var _ servicehost.Container = (*serviceContainer)(nil)

// startService starts a service's container with its files and its network. What
// a failure made is gone, or is what the Orker owes a removal.
func (a *Activities) startService(ctx context.Context, s serviceStart) (_ *serviceContainer, failure *instanceFailure) {
	d := a.dispatcher
	rt, ok := d.Runtime.(serviceRuntime)
	if !ok {
		return nil, instanceFail(eventsources.ReasonServiceHostUnfit, "this Orker's runtime hosts no service")
	}
	c := &serviceContainer{a: a, rt: rt, admission: admissionFrom(ctx), exited: make(chan struct{})}
	defer func() {
		if failure != nil {
			if stuck := c.discard(ctx); stuck != nil {
				d.logger().Error("a service that failed to start left a container that could not be removed", "failure", failure.Reason, "detail", failure.Detail)
				failure = stuck
			}
		}
	}()
	var f *instanceFailure
	if c.ws, f = a.newServiceWorkspace(s.name, s.context, s.spec); f != nil {
		return nil, f
	}
	c.mask = d.newAttemptMask(s.log.window.Orker)
	if f := c.mask.register(c.ws.secrets); f != nil {
		reason := eventsources.ReasonServiceHostStalled
		if f.Reason == ReasonMaskCapacityExceeded {
			reason = eventsources.ReasonMaskCapacityExceeded
		}
		return nil, instanceFail(reason, "%s", f.Message)
	}
	// The mask keeps them from here.
	c.ws.secrets = nil
	stalled := func(what string, err error) *instanceFailure {
		return instanceFail(eventsources.ReasonServiceHostStalled, "%s: %v", what, err)
	}
	var err error
	if c.ask, err = servicehost.OpenAskDir(c.ws.ask); err != nil {
		return nil, stalled("open its ask directory", err)
	}
	// Its container must find a beat when it starts.
	if err := c.ask.WriteJSON("alive.json", map[string]any{"schemaVersion": servicehost.SchemaVersion, "beat": 0}); err != nil {
		return nil, stalled("write its first beat", err)
	}
	if c.imageRoot, err = rt.ImageRunsAsRoot(ctx, s.image); err != nil {
		// It names an end a root image comes to, and decides nothing.
		d.logger().Warn("whether a service's image declares root could not be read", "image", s.image, "error", err)
	}
	labels := map[string]string{"ork.orker": d.OrkerID, "ork.source": s.spec.Source.ID, "ork.instance": s.name}
	if c.network, err = rt.CreateServiceNetwork(ctx, "ork-svc-"+s.name, labels); err != nil {
		return nil, stalled("make its network", err)
	}
	spec := a.serviceSpec(s, c, labels)
	if c.id, err = rt.Create(ctx, spec); c.id != "" && !errors.Is(err, ErrCreateUnanswered) {
		c.admission.ContainerCreated(c.id)
	}
	if err != nil {
		if errors.Is(err, ErrCreateUnanswered) {
			// The engine may yet finish it, whatever a removal finds now: it
			// is owed one, and holds nothing, for it is never started.
			d.removeFailedCreate(ctx, c.id, err)
			c.id = ""
		}
		return nil, stalled("create its container", err)
	}
	out, errs := logstream.NewLineWriter(s.log.Line, ""), logstream.NewLineWriter(s.log.Line, logstream.StderrMarker)
	c.lines = []*logstream.LineWriter{out, errs}
	if err := rt.Start(ctx, c.id, c.mask.writer(StreamStdout, out), c.mask.writer(StreamStderr, errs)); err != nil {
		if errors.Is(err, ErrRuntimeStateUnknown) {
			return nil, stalled("start its container", err)
		}
		return nil, instanceFail(eventsources.ReasonServiceFailed, "its container did not start: %v", err)
	}
	c.started = time.Now()
	waited := context.WithoutCancel(ctx)
	go func() {
		defer close(c.exited)
		c.status, c.waitErr = rt.Wait(waited, c.id)
	}()
	pid, err := rt.ContainerPid(ctx, c.id)
	if err == nil {
		c.answer, err = s.openAnswer(pid, servicehost.PathAnswer)
	}
	if err == nil {
		// Asked again: what was opened is this container's only if it is still
		// its process, for a number is another's once its own has ended.
		var still int
		if still, err = rt.ContainerPid(ctx, c.id); err == nil && still != pid {
			err = fmt.Errorf("its process was %d and is %d", pid, still)
		}
	}
	if err != nil {
		// One that is already gone is asked how it ended, not how it was
		// looked for.
		select {
		case <-c.exited:
			if e := c.ending(false, true); e.Reason != "" {
				return nil, instanceFail(e.Reason, "%s", e.Detail)
			}
		case <-time.After(exitSettle):
		case <-ctx.Done():
		}
		return nil, stalled("open its answer filesystem", err)
	}
	return c, nil
}

// exitSettle is how long a container whose process was not found is given to be
// reported ended.
const exitSettle = 2 * time.Second

// serviceSpec is a service's container: a task's sandbox, and what the Orker
// Service Contract adds to it.
func (a *Activities) serviceSpec(s serviceStart, c *serviceContainer, labels map[string]string) ContainerSpec {
	policy := a.dispatcher.Policy
	return ContainerSpec{
		Image: s.image, Env: s.spec.Env, WorkDir: "/", Network: NetworkService, ServiceNetworkID: c.network,
		Mounts: c.ws.mounts(), TmpfsPath: ContainerScratchTmp, TmpfsBytes: policy.TmpfsBytes,
		ServiceTmpfs: &ServiceTmpfs{Path: servicehost.PathAnswer, Bytes: servicehost.AnswerBytes(s.spec.Concurrency, s.spec.Limits.MaxBodyBytes),
			Inodes: servicehost.AnswerInodes(s.spec.Concurrency), UID: os.Getuid(), GID: os.Getgid()},
		NoSwap: true, NoCoreDumps: true, RunAsOrker: true,
		CPUs: s.spec.Resources.CPU, Memory: s.spec.Resources.Memory, PidsLimit: policy.PidsLimit, Labels: labels,
		ReadOnlyRootfs: true, DropAllCapabilities: true, NoNewPrivileges: true, SeccompProfile: SeccompBuiltin, Init: true,
		StopTimeoutSeconds: s.spec.Limits.StopSeconds,
	}
}

// Signal is servicehost.Container's.
func (c *serviceContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	switch sig {
	case syscall.SIGTERM:
		return c.rt.Signal(ctx, c.id, SIGTERM)
	case syscall.SIGKILL:
		return c.rt.Signal(ctx, c.id, SIGKILL)
	}
	return fmt.Errorf("a service is not sent signal %d", sig)
}

// Wait is servicehost.Container's: nil only when the container is known to have
// ended.
func (c *serviceContainer) Wait(ctx context.Context) error {
	select {
	case <-c.exited:
	case <-ctx.Done():
		return ctx.Err()
	}
	if c.waitErr == nil {
		return nil
	}
	_, err := c.rt.Wait(ctx, c.id)
	return err
}

// Remove is servicehost.Container's.
func (c *serviceContainer) Remove(ctx context.Context) error { return c.rt.Remove(ctx, c.id) }

// exitCode is the code it ended with, nil where that is not known.
func (c *serviceContainer) exitCode() *int32 {
	select {
	case <-c.exited:
	default:
		return nil
	}
	if c.waitErr != nil {
		return nil
	}
	code := c.status.Code
	return &code
}

// ending is how the container ended, once exited is closed.
func (c *serviceContainer) ending(asked, beforeFirstStatus bool) sourcehost.Ending {
	if c.waitErr != nil {
		return sourcehost.Ending{Reason: eventsources.ReasonServiceHostStalled, Retried: true, Detail: "its runtime could not say how it ended: " + c.waitErr.Error()}
	}
	e := servicehost.Meaning(servicehost.Exit{Code: int(c.status.Code), AskedToStop: asked, OOMKilled: c.status.OOMKilled,
		StoppedByRuntime: c.status.StoppedFromOutside, BeforeFirstStatus: beforeFirstStatus, ImageUserRoot: c.imageRoot})
	return sourcehost.Ending{Reason: e.Reason, Retried: e.Retried, Detail: e.Detail}
}

// stop runs the stop sequence, the container having been told, and discards what
// it had. A container that cannot be stopped is kept, and said.
func (c *serviceContainer) stop(ctx context.Context, stopSeconds int) *instanceFailure {
	res, err := servicehost.Stop(ctx, c, servicehost.DefaultStopTimes(stopSeconds))
	if res.Killed && res.TermErr == nil {
		c.a.dispatcher.logger().Warn("a service's container did not exit within its grace and was killed", "container", string(c.id), "stop_seconds", stopSeconds)
	}
	if err != nil {
		return c.keep(err)
	}
	c.removed()
	return nil
}

// discard removes what a start that failed made.
func (c *serviceContainer) discard(ctx context.Context) *instanceFailure {
	if c.id != "" {
		rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controlTimeout)
		defer cancel()
		if err := c.rt.Remove(rmCtx, c.id); err != nil {
			return c.keep(fmt.Errorf("%w: not removed: %v", servicehost.ErrStopStuck, err))
		}
	}
	c.removed()
	return nil
}

// removed ends the container's reservation and discards its files and network.
func (c *serviceContainer) removed() {
	c.admission.ContainerRemoved(nil)
	c.cleanup()
}

// keep leaves a container that could not be removed to the Orker's removal retry:
// it keeps its reservation, its files and its network, and the Orker takes no
// further service until it is gone.
func (c *serviceContainer) keep(err error) *instanceFailure {
	c.admission.ContainerRemoved(err)
	if c.a.capacity == nil || !c.a.capacity.owes(c.id) {
		c.a.dispatcher.unremoved.owe(c.id, time.Time{})
	}
	c.a.keepStuckService(c.id, c.cleanup)
	return instanceFail(eventsources.ReasonServiceStopStuck, "%v", err)
}

// closeFiles closes the handles on the conversation once nothing uses them.
func (c *serviceContainer) closeFiles() error {
	close := func() error { return errors.Join(c.answer.Close(), c.ask.Close()) }
	if c.users == nil {
		return close()
	}
	select {
	case <-c.users:
		return close()
	default:
	}
	go func() {
		<-c.users
		if err := close(); err != nil {
			c.a.dispatcher.logger().Warn("a service's conversation did not close", "container", string(c.id), "error", err)
		}
	}()
	return nil
}

// cleanup discards what the container had on its host. It is asked once it is
// gone.
func (c *serviceContainer) cleanup() {
	log := c.a.dispatcher.logger()
	if c.id != "" && !c.started.IsZero() {
		// Its streams end with it; what they held of a secret is masked first.
		if err := c.mask.closeStreams(); err != nil {
			log.Warn("a service's log stream did not close", "container", string(c.id), "error", err)
		}
	}
	for _, l := range c.lines {
		l.Close()
	}
	var errs []error
	errs = append(errs, c.closeFiles())
	if c.network != "" {
		ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
		errs = append(errs, c.rt.RemoveServiceNetwork(ctx, c.network))
		cancel()
	}
	errs = append(errs, c.ws.remove())
	if err := errors.Join(errs...); err != nil {
		log.Warn("what a service had on this host was not all removed; the next boot removes it", "container", string(c.id), "error", err)
	}
}
