package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// `orkctl service test <image>` (docs/ORKER_SERVICE_CONTRACT.md, "Testing
// a service image"): the twin of `activity test`. It runs an image through
// the same host code an Orker uses (servicehost and sourcehost), plays the
// engine's side with an in-process door that accepts every message, and
// writes nothing to a deployment. Network none, the Orker's own non-root
// uid, the bounded answer filesystem, no swap, no core dumps.

func newServiceCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "service",
		Short: "Work with service images (event sources)",
	}
	cmd.AddCommand(newServiceTestCommand(g))
	return cmd
}

func newServiceTestCommand(g *globalFlags) *cobra.Command {
	var publish string
	var keep bool
	var startup time.Duration
	cmd := &cobra.Command{
		Use:   "test <image>",
		Short: "Check a service image against the Orker Service Contract",
		Long: "Run a service image through the Orker's own host code, with no deployment.\n\n" +
			"The image runs as this user's uid (never root), with network none, the\n" +
			"bounded answer filesystem, no swap and no core dumps, and is pinned by\n" +
			"digest first: push it to a registry (`orkctl activity build`), as for\n" +
			"`activity test`. The engine's side is played by an in-process door that\n" +
			"accepts every message.\n\n" +
			"--publish-exec runs a command that publishes one message to the image's\n" +
			"broker; it is given ORK_SERVICE_ROOT, the container's root as this host\n" +
			"sees it. Without it the message checks are skipped, never passed.\n\n" +
			"Requires a working container runtime on Linux. Exits non-zero if any check\n" +
			"fails.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireLocalRunHost(g.hostOS(), "service test"); err != nil {
				return err
			}
			s := &serviceSuite{image: args[0], publish: publish, keep: keep, startup: startup}
			report, err := s.run(cmd.Context())
			if err != nil {
				return err
			}
			if g.json {
				if err := writeGoJSON(cmd.OutOrStdout(), report); err != nil {
					return err
				}
			} else {
				renderServiceTestReport(cmd.OutOrStdout(), report)
			}
			if !report.OK {
				cmd.SilenceUsage, cmd.SilenceErrors = true, true
				return fmt.Errorf("%s does not satisfy the service contract", terminalSafe(report.Image))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&publish, "publish-exec", "", "a command that publishes one message to the image's broker (run with ORK_SERVICE_ROOT)")
	cmd.Flags().BoolVar(&keep, "keep", false, "keep the workspace for inspection (never a secret)")
	cmd.Flags().DurationVar(&startup, "startup", 60*time.Second, "how long the image may take to write its first status")
	return cmd
}

// serviceCheck is one check's outcome; skipped is never a pass.
type serviceCheck struct {
	Name    string `json:"name"`
	Result  string `json:"result"` // pass, fail, skipped
	Details string `json:"details,omitzero"`
}

type serviceTestReport struct {
	Image  string         `json:"image"`
	Pinned string         `json:"pinned"`
	OK     bool           `json:"ok"`
	Checks []serviceCheck `json:"checks"`
}

func renderServiceTestReport(out io.Writer, r *serviceTestReport) {
	fmt.Fprintf(out, "image  %s\npinned %s\n\n", terminalSafe(r.Image), terminalSafe(r.Pinned))
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CHECK\tRESULT\tDETAILS")
	for _, c := range r.Checks {
		fmt.Fprintf(w, "%s\t%s\t%s\n", c.Name, c.Result, terminalSafe(c.Details))
	}
	_ = w.Flush()
}

type serviceSuite struct {
	image   string
	publish string
	keep    bool
	startup time.Duration

	runtime *orker.DockerRuntime
	pinned  string
	spec    eventsources.ServiceSpec
	checks  []serviceCheck
}

func (s *serviceSuite) add(name string, err error) bool {
	c := serviceCheck{Name: name, Result: "pass"}
	if err != nil {
		c.Result, c.Details = "fail", err.Error()
	}
	s.checks = append(s.checks, c)
	return err == nil
}

func (s *serviceSuite) skip(name, why string) {
	s.checks = append(s.checks, serviceCheck{Name: name, Result: "skipped", Details: why})
}

// testLimits are the bounds the suite holds the image to: the contract's
// own, with a short beat so the lost-beat check ends within a minute.
func testLimits(startup time.Duration) eventsources.SpecLimits {
	return eventsources.SpecLimits{
		StatusSeconds: eventsources.DefaultStatusSeconds, StartupSeconds: max(int(startup/time.Second), 1), StopSeconds: eventsources.DefaultStopSeconds,
		AliveSeconds: 2, OrphanSeconds: 10, MaxBodyBytes: 1 << 20, MaxAttributesBytes: eventsources.MaxAttributesBytes,
		PollWaitSeconds: 1, AnswerSeconds: 11, SettleSeconds: eventsources.DefaultSettleSeconds, HoldSeconds: 120,
	}
}

func (s *serviceSuite) run(ctx context.Context) (*serviceTestReport, error) {
	resolution, err := dsl.NewResolver(os.Getenv("ORK_REGISTRY_AUTH_FILE")).Inspect(ctx, s.image, s.image)
	if err != nil {
		return nil, fmt.Errorf("%w\n\n`service test` runs the image the way an Orker will, by digest: push it first", err)
	}
	s.pinned = resolution.Image.GetPinned()
	img, labelErr := eventsources.ReadServiceLabels(resolution.ServiceLabels)
	if !s.add("labels and interface", labelErr) {
		return s.report(), nil
	}
	s.runtime = orker.NewDockerRuntime()
	if _, err := s.runtime.Pull(ctx, s.pinned); err != nil {
		return nil, fmt.Errorf("pull %s: %w", s.pinned, err)
	}
	s.spec = eventsources.ServiceSpec{
		Format: eventsources.SpecFormat, Source: eventsources.SpecSource{ID: "evs-0000000000000001", Namespace: "default", Name: "service-test"},
		Image: s.pinned, Contracts: img.Contracts, Profiles: img.Profiles, Concurrency: 1,
		Delivery: eventsources.DeliveryWord(img.Interface.GetEventSource().GetDelivery()), Limits: testLimits(s.startup),
	}

	main, err := s.start(ctx, "main")
	if err != nil {
		return nil, err
	}
	s.converse(ctx, main)
	s.add("stop: the notice, SIGTERM, an exit within stopSeconds", main.stop(ctx))

	if orphan, err := s.start(ctx, "beat"); err != nil {
		s.add("a lost beat: the container ends itself", err)
	} else {
		s.add("a lost beat: the container ends itself", orphan.loseBeat(ctx))
	}
	return s.report(), nil
}

func (s *serviceSuite) report() *serviceTestReport {
	r := &serviceTestReport{Image: s.image, Pinned: s.pinned, OK: true, Checks: s.checks}
	for _, c := range s.checks {
		if c.Result == "fail" {
			r.OK = false
		}
	}
	return r
}

// serviceRun is one container of the image under test, with the Orker's
// side of its files.
type serviceRun struct {
	s      *serviceSuite
	root   string
	id     orker.ContainerID
	pid    int
	ask    *servicehost.AskDir
	answer *servicehost.AnswerDir
	exited chan struct{}
	// waitErr is set, before exited closes, when the runtime could not say
	// the container exited: exited then means only that the wait ended.
	waitErr error
	logs    *serviceLog
	beat    servicehost.Beat
	beating chan struct{}
	once    sync.Once
}

func (s *serviceSuite) start(ctx context.Context, name string) (*serviceRun, error) {
	root, err := os.MkdirTemp("", "orkctl-service-test-")
	if err != nil {
		return nil, err
	}
	r := &serviceRun{s: s, root: root, exited: make(chan struct{}), logs: &serviceLog{}, beating: make(chan struct{})}
	started := false
	defer func() {
		// A start that failed part way removes what it made: the container,
		// the handles and the workspace.
		if !started {
			r.cleanup(ctx)
		}
	}()
	input, askDir := filepath.Join(root, "input"), filepath.Join(root, "ask")
	for _, d := range []string{filepath.Join(input, "secrets"), askDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	serviceContext, err := servicehost.BuildContext(s.spec, "orkctl", 0, 1, name+"-1")
	if err != nil {
		return nil, err
	}
	raw, err := stablejson.Marshal(serviceContext)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(input, "service.json"), raw, 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(input, "config.json"), []byte("{}"), 0o644); err != nil {
		return nil, err
	}
	if r.ask, err = servicehost.OpenAskDir(askDir); err != nil {
		return nil, err
	}
	if err := r.ask.WriteJSON("alive.json", r.beat.Next()); err != nil {
		return nil, err
	}
	l := s.spec.Limits
	spec := orker.ContainerSpec{
		Image: s.pinned, WorkDir: "/", Network: orker.NetworkNone, RunAsOrker: true,
		CPUs: "1", Memory: "256Mi", PidsLimit: 256, TmpfsPath: "/tmp", TmpfsBytes: 64 << 20,
		ServiceTmpfs: &orker.ServiceTmpfs{Path: servicehost.PathAnswer, Bytes: int64(s.spec.Concurrency)*(2*l.MaxBodyBytes+256<<10) + 1<<20,
			Inodes: 16*int(s.spec.Concurrency) + 64, UID: os.Getuid(), GID: os.Getgid()},
		NoSwap: true, NoCoreDumps: true,
		Mounts: []orker.Mount{{Source: input, Destination: "/orkstack/input", ReadOnly: true},
			{Source: askDir, Destination: servicehost.PathAsk, ReadOnly: true}},
		Labels:         map[string]string{"ork.service-test": strconv.Itoa(os.Getpid())},
		ReadOnlyRootfs: true, DropAllCapabilities: true, NoNewPrivileges: true, Init: true, SeccompProfile: orker.SeccompBuiltin,
		StopTimeoutSeconds: l.StopSeconds,
	}
	if r.id, err = s.runtime.Create(ctx, spec); err != nil {
		return nil, fmt.Errorf("create the container: %w", err)
	}
	if err := s.runtime.Start(ctx, r.id, r.logs, r.logs); err != nil {
		return nil, fmt.Errorf("start the container: %w", err)
	}
	go func() {
		if _, err := s.runtime.Wait(context.WithoutCancel(ctx), r.id); err != nil {
			r.waitErr = fmt.Errorf("the runtime could not say whether the container exited: %w", err)
		}
		close(r.exited)
	}()
	if r.pid, err = s.runtime.ContainerPid(ctx, r.id); err != nil {
		select {
		case <-r.exited:
		case <-time.After(2 * time.Second):
		}
		return nil, startFailure(err, r.exited, func() error { return r.waitErr }, r.logs.bytes())
	}
	if r.answer, err = servicehost.OpenContainerAnswer(r.pid, servicehost.PathAnswer); err != nil {
		return nil, fmt.Errorf("open the container's answer filesystem: %w", err)
	}
	go r.beatLoop(time.Duration(l.AliveSeconds) * time.Second)
	started = true
	return r, nil
}

// serviceLog is the container's stdout and stderr together, written by
// the runtime's two copies at once and read by the suite.
type serviceLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *serviceLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *serviceLog) bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Clone(l.buf.Bytes())
}

// startFailure is what the suite says when the container's process could
// not be found: a container that already exited is reported as that, with
// the last 2 KiB of what it wrote, so an image that dies at once says why.
func startFailure(err error, exited <-chan struct{}, waitErr func() error, logs []byte) error {
	select {
	case <-exited:
		if werr := waitErr(); werr != nil {
			return errors.Join(err, werr)
		}
	default:
		return err
	}
	const tail = 2048
	if len(logs) > tail {
		logs = logs[len(logs)-tail:]
	}
	return fmt.Errorf("the container exited before it could be watched; it wrote:\n%s", terminalSafe(string(bytes.TrimSpace(logs))))
}

func (r *serviceRun) beatLoop(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-r.beating:
			return
		case <-t.C:
			_ = r.ask.WriteJSON("alive.json", r.beat.Next())
		}
	}
}

func (r *serviceRun) stopBeat() { r.once.Do(func() { close(r.beating) }) }

// waitReady reads the status as an Orker does, with the profile's rules
// applied, until the service is ready and healthy, says it is fatal, never
// reports, or is still not healthy when its startup bound ends.
func (s *serviceSuite) waitReady(ctx context.Context, r *serviceRun) error {
	started := time.Now()
	startup := time.Duration(s.spec.Limits.StartupSeconds) * time.Second
	watch := servicehost.NewStatusWatch(started, s.spec.Limits.StartupSeconds, s.spec.Limits.StatusSeconds)
	atMostOnce := s.spec.Delivery == eventsources.DeliveryWord(orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE)
	var last servicehost.StepID
	for {
		raw, id, err := r.answer.ReadStepID(servicehost.StatusName, servicehost.MaxStatusBytes)
		switch {
		case err == nil:
			st, err := servicehost.ReadStatus(raw, sourcehost.Vocabulary)
			if err != nil {
				return err
			}
			st, _ = sourcehost.JudgeStatus(st, atMostOnce)
			watch.Observe(st, time.Now(), id != last)
			last = id
		case !errors.Is(err, servicehost.ErrNotYet):
			return err
		}
		st, _ := watch.Last()
		if done, err := readyVerdict(watch.Health(time.Now()), st, time.Since(started), startup); done {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// readyVerdict judges one instant of waitReady: done with no error when
// the service is ready; done with the reason when it says it can never
// work, never reported, or is still not ready and healthy after its
// startup bound; not done while it may yet become so.
func readyVerdict(h servicehost.Health, st servicehost.Status, elapsed, startup time.Duration) (bool, error) {
	switch h {
	case servicehost.HealthReady:
		return true, nil
	case servicehost.HealthFatal:
		return true, fmt.Errorf("the image says it can never work with this configuration: %s %s", st.Reason, st.Field)
	case servicehost.HealthNeverReported:
		return true, fmt.Errorf("no status within %s", startup)
	}
	if elapsed <= startup {
		return false, nil
	}
	switch h {
	case servicehost.HealthUnhealthy:
		return true, fmt.Errorf("its check is not ok after %s: %s %s", startup, st.Reason, st.Field)
	case servicehost.HealthNotReady:
		return true, fmt.Errorf("not ready after %s", startup)
	case servicehost.HealthNoVerdict:
		return true, fmt.Errorf("no fresh status after %s", startup)
	}
	return false, nil
}

// converse runs the status, a quiet poll and, with --publish-exec, a message
// accepted and a message released, through the Orker's own lane driver.
func (s *serviceSuite) converse(ctx context.Context, r *serviceRun) {
	if !s.add("a status within the startup bound, ready and healthy", s.waitReady(ctx, r)) {
		return
	}
	door := &acceptingDoor{}
	judge := sourcehost.NewJudge(&orkstackv1.EventSource{Id: s.spec.Source.ID, Name: s.spec.Source.Name, Namespace: "default", JudgingRevision: 1},
		1, door, noStager{}, time.Now)
	driver := &sourcehost.LaneDriver{Index: 0, Steps: r.answer, Asks: r.ask, Judge: judge, Recorder: noReceipts{}, Now: time.Now,
		Limits: sourcehost.LaneLimits{PollWait: time.Second, Answer: 11 * time.Second, Settle: 30 * time.Second, Hold: 120 * time.Second,
			LeaseSeconds: 60, MaxBodyBytes: s.spec.Limits.MaxBodyBytes}}
	poll := func(wait int, within time.Duration) (sourcehost.Exchange, error) {
		pollCtx, cancel := context.WithTimeout(ctx, within)
		defer cancel()
		return driver.Poll(pollCtx, wait)
	}
	ex, err := poll(1, 30*time.Second)
	if err == nil && ex.Kind != sourcehost.ExchangeQuiet {
		err = fmt.Errorf("an empty broker answered exchange kind %d, not empty.json", ex.Kind)
	}
	s.add("a quiet poll is answered empty.json within the wait", err)

	if s.publish == "" {
		s.skip("a message is handed over and acknowledged", "no --publish-exec")
		s.skip("a released message is delivered again, counted", "no --publish-exec")
		return
	}
	err = s.publishOne(ctx, r)
	if err == nil {
		ex, err = poll(5, time.Minute)
	}
	if err == nil && (ex.Kind != sourcehost.ExchangeSettled || ex.Decision.Action != sourcehost.ActionAck || !ex.Settled.OK) {
		err = fmt.Errorf("the message ended kind %d action %q, not acknowledged", ex.Kind, ex.Decision.Action)
	}
	if err == nil {
		if again, perr := poll(1, 30*time.Second); perr != nil {
			err = perr
		} else if again.Kind != sourcehost.ExchangeQuiet {
			err = errors.New("an acknowledged message was delivered again")
		}
	}
	s.add("a message is handed over and acknowledged", err)

	door.setWait(true)
	err = s.publishOne(ctx, r)
	var held sourcehost.Exchange
	if err == nil {
		held, err = poll(5, time.Minute)
	}
	if err == nil && held.Kind != sourcehost.ExchangeHeld {
		err = fmt.Errorf("the message ended kind %d, not held", held.Kind)
	}
	if err == nil {
		vctx, cancel := context.WithTimeout(ctx, time.Minute)
		var released sourcehost.Exchange
		released, err = driver.Verdict(vctx, held.Message, sourcehost.Decision{Disposition: sourcehost.Wait, Action: sourcehost.ActionRelease})
		cancel()
		if err == nil && released.Kind != sourcehost.ExchangeSettled {
			err = fmt.Errorf("the release ended kind %d, not settled", released.Kind)
		}
	}
	if err == nil {
		door.setWait(false)
		again, perr := poll(5, time.Minute)
		switch {
		case perr != nil:
			err = perr
		case again.Message.ID != held.Message.ID || again.Message.DeliveryCount <= held.Message.DeliveryCount:
			err = fmt.Errorf("after a release: id %q count %d, then %q count %d", held.Message.ID, held.Message.DeliveryCount,
				again.Message.ID, again.Message.DeliveryCount)
		}
	}
	s.add("a released message is delivered again, counted", err)
}

func (s *serviceSuite) publishOne(ctx context.Context, r *serviceRun) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", s.publish)
	cmd.Env = append(os.Environ(), "ORK_SERVICE_ROOT=/proc/"+strconv.Itoa(r.pid)+"/root")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("--publish-exec: %w: %s", err, bytes.TrimSpace(out))
	}
	return nil
}

// stop runs the stop sequence: the notice, then SIGTERM, and the container
// must exit by itself within stopSeconds.
func (r *serviceRun) stop(ctx context.Context) error {
	defer r.cleanup(ctx)
	if err := r.ask.WriteJSON("stop.json", map[string]any{"schemaVersion": 1}); err != nil {
		return err
	}
	res, err := servicehost.Stop(ctx, runtimeContainer{r}, servicehost.DefaultStopTimes(r.s.spec.Limits.StopSeconds))
	if err != nil {
		return err
	}
	if res.Killed {
		return fmt.Errorf("it did not exit within %ds of SIGTERM and was killed", r.s.spec.Limits.StopSeconds)
	}
	return nil
}

// loseBeat stops the beat and waits for the container to end itself.
func (r *serviceRun) loseBeat(ctx context.Context) error {
	defer r.cleanup(ctx)
	if err := r.s.waitReady(ctx, r); err != nil {
		return err
	}
	r.stopBeat()
	bound := time.Duration(r.s.spec.Limits.OrphanSeconds)*time.Second + 20*time.Second
	select {
	case <-r.exited:
		return r.waitErr
	case <-time.After(bound):
		return fmt.Errorf("still running %s after its last beat: a conforming image ends itself", bound)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *serviceRun) cleanup(ctx context.Context) {
	r.stopBeat()
	if r.answer != nil {
		_ = r.answer.Close()
	}
	detached := context.WithoutCancel(ctx)
	if r.id != "" {
		_ = r.s.runtime.Signal(detached, r.id, orker.SIGKILL)
		_ = r.s.runtime.Remove(detached, r.id)
	}
	if r.ask != nil {
		_ = r.ask.Close()
	}
	if !r.s.keep {
		_ = os.RemoveAll(r.root)
	}
}

// runtimeContainer is the stop sequence's view of the container under test.
type runtimeContainer struct{ r *serviceRun }

func (c runtimeContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	s := orker.SIGTERM
	if sig == syscall.SIGKILL {
		s = orker.SIGKILL
	}
	return c.r.s.runtime.Signal(ctx, c.r.id, s)
}

func (c runtimeContainer) Wait(ctx context.Context) error {
	select {
	case <-c.r.exited:
		return c.r.waitErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c runtimeContainer) Remove(ctx context.Context) error { return c.r.s.runtime.Remove(ctx, c.r.id) }

// acceptingDoor plays the engine's side of a source for the test: every
// message is accepted, or waited on while wait is set. It is OrkStack's own
// door, not a broker: the image's broker is real.
type acceptingDoor struct {
	mu   sync.Mutex
	wait bool
	runs int
}

func (d *acceptingDoor) setWait(w bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.wait = w
}

func (d *acceptingDoor) Confirm(context.Context, int64) (sourcehost.Confirmation, error) {
	return sourcehost.Confirmation{Revision: 1}, nil
}

func (d *acceptingDoor) Offer(context.Context, sourcehost.Offer) (sourcehost.Answer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.wait {
		return sourcehost.Answer{Kind: sourcehost.AnswerWait, Cause: "service_test"}, nil
	}
	return sourcehost.Answer{Kind: sourcehost.AnswerProceed}, nil
}

func (d *acceptingDoor) Start(context.Context, sourcehost.Start) (sourcehost.Answer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.runs++
	return sourcehost.Answer{Kind: sourcehost.AnswerAccepted, RunID: "service-test-" + strconv.Itoa(d.runs)}, nil
}

func (d *acceptingDoor) Probe(context.Context) error                { return nil }
func (d *acceptingDoor) RequestPause(context.Context, string) error { return nil }

type noStager struct{}

func (noStager) Stage(context.Context, sourcehost.HandedMessage, sourcehost.Part, []byte) (sourcehost.StagedPart, error) {
	return sourcehost.StagedPart{}, errors.New("service test stages nothing")
}

type noReceipts struct{}

func (noReceipts) Record(context.Context, sourcehost.HandedMessage, int64, sourcehost.Receipt) error {
	return nil
}
