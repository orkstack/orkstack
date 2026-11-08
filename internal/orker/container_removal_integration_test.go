//go:build integration

package orker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests answer one question with real containers: when a task ends, or
// the Orker that ran it does, what stays on the host, for how long, and what
// removes it (docs/DEPLOY.md, "What removes a task's container"). A count is
// always by label, never of the host's containers: other proofs run beside
// these.

// removalRunID is a run id no other process's test uses, which is the label
// these tests count by.
func removalRunID(name string) string {
	return fmt.Sprintf("ork-removal-%d-%s", os.Getpid(), name)
}

// removalInput is baseInput under a run id of its own.
func removalInput(t *testing.T, name string, command ...string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := baseInput(t, name, command...)
	in.RunId = removalRunID(name)
	in.IdempotencyKey = "default/" + in.GetRunId() + "/" + name
	t.Cleanup(func() { removeByLabel(t, "ork.run="+in.GetRunId()) })
	return in
}

// removeByLabel is a test's own cleanup, for a test that failed before the
// system under test removed what it should have.
func removeByLabel(t *testing.T, label string) {
	t.Helper()
	cli := containerCLI(t)
	out, err := exec.Command(cli, "ps", "--all", "--quiet", "--filter", "label="+label).Output()
	if err != nil {
		return
	}
	for _, id := range strings.Fields(string(out)) {
		_ = exec.Command(cli, "rm", "--force", "--volumes", id).Run()
	}
}

// containersLabelled is every container, in any state, carrying label.
func containersLabelled(ctx context.Context, t *testing.T, label string) []string {
	t.Helper()
	out, err := exec.CommandContext(ctx, containerCLI(t), "ps", "--all", "--quiet", "--filter", "label="+label).Output()
	if err != nil {
		t.Fatalf("list containers labelled %s: %v", label, err)
	}
	return strings.Fields(string(out))
}

// awaitLabelled waits until a container carries label and returns its id.
func awaitLabelled(ctx context.Context, t *testing.T, label string) string {
	t.Helper()
	for {
		if ids := containersLabelled(ctx, t, label); len(ids) > 0 {
			return ids[0]
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no container labelled %s appeared: %v", label, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// containerState is a container's state as the engine names it.
func containerState(ctx context.Context, t *testing.T, id string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, containerCLI(t), "inspect", "--format", "{{.State.Status}}", id).Output()
	if err != nil {
		t.Fatalf("inspect container %s: %v", id, err)
	}
	return strings.TrimSpace(string(out))
}

// awaitState waits until a container is in state.
func awaitState(ctx context.Context, t *testing.T, id, state string) {
	t.Helper()
	for {
		if got := containerState(ctx, t, id); got == state {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("container %s never reached %s (it is %s): %v", id, state, containerState(context.Background(), t, id), ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// awaitTicks waits until a container that prints one "tick" a second has
// printed count of them. It is the positive control of every window below in
// which something must NOT happen: a container still ticking was running, and
// the window really passed, however slow the host.
func awaitTicks(ctx context.Context, t *testing.T, id string, count int) {
	t.Helper()
	for {
		out, err := exec.CommandContext(ctx, containerCLI(t), "logs", id).CombinedOutput()
		if err != nil {
			t.Fatalf("read the logs of container %s: %v: %s", id, err, out)
		}
		if strings.Count(string(out), "tick") >= count {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("container %s printed fewer than %d ticks: %v", id, count, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// removalWitness is the real runtime, recording each container an attempt
// created and the state it was in when the attempt asked to remove it. It is
// what makes "nothing is left" mean something: an attempt that never made a
// container leaves nothing either.
type removalWitness struct {
	orker.ContainerRuntime
	mu      sync.Mutex
	created []orker.ContainerID
	removed map[orker.ContainerID]string
	cli     string
}

func witnessRuntime(t *testing.T, runtime orker.ContainerRuntime) *removalWitness {
	t.Helper()
	return &removalWitness{ContainerRuntime: runtime, cli: containerCLI(t), removed: map[orker.ContainerID]string{}}
}

func (w *removalWitness) Create(ctx context.Context, spec orker.ContainerSpec) (orker.ContainerID, error) {
	id, err := w.ContainerRuntime.Create(ctx, spec)
	if err == nil {
		w.mu.Lock()
		w.created = append(w.created, id)
		w.mu.Unlock()
	}
	return id, err
}

func (w *removalWitness) Remove(ctx context.Context, id orker.ContainerID) error {
	state, _ := exec.CommandContext(ctx, w.cli, "inspect", "--format", "{{.State.Status}}", string(id)).Output()
	w.mu.Lock()
	w.removed[id] = strings.TrimSpace(string(state))
	w.mu.Unlock()
	return w.ContainerRuntime.Remove(ctx, id)
}

func (w *removalWitness) createdCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.created)
}

// requireOneCreatedAndRemoved holds the attempt to having made exactly one
// container, removed it, and left nothing labelled for its run. It returns the
// state the container was in when the attempt asked to remove it.
func (w *removalWitness) requireOneCreatedAndRemoved(ctx context.Context, t *testing.T, runID string) string {
	t.Helper()
	w.mu.Lock()
	created := append([]orker.ContainerID(nil), w.created...)
	states := map[orker.ContainerID]string{}
	for id, state := range w.removed {
		states[id] = state
	}
	w.mu.Unlock()
	if len(created) != 1 {
		t.Fatalf("the attempt created %d containers, want 1; the case proves nothing", len(created))
	}
	state, asked := states[created[0]]
	if !asked {
		t.Errorf("the attempt never asked to remove its container %s", created[0])
	}
	t.Logf("observed: the container was %q when the attempt removed it", state)
	if left := containersLabelled(ctx, t, "ork.run="+runID); len(left) != 0 {
		t.Errorf("containers left behind for run %s: %v", runID, left)
	}
	return state
}

// TestIntegration_ContainerRemoval_HoweverAnAttemptEnds is every way an attempt
// that made a container can end while its Orker lives. Each leaves nothing,
// because the removal is a defer of the attempt on a context its cancellation
// does not reach.
func TestIntegration_ContainerRemoval_HoweverAnAttemptEnds(t *testing.T) {
	requireDocker(t)
	const ticking = `while :; do echo tick; sleep 1; done`
	cases := []struct {
		name    string
		command string
		// timeout is the task's own timeout in seconds; 0 keeps the default.
		timeout int32
		// cancelOnRunning cancels the attempt once its workload prints.
		cancelOnRunning bool
		wantReason      string
	}{
		{name: "success", command: `printf '{"ok":true}' > /orkstack/output/values.json`},
		{name: "exit-1", command: `exit 1`, wantReason: orker.ReasonContainerExitNonzero},
		{name: "malformed-output", command: `printf '{not json' > /orkstack/output/values.json`, wantReason: orker.ReasonValuesMalformed},
		{name: "no-output", command: `true`, wantReason: orker.ReasonValuesMissing},
		{name: "timeout", command: ticking, timeout: 1, wantReason: orker.ReasonContainerTimeout},
		{name: "cancel", command: ticking, cancelOnRunning: true},
		{name: "ignored-sigterm", command: `trap "" TERM; ` + ticking, cancelOnRunning: true},
		{name: "ignored-sigterm-timeout", command: `trap "" TERM; ` + ticking, timeout: 1, wantReason: orker.ReasonContainerTimeout},
	}
	// One dispatcher, because its bind probe is the slow part of each case; each
	// case watches the real runtime through a witness of its own.
	dispatcher, _ := newDispatcher(t)
	real := dispatcher.Runtime
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			witness := witnessRuntime(t, real)
			dispatcher.Runtime = witness
			in := removalInput(t, tt.name, "/bin/sh", "-c", tt.command)
			in.CancellationGracePeriodSeconds = 1
			if tt.timeout > 0 {
				in.TimeoutSeconds = tt.timeout
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()
			if tt.cancelOnRunning {
				go func() {
					// The workload's own output says it is running, so the
					// cancel lands on a live container.
					for runCtx.Err() == nil {
						out, _ := exec.CommandContext(runCtx, witness.cli, "ps", "--quiet", "--filter", "label=ork.run="+in.GetRunId()).Output()
						if id := strings.TrimSpace(string(out)); id != "" {
							logs, _ := exec.CommandContext(runCtx, witness.cli, "logs", id).CombinedOutput()
							if strings.Contains(string(logs), "tick") {
								cancelRun()
								return
							}
						}
						select {
						case <-runCtx.Done():
						case <-time.After(50 * time.Millisecond):
						}
					}
				}()
			}

			_, err := dispatcher.Execute(runCtx, in)
			switch {
			case tt.wantReason != "" && (err == nil || !strings.Contains(err.Error(), tt.wantReason)):
				t.Errorf("Execute = %v, want %s", err, tt.wantReason)
			case tt.wantReason == "" && !tt.cancelOnRunning && err != nil:
				t.Errorf("Execute = %v, want success", err)
			}
			if got := witness.requireOneCreatedAndRemoved(ctx, t, in.GetRunId()); got != "exited" {
				t.Errorf("the container was %q when removed, want exited: the attempt must end it first", got)
			}
		})
	}
}

// TestIntegration_ContainerRemoval_AStartTheRuntimeRefusesIsRemoved is the
// attempt that never ran: the engine made the container and could not start
// its command, so it stays in a state a `ps` without --all does not show.
func TestIntegration_ContainerRemoval_AStartTheRuntimeRefusesIsRemoved(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)
	witness := witnessRuntime(t, dispatcher.Runtime)
	dispatcher.Runtime = witness
	in := removalInput(t, "unstartable", "/no/such/command")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := dispatcher.Execute(ctx, in); err == nil {
		t.Fatal("Execute of a command that does not exist succeeded")
	}
	witness.requireOneCreatedAndRemoved(ctx, t, in.GetRunId())
}

// TestIntegration_ContainerRemoval_ACommitHitAndAMemoHitStartNoContainer: a
// retry that adopts a commit and a run that reuses a memo each hand back a
// result with no container, so neither has anything to remove. The runtime is
// the witness for "no container", and the label for "nothing left".
func TestIntegration_ContainerRemoval_ACommitHitAndAMemoHitStartNoContainer(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const command = `printf '{"ok":true}' > /orkstack/output/values.json`
	dispatcher, _ := newDispatcher(t)
	real := dispatcher.Runtime

	t.Run("commit-hit", func(t *testing.T) {
		witness := witnessRuntime(t, real)
		dispatcher.Runtime = witness
		in := removalInput(t, "commit-hit", "/bin/sh", "-c", command)
		in.CommitUri = "mem://commits/" + in.GetRunId() + "/commit.json"
		if _, err := dispatcher.Execute(ctx, in); err != nil {
			t.Fatalf("first attempt: %v", err)
		}
		witness.requireOneCreatedAndRemoved(ctx, t, in.GetRunId())
		second, err := dispatcher.Execute(ctx, in)
		if err != nil {
			t.Fatalf("retry: %v", err)
		}
		if !second.GetCommitAdopted() {
			t.Fatal("the retry did not adopt the commit; the case proves nothing")
		}
		if got := witness.createdCount(); got != 1 {
			t.Errorf("the retry created a container: %d in all", got)
		}
		if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 0 {
			t.Errorf("containers left behind: %v", left)
		}
	})

	t.Run("memo-hit", func(t *testing.T) {
		dispatcher.Memos = artifacts.NewMemoryMemoStore()
		witness := witnessRuntime(t, real)
		dispatcher.Runtime = witness
		cachedRun := func(name string) *orkstackv1.ExecuteTaskInput {
			in := removalInput(t, "memo", "/bin/sh", "-c", command)
			in.RunId = removalRunID(name)
			in.IdempotencyKey = "default/" + in.GetRunId() + "/memo"
			t.Cleanup(func() { removeByLabel(t, "ork.run="+in.GetRunId()) })
			key, err := artifacts.CommitKey("default", in.GetRunId(), "memo")
			if err != nil {
				t.Fatal(err)
			}
			in.CommitUri = "mem://commits/" + key
			in.OrkflowDefinitionDigest = "sha256:removal"
			in.Cache = &orkstackv1.CacheContext{Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW, TtlSeconds: 3600}
			return in
		}
		first := cachedRun("memo-first")
		if _, err := dispatcher.Execute(ctx, first); err != nil {
			t.Fatalf("first run: %v", err)
		}
		witness.requireOneCreatedAndRemoved(ctx, t, first.GetRunId())
		second := cachedRun("memo-second")
		result, err := dispatcher.Execute(ctx, second)
		if err != nil {
			t.Fatalf("second run: %v", err)
		}
		if got := result.GetCache().GetOutcome(); got != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			t.Fatalf("the second run's memo step was %s; the case proves nothing", got)
		}
		if got := witness.createdCount(); got != 1 {
			t.Errorf("a memo hit created a container: %d in all", got)
		}
		if left := containersLabelled(ctx, t, "ork.run="+second.GetRunId()); len(left) != 0 {
			t.Errorf("containers left behind: %v", left)
		}
	})
}

// TestIntegration_ContainerRemoval_APinKeepsItsDirectoryNotItsContainers: a
// pin's shared directory outlives each member, and its containers do not, on
// success and on failure alike.
func TestIntegration_ContainerRemoval_APinKeepsItsDirectoryNotItsContainers(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dispatcher, _ := newDispatcher(t)
	witness := witnessRuntime(t, dispatcher.Runtime)
	dispatcher.Runtime = witness
	instance := fmt.Sprintf("%08x-0000-4000-8000-000000000000", os.Getpid())
	runID := removalRunID("pin")
	t.Cleanup(func() { removeByLabel(t, "ork.run="+runID) })
	t.Cleanup(func() {
		if _, err := orker.ReleasePinDirectory(dispatcher.WorkRoot, runID, "build", instance); err != nil {
			t.Errorf("release the pin: %v", err)
		}
	})

	member := func(name, script string) *orkstackv1.ExecuteTaskInput {
		in := baseInput(t, name, "/bin/sh", "-c", script)
		in.RunId = runID
		in.IdempotencyKey = "default/" + runID + "/" + name
		in.Pin = &orkstackv1.PinContext{Name: "build", InstanceId: instance, TimeoutSeconds: 600}
		in.RunAsOrkerUser = true
		return in
	}
	if _, err := dispatcher.Execute(ctx, member("warm", `echo warm > /orkstack/shared/stage; printf '{}' > /orkstack/output/values.json`)); err != nil {
		t.Fatalf("first member: %v", err)
	}
	if left := containersLabelled(ctx, t, "ork.pin="+instance); len(left) != 0 {
		t.Errorf("the first member left containers while the pin is open: %v", left)
	}
	if _, err := dispatcher.Execute(ctx, member("fails", `exit 1`)); err == nil {
		t.Fatal("the failing member succeeded")
	}
	if left := containersLabelled(ctx, t, "ork.pin="+instance); len(left) != 0 {
		t.Errorf("the failing member left containers while the pin is open: %v", left)
	}
	if got := witness.createdCount(); got != 2 {
		t.Fatalf("the pin's members created %d containers, want 2; the case proves nothing", got)
	}
	pinRoot := filepath.Join(dispatcher.WorkRoot, "ork-pin-"+runID+"-build-"+instance)
	if body, err := os.ReadFile(filepath.Join(pinRoot, "shared", "stage")); err != nil || strings.TrimSpace(string(body)) != "warm" {
		t.Errorf("the pin's shared directory did not survive its members' removal: %q, %v", body, err)
	}
}

// heldOrkerVar names the spec of the child role of the tests below: TestMain
// runs a real Orker in this test binary, which the test SIGKILLs.
const heldOrkerVar = "ORK_TEST_HELD_ORKER"

// heldOrkerSpec is what the child Orker boots with and runs.
type heldOrkerSpec struct {
	OrkerID  string          `json:"orker_id"`
	WorkRoot string          `json:"work_root"`
	StoreDir string          `json:"store_dir"`
	Input    json.RawMessage `json:"input"`
}

// runHeldOrker is the child: it boots an Orker as a deployment does, claiming
// its identity and reaping by it, runs one attempt, and is killed during it.
func runHeldOrker(path string) int {
	fail := func(what string, err error) int {
		fmt.Fprintf(os.Stderr, "held orker: %s: %v\n", what, err)
		return 1
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return fail("read the spec", err)
	}
	var spec heldOrkerSpec
	if err := json.Unmarshal(body, &spec); err != nil {
		return fail("decode the spec", err)
	}
	var in orkstackv1.ExecuteTaskInput
	if err := protojson.Unmarshal(spec.Input, &in); err != nil {
		return fail("decode the input", err)
	}
	store, err := artifacts.NewLocalStore(spec.StoreDir)
	if err != nil {
		return fail("open the store", err)
	}
	defer store.Close()
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Runtime: orker.NewDockerRuntime(), OrkerID: spec.OrkerID, WorkRoot: spec.WorkRoot,
		Commits: artifacts.NewMemoryCommitStore(), Refs: artifacts.MemoryRefPredictor{},
	})
	ctx := context.Background()
	if err := activities.Boot(ctx); err != nil {
		return fail("boot", err)
	}
	if _, err := activities.Dispatcher().Execute(ctx, &in); err != nil {
		return fail("execute", err)
	}
	return 0
}

// removalWorkRoot is a fresh work root the daemon can bind.
func removalWorkRoot(t *testing.T, name string) string {
	t.Helper()
	if err := os.MkdirAll("../../.data/work", 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("../../.data/work", proof.WorkRootPattern(name))
	if err != nil {
		t.Fatal(err)
	}
	if root, err = filepath.Abs(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

// startHeldOrker runs the child Orker on the input and returns once its
// container is running. kill is a real SIGKILL of that one process, what an
// out-of-memory kill or a lost host's process does, so the two CLIs it started
// are left to exit by themselves as they are in a crash.
func startHeldOrker(ctx context.Context, t *testing.T, orkerID, workRoot string, in *orkstackv1.ExecuteTaskInput) (container string, kill func()) {
	t.Helper()
	encoded, err := protojson.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(heldOrkerSpec{OrkerID: orkerID, WorkRoot: workRoot, StoreDir: t.TempDir(), Input: encoded})
	if err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(t.TempDir(), "held-orker.json")
	if err := os.WriteFile(specPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), heldOrkerVar+"="+specPath)
	output := &boundedOutput{}
	cmd.Stdout, cmd.Stderr = output, output
	// The output is copied through a pipe the two CLIs it starts inherit
	// nothing of, because they get pipes of their own, so Wait returns with
	// the child.
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the Orker: %v", err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	var once sync.Once
	kill = func() {
		once.Do(func() {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			<-exited
		})
	}
	t.Cleanup(kill)
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		ids := containersLabelled(waitCtx, t, "ork.run="+in.GetRunId())
		if len(ids) > 0 && containerState(waitCtx, t, ids[0]) == "running" {
			return ids[0], kill
		}
		select {
		case <-exited:
			t.Fatalf("the Orker exited before its container ran: %s", output.String())
		case <-waitCtx.Done():
			t.Fatalf("the Orker's container never ran: %v: %s", waitCtx.Err(), output.String())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// boundedOutput keeps what a child printed, for the failure that quotes it.
type boundedOutput struct {
	mu  sync.Mutex
	buf []byte
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := 1<<16 - len(b.buf); room > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

func (b *boundedOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// removalLife is one Orker's Activities, booted, in this process. The test
// drives the retry itself (RetryRemovals), so that a periodic one cannot remove
// what it is still observing; TestActivities_RetriesARefusedRemovalWithNoCapacityDeclared
// proves the loop runs.
func removalLife(ctx context.Context, t *testing.T, orkerID, workRoot string, runtime orker.ContainerRuntime) *orker.Activities {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Runtime: runtime, OrkerID: orkerID, WorkRoot: workRoot,
		Commits: artifacts.NewMemoryCommitStore(), Refs: artifacts.MemoryRefPredictor{},
	})
	orker.SetRemovalRetryEvery(activities, time.Hour)
	if err := activities.Boot(ctx); err != nil {
		t.Fatalf("Boot: %v", err)
	}
	t.Cleanup(activities.Close)
	return activities
}

// TestIntegration_ContainerRemoval_AKilledOrkersContainerRunsUntilItsReplacementReapsIt:
// nothing in the daemon stops a dead Orker's container, and its task's timeout
// is the Orker's own clock, so the container outlives that timeout. A
// replacement under the same identity removes it at boot.
func TestIntegration_ContainerRemoval_AKilledOrkersContainerRunsUntilItsReplacementReapsIt(t *testing.T) {
	requireDocker(t)
	for _, tt := range []struct {
		name string
		// anotherRoot restarts the same explicit id over another work root:
		// the reap goes by the id, not by where the workspaces were.
		anotherRoot bool
	}{{name: "same-work-root"}, {name: "another-work-root", anotherRoot: true}} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			id := proof.OrkerID("removal-killed-" + tt.name)
			workRoot := removalWorkRoot(t, "removal-killed")
			in := removalInput(t, "killed-"+tt.name, "/bin/sh", "-c", `while :; do echo tick; sleep 1; done`)
			in.TimeoutSeconds = 2
			container, kill := startHeldOrker(ctx, t, id, workRoot, in)

			kill()
			killedAt := time.Now()
			// Its timeout of 2 s has plainly passed by the fifth tick, and it
			// still ticks.
			awaitTicks(ctx, t, container, 5)
			if got := containerState(ctx, t, container); got != "running" {
				t.Fatalf("the container is %q after its Orker was killed; a crash does not stop it", got)
			}
			// What still names the container once its Orker is gone: on Docker
			// its shim, and the Orker's two CLIs only for as long as they take
			// to notice.
			clis, _ := exec.CommandContext(ctx, "pgrep", "-af", container).Output()
			t.Logf("observed: after the kill, processes naming the container:\n%s", clis)

			replacementRoot := workRoot
			if tt.anotherRoot {
				replacementRoot = removalWorkRoot(t, "removal-killed-other")
			}
			removalLife(ctx, t, id, replacementRoot, orker.NewDockerRuntime())
			if left := containersLabelled(ctx, t, "ork.orker="+id); len(left) != 0 {
				t.Fatalf("the replacement left its predecessor's containers: %v", left)
			}
			t.Logf("observed: running for %s after the kill, removed by the replacement's boot", time.Since(killedAt).Round(time.Second))
			// A boot looks for attempt directories under its own work root only
			// (docs/DEPLOY.md, "What removes a task's container").
			left, err := filepath.Glob(filepath.Join(workRoot, "ork-"+in.GetRunId()+"-*"))
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tt.anotherRoot && len(left) != 1:
				t.Errorf("attempt directories under the old work root: %v, want the one no boot looks at", left)
			case !tt.anotherRoot && len(left) != 0:
				t.Errorf("attempt directories its replacement's boot left: %v", left)
			}
		})
	}
}

// TestIntegration_ContainerRemoval_AnotherIdentityLeavesThePredecessorsContainer:
// an Orker removes only what carries its own identity. Restarted under another
// id, or under another work root when the id is derived from it, it leaves its
// predecessor's container running, and what docs/DEPLOY.md tells an operator to
// run is what removes it.
func TestIntegration_ContainerRemoval_AnotherIdentityLeavesThePredecessorsContainer(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	dead := proof.OrkerID("removal-renamed-dead")
	in := removalInput(t, "renamed", "/bin/sh", "-c", `while :; do echo tick; sleep 1; done`)
	container, kill := startHeldOrker(ctx, t, dead, removalWorkRoot(t, "removal-renamed-old"), in)
	kill()

	removalLife(ctx, t, proof.OrkerID("removal-renamed-new"), removalWorkRoot(t, "removal-renamed-new"), orker.NewDockerRuntime())
	// Booted, and the old container still ticks past the boot.
	awaitTicks(ctx, t, container, 3)
	if left := containersLabelled(ctx, t, "ork.orker="+dead); len(left) != 1 || containerState(ctx, t, left[0]) != "running" {
		t.Fatalf("containers of the old identity after the new one booted: %v, want its one running container", left)
	}

	// The commands of docs/DEPLOY.md, "What removes a task's container".
	cli := containerCLI(t)
	for _, id := range containersLabelled(ctx, t, "ork.orker="+dead) {
		if out, err := exec.CommandContext(ctx, cli, "rm", "--force", "--volumes", id).CombinedOutput(); err != nil {
			t.Fatalf("the documented removal failed: %v: %s", err, out)
		}
	}
	if left := containersLabelled(ctx, t, "ork.orker="+dead); len(left) != 0 {
		t.Fatalf("the documented removal left: %v", left)
	}
}

// TestIntegration_ContainerRemoval_AnOrkerThatNeverReturnsLeavesAnExitedContainer:
// no Orker returns, so a workload that finishes on its own stays as an exited
// container, holding its writable layer and its log, until an operator removes
// it. The engine has nothing that would: no restart, and no removal on exit.
func TestIntegration_ContainerRemoval_AnOrkerThatNeverReturnsLeavesAnExitedContainer(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	in := removalInput(t, "abandoned", "/bin/sh", "-c", `echo tick; sleep 2; exit 0`)
	container, kill := startHeldOrker(ctx, t, proof.OrkerID("removal-abandoned"), removalWorkRoot(t, "removal-abandoned"), in)
	kill()
	awaitState(ctx, t, container, "exited")
	if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 1 {
		t.Fatalf("containers of the abandoned run: %v, want the one exited container", left)
	}
}

// gatedCLI is a stand-in for the container CLI that passes everything to the
// real one, except that while refuseRm exists it refuses every removal as a
// daemon does, and while lateCreate exists the next create is left to finish
// in the background two seconds after the CLI that asked for it hung.
//
// The late create drops its --mount flags. A daemon that finishes late had
// checked them when it took the request, while the attempt's workspace still
// existed; a real create two seconds later, after the attempt removed it, would
// be refused for that, so nothing would be left to observe.
type gatedCLI struct {
	path       string
	refuseRm   string
	lateCreate string
	asked      string
}

func newGatedCLI(t *testing.T) *gatedCLI {
	t.Helper()
	realCLI := containerCLI(t)
	dir := t.TempDir()
	g := &gatedCLI{
		path:       filepath.Join(dir, "docker"),
		refuseRm:   filepath.Join(dir, "refuse-rm"),
		lateCreate: filepath.Join(dir, "late-create"),
		asked:      filepath.Join(dir, "create-asked"),
	}
	script := `#!/bin/sh
if [ "$1" = rm ] && [ -e '` + g.refuseRm + `' ]; then
  echo "Error response from daemon: the test refuses this removal" >&2
  exit 1
fi
if [ "$1" = create ] && [ -e '` + g.lateCreate + `' ]; then
  rm -f '` + g.lateCreate + `'
  skip=0
  for arg in "$@"; do
    shift
    if [ "$skip" = 1 ]; then skip=0; continue; fi
    if [ "$arg" = --mount ]; then skip=1; continue; fi
    set -- "$@" "$arg"
  done
  ( sleep 2; '` + realCLI + `' "$@" ) >/dev/null 2>&1 </dev/null &
  : > '` + g.asked + `'
  exec sleep 600
fi
exec '` + realCLI + `' "$@"
`
	if err := os.WriteFile(g.path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return g
}

func arm(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestIntegration_ContainerRemoval_ARefusedRemovalIsTriedAgain is the daemon
// saying no, with no capacity declared: the attempt ends, says so, and the
// Orker keeps trying until the daemon lets it, not only until its next boot.
func TestIntegration_ContainerRemoval_ARefusedRemovalIsTriedAgain(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	gate := newGatedCLI(t)
	runtime := &orker.DockerRuntime{Bin: gate.path}
	activities := removalLife(ctx, t, proof.OrkerID("removal-refused"), removalWorkRoot(t, "removal-refused"), runtime)
	in := removalInput(t, "refused", "/bin/sh", "-c", `printf '{}' > /orkstack/output/values.json`)

	arm(t, gate.refuseRm)
	if _, err := activities.Dispatcher().Execute(ctx, in); err != nil {
		t.Fatalf("a refused removal must not fail the attempt: %v", err)
	}
	left := containersLabelled(ctx, t, "ork.run="+in.GetRunId())
	if len(left) != 1 {
		t.Fatalf("containers after the refused removal: %v, want the one the daemon kept", left)
	}
	if got := containerState(ctx, t, left[0]); got != "exited" {
		t.Errorf("the kept container is %q, want exited", got)
	}

	// Still refused: a retry fails and keeps it owed.
	activities.RetryRemovals(ctx)
	if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 1 {
		t.Fatalf("containers while the daemon still refuses: %v", left)
	}

	if err := os.Remove(gate.refuseRm); err != nil {
		t.Fatal(err)
	}
	activities.RetryRemovals(ctx)
	if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 0 {
		t.Fatalf("the retry after the daemon relented left: %v", left)
	}
}

// TestIntegration_ContainerRemoval_ABootReapTheDaemonRefusesIsTriedAgain is the
// same at boot: an orphan the daemon would not let go is retried while this
// Orker runs, capacity declared or not.
func TestIntegration_ContainerRemoval_ABootReapTheDaemonRefusesIsTriedAgain(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	id := proof.OrkerID("removal-boot-refused")
	workRoot := removalWorkRoot(t, "removal-boot-refused")
	in := removalInput(t, "boot-refused", "/bin/sh", "-c", `while :; do echo tick; sleep 1; done`)
	_, kill := startHeldOrker(ctx, t, id, workRoot, in)
	kill()

	gate := newGatedCLI(t)
	arm(t, gate.refuseRm)
	activities := removalLife(ctx, t, id, workRoot, &orker.DockerRuntime{Bin: gate.path})
	if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 1 {
		t.Fatalf("the orphan after a boot whose reap was refused: %v, want it still there", left)
	}
	// The boot's own probe containers were refused as well, and are owed too.
	t.Logf("observed: %d containers of this identity after the refused boot", len(containersLabelled(ctx, t, "ork.orker="+id)))
	if err := os.Remove(gate.refuseRm); err != nil {
		t.Fatal(err)
	}
	activities.RetryRemovals(ctx)
	if left := containersLabelled(ctx, t, "ork.orker="+id); len(left) != 0 {
		t.Fatalf("the retry after the daemon relented left: %v", left)
	}
}

// TestIntegration_ContainerRemoval_ACreateTheDaemonFinishesLateIsRemoved: the
// CLI is gone, its answer with it, and the daemon goes on to finish the create.
// The attempt has no id to remove, but it knows the name it asked for, and a
// retry that comes after the daemon finished removes what it made.
func TestIntegration_ContainerRemoval_ACreateTheDaemonFinishesLateIsRemoved(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	gate := newGatedCLI(t)
	runtime := &orker.DockerRuntime{Bin: gate.path}
	activities := removalLife(ctx, t, proof.OrkerID("removal-late"), removalWorkRoot(t, "removal-late"), runtime)
	in := removalInput(t, "late", "/bin/sh", "-c", `exit 0`)

	arm(t, gate.lateCreate)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	done := make(chan error, 1)
	go func() {
		_, err := activities.Dispatcher().Execute(runCtx, in)
		done <- err
	}()
	for {
		if _, err := os.Stat(gate.asked); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("the attempt ended before it asked to create: %v", err)
		case <-ctx.Done():
			t.Fatalf("the create was never asked for: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	// The CLI gives up now, as one past its deadline does.
	cancelRun()
	<-done

	awaitCtx, cancelAwait := context.WithTimeout(ctx, time.Minute)
	defer cancelAwait()
	container := awaitLabelled(awaitCtx, t, "ork.run="+in.GetRunId())
	if got := containerState(ctx, t, container); got != "created" {
		t.Fatalf("the late container is %q, want created", got)
	}
	activities.RetryRemovals(ctx)
	if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 0 {
		t.Fatalf("the retry left the container the daemon finished after the CLI gave up: %v", left)
	}
}

// TestIntegration_ContainerRemoval_TakesAnAnonymousVolumeWithIt: an image that
// declares a VOLUME gets an anonymous volume from the engine, which would
// outlive a removal without --volumes on a disk nothing else cleans.
func TestIntegration_ContainerRemoval_TakesAnAnonymousVolumeWithIt(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cli := containerCLI(t)
	requireProbeImage(ctx, t, cli)
	label := "ork.run=" + removalRunID("volume")
	t.Cleanup(func() { removeByLabel(t, label) })
	out, err := exec.CommandContext(ctx, cli, "create", "--label", label, "--volume", "/data", orker.BindProbeImage, "true").Output()
	if err != nil {
		t.Fatalf("create a container with an anonymous volume: %v", err)
	}
	id := strings.TrimSpace(string(out))
	raw, err := exec.CommandContext(ctx, cli, "inspect", "--format", `{{range .Mounts}}{{.Name}} {{end}}`, id).Output()
	if err != nil {
		t.Fatal(err)
	}
	volumes := strings.Fields(string(raw))
	if len(volumes) != 1 {
		t.Fatalf("volumes of the container: %v, want one; the case proves nothing", volumes)
	}
	t.Cleanup(func() { _ = exec.Command(cli, "volume", "rm", "--force", volumes[0]).Run() })
	if err := orker.NewDockerRuntime().Remove(ctx, orker.ContainerID(id)); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := exec.CommandContext(ctx, cli, "volume", "inspect", volumes[0]).Run(); err == nil {
		t.Errorf("anonymous volume %s outlived its container", volumes[0])
	}
}

// dindImage is the daemon the restart test restarts, pinned as every image the
// tests run is. It is a daemon of the test's own because the host's is every
// other proof's: restarting that one would fail them all.
const dindImage = "docker.io/library/docker:29-dind@sha256:08a98015fce594af7c382883bea080ea206de74402a95da9dc52b6428c8cd121"

// nestedDaemon is a real dockerd in a container of this test's own, with the
// work root bound at the same path so the Orker's mounts resolve. The loop is
// its process manager: killing dockerd is a restart, on the same state. The
// outer timeout ends it even if this process is killed before its cleanup.
func nestedDaemon(ctx context.Context, t *testing.T, workRoot string, flags string) (host string, signalDaemon func(sig string)) {
	t.Helper()
	cli := containerCLI(t)
	out, err := exec.CommandContext(ctx, cli, "run", "--rm", "--detach", "--privileged",
		"--publish", "127.0.0.1::2375", "--volume", workRoot+":"+workRoot,
		"--env", "DOCKER_TLS_CERTDIR=", "--entrypoint", "timeout", dindImage,
		"-s", "KILL", "900", "sh", "-c",
		"while :; do dockerd-entrypoint.sh dockerd --host=tcp://0.0.0.0:2375 --tls=false "+flags+"; sleep 0.2; done").Output()
	if err != nil {
		infraAbsent(t, "start a nested daemon: %v", err)
	}
	id := strings.TrimSpace(string(out))
	hostEnv := os.Environ()
	t.Cleanup(func() {
		cmd := exec.Command(cli, "rm", "--force", "--volumes", id)
		cmd.Env = hostEnv
		_ = cmd.Run()
	})
	port, err := exec.CommandContext(ctx, cli, "port", id, "2375/tcp").Output()
	if err != nil {
		t.Fatalf("read the nested daemon's port: %v", err)
	}
	host = "tcp://" + strings.TrimSpace(strings.Split(string(port), "\n")[0])
	signalDaemon = func(sig string) {
		cmd := exec.CommandContext(ctx, cli, "exec", id, "pkill", "-"+sig, "-x", "dockerd")
		cmd.Env = hostEnv
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("signal the nested daemon: %v: %s", err, out)
		}
	}
	awaitNestedDaemon(ctx, t, host)
	return host, signalDaemon
}

// awaitNestedDaemon waits until the daemon at host answers.
func awaitNestedDaemon(ctx context.Context, t *testing.T, host string) {
	t.Helper()
	for {
		cmd := exec.CommandContext(ctx, containerCLI(t), "version", "--format", "{{.Server.Version}}")
		cmd.Env = append(os.Environ(), "DOCKER_HOST="+host)
		if cmd.Run() == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the nested daemon at %s never answered: %v", host, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestIntegration_ContainerRemoval_ADaemonRestartFailsTheAttemptAndItsContainerIsRemoved
// restarts a real daemon under a running task, cleanly (what `systemctl restart
// docker` sends) and by a crash, and under live-restore, where the workload goes
// on running with nothing watching it. The engine restarts no container, the
// attempt fails explicitly, and the container it could not remove while the
// daemon was away is removed once the daemon is back.
func TestIntegration_ContainerRemoval_ADaemonRestartFailsTheAttemptAndItsContainerIsRemoved(t *testing.T) {
	requireDocker(t)
	if onPodman(t) {
		t.Log("Podman has no daemon to restart: its API service holds no containers, and each container has a monitor of its own")
		return
	}
	const ticking = `while :; do echo tick; sleep 1; done`
	for _, tt := range []struct {
		name, signal, flags string
		// command is the workload; empty is one with no handler of its own.
		command string
		// wantReason is how the attempt must end. The two reasons of a daemon
		// that went away are the two ways it could: it stopped the container
		// and said so, or it was gone before it could say anything.
		wantReason string
	}{
		{name: "term", signal: "TERM", wantReason: orker.ReasonContainerStoppedByRuntime},
		// A workload's handler decides the number and changes nothing: the daemon
		// is why it ended, and valid-looking output is not a result.
		{
			name: "term-handler-exits-143", signal: "TERM", wantReason: orker.ReasonContainerStoppedByRuntime,
			command: `trap 'exit 143' TERM; ` + ticking,
		},
		{
			name: "term-handler-exits-0", signal: "TERM", wantReason: orker.ReasonContainerStoppedByRuntime,
			command: `trap 'printf "{}" > /orkstack/output/values.json; exit 0' TERM; ` + ticking,
		},
		{
			name: "term-ignored", signal: "TERM", wantReason: orker.ReasonContainerStoppedByRuntime,
			command: `trap "" TERM; ` + ticking,
		},
		{name: "kill", signal: "KILL", wantReason: orker.ReasonRuntimeRefused},
		{name: "live-restore", signal: "TERM", flags: "--live-restore", wantReason: orker.ReasonRuntimeRefused},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			workRoot := removalWorkRoot(t, "removal-restart-"+tt.name)
			host, signalDaemon := nestedDaemon(ctx, t, workRoot, tt.flags)
			// The Orker and this test's own reads now reach the nested daemon.
			t.Setenv("DOCKER_HOST", host)
			activities := removalLife(ctx, t, proof.OrkerID("removal-restart-"+tt.name), workRoot, orker.NewDockerRuntime())
			command := tt.command
			if command == "" {
				command = ticking
			}
			in := baseInput(t, "restart", "/bin/sh", "-c", command)
			in.RunId = removalRunID("restart-" + tt.name)
			in.CommitUri = "mem://commits/" + in.GetRunId() + "/commit.json"
			in.IdempotencyKey = "default/" + in.GetRunId() + "/restart"

			done := make(chan error, 1)
			go func() {
				_, err := activities.Dispatcher().Execute(ctx, in)
				done <- err
			}()
			awaitCtx, cancelAwait := context.WithTimeout(ctx, 2*time.Minute)
			defer cancelAwait()
			container := awaitLabelled(awaitCtx, t, "ork.run="+in.GetRunId())
			awaitTicks(awaitCtx, t, container, 1)

			restarted := time.Now()
			signalDaemon(tt.signal)
			var err error
			select {
			case err = <-done:
			case <-ctx.Done():
				t.Fatalf("the attempt never ended after the daemon restarted: %v", ctx.Err())
			}
			if err == nil {
				t.Error("the attempt succeeded across a daemon restart")
			}
			t.Logf("observed: the attempt ended %s after the %s with: %v", time.Since(restarted).Round(time.Millisecond), tt.signal, err)
			if err != nil && !strings.Contains(err.Error(), tt.wantReason) {
				t.Errorf("the attempt ended %v, want %s", err, tt.wantReason)
			}

			awaitNestedDaemon(ctx, t, host)
			left := containersLabelled(ctx, t, "ork.run="+in.GetRunId())
			t.Logf("observed: %d containers once the daemon was back, %s after the %s", len(left), time.Since(restarted).Round(time.Millisecond), tt.signal)
			for _, id := range left {
				t.Logf("observed: container %s is %s", id, containerState(ctx, t, id))
			}
			activities.RetryRemovals(ctx)
			if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 0 {
				t.Fatalf("containers left after the daemon came back and a retry ran: %v", left)
			}
		})
	}
}
