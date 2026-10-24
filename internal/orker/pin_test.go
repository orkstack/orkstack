package orker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// pinSpecRuntime is a ContainerRuntime that records the ContainerSpec it was
// asked to create and the task.json the Orker had already sealed by then, and
// then behaves like a workload that succeeded.
//
// Recording task.json at CREATE is the only place it can be read: the input
// tree is sealed just before this call and the whole attempt directory is
// removed on every exit path, so a test that looked afterwards would find
// nothing.
type pinSpecRuntime struct {
	mu       sync.Mutex
	specs    []ContainerSpec
	taskJSON []map[string]any
}

func (r *pinSpecRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{Platform: "linux/amd64", Delivery: DeliveryRegistry}, nil
}

func (r *pinSpecRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no embedded layout in this test")
}

func (r *pinSpecRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	var doc map[string]any
	for _, m := range spec.Mounts {
		switch m.Destination {
		case ContainerInput:
			raw, err := os.ReadFile(filepath.Join(m.Source, "task.json"))
			if err != nil {
				return "", err
			}
			if err := json.Unmarshal(raw, &doc); err != nil {
				return "", err
			}
		case ContainerOutput:
			// A workload with no result writes {}. Without it the attempt fails
			// at VALUES_MISSING and never reaches the assertions.
			if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(`{}`), 0o644); err != nil {
				return "", err
			}
		}
	}
	r.mu.Lock()
	r.taskJSON = append(r.taskJSON, doc)
	r.mu.Unlock()
	return "fake-container", nil
}

func (r *pinSpecRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error { return nil }
func (r *pinSpecRuntime) Signal(context.Context, ContainerID, Signal) error              { return nil }

func (r *pinSpecRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: 0}, nil
}

func (r *pinSpecRuntime) Remove(context.Context, ContainerID) error { return nil }
func (r *pinSpecRuntime) Reap(context.Context, string) (int, error) { return 0, nil }

func (r *pinSpecRuntime) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{Name: "pin-spec-recorder"}, nil
}

func (r *pinSpecRuntime) recorded(t *testing.T, i int) (ContainerSpec, map[string]any) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.specs) {
		t.Fatalf("attempt %d was never created; only %d container(s) were", i, len(r.specs))
	}
	return r.specs[i], r.taskJSON[i]
}

// newPinDispatcher builds a dispatcher over pinSpecRuntime with a work root the
// test owns, so the pin directory it creates can be inspected after the attempt
// that created it is gone — which is the point of the pin directory.
func newPinDispatcher(t *testing.T, runtime *pinSpecRuntime, workRoot string) *OCIDispatcher {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local artifact store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &OCIDispatcher{
		Runtime:  runtime,
		Store:    store,
		Refs:     artifacts.MemoryRefPredictor{},
		Policy:   DefaultSecurityPolicy(),
		WorkRoot: workRoot,
		OrkerID:  "orker-pin-test",
		BuildID:  "test",
	}
}

// pinnedInput is baseExportInput with a pin context attached, and with the run
// id forced to one value.
//
// Members of a pin are tasks of ONE run, and the run id is part of what names
// the pin's directory — baseExportInput derives a run id per task, which would
// make two members of one pin look like two runs and defeat the sharing this
// file measures.
func pinnedInput(runID, taskID, pinName, instanceID string) *orkstackv1.ExecuteTaskInput {
	in := baseExportInput(taskID)
	in.RunId = runID
	in.IdempotencyKey = "default/" + runID + "/" + taskID
	in.Pin = &orkstackv1.PinContext{Name: pinName, InstanceId: instanceID, TimeoutSeconds: 3600}
	return in
}

// mountFor finds one destination in a rendered mount set.
func mountFor(mounts []Mount, destination string) (Mount, bool) {
	for _, m := range mounts {
		if m.Destination == destination {
			return m, true
		}
	}
	return Mount{}, false
}

// TestExecute_AnUnpinnedAttemptIsUNCHANGED is the backward-compatibility proof
// at the dispatcher level, and it is deliberately an assertion about absence.
//
// A document with no pin must dispatch exactly as it did before pins
// existed. So an unpinned attempt must render the SAME four
// mounts, carry NO ork.pin label, write NO task.pin and NO paths.shared, and
// leave no pin directory behind. If any of those four changed, every unpinned
// deployment on earth changed with it.
func TestExecute_AnUnpinnedAttemptIsUNCHANGED(t *testing.T) {
	workRoot := t.TempDir()
	runtime := &pinSpecRuntime{}
	dispatcher := newPinDispatcher(t, runtime, workRoot)

	if _, err := dispatcher.Execute(context.Background(), baseExportInput("unpinned")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	spec, doc := runtime.recorded(t, 0)

	wantDestinations := []string{ContainerInput, ContainerSecrets, ContainerOutput, ContainerTmp}
	if len(spec.Mounts) != len(wantDestinations) {
		t.Fatalf("an unpinned attempt rendered %d mounts, want exactly %d: %+v",
			len(spec.Mounts), len(wantDestinations), spec.Mounts)
	}
	for i, want := range wantDestinations {
		if spec.Mounts[i].Destination != want {
			t.Errorf("mount %d is %q, want %q", i, spec.Mounts[i].Destination, want)
		}
	}
	if _, present := mountFor(spec.Mounts, ContainerShared); present {
		t.Error("an unpinned attempt was given /orkstack/shared")
	}
	if value, present := spec.Labels["ork.pin"]; present {
		t.Errorf("an unpinned container carries ork.pin=%q", value)
	}
	task, _ := doc["task"].(map[string]any)
	if _, present := task["pin"]; present {
		t.Errorf("an unpinned task.json carries task.pin: %v", task["pin"])
	}
	paths, _ := doc["paths"].(map[string]any)
	if _, present := paths["shared"]; present {
		t.Errorf("an unpinned task.json carries paths.shared: %v", paths["shared"])
	}
	dirs, err := pinDirectories(workRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 0 {
		t.Errorf("an unpinned attempt left pin directories behind: %v", dirs)
	}
}

// TestExecute_TwoPinnedAttemptsShareOneSharedDirectory is the core promise, at
// the level a unit test can reach it: two SEPARATE attempts carrying the same
// pin context get the same host directory bind-mounted at /orkstack/shared,
// and it outlives both of their per-attempt trees.
//
// The two attempts here are two different TASKS of one pin, which is the case
// that matters — a retry of one task would share by the same mechanism. The
// real bind mount, with real containers passing a real file, is proved by
// TestIntegration_TwoPinnedContainersShareOneDirectory.
func TestExecute_TwoPinnedAttemptsShareOneSharedDirectory(t *testing.T) {
	workRoot := t.TempDir()
	runtime := &pinSpecRuntime{}
	dispatcher := newPinDispatcher(t, runtime, workRoot)

	for _, taskID := range []string{"clone", "compile"} {
		if _, err := dispatcher.Execute(
			context.Background(), pinnedInput("ork-pin-share", taskID, "build", testInstanceID)); err != nil {
			t.Fatalf("Execute %s: %v", taskID, err)
		}
	}

	first, firstDoc := runtime.recorded(t, 0)
	second, secondDoc := runtime.recorded(t, 1)

	firstShared, ok := mountFor(first.Mounts, ContainerShared)
	if !ok {
		t.Fatalf("a pinned attempt rendered no %s mount: %+v", ContainerShared, first.Mounts)
	}
	secondShared, ok := mountFor(second.Mounts, ContainerShared)
	if !ok {
		t.Fatalf("the second pinned attempt rendered no %s mount: %+v", ContainerShared, second.Mounts)
	}
	if firstShared.Source != secondShared.Source {
		t.Errorf("two tasks of ONE pin were given two different directories:\n  %s\n  %s\n"+
			"a pin whose second task finds an empty shared/ is the silently-wrong result the "+
			"design refuses", firstShared.Source, secondShared.Source)
	}
	if firstShared.ReadOnly {
		t.Error("/orkstack/shared is read-only; the pin exists so the next task can read what " +
			"this one wrote")
	}
	// The five mounts are the four plus one, so nothing was replaced.
	if len(first.Mounts) != 5 {
		t.Errorf("a pinned attempt rendered %d mounts, want the four plus shared: %+v",
			len(first.Mounts), first.Mounts)
	}
	// The per-attempt trees are gone; the pin's directory is not. That is the
	// one difference in lifetime the whole feature is.
	if _, err := os.Stat(firstShared.Source); err != nil {
		t.Errorf("the shared directory did not outlive the attempt that created it: %v", err)
	}
	if _, err := os.Stat(first.Mounts[0].Source); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the per-attempt input tree survived the attempt (%v); pins change nothing "+
			"about per-attempt cleanup", err)
	}

	// One directory, named by the (run, pin, instance) triple.
	dirs, err := pinDirectories(workRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 {
		t.Fatalf("pin directories = %v, want exactly one for one opening of one pin", dirs)
	}
	if want := filepath.Join(dirs[0], "shared"); firstShared.Source != want {
		t.Errorf("shared mount source = %q, want %q", firstShared.Source, want)
	}

	// The label and the two task.json keys, on both attempts.
	for i, spec := range []ContainerSpec{first, second} {
		if got := spec.Labels["ork.pin"]; got != testInstanceID {
			t.Errorf("attempt %d label ork.pin = %q, want %q", i, got, testInstanceID)
		}
		if got := spec.Labels["ork.orker"]; got != "orker-pin-test" {
			t.Errorf("attempt %d lost its ork.orker label (%q); the reaper's key is unchanged", i, got)
		}
	}
	for i, doc := range []map[string]any{firstDoc, secondDoc} {
		task, _ := doc["task"].(map[string]any)
		pin, _ := task["pin"].(map[string]any)
		if pin["name"] != "build" || pin["instanceId"] != testInstanceID {
			t.Errorf("attempt %d task.pin = %v, want {build, %s}", i, pin, testInstanceID)
		}
		paths, _ := doc["paths"].(map[string]any)
		if paths["shared"] != ContainerShared {
			t.Errorf("attempt %d paths.shared = %v, want %q", i, paths["shared"], ContainerShared)
		}
	}
}

// TestExecute_ARefusedPinInstanceIDFailsTheAttempt: a pin context the Orker
// cannot trust fails the attempt as a workspace failure, which is what it is —
// the Orker could not build the filesystem the contract promises — rather than
// running the container against an unpinned tree and committing a wrong result.
func TestExecute_ARefusedPinInstanceIDFailsTheAttempt(t *testing.T) {
	workRoot := t.TempDir()
	runtime := &pinSpecRuntime{}
	dispatcher := newPinDispatcher(t, runtime, workRoot)

	_, err := dispatcher.Execute(
		context.Background(),
		pinnedInput("ork-pin-refused", "compile", "build", "../../../../../../tmp/pwn-aaaaaaaaa"))
	if err == nil {
		t.Fatal("a malformed pin instance id ran the container anyway")
	}
	runtime.mu.Lock()
	created := len(runtime.specs)
	runtime.mu.Unlock()
	if created != 0 {
		t.Errorf("%d container(s) were created for a refused pin", created)
	}
	dirs, err := pinDirectories(workRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 0 {
		t.Errorf("a refused pin instance id still created %v", dirs)
	}
}

// TestDescribeOrker_ReportsTheConfiguredPinCapacity: the number is what
// submit reads to decide whether a pinned task's pool could ever open its pin,
// so a zero has to travel as a zero: it is the difference between "refused at
// submit with a message naming the setting" and "waits five minutes and then
// fails with PIN_UNAVAILABLE".
func TestDescribeOrker_ReportsTheConfiguredPinCapacity(t *testing.T) {
	for name, want := range map[string]int32{"no pins": 0, "a pin host": 4} {
		t.Run(name, func(t *testing.T) {
			store, err := artifacts.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			activities := NewActivitiesWithOptions(store, Options{
				Runtime:  &reapRecorder{},
				WorkRoot: t.TempDir(),
				Pool:     "gpu",
				MaxPins:  want,
			})
			result, err := activities.DescribeOrker(context.Background())
			if err != nil {
				t.Fatalf("DescribeOrker: %v", err)
			}
			if result.GetMaxConcurrentPins() != want {
				t.Errorf("max_concurrent_pins = %d, want %d", result.GetMaxConcurrentPins(), want)
			}
			if result.GetPool() != "gpu" {
				t.Errorf("the pin field disturbed pool reporting: %q", result.GetPool())
			}
		})
	}
}

// TestBoot_ReapsPinDirectoriesWhetherOrNotThisOrkerHostsPins is the boot reap
// (docs/OCI_ACTIVITY_CONTRACT.md §6.4), and the "whether or not" is the point.
//
// A pin directory left by a PREVIOUS life that did host pins must still be
// removed by a boot that does not, or turning ORK_ORKER_MAX_PINS back to 0
// would strand the last run's shared trees with nothing left that would look
// for them. The reap is also unconditional in the stronger sense that it reads
// no lease: a fresh session-resource id is minted per worker start, so every
// pin directory found at boot is dead by construction.
func TestBoot_ReapsPinDirectoriesWhetherOrNotThisOrkerHostsPins(t *testing.T) {
	for name, maxPins := range map[string]int32{"a boot that hosts no pins": 0, "a pin host": 2} {
		t.Run(name, func(t *testing.T) {
			workRoot := t.TempDir()
			// A predecessor's pin directory, complete with the 0500 subdirectory
			// a workload can genuinely leave under the 1777 shared/ tree — which
			// is why the reap needs the chmod-then-RemoveAll walk and not a
			// plain RemoveAll.
			stale, err := AcquirePin(workRoot, "ork-previous-life", "build", testInstanceID, 3600)
			if err != nil {
				t.Fatalf("AcquirePin: %v", err)
			}
			locked := filepath.Join(stale.Shared, "locked")
			if err := os.Mkdir(locked, 0o700); err != nil {
				t.Fatal(err)
			}
			mustWrite(t, filepath.Join(locked, "build.log"), "from a dead orker")
			if err := os.Chmod(locked, 0o500); err != nil {
				t.Fatal(err)
			}

			store, err := artifacts.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			replacement := NewActivitiesWithOptions(store, Options{
				Runtime: &reapRecorder{}, WorkRoot: workRoot, MaxPins: maxPins,
			})
			if err := replacement.Boot(context.Background()); err != nil {
				t.Fatalf("Boot: %v", err)
			}
			t.Cleanup(replacement.Close)

			if _, err := os.Stat(stale.Root); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a dead predecessor's pin directory survived boot with max_pins=%d: %v",
					maxPins, err)
			}
		})
	}
}

// TestReleasePinActivity_RemovesTheRightDirectoryAndIsIdempotent covers the
// ReleasePin activity (docs/OCI_ACTIVITY_CONTRACT.md §6.4).
//
// The engine schedules this on the pin's own session, so it can only ever land
// on the host that owns the directory; what this asserts is the other three
// properties — it removes the right one, an absent one is a success, and a
// malformed instance id is refused before anything is touched.
func TestReleasePinActivity_RemovesTheRightDirectoryAndIsIdempotent(t *testing.T) {
	workRoot := t.TempDir()
	target, err := AcquirePin(workRoot, "ork-1", "build", testInstanceID, 3600)
	if err != nil {
		t.Fatalf("AcquirePin: %v", err)
	}
	const siblingInstance = "bbbbbbbb-1111-2222-3333-444444444444"
	bystander, err := AcquirePin(workRoot, "ork-1", "test", siblingInstance, 3600)
	if err != nil {
		t.Fatalf("AcquirePin: %v", err)
	}

	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	activities := NewActivitiesWithOptions(store, Options{
		Runtime: &reapRecorder{}, WorkRoot: workRoot, MaxPins: 1,
	})

	release := func(instanceID string) error {
		return activities.ReleasePin(context.Background(), &orkstackv1.ReleasePinInput{
			RunId:     "ork-1",
			Namespace: "default",
			Pin:       &orkstackv1.PinContext{Name: "build", InstanceId: instanceID},
		})
	}

	if err := release(testInstanceID); err != nil {
		t.Fatalf("ReleasePin: %v", err)
	}
	if _, err := os.Stat(target.Root); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the pin's directory survived ReleasePin: %v", err)
	}
	if _, err := os.Stat(bystander.Root); err != nil {
		t.Errorf("ReleasePin removed another pin's directory (%v); the (run, pin, instance) "+
			"triple is what names the one it owns", err)
	}

	// Idempotent: release genuinely races a replacement Orker's boot reap and
	// the lease sweep, so an absent directory is the expected outcome and not
	// an error to surface to a coroutine that would only log it.
	if err := release(testInstanceID); err != nil {
		t.Fatalf("a repeated ReleasePin failed: %v", err)
	}

	// And a malformed id is refused rather than rewritten, with the bystander
	// still there to prove nothing was removed on the way to the refusal.
	if err := release("../../../../../../tmp/pwned-aaaaaaaa"); err == nil {
		t.Error("ReleasePin accepted an instance id shaped like a path traversal")
	}
	if _, err := os.Stat(bystander.Root); err != nil {
		t.Errorf("a refused ReleasePin still removed something: %v", err)
	}
}

// TestSweepPinLeases_StopsWhenTheProcessDoes: the sweeper is a goroutine
// cmd/orker starts on the process's own signal context and never joins, so it
// must return on cancellation rather than outlive the shutdown that cancelled
// it.
func TestSweepPinLeases_StopsWhenTheProcessDoes(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	activities := NewActivitiesWithOptions(store, Options{
		Runtime: &reapRecorder{}, WorkRoot: t.TempDir(), MaxPins: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		activities.SweepPinLeases(ctx)
		close(stopped)
	}()
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatalf("SweepPinLeases did not return within 5s of cancellation; its ticker is %v, so "+
			"a loop that only checked the clock would hang shutdown for that long",
			PinLeaseSweepInterval)
	}
}
