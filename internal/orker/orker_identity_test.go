package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// reapRecorder is a ContainerRuntime that only remembers what it was asked to
// reap. Boot's other steps do not need a real daemon, so the identity question
// can be asked without one.
type reapRecorder struct {
	// labelled is the set of orker ids whose containers are still alive on
	// this host, standing in for `docker ps --filter label=ork.orker=<id>`.
	labelled map[string]int
	asked    []string
}

func (r *reapRecorder) Pull(context.Context, string) (PullInfo, error) { return PullInfo{}, nil }
func (r *reapRecorder) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, nil
}
func (r *reapRecorder) Create(context.Context, ContainerSpec) (ContainerID, error) { return "", nil }
func (r *reapRecorder) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	return nil
}
func (r *reapRecorder) Signal(context.Context, ContainerID, Signal) error { return nil }
func (r *reapRecorder) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{}, nil
}
func (r *reapRecorder) Remove(context.Context, ContainerID) error { return nil }
func (r *reapRecorder) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{Name: "recorder"}, nil
}
func (r *reapRecorder) Reap(_ context.Context, orkerID string) (int, error) {
	r.asked = append(r.asked, orkerID)
	removed := r.labelled[orkerID]
	delete(r.labelled, orkerID)
	return removed, nil
}

// newLife builds an Activities the way cmd/orker does — WITHOUT an explicit
// OrkerID, so the default identity path is the thing under test.
func newLife(t *testing.T, workRoot string, runtime ContainerRuntime) *Activities {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewActivitiesWithOptions(store, Options{Runtime: runtime, WorkRoot: workRoot})
}

// TestDefaultOrkerID_SurvivesARestartSoTheReaperCanMatch is the regression for
// the defect where the default identity embedded os.Getpid().
//
// The reaper's only key is the Orker's identity: Boot passes it to Reap, which
// filters `label=ork.orker=<id>`. A pid-derived identity therefore changed on
// every restart, so a replacement Orker matched NONE of its dead predecessor's
// containers and force-removed nothing — precisely the scenario §5.5 measure 2
// exists for ("so an Orker can recognize its *own* previous life's
// containers").
func TestDefaultOrkerID_SurvivesARestartSoTheReaperCanMatch(t *testing.T) {
	workRoot := t.TempDir()

	// Life one: labels a container, then "crashes" without stopping it.
	first := newLife(t, workRoot, &reapRecorder{labelled: map[string]int{}})
	orphanOwner := first.Dispatcher().OrkerID
	if orphanOwner == "" {
		t.Fatal("an Orker with no identity cannot label or reap anything")
	}
	if strings.Contains(orphanOwner, strconv.Itoa(os.Getpid())) {
		t.Errorf("default identity %q embeds this process's pid; a restart would "+
			"mint a different identity and the reaper could never match", orphanOwner)
	}

	// Life two: a replacement process on the same host, same work root, no
	// explicit ORK_ORKER_ID — exactly what every documented way of running an
	// Orker produces.
	runtime := &reapRecorder{labelled: map[string]int{orphanOwner: 1}}
	second := newLife(t, workRoot, runtime)

	if got := second.Dispatcher().OrkerID; got != orphanOwner {
		t.Fatalf("identity changed across a restart: life one %q, life two %q; "+
			"the replacement cannot recognise its predecessor's containers", orphanOwner, got)
	}
	if err := second.Boot(context.Background()); err != nil {
		t.Fatalf("replacement Boot: %v", err)
	}
	if len(runtime.labelled) != 0 {
		t.Fatalf("the predecessor's container survived the replacement's boot: %v; "+
			"reap was asked for %v", runtime.labelled, runtime.asked)
	}

	// Determinism is what makes the identity survive a real restart, where the
	// replacement is a different process. defaultOrkerID must therefore be a
	// pure function of the host and the work root, reading nothing
	// process-specific: assert the exact value rather than only self-agreement,
	// which two calls in ONE process would satisfy even for a pid-derived id.
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "orker"
	}
	abs, err := filepath.Abs(workRoot)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(abs))
	want := safeNameComponent(host) + "-" + hex.EncodeToString(sum[:4])
	if orphanOwner != want {
		t.Fatalf("default identity %q is not the pure function of host and work root "+
			"%q; anything else cannot be reproduced by a replacement process",
			orphanOwner, want)
	}
}

// TestDefaultOrkerID_DistinguishesTwoOrkersOnOneHost is the other half of the
// constraint. Making the identity stable must not make two Orkers deliberately
// run side by side reap each other's LIVE work, which would be a worse defect
// than the one being fixed.
func TestDefaultOrkerID_DistinguishesTwoOrkersOnOneHost(t *testing.T) {
	a := newLife(t, t.TempDir(), &reapRecorder{labelled: map[string]int{}})
	b := newLife(t, t.TempDir(), &reapRecorder{labelled: map[string]int{}})

	if a.Dispatcher().OrkerID == b.Dispatcher().OrkerID {
		t.Fatalf("two Orkers with different work roots share identity %q; "+
			"each would reap the other's running containers at boot",
			a.Dispatcher().OrkerID)
	}

	// The live one holds its containers; the other's boot must not touch them.
	live := &reapRecorder{labelled: map[string]int{a.Dispatcher().OrkerID: 2}}
	other := newLife(t, t.TempDir(), live)
	if err := other.Boot(context.Background()); err != nil {
		t.Fatalf("Boot: %v", err)
	}
	if live.labelled[a.Dispatcher().OrkerID] != 2 {
		t.Fatalf("a peer Orker's boot reaped a live Orker's containers: %v", live.labelled)
	}
}

// TestBoot_RefusesASecondLiveOrkerOnOneIdentity closes the hole that making the
// identity stable would otherwise open: two CONCURRENT processes sharing a work
// root now share an identity, so the second one's boot reap would destroy the
// first one's running containers. Boot takes an exclusive lock on the identity
// and refuses instead.
func TestBoot_RefusesASecondLiveOrkerOnOneIdentity(t *testing.T) {
	workRoot := t.TempDir()
	runtime := &reapRecorder{labelled: map[string]int{}}

	live := newLife(t, workRoot, runtime)
	if err := live.Boot(context.Background()); err != nil {
		t.Fatalf("first Boot: %v", err)
	}

	second := newLife(t, workRoot, &reapRecorder{labelled: map[string]int{live.Dispatcher().OrkerID: 3}})
	err := second.Boot(context.Background())
	if err == nil {
		t.Fatal("a second live Orker on one identity started anyway; its reap would " +
			"have force-removed the first Orker's running containers")
	}
	if !strings.Contains(err.Error(), OrkerIDVar) && !strings.Contains(err.Error(), WorkRootVar) {
		t.Errorf("refusal %q names neither %s nor %s, so an operator is not told "+
			"how to run two Orkers on one host", err.Error(), OrkerIDVar, WorkRootVar)
	}

	// Releasing the identity lets a genuine replacement take over.
	live.Close()
	replacement := newLife(t, workRoot, &reapRecorder{labelled: map[string]int{live.Dispatcher().OrkerID: 3}})
	if err := replacement.Boot(context.Background()); err != nil {
		t.Fatalf("replacement Boot after the predecessor released: %v", err)
	}
	replacement.Close()
}
