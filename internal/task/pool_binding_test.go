package task_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/task"
)

// TestResolvePoolBinding_ResolvesTheFiveWaysAProcessCanBeToldWhereItsWorkLives
// is the whole of the ORK_ORKER_POOL / ORK_ORKER_QUEUE contract, in one table.
//
// The two variables answer different questions. ORK_ORKER_POOL is OrkStack's:
// which pool does this process serve. ORK_ORKER_QUEUE is the operator's escape
// hatch to the raw execution channel underneath, which the test harnesses use
// to isolate themselves (AGENTS.md §6). Both binaries resolve them the same
// way, or an Orker polls one place while the engine dispatches to another and
// the run strands in RUNNING.
func TestResolvePoolBinding_ResolvesTheFiveWaysAProcessCanBeToldWhereItsWorkLives(t *testing.T) {
	poolQueue, err := task.QueueForPool("gpu")
	if err != nil {
		t.Fatalf("QueueForPool(gpu): %v", err)
	}

	for _, tc := range []struct {
		name      string
		pool      string
		rawQueue  string
		wantPool  string
		wantQueue string
		wantRaw   bool
	}{{
		// Zero configuration must behave exactly as it did before pools
		// existed: `make run-ork` + `make run-orker` and nothing else.
		name:      "neither",
		wantPool:  task.DefaultPool,
		wantQueue: task.DefaultQueue,
	}, {
		name:      "pool only",
		pool:      "gpu",
		wantPool:  "gpu",
		wantQueue: poolQueue,
	}, {
		// A raw channel this package did not generate is nobody's pool, so the
		// process reports the built-in default and flags that its channel was
		// set by hand.
		name:      "raw override only",
		rawQueue:  "ork-tasks-m7-9",
		wantPool:  task.DefaultPool,
		wantQueue: "ork-tasks-m7-9",
		wantRaw:   true,
	}, {
		// A raw override that IS a pool's image resolves to that pool: the
		// operator wrote the same thing twice.
		name:      "raw override that is a pool's image",
		rawQueue:  poolQueue,
		wantPool:  "gpu",
		wantQueue: poolQueue,
		wantRaw:   true,
	}, {
		name:      "both, agreeing",
		pool:      "gpu",
		rawQueue:  poolQueue,
		wantPool:  "gpu",
		wantQueue: poolQueue,
		wantRaw:   true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			binding, err := task.ResolvePoolBinding(tc.pool, tc.rawQueue)
			if err != nil {
				t.Fatalf("ResolvePoolBinding(%q, %q): %v", tc.pool, tc.rawQueue, err)
			}
			if binding.Pool != tc.wantPool {
				t.Errorf("pool = %q, want %q", binding.Pool, tc.wantPool)
			}
			if binding.Queue != tc.wantQueue {
				t.Errorf("queue = %q, want %q", binding.Queue, tc.wantQueue)
			}
			if binding.RawOverride != tc.wantRaw {
				t.Errorf("raw override = %v, want %v", binding.RawOverride, tc.wantRaw)
			}
		})
	}
}

// TestResolvePoolBinding_RefusesAPoolAndARawChannelThatDisagree is the failure
// this pair of variables exists to make impossible.
//
// Set the pool on one binary and the raw channel on the other and the run
// strands in RUNNING with nothing to say why (AGENTS.md §6). Set both on one
// binary and disagree, and the process would have to silently pick a winner.
// It refuses at boot instead, while the error can still name both variables.
func TestResolvePoolBinding_RefusesAPoolAndARawChannelThatDisagree(t *testing.T) {
	binding, err := task.ResolvePoolBinding("gpu", "ork-tasks-m7-9")
	if !errors.Is(err, task.ErrPoolBindingConflict) {
		t.Fatalf("ResolvePoolBinding(gpu, ork-tasks-m7-9) = %+v, %v; want ErrPoolBindingConflict",
			binding, err)
	}
	for _, want := range []string{"ORK_ORKER_POOL", "ORK_ORKER_QUEUE", "gpu", "ork-tasks-m7-9"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q; an operator cannot fix what the error will not name",
				err, want)
		}
	}
}

// TestResolvePoolBinding_RefusesAPoolNameItCannotMap keeps a typo from booting
// a worker onto a channel nobody dispatches to.
func TestResolvePoolBinding_RefusesAPoolNameItCannotMap(t *testing.T) {
	if _, err := task.ResolvePoolBinding("GPU Large", ""); !errors.Is(err, task.ErrInvalidPool) {
		t.Fatalf("ResolvePoolBinding(%q, \"\") error = %v, want ErrInvalidPool", "GPU Large", err)
	}
}
