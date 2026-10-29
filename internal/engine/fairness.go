package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Fair dispatch across namespaces (docs/DEPLOY.md "Fair dispatch across
// namespaces") is Temporal's task-queue fairness, keyed by namespace. Every
// task activity of a run carries its namespace's fairness key and weight, both
// decided at submit and pinned in the plan (RunPlan.Fairness), so a namespace
// whose weight changes moves only the runs submitted after it. It is never an
// OrkStack queue: the cluster orders what waits on one channel, and only when
// its operator enabled fairness (matching.enableFairness); without that the
// keys are ignored and dispatch is first in, first out per channel.

// FairnessKeysChange is the workflow.GetVersion change id of the fairness key
// and weight a run's plan carries. Asked lazily, where a run first schedules a
// task activity (priorityFor), and only by a run whose plan carries a
// selection: a history recorded before the gate, or by a build that ignored
// the plan field, replays its DefaultVersion branch, which sets no priority.
const FairnessKeysChange = "fairness-keys"

// maxFairnessKey is Temporal's bound on a fairness key, in bytes.
const maxFairnessKey = 64

// FairnessSelection is one run's fairness key and weight. Weight 0 is the
// cluster's default weight of 1.
type FairnessSelection struct {
	Key    string  `json:"key"`
	Weight float32 `json:"weight,omitempty"`
}

// FairnessKeyFor is a namespace's fairness key: its name when that fits in 64
// bytes, and otherwise a readable prefix of it and a SHA-256 suffix, so two
// long names never share a key and every key still says whose it is.
func FairnessKeyFor(namespace string) string {
	if len(namespace) <= maxFairnessKey {
		return namespace
	}
	sum := sha256.Sum256([]byte(namespace))
	suffix := hex.EncodeToString(sum[:])[:16]
	return namespace[:maxFairnessKey-len(suffix)-1] + "-" + suffix
}

// fairnessFor is the selection SubmitRun pins for a run of ns.
func fairnessFor(ns *orkstackv1.Namespace) *FairnessSelection {
	return &FairnessSelection{Key: FairnessKeyFor(namespaceName(ns)), Weight: float32(ns.GetFairShareWeight())}
}

// fairnessKeysGateDisabledForTest is a TEST-ONLY switch, the precedent of
// pinCreationTimeoutGateDisabledForTest: when set, priorityFor takes the
// DefaultVersion branch WITHOUT asking its version, so a recorder can write a
// history in the shape of the builds before the gate. Never set in production
// code.
var fairnessKeysGateDisabledForTest atomic.Bool

// priorityFor is the priority a task activity of this run is scheduled with:
// the plan's fairness key and weight once the gate says so, and none for a run
// whose plan carries no selection or whose history predates the gate.
func (s *runState) priorityFor(ctx workflow.Context) temporal.Priority {
	fairness := s.plan.Fairness
	if fairness == nil || fairnessKeysGateDisabledForTest.Load() ||
		s.version(ctx, FairnessKeysChange, 1) < 1 {
		return temporal.Priority{}
	}
	return temporal.Priority{FairnessKey: fairness.Key, FairnessWeight: fairness.Weight}
}
