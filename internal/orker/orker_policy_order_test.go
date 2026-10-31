package orker

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/interceptor"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The per-Orker policy checks — this Orker's cap, its secret schemes, its
// secret floor and whether its runtime can run a task as the Orker's own
// user — run after the retry-start commit check and, for a cached task, after
// the memo step (docs/OCI_ACTIVITY_CONTRACT.md §7.1). A
// result already committed, or a memo that hits, was produced under some
// Orker's policy and starts no container here, so a narrower Orker of the
// same pool must hand it back rather than fail the run. A miss is still
// refused, before any secret is resolved, any image is pulled or any
// config.json is written.

// orderSecretVar is the one secret every input of these tests names.
const orderSecretVar = "ORK_TEST_POLICY_ORDER_TOKEN"

// narrowerOrker is one per-Orker setting a second Orker of a pool may hold
// more narrowly than the Orker that ran the first attempt.
type narrowerOrker struct {
	name   string
	reason string
	// ask makes the input need what the narrower setting refuses.
	ask func(in *orkstackv1.ExecuteTaskInput)
	// narrow turns a permissive dispatcher into the narrower Orker.
	narrow func(d *OCIDispatcher)
}

var narrowerOrkers = []narrowerOrker{
	{
		name:   "a smaller cpu cap",
		reason: ReasonResourceRefused,
		ask:    func(in *orkstackv1.ExecuteTaskInput) { in.Resources = &orkstackv1.ResourceRequest{Cpu: "2"} },
		narrow: func(d *OCIDispatcher) { d.Policy.MaxCPU, d.Policy.DefaultCPU = "1", "1" },
	},
	{
		name:   "a smaller memory cap",
		reason: ReasonResourceRefused,
		ask:    func(in *orkstackv1.ExecuteTaskInput) { in.Resources = &orkstackv1.ResourceRequest{Memory: "2Gi"} },
		narrow: func(d *OCIDispatcher) { d.Policy.MaxMemory, d.Policy.DefaultMemory = "1Gi", "512Mi" },
	},
	{
		name:   "a secret scheme it does not resolve",
		reason: ReasonSecretSchemeForbid,
		ask:    func(*orkstackv1.ExecuteTaskInput) {},
		narrow: func(d *OCIDispatcher) { d.Policy.SecretSchemes = []string{"file"} },
	},
	{
		name:   "a secret floor that names other sources",
		reason: ReasonSecretNotPermitted,
		ask:    func(*orkstackv1.ExecuteTaskInput) {},
		narrow: func(d *OCIDispatcher) { d.Policy.SecretAllow = []string{"env:ORK_TEST_POLICY_ORDER_OTHER_*"} },
	},
	{
		name:   "a runtime where running as the Orker does not land",
		reason: ReasonRunAsOrkerUnavailable,
		ask:    func(in *orkstackv1.ExecuteTaskInput) { in.RunAsOrkerUser = true },
		narrow: func(d *OCIDispatcher) {
			d.runAsOrker.Store(&runAsOrkerFinding{err: fmt.Errorf("%w: test", ErrRunAsOrkerUnavailable)})
		},
	},
	{
		name:   "a forced run as the Orker that does not land",
		reason: ReasonRunAsOrkerUnavailable,
		ask:    func(*orkstackv1.ExecuteTaskInput) {},
		narrow: func(d *OCIDispatcher) {
			d.Policy.ForceUser = true
			d.runAsOrker.Store(&runAsOrkerFinding{err: fmt.Errorf("%w: test", ErrRunAsOrkerUnavailable)})
		},
	},
}

// policyOrderHarness is a permissive Orker over the cache harness's stores:
// its floor names the one secret the inputs carry, so the first attempt
// resolves it.
func policyOrderHarness(t *testing.T) *cacheHarness {
	t.Helper()
	t.Setenv(orderSecretVar, "order-secret-value")
	h := newCacheHarness(t)
	h.dispatcher.Policy.SecretAllow = []string{"env:" + orderSecretVar}
	return h
}

// policyOrderInput is one attempt of task "work" of runID, cached or not,
// that names the secret and needs what row's narrower setting refuses.
func policyOrderInput(t *testing.T, row narrowerOrker, runID string, cached bool) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	var in *orkstackv1.ExecuteTaskInput
	if cached {
		in = cachedTaskInput(t, runID)
	} else {
		in = baseExportInput("work")
		in.RunId = runID
		in.IdempotencyKey = "default/" + runID + "/work"
		key, err := artifacts.CommitKey("default", runID, "work")
		if err != nil {
			t.Fatal(err)
		}
		in.CommitUri = cacheRoot + "/" + key
	}
	in.Secrets = []*orkstackv1.SecretRef{{Name: "token", Source: "env:" + orderSecretVar}}
	// A selection that admits every image: the image-policy step enters its
	// phase for it, so a refusal that came after that step would show it.
	in.ImagePolicy = &orkstackv1.ImagePolicySelection{}
	row.ask(in)
	return in
}

// phasesOf is the phases one attempt record names, in order.
func phasesOf(record *orkstackv1.TaskAttempt) []string {
	var phases []string
	for _, span := range record.GetPhases() {
		phases = append(phases, span.GetPhase())
	}
	return phases
}

// requireNoAttemptDirectory fails when an attempt's workspace is still under
// the work root: a refusal leaves nothing behind.
func requireNoAttemptDirectory(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read the work root: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "ork-") {
			t.Fatalf("attempt directory %q is still under the work root after the refusal", e.Name())
		}
	}
}

// asItem makes in item 1 of 3 of a fan-out over the run input's "parts".
func asItem(t *testing.T, _ *cacheHarness, in *orkstackv1.ExecuteTaskInput) {
	t.Helper()
	input, err := structpb.NewStruct(map[string]any{"rows": 3, "parts": []any{"a", "b", "c"}})
	if err != nil {
		t.Fatal(err)
	}
	in.RunInput = &orkstackv1.Payload{Data: input}
	in.IdempotencyKey = task.ItemIdempotencyKeyFor("default", in.GetRunId(), in.GetTaskId(), 1)
	key, err := artifacts.ItemCommitKey("default", in.GetRunId(), in.GetTaskId(), 1)
	if err != nil {
		t.Fatal(err)
	}
	in.CommitUri = cacheRoot + "/" + key
	in.Each = &orkstackv1.EachContext{Index: 1, Count: 3, As: "part", Source: &orkstackv1.EachSource{Input: "parts"}}
}

// onlyAttemptOf is the one attempt record of in, an item's or the task's.
func onlyAttemptOf(t *testing.T, store *artifacts.MemoryAttemptStore, in *orkstackv1.ExecuteTaskInput) *orkstackv1.TaskAttempt {
	t.Helper()
	item := artifacts.NoItem
	if each := in.GetEach(); each != nil {
		item = artifacts.ItemAt(each.GetIndex())
	}
	records, err := store.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), item)
	if err != nil || len(records) != 1 {
		t.Fatalf("attempt records of %s/%s item %s = %v (%v), want one", in.GetRunId(), in.GetTaskId(), item, records, err)
	}
	return records[0]
}

// TestExecute_AnAdoptedCommitIsNeverRefusedForThisOrkersPolicy: a retry that
// lands on a narrower Orker after the first attempt committed adopts the
// commit, with no container, whatever the narrower setting.
func TestExecute_AnAdoptedCommitIsNeverRefusedForThisOrkersPolicy(t *testing.T) {
	for _, row := range narrowerOrkers {
		t.Run(row.name, func(t *testing.T) {
			h := policyOrderHarness(t)
			in := policyOrderInput(t, row, "ork-order-adopt", false)
			first, err := h.dispatcher.Execute(context.Background(), in)
			if err != nil || first.GetCommitAdopted() {
				t.Fatalf("the first attempt: %v (adopted %t)", err, first.GetCommitAdopted())
			}
			row.narrow(h.dispatcher)
			pulls, creates := h.runtime.counts()

			result, err := h.dispatcher.Execute(context.Background(), in)
			if err != nil {
				t.Fatalf("the retry on the narrower Orker = %v; a committed result must be adopted, never refused", err)
			}
			if !result.GetCommitAdopted() || string(result.GetValuesJson()) != string(first.GetValuesJson()) {
				t.Fatalf("result = %v, want the first attempt's commit adopted", result)
			}
			if p, c := h.runtime.counts(); p != pulls || c != creates {
				t.Fatalf("the adopting retry pulled (%d→%d) or created (%d→%d)", pulls, p, creates, c)
			}
			if record := lastAttempt(t, h.attempts, in.GetRunId(), in.GetTaskId()); !record.GetCommitAdopted() {
				t.Fatalf("the retry's record = %v, want commit_adopted", record)
			}
		})
	}
}

// TestExecute_AMemoHitIsNeverRefusedForThisOrkersPolicy: a cached task whose
// memo hits on a narrower Orker reuses the source run's result, with no
// container, whatever the narrower setting.
func TestExecute_AMemoHitIsNeverRefusedForThisOrkersPolicy(t *testing.T) {
	for _, row := range narrowerOrkers {
		t.Run(row.name, func(t *testing.T) {
			h := policyOrderHarness(t)
			h.produce(t, policyOrderInput(t, row, "ork-order-memo-1", true))
			row.narrow(h.dispatcher)
			pulls, creates := h.runtime.counts()

			second := policyOrderInput(t, row, "ork-order-memo-2", true)
			result, err := h.dispatcher.Execute(context.Background(), second)
			if err != nil {
				t.Fatalf("the hit on the narrower Orker = %v; a memo hit must be reused, never refused", err)
			}
			if c := result.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || c.GetSource().GetRunId() != "ork-order-memo-1" {
				t.Fatalf("cache = %v, want a HIT of ork-order-memo-1", c)
			}
			if p, c := h.runtime.counts(); p != pulls || c != creates {
				t.Fatalf("the hit pulled (%d→%d) or created (%d→%d)", pulls, p, creates, c)
			}
		})
	}
}

// TestExecute_AMissIsRefusedForThisOrkersPolicyBeforeAnythingRuns is the
// negative control: the same narrower Orker still refuses a task that has
// neither a commit nor a hit, with the setting's own reason, before any
// secret is resolved, any image pulled or any config.json written, and it
// leaves no workspace behind. The secret's variable is unset, so a secret
// resolved before the refusal would have failed SECRET_UNRESOLVED instead.
func TestExecute_AMissIsRefusedForThisOrkersPolicyBeforeAnythingRuns(t *testing.T) {
	beforeAnyWork := []string{"image-policy", "workspace", "stage-inputs", "image", "render"}
	afterTheMemo := []string{"stage-artifacts", "image", "render"}
	for _, path := range []struct {
		name   string
		cached bool
		// edit shapes the input for the path.
		edit func(t *testing.T, h *cacheHarness, in *orkstackv1.ExecuteTaskInput)
		// ran is the phase the refusal must come after: the commit check,
		// or the memo step that did not hit.
		ran string
		// notRun are phases the refusal must come before.
		notRun []string
		// outcome is the cached task's memo outcome on its record.
		outcome orkstackv1.CacheOutcome
		// missReason, when set, is the miss reason the record must carry.
		missReason string
	}{
		{name: "uncached", ran: "commit-check", notRun: beforeAnyWork},
		{
			name: "pinned", ran: "commit-check", notRun: beforeAnyWork,
			edit: func(_ *testing.T, _ *cacheHarness, in *orkstackv1.ExecuteTaskInput) {
				in.Pin = &orkstackv1.PinContext{Name: "build", InstanceId: "ork-order-miss-pin", TimeoutSeconds: 60}
			},
		},
		{name: "a fan-out item", ran: "commit-check", notRun: beforeAnyWork, edit: asItem},
		{
			name: "cached", cached: true, ran: "memo", notRun: afterTheMemo,
			outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS,
		},
		{
			name: "cached under no_cache", cached: true, ran: "memo", notRun: afterTheMemo,
			edit: func(_ *testing.T, _ *cacheHarness, in *orkstackv1.ExecuteTaskInput) {
				in.Cache.Refresh = true
			},
			outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED,
		},
		{
			name: "cached with no key", cached: true, ran: "memo", notRun: afterTheMemo,
			edit: func(t *testing.T, h *cacheHarness, in *orkstackv1.ExecuteTaskInput) {
				// An upstream artifact with no sha256: nothing can be keyed.
				ref := h.put(t, "data.csv", []byte("x"))
				ref.Sha256 = ""
				in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "fetch", Artifacts: []*orkstackv1.ArtifactRef{ref}}}
			},
			outcome:    orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS,
			missReason: MemoMissInputUnhashed,
		},
		{
			name: "a cached fan-out item", cached: true, ran: "memo", notRun: afterTheMemo, edit: asItem,
			outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS,
		},
	} {
		for _, row := range narrowerOrkers {
			t.Run(path.name+"/"+row.name, func(t *testing.T) {
				h := policyOrderHarness(t)
				row.narrow(h.dispatcher)
				if err := os.Unsetenv(orderSecretVar); err != nil {
					t.Fatal(err)
				}
				in := policyOrderInput(t, row, "ork-order-miss", path.cached)
				if path.edit != nil {
					path.edit(t, h, in)
				}

				_, err := h.dispatcher.Execute(context.Background(), in)
				failure := failureOf(err)
				if failure == nil || failure.Reason != row.reason {
					t.Fatalf("Execute = %v, want %s", err, row.reason)
				}
				if p, c := h.runtime.counts(); p != 0 || c != 0 {
					t.Fatalf("a refused task pulled %d images and created %d containers, want none", p, c)
				}
				requireNoAttemptDirectory(t, h.dispatcher.WorkRoot)
				record := onlyAttemptOf(t, h.attempts, in)
				phases := phasesOf(record)
				joined := " " + strings.Join(phases, " ") + " "
				if !strings.Contains(joined, " "+path.ran+" ") {
					t.Fatalf("phases = %v; the refusal must come after %q", phases, path.ran)
				}
				for _, phase := range path.notRun {
					if strings.Contains(joined, " "+phase+" ") {
						t.Fatalf("phases = %v; the refusal must come before %q", phases, phase)
					}
				}
				if path.cached {
					if got := record.GetCache().GetOutcome(); got != path.outcome {
						t.Fatalf("the refused attempt's cache outcome = %v, want %v", got, path.outcome)
					}
					if h.hasLine("running the container") {
						t.Fatalf("a refused task logged that it runs its container: %q", h.loggedLines())
					}
				}
				if got := record.GetCache().GetMissReason(); path.missReason != "" && got != path.missReason {
					t.Fatalf("the refused attempt's miss reason = %q, want %q", got, path.missReason)
				}
				if path.outcome == orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS && path.missReason != MemoMissInputUnhashed {
					if gets, _ := h.memos.calls(); gets == 0 {
						t.Fatal("the cached task was refused before its memo was looked up")
					}
				}
			})
		}
	}
}

// TestValidate_RefusesWhatThisOrkerWouldRefuse: Validate, which orkctl task
// run calls before it fetches anything, still answers every refusal this
// Orker would make of a task that has no commit and no hit — the checks the
// same on every Orker and this Orker's own policy alike.
func TestValidate_RefusesWhatThisOrkerWouldRefuse(t *testing.T) {
	for _, row := range narrowerOrkers {
		t.Run(row.name, func(t *testing.T) {
			h := policyOrderHarness(t)
			in := policyOrderInput(t, row, "ork-order-validate", false)
			if failure := h.dispatcher.Validate(in); failure != nil {
				t.Fatalf("the permissive Orker's Validate = %v, want nil", failure)
			}
			row.narrow(h.dispatcher)
			if failure := h.dispatcher.Validate(in); failure == nil || failure.Reason != row.reason {
				t.Fatalf("the narrower Orker's Validate = %v, want %s", failure, row.reason)
			}
		})
	}
	h := policyOrderHarness(t)
	unpinned := policyOrderInput(t, narrowerOrkers[0], "ork-order-validate", false)
	unpinned.Image.Pinned = "example/image:latest"
	if failure := h.dispatcher.Validate(unpinned); failure == nil || failure.Reason != ReasonImageNotPinned {
		t.Fatalf("Validate of an unpinned image = %v, want %s", failure, ReasonImageNotPinned)
	}
}

// asIs is an input that needs nothing a narrower Orker refuses.
var asIs = narrowerOrker{ask: func(*orkstackv1.ExecuteTaskInput) {}}

// TestExecute_TheTasksOwnShapeIsRefusedBeforeTheCommitCheck: what reads
// nothing of the Orker — a network mode that does not exist, a malformed or
// zero request, a malformed secret name — is refused before the commit check,
// on every Orker alike, so neither a committed result nor a memo hit hands
// back a task no Orker would run. The network mode is here although it hangs
// off the policy: the memo key folds every mode but none into one component,
// so checking it after the memo step would let a hit through.
func TestExecute_TheTasksOwnShapeIsRefusedBeforeTheCommitCheck(t *testing.T) {
	for _, shape := range []struct {
		name   string
		reason string
		spoil  func(in *orkstackv1.ExecuteTaskInput)
	}{
		{
			name: "a network mode that does not exist", reason: ReasonNetworkModeRefused,
			spoil: func(in *orkstackv1.ExecuteTaskInput) { in.Network = orkstackv1.NetworkMode(99) },
		},
		{
			name: "a malformed cpu request", reason: ReasonResourceRefused,
			spoil: func(in *orkstackv1.ExecuteTaskInput) { in.Resources = &orkstackv1.ResourceRequest{Cpu: "lots"} },
		},
		{
			name: "a zero memory request", reason: ReasonResourceRefused,
			spoil: func(in *orkstackv1.ExecuteTaskInput) { in.Resources = &orkstackv1.ResourceRequest{Memory: "0"} },
		},
		{
			name: "a malformed secret name", reason: ReasonSecretSchemeForbid,
			spoil: func(in *orkstackv1.ExecuteTaskInput) { in.Secrets[0].Name = "not a name" },
		},
	} {
		for _, over := range []struct {
			name   string
			cached bool
		}{
			{name: "over an adopted commit"},
			{name: "over a memo hit", cached: true},
		} {
			t.Run(shape.name+"/"+over.name, func(t *testing.T) {
				h := policyOrderHarness(t)
				first := policyOrderInput(t, asIs, "ork-order-shape-1", over.cached)
				if _, err := h.dispatcher.Execute(context.Background(), first); err != nil {
					t.Fatalf("the first attempt: %v", err)
				}
				pulls, creates := h.runtime.counts()
				again := first
				if over.cached {
					again = policyOrderInput(t, asIs, "ork-order-shape-2", true)
				}
				shape.spoil(again)

				_, err := h.dispatcher.Execute(context.Background(), again)
				if failure := failureOf(err); failure == nil || failure.Reason != shape.reason {
					t.Fatalf("Execute = %v, want %s", err, shape.reason)
				}
				if p, c := h.runtime.counts(); p != pulls || c != creates {
					t.Fatalf("the refused task pulled (%d→%d) or created (%d→%d)", pulls, p, creates, c)
				}
				if phases := phasesOf(lastAttempt(t, h.attempts, again.GetRunId(), again.GetTaskId())); len(phases) != 1 || phases[0] != "start" {
					t.Fatalf("phases = %v; the task's own shape is refused before the commit check", phases)
				}
				if failure := h.dispatcher.Validate(again); failure == nil || failure.Reason != shape.reason {
					t.Fatalf("Validate = %v, want %s", failure, shape.reason)
				}
			})
		}
	}
}

// TestCapacity_ARequestAboveThisOrkersCapReservesNothingWhateverItsOutcome:
// the capacity ledger is asked the request the policy resolves, which is
// nothing for a request above this Orker's cap. An attempt that adopts a
// commit, reuses a hit or is refused for that request therefore reserves
// nothing past its poll, is never a RESOURCE_ADMISSION_MISMATCH, and leaves
// the ledger at rest.
func TestCapacity_ARequestAboveThisOrkersCapReservesNothingWhateverItsOutcome(t *testing.T) {
	cpuCap := narrowerOrkers[0]
	for _, path := range []struct {
		name   string
		cached bool
		// seed runs the first attempt on the permissive Orker.
		seed bool
		// refused is whether the narrower Orker refuses the attempt.
		refused bool
	}{
		{name: "adopted", seed: true},
		{name: "a memo hit", cached: true, seed: true},
		{name: "refused", refused: true},
	} {
		t.Run(path.name, func(t *testing.T) {
			h := policyOrderHarness(t)
			first := policyOrderInput(t, cpuCap, "ork-order-ledger", path.cached)
			if path.seed {
				if _, err := h.dispatcher.Execute(context.Background(), first); err != nil {
					t.Fatalf("the first attempt: %v", err)
				}
			}
			in := first
			if path.cached {
				in = policyOrderInput(t, cpuCap, "ork-order-ledger-2", true)
			}
			cpuCap.narrow(h.dispatcher)
			ceiling, err := h.dispatcher.Policy.ceiling()
			if err != nil {
				t.Fatal(err)
			}
			c := newCapacity(ceiling.add(ceiling), ceiling, 16)

			permit := mustReserve(t, c)
			c.markUsed(permit)
			request := executeTaskRequest(h.dispatcher.Policy, &interceptor.ExecuteActivityInput{Args: []any{in}})
			if request != (amount{}) {
				t.Fatalf("a request above this Orker's cap reserves %s, want nothing", request)
			}
			admission, err := c.admit(true, request)
			if err != nil {
				t.Fatalf("admit = %v; a request the policy resolves to nothing is never a mismatch", err)
			}
			wantReserved(t, c, "admitted", amount{})

			_, err = h.dispatcher.Execute(context.Background(), in)
			if (err != nil) != path.refused {
				t.Fatalf("Execute = %v, want refused %t", err, path.refused)
			}
			admission.Done()
			c.release(permit)
			wantReserved(t, c, "done", amount{})
		})
	}
}
