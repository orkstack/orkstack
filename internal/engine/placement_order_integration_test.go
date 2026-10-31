//go:build integration

package engine_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The placement-order suite (docs/OCI_ACTIVITY_CONTRACT.md §7.1, AGENTS.md
// §5): an Orker checks a task against its OWN policy — its cap, its secret
// schemes and floor, its network modes, its runtime shape — only after the
// retry-start commit check and, for a cached task, after the memo step. A
// result another Orker of the pool already committed, or a memo that hits,
// starts no container, so an Orker whose settings are narrower than the one
// that produced it hands it back instead of failing the run. A task with no
// commit and no hit is still refused there, explicitly, before any container
// exists.
//
// Every Orker here is real: real Docker, real commit, attempt and memo stores
// in the stack's bucket. "No container started" is counted from the Docker
// daemon by the run's ork.run label, subscribed before the run starts, and
// each count is held against a container the same subscription saw.

// cappedPolicy is the default Orker policy with a cpu cap of cpu cores.
func cappedPolicy(cpu string) *orker.SecurityPolicy {
	policy := orker.DefaultSecurityPolicy()
	policy.MaxCPU = cpu
	return &policy
}

// containerStarts is a subscription to the Docker daemon's container starts
// of one run, filtered by its ork.run label. It subscribes rather than reading
// the daemon's event backlog afterwards, which a busy host rolls over; it asks
// for the backlog from just before it subscribed, so a start in the
// subscription's own setup window is not missed.
type containerStarts struct {
	cmd  *exec.Cmd
	done chan struct{}
	mu   sync.Mutex
	// starts is one "<ork.orker> <ork.task>" per container started.
	starts []string
}

func watchContainerStarts(t *testing.T, runID string) *containerStarts {
	t.Helper()
	since := strconv.FormatInt(time.Now().Add(-2*time.Second).Unix(), 10)
	cmd := exec.Command("docker", "events", "--since", since,
		"--filter", "type=container", "--filter", "event=start", "--filter", "label=ork.run="+runID,
		"--format", `{{index .Actor.Attributes "ork.orker"}} {{index .Actor.Attributes "ork.task"}}`)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("docker events: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("docker events: %v", err)
	}
	w := &containerStarts{cmd: cmd, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			w.mu.Lock()
			w.starts = append(w.starts, strings.TrimSpace(scanner.Text()))
			w.mu.Unlock()
		}
	}()
	t.Cleanup(w.stop)
	return w
}

// by counts the starts of orkerID's containers.
func (w *containerStarts) by(orkerID string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, start := range w.starts {
		if strings.HasPrefix(start, orkerID+" ") {
			n++
		}
	}
	return n
}

// awaitBy waits, until the deadline, for at least want starts by orkerID:
// the positive control that the subscription sees this run's containers.
func (w *containerStarts) awaitBy(t *testing.T, orkerID string, want int, within time.Duration) {
	t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for w.by(orkerID) < want {
		select {
		case <-tick.C:
		case <-deadline.C:
			w.mu.Lock()
			seen := append([]string(nil), w.starts...)
			w.mu.Unlock()
			t.Fatalf("the daemon reported %v for this run within %s, want %d start(s) by %s", seen, within, want, orkerID)
		}
	}
}

// requireLive fails when the subscription has ended: a count of zero from a
// subscription that is no longer listening would pass vacuously.
func (w *containerStarts) requireLive(t *testing.T) {
	t.Helper()
	select {
	case <-w.done:
		t.Fatal("the docker events subscription ended before its window closed")
	default:
	}
}

// stop ends the subscription this test started, by its own process.
func (w *containerStarts) stop() {
	if w.cmd.Process != nil {
		_ = w.cmd.Process.Kill()
	}
	<-w.done
	_ = w.cmd.Wait()
}

// requireNoContainerPhase fails when an attempt record shows an image
// resolved, a config rendered or a container created or run.
func requireNoContainerPhase(t *testing.T, record *orkstackv1.TaskAttempt) {
	t.Helper()
	for _, span := range record.GetPhases() {
		switch span.GetPhase() {
		case "image", "render", "create", "run":
			t.Fatalf("attempt %d on %s reached phase %q: %v", record.GetAttempt(), record.GetOrkerId(), span.GetPhase(), record.GetPhases())
		}
	}
}

// TestIntegration_ACommittedRetryIsAdoptedOnASmallerOrker: a committed retry
// lands on an Orker whose cap is smaller than the task's request.
// Orker A (cpu cap 2) runs the 2-cpu task, writes its commit.json and is
// killed before it reports completion. The retry lands on Orker B (cpu cap
// 1), which adopts the commit: the run succeeds, the record says
// commit_adopted, and B started no container. Then the negative control: a
// fresh run of the same task, with no commit, is refused on B with
// RESOURCE_REQUEST_REFUSED before any container exists.
func TestIntegration_ACommittedRetryIsAdoptedOnASmallerOrker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "placement/committed-retry")
	stores := s.realStores()
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	committed := make(chan string, 1)
	var once sync.Once
	proxyA := s.temporalProxy()
	orkerA, idA := s.startRealOrkerWithPolicy("orker-a", "real", realQueue,
		hookedCommitStores(stores, func(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
			record, err := stores.commits.Create(ctx, uri, c)
			if err != nil {
				return record, err
			}
			once.Do(func() { committed <- uri })
			<-release
			return artifacts.CommitRecord{}, errors.New("orker A was killed after its commit")
		}), s.dial("orker-a", proxyA.Addr()), cappedPolicy("2"), nil)

	wf := realTaskDocument(t, "placement-committed-retry", pinnedAlpineForCommitE2E(t),
		`printf '{"ok":true}' > /orkstack/output/values.json`, "maximum_attempts: 3")
	for _, tk := range wf.GetTasks() {
		tk.Resources = &orkstackv1.ResourceRequest{Cpu: "2"}
	}
	plan := realTaskPlan(t, s, realQueue)
	runID := s.newRunID()
	s.transcript.run(runID)
	starts := watchContainerStarts(t, runID)
	s.start(runID, wf, nil, plan)
	stream := s.openStream(s.reader, runID, 0)

	select {
	case uri := <-committed:
		s.transcript.step("Orker A (cpu cap 2) wrote %s and is held before it reports completion", uri)
	case <-time.After(3 * time.Minute):
		t.Fatal("Orker A never wrote the task's commit.json")
	}
	if n := commitObjectCount(t, s.cfg, runID); n != 1 {
		t.Fatalf("%d commit.json objects before the kill, want the one Orker A wrote", n)
	}
	proxyA.Cut()
	orkerA.stop()
	_, idB := s.startRealOrkerWithPolicy("orker-b", "real", realQueue, stores, nil, cappedPolicy("1"), nil)
	s.transcript.step("Orker A killed after its commit; Orker B (cpu cap 1) started")

	status, err := s.wait(runID, 5*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	records, err := stores.attempts.ListAttempts(ctx, runID, "work", artifacts.NoItem)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("%d attempt records, want the retry's alone (attempt 1 is held before its record): %v", len(records), records)
	}
	retry := records[0]
	if retry.GetAttempt() != 2 || retry.GetOrkerId() != idB || !retry.GetCommitAdopted() ||
		retry.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
		t.Fatalf("the retry's record is attempt %d on %s, adopted %t, %v %v; want attempt 2 on %s, adopted, SUCCEEDED",
			retry.GetAttempt(), retry.GetOrkerId(), retry.GetCommitAdopted(), retry.GetOutcome(), retry.GetError(), idB)
	}
	requireNoContainerPhase(t, retry)
	if n := commitObjectCount(t, s.cfg, runID); n != 1 {
		t.Fatalf("%d commit.json objects, want exactly the one Orker A wrote", n)
	}
	starts.awaitBy(t, idA, 1, time.Minute)
	starts.requireLive(t)
	if a, b := starts.by(idA), starts.by(idB); a != 1 || b != 0 {
		t.Fatalf("containers started for the run: %d by Orker A, %d by Orker B; want 1 and 0", a, b)
	}
	s.transcript.figure("containers_by_orker_b", 0)

	// The negative control: the same 2-cpu task with no commit, on B alone.
	missID := s.newRunID()
	s.transcript.run(missID)
	missStarts := watchContainerStarts(t, missID)
	s.start(missID, wf, nil, plan)
	if status, _ := s.wait(missID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the uncommitted run closed %v, want FAILED", status)
	}
	run, err := s.reader.GetRun(ctx, missID)
	if err != nil {
		t.Fatal(err)
	}
	work := taskRunOfRun(run, "work")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || work.GetError().GetCode() != orker.ReasonResourceRefused {
		t.Fatalf("the uncommitted run = %v, work %v %v; want FAILED with %s", run.GetStatus(), work.GetStatus(), work.GetError(), orker.ReasonResourceRefused)
	}
	refused, err := stores.attempts.ListAttempts(ctx, missID, "work", artifacts.NoItem)
	if err != nil {
		t.Fatal(err)
	}
	if len(refused) != 1 || refused[0].GetOrkerId() != idB || refused[0].GetError().GetCode() != orker.ReasonResourceRefused {
		t.Fatalf("the uncommitted run's records = %v, want one refusal on %s", refused, idB)
	}
	requireNoContainerPhase(t, refused[0])
	missStarts.requireLive(t)
	if n := missStarts.by(idB); n != 0 {
		t.Fatalf("Orker B started %d containers for the refused task, want 0", n)
	}
	t.Logf("committed retry on a smaller Orker: attempt 2 on %s (cpu cap 1) adopted the commit %s (cpu cap 2) wrote, with no container; "+
		"a fresh 2-cpu task on %s failed %s with no container", idB, idA, idB, orker.ReasonResourceRefused)
}

// placementVariant is one way the second Orker of a pool is narrower than the
// Orker that produced a cached task's memo.
type placementVariant struct {
	name   string
	reason string
	// seed is the Orker that runs the first run; small takes its place.
	seed, small *orker.SecurityPolicy
	secrets     []*orkstackv1.SecretRef
}

// TestIntegration_AMemoHitIsReusedOnASmallerOrker: a memo hit lands on an
// Orker with a smaller cap, or with a stricter secret floor. Run 1 seeds the
// memo of the cached 2-cpu task "work" on a seed Orker; a narrower Orker then
// takes the seed's place on that pool. In run 2, "work" hits on the narrower
// Orker and starts no container, while the uncached task "other" of the same
// run runs its container on the cap-2 Orker of the "big" pool. Run 3 changes
// the input, so "work" misses: the narrower Orker refuses it with the
// setting's own reason after the memo step and before any container exists.
func TestIntegration_AMemoHitIsReusedOnASmallerOrker(t *testing.T) {
	const secretVar = "SANDBOX_PLACEMENT_MEMO_TOKEN"
	floor := func(allow string) *orker.SecurityPolicy {
		policy := cappedPolicy("2")
		policy.SecretAllow = []string{allow}
		return policy
	}
	for _, v := range []placementVariant{
		{
			name: "cap", reason: orker.ReasonResourceRefused,
			seed: cappedPolicy("2"), small: cappedPolicy("1"),
		},
		{
			name: "secret-floor", reason: orker.ReasonSecretNotPermitted,
			seed: floor("env:" + secretVar), small: floor("env:SANDBOX_PLACEMENT_MEMO_OTHER_*"),
			secrets: []*orkstackv1.SecretRef{{Name: "token", Source: "env:" + secretVar}},
		},
	} {
		t.Run(v.name, func(t *testing.T) {
			t.Setenv(secretVar, "placement-memo-secret")
			c := startCacheE2E(t)
			bigQueue := mustQueueForPool(t, "big")
			label := fmt.Sprintf("placement-memo-%s-%d", v.name, time.Now().UnixNano())
			idSeed, idSmall, idBig := proof.OrkerID(label+"-seed"), proof.OrkerID(label+"-small"), proof.OrkerID(label+"-big")
			c.startCacheOrkerOn(idBig, "big", bigQueue, cappedPolicy("2"), nil, nil)
			seed := c.startCacheOrkerOn(idSeed, task.DefaultPool, c.orkerQueue, v.seed, nil, nil)

			wf := placementMemoOrkflow(t, c, label, v.secrets)
			plan := c.cachePlan(false)
			plan.Routes["big"] = bigQueue
			start := func(runID string, rows int) (*orkstackv1.Run, *containerStarts) {
				t.Helper()
				starts := watchContainerStarts(t, runID)
				data, _ := structpb.NewStruct(map[string]any{"rows": rows})
				starter := engine.NewTemporalStarterWithOrkerQueue(c.temporal, c.runQueue, c.orkerQueue)
				if err := starter.StartRun(c.ctx, runID, wf, &orkstackv1.Payload{Data: data}, plan); err != nil {
					t.Fatalf("StartRun: %v", err)
				}
				return c.wait(runID), starts
			}

			run1ID, run2ID, run3ID := runIDFor(label+"-1"), runIDFor(label+"-2"), runIDFor(label+"-3")
			run1, _ := start(run1ID, 1)
			if run1.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("the seeding run = %v %v", run1.GetStatus(), run1.GetError())
			}
			requireOutcome(t, run1, "work", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, "MEMO_ABSENT")
			seed.Stop()
			c.startCacheOrkerOn(idSmall, task.DefaultPool, c.orkerQueue, v.small, nil, nil)

			// Run 2: the hit on the narrower Orker.
			run2, starts2 := start(run2ID, 1)
			if run2.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("run 2 = %v %v; a memo hit must be reused on the narrower Orker", run2.GetStatus(), run2.GetError())
			}
			requireOutcome(t, run2, "work", orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, "")
			if src := cacheOf(run2, "work").GetSource().GetRunId(); src != run1ID {
				t.Fatalf("the hit names %q, want run 1 %s", src, run1ID)
			}
			hit := onlyRecord(t, c, run2ID, "work")
			if hit.GetOrkerId() != idSmall || hit.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED ||
				hit.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
				t.Fatalf("the hit's record = %v; want a HIT on %s", hit, idSmall)
			}
			requireNoContainerPhase(t, hit)
			other := onlyRecord(t, c, run2ID, "other")
			if other.GetOrkerId() != idBig || other.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
				t.Fatalf("run 2's uncached task's record = %v; want SUCCEEDED on %s", other, idBig)
			}
			starts2.awaitBy(t, idBig, 1, time.Minute)
			starts2.requireLive(t)
			if small := starts2.by(idSmall); small != 0 {
				t.Fatalf("the narrower Orker started %d containers in run 2, want 0", small)
			}

			// Run 3: a miss on the narrower Orker is still refused.
			run3, starts3 := start(run3ID, 2)
			work := taskRunNamed(run3, "work")
			if run3.GetStatus() != orkstackv1.Status_STATUS_FAILED || work.GetError().GetCode() != v.reason {
				t.Fatalf("run 3 = %v, work %v %v; want FAILED with %s", run3.GetStatus(), work.GetStatus(), work.GetError(), v.reason)
			}
			refused := onlyRecord(t, c, run3ID, "work")
			if refused.GetOrkerId() != idSmall || refused.GetError().GetCode() != v.reason ||
				refused.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS {
				t.Fatalf("run 3's refused record = %v; want %s after a MISS on %s", refused, v.reason, idSmall)
			}
			requireNoContainerPhase(t, refused)
			for _, span := range refused.GetPhases() {
				if span.GetPhase() == "stage-artifacts" {
					t.Fatalf("the refusal came after the secrets and artifacts were staged: %v", refused.GetPhases())
				}
			}
			starts3.requireLive(t)
			if small := starts3.by(idSmall); small != 0 {
				t.Fatalf("the narrower Orker started %d containers in run 3, want 0", small)
			}
			t.Logf("memo hit on a narrower Orker (%s): run 2's work hit on %s with no container while other ran on %s; "+
				"run 3's miss was refused %s on %s with no container", v.name, idSmall, idBig, v.reason, idSmall)
		})
	}
}

// placementMemoOrkflow is two independent 2-cpu tasks over alpine: "work",
// cached, on the default pool, naming secrets; and "other", uncached, on the
// "big" pool.
func placementMemoOrkflow(t *testing.T, c *cacheE2E, name string, secrets []*orkstackv1.SecretRef) *orkstackv1.Orkflow {
	t.Helper()
	image := &orkstackv1.ImageRef{Reference: c.alpine, Pinned: c.alpine, IndexDigest: digestOf(c.alpine)}
	with, _ := structpbStruct(map[string]any{"rows": "{{ .inputs.rows }}"})
	inputs, _ := structpbStruct(map[string]any{"rows": map[string]any{"type": "number", "default": 1}})
	request := func() *orkstackv1.ResourceRequest { return &orkstackv1.ResourceRequest{Cpu: "2"} }
	wf := &orkstackv1.Orkflow{
		Name: name, Version: "0.1.0", Inputs: inputs,
		Tasks: []*orkstackv1.Task{
			{Id: "work", Uses: c.alpine, Image: image, With: with, Secrets: secrets, Resources: request(),
				Command:        []string{"/bin/sh", "-c", `cat /orkstack/input/config.json && printf '{"ok":true}' > /orkstack/output/values.json`},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
				Cache: &orkstackv1.CacheSpec{TtlSeconds: 86400, Scope: orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW}},
			{Id: "other", Uses: c.alpine, Image: image, Pool: "big", Resources: request(),
				Command:        []string{"/bin/sh", "-c", `printf '{}' > /orkstack/output/values.json`},
				TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE},
		},
	}
	stampDigest(t, wf)
	return wf
}

// onlyRecord is the one attempt record of runID's taskID.
func onlyRecord(t *testing.T, c *cacheE2E, runID, taskID string) *orkstackv1.TaskAttempt {
	t.Helper()
	records, err := c.attempts.ListAttempts(c.ctx, runID, taskID, artifacts.NoItem)
	if err != nil || len(records) != 1 {
		t.Fatalf("attempt records of %s/%s = %v (%v), want exactly one", runID, taskID, records, err)
	}
	return records[0]
}
