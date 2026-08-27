//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_AReplicaThatStartedNothingAnswersTheAdminSurface is M11.3's
// acceptance, live.
//
// Two real `ork` processes share one Temporal namespace, one execution channel
// and one MinIO bucket — the production topology behind a load balancer. Every
// WRITE goes to replica A. Every ADMIN READ goes to replica B, which registered
// no namespace, applied no orkflow and started no run, so nothing it answers
// can have come from its own memory (AGENTS.md §1.7):
//
//   - system information: B describes ITSELF, which is the one thing only the
//     process can answer — and the two replicas of one release answer
//     identically, field for field, which is the property that makes it safe
//     to ask whichever replica the load balancer picked.
//   - namespaces: from the durable registry in the bucket, including the one
//     A created a moment ago.
//   - pool health: live Orker counts from the cluster's poller information and
//     the plugins asked of the Orkers themselves — a pool nothing serves is
//     reported as empty rather than omitted.
//   - plugin inventory: asked of the connected Orkers.
//
// The topology covers the two shapes an earlier round of this slice broke, so
// that neither can regress unnoticed:
//
//   - a pool with SEVERAL live Orkers. A pool IS a set of Orkers, so two of
//     them is the ordinary case and the one an operator scales into. Two real
//     `orker` processes poll this run's default pool.
//   - a live NON-DEFAULT pool. A third `orker` polls a second pool, which the
//     namespace allow-lists. It must appear in pool health, and it must not
//     cost the inventory its answer.
//
// Then the write-safety rules, exercised against B because a write refused only
// by the replica that happens to hold state is not a rule (acceptance 5): the
// slug validated and never repaired, `default` undeletable, the etag
// compare-and-swap, and a delete refused while the namespace still holds
// orkflows, saying how many. Those refusals are the only writes B issues; it
// registered no namespace and applied no orkflow before the reads above.
//
// Isolation (AGENTS.md §6): a Temporal namespace unique to this run, and a
// pool unique to this process. Note the deliberate inversion of the usual rule
// — the two replicas SHARE that namespace, because sharing it is the condition
// under test.
func TestIntegration_AReplicaThatStartedNothingAnswersTheAdminSurface(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	// The registry and the orkflow store live in the bucket. That is not a
	// convenience here: it is what lets a replica answer for a namespace it
	// never created, so a run without MinIO would be measuring something else.
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	temporalNamespace := isolatedTemporalNamespace("ork-m11-3-admin-surface")
	t.Logf("temporal namespace %s", temporalNamespace)
	dialIsolatedNamespace(ctx, t, address, temporalNamespace)

	pool := privateTaskPool()
	channel, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map test pool: %v", err)
	}
	// A second pool this deployment does NOT route by default. Its Orker is
	// reachable only by naming the pool, which is the case the admin reads
	// have to cover: an inventory that fails, or a pool list that refuses,
	// because one pool is not the default route would be useless in every
	// deployment that has more than one pool.
	auxPool := pool + "-aux"
	auxChannel, err := task.QueueForPool(auxPool, "")
	if err != nil {
		t.Fatalf("map aux test pool: %v", err)
	}
	root := repoRoot(t)
	binDir := t.TempDir()
	orkctlPath := buildBinary(t, root, binDir, "orkctl")
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")

	// TWO Orkers on this run's default pool, and one on the aux pool. Two is
	// the point: a pool is a set of Orkers, and an admin read that works only
	// while a pool has exactly one is broken for every pool anybody scales.
	// Each Orker gets its OWN work root, because the default Orker identity is
	// derived from it and one identity may have only one live holder (§5.5):
	// three Orkers under one root would be one Orker and two refusals.
	startOrkerOn := func(name, queue string) {
		workRoot := filepath.Join(bindableOrkerWorkRoot(t), name)
		if err := os.MkdirAll(workRoot, 0o755); err != nil {
			t.Fatalf("create work root for %s: %v", name, err)
		}
		startProcess(ctx, t, name, orkerPath,
			"ORK_TEMPORAL_ADDRESS="+address,
			"ORK_TEMPORAL_NAMESPACE="+temporalNamespace,
			"ORK_ORKER_QUEUE="+queue,
			"ORK_ORKER_WORKDIR="+workRoot,
			"ORK_ARTIFACT_BACKEND=s3",
		)
	}
	startOrkerOn("orker-1", channel)
	startOrkerOn("orker-2", channel)
	startOrkerOn("orker-aux", auxChannel)

	// The two replicas: identical configuration, different ports. The
	// deployment name is this run's, so `admin info` reporting it proves the
	// value came from configuration rather than from a default that would
	// look the same on any machine.
	deployment := fmt.Sprintf("orkstack-m11-3-%d", os.Getpid())
	engineEnv := func(addr string) []string {
		return []string{
			"ORK_HTTP_ADDR=" + addr,
			"ORK_TEMPORAL_ADDRESS=" + address,
			"ORK_TEMPORAL_NAMESPACE=" + temporalNamespace,
			"ORK_ORKER_QUEUE=" + channel,
			"ORK_ARTIFACT_BACKEND=s3",
			"ORK_DEPLOYMENT_NAME=" + deployment,
		}
	}
	addrA, addrB := freeAddress(t), freeAddress(t)
	startProcess(ctx, t, "ork-A", enginePath, engineEnv(addrA)...)
	startProcess(ctx, t, "ork-B", enginePath, engineEnv(addrB)...)

	session := &sessionRecorder{}
	configPath := filepath.Join(t.TempDir(), "orkctl.json")
	// Two callers, one binary: `against` records into the transcript, `probe`
	// does not. The readiness poll runs `orkflow list` against a bucket other
	// milestones have been applying into all day, so recording it would bury
	// this proof's fifteen commands under two hundred lines of somebody
	// else's catalog.
	probe := func(addr string) func(args ...string) (string, string, int) {
		return func(args ...string) (string, string, int) {
			full := append(append([]string(nil), args...), "--server", "http://"+addr)
			return runBinaryWithEnv(ctx, t, orkctlPath, root,
				[]string{configEnvVar + "=" + configPath, namespaceEnvVar + "="}, full...)
		}
	}
	against := func(name, addr string) func(args ...string) (string, string, int) {
		return func(args ...string) (string, string, int) {
			stdout, stderr, code := probe(addr)(args...)
			full := append(append([]string{"#" + name}, args...), "--server", "http://"+addr)
			session.record(full, stdout, stderr, code)
			return stdout, stderr, code
		}
	}
	onA := against("A", addrA)
	onB := against("B", addrB)
	waitForEngine(t, probe(addrA))
	waitForEngine(t, probe(addrB))

	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	namespace := "m113-admin-" + token
	orkflowName := "admin-probe-" + token
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		removeNamespaceObjects(t, cleanupCtx, namespace)
	})
	t.Cleanup(func() {
		publishAdminTranscript(t, session.transcript.String())
	})

	// ---- the setup writes, on A only --------------------------------------
	//
	// `cold` is a pool this deployment has no Orkers for. It is in the
	// allow-list on purpose: an empty pool must be REPORTED, because "nobody
	// is serving this" is the answer pool health exists to give. The aux pool
	// is allow-listed too, so pool health has a LIVE non-default pool to
	// report beside the empty one.
	requireCLI(t, onA, "A registers the namespace", "namespace", "create", namespace,
		"--description", "M11.3 admin surface probe",
		"--default-pool", pool,
		"--allow-pool", pool,
		"--allow-pool", auxPool,
		"--allow-pool", "cold")
	requireCLI(t, onA, "A applies an orkflow into it", "apply",
		writeAdminProbeOrkflow(t, orkflowName, pool), "-n", namespace)

	// ---- system information, from the replica that started nothing --------
	infoB := requireCLI(t, onB, "B describes itself", "admin", "info")
	for _, want := range []string{deployment, address, "versioning:", "off", "s3://"} {
		if !strings.Contains(infoB, want) {
			t.Errorf("admin info on B does not report %q:\n%s", want, infoB)
		}
	}
	if !strings.Contains(infoB, "not a measurement of the cluster") {
		t.Errorf("admin info does not label its values as configuration:\n%s", infoB)
	}
	systemA := decodeSystemInfo(t, requireCLI(t, onA, "A describes itself, as JSON",
		"--json", "admin", "info"))
	systemB := decodeSystemInfo(t, requireCLI(t, onB, "B describes itself, as JSON",
		"--json", "admin", "info"))

	// Two replicas of one release describe the deployment identically. The
	// server time is the one field that must differ in kind — it is the only
	// observed value in the message — so it is compared as a clock, not as a
	// string.
	if systemA.DeploymentName != deployment || systemB.DeploymentName != deployment {
		t.Errorf("deployment name = %q/%q, want the configured %q",
			systemA.DeploymentName, systemB.DeploymentName, deployment)
	}
	if systemB.VersioningActive || systemB.BuildID != "" {
		t.Errorf("B reports versioning active (%v, build %q) with no build id configured",
			systemB.VersioningActive, systemB.BuildID)
	}
	if systemB.ArtifactBackend != artifacts.BackendS3 {
		t.Errorf("artifact backend = %q, want s3", systemB.ArtifactBackend)
	}
	// The location is the bucket and nothing else. Asserted as equality
	// rather than as "does not contain the key": the dev access key is
	// `orkstack` and the dev bucket is `orkstack-artifacts`, so a substring
	// check reads a credential in every correct answer. Equality says the
	// same thing without the false alarm — anything appended, a key, a
	// session token, a query string, fails it.
	if want := "s3://" + artifactCfg.S3Bucket; systemB.ArtifactLocation != want {
		t.Errorf("artifact location = %q, want exactly %q — the bucket, no credential",
			systemB.ArtifactLocation, want)
	}
	if strings.Contains(systemB.ArtifactLocation, artifactCfg.S3SecretKey) {
		t.Errorf("artifact location leaks the secret key: %q", systemB.ArtifactLocation)
	}
	if systemB.TemporalAddress != address {
		t.Errorf("cluster address = %q, want the configured %q", systemB.TemporalAddress, address)
	}
	if systemA.EngineVersion != systemB.EngineVersion || systemA.GoVersion != systemB.GoVersion {
		t.Errorf("two replicas of one release report different builds: %+v vs %+v", systemA, systemB)
	}
	stamped, err := time.Parse(time.RFC3339, systemB.ServerTime)
	if err != nil {
		t.Errorf("server time %q is not a clock reading: %v", systemB.ServerTime, err)
	} else if time.Since(stamped) > 5*time.Minute {
		t.Errorf("server time %s is stale; it is meant to be read at the request", stamped)
	}

	// ---- the namespace registry, from B -----------------------------------
	listed := requireCLI(t, onB, "B lists a namespace it never created",
		"namespace", "list")
	if !strings.Contains(listed, namespace) {
		t.Fatalf("B does not list namespace %q, which A registered:\n%s", namespace, listed)
	}
	described := decodeNamespaceDocument(t, requireCLI(t, onB,
		"B reads the namespace's pool policy", "--json", "namespace", "get", namespace))
	if described.Namespace.DefaultPool != pool {
		t.Errorf("default pool = %q, want %q", described.Namespace.DefaultPool, pool)
	}
	if len(described.Namespace.AllowedPools) != 3 {
		t.Errorf("allowed pools = %v, want the three A registered", described.Namespace.AllowedPools)
	}
	if described.Namespace.Etag == "" {
		t.Error("a stored namespace read back with no etag; an update could not be conditional")
	}

	// ---- pool health, from B ----------------------------------------------
	//
	// Poller information reaches the cluster a moment after a worker starts,
	// so the live counts are awaited. What must NOT be awaited is B knowing
	// anything about them: B started no Orker and dispatched no task.
	//
	// Two Orkers on the default pool and one on the aux pool. Both numbers are
	// asserted, because both were a refusal one round ago.
	pools := awaitLivePoolsOnB(t, onB, namespace, map[string]int{pool: 2, auxPool: 1})
	if got := pools.byName(pool); got == nil || got.LiveOrkerCount < 2 {
		t.Fatalf("pool %q does not report its two Orkers on B: %+v", pool, pools)
	}
	if got := pools.byName(auxPool); got == nil || got.LiveOrkerCount < 1 {
		t.Fatalf("the live non-default pool %q is missing from B's report: %+v", auxPool, pools)
	} else {
		if got.IsDefault {
			t.Errorf("pool %q is marked default; %q is", auxPool, pool)
		}
	}
	if cold := pools.byName("cold"); cold == nil {
		t.Error("the allow-listed pool nothing serves is missing; an empty pool must be reported")
	} else if cold.LiveOrkerCount != 0 {
		t.Errorf("pool cold = %+v, want no Orkers", cold)
	} else if cold.LastSeen != "" {
		// A pool nothing polls has no fresh observation, so it has no time to
		// report. See Pool.last_seen: this column never dates a dead pool.
		t.Errorf("pool cold reports last_seen %q with no live Orker", cold.LastSeen)
	}
	if pools.DefaultPool != pool {
		t.Errorf("default pool = %q, want %q — where a task naming none runs",
			pools.DefaultPool, pool)
	}
	humanPools := requireCLI(t, onB, "B renders pool health", "pool", "list", "-n", namespace)
	if !strings.Contains(humanPools, "no live orkers") {
		t.Errorf("a pool with no Orkers is rendered as a bare number:\n%s", humanPools)
	}

	// The two replicas agree about the cluster, because both read the cluster.
	fromA := decodePoolList(t, requireCLI(t, onA, "A reads the same pools",
		"--json", "pool", "list", "-n", namespace))
	if fromA.DefaultPool != pools.DefaultPool || len(fromA.Pools) != len(pools.Pools) {
		t.Errorf("the replicas disagree about the pools:\nA %+v\nB %+v", fromA, pools)
	}

	// ---- the activity catalog, from B --------------------------------------
	//
	// v0.0.5 retired the per-Orker plugin inventory: an activity is an image,
	// and which images exist is engine configuration. The read that replaced it
	// is ActivityService.ListActivities, and B answers it having started
	// nothing, exactly as it answers pool health.
	catalog := requireCLI(t, onB, "B lists the activity catalog", "activity", "list")
	for _, want := range []string{"shell", "http", "file"} {
		if !strings.Contains(catalog, want) {
			t.Errorf("the catalog does not report the %q activity:\n%s", want, catalog)
		}
	}

	// ---- write safety, on B ------------------------------------------------
	//
	// 1. A name the rule forbids is refused, and NOT repaired into one that is
	//    allowed: a name that has to be sanitized to be stored is a name that
	//    can be spelled two ways, and a key built from it addresses somebody
	//    else's tenant (AGENTS.md §1.10).
	badName := "M113_Bad Name"
	_, stderr, code := onB("namespace", "create", badName)
	if code == 0 {
		t.Fatal("a namespace name the rule forbids was accepted")
	}
	if !strings.Contains(stderr, "invalid_argument") {
		t.Errorf("the refusal is not an invalid argument:\n%s", stderr)
	}
	afterBad := requireCLI(t, onB, "nothing was stored under a repaired name",
		"namespace", "list")
	for _, sanitized := range []string{"m113-bad-name", "m113_bad name", badName} {
		if strings.Contains(afterBad, sanitized) {
			t.Errorf("the refused name reached the registry as %q:\n%s", sanitized, afterBad)
		}
	}

	// 2. The default namespace is refused unconditionally, and still resolves
	//    afterwards: every orkflow that names no namespace is in it.
	_, stderr, code = onB("namespace", "delete", artifacts.DefaultNamespace)
	if code == 0 {
		t.Fatal("the default namespace was deleted")
	}
	if !strings.Contains(stderr, "failed_precondition") {
		t.Errorf("deleting the default namespace was refused with the wrong code:\n%s", stderr)
	}
	stillThere := requireCLI(t, onB, "the default namespace still resolves",
		"namespace", "get", artifacts.DefaultNamespace)
	if !strings.Contains(stillThere, artifacts.DefaultNamespace) {
		t.Errorf("the default namespace no longer resolves:\n%s", stillThere)
	}

	// 3. A delete is refused while the namespace holds orkflows, and says how
	//    many and which: deleting the registry entry would not delete the
	//    definitions, and they would survive under a namespace nothing lists.
	_, stderr, code = onB("namespace", "delete", namespace)
	if code == 0 {
		t.Fatal("a namespace holding an orkflow was deleted")
	}
	for _, want := range []string{"failed_precondition", "1 orkflow", orkflowName} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, stderr)
		}
	}

	// 4. The etag is a compare-and-swap, and the loser is told to re-read. The
	//    stale token is sent through the Connect client because that is what a
	//    second operator's browser tab holds: a value read before somebody
	//    else's write landed.
	namespaces := orkstackv1connect.NewNamespaceServiceClient(h2cClient(t), "http://"+addrB)
	before, err := namespaces.GetNamespace(ctx, connect.NewRequest(
		&orkstackv1.GetNamespaceRequest{Name: namespace}))
	if err != nil {
		t.Fatalf("read %s from B: %v", namespace, err)
	}
	stale := before.Msg.GetNamespace()
	requireCLI(t, onA, "A updates the namespace, moving its etag",
		"namespace", "update", namespace, "--description", "changed by A")
	edited := stale
	edited.Description = "changed by B, from a token that has moved"
	_, err = namespaces.UpdateNamespace(ctx, connect.NewRequest(
		&orkstackv1.UpdateNamespaceRequest{Namespace: edited}))
	if err == nil {
		t.Fatal("an update carrying a stale etag overwrote a namespace somebody else had changed")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("the stale update failed with %v, want FailedPrecondition", connect.CodeOf(err))
	}
	session.record([]string{"#B", "namespace", "update", namespace, "(with the etag read before A's write)"},
		"", err.Error(), 1)
	after := requireCLI(t, onB, "B reads what survived", "namespace", "get", namespace)
	if !strings.Contains(after, "changed by A") {
		t.Errorf("the losing write landed anyway:\n%s", after)
	}

	// 5. The read-modify-write round trip that IS the compare-and-swap still
	//    succeeds on B: re-read, edit, apply.
	updated := requireCLI(t, onB, "B re-reads and re-applies, which is the way through",
		"namespace", "update", namespace, "--description", "changed by B after re-reading")
	if !strings.Contains(updated, "changed by B after re-reading") {
		t.Errorf("a re-read update did not land:\n%s", updated)
	}
}

// adminSystemInfo is `orkctl admin info --json`, decoded.
type adminSystemInfo struct {
	EngineVersion    string `json:"engineVersion"`
	GoVersion        string `json:"goVersion"`
	DeploymentName   string `json:"deploymentName"`
	BuildID          string `json:"buildId"`
	VersioningActive bool   `json:"versioningActive"`
	ArtifactBackend  string `json:"artifactBackend"`
	ArtifactLocation string `json:"artifactLocation"`
	TemporalAddress  string `json:"temporalAddress"`
	ServerTime       string `json:"serverTime"`
}

func decodeSystemInfo(t *testing.T, stdout string) adminSystemInfo {
	t.Helper()
	var info adminSystemInfo
	if err := json.Unmarshal([]byte(stdout), &info); err != nil {
		t.Fatalf("admin info --json is not valid JSON: %v\n%s", err, stdout)
	}
	return info
}

// adminPoolList is `orkctl pool list --json`, decoded.
type adminPoolList struct {
	Pools []struct {
		Name           string   `json:"name"`
		LiveOrkerCount int      `json:"liveOrkerCount"`
		Plugins        []string `json:"plugins"`
		IsDefault      bool     `json:"isDefault"`
		LastSeen       string   `json:"lastSeen"`
	} `json:"pools"`
	DefaultPool string `json:"defaultPool"`
}

func (l adminPoolList) byName(name string) *struct {
	Name           string   `json:"name"`
	LiveOrkerCount int      `json:"liveOrkerCount"`
	Plugins        []string `json:"plugins"`
	IsDefault      bool     `json:"isDefault"`
	LastSeen       string   `json:"lastSeen"`
} {
	for i := range l.Pools {
		if l.Pools[i].Name == name {
			return &l.Pools[i]
		}
	}
	return nil
}

func decodePoolList(t *testing.T, stdout string) adminPoolList {
	t.Helper()
	var listing adminPoolList
	if err := json.Unmarshal([]byte(stdout), &listing); err != nil {
		t.Fatalf("pool list --json is not valid JSON: %v\n%s", err, stdout)
	}
	return listing
}

// adminNamespaceDocument is `orkctl namespace get --json`, decoded.
type adminNamespaceDocument struct {
	Namespace struct {
		Name         string   `json:"name"`
		DefaultPool  string   `json:"defaultPool"`
		AllowedPools []string `json:"allowedPools"`
		Etag         string   `json:"etag"`
	} `json:"namespace"`
}

func decodeNamespaceDocument(t *testing.T, stdout string) adminNamespaceDocument {
	t.Helper()
	var document adminNamespaceDocument
	if err := json.Unmarshal([]byte(stdout), &document); err != nil {
		t.Fatalf("namespace get --json is not valid JSON: %v\n%s", err, stdout)
	}
	return document
}

// adminPluginList is `orkctl plugin list --json`, decoded.
type adminPluginList struct {
	Plugins []struct {
		Name        string `json:"name"`
		Version     string `json:"version"`
		Description string `json:"description"`
	} `json:"plugins"`
}

func (l adminPluginList) byName(name string) *struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
} {
	for i := range l.Plugins {
		if l.Plugins[i].Name == name {
			return &l.Plugins[i]
		}
	}
	return nil
}

func decodePluginList(t *testing.T, stdout string) adminPluginList {
	t.Helper()
	var listing adminPluginList
	if err := json.Unmarshal([]byte(stdout), &listing); err != nil {
		t.Fatalf("plugin list --json is not valid JSON: %v\n%s", err, stdout)
	}
	return listing
}

// awaitLivePoolsOnB polls B's pool health until every named pool reports at
// least the Orkers it was given.
//
// The wait is for the CLUSTER, not for B: Temporal records a poller a moment
// after a worker starts, and until it does, "no Orkers" is the honest answer.
// B is asked from the first attempt, and every attempt is B answering for
// pools it never dispatched to.
//
// It waits for a COUNT, not merely for presence, because "at least one Orker"
// is the case that already worked. Two Orkers on one pool is the case this
// test exists to hold, and a poll that stopped at the first would report green
// while the second Orker was still registering.
func awaitLivePoolsOnB(
	t *testing.T,
	onB func(args ...string) (string, string, int),
	namespace string,
	want map[string]int,
) adminPoolList {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var last adminPoolList
	for time.Now().Before(deadline) {
		stdout, stderr, code := onB("--json", "pool", "list", "-n", namespace)
		if code != 0 {
			t.Fatalf("pool list on B exited %d:\n%s\n%s", code, stdout, stderr)
		}
		last = decodePoolList(t, stdout)
		ready := true
		for pool, orkers := range want {
			found := last.byName(pool)
			if found == nil || found.LiveOrkerCount < orkers {
				ready = false
				break
			}
		}
		if ready {
			return last
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("B never reported %v: %+v", want, last)
	return last
}

// writeAdminProbeOrkflow writes a one-task orkflow pinned to this run's pool.
// It is never executed: it exists so the namespace HOLDS something, which is
// what makes the delete refusal a real refusal.
func writeAdminProbeOrkflow(t *testing.T, name, pool string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".ork.yaml")
	document := fmt.Sprintf(`name: %s
version: "1.0.0"
description: M11.3 admin surface probe
tasks:
  - id: report
    uses: shell
    pool: %s
    with:
      op: exec
      command: ["sh", "-c", "echo admin"]
`, name, pool)
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// publishAdminTranscript records the session when the milestone's reproduce.sh
// asks for one (ORK_PROOF_DIR). A plain `go test -tags=integration ./...`
// writes nothing into the tree.
func publishAdminTranscript(t *testing.T, body string) {
	t.Helper()
	dir := os.Getenv(envProofDir)
	if dir == "" {
		t.Logf("%s is unset: the admin session was logged, not published.", envProofDir)
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Logf("cannot create %s: %v", dir, err)
		return
	}
	header := fmt.Sprintf(
		"# M11.3: the admin surface answered by a replica that started nothing (%s)\n"+
			"#\n"+
			"# Two real `ork` processes share one Temporal namespace, one execution\n"+
			"# channel and one MinIO bucket. Three real `orker` processes serve two\n"+
			"# pools: two Orkers on the deployment's default pool, one on a second\n"+
			"# pool it does not route to by default.\n"+
			"#\n"+
			"# Lines marked #A are issued against replica A, which performed every\n"+
			"# SETUP write: it registered the namespace and applied the orkflow. Lines\n"+
			"# marked #B are issued against replica B, which registered no namespace,\n"+
			"# applied no orkflow and started no run, so every READ it answers comes\n"+
			"# from the cluster or the bucket (AGENTS.md §1.7) — except `admin info`,\n"+
			"# which describes B itself and says so.\n"+
			"#\n"+
			"# B does issue writes, on purpose, in the write-safety section below: a\n"+
			"# rule enforced only by the replica that happens to hold state is not a\n"+
			"# rule. Three of those are refusals; the last is the re-read-and-reapply\n"+
			"# round trip that IS the etag compare-and-swap.\n"+
			"# generated by: go test -tags=integration -p 1 ./cmd/orkctl/ \\\n"+
			"#                 -run TestIntegration_AReplicaThatStartedNothingAnswersTheAdminSurface\n\n",
		time.Now().UTC().Format(time.RFC3339))
	path := filepath.Join(dir, "admin-surface-session.txt")
	if err := os.WriteFile(path, []byte(header+body), 0o644); err != nil {
		t.Logf("cannot write %s: %v", path, err)
		return
	}
	t.Logf("wrote %s", path)
}
