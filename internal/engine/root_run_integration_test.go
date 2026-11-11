//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// requireOrigin fails unless origin names parent and root.
func requireOrigin(t *testing.T, label string, origin *orkstackv1.RunOrigin, parent, root string) {
	t.Helper()
	if origin.GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN ||
		origin.GetId() != parent || origin.GetRootRunId() != root {
		t.Fatalf("%s: origin = %v, want parent %s and root %s", label, origin, parent, root)
	}
}

// listedIDs is the ids of runs, sorted, for comparing with what a list should
// hold whatever its order.
func listedIDs(runs []*orkstackv1.Run) string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.GetId())
	}
	sort.Strings(ids)
	return strings.Join(ids, " ")
}

// TestIntegration_ThreeLevelChainNamesItsRoot is the chain on real containers:
// top composes mid composes leaf, and leaf, which only its parent ever started,
// holds top's id, in its live snapshot and in its archived body, found through
// the origin index. One query returns the runs below top from the cluster's
// index, by the real attribute, and again from the archive, by the real root
// row. A chain accepted before the field is in neither. The real histories then
// replay against this build, with the root recorded in their child starts.
func TestIntegration_ThreeLevelChainNamesItsRoot(t *testing.T) {
	e := startEachE2E(t, true)
	e.startOrker("root-chain")
	rootIndexed, err := task.EnsureRunRootSearchAttribute(e.ctx, e.temporal, e.namespace)
	if err != nil || !rootIndexed {
		t.Fatalf("register the root attribute: %v, %v", rootIndexed, err)
	}
	reader := engine.NewTemporalRunReader(e.temporal).WithSnapshotStore(e.snapshots).WithRootIndex(true)
	leaf := e.composedChild("chain-leaf", "")
	mid := e.composedParent("chain-mid", leaf)
	top := e.composedParent("chain-top", mid)

	topID := fmt.Sprintf("ork-chain-%d", time.Now().UnixNano())
	midID, leafID := topID+"-mid", topID+"-leaf"
	leafPlan, err := engine.NewChildRunPlan(leaf, e.plan(), false, false)
	must.OK(t, err)
	leafPlan.RunID = leafID
	nested := e.plan()
	nested.Children = map[string]*engine.ChildRunPlan{"release": leafPlan}
	midPlan, err := engine.NewChildRunPlan(mid, nested, false, false)
	must.OK(t, err)
	midPlan.RunID = midID
	plan := e.plan()
	plan.RecordRootRun = true
	plan.RootRunIndexed = true
	plan.Children = map[string]*engine.ChildRunPlan{"release": midPlan}
	starter := engine.NewTemporalStarterWithOrkerQueue(e.temporal, e.runQueue, e.orkerQueue)
	must.OKf(t, starter.StartRun(e.ctx, topID, top, nil, plan), "StartRun")

	// A chain as a run accepted before the field starts one: its plan does not
	// ask, so its child holds no root and has no attribute or row.
	olderTop := e.composedParent("older-top", leaf)
	olderID := topID + "-older"
	olderLeafID := olderID + "-leaf"
	olderLeafPlan, err := engine.NewChildRunPlan(leaf, e.plan(), false, false)
	must.OK(t, err)
	olderLeafPlan.RunID = olderLeafID
	older := e.plan()
	older.Children = map[string]*engine.ChildRunPlan{"release": olderLeafPlan}
	must.OKf(t, starter.StartRun(e.ctx, olderID, olderTop, nil, older), "StartRun older")

	// Top first: a child does not exist until its parent starts it, and a
	// finished parent has finished its children.
	ids := []string{topID, midID, leafID}
	for _, id := range append(append([]string{}, ids...), olderID, olderLeafID) {
		if run := e.wait(id); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %s ended %v: %v", id, run.GetStatus(), run.GetError())
		}
	}
	if origin := e.wait(topID).GetOrigin(); origin.GetRootRunId() != "" {
		t.Fatalf("the top-level run holds a root: %v", origin)
	}
	requireOrigin(t, "mid, live", e.wait(midID).GetOrigin(), topID, topID)
	requireOrigin(t, "leaf, live", e.wait(leafID).GetOrigin(), midID, topID)
	requireOrigin(t, "a child of an older run", e.wait(olderLeafID).GetOrigin(), olderID, "")

	// Live: the cluster selects by the attribute, which it holds only because
	// the child starts stamped it. Visibility is eventually consistent, so this
	// reads to a deadline; the older chain's query runs after it has caught up.
	want := listedIDs([]*orkstackv1.Run{{Id: leafID}, {Id: midID}})
	var live []*orkstackv1.Run
	for deadline := time.Now().Add(30 * time.Second); ; {
		live, err = reader.ListRuns(e.ctx, engine.RunQuery{RootRunID: topID, Limit: 50, Complete: true})
		if err == nil && listedIDs(live) == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the chain from the cluster's index = %q (%v), want %q", listedIDs(live), err, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, id := range []string{olderID, topID} {
		if got, err := reader.ListRuns(e.ctx, engine.RunQuery{RootRunID: id, Limit: 50, Complete: true}); err != nil || (id == olderID && len(got) != 0) {
			t.Fatalf("the runs below %s from the cluster's index = %q, %v", id, listedIDs(got), err)
		}
	}

	store, err := artifacts.NewRunArchive(e.cfg)
	must.OKf(t, err, "open run archive")
	archiver := engine.NewRunArchiver(store)
	// Each child's archive precedes its parent's, as their terminal events do.
	for _, id := range []string{leafID, midID, topID, olderLeafID, olderID} {
		snapshot, ok := e.runs.Get(id)
		if !ok {
			t.Fatalf("no snapshot of %s", id)
		}
		must.OKf(t, archiver.Archive(e.ctx, snapshot, e.bus.Events(id, 0)), "archive %s", id)
	}
	archived, err := archiver.ListRuns(e.ctx, engine.ArchiveQuery{
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, OriginID: midID,
	})
	if err != nil || len(archived) != 1 || archived[0].GetId() != leafID {
		t.Fatalf("archived children of %s = %v (%v)", midID, archived, err)
	}
	requireOrigin(t, "leaf, archived row", archived[0].GetOrigin(), midID, topID)
	loaded, err := archiver.LoadRun(e.ctx, leafID)
	must.OKf(t, err, "load archived leaf")
	requireOrigin(t, "leaf, archived body", loaded.GetOrigin(), midID, topID)

	// Archived: the same one query, answered by the root rows.
	chain, err := archiver.ListRuns(e.ctx, engine.ArchiveQuery{RootRunID: topID})
	if err != nil || listedIDs(chain) != want {
		t.Fatalf("the chain from the archive = %q (%v), want %q", listedIDs(chain), err, want)
	}
	if none, err := archiver.ListRuns(e.ctx, engine.ArchiveQuery{RootRunID: olderID}); err != nil || len(none) != 0 {
		t.Fatalf("the older chain from the archive = %q (%v), want none", listedIDs(none), err)
	}

	for _, id := range ids {
		must.OKf(t, replayRecorded(e.history(id), id), "replaying %s, which recorded the root, against this build", id)
	}
	proof.Write(t, "engine", "root-run-chain.txt", []byte(fmt.Sprintf(
		"top=%s mid=%s leaf=%s\nleaf live: %v\nleaf archived: %v\nchain, live: %s\nchain, archived: %s\nall three histories replayed\n",
		topID, midID, leafID, e.wait(leafID).GetOrigin(), loaded.GetOrigin(), listedIDs(live), listedIDs(chain))))
}

const (
	carriedTopDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: carried-top
version: "1.0.0"
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: release
    uses: orkflow:carried-mid@1.0.0
    needs: [b]
`
	carriedMidDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: carried-mid
version: "1.0.0"
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: release
    uses: orkflow:carried-leaf@1.0.0
    needs: [b]
`
	carriedLeafDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: carried-leaf
version: "1.0.0"
tasks:
  - id: a
    uses: shell
    with: {op: exec}
`
)

// stampComposedDigest stamps child's digest into the composed task of parent,
// as apply does, and then parent's own.
func stampComposedDigest(t *testing.T, parent, child *orkstackv1.Orkflow) {
	t.Helper()
	for _, tk := range parent.GetTasks() {
		if tk.GetOrkflow() != nil {
			tk.Orkflow.DefinitionDigest = child.GetDefinitionDigest()
		}
	}
	stampDigest(t, parent)
}

// TestIntegration_RootAttributeRefusedAtTheClusterLimit is the deployment that
// cannot add the attribute, on a real cluster at its limit of custom keyword
// attributes: boot's answer is false and names the attribute, and the origin
// attributes it holds keep working. TestIntegration_RootSurvivesContinuing runs a
// chain on that answer.
func TestIntegration_RootAttributeRefusedAtTheClusterLimit(t *testing.T) {
	e := startEachE2E(t, false)
	originIndexed, err := task.EnsureRunOriginSearchAttributes(e.ctx, e.temporal, e.namespace)
	if err != nil || !originIndexed {
		t.Fatalf("register the origin attributes: %v, %v", originIndexed, err)
	}
	// Fill the namespace's keyword attributes one at a time until the cluster
	// refuses. A cluster that never does cannot show this.
	filled := 0
	for ; filled < 64; filled++ {
		_, err := e.temporal.OperatorService().AddSearchAttributes(e.ctx, &operatorservice.AddSearchAttributesRequest{
			Namespace:        e.namespace,
			SearchAttributes: map[string]enumspb.IndexedValueType{fmt.Sprintf("RootLimitProbe%d", filled): enumspb.INDEXED_VALUE_TYPE_KEYWORD},
		})
		if err != nil {
			break
		}
	}
	if filled == 64 {
		t.Fatal("the cluster took 64 custom keyword attributes and never refused one: this test proves nothing on it")
	}
	t.Logf("the cluster took %d custom keyword attributes beside the two origin ones, and refused the next", filled)

	rootIndexed, err := task.EnsureRunRootSearchAttribute(e.ctx, e.temporal, e.namespace)
	if rootIndexed || err == nil || !strings.Contains(err.Error(), task.SearchAttrOrkflowOriginRootRunId) {
		t.Fatalf("the root attribute at the limit = %v, %v; want unusable, naming it", rootIndexed, err)
	}
	if again, err := task.EnsureRunOriginSearchAttributes(e.ctx, e.temporal, e.namespace); err != nil || !again {
		t.Fatalf("the origin attributes at the limit = %v, %v; want still usable", again, err)
	}
}

// TestIntegration_RootSurvivesContinuing has the top run and the middle run
// each carry on before starting its child, so a later generation decides the
// child's root: top's id as read from the workflow id, whose generation has an
// empty run-id argument, and mid's copy from the plan that crossed with it. A
// live listing row reads the root from the memo the switch copied onward, and
// the archived body written by the projection holds it. Every generation of
// the chain replays.
//
// It is also the deployment that cannot add the attribute: the namespace does not
// have it, so a plan pins false and the child starts stamp nothing, and the chain
// still runs and is still found by its root, from the memo and the root rows.
func TestIntegration_RootSurvivesContinuing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "rootcan", false, canFixtureBehavior)
	described, err := s.client.OperatorService().ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: s.namespace})
	must.OK(t, err)
	if _, present := described.GetCustomAttributes()[task.SearchAttrOrkflowOriginRootRunId]; present {
		t.Fatal("the namespace has the root attribute: this test proves nothing about a deployment without it")
	}

	leaf := compile(t, carriedLeafDocument)
	stampDigest(t, leaf)
	mid := compile(t, carriedMidDocument)
	stampComposedDigest(t, mid, leaf)
	top := compile(t, carriedTopDocument)
	stampComposedDigest(t, top, mid)

	topID := fmt.Sprintf("ork-carried-%d", time.Now().UnixNano())
	midID, leafID := topID+"-mid", topID+"-leaf"
	childPlan := func() engine.RunPlan {
		plan := rawPlan(s.orkerQueue)
		plan.CommitRoot = "mem://commits"
		return plan
	}
	leafPlan, err := engine.NewChildRunPlan(leaf, childPlan(), false, false)
	must.OK(t, err)
	leafPlan.RunID = leafID
	nested := childPlan()
	nested.Children = map[string]*engine.ChildRunPlan{"release": leafPlan}
	midPlan, err := engine.NewChildRunPlan(mid, nested, false, false)
	must.OK(t, err)
	midPlan.RunID = midID
	budget := engine.DefaultContinuationBudget(2, 1<<20, time.Minute)
	plan := s.plan(engine.DefaultHistoryMaxEvents, &budget)
	plan.RecordRootRun = true
	plan.Children = map[string]*engine.ChildRunPlan{"release": midPlan}
	must.OKf(t, s.starter.StartRun(ctx, topID, top, nil, plan), "StartRun")
	must.OKf(t, s.client.GetWorkflow(ctx, topID, "").Get(ctx, nil), "the chain failed")

	// Every generation's history, and proof that both runs did carry on.
	for _, id := range []string{topID, midID} {
		described, err := s.client.DescribeWorkflowExecution(ctx, id, "")
		must.OK(t, err)
		chain := s.chainOf(id, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
		if len(chain) < 2 {
			t.Fatalf("run %s never carried on: the test proves nothing about a later generation", id)
		}
		for _, execution := range chain {
			must.OKf(t, replayRecorded(s.history(id, execution), id), "replaying %s/%s", id, execution)
		}
	}
	must.OKf(t, replayRecorded(s.history(leafID, ""), leafID), "replaying %s", leafID)

	got, err := s.reader.GetRun(ctx, leafID)
	must.OK(t, err)
	requireOrigin(t, "leaf, GetRun", got.GetOrigin(), midID, topID)
	got, err = s.reader.GetRun(ctx, midID)
	must.OK(t, err)
	requireOrigin(t, "mid, GetRun", got.GetOrigin(), topID, topID)

	// Visibility is eventually consistent, so this reads to a deadline. A row
	// is the shallow projection, which holds only what the memo holds.
	rows := map[string]*orkstackv1.RunOrigin{}
	var (
		chainIDs = []string{topID, midID, leafID}
		listErr  error
	)
	deadline := time.Now().Add(30 * time.Second)
	for {
		listed, err := s.reader.ListRuns(ctx, engine.RunQuery{Limit: 100, Complete: true})
		listErr = err
		for _, run := range listed {
			for _, id := range chainIDs {
				if run.GetId() == id && run.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
					rows[id] = run.GetOrigin()
				}
			}
		}
		if len(rows) == len(chainIDs) || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(rows) != len(chainIDs) {
		t.Fatalf("listed %d of the chain's finished runs (%v): %v", len(rows), listErr, rows)
	}
	requireOrigin(t, "mid, listed after carrying on", rows[midID], topID, topID)
	requireOrigin(t, "leaf, listed", rows[leafID], midID, topID)
	if root := rows[topID].GetRootRunId(); root != "" {
		t.Fatalf("the top-level run lists root %q", root)
	}

	var body *orkstackv1.Run
	for deadline := time.Now().Add(30 * time.Second); ; {
		body, err = s.archive.LoadRun(ctx, leafID)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	must.OKf(t, err, "the leaf was never archived")
	requireOrigin(t, "leaf, archived", body.GetOrigin(), midID, topID)

	want := listedIDs([]*orkstackv1.Run{{Id: leafID}, {Id: midID}})
	live, err := s.reader.ListRuns(ctx, engine.RunQuery{RootRunID: topID, Limit: 50, Complete: true})
	if err != nil || listedIDs(live) != want {
		t.Fatalf("the chain with no attribute = %q (%v), want %q", listedIDs(live), err, want)
	}
	for deadline := time.Now().Add(30 * time.Second); ; {
		archived, err := s.archive.ListRuns(ctx, engine.ArchiveQuery{RootRunID: topID})
		if err == nil && listedIDs(archived) == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the chain from the archive = %q (%v), want %q", listedIDs(archived), err, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
