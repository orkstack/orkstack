package engine

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// plainSites is every function of the engine that writes a value plaintext
// in a sealed run, and why. In a sealed run's workflow code every other value
// is sealed under the run's key by default (the user's decision, round 20,
// E1, 2026-11-20), so this list is the whole of what a sealed run writes in
// plaintext (beside the search attributes and the memo the start sets).
var plainSites = map[string]string{
	"concurrency_pause.go:(*runState).leaveRegainLine (memo)":     "the slot record's removal",
	"concurrency_pause.go:(*runState).leaveRequeue (memo)":        "the slot record's removal",
	"concurrency_run.go:releaseSlots":                             "a gate release: the run id and a sequence, read by a gate that holds no key",
	"concurrency_pause.go:(*runState).releaseRunEntry":            "a gate release: the run id, a sequence and indices",
	"concurrency_run.go:(*runState).recordRunSlot (memo)":         "the run's slot record, from which a gate is rebuilt",
	"concurrency_run.go:(*runState).restoreConcRecords (memo)":    "the run's slot record, from which a gate is rebuilt",
	"concurrency_pause.go:(*runState).releaseRunEntry (memo)":     "the slot record's removal",
	"payload_signals.go:plainMemo":                                "a memo is what every list and count is answered from",
	"continuation.go:(*runState).continueAsNew (memo)":            "the run's created_at memo field",
	"pause.go:(*runState).syncPauseIndex (memo)":                  "the pause's expiry memo field",
	"compose.go:(*runState).startChild (memo)":                    "a child run's start memo",
	"schedule_fire.go:ScheduleFireWorkflow (memo)":                "the fire's memo fields and its run's start memo",
	"schedule_fire.go:refuseFire (memo)":                          "a refused fire's reason memo field: fixed words",
	"schedule_fire.go:completeNotStarted (memo)":                  "a fire's skip code, a closed vocabulary, and its message: fixed words",
	"resume_holder.go:loadResumeAtR (memo)":                       "the run's created_at memo field: a time",
	"schedule_fire.go:ScheduleFireWorkflow":                       "a fire's run start: never under the schedule's key (P20b hands a sealed start on)",
	"payload_start.go:keylessEnding":                              "a keyless run's one event: fixed words, and its memo's plaintext",
	"continuation.go:(*runState).endUnrestorable":                 "an event publish's dedupe key: the run id and a sequence",
	"payload_signals.go:(*plainActivityOutbound).ExecuteActivity": "plainActivities: their workers read their input with no run key",
	"workflow.go:(*runState).emitTaskEvent":                       "an event publish's dedupe key: the run id and a sequence",
	"lean_events.go:(*runState).publishDetached":                  "an event publish's dedupe key: the run id and a sequence",
	"each.go:(*runState).emitItem":                                "an event publish's dedupe key: the run id and a sequence",
}

// TestPayloads_EveryValueSiteDecides audits the Plain list: every call of
// payloadcrypt.Plain, PlainIn, PlainBinary or plainMemo in the engine's
// non-test code is in plainSites, and every entry of plainSites still makes
// one. A new plaintext site is a decision this list records.
func TestPayloads_EveryValueSiteDecides(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := siteName(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					if pkg, ok := fun.X.(*ast.Ident); ok && pkg.Name == "payloadcrypt" &&
						(fun.Sel.Name == "Plain" || fun.Sel.Name == "PlainIn" || fun.Sel.Name == "PlainBinary") {
						found[f+":"+name] = true
					}
				case *ast.Ident:
					if fun.Name == "plainMemo" {
						found[f+":"+name+" (memo)"] = true
					}
				}
				return true
			})
		}
	}
	var unlisted, stale []string
	for site := range found {
		if _, ok := plainSites[site]; !ok {
			unlisted = append(unlisted, site)
		}
	}
	for site := range plainSites {
		if !found[site] {
			stale = append(stale, site)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(stale)
	if len(unlisted) > 0 || len(stale) > 0 {
		t.Fatalf("the Plain list is out of step: unlisted plaintext sites %v; listed sites that make none %v", unlisted, stale)
	}
}

// The activities whose arguments a sealed run writes plaintext are exactly
// these.
func TestPayloads_plainActivitiesAreTheListed(t *testing.T) {
	want := []string{"ReleasePin", "internalSessionCompletionActivity", "internalSessionCreationActivity"}
	var got []string
	for a := range plainActivities {
		got = append(got, a)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("plainActivities = %v, want %v", got, want)
	}
}

// A sealed parent's pause cascade is sealed for its child's own id, so the
// child opens it; an unsealed parent's is the request itself.
func TestPauseCascade_SealedForTheChild(t *testing.T) {
	key, err := payloadcrypt.NewKey(bytes.Repeat([]byte{6}, payloadcrypt.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	parent := payloadcrypt.NewScope(key, payloadcrypt.RunContext("ork-parent"))
	s := &runState{scope: parent, children: map[string]*childState{"release": {plan: &ChildRunPlan{RunID: "ork-child"}}}}
	sig := &orkstackv1.RunPauseSignal{}
	p, err := NewDataConverter(nil).ToPayload(s.sealedForChild("release", sig))
	if err != nil || !payloadcrypt.IsSealed(p) {
		t.Fatalf("cascade payload = %v, %v", p, err)
	}
	if _, err := parent.For(payloadcrypt.RunContext("ork-child")).Open(p); err != nil {
		t.Fatalf("the child cannot open its cascade: %v", err)
	}
	if _, err := parent.Open(p); err == nil {
		t.Fatal("the cascade was sealed for the parent")
	}
	s.scope = payloadcrypt.Unsealed()
	if got := s.sealedForChild("release", sig); got != any(sig) {
		t.Fatalf("an unsealed parent's cascade = %T, want the request itself", got)
	}
}

// crossSites is every call in the engine's workflow code that hands a value
// to another workflow or to an activity worker, and how the reader opens it.
// Under the default seal an unmarked value is sealed for this run's own id,
// which another workflow cannot open, so each such site is a decision.
var crossSites = map[string]string{
	"concurrency_run.go:releaseSlots SignalExternalWorkflow":                  "PlainIn: the gate holds no run's key",
	"concurrency_pause.go:(*runState).releaseRunEntry SignalExternalWorkflow": "PlainIn: the gate holds no run's key",
	"pause_cascade.go:(*runState).goSend SignalChildWorkflow":                 "sealed for the child's own id (sealedForChild)",
	"compose.go:(*runState).startChild ExecuteChildWorkflow":                  "sealed for the child's own id",
	"schedule_fire.go:ScheduleFireWorkflow ExecuteChildWorkflow":              "PlainIn: never under the schedule's key",
	"concurrency_gate.go:(*gateRun).grant SignalExternalWorkflow":             "the gate is not sealed: it writes plaintext",
	"workflow.go:(*runState).schedule ExecuteActivity":                        "ExecuteTask: a raw payload, sealed by the dispatch",
	"each.go:(*runState).scheduleItem ExecuteActivity":                        "ExecuteTask: a raw payload, sealed by the dispatch",
	"each_lines.go:(*runState).startEachLines ExecuteActivity":                "IndexEachLines: sealed; the Orker opens it with the run's key",
	"workflow.go:(*runState).hostPin ExecuteActivity":                         "ReleasePin: plainActivities",
	"workflow.go:(*runState).emitTaskEvent ExecuteActivity":                   "PublishEvent: sealed; the engine opens it with the run's key",
	"lean_events.go:(*runState).publishDetached ExecuteActivity":              "PublishEvent: sealed; the engine opens it with the run's key",
	"each.go:(*runState).emitItem ExecuteActivity":                            "PublishEvent: sealed; the engine opens it with the run's key",
	"continuation.go:(*runState).endUnrestorable ExecuteActivity":             "PublishEvent: sealed; the engine opens it with the run's key",
	"payload_start.go:keylessEnding ExecuteActivity":                          "PublishEvent of a keyless run: Plain, since no reader is given its key",
	"webhook_delivery.go:WebhookDeliveryWorkflow ExecuteActivity":             "a webhook delivery is not a sealed run",
	"force_cancel_cascade_workflow.go:(*cascade).one ExecuteChildWorkflow":    "a force cancel of a run below is no run: a decision, ids and settings",
	"force_cancel_workflow.go:(*forceCancel).release SignalExternalWorkflow":  "the gate holds no run's key: a run id and a sequence",
	"force_cancel_workflow.go:(*forceCancel).publish ExecuteActivity":         "a force cancel is no run: the event says what its record does, or is the run's own ending announced again; a sealed run's is refused (errForceCancelSealed), and is said to be unpublished",
}

// TestPayloads_EveryCrossWorkflowWriteDecides: every SignalChildWorkflow,
// SignalExternalWorkflow, ExecuteChildWorkflow and ExecuteActivity call in the
// engine's non-test code is in crossSites, with how its reader opens it.
func TestPayloads_EveryCrossWorkflowWriteDecides(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	var unmarkedMemos []string
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				if r, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
					name = "(*" + r.X.(*ast.Ident).Name + ")." + name
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "UpsertMemo":
					if arg, ok := call.Args[len(call.Args)-1].(*ast.CallExpr); ok {
						if id, ok := arg.Fun.(*ast.Ident); ok && id.Name == "plainMemo" {
							break
						}
					}
					unmarkedMemos = append(unmarkedMemos, fset.Position(call.Pos()).String())
				case "SignalChildWorkflow", "SignalExternalWorkflow", "ExecuteChildWorkflow":
					found[f+":"+name+" "+sel.Sel.Name] = true
				case "ExecuteActivity":
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "workflow" {
						found[f+":"+name+" ExecuteActivity"] = true
					}
				}
				return true
			})
		}
	}
	if len(unmarkedMemos) > 0 {
		t.Errorf("memo writes that are not plainMemo's: %v", unmarkedMemos)
	}
	var unlisted, stale []string
	for site := range found {
		if _, ok := crossSites[site]; !ok {
			unlisted = append(unlisted, site)
		}
	}
	for site := range crossSites {
		if !found[site] {
			stale = append(stale, site)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(stale)
	if len(unlisted) > 0 || len(stale) > 0 {
		t.Fatalf("the cross-workflow write list is out of step: unlisted %v; stale %v", unlisted, stale)
	}
}

// localReads is every engine function that reads a local activity's result
// itself, unopened, and why. Every other reads it through getLocal, which
// opens a sealed failure before anything classifies it.
var localReads = map[string]string{
	"payload_start.go:keylessEnding":                   "a run that holds no key: nothing would open",
	"concurrency_census.go:ConcurrencyCensusWorkflow":  "the census is no run, and is never sealed",
	"concurrency_gate.go:(*gateRun).pass":              "a gate is no run, and is never sealed",
	"concurrency_gate.go:(*gateRun).listAll":           "a gate is no run, and is never sealed",
	"concurrency_gate_report.go:(*gateRun).sendReport": "a gate is no run, and is never sealed",
	"force_cancel_workflow.go:(*forceCancel).stepOn":   "a force cancel is no run, and is never sealed",
	"force_cancel_cascade_workflow.go:(*cascade).step": "a force cancel is no run, and is never sealed",
}

// TestPayloads_EveryLocalActivityReadOpens: a local activity's failure is
// written through the failure converter, sealed in a sealed run, so every
// read of one goes through getLocal or is listed.
func TestPayloads_EveryLocalActivityReadOpens(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	opened := 0
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			inGetLocal := map[ast.Node]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if fun.Name == "getLocal" {
						for _, a := range call.Args {
							inGetLocal[a] = true
						}
					}
				case *ast.SelectorExpr:
					if fun.Sel.Name == "getLocal" {
						for _, a := range call.Args {
							inGetLocal[a] = true
						}
					}
					if fun.Sel.Name == "ExecuteLocalActivity" {
						if pkg, ok := fun.X.(*ast.Ident); !ok || pkg.Name != "workflow" {
							return true
						}
						if inGetLocal[call] {
							opened++
						} else {
							found[f+":"+siteName(fn)] = true
						}
					}
				}
				return true
			})
		}
	}
	if opened == 0 {
		t.Fatal("no read through getLocal was found")
	}
	for site := range found {
		if _, ok := localReads[site]; !ok {
			t.Errorf("%s reads a local activity's result without getLocal", site)
		}
	}
	for site := range localReads {
		if !found[site] {
			t.Errorf("%s is listed and reads none", site)
		}
	}
}

// unsealedDoors are the workflow calls and options whose values the SDK
// writes, or answers, with a converter that holds no run's key, and which no
// run's workflow code uses: nothing here would seal what they were given.
var unsealedDoors = map[string]bool{
	"SideEffect": true, "SideEffectWithOptions": true, "MutableSideEffect": true, "MutableSideEffectWithOptions": true,
	"SetUpdateHandler": true, "SetUpdateHandlerWithOptions": true, "SetQueryHandler": true, "SetQueryHandlerWithOptions": true,
	"SetCurrentDetails": true, "NewTimerWithOptions": true, "AwaitWithOptions": true, "NewNexusClient": true,
	"UpsertSearchAttributes": true, "WithDataConverter": true, "SetDetails": true,
	"Summary": true, "Details": true, "StaticSummary": true, "StaticDetails": true, "EventGroups": true,
}

// otherDoors is every engine file that uses one, and why: a workflow that is
// no run, and is never sealed.
var otherDoors = map[string]string{
	"claim.go":                         "an idempotency claim is no run: it holds a run id and a fingerprint",
	"concurrency_gate.go":              "a gate is no run: it holds ids, sequences and its limit's own terms",
	"force_cancel_cascade_workflow.go": "a force cancel is no run: its cascade answers ids, Orkflow names, states and causes",
}

// TestPayloads_NoRunUsesADoorNothingSeals: a new use of one needs a decision
// here, and a test that feeds it run data.
func TestPayloads_NoRunUsesADoorNothingSeals(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		use := func(n ast.Node, name string) {
			used[f] = true
			if _, ok := otherDoors[f]; !ok {
				t.Errorf("%s: workflow.%s writes what nothing seals", fset.Position(n.Pos()), name)
			}
		}
		ofWorkflow := func(e ast.Expr) (string, bool) {
			sel, ok := e.(*ast.SelectorExpr)
			if !ok {
				return "", false
			}
			pkg, ok := sel.X.(*ast.Ident)
			return sel.Sel.Name, ok && pkg.Name == "workflow"
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CallExpr:
				if name, ok := ofWorkflow(n.Fun); ok && unsealedDoors[name] {
					use(n, name)
				}
			case *ast.CompositeLit:
				name, ok := ofWorkflow(n.Type)
				if !ok || !strings.HasSuffix(name, "Options") {
					return true
				}
				for _, el := range n.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						if key, ok := kv.Key.(*ast.Ident); ok && unsealedDoors[key.Name] {
							use(kv, name+"."+key.Name)
						}
					}
				}
			}
			return true
		})
	}
	for f := range otherDoors {
		if !used[f] {
			t.Errorf("%s is listed and uses none", f)
		}
	}
}

// siteName is fn's name with its receiver's type.
func siteName(fn *ast.FuncDecl) string {
	name := fn.Name.Name
	if fn.Recv != nil && len(fn.Recv.List) == 1 {
		switch r := fn.Recv.List[0].Type.(type) {
		case *ast.StarExpr:
			return "(*" + r.X.(*ast.Ident).Name + ")." + name
		case *ast.Ident:
			return r.Name + "." + name
		}
	}
	return name
}

// plainReads is every function of a reader that reads a payload with the
// reader's own converter, which opens nothing, and why.
var plainReads = map[string]string{
	"reader.go:(*TemporalRunReader).searchKeyword":                  "a search attribute: plaintext by design",
	"reader.go:(*TemporalRunReader).memoString":                     "a memo field: plaintext by design",
	"force_cancel_activities.go:(*ForceCancelActivities).ownEnding": "never a sealed run's, which Publish refuses: what it answers is in the force cancel's own history",
	"reader_payload.go:(*TemporalRunReader).runConverter":           "what the run's converter is made of",
}

// TestPayloads_AReaderReadsARunsPayloadsWithItsOwnConverter: what a run's
// history holds is read with the converter that opens it for the run asked
// for (runConverter); a read with the reader's own sees nothing of a sealed
// run. A new one is decided here.
func TestPayloads_AReaderReadsARunsPayloadsWithItsOwnConverter(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "converter" {
					x, ok := sel.X.(*ast.Ident)
					of, isField := sel.X.(*ast.SelectorExpr)
					if ok && x.Name == "r" || isField && of.Sel.Name == "reader" {
						found[f+":"+siteName(fn)] = true
					}
				}
				return true
			})
		}
	}
	for site := range found {
		if _, ok := plainReads[site]; !ok {
			t.Errorf("%s reads with the reader's own converter", site)
		}
	}
	for site := range plainReads {
		if !found[site] {
			t.Errorf("%s is listed and reads with none", site)
		}
	}
}
