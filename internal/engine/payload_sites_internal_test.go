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
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				switch r := fn.Recv.List[0].Type.(type) {
				case *ast.StarExpr:
					name = "(*" + r.X.(*ast.Ident).Name + ")." + name
				case *ast.Ident:
					name = r.Name + "." + name
				}
			}
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
	want := []string{"IndexEachLines", "ReleasePin", "internalSessionCompletionActivity", "internalSessionCreationActivity"}
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
	"each_lines.go:(*runState).startEachLines ExecuteActivity":                "IndexEachLines: plainActivities",
	"workflow.go:(*runState).hostPin ExecuteActivity":                         "ReleasePin: plainActivities",
	"workflow.go:(*runState).emitTaskEvent ExecuteActivity":                   "PublishEvent: sealed; the engine opens it with the run's key",
	"lean_events.go:(*runState).publishDetached ExecuteActivity":              "PublishEvent: sealed; the engine opens it with the run's key",
	"each.go:(*runState).emitItem ExecuteActivity":                            "PublishEvent: sealed; the engine opens it with the run's key",
	"continuation.go:(*runState).endUnrestorable ExecuteActivity":             "PublishEvent: sealed; the engine opens it with the run's key",
	"payload_start.go:keylessEnding ExecuteActivity":                          "PublishEvent of a keyless run: not sealed, holds the memo's plaintext only",
	"webhook_delivery.go:WebhookDeliveryWorkflow ExecuteActivity":             "a webhook delivery is not a sealed run",
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
