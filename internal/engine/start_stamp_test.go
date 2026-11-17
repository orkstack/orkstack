package engine

import (
	"reflect"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// startStampPending names the start paths that still build a run's memo
// without its start stamp. Each is a start inside workflow code (the
// interpreter); a run it starts has no stamp and is dated by asking the
// cluster (WalkLowWater), which costs more but is never unsafe. The list only
// ever shrinks.
var startStampPending = []string{}

func TestRunStartMemoAt_DiffersOnlyInTheStartStamp(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", Namespace: "payments", DefinitionNamespace: "platform"}
	plan := RunPlan{Origin: &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-parent", RootRunID: "ork-root"}}
	at := time.Date(2026, 11, 16, 12, 0, 0, 5, time.UTC)
	old, stamped := RunStartMemo(wf, plan), RunStartMemoAt(wf, plan, at)
	if stamped[MemoStartStamp] != at.Format(time.RFC3339Nano) {
		t.Fatalf("stamp = %v", stamped[MemoStartStamp])
	}
	delete(stamped, MemoStartStamp)
	if !reflect.DeepEqual(old, stamped) {
		t.Fatalf("the stamped memo differs beyond the stamp:\n%v\n%v", old, stamped)
	}
}

// TestStartStamp_EveryRunStartCarriesIt enumerates every start of a run in the
// engine and the API: each builds its memo with RunStartMemoAt, so the artifact
// GC dates the run from its record. A new start path that calls RunStartMemo
// directly fails here unless it is named in startStampPending.
func TestStartStamp_EveryRunStartCarriesIt(t *testing.T) {
	var direct []string
	stamped := map[string]bool{}
	fset := token.NewFileSet()
	for _, dir := range []string{".", "../api", "../../cmd/ork"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				fn, ok := n.(*ast.FuncDecl)
				if ok && fn.Name.Name == "RunStartMemoAt" {
					return false // the one caller that adds the stamp
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var called string
				switch f := call.Fun.(type) {
				case *ast.Ident:
					called = f.Name
				case *ast.SelectorExpr:
					called = f.Sel.Name
				}
				switch called {
				case "RunStartMemo":
					direct = append(direct, name)
				case "RunStartMemoAt":
					stamped[name] = true
				}
				return true
			})
		}
	}
	slices.Sort(direct)
	direct = slices.Compact(direct)
	// A file may build the unstamped memo in a pure helper only if the same
	// file stamps the options it starts with (compose.go's childWorkflowOptions).
	direct = slices.DeleteFunc(direct, func(name string) bool { return stamped[name] })
	pending := slices.Clone(startStampPending)
	slices.Sort(pending)
	if !slices.Equal(direct, pending) {
		t.Fatalf("run starts without the start stamp: %v; want only the pending %v. "+
			"Build a run's memo with RunStartMemoAt, and drop a path from startStampPending once it does.", direct, pending)
	}
}
