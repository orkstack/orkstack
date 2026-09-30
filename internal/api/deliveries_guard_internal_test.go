package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestDeliveryRecords_NothingReadsThemToDecideAnything is the structural
// guardrail that keeps delivery records a LOG and never a queue: a record is
// read to answer an operator and for nothing else.
//
// It checks, over every non-test Go file of the module:
//
//  1. pkg/webhooks — the notifier and the dispatcher — imports neither
//     pkg/artifacts nor internal/api, so nothing that SENDS can reach a
//     record;
//  2. DeliveryStore's method set is exactly Put, Get and List — no update,
//     delete, claim or lease a scheduler could be built on;
//  3. a delivery store is named (type or constructor) outside pkg/artifacts
//     only by the recorder and RPCs (internal/api/deliveries.go), the Service
//     that holds one (internal/api/service.go) and the wiring
//     (cmd/ork/main.go);
//  4. in those files, the only calls that READ records — ListDeliveries and
//     GetDelivery on the store — are the two RPC handlers' (ListDeliveries,
//     and readDelivery, which GetDelivery and RedeliverWebhook share).
//
// A package that never names a delivery store can still call a method named
// ListDeliveries: that is the Connect client (orkctl), which reads through
// the RPC and is therefore the handler's caller, not the store's.
func TestDeliveryRecords_NothingReadsThemToDecideAnything(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("module root: %v", err)
	}
	const module = "github.com/orkstack/orkstack/"

	storeType := reflect.TypeOf((*artifacts.DeliveryStore)(nil)).Elem()
	var methods []string
	for i := range storeType.NumMethod() {
		methods = append(methods, storeType.Method(i).Name)
	}
	if want := []string{"GetDelivery", "ListDeliveries", "PutDelivery"}; !slices.Equal(methods, want) {
		t.Errorf("DeliveryStore methods = %v, want exactly %v: a record has one state, written", methods, want)
	}

	storeNames := map[string]bool{"DeliveryStore": true, "NewDeliveryStore": true, "NewMemoryDeliveryStore": true,
		"NewLocalDeliveryStore": true, "NewS3DeliveryStore": true}
	allowedHolders := map[string]bool{
		"internal/api/deliveries.go": true, "internal/api/service.go": true, "cmd/ork/main.go": true,
	}
	readHandlers := map[string]bool{"ListDeliveries": true, "readDelivery": true}

	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			switch {
			case rel == "gen", rel == "ui", rel == "proof", strings.HasPrefix(entry.Name(), "."),
				entry.Name() == "node_modules", entry.Name() == "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		if strings.HasPrefix(rel, "pkg/webhooks/") {
			for _, spec := range file.Imports {
				imported, _ := strconv.Unquote(spec.Path.Value)
				if imported == module+"pkg/artifacts" || imported == module+"internal/api" {
					t.Errorf("%s imports %s: the delivery path must not be able to reach a record", rel, imported)
				}
			}
		}
		if strings.HasPrefix(rel, "pkg/artifacts/") {
			return nil
		}
		namesStore := false
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if storeNames[node.Sel.Name] {
					namesStore = true
				}
			case *ast.Ident:
				if storeNames[node.Name] && strings.HasPrefix(rel, "internal/api/") {
					namesStore = true
				}
			}
			return true
		})
		if namesStore && !allowedHolders[rel] {
			t.Errorf("%s names a delivery store; only the recorder, the RPCs and the wiring may hold one", rel)
		}
		if !namesStore {
			return nil
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "ListDeliveries" && sel.Sel.Name != "GetDelivery") {
					return true
				}
				if !readHandlers[fn.Name.Name] || rel != "internal/api/deliveries.go" {
					t.Errorf("%s: %s calls %s; only the ListDeliveries and GetDelivery handlers read records",
						fset.Position(call.Pos()), fn.Name.Name, sel.Sel.Name)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}
