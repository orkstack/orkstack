package engine_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// One constructor gives an engine's client everything payload encryption
// needs, and every worker built from that client its interceptor; what the
// caller set stays.
func TestClientOptions_CarriesThePayloadConverters(t *testing.T) {
	key, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	must.OK(t, err)
	keys, err := payloadcrypt.NewEngineKeys(key)
	must.OK(t, err)
	own := &interceptor.ClientInterceptorBase{}
	for name, ek := range map[string]*payloadcrypt.EngineKeys{"no keys": nil, "keys": keys} {
		t.Run(name, func(t *testing.T) {
			o := engine.ClientOptions(client.Options{Namespace: "ns", Interceptors: []interceptor.ClientInterceptor{own}}, ek)
			if o.Namespace != "ns" || len(o.Interceptors) != 2 || o.Interceptors[0] != own {
				t.Fatalf("options = %+v, want the caller's own kept first", o)
			}
			if _, ok := o.Interceptors[1].(interceptor.Interceptor); !ok {
				t.Fatalf("%T is no worker interceptor: workers of this client would run without it", o.Interceptors[1])
			}
			if _, ok := o.DataConverter.(*payloadcrypt.Converter); !ok {
				t.Fatalf("data converter = %T", o.DataConverter)
			}
			// A process that holds keys seals: an unmarked error is withheld.
			f := o.FailureConverter.ErrorToFailure(temporal.NewApplicationError("text", "T"))
			if withheld := f.GetMessage() == payloadcrypt.Withheld; withheld != (ek != nil) {
				t.Fatalf("an unmarked error's message = %q with keys %v", f.GetMessage(), ek != nil)
			}
		})
	}
}

// Every client a Go test of the engine, its API or its binary dials is dialed
// with engine.ClientOptions, as cmd/ork dials: a harness runs what production
// runs.
func TestClientOptions_EveryHarnessDialsWithThem(t *testing.T) {
	dials := 0
	for _, dir := range []string{".", "../api", "../../cmd/ork"} {
		files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
		must.OK(t, err)
		for _, f := range files {
			src, err := os.ReadFile(f)
			must.OK(t, err)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, f, src, 0)
			must.OK(t, err)
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "client" {
					return true
				}
				switch sel.Sel.Name {
				case "Dial", "DialContext", "NewLazyClient", "NewClientFromExisting", "NewClientFromExistingWithContext":
				default:
					return true
				}
				dials++
				arg := string(src[fset.Position(call.Args[len(call.Args)-1].Pos()).Offset:fset.Position(call.Args[len(call.Args)-1].End()).Offset])
				if !strings.HasPrefix(arg, "engine.ClientOptions(") && !strings.HasPrefix(arg, "ClientOptions(") {
					t.Errorf("%s: %s dials without engine.ClientOptions", fset.Position(call.Pos()), sel.Sel.Name)
				}
				return true
			})
		}
	}
	if dials == 0 {
		t.Fatal("no dial found")
	}
}

// Which workflows' local-activity errors a process that seals writes as they
// are, where the execution carries no key, is a decision: a new one is made
// here, never by default.
func TestClientOptions_UnsealedErrorTypesAreTheListed(t *testing.T) {
	want := "ConcurrencyCensusWorkflow,ConcurrencyGateWorkflow,RunWorkflow,ScheduleFireWorkflow"
	got := engine.UnsealedErrorTypesForTest()
	slices.Sort(got)
	if strings.Join(got, ",") != want {
		t.Fatalf("unsealed error types %v, want %s", got, want)
	}
}
