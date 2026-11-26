package api

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// probedStore counts the probe's writes.
type probedStore struct {
	n   int
	err error
}

func (p *probedStore) ProbeWrite(context.Context) error { p.n++; return p.err }

func newProbeWorld(t *testing.T) (*startWorld, *probedStore, *time.Time) {
	t.Helper()
	w := newStartWorld(t)
	p, now := &probedStore{}, time.Unix(1000, 0)
	w.s.startProbe = p
	w.s.probes = newKeptProbes(func() time.Time { return now })
	return w, p, &now
}

func (w *startWorld) probe(t *testing.T) *orkstackv1.DoorAnswer {
	t.Helper()
	resp, err := w.s.ProbeEventSource(asHost(hostOrker), connect.NewRequest(&orkstackv1.ProbeEventSourceRequest{Instance: w.ref(4)}))
	if err != nil {
		t.Fatal(err)
	}
	return answered(t, "ProbeEventSource", resp.Msg.GetAnswer())
}

// TestProbeEventSource_ASourceThatChanged: what was refused of a document
// that has been replaced is asked again.
func TestProbeEventSource_ASourceThatChanged(t *testing.T) {
	w, _, _ := newProbeWorld(t)
	w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName = "nothing" })
	w.s.eventSources = &changingSources{Store: w.s.eventSources, change: func() {
		w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName = "deploy" })
	}, times: 1}
	if got := w.probe(t); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT || w.stored(t).GetPause() != nil {
		t.Fatalf("%v", got)
	}
	if got := w.probe(t); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
		t.Fatalf("%v", got)
	}
}

// TestProbeEventSource: each side of a start is asked, in order, once in ten
// seconds, and nothing is started, claimed or staged.
func TestProbeEventSource(t *testing.T) {
	w, p, now := newProbeWorld(t)
	for range 3 {
		if got := w.probe(t); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_OK {
			t.Fatalf("%v", got)
		}
	}
	if p.n != 1 || w.starts.asks != 1 {
		t.Fatalf("%d writes, %d reads", p.n, w.starts.asks)
	}
	*now = now.Add(probeAge)
	w.probe(t)
	if p.n != 2 || w.starts.asks != 2 || len(w.starter.plans) != 0 {
		t.Fatalf("%d writes, %d reads, %d runs", p.n, w.starts.asks, len(w.starter.plans))
	}
}

// TestProbeEventSource_WhatItFinds: the first side that does not pass is the
// answer, a message's own refusal is none, and a refusal of the source pauses
// it.
func TestProbeEventSource_WhatItFinds(t *testing.T) {
	const ok, wait, stop, uncertain = orkstackv1.Disposition_DISPOSITION_OK, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_UNCERTAIN
	for name, c := range map[string]struct {
		set   func(*testing.T, *startWorld, *probedStore)
		want  orkstackv1.Disposition
		cause string
		code  string
		asked int
	}{
		"a hold": {func(_ *testing.T, w *startWorld, _ *probedStore) {
			w.s.pools.Namespaces = namespacesOf{&orkstackv1.Namespace{Name: "default", Hold: &orkstackv1.NamespaceHold{Reason: "x"}}}
		}, wait, waitAdmission, "", 0},
		"the Orkflow gone": {func(t *testing.T, w *startWorld, _ *probedStore) {
			w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName = "nothing" })
		}, stop, "", "ORKFLOW_NOT_FOUND", 0},
		"the store down":     {func(_ *testing.T, _ *startWorld, p *probedStore) { p.err = errFault }, wait, waitOutage, "", 1},
		"the store refusing": {func(_ *testing.T, _ *startWorld, p *probedStore) { p.err = s3Status(403) }, uncertain, "", codeStoreRefused, 1},
		"the cluster down":   {func(_ *testing.T, w *startWorld, _ *probedStore) { w.starts.err = errFault }, wait, waitOutage, "", 1},
	} {
		t.Run(name, func(t *testing.T) {
			w, p, _ := newProbeWorld(t)
			c.set(t, w, p)
			for range 2 {
				got := w.probe(t)
				if got.GetDisposition() != c.want || got.GetCause() != c.cause || got.GetRefusalCode() != c.code {
					t.Fatalf("%v; want %s %s %s", got, c.want, c.cause, c.code)
				}
				if c.want == stop {
					break
				}
			}
			if p.n != c.asked {
				t.Fatalf("the store was written %d times; want %d", p.n, c.asked)
			}
		})
	}
}

// TestProbeRefuses: a message's own refusal is none of a probe's.
func TestProbeRefuses(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{{nil, false}, {refused(refusalReject, eventsources.ReasonMessageConcurrencyKeyInvalid, errFault), false},
		{refused(refusalPause, "X", errFault), true}, {refused(refusalWait, waitPool, errFault), true}, {errFault, true}} {
		if got := probeRefuses(c.err); got != c.want {
			t.Errorf("%v: %t", c.err, got)
		}
	}
}

// TestKeptProbes: callers that ask for the same at once share one probe, and
// a probe that failed is kept as one that passed is.
func TestKeptProbes(t *testing.T) {
	now := time.Unix(1000, 0)
	k := newKeptProbes(func() time.Time { return now })
	var n int
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := k.run(context.Background(), "a", func() error { n++; return errFault }); !errors.Is(err, errFault) {
				t.Errorf("%v", err)
			}
		})
	}
	wg.Wait()
	if n != 1 {
		t.Fatalf("%d probes", n)
	}
	// One that waits for another's probe leaves with its own context.
	hold, left := make(chan struct{}), make(chan error, 1)
	asked := make(chan struct{})
	go func() {
		_ = k.run(context.Background(), "b", func() error { close(asked); <-hold; return nil })
		left <- nil
	}()
	<-asked
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	if err := k.run(gone, "b", func() error { return errFault }); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	close(hold)
	<-left
	now = now.Add(-time.Hour)
	_ = k.run(context.Background(), "a", func() error { n++; return nil })
	if n != 2 {
		t.Fatal("a clock that went back kept an answer")
	}
	for i := range maxKeptProbes + 5 {
		_ = k.run(context.Background(), strconv.Itoa(i), func() error { return nil })
	}
	if len(k.kept) > maxKeptProbes {
		t.Fatalf("%d kept", len(k.kept))
	}
}

// TestRequestEventSourcePause: a breaker pauses the source, whatever else of
// it changed, and its Orker is on the pause; what is no breaker, and a code
// this service does not answer, is no request.
func TestRequestEventSourcePause(t *testing.T) {
	ask := func(w *startWorld, code, refusal string) (*orkstackv1.DoorAnswer, error) {
		resp, err := w.s.RequestEventSourcePause(asHost(hostOrker), connect.NewRequest(&orkstackv1.RequestEventSourcePauseRequest{Instance: w.ref(4), Code: code, RefusalCode: refusal}))
		if err != nil {
			return nil, err
		}
		return answered(t, "RequestEventSourcePause", resp.Msg.GetAnswer()), nil
	}
	for _, c := range [][2]string{{eventsources.PauseAllRejected, ""}, {eventsources.PauseSettleFailing, ""}, {eventsources.PauseSettleUnanswered, ""},
		{eventsources.PauseRefused, codeUnclassified}, {eventsources.PauseRefused, "RESOURCES_UNSATISFIABLE"}} {
		w := newStartWorld(t)
		got, err := ask(w, c[0], c[1])
		if err != nil || got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_STOP || got.GetReason() != eventsources.ReasonPaused || got.GetRefusalCode() != c[1] {
			t.Fatalf("%v: %v, %v", c, got, err)
		}
		if p := w.stored(t).GetPause(); p.GetCode() != c[0] || p.GetRefusalCode() != c[1] || p.GetRequestedByOrker() != hostOrker || p.GetReason() == "" {
			t.Fatalf("%v: %v", c, p)
		}
	}
	for _, c := range [][2]string{{"", ""}, {eventsources.PauseInstanceFailed, ""}, {"SOMETHING", ""}, {eventsources.PauseRefused, ""},
		{eventsources.PauseRefused, "ORKFLOW_NOT_FOUND"}} {
		w := newStartWorld(t)
		if _, err := ask(w, c[0], c[1]); connect.CodeOf(err) != connect.CodeInvalidArgument || w.stored(t).GetPause() != nil {
			t.Errorf("%v: %v", c, err)
		}
	}

	// Another breaker's pause carries no refusal code.
	w := newStartWorld(t)
	if got, err := ask(w, eventsources.PauseAllRejected, codeUnclassified); err != nil || got.GetRefusalCode() != "" || w.stored(t).GetPause().GetRefusalCode() != "" {
		t.Fatalf("%v, %v", got, err)
	}
	w = newStartWorld(t)
	store := &changingSources{Store: w.s.eventSources, change: func() {
		w.save(t, func(src *orkstackv1.EventSource) { src.Description += "x" })
	}, times: maxPauseTries - 1}
	w.s.eventSources = store
	if got, err := ask(w, eventsources.PauseAllRejected, ""); err != nil || got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_STOP {
		t.Fatalf("%v, %v", got, err)
	}
	w = newStartWorld(t)
	w.s.eventSources = &changingSources{Store: w.s.eventSources, change: func() {
		w.save(t, func(src *orkstackv1.EventSource) { src.Description += "x" })
	}, times: maxPauseTries}
	if got, err := ask(w, eventsources.PauseAllRejected, ""); err != nil || got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT || w.stored(t).GetPause() != nil {
		t.Fatalf("%v, %v", got, err)
	}
	w = newStartWorld(t)
	w.s.eventSources = unwrittenSources{w.s.eventSources}
	if got, err := ask(w, eventsources.PauseAllRejected, ""); err != nil || got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT || got.GetCause() != waitOutage {
		t.Fatalf("%v, %v", got, err)
	}
}

// changingSources is a registry whose document changes before each of the
// first writes.
type changingSources struct {
	eventsources.Store
	change func()
	times  int
}

func (c *changingSources) Replace(ctx context.Context, src *orkstackv1.EventSource, etag string) (*orkstackv1.EventSource, error) {
	if c.times > 0 {
		c.times--
		c.change()
	}
	return c.Store.Replace(ctx, src, etag)
}

// unwrittenSources is a registry that cannot be written.
type unwrittenSources struct{ eventsources.Store }

func (unwrittenSources) Replace(context.Context, *orkstackv1.EventSource, string) (*orkstackv1.EventSource, error) {
	return nil, errFault
}

// TestUncertainCodes_AreTheOnesAnswered: an Orker may ask a pause with every
// code an uncertain answer carries, and no other.
func TestUncertainCodes_AreTheOnesAnswered(t *testing.T) {
	names := map[string]string{"codeUnclassified": codeUnclassified, "codeStoreRefused": codeStoreRefused, "codeClusterRefused": codeClusterRefused,
		"engine.ReasonProfileUnavailable": engine.ReasonProfileUnavailable, "code": ""}
	answered := map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	add := func(at string, e ast.Expr) {
		if lit, ok := e.(*ast.BasicLit); ok {
			code, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatal(err)
			}
			answered[code] = true
			return
		}
		code, ok := names[exprString(e)]
		if !ok {
			t.Errorf("%s: an uncertain verdict of %s, which this test does not know", at, exprString(e))
		}
		if code != "" {
			answered[code] = true
		}
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		files := token.NewFileSet()
		f, err := parser.ParseFile(files, entry.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CallExpr:
				fn := exprString(n.Fun)
				if fn == "uncertainOf" && len(n.Args) == 1 {
					add(files.Position(n.Pos()).String(), n.Args[0])
				}
				if (fn == "refused" || fn == "unlessTyped") && len(n.Args) > 1 && exprString(n.Args[0]) == "refusalUncertain" {
					add(files.Position(n.Pos()).String(), n.Args[1])
				}
			case *ast.CompositeLit:
				uncertain, code := false, ast.Expr(nil)
				for _, el := range n.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					switch exprString(kv.Key) {
					case "class":
						uncertain = exprString(kv.Value) == "refusalUncertain"
					case "code":
						code = kv.Value
					}
				}
				if exprString(n.Type) == "verdict" && uncertain && code != nil {
					add(files.Position(n.Pos()).String(), code)
				}
			}
			return true
		})
	}
	if len(answered) < 5 {
		t.Fatalf("only %v found", answered)
	}
	for code := range answered {
		if !uncertainCodes[code] {
			t.Errorf("%s is answered, and no Orker may ask a pause with it", code)
		}
	}
	for code := range uncertainCodes {
		if !answered[code] {
			t.Errorf("%s is never answered", code)
		}
	}
}
