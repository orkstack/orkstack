package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// fakeCapabilities answers the fleet check: lacking is every identity it
// reports, err a failed describe; calls counts the questions.
type fakeCapabilities struct {
	mu      sync.Mutex
	lacking []string
	err     error
	calls   int
	asked   []string
}

func (f *fakeCapabilities) MissingCapability(_ context.Context, capability string, scope engine.CapabilityScope) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.asked = append(f.asked, fmt.Sprintf("%s/%d", capability, scope))
	return f.lacking, f.err
}

func (f *fakeCapabilities) questions() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fanOutDoc is an Orkflow whose one fan-out declares items, each item's own
// `with:` padded to pad bytes.
func fanOutDoc(name string, items, pad int) string {
	return fmt.Sprintf(`name: %s
inputs:
  parts: {type: array}
tasks:
  - id: convert
    uses: busybox:latest
    each: {over: "{{ .inputs.parts }}", max_items: %d}
    with: {pad: "%s"}
`, name, items, strings.Repeat("p", pad))
}

// itemsService is a deployment allowing items fan-out items, with
// continuation on and the fleet check answered by caps.
func itemsService(t *testing.T, orkflows *engine.MemoryOrkflowStore, starter *fakeStarter, items int32,
	caps engine.CapabilityChecker, logs *bytes.Buffer,
) *api.Service {
	t.Helper()
	cfg := api.Config{Orkflows: orkflows, Starter: starter, EachMaxItems: items, ContinuationEnabled: true,
		Capabilities: caps}
	if logs != nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	return api.NewService(cfg)
}

func applyDoc(t *testing.T, svc *api.Service, yaml string) error {
	t.Helper()
	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml}))
	return err
}

func submitName(svc *api.Service, name string) error {
	_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name}))
	return err
}

func TestEachMaxItemsOf_AcceptsUpToTheBuiltInMaximum(t *testing.T) {
	for _, tc := range []struct{ configured, want int32 }{
		{0, 1000}, {1000, 1000}, {5000, 5000}, {10000, 10000}, {20000, 10000},
	} {
		if got := api.EachMaxItemsOfForTest(tc.configured); got != tc.want {
			t.Errorf("eachMaxItemsOf(%d) = %d, want %d", tc.configured, got, tc.want)
		}
	}
}

func TestApplyOrkflow_AcceptsFiveThousandItemsWhenTheDeploymentAllowsThem(t *testing.T) {
	doc := fanOutDoc("wide", 5000, 1)
	raised := itemsService(t, engine.NewMemoryOrkflowStore(), &fakeStarter{}, 5000, &fakeCapabilities{}, nil)
	resp, err := raised.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
	if err != nil || len(resp.Msg.GetIssues()) != 0 {
		t.Fatalf("validate at 5,000 = %v, %v; want accepted", resp.Msg.GetIssues(), err)
	}
	if err := applyDoc(t, raised, doc); err != nil {
		t.Fatalf("apply at 5,000: %v", err)
	}

	store := engine.NewMemoryOrkflowStore()
	defaulted := itemsService(t, store, &fakeStarter{}, 1000, &fakeCapabilities{}, nil)
	err = applyDoc(t, defaulted, doc)
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "ORK_EACH_MAX_ITEMS") ||
		!strings.Contains(err.Error(), "this deployment allows 1000") {
		t.Fatalf("apply at 1,000 = %v; want invalid_argument naming ORK_EACH_MAX_ITEMS", err)
	}
	if _, getErr := store.Get(context.Background(), "default", "wide", ""); getErr == nil {
		t.Fatal("a refused apply stored a definition")
	}
}

func TestSubmitRun_AcceptsFiveThousandItemsWhenTheDeploymentAllowsThem(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	starter := &fakeStarter{}
	caps := &fakeCapabilities{}
	svc := itemsService(t, orkflows, starter, 5000, caps, nil)
	if err := applyDoc(t, svc, fanOutDoc("wide", 5000, 1)); err != nil {
		t.Fatal(err)
	}
	if err := submitName(svc, "wide"); err != nil {
		t.Fatalf("submit at 5,000: %v", err)
	}
	if len(starter.startedRuns()) != 1 || caps.questions() != 1 {
		t.Fatalf("started %v after %d fleet questions; want one run after one", starter.startedRuns(), caps.questions())
	}
	if floor := starter.plans[0].Continuation.ItemInputFloorBytes; floor != engine.DefaultItemInputFloorBytes {
		t.Fatalf("the run pinned an item input floor of %d, want %d", floor, engine.DefaultItemInputFloorBytes)
	}

	// checkEachCapAtSubmit reads the same setting: a deployment that lowered
	// it refuses the version applied above it.
	lowered := itemsService(t, orkflows, starter, 4000, caps, nil)
	err := submitName(lowered, "wide")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "now allows 4000") {
		t.Fatalf("submit after lowering to 4,000 = %v; want failed_precondition", err)
	}
}

func TestEachItemBounds_ApplyUsesTheConfiguredFloor(t *testing.T) {
	// 8 KiB of every item's own input at 5,000 items: over 16 MiB / 5,000
	// (about 3.3 KiB), under the 16 KiB floor a run with continuation pins.
	doc := fanOutDoc("padded", 5000, 8<<10)
	on := itemsService(t, engine.NewMemoryOrkflowStore(), &fakeStarter{}, 5000, &fakeCapabilities{}, nil)
	if err := applyDoc(t, on, doc); err != nil {
		t.Fatalf("continuation on: %v; want the floor to admit an 8 KiB item", err)
	}
	off := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), EachMaxItems: 5000})
	err := applyDoc(t, off, doc)
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "which allows 3355 bytes per item") {
		t.Fatalf("continuation off: %v; want the 3,355-byte share without a floor", err)
	}
}

func TestSubmitRun_RefusesManyItemsWhileAnOlderEngineProcessPolls(t *testing.T) {
	starter := &fakeStarter{}
	caps := &fakeCapabilities{lacking: []string{"77@old@"}}
	svc := itemsService(t, engine.NewMemoryOrkflowStore(), starter, 5000, caps, nil)
	if err := applyDoc(t, svc, fanOutDoc("wide", 5000, 1)); err != nil {
		t.Fatal(err)
	}
	err := submitName(svc, "wide")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("submit = %v; want failed_precondition", err)
	}
	for _, want := range []string{"declares 5000 fan-out items", "1 engine process runs a release that cannot run more than 1000",
		"finish the rollout first"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q lacks %q", err.Error(), want)
		}
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatalf("a refused submission started %v", starter.startedRuns())
	}
	if want := fmt.Sprintf("%s/%d", engine.CapabilityEachItems10k, engine.ScopeWorkflow); len(caps.asked) != 1 || caps.asked[0] != want {
		t.Fatalf("asked %v, want [%s]", caps.asked, want)
	}
}

func TestSubmitRun_RefusalNamesACountNotAnIdentity(t *testing.T) {
	var logs bytes.Buffer
	caps := &fakeCapabilities{lacking: []string{"123@host-a@ ork-caps=", "456@host-b@"}}
	svc := itemsService(t, engine.NewMemoryOrkflowStore(), &fakeStarter{}, 5000, caps, &logs)
	if err := applyDoc(t, svc, fanOutDoc("wide", 5000, 1)); err != nil {
		t.Fatal(err)
	}
	err := submitName(svc, "wide")
	if err == nil || !strings.Contains(err.Error(), "2 engine processes") {
		t.Fatalf("refusal %v; want it to count 2 engine processes", err)
	}
	for _, leaked := range []string{"123@", "456@", "host-a", "host-b", "ork-caps"} {
		if strings.Contains(err.Error(), leaked) {
			t.Fatalf("refusal %q names %q: identities carry host names, an operator surface only", err.Error(), leaked)
		}
	}
	line := ""
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, `"missing_capability"`) && strings.Contains(l, `"level":"WARN"`) {
			line = l
		}
	}
	if !strings.Contains(line, `123@host-a@ ork-caps=`) || !strings.Contains(line, `456@host-b@`) ||
		!strings.Contains(line, engine.CapabilityEachItems10k) {
		t.Fatalf("the engine's Warn line %q does not name both identities and the capability", line)
	}
}

func TestSubmitRun_ManyItemsCheckFailsClosed(t *testing.T) {
	starter := &fakeStarter{}
	svc := itemsService(t, engine.NewMemoryOrkflowStore(), starter, 5000,
		&fakeCapabilities{err: errors.New("describe: unavailable")}, nil)
	if err := applyDoc(t, svc, fanOutDoc("wide", 5000, 1)); err != nil {
		t.Fatal(err)
	}
	err := submitName(svc, "wide")
	if connect.CodeOf(err) != connect.CodeUnavailable || strings.Contains(err.Error(), "describe: unavailable") {
		t.Fatalf("submit with an unreadable fleet = %v; want unavailable, without the cluster's own text", err)
	}
	// No checker wired at all is the same unanswered question.
	unwired := itemsService(t, engine.NewMemoryOrkflowStore(), starter, 5000, nil, nil)
	if err := applyDoc(t, unwired, fanOutDoc("wide", 5000, 1)); err != nil {
		t.Fatal(err)
	}
	if err := submitName(unwired, "wide"); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("submit with no checker = %v; want unavailable", err)
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatalf("an unchecked submission started %v", starter.startedRuns())
	}
}

func TestSubmitRun_FewItemsSkipTheFleetCheck(t *testing.T) {
	starter := &fakeStarter{}
	caps := &fakeCapabilities{lacking: []string{"77@old@"}, err: errors.New("never asked")}
	svc := itemsService(t, engine.NewMemoryOrkflowStore(), starter, 5000, caps, nil)
	if err := applyDoc(t, svc, fanOutDoc("small", 1000, 1)); err != nil {
		t.Fatal(err)
	}
	if err := submitName(svc, "small"); err != nil {
		t.Fatalf("a 1,000-item submit = %v; want it to start whatever the fleet runs", err)
	}
	if caps.questions() != 0 {
		t.Fatalf("a 1,000-item submit asked the fleet %d times; want none", caps.questions())
	}
}

// A composed child is a run of its own: a parent with no fan-out that
// composes a 5,000-item child is checked as the child is.
func TestSubmitRun_FleetCheckCoversComposedChildren(t *testing.T) {
	caps := &fakeCapabilities{lacking: []string{"77@old@"}}
	f := newComposeFixture(t, func(cfg *api.Config) {
		cfg.EachMaxItems, cfg.ContinuationEnabled, cfg.Capabilities = 5000, true, caps
	})
	f.mustApply(t, strings.Replace(fanOutDoc("release-block", 5000, 1), "name: release-block\n",
		"name: release-block\nversion: \"2.1.0\"\n", 1))
	f.mustApply(t, composeParentYAMLWithout(t))
	_, err := f.submit("deploy")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "orkflow release-block@2.1.0 declares 5000 fan-out items") {
		t.Fatalf("submit = %v; want the child's items refused", err)
	}
}
