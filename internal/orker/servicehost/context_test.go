package servicehost

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

func testSpec() eventsources.ServiceSpec {
	return eventsources.ServiceSpec{
		Format: 1, Source: eventsources.SpecSource{ID: "evs-4a1c9e0b7d2f6a35", Namespace: "payments", Name: "order-created"},
		Contracts: []int32{1}, Profiles: []int32{1}, Concurrency: 4, Delivery: "at-least-once",
		Limits: eventsources.SpecLimits{StatusSeconds: 30, StartupSeconds: 60, StopSeconds: 20, AliveSeconds: 10, OrphanSeconds: 60,
			MaxBodyBytes: 1 << 20, MaxAttributesBytes: 64 << 10, PollWaitSeconds: 20, AnswerSeconds: 30, SettleSeconds: 30, HoldSeconds: 960},
	}
}

// TestContext_TheSpecsExample: the context is the document 06 §3 shows.
func TestContext_TheSpecsExample(t *testing.T) {
	ctx, err := BuildContext(testSpec(), "prod-eu", 0, 2, "12-1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := stablejson.Marshal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schemaVersion":1,"contract":1,"profile":{"name":"event-source","version":1},"deployment":"prod-eu","namespace":"payments",` +
		`"instance":{"index":0,"of":2,"key":"prod-eu/payments/order-created/0","run":"12-1"},"lanes":4,` +
		`"eventSource":{"id":"evs-4a1c9e0b7d2f6a35","name":"order-created","key":"prod-eu/payments/order-created","delivery":"at-least-once"},` +
		`"limits":{"statusSeconds":30,"startupSeconds":60,"stopSeconds":20,"aliveSeconds":10,"orphanSeconds":60,"maxBodyBytes":1048576,` +
		`"maxAttributesBytes":65536,"pollWaitSeconds":20,"answerSeconds":30,"settleSeconds":30,"holdSeconds":960},` +
		`"paths":{"config":"/orkstack/input/config.json","secrets":"/orkstack/input/secrets","ask":"/orkstack/service/ask","answer":"/orkstack/service/answer"}}`
	if string(got) != want {
		t.Fatalf("service.json\n%s\nwant\n%s", got, want)
	}
}

func TestContext_HighestCommonVersionsAndRefusals(t *testing.T) {
	if v, ok := Highest([]int32{1, 2, 3}, []int32{1, 2}); !ok || v != 2 {
		t.Fatalf("Highest = %d, %v", v, ok)
	}
	if _, ok := Highest([]int32{3}, []int32{1}); ok {
		t.Fatal("no common version found one")
	}
	spec := testSpec()
	spec.Contracts = []int32{9}
	if _, err := BuildContext(spec, "d", 0, 1, "r"); err == nil || !strings.Contains(err.Error(), "SERVICE_CONTRACT_UNSUPPORTED") {
		t.Fatalf("contract: %v", err)
	}
	spec = testSpec()
	spec.Profiles = []int32{9}
	if _, err := BuildContext(spec, "d", 0, 1, "r"); err == nil || !strings.Contains(err.Error(), "SERVICE_PROFILE_UNSUPPORTED") {
		t.Fatalf("profile: %v", err)
	}
	for name, call := range map[string]func() error{
		"no deployment": func() error { _, err := BuildContext(testSpec(), "", 0, 1, "r"); return err },
		"index past of": func() error { _, err := BuildContext(testSpec(), "d", 2, 2, "r"); return err },
		"no run":        func() error { _, err := BuildContext(testSpec(), "d", 0, 1, ""); return err },
	} {
		if call() == nil {
			t.Errorf("%s: built", name)
		}
	}
}
