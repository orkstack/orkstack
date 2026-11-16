package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// maskingFleet is one Orker for each of maskingDocument's pools: gpu-3
// (build 2026.09.01) and cpu-1 (build 2026.08.01), each answering
// secret_masking_supported as given.
func maskingFleet(gpuSupported, cpuSupported bool) *fakeFleet {
	gpu := currentOrker("gpu-3", "gpu", 0)
	gpu.BuildId = "2026.09.01"
	gpu.SecretMaskingSupported = gpuSupported
	cpu := currentOrker("cpu-1", "cpu", 0)
	cpu.BuildId = "2026.08.01"
	cpu.SecretMaskingSupported = cpuSupported
	return fleetOf(cpu, gpu)
}

func maskingResolver(fleet engine.FleetReader, logs *bytes.Buffer) *engine.PoolResolver {
	cpu, _ := task.QueueForPool("cpu", "")
	gpu, _ := task.QueueForPool("gpu", "")
	return &engine.PoolResolver{Probe: livePools{cpu: 1, gpu: 1}, Fleet: fleet,
		Logger: slog.New(slog.NewTextHandler(logs, nil)), MaskingWarned: &engine.PoolsWarned{}}
}

// maskingDocument: gpu carries a `mask: false` secret, cpu only masked ones.
const maskingDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: masking-pools
tasks:
  - id: build
    uses: shell
    pool: cpu
    with: {op: exec}
    secrets:
      - name: token
        source: env:TOKEN
  - id: debug
    uses: shell
    pool: gpu
    with: {op: exec}
    secrets:
      - name: trace-key
        source: env:TRACE_KEY
        mask: false
      - name: token
        source: env:TOKEN
`

// TestPoolResolverPlan_RefusesAnUnmaskedSecretOnAnOrkerThatCannotReadIt: an
// Orker built before secret masking refuses SecretRef.unmasked as an unknown
// field, so it would fail every attempt of a task with a `mask: false`
// secret. Submit refuses it first, naming the Orker and its build.
func TestPoolResolverPlan_RefusesAnUnmaskedSecretOnAnOrkerThatCannotReadIt(t *testing.T) {
	var logs bytes.Buffer
	_, _, err := maskingResolver(maskingFleet(false, true), &logs).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, maskingDocument), false)
	refused, ok := err.(*engine.PoolNoSecretMaskingError)
	if !ok {
		t.Fatalf("Plan = %v, want PoolNoSecretMaskingError", err)
	}
	if refused.Pool != "gpu" || fmt.Sprint(refused.Tasks) != "[debug]" {
		t.Fatalf("refusal = %+v", refused)
	}
	want := `pool "gpu" (namespace "ml", tasks: debug) is served by Orker "gpu-3" (build "2026.09.01"), ` +
		`which predates secret masking: it cannot read a secret's mask: false and would fail the task. ` +
		`Upgrade the pool's Orkers before submitting this Orkflow.`
	if err.Error() != want {
		t.Fatalf("message = %q\nwant %q", err.Error(), want)
	}
	// allow_empty_pool does not downgrade it: the Orker is present and wrong.
	if _, _, err := maskingResolver(maskingFleet(false, true), &logs).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, maskingDocument), true); err == nil {
		t.Fatal("Plan with allow_empty_pool accepted the unmasked secret")
	}
	if _, _, err := maskingResolver(maskingFleet(true, true), &logs).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, maskingDocument), false); err != nil {
		t.Fatalf("a pool whose Orker reads mask: false = %v", err)
	}
}

// TestPoolResolverPlan_AMaskedSecretOnAnOlderOrkerIsWarnedOncePerPool: an
// older Orker never masks, which is how it ran before; refusing every task
// with a secret would stop the engines-first rollout. Submit accepts it and
// warns once per pool, so the log says where output is still unmasked.
func TestPoolResolverPlan_AMaskedSecretOnAnOlderOrkerIsWarnedOncePerPool(t *testing.T) {
	var logs bytes.Buffer
	shared := maskingResolver(maskingFleet(true, false), &logs)
	for range 3 {
		// A copy per submit, as the API takes one (resolverFor): the set of
		// pools already warned is shared through it.
		resolver := &engine.PoolResolver{Probe: shared.Probe, Fleet: shared.Fleet,
			Logger: shared.Logger, MaskingWarned: shared.MaskingWarned}
		if _, _, err := resolver.Plan(context.Background(),
			&orkstackv1.Namespace{Name: "ml"}, compile(t, maskingDocument), false); err != nil {
			t.Fatalf("a masked secret on an older Orker refused the submit: %v", err)
		}
	}
	lines := strings.Count(logs.String(), "masks no secret")
	if lines != 1 || !strings.Contains(logs.String(), "pool=cpu") ||
		!strings.Contains(logs.String(), "orker=cpu-1") || !strings.Contains(logs.String(), "build=2026.08.01") {
		t.Fatalf("got %d warnings, want one naming pool cpu, its Orker and build:\n%s", lines, logs.String())
	}
}

// TestPoolResolverPlan_OnlyAnUnmaskedSecretReadsTheCensus: masked secrets add
// no inventory read to a submit that asks the pool nothing else.
func TestPoolResolverPlan_OnlyAnUnmaskedSecretReadsTheCensus(t *testing.T) {
	var logs bytes.Buffer
	masked := strings.ReplaceAll(maskingDocument, "        mask: false\n", "")
	if _, _, err := maskingResolver(refusingFleet{t}, &logs).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, masked), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
}
