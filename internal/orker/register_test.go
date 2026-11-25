package orker

import (
	"context"
	"slices"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ledgerRuntime records, as a task's container is created, what the ledger
// holds and which units the container is given.
type ledgerRuntime struct {
	cacheRuntime
	ledger  *Capacity
	created int
	held    amount
	cdi     []string
}

func (r *ledgerRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.created++
	_, r.held = r.ledger.snapshot()
	r.cdi = slices.Clone(spec.CDIDevices)
	return r.cacheRuntime.Create(ctx, spec)
}

// Through the activities as cmd/orker registers them and the interceptor it
// installs: an attempt holds exactly what its own task requests while its
// container runs, in plaintext and sealed alike, polled and pinned alike, and
// a task that asks for gpu units is given them, which its record names.
func TestRegisterTaskActivities_AnAttemptHoldsItsOwnRequest(t *testing.T) {
	const runID = "default-test-workflow-id" // the workflow the test environment schedules for
	key := taskKey(t, 5)
	scope := payloadcrypt.NewScope(key, payloadcrypt.RunContext(runID))
	for name, tc := range map[string]struct {
		sealed, pinned bool
		request        *orkstackv1.ResourceRequest
		want           amount
		cdi            []string
	}{
		"plaintext":                  {request: &orkstackv1.ResourceRequest{Cpu: "1.5", Memory: "512Mi"}, want: amount{milli: 1500, bytes: 512 << 20}},
		"plaintext, cpu alone":       {request: &orkstackv1.ResourceRequest{Cpu: "500m"}, want: amount{milli: 500, bytes: 1 << 30}},
		"plaintext, no request":      {want: amount{milli: 1000, bytes: 1 << 30}},
		"sealed":                     {sealed: true, request: &orkstackv1.ResourceRequest{Cpu: "1.5", Memory: "512Mi"}, want: amount{milli: 1500, bytes: 512 << 20}},
		"sealed, no request":         {sealed: true, want: amount{milli: 1000, bytes: 1 << 30}},
		"plaintext, pinned":          {pinned: true, request: &orkstackv1.ResourceRequest{Cpu: "1.5", Memory: "512Mi"}, want: amount{milli: 1500, bytes: 512 << 20}},
		"sealed, pinned":             {sealed: true, pinned: true, request: &orkstackv1.ResourceRequest{Cpu: "1.5", Memory: "512Mi"}, want: amount{milli: 1500, bytes: 512 << 20}},
		"plaintext, gpu alone":       {request: &orkstackv1.ResourceRequest{Gpu: 1}, want: amount{milli: 1000, bytes: 1 << 30, gpu: 1}, cdi: []string{"amd.com/gpu=u0"}},
		"plaintext, one gpu of two":  {request: &orkstackv1.ResourceRequest{Cpu: "1", Memory: "1Gi", Gpu: 1}, want: amount{milli: 1000, bytes: 1 << 30, gpu: 1}, cdi: []string{"amd.com/gpu=u0"}},
		"sealed, one gpu of two":     {sealed: true, request: &orkstackv1.ResourceRequest{Cpu: "1", Memory: "1Gi", Gpu: 1}, want: amount{milli: 1000, bytes: 1 << 30, gpu: 1}, cdi: []string{"amd.com/gpu=u0"}},
		"sealed, both gpus":          {sealed: true, request: &orkstackv1.ResourceRequest{Cpu: "1", Memory: "1Gi", Gpu: 2}, want: amount{milli: 1000, bytes: 1 << 30, gpu: 2}, cdi: []string{"amd.com/gpu=u0", "amd.com/gpu=u1"}},
		"sealed, refused, above cap": {sealed: true, request: &orkstackv1.ResourceRequest{Cpu: "64"}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCacheHarness(t)
			ceiling, err := h.dispatcher.Policy.ceiling()
			must.OK(t, err)
			h.dispatcher.Policy.MaxGPU = 2
			c := newCapacity(ceiling, ceiling, 16)
			c.useUnits("amd", 2, 2, nil)
			runtime := &ledgerRuntime{ledger: c}
			h.dispatcher.Runtime = runtime
			amd := Profile{Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
				GPU: &GPUProfile{CDIKind: "amd.com/gpu", Units: []string{"u0", "u1"}}}
			h.dispatcher.profiles = newProfileState(Profiles{List: []Profile{amd}}, "")
			h.dispatcher.profiles.attach(NewCordon())
			h.dispatcher.profiles.setAudited(map[string]profileAudit{"amd": {
				identity: "d1", def: "1", contracts: map[string]contractAudit{"1": {identity: "d1"}}}})
			h.dispatcher.gpu.Store(&gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: []string{"u0", "u1"}, lock: fixedLock{id: "L"}})

			records := artifacts.NewMemoryAttemptStore()
			h.dispatcher.Attempts = records

			in := baseExportInput("work")
			in.RunId, in.IdempotencyKey, in.Resources = runID, "default/"+runID+"/work", tc.request
			commit, err := artifacts.CommitKey("default", runID, "work")
			must.OK(t, err)
			in.CommitUri = cacheRoot + "/" + commit
			if tc.request.GetGpu() > 0 {
				in.Devices = []*orkstackv1.ProfileRef{{Name: "amd", IdentityDigest: "d1"}}
			}

			a := &Activities{dispatcher: h.dispatcher, cordon: NewCordon(), pins: newOpenPins(), capacity: c,
				taskQueue: testActivityQueue, taskKeys: staticTaskKeys{payloadcrypt.RunContext(runID): key}}
			a.dispatcher.heartbeaters = sdkRouteHeartbeater
			if tc.pinned {
				a.taskQueue = "another channel: this one is a pin's"
			}
			env := activityEnv(a)
			env.SetDataConverter(payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil))
			a.RegisterTaskActivities(env)

			var arg any = in
			if tc.sealed {
				arg = sealedInput(t, scope, in)
			}
			permit := mustReserve(t, c)
			if !tc.pinned {
				c.markUsed(permit)
			}
			polled := c.reservedNow()
			_, err = env.ExecuteActivity(task.ActivityExecuteTask, arg)
			if tc.want == (amount{}) {
				if err == nil || !strings.Contains(err.Error(), ReasonResourceRefused) && !tc.sealed || runtime.created != 0 {
					t.Fatalf("a request above the cap ended %v after %d containers", err, runtime.created)
				}
			} else {
				want := tc.want
				if tc.pinned {
					want = want.add(polled) // beside the idle poll's own ceiling
				}
				if runtime.created != 1 || runtime.held != want || !slices.Equal(runtime.cdi, tc.cdi) {
					t.Fatalf("while its container ran (%d created, %v) the ledger held %s and it was given %v; want %s and %v",
						runtime.created, err, runtime.held, runtime.cdi, want, tc.cdi)
				}
				if len(tc.cdi) > 0 {
					recorded, err := records.ListAttempts(context.Background(), runID, "work", artifacts.NoItem)
					if err != nil || len(recorded) != 1 || len(recorded[0].GetProfiles()) != 1 ||
						!slices.Equal(recorded[0].GetProfiles()[0].GetUnitIndices(), []int32{0, 1}[:len(tc.cdi)]) {
						t.Fatalf("the attempt's record = %v, %v; want the %d units it held", recorded, err, len(tc.cdi))
					}
				}
			}
			c.release(permit)
			if rest := c.reservedNow(); rest != (amount{}) {
				t.Fatalf("after the attempt the ledger holds %s", rest)
			}
		})
	}
}

// An input nobody has read holds the whole poll, gpu included, until it is
// resolved: once, to the task's own request, and never past what is free.
func TestAdmission_AnInputNotReadIsUnknownUntilResolved(t *testing.T) {
	ceiling := amount{milli: 8000, bytes: 16 << 30, gpu: 2}
	admitted := func(t *testing.T, arg any, queue string) (*Capacity, *Admission, func()) {
		t.Helper()
		c := newCapacity(ceiling.cpuMemory(), ceiling.cpuMemory(), 4)
		c.useUnits("amd", 2, 2, nil)
		permit := mustReserve(t, c)
		if queue == "pool" {
			c.markUsed(permit)
		}
		a := &Activities{dispatcher: &OCIDispatcher{Policy: DefaultSecurityPolicy()}, capacity: c, taskQueue: "pool"}
		admission, err := a.admitTask(queue, &interceptor.ExecuteActivityInput{Args: []any{arg}})
		must.OK(t, err)
		return c, admission, func() {
			admission.Done()
			c.release(permit)
			if rest := c.reservedNow(); rest != (amount{}) {
				t.Errorf("at rest the ledger holds %s", rest)
			}
		}
	}
	sealed := sealedInput(t, payloadcrypt.NewScope(taskKey(t, 5), payloadcrypt.RunContext("ork-1")), &orkstackv1.ExecuteTaskInput{})
	for name, arg := range map[string]any{
		"sealed": sealed, "not decoding": converter.NewRawValue(&commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("[")}),
		"no payload": converter.NewRawValue(nil), "another type": "not an input",
	} {
		t.Run(name, func(t *testing.T) {
			_, admission, end := admitted(t, arg, "pool")
			defer end()
			if admission.held != ceiling || !admission.unknown {
				t.Fatalf("admitted as %s, unknown %v; want the whole poll, %s", admission.held, admission.unknown, ceiling)
			}
		})
	}
	t.Run("plaintext is known at once", func(t *testing.T) {
		_, admission, end := admitted(t, sealedInput(t, payloadcrypt.Unsealed(), &orkstackv1.ExecuteTaskInput{
			Resources: &orkstackv1.ResourceRequest{Cpu: "500m", Memory: "1Mi"}}), "a pin's")
		defer end()
		if want := (amount{milli: 500, bytes: 1 << 20}); admission.held != want || admission.unknown {
			t.Fatalf("admitted as %s, unknown %v; want %s", admission.held, admission.unknown, want)
		}
	})
	t.Run("resolved once", func(t *testing.T) {
		c, admission, end := admitted(t, sealed, "pool")
		defer end()
		must.OK(t, admission.Resolve(gpuReq(1)))
		if admission.held != gpuReq(1) || c.reservedNow() != gpuReq(1) {
			t.Fatalf("resolved to %s with %s reserved, want %s", admission.held, c.reservedNow(), gpuReq(1))
		}
		must.OK(t, admission.Resolve(gpuReq(2)))
		if admission.held != gpuReq(1) {
			t.Fatalf("resolved twice, to %s", admission.held)
		}
	})
	t.Run("larger than is free", func(t *testing.T) {
		c, admission, end := admitted(t, sealed, "pool")
		defer end()
		if err := admission.Resolve(amount{milli: 9000}); !isReason(err, ReasonResourceAdmissionMismatch) {
			t.Fatalf("= %v, want %s", err, ReasonResourceAdmissionMismatch)
		}
		if c.reservedNow() != ceiling {
			t.Fatalf("the refused attempt holds %s, want its poll until it is done", c.reservedNow())
		}
	})
	t.Run("pinned", func(t *testing.T) {
		c, admission, end := admitted(t, sealed, "a pin's")
		defer end()
		if admission.held != ceiling.cpuMemory() {
			t.Fatalf("a pinned member is admitted as %s, want no gpu", admission.held)
		}
		must.OK(t, admission.Resolve(amount{milli: 100}))
		if want := ceiling.add(amount{milli: 100}); c.reservedNow() != want {
			t.Fatalf("%s reserved, want %s", c.reservedNow(), want)
		}
	})
	t.Run("no ledger", func(t *testing.T) {
		must.OK(t, (*Admission)(nil).Resolve(gpuReq(1)))
	})
}

// A refused admission's reason reaches a run that is not sealed from an Orker
// that seals too: it holds nothing of the run, and is written as it is.
func TestAdmission_ARefusalIsWrittenAsItIs(t *testing.T) {
	for name, keys := range map[string]TaskKeys{"an Orker with no keys": nil, "an Orker that seals": staticTaskKeys{}} {
		t.Run(name, func(t *testing.T) {
			c := newCapacity(amount{milli: 100}, amount{milli: 100}, 4)
			c.markUsed(mustReserve(t, c))
			a := &Activities{dispatcher: &OCIDispatcher{Policy: DefaultSecurityPolicy()}, capacity: c, taskQueue: "pool", taskKeys: keys}
			_, err := a.admitTask("pool", &interceptor.ExecuteActivityInput{Args: []any{
				sealedInput(t, payloadcrypt.Unsealed(), &orkstackv1.ExecuteTaskInput{})}})
			dc := payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil)
			f := payloadcrypt.NewFailureConverter(dc, keys != nil).ErrorToFailure(err)
			if !strings.Contains(f.GetMessage(), "larger than the") || !strings.Contains(f.String(), ReasonResourceAdmissionMismatch) {
				t.Fatalf("written as %v, want its own words and %s", f, ReasonResourceAdmissionMismatch)
			}
		})
	}
}
