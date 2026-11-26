package orker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// activityRig is a host rig whose activity runs as its worker runs it. Its cluster
// answers every heartbeat.
func activityRig(t *testing.T) *hostRig {
	t.Helper()
	r := newHostRig(t)
	r.a.dispatcher.heartbeaters = func(context.Context) *Heartbeater {
		return &Heartbeater{send: func(context.Context, HeartbeatProgress) error { return nil }}
	}
	t.Cleanup(func() { r.rt.once.Do(func() { close(r.rt.done) }) })
	return r
}

func stopped(int) (*orkstackv1.DoorAnswer, error) {
	return door(orkstackv1.Disposition_DISPOSITION_STOP, "EVENT_SOURCE_PAUSED"), nil
}

// TestHostEventSourceActivity_WhatItReturns: the engine's word completes it, a
// host's refusal is an error the cluster retries on another after a second, and
// neither its history nor its error holds a sentence of a host's or a tenant's.
func TestHostEventSourceActivity_WhatItReturns(t *testing.T) {
	r := activityRig(t)
	r.engine.confirm = stopped
	env := activityEnv(r.a)
	r.a.RegisterSourceActivities(env)
	got, err := env.ExecuteActivity(task.ActivityHostEventSource, r.in)
	must.OK(t, err)
	var res orkstackv1.HostEventSourceResult
	must.OK(t, got.Get(&res))
	if res.GetEnd() != orkstackv1.InstanceEnd_INSTANCE_END_STOPPED || res.GetReason() != "EVENT_SOURCE_PAUSED" || res.GetAttempt() != 1 {
		t.Errorf("%v", &res)
	}
	r.a.cordon.Set("the rack at 10.0.0.7", time.Now())
	_, err = env.ExecuteActivity(task.ActivityHostEventSource, r.in)
	var failed *temporal.ApplicationError
	if !errors.As(err, &failed) || failed.Type() != "ORKER_CORDONED" || failed.NonRetryable() || failed.NextRetryDelay() != time.Second || failed.Message() != endingSentence {
		t.Errorf("%v", err)
	}
	// Retried with the back-off, never at once and never finally: another source
	// host of the pool may serve it, and no setting of an Orker's is the source's.
	r.serves("billing")
	_, err = env.ExecuteActivity(task.ActivityHostEventSource, r.in)
	if !errors.As(err, &failed) || failed.Type() != "NAMESPACE_NOT_SERVED" || failed.NonRetryable() || failed.NextRetryDelay() != 0 {
		t.Errorf("another namespace: %v", err)
	}
	r.a.hosting = nil
	_, err = env.ExecuteActivity(task.ActivityHostEventSource, r.in)
	if !errors.As(err, &failed) || failed.Type() != "SERVICE_HOST_UNFIT" {
		t.Errorf("one that hosts none: %v", err)
	}
}

// TestHostEventSourceActivity_IsCancelled: the cluster's cancel is answered as one,
// once its container is gone.
func TestHostEventSourceActivity_IsCancelled(t *testing.T) {
	r := activityRig(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r.engine.report = func(int) (*orkstackv1.DoorAnswer, error) {
		cancel()
		return ok, nil
	}
	env := activityEnv(r.a)
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx, Interceptors: []interceptor.WorkerInterceptor{r.a.WorkerInterceptor()}})
	r.a.RegisterSourceActivities(env)
	_, err := env.ExecuteActivity(task.ActivityHostEventSource, r.in)
	if !temporal.IsCanceledError(err) {
		t.Errorf("%v", err)
	}
	leftNothing(t, r.rt.faultServiceRuntime, r.own)
}

// TestHostEventSourceActivity_TakesOverWhatAnEarlierAttemptSaid: an activity's
// attempt reads the last heartbeat of the one before it; what it cannot read is
// nothing.
func TestHostEventSourceActivity_TakesOverWhatAnEarlierAttemptSaid(t *testing.T) {
	since := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	for name, tc := range map[string]struct {
		details any
		want    time.Time
	}{
		"its own":         {eventsources.HeartbeatDetails{Epoch: 12, HeldSince: since}.Encode(), since},
		"another shape's": {"an earlier release's", time.Time{}},
	} {
		r := activityRig(t)
		var got time.Time
		r.a.dispatcher.heartbeaters = func(ctx context.Context) *Heartbeater {
			got = earlierDetails(ctx).HeldSince
			return &Heartbeater{send: func(context.Context, HeartbeatProgress) error { return nil }}
		}
		r.engine.confirm = stopped
		env := activityEnv(r.a)
		env.SetHeartbeatDetails(tc.details)
		r.a.RegisterSourceActivities(env)
		_, err := env.ExecuteActivity(task.ActivityHostEventSource, r.in)
		must.OK(t, err)
		if !got.Equal(tc.want) {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
}

// TestHostEventSourceActivity_IsAdmitted: an attempt holds what its activity asks
// for of the Orker's capacity, or its whole poll where that does not read or is
// more, and nothing once it ended; until then it is an instance its Orker hosts.
func TestHostEventSourceActivity_IsAdmitted(t *testing.T) {
	for name, tc := range map[string]struct {
		resources *orkstackv1.ResourceRequest
		want      amount
	}{
		"its request":        {&orkstackv1.ResourceRequest{Cpu: "100m", Memory: "64Mi"}, amount{milli: 100, bytes: 64 << 20}},
		"none":               {nil, sourceCeiling},
		"one nobody reads":   {&orkstackv1.ResourceRequest{Cpu: "lots", Memory: "64Mi"}, sourceCeiling},
		"more than its poll": {&orkstackv1.ResourceRequest{Cpu: "4", Memory: "64Mi"}, sourceCeiling},
	} {
		r := activityRig(t)
		r.in.Resources = tc.resources
		r.a.capacity = newSourceCapacity(1)
		// The test environment's queue.
		r.a.sourceQueue, r.a.taskQueue = "default-test-taskqueue", "q"
		r.a.capacity.markUsed(mustPoll(t, r.a.capacity, r.a.capacity.source))
		var held amount
		var hosted int32
		r.engine.confirm = func(n int) (*orkstackv1.DoorAnswer, error) {
			held, hosted = r.a.capacity.reservedNow(), r.a.instances.load()
			return stopped(n)
		}
		env := activityEnv(r.a)
		r.a.RegisterSourceActivities(env)
		_, err := env.ExecuteActivity(task.ActivityHostEventSource, r.in)
		must.OK(t, err)
		if held != tc.want {
			t.Errorf("%s: held %s, want %s", name, held, tc.want)
		}
		wantReserved(t, r.a.capacity, name, amount{})
		// A stop waits for what is counted.
		if left := r.a.instances.load(); hosted != 1 || left != 0 {
			t.Errorf("%s: %d instances counted while it ran, %d after", name, hosted, left)
		}
	}
}
