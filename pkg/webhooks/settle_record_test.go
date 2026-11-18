package webhooks_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/webhooktest"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// TestSettleRecord_SaysWhetherThisCallWroteTheRecord: a record the recorder
// says is already there is a retried settle's success, reported as not
// written now; any other recorder error is returned, and observers are
// handed only what was recorded.
func TestSettleRecord_SaysWhetherThisCallWroteTheRecord(t *testing.T) {
	for _, tc := range []struct {
		name        string
		recordErr   error
		wantCreated bool
		wantErr     bool
		wantSeen    int
	}{
		{name: "written now", wantCreated: true, wantSeen: 1},
		{name: "already there", recordErr: fmt.Errorf("put: %w", webhooks.ErrRecordExists), wantSeen: 1},
		{name: "the store is down", recordErr: errors.New("store down"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := webhooks.NewRegistry(webhooks.WithDestinationPolicy(webhooktest.LoopbackPolicy()))
			notifier := webhooks.NewNotifier(registry, nil, webhooks.NotifierOptions{
				Record: func(context.Context, webhooks.Result) error { return tc.recordErr },
			})
			seen := 0
			notifier.Observe(func(webhooks.Result) { seen++ })
			created, err := notifier.SettleRecord(context.Background(), webhooks.Result{DeliveryID: "d"})
			if (err != nil) != tc.wantErr || created != tc.wantCreated || seen != tc.wantSeen {
				t.Fatalf("created=%v err=%v seen=%d; want created=%v error=%v seen=%d",
					created, err, seen, tc.wantCreated, tc.wantErr, tc.wantSeen)
			}
			if err := notifier.Settle(context.Background(), webhooks.Result{DeliveryID: "d"}); (err != nil) != tc.wantErr {
				t.Fatalf("Settle err = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}

func TestNotQueuedReason(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&webhooks.NotQueuedError{Reason: "r", Code: webhooks.NotQueuedNotStarted, Err: errors.New("x")}, webhooks.NotQueuedNotStarted},
		{fmt.Errorf("wrapped: %w", &webhooks.NotQueuedError{Code: webhooks.NotQueuedBodyUnstored, Err: errors.New("x")}), webhooks.NotQueuedBodyUnstored},
		{&webhooks.NotQueuedError{Reason: "r", Code: "made-up", Err: errors.New("x")}, webhooks.NotQueuedUnknown},
		{errors.New("cluster unreachable"), webhooks.NotQueuedUnknown},
	} {
		if got := webhooks.NotQueuedReason(tc.err); got != tc.want {
			t.Errorf("NotQueuedReason(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// TestNotifier_TellsOnNotQueuedEachDeliveryItCouldNotQueue: the hook hears
// the delivery's namespace and its closed reason.
func TestNotifier_TellsOnNotQueuedEachDeliveryItCouldNotQueue(t *testing.T) {
	registry := webhooks.NewRegistry(webhooks.WithDestinationPolicy(webhooktest.LoopbackPolicy()))
	mustRegister(t, registry, webhooks.Registration{URL: "https://example.test/hook"})
	var mu sync.Mutex
	var told []string
	notifier := webhooks.NewNotifier(registry, nil, webhooks.NotifierOptions{
		Launcher: &fakeLauncher{refuse: &webhooks.NotQueuedError{Reason: "the engine could not start it",
			Code: webhooks.NotQueuedNotStarted, Err: errors.New("cluster unreachable")}},
		OnNotQueued: func(_ context.Context, namespace, reason string) {
			mu.Lock()
			defer mu.Unlock()
			told = append(told, namespace+"/"+reason)
		},
	})
	delivery := sampleDelivery()
	notifier.Notify(context.Background(), delivery)
	drain(t, notifier)
	mu.Lock()
	defer mu.Unlock()
	if len(told) != 1 || told[0] != delivery.Namespace+"/"+webhooks.NotQueuedNotStarted {
		t.Fatalf("told %v, want one %s/%s", told, delivery.Namespace, webhooks.NotQueuedNotStarted)
	}
}
