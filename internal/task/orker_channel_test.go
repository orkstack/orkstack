package task_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/task"
)

// TestQueueForOrker_UsesTheOrkerPrefix pins the control channel's spelling and
// the id rule every Orker must meet to have one (docs/DEPLOY.md "Removing an Orker safely: cordon and drain").
func TestQueueForOrker_UsesTheOrkerPrefix(t *testing.T) {
	queue, err := task.QueueForOrker("orker-7f3a")
	if err != nil || queue != "ork-orker-orker-7f3a" {
		t.Fatalf("QueueForOrker(orker-7f3a) = %q, %v; want ork-orker-orker-7f3a", queue, err)
	}
	// The id becomes part of a task queue name, which travels as a protobuf
	// string: text that is not UTF-8 would be refused there, at the first poll.
	for _, bad := range []string{"", strings.Repeat("a", 201), "a b", "a\tb", "a\x7fb", "a\nb", "a\xffb", "\xed\xa0\x80"} {
		if err := task.ValidOrkerID(bad); err == nil {
			t.Errorf("ValidOrkerID(%q) accepted an id with no addressable control channel", bad)
		}
		if _, err := task.QueueForOrker(bad); err == nil {
			t.Errorf("QueueForOrker(%q) built a channel from an invalid id", bad)
		}
	}
	for _, good := range []string{"host-1a2b3c4d", strings.Repeat("a", 200), "orker.eu-west@rack/7"} {
		if err := task.ValidOrkerID(good); err != nil {
			t.Errorf("ValidOrkerID(%q) = %v; want accepted", good, err)
		}
	}
}

// TestQueueForPool_RefusesTheOrkerChannels: a deployment cannot bind its
// default pool to the presence channel or to any Orker's control channel,
// while a legal pool NAMED orkers still routes to its own, different queue.
func TestQueueForPool_RefusesTheOrkerChannels(t *testing.T) {
	for _, bound := range []string{task.OrkersQueue, "ork-orker-x", task.EngineEventQueue} {
		if queue, err := task.QueueForPool("", bound); !errors.Is(err, task.ErrReservedQueue) {
			t.Errorf("QueueForPool(\"\", %q) = %q, %v; want ErrReservedQueue", bound, queue, err)
		}
	}
	queue, err := task.QueueForPool("orkers", "")
	if err != nil || queue != "ork-pool-orkers" {
		t.Fatalf("QueueForPool(orkers) = %q, %v; want ork-pool-orkers", queue, err)
	}
}

// TestResolvePoolBinding_RefusesTheOrkerChannels: ORK_ORKER_QUEUE naming an
// Orker channel refuses boot with a message naming the variable.
func TestResolvePoolBinding_RefusesTheOrkerChannels(t *testing.T) {
	for _, raw := range []string{task.OrkersQueue, "ork-orker-x"} {
		_, err := task.ResolvePoolBinding("", raw)
		if !errors.Is(err, task.ErrReservedQueue) || !strings.Contains(err.Error(), "ORK_ORKER_QUEUE") {
			t.Errorf("ResolvePoolBinding(\"\", %q) = %v; want ErrReservedQueue naming ORK_ORKER_QUEUE", raw, err)
		}
	}
}

// TestPoolForQueue_NeverNamesAnOrkerChannel: neither Orker channel can ever be
// reported as a pool.
func TestPoolForQueue_NeverNamesAnOrkerChannel(t *testing.T) {
	control, err := task.QueueForOrker("orker-7f3a")
	if err != nil {
		t.Fatal(err)
	}
	for _, queue := range []string{task.OrkersQueue, control} {
		if pool, ok := task.PoolForQueue(queue); ok {
			t.Errorf("PoolForQueue(%q) = %q, true; an Orker channel is no pool", queue, pool)
		}
	}
}
