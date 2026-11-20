package task

import (
	"errors"
	"strings"
	"testing"
)

func TestQueueForSource(t *testing.T) {
	tests := []struct {
		name, pool, defaultQueue, want string
		wantErr                        error
	}{
		{name: "default pool, unconfigured", pool: "", defaultQueue: "", want: "ork-source-main"},
		{name: "main, unconfigured", pool: "main", defaultQueue: "", want: "ork-source-main"},
		{name: "named pool", pool: "gpu", defaultQueue: "", want: "ork-source-gpu"},
		{name: "default bound to a pool's queue", pool: "", defaultQueue: "ork-pool-gpu", want: "ork-source-gpu"},
		{name: "main bound to a pool's queue", pool: "main", defaultQueue: "ork-pool-gpu", want: "ork-source-gpu"},
		{name: "that pool by name, bound as default", pool: "gpu", defaultQueue: "ork-pool-gpu", want: "ork-source-gpu"},
		{name: "raw isolation channel keeps its own", pool: "", defaultQueue: "ork-tasks-agent", want: "ork-source-_ork-tasks-agent"},
		{name: "a named pool beside a raw channel", pool: "gpu", defaultQueue: "ork-tasks-agent", want: "ork-source-gpu"},
		{name: "a pool named like a channel cannot forge one", pool: "ork-pool-x", defaultQueue: "", want: "ork-source-ork-pool-x"},
		{name: "invalid pool", pool: "GPU", wantErr: ErrInvalidPool},
		{name: "pool past 200 bytes", pool: "a" + strings.Repeat("b", 200), wantErr: ErrInvalidPool},
		{name: "a source channel as the bound default", pool: "", defaultQueue: "ork-source-main", wantErr: ErrReservedQueue},
		{name: "the engine's event channel as the default", pool: "", defaultQueue: EngineEventQueue, wantErr: ErrReservedQueue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := QueueForSource(tt.pool, tt.defaultQueue)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("QueueForSource(%q, %q) = %q, %v; want %v", tt.pool, tt.defaultQueue, got, err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("QueueForSource(%q, %q) = %q, %v; want %q", tt.pool, tt.defaultQueue, got, err, tt.want)
			}
		})
	}
}

// TestQueueForSource_NeverATaskChannel: no pool's source channel is a channel
// a task can be routed to, and no task channel is a source channel.
func TestQueueForSource_NeverATaskChannel(t *testing.T) {
	for _, def := range []string{"", "ork-pool-gpu", "ork-tasks-agent"} {
		for _, pool := range []string{"", "main", "gpu", "source-main", "x"} {
			source, err := QueueForSource(pool, def)
			if err != nil {
				t.Fatal(err)
			}
			task, err := QueueForPool(pool, def)
			if err != nil {
				t.Fatal(err)
			}
			if source == task || !strings.HasPrefix(source, sourceQueuePrefix) || strings.HasPrefix(task, sourceQueuePrefix) {
				t.Fatalf("pool %q, default %q: source %q, task %q", pool, def, source, task)
			}
			if _, err := QueueForPool("", source); !errors.Is(err, ErrReservedQueue) {
				t.Fatalf("a source channel %q bound as a task default: %v", source, err)
			}
		}
	}
}

func TestPoolForSourceQueue(t *testing.T) {
	for _, pool := range []string{"main", "gpu", "a-b"} {
		queue, err := QueueForSource(pool, "")
		if err != nil {
			t.Fatal(err)
		}
		got, ok := PoolForSourceQueue(queue)
		if !ok || got != pool {
			t.Fatalf("PoolForSourceQueue(%q) = %q, %v; want %q", queue, got, ok, pool)
		}
	}
	for _, queue := range []string{"ork-source-", "ork-source-_ork-tasks-agent", "ork-source-GPU", "ork-pool-gpu", "ork-tasks", "ork-source-ork-tasks-agent!", "", "ork-sourcegpu"} {
		if pool, ok := PoolForSourceQueue(queue); ok {
			t.Fatalf("PoolForSourceQueue(%q) = %q, true; want false", queue, pool)
		}
	}
}

func FuzzQueueForSource(f *testing.F) {
	for _, seed := range [][2]string{{"", ""}, {"gpu", ""}, {"", "ork-tasks-agent"}, {"main", "ork-pool-gpu"}, {"ork-source-x", ""}, {"", "ork-source-x"}} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, pool, def string) {
		queue, err := QueueForSource(pool, def)
		if err != nil {
			return
		}
		if !strings.HasPrefix(queue, sourceQueuePrefix) {
			t.Fatalf("QueueForSource(%q, %q) = %q: not a source channel", pool, def, queue)
		}
		if reservedQueue(queue) == nil {
			t.Fatalf("source channel %q is not reserved against task routing", queue)
		}
		if p, ok := PoolForSourceQueue(queue); ok && validPool(p) != nil {
			t.Fatalf("PoolForSourceQueue(%q) named an invalid pool %q", queue, p)
		}
	})
}
