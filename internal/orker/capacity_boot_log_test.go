package orker

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
)

// lockedBuffer is a bytes.Buffer a logger may write while a test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestBoot_SaysWhetherACapacityBoundsAdmission: an Orker that declares no
// capacity admits by max_concurrent alone, a count of tasks, and says at boot
// that its admission is best effort; one that declares a capacity says that
// instead (docs/DEPLOY.md, "Declaring an Orker's capacity").
func TestBoot_SaysWhetherACapacityBoundsAdmission(t *testing.T) {
	const (
		declared   = `msg="capacity declared"`
		undeclared = `msg="capacity not declared: admission is best effort, bounded only by max_concurrent"`
	)
	for _, tc := range []struct {
		name        string
		capacityCPU string
		want        []string
		notWant     string
	}{
		{
			name: "no capacity",
			want: []string{undeclared, "max_concurrent=16", `per_task_cap="8 cpu, 17179869184 bytes"`},
			// The declared line is not printed for an Orker that declares none.
			notWant: declared,
		},
		{
			name:        "a capacity",
			capacityCPU: "16",
			want:        []string{declared},
			notWant:     undeclared,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedBuffer{}
			activities := NewActivitiesWithOptions(nil, Options{
				Runtime:     &reapRecorder{labelled: map[string]int{}},
				WorkRoot:    t.TempDir(),
				CapacityCPU: tc.capacityCPU,
				TaskQueue:   "q",
				Logger:      slogTo(logs),
			})
			t.Cleanup(activities.Close)
			if err := activities.Boot(context.Background()); err != nil {
				t.Fatalf("Boot = %v", err)
			}
			got := logs.String()
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("boot log lacks %s:\n%s", want, got)
				}
			}
			if strings.Contains(got, tc.notWant) {
				t.Errorf("boot log carries %s:\n%s", tc.notWant, got)
			}
		})
	}
}
