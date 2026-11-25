package authn

import (
	"sync"
	"testing"
	"time"
)

func TestLogBound_OneLineASecondOfEachKindSaysWhatItDropped(t *testing.T) {
	at := time.Unix(1700000000, 0)
	b := &LogBound{now: func() time.Time { return at }}
	var lines []int
	ask := func(kind string) { b.Do(kind, func(dropped int) { lines = append(lines, dropped) }) }
	ask("a")
	ask("a")
	ask("a")
	ask("b") // another kind is not held back
	if len(lines) != 2 || lines[0] != 0 || lines[1] != 0 {
		t.Fatalf("lines = %v, want one of each kind", lines)
	}
	at = at.Add(time.Second)
	ask("a")
	if len(lines) != 3 || lines[2] != 2 {
		t.Fatalf("lines = %v, want the next one to say it dropped 2", lines)
	}
	at = at.Add(time.Hour)
	ask("a")
	if lines[len(lines)-1] != 0 {
		t.Fatalf("lines = %v, want the count started again", lines)
	}
}

func TestLogBound_IsSafeAndLogsOutsideItsLock(t *testing.T) {
	var b LogBound
	b.Do("outer", func(int) { b.Do("inner", func(int) {}) }) // would deadlock under the lock
	var wg sync.WaitGroup
	var mu sync.Mutex
	lines := 0
	for range 64 {
		wg.Go(func() { b.Do("k", func(int) { mu.Lock(); lines++; mu.Unlock() }) })
	}
	wg.Wait()
	if lines != 1 {
		t.Fatalf("%d lines, want 1", lines)
	}
}
