package racesuite

import (
	"sync"
	"testing"
)

// TestRacy writes one variable from two goroutines with nothing between them:
// the race detector reports it, and the test's own assertions still hold.
func TestRacy(t *testing.T) {
	var n int
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				n++
			}
		}()
	}
	wg.Wait()
	if n == 0 {
		t.Fatal("no increments")
	}
}

// TestClean shares a variable under a mutex.
func TestClean(t *testing.T) {
	var mu sync.Mutex
	var n int
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				mu.Lock()
				n++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if n != 2000 {
		t.Fatalf("n = %d", n)
	}
}

// TestScaleSkippedByShort is the shape of the repository's scale tests: it
// skips under -short, which the race gate passes.
func TestScaleSkippedByShort(t *testing.T) {
	if testing.Short() {
		t.Skip("a scale test: the unit gate runs it without -short")
	}
}
