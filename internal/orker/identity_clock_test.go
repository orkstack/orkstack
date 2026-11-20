package orker

import (
	"context"
	"testing"
	"time"
)

// TestSystemIdentityClock: the boot clock moves forward with time, and a
// sleep ends early when its context does.
func TestSystemIdentityClock(t *testing.T) {
	var c systemIdentityClock
	a := c.Boot()
	if a <= 0 {
		t.Fatalf("boot clock %v", a)
	}
	if err := c.Sleep(context.Background(), 2*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if b := c.Boot(); b-a < 2*time.Millisecond {
		t.Fatalf("the boot clock moved %v across a 2 ms sleep", b-a)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Hour); err == nil {
		t.Fatal("a sleep outlived its context")
	}
}
