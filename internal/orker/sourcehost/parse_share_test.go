package sourcehost

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
)

// asked is one lane's request of a share, in a bubble.
type asked struct {
	release func()
	err     error
	cancel  context.CancelFunc
	done    chan struct{}
}

func ask(p *ParseShare, owner string, n int64) *asked {
	ctx, cancel := context.WithCancel(context.Background())
	a := &asked{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(a.done)
		a.release, a.err = p.Acquire(ctx, owner, n)
	}()
	synctest.Wait()
	return a
}

func (a *asked) has() bool {
	synctest.Wait()
	select {
	case <-a.done:
		return a.err == nil
	default:
		return false
	}
}

func wantHas(t *testing.T, what string, want bool, as ...*asked) {
	t.Helper()
	for i, a := range as {
		if a.has() != want {
			t.Errorf("%s, %d: has %v", what, i, a.has())
		}
	}
}

// TestParseShare: lanes are served in the order they asked, an instance holds no
// more than half while another's lane waits, and a body over the whole share is
// read alone.
func TestParseShare(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewParseShare(100)
		a1, a2 := ask(p, "a", 40), ask(p, "a", 60)
		wantHas(t, "an instance alone has all of it", true, a1, a2)
		b1, c1 := ask(p, "b", 50), ask(p, "c", 10)
		wantHas(t, "full", false, b1, c1)
		a1.release()
		wantHas(t, "what is first is not passed by what is smaller", false, b1, c1)
		a2.release()
		wantHas(t, "in order", true, b1, c1)
		a2.release() // twice is once
		d1 := ask(p, "d", 41)
		wantHas(t, "a share given back twice", false, d1)
		b1.release()
		c1.release()
		wantHas(t, "", true, d1)
		d1.release()
	})
	synctest.Test(t, func(t *testing.T) {
		p := NewParseShare(100)
		a1, a2 := ask(p, "a", 40), ask(p, "a", 50)
		b1, a3, b2 := ask(p, "b", 20), ask(p, "a", 40), ask(p, "b", 30)
		wantHas(t, "another's lane waits", false, b1, a3, b2)
		a1.release()
		wantHas(t, "the other is served past an instance over its half", true, b1, b2)
		wantHas(t, "over its half", false, a3)
		b1.release()
		wantHas(t, "over its half, and full", false, a3)
		b2.release()
		wantHas(t, "alone again", true, a3)
		a2.release()
		a3.release()
	})
	synctest.Test(t, func(t *testing.T) {
		p := NewParseShare(100)
		a1, a2 := ask(p, "a", 30), ask(p, "a", 30)
		big := ask(p, "b", 1000)
		a3 := ask(p, "a", 30)
		wantHas(t, "larger than all of it", false, big, a3)
		a1.release()
		a2.release()
		wantHas(t, "alone", true, big)
		wantHas(t, "beside it", false, a3)
		big.release()
		wantHas(t, "after it", true, a3)
		a3.release()
	})
	synctest.Test(t, func(t *testing.T) {
		p := NewParseShare(100)
		a1 := ask(p, "a", 100)
		b1, b2 := ask(p, "b", 90), ask(p, "c", 10)
		b1.cancel()
		if wantHas(t, "cancelled", false, b1); !errors.Is(b1.err, context.Canceled) {
			t.Error(b1.err)
		}
		a1.release()
		wantHas(t, "what waited behind one that gave up", true, b2)
		all := ask(p, "d", 90)
		wantHas(t, "what one that gave up held", true, all)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := p.Acquire(ctx, "e", 1); !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
		all.release()
		b2.release()
		if p.used != 0 || len(p.held) != 0 || len(p.wait) != 0 {
			t.Errorf("%d used, %v, %d waiting", p.used, p.held, len(p.wait))
		}
	})
	if release, err := (*ParseShare)(nil).Acquire(t.Context(), "a", 1); err != nil {
		t.Error(err)
	} else {
		release()
	}
}

// TestParseShare_NeverOver: whatever asks, what is held is never over the share,
// and everything asked for is had.
func TestParseShare_NeverOver(t *testing.T) {
	p := NewParseShare(1000)
	var mu sync.Mutex
	var held, most int64
	var wg sync.WaitGroup
	for lane := range 16 {
		wg.Go(func() {
			for i := range 200 {
				n := int64((lane*37+i*61)%700 + 1)
				release, err := p.Acquire(t.Context(), lane%4, n)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				held += n
				most = max(most, held)
				mu.Unlock()
				runtime.Gosched()
				mu.Lock()
				held -= n
				mu.Unlock()
				release()
			}
		})
	}
	wg.Wait()
	if most > 1000 || p.used != 0 || len(p.held) != 0 {
		t.Errorf("at most %d held at once, %d left", most, p.used)
	}
}
