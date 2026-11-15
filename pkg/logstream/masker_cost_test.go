package logstream_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/logstream"
)

// maskCostPerMiB bounds the thread CPU time masking one MiB of output takes
// with a full set of forms: about five times the 18 ms it took where it was
// measured. It is a cost bound, never a wall clock (AGENTS.md §11.1).
const maskCostPerMiB = 90 * time.Millisecond

// A task's output costs one table step per byte, whatever the number of
// forms: 4 MiB of log lines, with secrets among them, through a masker
// holding about a thousand forms stays within a thread-CPU bound per MiB.
func TestMasker_CostPerMiBIsBoundedWithAFullSet(t *testing.T) {
	slowdown := time.Duration(1)
	if raceBuild {
		slowdown = raceSlowdown
	}
	m := logstream.NewMasker()
	rng := rand.New(rand.NewPCG(1, 2))
	var secrets [][]byte
	for i := 0; ; i++ {
		v := make([]byte, 24)
		for j := range v {
			v[j] = byte('a' + rng.IntN(26))
		}
		if _, err := m.Add(fmt.Sprintf("v%d", i), v); err != nil {
			break // the set is full
		}
		secrets = append(secrets, v)
	}
	forms, size := m.Size()
	if forms < logstream.MaskMaxForms-32 {
		t.Fatalf("the set holds %d forms, want it nearly full", forms)
	}
	var stream bytes.Buffer
	for stream.Len() < 4<<20 {
		fmt.Fprintf(&stream, "%s step %d: processing record %d of the batch\n", time.Duration(stream.Len()), stream.Len()%977, rng.IntN(1e6))
		if rng.IntN(50) == 0 {
			fmt.Fprintf(&stream, "token=%s\n", secrets[rng.IntN(len(secrets))])
		}
	}
	input := stream.Bytes()
	w := m.Writer(io.Discard)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	startCPU, cpu := threadCPU()
	startWall := time.Now()
	for off := 0; off < len(input); off += 32 << 10 {
		if _, err := w.Write(input[off:min(off+32<<10, len(input))]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	cost, clock := time.Since(startWall), "wall clock"
	if end, ok := threadCPU(); cpu && ok {
		cost, clock = end-startCPU, "thread CPU time"
	}
	mib := float64(len(input)) / (1 << 20)
	perMiB := time.Duration(float64(cost) / mib)
	t.Logf("%.1f MiB through %d forms (%d bytes) took %v of %s: %v per MiB", mib, forms, size, cost, clock, perMiB)
	if bound := maskCostPerMiB * slowdown; perMiB > bound {
		t.Errorf("masking took %v per MiB of %s, want at most %v", perMiB, clock, bound)
	}
}

// Forms added and dropped while both streams are written: no race, and the
// value registered for the whole run is masked in both.
func TestMasker_AddAndDropWhileStreamsRun(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("static", []byte("static-secret-value")); err != nil {
		t.Fatal(err)
	}
	var outs [2]bytes.Buffer
	writers := [2]*logstream.MaskWriter{m.Writer(&outs[0]), m.Writer(&outs[1])}
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 500; n++ {
				if _, err := fmt.Fprintf(writers[i], "line %d static-secret-value end\n", n); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 200; n++ {
			if _, err := m.Add(fmt.Sprintf("lease-%d", n), []byte(fmt.Sprintf("rotating-value-%04d", n))); err != nil {
				t.Error(err)
				return
			}
			if n > 0 {
				m.Drop(fmt.Sprintf("lease-%d", n-1))
			}
		}
	}()
	wg.Wait()
	for i, w := range writers {
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(outs[i].Bytes(), []byte("static-secret")) {
			t.Errorf("stream %d shows the static value", i)
		}
		if got := bytes.Count(outs[i].Bytes(), []byte("***")); got != 500 {
			t.Errorf("stream %d has %d masked runs, want 500", i, got)
		}
	}
}

// A write allocates nothing once the stream is running.
func TestMasker_AWriteAllocatesNothing(t *testing.T) {
	if raceBuild {
		t.Skip("the race detector allocates on its own")
	}
	m := logstream.NewMasker()
	if _, err := m.Add("h", []byte("hunter2-correct-horse")); err != nil {
		t.Fatal(err)
	}
	w := m.Writer(io.Discard)
	line := []byte("a line with hunter2-correct-horse in it, and a prefix hunter2-c\n")
	_, _ = w.Write(line)
	if allocs := testing.AllocsPerRun(200, func() { _, _ = w.Write(line) }); allocs != 0 {
		t.Errorf("a write allocates %.1f times, want none", allocs)
	}
}
