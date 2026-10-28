package fuzzsuite

import "testing"

func TestPlain(t *testing.T) {}

// FuzzSeedPasses holds a seed every input of which passes.
func FuzzSeedPasses(f *testing.F) {
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, b []byte) {})
}

// FuzzSeedFails fails on the corpus file under testdata/fuzz/FuzzSeedFails,
// which `go test` runs as a seed with no -fuzz flag. A gate that ran only the
// f.Add seed would pass it; one that runs the corpus fails it.
func FuzzSeedFails(f *testing.F) {
	f.Add([]byte("seed"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if string(b) == "crasher" {
			t.Fatalf("the corpus input %q fails", b)
		}
	})
}
