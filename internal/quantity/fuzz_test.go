package quantity_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/orkstack/orkstack/internal/quantity"
)

// FuzzParseCPU states what the cpu grammar claims, for any input: it never
// panics; an accepted quantity is a positive, finite number of cpus; and an
// accepted value's canonical spellings — the decimal number and, when it is a
// whole number of millicpus, the "m" form — read back as the same value.
func FuzzParseCPU(f *testing.F) {
	for _, tc := range cpuCases {
		f.Add(tc.raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		cores, err := quantity.CPU(raw)
		if err != nil {
			return
		}
		if !quantity.PositiveFinite(cores) {
			t.Fatalf("CPU(%q) accepted %v, which is no limit", raw, cores)
		}
		canonical := strconv.FormatFloat(cores, 'g', -1, 64)
		if again, err := quantity.CPU(canonical); err != nil || again != cores {
			t.Fatalf("CPU(%q) = %v, but its canonical form %q reads %v, %v", raw, cores, canonical, again, err)
		}
		if milli := cores * 1000; milli == math.Trunc(milli) && milli < 1<<53 {
			spelled := strconv.FormatFloat(milli, 'f', -1, 64) + "m"
			if again, err := quantity.CPU(spelled); err != nil || again != milli/1000 {
				t.Fatalf("CPU(%q) = %v, but %q reads %v, %v", raw, cores, spelled, again, err)
			}
		}
	})
}

// FuzzParseMemory states the same for memory: no panic; an accepted quantity
// is a byte count an int64 holds; and a whole number of bytes written plainly
// reads back as itself.
func FuzzParseMemory(f *testing.F) {
	for _, tc := range memoryCases {
		f.Add(tc.raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		bytes, err := quantity.Memory(raw)
		if err != nil {
			return
		}
		if !quantity.ByteCount(bytes) {
			t.Fatalf("Memory(%q) accepted %v, which is no byte count", raw, bytes)
		}
		if bytes == math.Trunc(bytes) && bytes < 1<<53 {
			canonical := strconv.FormatFloat(bytes, 'f', -1, 64)
			if again, err := quantity.Memory(canonical); err != nil || again != bytes {
				t.Fatalf("Memory(%q) = %v, but %q reads %v, %v", raw, bytes, canonical, again, err)
			}
		}
	})
}
