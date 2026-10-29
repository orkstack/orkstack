package quantity_test

import (
	"errors"
	"testing"

	"github.com/orkstack/orkstack/internal/quantity"
)

// cpuCases and memoryCases are the spellings the table tests hold the
// grammar to, and the fuzz targets' seeds.
var cpuCases = []struct {
	raw  string
	want float64
	ok   bool
}{
	{"2", 2, true}, {"0.5", 0.5, true}, {"500m", 0.5, true}, {" 4 ", 4, true}, {"1500m", 1.5, true},
	{"", 0, false}, {"banana", 0, false}, {"8 cores", 0, false}, {"2c", 0, false},
	{"0", 0, false}, {"-1", 0, false}, {"0m", 0, false}, {"NaN", 0, false}, {"Inf", 0, false},
}

var memoryCases = []struct {
	raw  string
	want float64
	ok   bool
}{
	{"1024", 1024, true}, {"1Ki", 1024, true}, {"1Mi", 1 << 20, true}, {"4Gi", 4 << 30, true},
	{"1Ti", 1 << 40, true}, {"1M", 1e6, true}, {"2G", 2e9, true}, {"512k", 512e3, true},
	{"", 0, false}, {"4GB", 0, false}, {"16gi", 0, false}, {"lots", 0, false},
	{"0", 0, false}, {"0.5", 0, false}, {"-1Gi", 0, false}, {"NaN", 0, false}, {"1e300Ti", 0, false},
}

func TestCPU_ReadsTheKubernetesSpellingAndRequiresALimit(t *testing.T) {
	for _, tc := range cpuCases {
		got, err := quantity.CPU(tc.raw)
		if tc.ok != (err == nil) || (tc.ok && got != tc.want) {
			t.Errorf("CPU(%q) = %v, %v; want %v, ok=%v", tc.raw, got, err, tc.want, tc.ok)
		}
	}
	if _, err := quantity.CPU("0"); !errors.Is(err, quantity.ErrNotPositiveCPU) {
		t.Errorf("CPU(\"0\") = %v, want ErrNotPositiveCPU: a zero limit is unlimited to the runtime", err)
	}
}

func TestMemory_ReadsTheKubernetesSpellingAndRequiresALimit(t *testing.T) {
	for _, tc := range memoryCases {
		got, err := quantity.Memory(tc.raw)
		if tc.ok != (err == nil) || (tc.ok && got != tc.want) {
			t.Errorf("Memory(%q) = %v, %v; want %v, ok=%v", tc.raw, got, err, tc.want, tc.ok)
		}
	}
	if _, err := quantity.Memory("0"); !errors.Is(err, quantity.ErrNotByteCount) {
		t.Errorf("Memory(\"0\") = %v, want ErrNotByteCount", err)
	}
}
