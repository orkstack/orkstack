package artifacts

import (
	"strconv"
	"testing"
)

// FuzzParseMark: a resume's mark is read only as the bytes a mark of that
// ending is written with.
func FuzzParseMark(f *testing.F) {
	for _, seed := range []string{"", "0", "7", "007", "+7", "-1", "7\n", " 7", "18446744073709551615", "18446744073709551616", "x"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, mark []byte) {
		scope, ok := parseMark(mark)
		if ok != (strconv.FormatUint(scope, 10) == string(mark)) {
			t.Fatalf("parseMark(%q) = %d, %v", mark, scope, ok)
		}
		if again, ok := parseMark([]byte(strconv.FormatUint(uint64(len(mark)), 10))); !ok || again != uint64(len(mark)) {
			t.Fatalf("a mark of %d reads %d, %v", len(mark), again, ok)
		}
	})
}
