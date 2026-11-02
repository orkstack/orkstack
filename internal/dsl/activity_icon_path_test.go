package dsl

import (
	"strings"
	"testing"
)

func TestCheckPathData_AcceptsTheSVGPathGrammar(t *testing.T) {
	for _, d := range []string{
		"M4 7h16",
		"m4 7h16",
		"  M4 7 L 20 7  ",
		"M1,2 3,4 5,6",                          // a moveto's later pairs are linetos
		"M1-2-3-4",                              // a sign starts the next number
		"M.5.5.5.5",                             // a second point starts the next number
		"M1e2 1E-2l+3.5-.5",                     // exponents and signs
		"M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z", // arcs, flags, a repeated group
		"M3 3a1 1 0 0118 0",                     // flags written without a separator
		"M12 3c2.5 2.6 3.75 5.6 3.75 9s-1.25 6.4-3.75 9S9.5 5.6 12 3z",
		"M2 2q3 3 6 0t6 0Q1 1 2 2T4 4",
		"M2 2H10V10h-8v-8Z M4 4z",
		"M1 1\t\n\f\rL2 2",
		"M1 1z m2 2",
	} {
		if err := CheckIconPathData(d); err != nil {
			t.Errorf("CheckIconPathData(%q) refused it: %v", d, err)
		}
	}
}

func TestCheckPathData_RefusesAnythingElse(t *testing.T) {
	for _, tc := range []struct{ d, want string }{
		{"", "is empty"},
		{"   ", "is empty"},
		{"L1 2", "begins with 'L'"},
		{"1 2", "where a moveto (M or m) was expected"},
		{"M1", "ends where a number was expected"},
		{"M1 2 3", "ends where a number was expected"},
		{"M0 0L1", "L command"},
		{"M0 0H", "H command"},
		{"M0 0V", "V command"},
		{"M0 0C1 2 3 4", "C command"},
		{"M0 0S1 2 3 4 5 6", "S command"},
		{"M0 0Q1 2 3 4 5 6", "Q command"},
		{"M0 0T1", "T command"},
		{"M0 0A1 1 0 0 1 2", "A command"},
		{"M1 2Z3", "where a path command"},
		{"M1 2Z,", "where a path command"},
		{"M,1 2", "where a number was expected"},
		{"M1 2,", "a number after the comma"},
		{"M1,,2", "where a number was expected"},
		{"M1 2,L3 4", "a number after the comma"},
		{"M1 2 X", "where a path command"},
		{"M1e 2", "where a number was expected"}, // "1e" is the number 1 and an e that is no number
		{"M1 2e", "where a path command"},        // ...and no command
		{"M1 1e400", "not a number from -10000 to 10000"},
		{"M1 20000", "not a number from -10000 to 10000"},
		{"M1 2 \u00e9", `'é'`},
		{"M1 2 \xff", "0xff"},
		{"M1 2a1 1 0 2 0 3 3", "an arc flag"},
		{"M1 2a1 1 0 0", "an arc flag"},
		{"M1 2\"/><script>", "where a path command"},
		{"M1 2 url(#x)", "where a path command"},
		{"M1 2 &#76;", "where a path command"},
		{"M1 2 \u00a0", "where a path command"},
		{"M1 2 \x00", "where a path command"},
		{"M- 2", "where a number was expected"},
		{"M. 2", "where a number was expected"},
	} {
		err := CheckIconPathData(tc.d)
		if err == nil {
			t.Errorf("CheckIconPathData(%q) accepted it", tc.d)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("CheckIconPathData(%q) = %v, want it to say %q", tc.d, err, tc.want)
		}
	}
}

// TestCheckPathData_EachCommandTakesExactlyItsArity: for every command
// letter, either case, a group of exactly its arity is accepted and every
// shorter count is refused — so an arity made smaller or larger is caught.
// The arities are the SVG grammar's, written here apart from the scanner.
func TestCheckPathData_EachCommandTakesExactlyItsArity(t *testing.T) {
	arity := map[string]int{"M": 2, "L": 2, "T": 2, "H": 1, "V": 1, "C": 6, "S": 4, "Q": 4, "A": 7}
	for upper, n := range arity {
		for _, letter := range []string{upper, strings.ToLower(upper)} {
			// Ones are numbers and arc flags alike.
			numbers := func(k int) string { return strings.TrimSpace(strings.Repeat(" 1", k)) }
			if err := CheckIconPathData("M0 0" + letter + numbers(n)); err != nil {
				t.Errorf("%s with %d numbers: %v", letter, n, err)
			}
			for k := 1; k < n; k++ {
				if err := CheckIconPathData("M0 0" + letter + numbers(k)); err == nil {
					t.Errorf("%s with %d numbers was accepted; it takes %d", letter, k, n)
				}
			}
		}
	}
}
