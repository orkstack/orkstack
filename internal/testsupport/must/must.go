// Package must holds the one-line form of a test's most common check: stop
// the test when an operation that has to succeed did not.
//
//	must.OKf(t, err, "put attempt %d", n)
//
// reads exactly as
//
//	if err != nil {
//		t.Fatalf("put attempt %d: %v", n, err)
//	}
//
// and must.OK(t, err) as t.Fatal(err). OKf is a printf wrapper, so go vet
// checks its format against its arguments as it checks t.Fatalf's. It is for
// tests only: it stops the test, and nothing outside a test may do that.
package must

import (
	"fmt"
	"testing"
)

// OK fails t now with err when err is not nil.
func OK(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// OKf fails t now when err is not nil, naming the operation: the failure
// reads "<format, with args>: <err>".
func OKf(t testing.TB, err error, format string, args ...any) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprintf(format, args...), err)
	}
}
