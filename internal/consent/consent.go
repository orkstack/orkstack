// Package consent holds the digest a schedule's, a trigger's or an event
// source's binder consents to: what its Orkflow asks for, under a numbered
// projection (projection.go). The empty table's value was frozen first; the
// members of a table that is not empty are frozen with task identity. A later
// release that covers more adds c2: and never changes what c1: means.
package consent

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// Prefix1 opens a digest under projection 1.
const Prefix1 = "c1:"

func emptyBytes() []byte {
	b, err := stablejson.Marshal(table{Tasks: []roleTask{}, V: 1})
	if err != nil {
		// A constant value of a fixed type cannot fail to encode.
		panic("consent: the empty table does not encode: " + err.Error())
	}
	return b
}

var empty = func() string {
	sum := sha256.Sum256(emptyBytes())
	return Prefix1 + hex.EncodeToString(sum[:])
}()

// Empty is the empty table's digest: what a schedule or trigger whose
// Orkflow asks for nothing consents to.
func Empty() string { return empty }

// Valid reports a digest of projection 1: "c1:" and 64 lowercase hex digits.
func Valid(digest string) bool {
	hexPart, ok := strings.CutPrefix(digest, Prefix1)
	if !ok || len(hexPart) != 64 {
		return false
	}
	for i := 0; i < len(hexPart); i++ {
		if c := hexPart[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
