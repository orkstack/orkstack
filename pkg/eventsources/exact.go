package eventsources

import (
	"bytes"
	"encoding/json/jsontext"
	"math"
	"strconv"
)

// holdsExactly reports whether a run's input holds raw, one JSON value, as it
// is: every number in it, written again from the double it becomes, is the
// number the text gives. A message's number is never changed on its way in.
func holdsExactly(raw jsontext.Value) bool {
	if !raw.IsValid() {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	for {
		kind := dec.PeekKind()
		if kind == 0 {
			return true
		}
		if kind != '0' {
			if _, err := dec.ReadToken(); err != nil {
				return true
			}
			continue
		}
		tok, err := dec.ReadToken()
		if err != nil {
			return true
		}
		text := tok.String()
		f, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(f, 0) || !equalNumbers(text, strconv.FormatFloat(f, 'g', -1, 64)) {
			return false
		}
	}
}
