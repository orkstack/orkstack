package artifacts

import (
	"fmt"
)

// Item names one element of a fan-out task, or none. The ZERO VALUE is "not
// an item", so every existing literal (LogRef{RunID, TaskID, Attempt}, …)
// keeps its meaning; it is a comparable value type so refs stay usable as
// map keys and with ==.
type Item struct {
	Index int32
	Set   bool
}

// NoItem is the explicit spelling of the zero value at call sites.
var NoItem = Item{}

// ItemAt names item index of a fan-out.
func ItemAt(index int32) Item { return Item{Index: index, Set: true} }

// ItemOf converts a proto3 optional item selector: nil is NoItem.
func ItemOf(p *int32) Item {
	if p == nil {
		return NoItem
	}
	return ItemAt(*p)
}

// Proto renders the item as a proto3 optional: NoItem is nil.
func (i Item) Proto() *int32 {
	if !i.Set {
		return nil
	}
	v := i.Index
	return &v
}

// String renders "" for NoItem and the decimal index otherwise; for messages.
func (i Item) String() string {
	if !i.Set {
		return ""
	}
	return fmt.Sprintf("%d", i.Index)
}

// itemWidth is the zero-padded width of an item segment: five digits hold
// MaxItemIndex, so a lexicographic listing IS index order, and a five-wide
// segment can never be mistaken for a ten-wide attempt segment
// (parseFixedWidth checks width) — the two shapes cannot collide even under a
// listing that does not know which it is reading.
const itemWidth = 5

// MaxItemIndex is the largest index a key can carry. dsl.MaxEachItems is far
// below it; the builders refuse anything above it, or negative, with
// ErrInvalidCommitKey / ErrInvalidAttemptKey / ErrInvalidLogKey.
const MaxItemIndex = 99999

// errInvalidItem is wrapped by the family-specific sentinel at each builder.
var errInvalidItem = fmt.Errorf("artifacts: item index must be 0..%d", MaxItemIndex)

// ItemSegment renders one item as a key segment: fmt.Sprintf("%05d", index).
func ItemSegment(index int32) (string, error) {
	if index < 0 || index > MaxItemIndex {
		return "", fmt.Errorf("%w: got %d", errInvalidItem, index)
	}
	return fmt.Sprintf("%0*d", itemWidth, index), nil
}

// parseItemSegment reads a segment ItemSegment rendered: exactly itemWidth
// decimal digits. Any other width is not an item segment.
func parseItemSegment(s string) (int32, bool) {
	if len(s) != itemWidth {
		return 0, false
	}
	var n int32
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int32(c-'0')
	}
	return n, true
}
