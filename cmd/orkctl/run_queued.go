package main

import (
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// queuedLine is `run get`'s Queued line: why the run waits for its
// concurrency slot, where it stands, behind whom and until when, then the
// limit's note, if any (docs/CLI.md, run get; the UI's "Queued for key"
// panel says the same).
func queuedLine(q *orkstackv1.ConcurrencyQueued) string {
	place := "place unknown"
	if q.GetPositionKnown() {
		place = ordinal(int(q.GetPosition())) + " in line"
	}
	where := fmt.Sprintf("limit %d, %s", q.GetLimit(), place)
	var b strings.Builder
	switch q.GetCause() {
	case orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_UNPAUSE:
		fmt.Fprintf(&b, "waiting again for key %s after an unpause (%s)", q.GetKey(), where)
	case orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_RESUME:
		fmt.Fprintf(&b, "queued for key %s after a resume (%s)", q.GetKey(), where)
	case orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_FAILING:
		fmt.Fprintf(&b, "waiting for its concurrency slot to run its failure handlers (key %s, %s)", q.GetKey(), where)
	default:
		fmt.Fprintf(&b, "queued for key %s (%s)", q.GetKey(), where)
	}
	if holders := q.GetHoldersAhead(); len(holders) > 0 {
		b.WriteString(" behind " + strings.Join(holders, ", "))
		if more := int(q.GetHoldersCount()) - len(holders); more > 0 {
			fmt.Fprintf(&b, " and %d more", more)
		}
		if q.GetHoldersAsOfArrival() {
			b.WriteString(" (as of its arrival)")
		}
	}
	fmt.Fprintf(&b, "; since %s, times out %s", formatTime(q.GetQueuedSince()), formatTime(q.GetTimesOutAt()))
	if note := q.GetNote(); note != "" {
		b.WriteString("; " + note)
	}
	return b.String()
}

// ordinal spells n as 1st, 2nd, 3rd, 4th, 11th, 21st.
func ordinal(n int) string {
	suffix := "th"
	switch {
	case n%100 >= 11 && n%100 <= 13:
	case n%10 == 1:
		suffix = "st"
	case n%10 == 2:
		suffix = "nd"
	case n%10 == 3:
		suffix = "rd"
	}
	return fmt.Sprintf("%d%s", n, suffix)
}
