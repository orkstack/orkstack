// Package eachlines is the one parser of the lines of an artifact that a
// fan-out reads (over_artifact:). It holds the line rules (one splitter, with
// batching), their limits, the streaming indexer that turns a source into a
// line index, the v1 index codec, and the slice a batched item is staged from.
//
// It does no IO of its own beyond the reader it is handed, knows no store, no
// engine and no plan: a caller passes the limits it pinned. It is shared by the
// Orker, which builds and reads the index, and by `orkctl task run`.
//
// The rules, for a source read as bytes:
//
//   - a line ends at LF; one CR directly before an LF, or at the end of the
//     source, is not part of it; a BOM at byte 0 is skipped;
//   - a line with no content left is empty: skipped, and not counted;
//   - a line is UTF-8; in jsonl it is one JSON value (a key written twice is
//     refused), and one that holds only whitespace is refused;
//   - a line's content is at most values_max_bytes, and a line that has passed
//     values_max_bytes + 2 bytes without an LF fails at once, so a source with no
//     LF is never held in memory;
//   - item i holds non-empty lines i*K ... min((i+1)*K, lines) - 1, and its span
//     runs from the first byte of its first line to the last byte of its last
//     line's content, so terminators and empty lines between its lines are in it.
//
// A refusal is an *Error carrying the machine-readable reason; any other error
// is the reader's own or a cancelled context, and a caller may retry it.
package eachlines

import (
	"errors"
	"fmt"
)

// Format is how a line is read: as a JSON value or as text.
type Format uint8

const (
	// FormatJSONL reads each line as one JSON value.
	FormatJSONL Format = 1
	// FormatLines reads each line as text.
	FormatLines Format = 2
)

// ParseFormat maps the DSL's word to a Format.
func ParseFormat(s string) (Format, bool) {
	switch s {
	case "jsonl":
		return FormatJSONL, true
	case "lines":
		return FormatLines, true
	}
	return 0, false
}

// String is the DSL's word, or "unknown".
func (f Format) String() string {
	switch f {
	case FormatJSONL:
		return "jsonl"
	case FormatLines:
		return "lines"
	}
	return "unknown"
}

func (f Format) valid() bool { return f == FormatJSONL || f == FormatLines }

// Reasons: the machine-readable codes this package's refusals carry. They are
// the REASONS.md codes of the same names.
const (
	ReasonLineInvalid     = "EACH_LINE_INVALID"
	ReasonTooManyItems    = "EACH_TOO_MANY_ITEMS"
	ReasonSourceTooLarge  = "EACH_SOURCE_TOO_LARGE"
	ReasonBatchTooLarge   = "EACH_BATCH_TOO_LARGE"
	ReasonExpansionFailed = "EACH_EXPANSION_FAILED"
	// ReasonItemUnavailable is what a reader of a stored index reports for an
	// index that contradicts itself or its source.
	ReasonItemUnavailable = "EACH_ITEM_UNAVAILABLE"
)

const (
	// ReadBufferSize is the size of the buffer the indexer and SliceTo read
	// through: the most one Read of the source asks for, and the most the
	// indexer has read past the line it stopped on.
	ReadBufferSize = 1 << 20
	// MaxBatch is the largest batch: K is 1 ... MaxBatch.
	MaxBatch = 10000
)

// Error is a refusal with its reason.
type Error struct {
	Reason  string
	Message string
}

func (e *Error) Error() string { return e.Reason + ": " + e.Message }

func refuse(reason, format string, args ...any) error {
	return &Error{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ReasonOf is the reason of a refusal, or "" for any other error.
func ReasonOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

// SliceMax is the largest span of one item: K lines of values_max_bytes, each
// with a CR and an LF, bounded by the plan's slice_max_bytes.
func SliceMax(k int, valuesMaxBytes, planSliceMaxBytes int64) int64 {
	return min(int64(k)*(valuesMaxBytes+2), planSliceMaxBytes)
}

// ReadCap is the most bytes an index reads: max_items items of sliceMax each
// (and the 3 of a BOM), never more than artifact_max_bytes. sliceMax is
// SliceMax's value for the fan-out's K.
func ReadCap(maxItems int, sliceMax, artifactMaxBytes int64) int64 {
	return min(int64(maxItems)*sliceMax+3, artifactMaxBytes)
}
