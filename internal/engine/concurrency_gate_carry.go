package engine

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// A gate carries up to max_entries entries and max_tombstones tombstones
// across continue-as-new, within gateCarryBound (§5.1.9). Encoded as
// objects with RFC 3339 times they take about twice what that bound allows
// at the defaults, so each is a JSON array instead, its times in Unix
// milliseconds (0 for none). A tombstone's expiry is rounded up, so a carry
// never shortens a fence; every other time is rounded down, consistently,
// so the line's order is unchanged. Decoding is strict: a wrong length or
// type fails the whole carry, which then fails the gate's workflow task
// loudly rather than restoring a gate that is not the one that drained.

// The flag bits of an encoded entry.
const (
	gateFlagVerifyPending = 1 << iota
	gateFlagAdopted
	gateFlagGrantPending
	gateFlagStalled
)

func gateMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func gateMillisUp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	ms := t.UnixMilli()
	if time.UnixMilli(ms).Before(t) {
		ms++
	}
	return ms
}

func gateTimeOf(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// MarshalJSON encodes e as [run, task_ix, seq_base, limit, strategy, proto,
// label_ix, arrived_at, seq, wait_until, granted_at, missing_since, flags],
// and its slot ordinal after them only when it has one: an entry of ordinal
// 0 is written as a gate before ordinals wrote it (§22.15).
func (e gateEntry) MarshalJSON() ([]byte, error) {
	flags := 0
	for bit, on := range map[int]bool{gateFlagVerifyPending: e.VerifyPending, gateFlagAdopted: e.Adopted,
		gateFlagGrantPending: e.GrantPending, gateFlagStalled: e.Stalled} {
		if on {
			flags |= bit
		}
	}
	fields := []any{e.Run, e.TaskIx, e.SeqBase, e.Limit, e.Strategy, e.Proto, e.LabelIx,
		gateMillis(e.ArrivedAt), e.Seq, gateMillis(e.WaitUntil), gateMillis(e.GrantedAt), gateMillis(e.MissingSince), flags}
	if e.N > 0 {
		fields = append(fields, e.N)
	}
	return stablejson.Marshal(fields)
}

// gateFlagsAll is every flag bit an entry may carry.
const gateFlagsAll = gateFlagVerifyPending | gateFlagAdopted | gateFlagGrantPending | gateFlagStalled

// gateParts splits an encoded array into exactly n fields, or n+1 with a
// trailing slot ordinal, none of them null: a null would decode as a zero,
// silently.
func gateParts(b []byte, what string, n int) ([]jsontext.Value, error) {
	var parts []jsontext.Value
	if err := json.Unmarshal(b, &parts); err != nil {
		return nil, fmt.Errorf("a gate %s is not an array: %w", what, err)
	}
	if len(parts) != n && len(parts) != n+1 {
		return nil, fmt.Errorf("a gate %s has %d fields, want %d or %d", what, len(parts), n, n+1)
	}
	for i, p := range parts {
		if string(p) == "null" {
			return nil, fmt.Errorf("a gate %s's field %d is null", what, i)
		}
	}
	return parts, nil
}

// UnmarshalJSON is MarshalJSON's strict inverse: an empty run or strategy,
// an unknown flag bit and a negative time are refused, not read as zeros.
func (e *gateEntry) UnmarshalJSON(b []byte) error {
	parts, err := gateParts(b, "entry", 13)
	if err != nil {
		return err
	}
	var arrived, wait, granted, missing int64
	var flags int
	var out gateEntry
	for i, dst := range []any{&out.Run, &out.TaskIx, &out.SeqBase, &out.Limit, &out.Strategy, &out.Proto, &out.LabelIx,
		&arrived, &out.Seq, &wait, &granted, &missing, &flags} {
		if err := json.Unmarshal(parts[i], dst); err != nil {
			return fmt.Errorf("a gate entry's field %d: %w", i, err)
		}
	}
	if len(parts) == 14 {
		if err := json.Unmarshal(parts[13], &out.N); err != nil {
			return fmt.Errorf("a gate entry's ordinal: %w", err)
		}
		if out.N < 1 || out.N >= gateOrdinalWire {
			// An ordinal of 0 is never written; one is the reason to write
			// the field.
			return fmt.Errorf("a gate entry %s has ordinal %d; a written ordinal is 1 to %d", b, out.N, gateOrdinalWire-1)
		}
	}
	if out.Run == "" || out.Strategy == "" || flags&^gateFlagsAll != 0 || arrived < 0 || wait < 0 || granted < 0 || missing < 0 ||
		out.TaskIx < gateRunLevel || out.Limit < 1 || out.LabelIx < 0 || out.SeqBase < 1 || out.Seq < 0 {
		return fmt.Errorf("a gate entry %s is malformed (run %q, strategy %q, flags %d)", b, out.Run, out.Strategy, flags)
	}
	out.ArrivedAt, out.WaitUntil, out.GrantedAt, out.MissingSince = gateTimeOf(arrived), gateTimeOf(wait), gateTimeOf(granted), gateTimeOf(missing)
	out.VerifyPending, out.Adopted = flags&gateFlagVerifyPending != 0, flags&gateFlagAdopted != 0
	out.GrantPending, out.Stalled = flags&gateFlagGrantPending != 0, flags&gateFlagStalled != 0
	*e = out
	return nil
}

// MarshalJSON encodes t as [key, run_level, below, until, removed_by], and
// the highest ordinal it fences after them only when that is above 0.
func (t gateTomb) MarshalJSON() ([]byte, error) {
	level := 0
	if t.RunLevel {
		level = 1
	}
	fields := []any{t.Key, level, t.Below, gateMillisUp(t.Until), t.RemovedBy}
	if t.N > 0 {
		fields = append(fields, t.N)
	}
	return stablejson.Marshal(fields)
}

// UnmarshalJSON is MarshalJSON's strict inverse: an empty key, a level
// other than 0 or 1 and a negative time are refused.
func (t *gateTomb) UnmarshalJSON(b []byte) error {
	parts, err := gateParts(b, "tombstone", 5)
	if err != nil {
		return err
	}
	var out gateTomb
	var level int
	var until int64
	for i, dst := range []any{&out.Key, &level, &out.Below, &until, &out.RemovedBy} {
		if err := json.Unmarshal(parts[i], dst); err != nil {
			return fmt.Errorf("a gate tombstone's field %d: %w", i, err)
		}
	}
	if len(parts) == 6 {
		if err := json.Unmarshal(parts[5], &out.N); err != nil {
			return fmt.Errorf("a gate tombstone's ordinal: %w", err)
		}
		if out.N < 1 || out.N >= gateOrdinalWire {
			return fmt.Errorf("a gate tombstone %s has ordinal %d; a written ordinal is 1 to %d", b, out.N, gateOrdinalWire-1)
		}
	}
	if out.Key == "" || (level != 0 && level != 1) || until < 0 {
		return fmt.Errorf("a gate tombstone %s is malformed", b)
	}
	out.RunLevel, out.Until = level == 1, gateTimeOf(until)
	*t = out
	return nil
}
