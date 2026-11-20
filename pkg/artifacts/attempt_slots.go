package artifacts

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// The cap on attempts (docs/TASK_IDENTITY.md, "The cap on attempts"): each
// task of an admitted run, and each item of a fan-out, may hold an identity
// in at most identity_slots attempts, each recorded by one create-only
// slot object under the run's identity prefix.
const (
	attemptSlotFormat   = 1
	maxAttemptSlotBytes = 256
)

var attemptSlotKeys = []string{"v", "attempt", "started_at", "taken_at"}

// AttemptSlotKey renders the key of one slot:
// metadata/snapshots/{ns}/{run-id}/identity/slots/{task}/{item or "-"}/{scope}/{n}.json,
// n from 1 to MaxAttemptSlots and scope 0 for a run as it was admitted.
func AttemptSlotKey(namespace, runID, taskID string, item Item, scope uint64, n int) (string, error) {
	if n < 1 || n > MaxAttemptSlots {
		return "", fmt.Errorf("artifacts: attempt slot key: slot %d is not 1 to %d: %w", n, MaxAttemptSlots, ErrInvalidRecordKey)
	}
	if item.Set && item.Index < 0 {
		return "", fmt.Errorf("artifacts: attempt slot key: item %d is no item: %w", item.Index, ErrInvalidRecordKey)
	}
	if err := validCommitTaskID(taskID); err != nil {
		return "", fmt.Errorf("artifacts: attempt slot key: task id: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	itemSegment := "-"
	if item.Set {
		itemSegment = strconv.Itoa(int(item.Index))
	}
	return fmt.Sprintf("%sslots/%s/%s/%d/%d.json", prefix, taskID, itemSegment, scope, n), nil
}

// SlotHolder is the attempt a slot is held by, as the cluster reports it:
// its attempt number and the time the cluster started it. Two dispatches
// never share a start time, so a holder names one attempt.
type SlotHolder struct {
	Attempt   int32
	StartedAt time.Time
}

func (h SlotHolder) same(o SlotHolder) bool {
	return h.Attempt == o.Attempt && h.StartedAt.Equal(o.StartedAt)
}

// AttemptSlotRequest asks for the slot of one attempt of one task (or
// item) under one scope, among Slots, at the engine's time Now.
type AttemptSlotRequest struct {
	Namespace, RunID, TaskID string
	Item                     Item
	Scope                    uint64
	Slots                    int
	Holder                   SlotHolder
	Now                      time.Time
}

// SlotAnswer is what the search for an attempt's slot found.
type SlotAnswer int

const (
	// SlotHeld: the attempt holds slot N, taken at TakenAt (now, or by an
	// earlier request of the same attempt).
	SlotHeld SlotAnswer = iota + 1
	// SlotExhausted: every slot is held by another attempt, named in
	// Holders.
	SlotExhausted
	// SlotInvalid: the object at Object is not a slot this binary can
	// trust; the search stopped there.
	SlotInvalid
)

// AttemptSlot answers TakeAttemptSlot.
type AttemptSlot struct {
	Answer  SlotAnswer
	N       int
	TakenAt time.Time
	// Holders are the other attempts holding the slots, for an exhausted
	// answer's audit record.
	Holders []SlotHolder
	Object  string
	Reason  string
}

// TakeAttemptSlot finds the attempt's slot: from n = the attempt's own
// number, wrapping through 1 to Slots, it creates the slot. Created, the
// attempt holds n; already there and held by the same attempt, it holds n
// with the time first recorded; held by another, the next n. A request it
// cannot address is ErrInvalidRecordKey, before the store is asked; a store
// that does not answer is RECORD_STORE_UNANSWERED. Whether the slot has
// outlived the attempt's deadline is the caller's to judge from TakenAt.
func (s *RecordStore) TakeAttemptSlot(ctx context.Context, req AttemptSlotRequest) (AttemptSlot, error) {
	if req.Slots < 1 || req.Slots > MaxAttemptSlots {
		return AttemptSlot{}, fmt.Errorf("artifacts: attempt slot: %d slots is not 1 to %d: %w", req.Slots, MaxAttemptSlots, ErrInvalidRecordKey)
	}
	if req.Holder.Attempt < 1 || req.Holder.StartedAt.IsZero() || req.Now.IsZero() {
		return AttemptSlot{}, fmt.Errorf("artifacts: attempt slot: an attempt needs a number from 1, a start and a clock: %w", ErrInvalidRecordKey)
	}
	keys := make([]string, req.Slots)
	for i := range keys {
		key, err := AttemptSlotKey(req.Namespace, req.RunID, req.TaskID, req.Item, req.Scope, i+1)
		if err != nil {
			return AttemptSlot{}, err
		}
		keys[i] = key
	}
	body := encodeAttemptSlot(req.Holder, req.Now)
	first := int((req.Holder.Attempt-1)%int32(req.Slots)) + 1
	var holders []SlotHolder
	for i := range req.Slots {
		n := (first-1+i)%req.Slots + 1
		key := keys[n-1]
		err := s.createIn(ctx, key, body)
		if err == nil {
			return AttemptSlot{Answer: SlotHeld, N: n, TakenAt: req.Now.UTC().Round(0)}, nil
		}
		if !errors.Is(err, errBlobExists) {
			return AttemptSlot{}, err
		}
		raw, found, err := s.getIn(ctx, key, maxAttemptSlotBytes+1)
		if err != nil {
			return AttemptSlot{}, err
		}
		if !found {
			// It was there when the create was refused and is gone now:
			// only the run's own removal deletes a slot.
			return AttemptSlot{}, &RecordStoreUnanswered{Class: UnansweredUnavailable,
				Err: fmt.Errorf("artifacts: attempt slot %s was there and is gone", key)}
		}
		h, taken, answer, reason := decodeAttemptSlot(raw)
		if answer != RecordFound {
			if answer == RecordUnsupported {
				reason = "a newer format than this release reads; mint on an engine of the current release"
			}
			return AttemptSlot{Answer: SlotInvalid, Object: key, Reason: reason}, nil
		}
		if h.same(req.Holder) {
			return AttemptSlot{Answer: SlotHeld, N: n, TakenAt: taken}, nil
		}
		holders = append(holders, h)
	}
	return AttemptSlot{Answer: SlotExhausted, Holders: holders}, nil
}

// encodeAttemptSlot is a slot's stored bytes, its times in UTC with
// nanoseconds.
func encodeAttemptSlot(h SlotHolder, takenAt time.Time) []byte {
	return fmt.Appendf(nil, "{\"v\":%d,\"attempt\":%d,\"started_at\":%q,\"taken_at\":%q}\n", attemptSlotFormat, h.Attempt,
		h.StartedAt.UTC().Format(time.RFC3339Nano), takenAt.UTC().Format(time.RFC3339Nano))
}

// decodeAttemptSlot reads a slot. Only the exact bytes encodeAttemptSlot
// writes are a slot: anything else of format 1 is invalid, and a later
// format is unsupported.
func decodeAttemptSlot(raw []byte) (SlotHolder, time.Time, RecordAnswer, string) {
	if len(raw) > maxAttemptSlotBytes {
		return SlotHolder{}, time.Time{}, RecordInvalid, "too large"
	}
	version, bad := decodeVersioned(raw, attemptSlotFormat, attemptSlotKeys)
	switch {
	case bad != "":
		return SlotHolder{}, time.Time{}, RecordInvalid, bad
	case version > attemptSlotFormat:
		return SlotHolder{}, time.Time{}, RecordUnsupported, ""
	case version != attemptSlotFormat:
		return SlotHolder{}, time.Time{}, RecordInvalid, fmt.Sprintf("format %d is no format", version)
	}
	var w struct {
		V         int64  `json:"v"`
		Attempt   int32  `json:"attempt"`
		StartedAt string `json:"started_at"`
		TakenAt   string `json:"taken_at"`
	}
	if err := json.Unmarshal(raw, &w, json.RejectUnknownMembers(true)); err != nil {
		return SlotHolder{}, time.Time{}, RecordInvalid, "does not decode: " + err.Error()
	}
	started, err1 := time.Parse(time.RFC3339Nano, w.StartedAt)
	taken, err2 := time.Parse(time.RFC3339Nano, w.TakenAt)
	if err1 != nil || err2 != nil || w.Attempt < 1 || started.IsZero() || taken.IsZero() {
		return SlotHolder{}, time.Time{}, RecordInvalid, "names no attempt, start or time"
	}
	h := SlotHolder{Attempt: w.Attempt, StartedAt: started.UTC()}
	if !bytes.Equal(encodeAttemptSlot(h, taken), raw) {
		return SlotHolder{}, time.Time{}, RecordInvalid, "is not as a slot is written"
	}
	return h, taken.UTC(), RecordFound, ""
}
