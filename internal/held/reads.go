package held

import (
	"context"
	"errors"
	"slices"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ErrUndecodable is a document that is there and this engine cannot decode.
var ErrUndecodable = errors.New("held: the source's document cannot be decoded")

// PendingState is where a pending activity is.
type PendingState int

// The states a describe reports.
const (
	Scheduled PendingState = iota + 1
	Started
	CancelRequested
)

// Pending is one pending activity of a describe.
type Pending struct {
	ActivityID string
	State      PendingState
	Attempt    int32
	LastWorker string
}

// Workflow is what one describe of a source's workflow says, in the fields
// the check reads. No execution is one that is not open.
type Workflow struct {
	Open    bool
	Pending []Pending
	Memo    Memo
}

// Memo is the part of the source workflow's memo the check reads.
type Memo struct {
	Slots  map[int32]Slot `json:"slots,omitzero"`
	Revoke *Revoke        `json:"revoke,omitzero"`
}

// Slot is one index's slot: what its instance runs on, and what a roll's
// replacement beside it does.
type Slot struct {
	Current     SlotEpoch  `json:"current"`
	Replacement *SlotEpoch `json:"replacement,omitzero"`
}

// SlotEpoch is what one epoch was scheduled with.
type SlotEpoch struct {
	Epoch         int64  `json:"epoch"`
	Digest        string `json:"digest"`
	AdmissionSave string `json:"admissionSave,omitzero"`
}

// Revoke is a revoke that is observed and not yet decided.
type Revoke struct {
	Save string `json:"save"`
}

// epoch is what the memo says epoch of index was scheduled with.
func (m Memo) epoch(index int32, epoch int64) (SlotEpoch, bool) {
	slot, ok := m.Slots[index]
	if !ok {
		return SlotEpoch{}, false
	}
	if slot.Current.Epoch == epoch {
		return slot.Current, true
	}
	if slot.Replacement != nil && slot.Replacement.Epoch == epoch {
		return *slot.Replacement, true
	}
	return SlotEpoch{}, false
}

// Document is the source's document under its name, in the fields the check
// reads and Source, the whole of it, for the caller.
type Document struct {
	Found    bool
	ID       string
	Pool     string
	Paused   bool
	Deleting bool
	// Source is the document, opaque to the check.
	Source any
}

// OrkerRow is an Orker's own account of itself as a source host.
type OrkerRow struct {
	Pools        []string
	Fit          bool
	MaxInstances int32
}

func (r OrkerRow) hosts(pool string) bool {
	return r.Fit && r.MaxInstances > 0 && slices.Contains(r.Pools, pool)
}

// Decider decides one permission at one pool over one access document.
type Decider func(subject *authz.Subject, perm authz.Permission, pool string) bool

// Access is the replica's access document.
type Access interface {
	// Held decides over the document the replica holds; fresh says it was
	// validated recently enough to grant from. It never refuses anybody.
	Held(subject *authz.Subject, perm authz.Permission, pool string) (granted, fresh bool)
	// Read reads the document now, and answers with a decider over it.
	Read(ctx context.Context) (Decider, error)
}

// Clients are the raw reads. Each makes one read when it is called and keeps
// nothing: the Reader alone does.
type Clients struct {
	// Describe describes the workflow of the source of that id.
	Describe func(ctx context.Context, sourceID string) (Workflow, error)
	// Tombstoned reports whether the source was deleted.
	Tombstoned func(ctx context.Context, sourceID string) (bool, error)
	// Document reads the document under a name, validated against the store.
	Document func(ctx context.Context, namespace, name string) (Document, error)
	// Confirmed reads an attempt's confirm record.
	Confirmed func(ctx context.Context, a artifacts.InstanceAttempt) (artifacts.ConfirmRecord, bool, error)
	// Orker asks one Orker for its row.
	Orker  func(ctx context.Context, orkerID string) (OrkerRow, error)
	Access Access
}
