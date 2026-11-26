// Package held answers one question for an engine: does this Orker hold this
// attempt of this instance of an event source, now, and may it act for it?
// It is asked in two places, the host service's calls and the identity mint,
// and is written once so that the two cannot differ. A change to it, a
// defect's fix included, is made by agreement of both callers' owners.
//
// What a request says is only ever compared. What is believed is the
// cluster's describe of the source's workflow, the store's tombstone,
// document and confirm record, the replica's access document, and the
// Orker's row of the inventory, in that order.
package held

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// Purpose is what the caller asks to do.
type Purpose int

// The host service's six calls, and the mint.
const (
	PurposeConfirm Purpose = iota + 1
	PurposeReport
	PurposeOffer
	PurposeStart
	PurposeProbe
	PurposeRequestPause
	PurposeMint
)

func (p Purpose) String() string {
	switch p {
	case PurposeConfirm:
		return "confirm"
	case PurposeReport:
		return "report"
	case PurposeOffer:
		return "offer"
	case PurposeStart:
		return "start"
	case PurposeProbe:
		return "probe"
	case PurposeRequestPause:
		return "request-pause"
	case PurposeMint:
		return "mint"
	}
	return fmt.Sprintf("purpose(%d)", int(p))
}

// heldWhileCancelled are the purposes an activity whose cancellation was
// asked is still held for: a start in flight, and a report.
func (p Purpose) heldWhileCancelled() bool { return p == PurposeStart || p == PurposeReport }

// permissions are what the caller must hold at the document's pool.
func (p Purpose) permissions() []authz.Permission {
	if p == PurposeMint {
		return []authz.Permission{authz.PermEventSourceHost, authz.PermIdentityTokenMint}
	}
	return []authz.Permission{authz.PermEventSourceHost}
}

// Ref names the attempt, as the request names it.
type Ref struct {
	Namespace     string
	Name          string
	SourceID      string
	Index         int32
	Epoch         int64
	Attempt       int32
	ServiceDigest string
}

func (r Ref) attempt() artifacts.InstanceAttempt {
	return artifacts.InstanceAttempt{SourceID: r.SourceID, Index: int(r.Index), Epoch: r.Epoch, Attempt: int(r.Attempt)}
}

// valid is a request some attempt could be named by: every field is part of
// a key, so one out of its form is refused before anything is read.
func (r Ref) valid() bool {
	return eventsources.ValidName(r.Namespace) && eventsources.ValidName(r.Name) && eventsources.ValidID(r.SourceID) &&
		r.Attempt >= 1 && r.attempt().Valid() == nil && r.ServiceDigest != ""
}

// Caller is who asks, from its credential and never from the request.
type Caller struct {
	OrkerID string
	Subject *authz.Subject
}

// ActivityID is the id of the activity that runs one instance's epoch, which
// the source's workflow schedules it under.
func ActivityID(index int32, epoch int64) string {
	return "evs/" + strconv.FormatInt(int64(index), 10) + "/" + strconv.FormatInt(epoch, 10)
}

// Kind is what the check found.
type Kind int

const (
	// Held: the caller holds it and may go on.
	Held Kind = iota + 1
	// NotHeld: it does not. Which says why, for the engine's log and never
	// for an answer.
	NotHeld
	// NotPermitted: the caller's role lacks Permission at Pool. One Orker's
	// fault and an operator's to fix: never the source's.
	NotPermitted
	// Tombstoned: the source was deleted.
	Tombstoned
	// OtherID: the name holds another source's document.
	OtherID
	// NoDocument: the name holds none.
	NoDocument
	// PausedOrMarked: the source is paused, or marked for deletion: Which.
	PausedOrMarked
	// Unanswered: a read (Read) was not had, for a failure or a cap. It is
	// never a refusal.
	Unanswered
)

// What PausedOrMarked says in Which.
const (
	Paused = "paused"
	Marked = "marked"
)

// Read is one of the check's reads.
type Read string

// The reads a check makes.
const (
	ReadDescribe   Read = "describe"
	ReadConfirm    Read = "confirm record"
	ReadTombstone  Read = "tombstone"
	ReadDocument   Read = "document"
	ReadPermission Read = "permission"
	ReadInventory  Read = "inventory"
)

// Result is the check's answer.
type Result struct {
	Kind Kind
	// Which says which, for NotHeld (the log's) and PausedOrMarked.
	Which string
	// Permission and Pool are NotPermitted's.
	Permission authz.Permission
	Pool       string
	// Read is the read Unanswered did not have, Err why, and Undecodable a
	// document that is there and this engine cannot decode.
	Read        Read
	Err         error
	Undecodable bool
	// RevokePending, on Held for PurposeConfirm and PurposeMint: a revoke of
	// the attempt's admission save is observed and not yet decided. It is
	// read from the memo of the describe the check went by: one sent since
	// the call arrived for the mint and for an attempt's first confirmation,
	// and a kept one at most describeAge old for a confirmed attempt.
	RevokePending bool
	// Epoch is, on Held, what the attempt's epoch was scheduled with, from
	// its confirm record when it has one; Confirmed says whether it has.
	Epoch     SlotEpoch
	Confirmed bool
	// Document is, on Held, the source's document as this call read it.
	Document Document
}

func notHeldBecause(which string) Result { return Result{Kind: NotHeld, Which: which} }

func noAnswer(read Read, err error) Result {
	return Result{Kind: Unanswered, Read: read, Err: err, Undecodable: errors.Is(err, ErrUndecodable)}
}

// Instance is the check: whether the caller holds the attempt ref names, for
// purpose. arrived is when the request arrived: a refusal that rests on
// something not found or not granted rests on a read sent no earlier. The
// reads after the first have no cap of their own: the caller bounds how often
// one source's holder asks.
func Instance(ctx context.Context, r *Reader, purpose Purpose, ref Ref, caller Caller, arrived time.Time) Result {
	if purpose < PurposeConfirm || purpose > PurposeMint {
		return notHeldBecause("no such purpose")
	}
	if !ref.valid() || caller.OrkerID == "" {
		return notHeldBecause("the request names no attempt an Orker can hold")
	}

	// (1) and (2): the workflow is open, and the caller holds its activity.
	// Nothing else is read for a caller that does not.
	holding := func(wf Workflow) bool { return holds(wf, purpose, ref, caller) == "" }
	wf, err := r.describe(ctx, purpose == PurposeMint, ref, caller, arrived, holding, holding)
	if err != nil {
		return noAnswer(ReadDescribe, err)
	}
	if why := holds(wf, purpose, ref, caller); why != "" {
		return notHeldBecause(why)
	}

	// (2), the digest its epoch was scheduled with: the confirm record's,
	// which never changes, or for an attempt with none the memo's, and never
	// the workflow's target or its last good revision.
	record, confirmed, err := r.confirmed(ctx, ref, arrived)
	if err != nil {
		return noAnswer(ReadConfirm, err)
	}
	epoch := SlotEpoch{Epoch: ref.Epoch, Digest: record.ServiceDigest, AdmissionSave: record.AdmissionSave}
	switch {
	case !confirmed && purpose == PurposeMint:
		return noAnswer(ReadConfirm, errors.New("the attempt was never confirmed"))
	case !confirmed:
		scheduled := func(wf Workflow) bool {
			e, ok := wf.Memo.epoch(ref.Index, ref.Epoch)
			return ok && e.Digest == ref.ServiceDigest
		}
		if purpose == PurposeConfirm || !scheduled(wf) {
			// A kept memo may be from before this epoch was scheduled, or
			// before a revoke: the first confirmation makes the record a
			// credential rests on, so its own is never kept.
			confirms := func(wf Workflow) bool { return holding(wf) && scheduled(wf) }
			if wf, err = r.describe(ctx, purpose == PurposeConfirm, ref, caller, arrived, confirms, holding); err != nil {
				return noAnswer(ReadDescribe, err)
			}
			if why := holds(wf, purpose, ref, caller); why != "" {
				return notHeldBecause(why)
			}
		}
		var ok bool
		if epoch, ok = wf.Memo.epoch(ref.Index, ref.Epoch); !ok {
			return notHeldBecause("the workflow has no slot on that epoch")
		}
	}
	if epoch.Digest != ref.ServiceDigest {
		return notHeldBecause("the request presents another service spec's digest")
	}

	// (3) and (4): the source was not deleted, and its name holds its own
	// document, neither paused nor marked.
	tombstoned, err := r.tombstoned(ctx, ref.SourceID, arrived)
	if err != nil {
		return noAnswer(ReadTombstone, err)
	}
	if tombstoned {
		return Result{Kind: Tombstoned}
	}
	doc, err := r.document(ctx, ref, arrived)
	switch {
	case err != nil:
		return noAnswer(ReadDocument, err)
	case !doc.Found:
		return Result{Kind: NoDocument}
	case doc.ID != ref.SourceID:
		return Result{Kind: OtherID}
	case doc.Deleting:
		return Result{Kind: PausedOrMarked, Which: Marked}
	case doc.Paused:
		return Result{Kind: PausedOrMarked, Which: Paused}
	}

	// (5): every permission of the purpose, at the pool the document gives.
	for _, perm := range purpose.permissions() {
		granted, err := r.permitted(ctx, caller, perm, doc.Pool, arrived)
		if err != nil {
			return noAnswer(ReadPermission, err)
		}
		if !granted {
			return Result{Kind: NotPermitted, Permission: perm, Pool: doc.Pool}
		}
	}

	// (6): the caller's Orker is a fit source host of that pool.
	row, err := r.orker(ctx, caller.OrkerID, arrived, func(row OrkerRow) bool { return row.hosts(doc.Pool) })
	if err != nil {
		return noAnswer(ReadInventory, err)
	}
	if !row.hosts(doc.Pool) {
		return notHeldBecause("the Orker is no fit source host of the source's pool")
	}

	result := Result{Kind: Held, Epoch: epoch, Confirmed: confirmed, Document: doc}
	if purpose == PurposeConfirm || purpose == PurposeMint {
		revoke := wf.Memo.Revoke
		result.RevokePending = revoke != nil && revoke.Save == epoch.AdmissionSave
	}
	return result
}

// holds is (1) and (2)'s activity over one describe: why the caller does not
// hold the attempt, "" when it does.
func holds(wf Workflow, purpose Purpose, ref Ref, caller Caller) string {
	if !wf.Open {
		return "the source's workflow is not open"
	}
	id := ActivityID(ref.Index, ref.Epoch)
	i := slices.IndexFunc(wf.Pending, func(p Pending) bool { return p.ActivityID == id })
	if i < 0 {
		return "the workflow has no such activity pending"
	}
	p := wf.Pending[i]
	switch {
	case p.State != Started && p.State != CancelRequested:
		return "the activity is not started"
	case p.Attempt != ref.Attempt:
		return "the activity is at another attempt"
	case p.LastWorker != caller.OrkerID:
		return "another Orker holds the activity"
	case p.State == CancelRequested && !purpose.heldWhileCancelled():
		return "the activity's cancellation was asked"
	}
	return ""
}
