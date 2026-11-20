package sourcehost

import (
	"context"
	"encoding/json/jsontext"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Door is the engine's side of a source's messages as the Orker calls it
// (01 §4.2): the host service. Its answers are typed; an error is a call
// that got no answer (a transport failure, a timeout, a refused or
// overloaded replica), which is always a wait and never decides anything
// about a message. The client that implements it never decides from a
// connection's code.
type Door interface {
	// Confirm says which judging revision is the source's document's now,
	// with its spec when it is not the one the Orker holds.
	Confirm(ctx context.Context, revision int64) (Confirmation, error)
	// Offer judges a message the Orker has prejudged.
	Offer(ctx context.Context, o Offer) (Answer, error)
	// Start starts the message's run from its staged parts.
	Start(ctx context.Context, s Start) (Answer, error)
	// Probe is a waiting instance's question instead of a message (01
	// §4.8): nil when admission, a store write and a start-free call pass.
	Probe(ctx context.Context) error
	// RequestPause asks the engine to pause the source with one of the
	// breaker codes the Orker counts (01 §7).
	RequestPause(ctx context.Context, code string) error
}

// Confirmation is Confirm's answer.
type Confirmation struct {
	Revision int64
	// Source is the judging spec's source when Revision is not the one
	// asked about; nil otherwise.
	Source *orkstackv1.EventSource
	// Stop: the source is paused or deleted; the instance ends.
	Stop bool
}

// Offer is what the Orker sends about one message.
type Offer struct {
	Message      HandedMessage
	BodyDigest   string
	BodySize     int64
	Values       map[string]jsontext.Value
	PreRejection string
	Revision     int64
}

// The kinds of a part an offer asks the Orker to stage.
const (
	PartBody     = "body"
	PartMetadata = "metadata"
)

// Part is one part of a message the offer asks the Orker to stage.
type Part struct {
	Input       string
	Kind        string // PartBody or PartMetadata
	ContentType string
}

// Start is the start of a message's run from what was staged.
type Start struct {
	Offer  Offer
	Staged []StagedPart
	Retry  bool
}

// StagedPart is one part as staged: its input, digest and size.
type StagedPart struct {
	Input  string
	Digest string
	Size   int64
}

// AnswerKind is what the door decided.
type AnswerKind int

const (
	// AnswerProceed: stage the parts and start.
	AnswerProceed AnswerKind = iota + 1
	// AnswerAccepted: the run started; the message is settled.
	AnswerAccepted
	// AnswerDuplicate: the message's run exists; the message is settled.
	AnswerDuplicate
	// AnswerReject: the message can never be accepted.
	AnswerReject
	// AnswerStop: the source is paused (or deleted); the instance ends.
	AnswerStop
	// AnswerRefresh: the judging spec changed; judge again with it.
	AnswerRefresh
	// AnswerWait: nothing is wrong with the message or the source; wait.
	AnswerWait
	// AnswerUploadAgain: a staged part is missing; stage and start again.
	AnswerUploadAgain
	// AnswerUncertain: a refusal the engine's classifier does not know.
	AnswerUncertain
)

// Answer is one answer of the door.
type Answer struct {
	Kind   AnswerKind
	Reason string
	Cause  string
	RunID  string
	Parts  []Part
	Source *orkstackv1.EventSource
	At     time.Time
}
