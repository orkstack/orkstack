package engine

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// CancelRequest is what one CancelRun asks for: the caller's reason, which
// the audit trail records, and the principal the caller's gateway asserted
// in Ork-Principal, which the run records (Run.canceled_by). A struct rather
// than two strings, so the two cannot be passed in each other's place.
type CancelRequest struct {
	Reason    string
	Principal string
}

// cancelRequestFormat is RunCancelRequest.format: the one shape this release
// writes and the one it reads back.
const cancelRequestFormat = 1

// runCanceledCode is the run-level code of a run a cancellation ended.
const runCanceledCode = "RUN_CANCELED"

// CanceledByMaxBytes bounds Run.canceled_by. The API truncates the header to
// it before the cancellation is requested (internal/api, boundOriginAssertion,
// the bound RunOrigin.principal has too), so a record holding more was not
// written by CancelRun and records nobody.
const CanceledByMaxBytes = 256

// ErrCancelNotWired reports a starter asked to record a principal on a
// cancellation without the cluster namespace the request must name
// (TemporalStarter.WithClusterIdentity). It is refused rather than sent
// without the principal: a cancel that silently dropped the name would leave
// the run saying nobody canceled it. Only a mis-wired process meets it, so
// the API answers it as the internal error it is.
var ErrCancelNotWired = errors.New("this engine was started without what it needs to record who canceled a run, so the cancellation was not requested")

// encodeCancelRequest is the reason CancelRun gives the cluster: a
// RunCancelRequest naming principal, as protojson.
func encodeCancelRequest(principal string) (string, error) {
	data, err := protojson.Marshal(&orkstackv1.RunCancelRequest{
		Format:    cancelRequestFormat,
		Principal: principal,
	})
	if err != nil {
		return "", fmt.Errorf("encode cancel request: %w", err)
	}
	return string(data), nil
}

// cancelPrincipal is the principal a run's cancellation request recorded, read
// from the request's cause. Anything CancelRun did not write records nobody
// and reads as "": an operator's own reason text, a parent run's request
// (which carries no cause), a cancellation requested by a release that wrote
// no record, a record of another format, and a principal over the bound or
// not valid UTF-8.
//
// Unknown fields are discarded, so a later release may add some without an
// earlier reader losing the name.
func cancelPrincipal(cause string) string {
	if cause == "" {
		return ""
	}
	var request orkstackv1.RunCancelRequest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(cause), &request); err != nil {
		return ""
	}
	principal := request.GetPrincipal()
	if request.GetFormat() != cancelRequestFormat ||
		len(principal) > CanceledByMaxBytes || !utf8.ValidString(principal) {
		return ""
	}
	return principal
}

// applyCanceledBy records principal as who canceled run, and only when the
// run's own record says a cancellation ended it: status CANCELED with code
// RUN_CANCELED. A run terminated from outside (RUN_TERMINATED), a failed or
// succeeded one, and a run still draining after its cancel was requested
// (RUNNING) are left as they are. An empty principal sets nothing.
//
// It is the one rule, shared by the run reader (GetRun) and the projection
// activity (the archived run and the webhook delivery), so every surface
// answers the same.
func applyCanceledBy(run *orkstackv1.Run, principal string) {
	if run == nil || principal == "" {
		return
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != runCanceledCode {
		return
	}
	run.CanceledBy = principal
}
