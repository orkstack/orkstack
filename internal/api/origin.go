package api

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// OriginReferenceHeader carries a caller's correlation reference on the
// plain-HTTP trigger door, where the body is the run's input and must stay
// so (docs/ORKFLOW_TRIGGERS.md). Over Connect the same value is
// SubmitRunRequest.origin_reference; one spelling per surface, never both.
const OriginReferenceHeader = "Ork-Origin-Reference"

// OriginReferenceMaxBytes bounds RunOrigin.reference. A reference is a
// correlation key, not a document; 256 bytes is the bound the telemetry
// attribute values and the audit principal already use.
const OriginReferenceMaxBytes = 256

// originAssertionMaxBytes bounds the two origin fields the submitter did not
// author — the principal a gateway asserted and a trigger's description.
const originAssertionMaxBytes = 256

// ValidateOriginReference is the one rule for a reference: at most
// OriginReferenceMaxBytes, valid UTF-8, no byte in U+0000–U+001F or U+007F.
// Validated, never sanitized (AGENTS.md §7). The error names the limit the
// value broke.
func ValidateOriginReference(ref string) error {
	if len(ref) > OriginReferenceMaxBytes {
		return fmt.Errorf("origin reference is %d bytes; the limit is %d bytes", len(ref), OriginReferenceMaxBytes)
	}
	if !utf8.ValidString(ref) {
		return fmt.Errorf("origin reference is not valid UTF-8")
	}
	for i := 0; i < len(ref); i++ {
		if b := ref[i]; b < 0x20 || b == 0x7f {
			return fmt.Errorf("origin reference contains control character 0x%02x at byte %d; "+
				"a reference is one line of text of at most %d bytes", b, i, OriginReferenceMaxBytes)
		}
	}
	return nil
}

// boundOriginAssertion truncates an ASSERTION the caller did not author — a
// principal a gateway set, a description an operator typed into a trigger
// months ago — to 256 bytes with artifacts.AttemptTruncationMarker. A refusal
// here would fail a submit for something the submitter cannot change, which
// is why these two fields are bounded and the reference is validated.
//
// Invalid UTF-8 is replaced rather than refused, on the same reasoning: a
// protobuf string must be valid UTF-8 to be encoded at all, and a gateway's
// stray byte must not be what fails the run's snapshot.
func boundOriginAssertion(s string) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= originAssertionMaxBytes {
		return s
	}
	keep := originAssertionMaxBytes - len(artifacts.AttemptTruncationMarker)
	for keep > 0 && !utf8.RuneStart(s[keep]) {
		keep--
	}
	return s[:keep] + artifacts.AttemptTruncationMarker
}

// submitOrigin is the SUBMIT/RUN_AGAIN origin for one Connect request. It is
// the ONLY place a public request becomes an origin, and it can produce
// exactly two kinds. It is called by originForRequest only, after that
// function has validated the reference and confirmed sourceRunID exists.
func submitOrigin(req *connect.Request[orkstackv1.SubmitRunRequest], sourceRunID string) *orkstackv1.RunOrigin {
	origin := &orkstackv1.RunOrigin{
		Kind:      orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT,
		Reference: req.Msg.GetOriginReference(),
		Principal: boundOriginAssertion(req.Header().Get(AuditPrincipalHeader)),
	}
	if sourceRunID != "" {
		origin.Kind = orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN
		origin.Id = sourceRunID
	}
	return origin
}

// originForRequest is the one owner of reference validation and the
// source-run check on the Connect door. It runs before submitFrom, and so
// before prepare reads anything: the reference is checked first because it is
// the request's own mistake and costs nothing, then a named source run is
// confirmed to exist in the request's namespace (NotFound otherwise, on the
// rule every run read follows), then the origin is built.
func (s *Service) originForRequest(
	ctx context.Context,
	req *connect.Request[orkstackv1.SubmitRunRequest],
) (*orkstackv1.RunOrigin, error) {
	if err := ValidateOriginReference(req.Msg.GetOriginReference()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	sourceRunID := req.Msg.GetSourceRunId()
	if sourceRunID != "" {
		if _, err := s.LookupRunIn(ctx, sourceRunID, req.Msg.GetNamespace()); err != nil {
			return nil, err
		}
	}
	return submitOrigin(req, sourceRunID), nil
}
