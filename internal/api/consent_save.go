package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// reasonConsentMismatch refuses a save whose consent is not to what the
// Orkflow asks for now (docs/REASONS.md).
const reasonConsentMismatch = "IDENTITY_CONSENT_MISMATCH"

// consentRecord is the consent an object stores beside its binder: the
// digest given, when, by whom, and the short list a person reads
// (workload-identity §3.2).
type consentRecord struct {
	Digest    string
	At        time.Time
	By        string
	Audiences []string
	Grants    []string
}

// view is the record as the API shows it.
func (c consentRecord) view() *orkstackv1.ConsentView {
	if !consent.Valid(c.Digest) {
		return nil
	}
	return &orkstackv1.ConsentView{Digest: c.Digest, At: timestamppb.New(c.At), Audiences: c.Audiences, Grants: c.Grants}
}

// consentSave is one save of a schedule, a trigger or an event source, as
// the consent rule judges it.
type consentSave struct {
	Kind    string // "schedule", "trigger", "event-source": the orkctl noun
	Name    string // the object's name, for the command a refusal hands out
	Orkflow string
	// Binds is a save that makes its caller the binder: a create, or a save
	// that changes what the object submits.
	Binds bool
	// Given is the request's consent: "" is none.
	Given string
	Asks  consent.Asks
	// Unknown is an Orkflow whose composed children cannot all be read, so
	// what it asks for cannot be known: a save that binds records no
	// consent, and its runs that would hold an identity are refused one at
	// admission until someone consents. A consent given to it is refused.
	Unknown bool
	Prior   consentRecord
	By      string
	Now     time.Time
}

// decideConsent is the save table of workload-identity §3.2. A consent is
// recorded only for the digest of what the Orkflow asks for at this moment;
// a save that binds needs one unless the Orkflow asks for nothing; a save
// that binds nothing and gives none keeps the consent it had. A refusal is
// IDENTITY_CONSENT_MISMATCH, failed_precondition, with what changed and the
// digest to consent to, and records nothing.
func decideConsent(s consentSave) (consentRecord, error) {
	switch {
	case s.Unknown && s.Given != "":
		return consentRecord{}, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: what Orkflow %s asks for cannot be known, because an Orkflow it composes is not there; nothing can be consented to until it is", reasonConsentMismatch, s.Orkflow))
	case s.Unknown && s.Binds:
		return consentRecord{}, nil
	case s.Unknown:
		return s.Prior, nil
	case s.Given != "" && s.Given == s.Asks.Digest:
		return consentRecord{Digest: s.Given, At: s.Now, By: s.By, Audiences: s.Asks.Audiences, Grants: s.Asks.Grants}, nil
	case s.Given != "":
		return consentRecord{}, consentRefusal(s, fmt.Sprintf(
			"what Orkflow %s asks for has changed since you were shown it", s.Orkflow))
	case !s.Binds:
		return s.Prior, nil
	case s.Asks.Empty():
		return consentRecord{Digest: s.Asks.Digest, At: s.Now, By: s.By}, nil
	default:
		return consentRecord{}, consentRefusal(s, fmt.Sprintf(
			"Orkflow %s asks for an identity, and this save binds the %s", s.Orkflow, s.Kind))
	}
}

func consentRefusal(s consentSave, why string) error {
	var shown *orkstackv1.ConsentView
	if s.Given != "" {
		shown = s.Prior.view()
		if shown == nil {
			shown = &orkstackv1.ConsentView{Digest: s.Given}
		}
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s: %s: %s. Consent with `orkctl %s update %s --consent=%s`",
		reasonConsentMismatch, why, consent.Difference(shown, s.Asks), s.Kind, s.Name, s.Asks.Digest))
}

// asksFor is consent projection 1 of Orkflow name at version as an object
// writes it ("" follows the current pointer), its composed Orkflows read at
// the versions they pin. An Orkflow or a child that is not there is
// not_found or failed_precondition; a store that does not answer is never
// "asks for nothing".
func (s *Service) asksFor(ctx context.Context, ns, name, version string) (consent.Asks, error) {
	def, err := s.orkflows.Get(ctx, ns, name, version)
	if err != nil {
		return consent.Asks{}, orkflowStoreError(err, "orkflow %q version %q in namespace %q", name, version, ns)
	}
	asks, err := consent.Compute(ctx, artifacts.ResolveNamespace(ns), def,
		func(ctx context.Context, owner, child, pinned string) (*orkstackv1.Orkflow, error) {
			got, err := s.orkflows.Get(ctx, owner, child, pinned)
			if errors.Is(err, artifacts.ErrNotFound) {
				return nil, nil
			}
			return got, err
		})
	switch {
	case errors.Is(err, consent.ErrChildMissing), errors.Is(err, consent.ErrTooDeep):
		return consent.Asks{}, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"what orkflow %q asks for cannot be known: %w", name, err))
	case err != nil:
		return consent.Asks{}, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"what orkflow %q asks for could not be read: %w", name, err))
	}
	return asks, nil
}

// unknownAsks reports an asksFor error that says what an Orkflow asks for
// cannot be known (a composed Orkflow is not there), as against one it does
// not exist or that the store did not answer.
func unknownAsks(err error) bool {
	return errors.Is(err, consent.ErrChildMissing) || errors.Is(err, consent.ErrTooDeep)
}

// asksForSave is asksFor for a save: an Orkflow whose children cannot all be
// read is unknown, not refused, so a save that would have been accepted
// before consent existed still is (decideConsent records no consent).
func (s *Service) asksForSave(ctx context.Context, ns, name, version string) (consent.Asks, bool, error) {
	asks, err := s.asksFor(ctx, ns, name, version)
	if unknownAsks(err) {
		return consent.Asks{}, true, nil
	}
	return asks, false, err
}

// asksMemo answers asksFor once for each Orkflow version within one
// request, so a list of many objects bound to one Orkflow reads it once.
type asksMemo map[string]asksAnswer

type asksAnswer struct {
	asks consent.Asks
	err  error
}

// asksForMemo is asksFor through memo; a nil memo reads every time.
func (s *Service) asksForMemo(ctx context.Context, memo asksMemo, ns, name, version string) (consent.Asks, error) {
	if memo == nil {
		return s.asksFor(ctx, ns, name, version)
	}
	key := ns + "\x00" + name + "\x00" + version
	if a, ok := memo[key]; ok {
		return a.asks, a.err
	}
	asks, err := s.asksFor(ctx, ns, name, version)
	memo[key] = asksAnswer{asks, err}
	return asks, err
}
